LOCK TABLE work_items, job_attempts, job_outputs, provider_submissions,
    executor_executions, provider_submission_attachments IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_submissions submission
        JOIN executor_executions execution
          ON execution.executor_execution_id = submission.executor_execution_id
         AND execution.submission_id = submission.submission_id
        WHERE submission.state IN ('prepared', 'running')
           OR execution.state IN ('prepared', 'leased', 'running')
    ) THEN
        RAISE EXCEPTION
            'executor handoff migration requires active executor submissions to be drained';
    END IF;
END;
$$;

ALTER TABLE work_items
    ADD COLUMN handed_off_at_ms BIGINT,
    DROP CONSTRAINT work_items_state_check,
    DROP CONSTRAINT work_items_check,
    ADD CONSTRAINT work_items_state_check CHECK (
        state IN (
            'ready', 'leased', 'running', 'awaiting_executor',
            'succeeded', 'failed', 'uncertain'
        )
    ),
    ADD CONSTRAINT work_items_ownership_check CHECK (
        (state IN ('leased', 'running')
            AND lease_owner IS NOT NULL
            AND lease_expires_at_ms IS NOT NULL
            AND execution_id IS NOT NULL
            AND handed_off_at_ms IS NULL)
        OR
        (state = 'awaiting_executor'
            AND lease_owner IS NULL
            AND lease_expires_at_ms IS NULL
            AND execution_id IS NOT NULL
            AND execution_profile_id IS NOT NULL
            AND handed_off_at_ms IS NOT NULL)
        OR
        (state = 'ready'
            AND lease_owner IS NULL
            AND lease_expires_at_ms IS NULL
            AND handed_off_at_ms IS NULL)
        OR
        (state IN ('succeeded', 'failed', 'uncertain')
            AND lease_owner IS NULL
            AND lease_expires_at_ms IS NULL)
    );

ALTER TABLE job_attempts
    ADD COLUMN handed_off_at_ms BIGINT,
    DROP CONSTRAINT job_attempts_state_check,
    ADD CONSTRAINT job_attempts_state_check CHECK (
        state IN ('claimed', 'running', 'handed_off', 'succeeded', 'failed', 'uncertain')
    ),
    ADD CONSTRAINT job_attempts_handoff_shape_check CHECK (
        state <> 'handed_off'
        OR (
            handed_off_at_ms IS NOT NULL
            AND started_at_ms IS NULL
            AND finished_at_ms IS NULL
            AND error_code IS NULL
        )
    );

CREATE FUNCTION enforce_work_handoff_transition() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.handed_off_at_ms IS NOT NULL
       AND NEW.handed_off_at_ms IS DISTINCT FROM OLD.handed_off_at_ms THEN
        RAISE EXCEPTION 'work handoff timestamp is immutable';
    END IF;
    IF NEW.state = 'awaiting_executor'
       AND OLD.state NOT IN ('leased', 'awaiting_executor') THEN
        RAISE EXCEPTION 'work may only enter executor ownership from a lease';
    END IF;
    IF OLD.state = 'awaiting_executor'
       AND NEW.state NOT IN ('awaiting_executor', 'succeeded', 'failed', 'uncertain') THEN
        RAISE EXCEPTION 'executor-owned work cannot return to worker ownership';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER work_items_handoff_transition
    BEFORE UPDATE ON work_items
    FOR EACH ROW EXECUTE FUNCTION enforce_work_handoff_transition();

CREATE FUNCTION enforce_attempt_handoff_transition() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.handed_off_at_ms IS NOT NULL
       AND NEW.handed_off_at_ms IS DISTINCT FROM OLD.handed_off_at_ms THEN
        RAISE EXCEPTION 'attempt handoff timestamp is immutable';
    END IF;
    IF NEW.state = 'handed_off'
       AND OLD.state NOT IN ('claimed', 'handed_off') THEN
        RAISE EXCEPTION 'attempt may only hand off before worker execution starts';
    END IF;
    IF OLD.state = 'handed_off'
       AND NEW.state NOT IN ('handed_off', 'succeeded', 'failed', 'uncertain') THEN
        RAISE EXCEPTION 'handed-off attempt cannot return to worker ownership';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_attempts_handoff_transition
    BEFORE UPDATE ON job_attempts
    FOR EACH ROW EXECUTE FUNCTION enforce_attempt_handoff_transition();

CREATE FUNCTION validate_executor_handoff() RETURNS TRIGGER AS $$
DECLARE
    contract_version SMALLINT;
    expected_outputs INTEGER;
    attempt_state TEXT;
    attempt_handed_off_at BIGINT;
    output_count BIGINT;
    missing_output_count BIGINT;
    submission_count BIGINT;
    execution_count BIGINT;
    attachment_count BIGINT;
BEGIN
    IF NEW.state <> 'awaiting_executor' THEN
        RETURN NULL;
    END IF;

    SELECT job.economics_contract_version, job.requested_units,
           attempt.state, attempt.handed_off_at_ms
      INTO contract_version, expected_outputs, attempt_state, attempt_handed_off_at
    FROM jobs job
    JOIN job_attempts attempt
      ON attempt.work_item_id = NEW.work_item_id
     AND attempt.execution_id = NEW.execution_id
     AND attempt.lease_epoch = NEW.lease_epoch
    WHERE job.job_id = NEW.job_id;

    SELECT COUNT(*),
           COUNT(*) FILTER (
               WHERE output.output_index < 0
                  OR output.output_index >= expected_outputs
           )
      INTO output_count, missing_output_count
    FROM job_outputs output
    WHERE output.job_id = NEW.job_id;

    SELECT COUNT(*)
      INTO submission_count
    FROM provider_submissions submission
    JOIN job_outputs output
      ON output.output_id = submission.output_id
     AND output.job_id = submission.job_id
    WHERE submission.job_id = NEW.job_id
      AND submission.work_item_id = NEW.work_item_id
      AND submission.created_by_execution_id = NEW.execution_id
      AND submission.created_by_lease_epoch = NEW.lease_epoch
      AND submission.execution_profile_id = NEW.execution_profile_id
      AND submission.state = 'prepared';

    SELECT COUNT(*)
      INTO execution_count
    FROM executor_executions execution
    JOIN provider_submissions submission
      ON submission.executor_execution_id = execution.executor_execution_id
     AND submission.submission_id = execution.submission_id
    WHERE submission.job_id = NEW.job_id
      AND submission.work_item_id = NEW.work_item_id
      AND execution.state = 'prepared';

    SELECT COUNT(*)
      INTO attachment_count
    FROM provider_submission_attachments attachment
    JOIN provider_submissions submission
      ON submission.submission_id = attachment.submission_id
     AND submission.job_id = attachment.job_id
     AND submission.work_item_id = attachment.work_item_id
    WHERE attachment.job_id = NEW.job_id
      AND attachment.work_item_id = NEW.work_item_id
      AND attachment.attempt_execution_id = NEW.execution_id
      AND attachment.lease_epoch = NEW.lease_epoch;

    IF contract_version IS DISTINCT FROM 2
       OR expected_outputs IS NULL OR expected_outputs <= 0
       OR attempt_state IS DISTINCT FROM 'handed_off'
       OR attempt_handed_off_at IS DISTINCT FROM NEW.handed_off_at_ms
       OR output_count <> expected_outputs
       OR missing_output_count <> 0
       OR submission_count <> expected_outputs
       OR execution_count <> expected_outputs
       OR attachment_count <> expected_outputs THEN
        RAISE EXCEPTION 'executor handoff is incomplete or inconsistent';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER work_items_executor_handoff_complete
    AFTER INSERT OR UPDATE ON work_items
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_executor_handoff();
