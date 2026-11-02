-- Accepted remote work has no task-lifetime timeout. This does not renew submit
-- authority: every query/download still needs a short, fenced runtime attempt.
-- Deploy only with all old submit/poll/terminal writers drained and stopped.
CREATE TABLE accepted_tracking_authorities (
    submission_id UUID PRIMARY KEY REFERENCES provider_remote_tasks(submission_id) ON DELETE RESTRICT,
    executor_execution_id UUID NOT NULL UNIQUE,
    provider_account_id UUID NOT NULL,
    remote_operation_id TEXT NOT NULL,
    original_submit_deadline_at_ms BIGINT NOT NULL,
    accepted_at_ms BIGINT NOT NULL CHECK (accepted_at_ms > 0),
    attention_after_at_ms BIGINT NOT NULL,
    grant_source TEXT NOT NULL CHECK (grant_source IN ('accepted_attach', 'released_poll', 'migration_0167')),
    granted_at_ms BIGINT NOT NULL CHECK (granted_at_ms >= accepted_at_ms),
    CHECK (attention_after_at_ms = accepted_at_ms + 86400000),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES provider_submissions(executor_execution_id, submission_id)
);

CREATE FUNCTION dreamina_accepted_tracking_eligible(target UUID)
RETURNS BOOLEAN LANGUAGE SQL VOLATILE AS $$
    SELECT dreamina_accepted_completion_budget_eligible(target) AND EXISTS (
        SELECT 1 FROM provider_remote_tasks task
        JOIN provider_submissions submission USING (submission_id, executor_execution_id)
        JOIN provider_remote_submit_intents intent USING (submission_id, executor_execution_id)
        JOIN provider_submit_recoveries recovery USING (submission_id, executor_execution_id)
        WHERE task.submission_id = target
          AND task.provider_id = submission.provider_id
          AND task.provider_account_id = submission.provider_account_id
          AND intent.provider_id = task.provider_id AND intent.provider_account_id = task.provider_account_id
          AND recovery.provider_id = task.provider_id AND recovery.provider_account_id = task.provider_account_id
          AND submission.operation_binding_version = 2 AND submission.completion_mode = 'remote_task'
          AND submission.command_schema = 'dreamina-cli.submit.v1'
          AND task.remote_operation_id = intent.remote_operation_id
          AND task.created_at_ms <= 9223372036768375807
          AND NOT EXISTS (SELECT 1 FROM executor_capacity_risk_waivers WHERE submission_id = target)
          AND NOT EXISTS (SELECT 1 FROM provider_remote_task_quarantines WHERE submission_id = target)
          AND NOT EXISTS (SELECT 1 FROM dreamina_image_result_sets WHERE submission_id = target AND cleanup_state <> 'available')
          AND NOT EXISTS (SELECT 1 FROM dreamina_image_stage_intents stage WHERE submission_id = target
                          AND (cleanup_state = 'deleting' OR (cleanup_state = 'deleted' AND NOT EXISTS (
                              SELECT 1 FROM dreamina_image_result_sets result_set
                              WHERE result_set.submission_id = target AND result_set.cleanup_state = 'available'))))
          AND NOT EXISTS (SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE job_id = submission.job_id)
          AND NOT EXISTS (SELECT 1 FROM dreamina_operator_image_supplement_authorizations WHERE job_id = submission.job_id)
    );
$$;

CREATE FUNCTION validate_accepted_tracking_authority() RETURNS TRIGGER AS $$
DECLARE
    task provider_remote_tasks%ROWTYPE;
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    SELECT * INTO STRICT task FROM provider_remote_tasks WHERE submission_id = NEW.submission_id FOR UPDATE;
    IF NOT dreamina_accepted_tracking_eligible(NEW.submission_id)
       OR NEW.executor_execution_id <> task.executor_execution_id
       OR NEW.provider_account_id <> task.provider_account_id
       OR NEW.remote_operation_id <> task.remote_operation_id
       OR NEW.original_submit_deadline_at_ms <> task.provider_deadline_at_ms
       OR NEW.accepted_at_ms <> task.created_at_ms
       OR NEW.granted_at_ms NOT BETWEEN now_ms - 1000 AND now_ms THEN
        RAISE EXCEPTION 'tracking requires one untouched accepted Dreamina operation without a poll lease';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER accepted_tracking_authority_insert BEFORE INSERT ON accepted_tracking_authorities
    FOR EACH ROW EXECUTE FUNCTION validate_accepted_tracking_authority();
CREATE TRIGGER accepted_tracking_authority_immutable BEFORE UPDATE OR DELETE ON accepted_tracking_authorities
    FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation();
CREATE TRIGGER accepted_tracking_authority_no_truncate BEFORE TRUNCATE ON accepted_tracking_authorities
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();

CREATE OR REPLACE FUNCTION provider_task_completion_deadline(target_submission UUID, original_deadline BIGINT)
RETURNS BIGINT LANGUAGE SQL STABLE AS $$
    -- Internal SQL sentinel only. Never pass this to a CLI or a Tokio timeout.
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM accepted_tracking_authorities
        WHERE submission_id = target_submission AND original_submit_deadline_at_ms = original_deadline
    ) THEN 9223372036854775807::BIGINT ELSE COALESCE((
        SELECT completion_deadline_at_ms FROM accepted_completion_budgets
        WHERE submission_id = target_submission AND original_submit_deadline_at_ms = original_deadline
    ), original_deadline) END;
$$;

CREATE FUNCTION grant_dreamina_accepted_tracking() RETURNS TRIGGER AS $$
DECLARE task provider_remote_tasks%ROWTYPE;
BEGIN
    IF NEW.state <> 'provider_waiting' OR NEW.provider_id <> 'dreamina-cli'
       OR EXISTS (SELECT 1 FROM accepted_tracking_authorities WHERE submission_id = NEW.submission_id) THEN
        RETURN NULL;
    END IF;
    SELECT * INTO task FROM provider_remote_tasks WHERE submission_id = NEW.submission_id FOR UPDATE;
    IF FOUND AND dreamina_accepted_tracking_eligible(task.submission_id) THEN
        INSERT INTO accepted_tracking_authorities
            (submission_id, executor_execution_id, provider_account_id, remote_operation_id,
             original_submit_deadline_at_ms, accepted_at_ms, attention_after_at_ms, grant_source, granted_at_ms)
        VALUES (task.submission_id, task.executor_execution_id, task.provider_account_id, task.remote_operation_id,
                task.provider_deadline_at_ms, task.created_at_ms, task.created_at_ms + 86400000,
                CASE WHEN TG_TABLE_NAME = 'provider_submissions' THEN 'accepted_attach' ELSE 'released_poll' END,
                floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_accepted_tracking AFTER UPDATE OF state ON provider_submissions
    FOR EACH ROW EXECUTE FUNCTION grant_dreamina_accepted_tracking();
CREATE TRIGGER dreamina_released_poll_tracking AFTER UPDATE ON provider_remote_tasks
    FOR EACH ROW WHEN (NEW.state = 'provider_waiting' AND NEW.poll_owner IS NULL)
    EXECUTE FUNCTION grant_dreamina_accepted_tracking();

DO $migration$
DECLARE task provider_remote_tasks%ROWTYPE;
BEGIN
    -- Refuse a partial upgrade rather than changing a seal held by an old poller.
    LOCK TABLE provider_remote_tasks IN SHARE ROW EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM provider_remote_tasks
               WHERE provider_id = 'dreamina-cli' AND state = 'provider_waiting' AND poll_owner IS NOT NULL) THEN
        RAISE EXCEPTION 'accepted tracking migration requires drained Dreamina poll leases';
    END IF;
    FOR task IN SELECT * FROM provider_remote_tasks
                WHERE dreamina_accepted_tracking_eligible(submission_id) FOR UPDATE
    LOOP
        INSERT INTO accepted_tracking_authorities
            (submission_id, executor_execution_id, provider_account_id, remote_operation_id,
             original_submit_deadline_at_ms, accepted_at_ms, attention_after_at_ms, grant_source, granted_at_ms)
        VALUES (task.submission_id, task.executor_execution_id, task.provider_account_id, task.remote_operation_id,
                task.provider_deadline_at_ms, task.created_at_ms, task.created_at_ms + 86400000,
                'migration_0167', floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT);
    END LOOP;
END;
$migration$;

-- Read-only diagnostic: 24 hours is an attention threshold, not terminal evidence.
CREATE VIEW accepted_tracking_delays AS
SELECT authority.submission_id, authority.executor_execution_id, authority.provider_account_id,
       authority.accepted_at_ms, authority.attention_after_at_ms,
       task.updated_at_ms AS last_task_update_at_ms, task.next_poll_at_ms, task.poll_lease_expires_at_ms
FROM accepted_tracking_authorities authority
JOIN provider_remote_tasks task USING (submission_id, executor_execution_id)
WHERE task.state = 'provider_waiting'
  AND authority.attention_after_at_ms <= floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
