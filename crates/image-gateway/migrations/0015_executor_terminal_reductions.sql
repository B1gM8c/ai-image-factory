CREATE TABLE executor_terminal_reductions (
    submission_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    resolution_decision_id UUID NOT NULL UNIQUE,
    resolved_state TEXT NOT NULL CHECK (
        resolved_state IN ('succeeded', 'failed', 'uncertain', 'canceled')
    ),
    state TEXT NOT NULL CHECK (state IN ('ready', 'leased', 'completed')),
    lease_owner TEXT CHECK (
        lease_owner IS NULL
        OR (char_length(lease_owner) BETWEEN 1 AND 255 AND lease_owner !~ '[[:cntrl:]]')
    ),
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT,
    claimed_at_ms BIGINT,
    completed_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES provider_submissions(executor_execution_id, submission_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (
        resolution_decision_id, executor_execution_id, submission_id, resolved_state
    ) REFERENCES executor_resolution_decisions (
        decision_id, executor_execution_id, submission_id, resolved_state
    ) ON DELETE RESTRICT,
    CHECK (
        (state = 'ready'
            AND lease_owner IS NULL AND lease_epoch = 0
            AND lease_expires_at_ms IS NULL AND claimed_at_ms IS NULL
            AND completed_at_ms IS NULL)
        OR
        (state = 'leased'
            AND lease_owner IS NOT NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NOT NULL AND claimed_at_ms IS NOT NULL
            AND completed_at_ms IS NULL)
        OR
        (state = 'completed'
            AND lease_owner IS NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NULL AND claimed_at_ms IS NOT NULL
            AND completed_at_ms IS NOT NULL)
    )
);

CREATE INDEX executor_terminal_reductions_claim_idx
    ON executor_terminal_reductions (created_at_ms, submission_id)
    WHERE state IN ('ready', 'leased');

CREATE FUNCTION enqueue_executor_terminal_reduction() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.state IN ('succeeded', 'failed', 'uncertain', 'canceled')
       AND OLD.state NOT IN ('succeeded', 'failed', 'uncertain', 'canceled')
       AND EXISTS (
           SELECT 1 FROM jobs job
           WHERE job.job_id = NEW.job_id AND job.economics_contract_version = 2
       ) THEN
        INSERT INTO executor_terminal_reductions (
            submission_id, executor_execution_id, resolution_decision_id,
            resolved_state, state, created_at_ms, updated_at_ms
        ) VALUES (
            NEW.submission_id, NEW.executor_execution_id, NEW.resolution_decision_id,
            NEW.state, 'ready', NEW.finished_at_ms, NEW.finished_at_ms
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submission_enqueue_terminal_reduction
    AFTER UPDATE ON provider_submissions
    FOR EACH ROW EXECUTE FUNCTION enqueue_executor_terminal_reduction();

INSERT INTO executor_terminal_reductions (
    submission_id, executor_execution_id, resolution_decision_id,
    resolved_state, state, created_at_ms, updated_at_ms
)
SELECT submission.submission_id, submission.executor_execution_id,
       submission.resolution_decision_id, submission.state,
       'ready', submission.finished_at_ms, submission.finished_at_ms
FROM provider_submissions submission
JOIN jobs job ON job.job_id = submission.job_id
WHERE job.economics_contract_version = 2
  AND submission.state IN ('succeeded', 'failed', 'uncertain', 'canceled');

CREATE FUNCTION validate_executor_terminal_reduction_projection() RETURNS TRIGGER AS $$
DECLARE
    reduction_count BIGINT;
BEGIN
    IF NEW.state NOT IN ('succeeded', 'failed', 'uncertain', 'canceled')
       OR NOT EXISTS (
           SELECT 1 FROM jobs job
           WHERE job.job_id = NEW.job_id AND job.economics_contract_version = 2
       ) THEN
        RETURN NULL;
    END IF;

    SELECT COUNT(*) INTO reduction_count
    FROM executor_terminal_reductions reduction
    WHERE reduction.submission_id = NEW.submission_id
      AND reduction.executor_execution_id = NEW.executor_execution_id
      AND reduction.resolution_decision_id = NEW.resolution_decision_id
      AND reduction.resolved_state = NEW.state;
    IF reduction_count <> 1 THEN
        RAISE EXCEPTION 'V2 terminal provider submission requires one canonical reduction';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_submission_terminal_reduction_check
    AFTER INSERT OR UPDATE ON provider_submissions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_executor_terminal_reduction_projection();

CREATE FUNCTION enforce_executor_terminal_reduction_lease() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.resolution_decision_id IS DISTINCT FROM OLD.resolution_decision_id
       OR NEW.resolved_state IS DISTINCT FROM OLD.resolved_state
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms THEN
        RAISE EXCEPTION 'terminal reduction identity is immutable';
    END IF;
    IF OLD.state = 'completed' THEN
        RAISE EXCEPTION 'completed terminal reduction is immutable';
    END IF;
    IF OLD.state = 'ready' AND NEW.state = 'leased' THEN
        IF NEW.lease_epoch <> 1 OR NEW.lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'terminal reduction claim requires its first future lease';
        END IF;
    ELSIF OLD.state = 'leased' AND NEW.state = 'leased' THEN
        IF NEW.lease_owner IS NOT DISTINCT FROM OLD.lease_owner
           AND NEW.lease_epoch = OLD.lease_epoch THEN
            IF OLD.lease_expires_at_ms <= now_ms
               OR NEW.lease_expires_at_ms < OLD.lease_expires_at_ms
               OR NEW.claimed_at_ms IS DISTINCT FROM OLD.claimed_at_ms THEN
                RAISE EXCEPTION 'terminal reduction heartbeat requires the live lease';
            END IF;
        ELSIF OLD.lease_expires_at_ms > now_ms
              OR NEW.lease_epoch <> OLD.lease_epoch + 1
              OR NEW.lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'terminal reduction reclaim requires an expired lease';
        END IF;
    ELSIF OLD.state = 'leased' AND NEW.state = 'completed' THEN
        IF OLD.lease_expires_at_ms <= now_ms
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.claimed_at_ms IS DISTINCT FROM OLD.claimed_at_ms THEN
            RAISE EXCEPTION 'terminal reduction completion requires the live lease';
        END IF;
    ELSE
        RAISE EXCEPTION 'invalid terminal reduction state transition';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_terminal_reduction_lease_guard
    BEFORE UPDATE ON executor_terminal_reductions
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_terminal_reduction_lease();

CREATE FUNCTION reject_executor_terminal_reduction_delete() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'terminal reductions are durable and cannot be deleted';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_terminal_reductions_reject_delete
    BEFORE DELETE ON executor_terminal_reductions
    FOR EACH ROW EXECUTE FUNCTION reject_executor_terminal_reduction_delete();

CREATE TRIGGER executor_terminal_reductions_reject_truncate
    BEFORE TRUNCATE ON executor_terminal_reductions
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_terminal_reduction_delete();
