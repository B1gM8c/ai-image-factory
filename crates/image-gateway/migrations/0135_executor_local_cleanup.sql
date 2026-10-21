-- Execution-local payloads are disposable only after canonical reduction has committed.
-- This queue contains identifiers and cleanup bookkeeping, never request or media bytes.
CREATE TABLE executor_local_cleanup (
    executor_execution_id UUID PRIMARY KEY,
    submission_id UUID NOT NULL UNIQUE,
    execution_profile_id UUID NOT NULL REFERENCES provider_execution_profiles(execution_profile_id)
        ON DELETE RESTRICT,
    resolution_decision_id UUID NOT NULL UNIQUE,
    resolved_state TEXT NOT NULL CHECK (resolved_state IN ('succeeded', 'failed')),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'cleaning', 'cleaned')),
    eligible_at_ms BIGINT NOT NULL,
    next_attempt_at_ms BIGINT NOT NULL,
    lease_owner TEXT CHECK (lease_owner IS NULL OR (
        char_length(lease_owner) BETWEEN 1 AND 255 AND lease_owner !~ '[[:cntrl:]]'
    )),
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error_code TEXT CHECK (
        last_error_code IS NULL OR last_error_code ~ '^[A-Za-z0-9_.-]{1,128}$'
    ),
    deleted_bytes BIGINT NOT NULL DEFAULT 0 CHECK (deleted_bytes >= 0),
    completed_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES executor_executions(executor_execution_id, submission_id) ON DELETE RESTRICT,
    FOREIGN KEY (resolution_decision_id, executor_execution_id, submission_id, resolved_state)
        REFERENCES executor_resolution_decisions(
            decision_id, executor_execution_id, submission_id, resolved_state
        ) ON DELETE RESTRICT,
    CHECK (next_attempt_at_ms >= eligible_at_ms),
    CHECK (
        (state = 'pending' AND lease_owner IS NULL AND lease_expires_at_ms IS NULL
            AND completed_at_ms IS NULL)
        OR (state = 'cleaning' AND lease_owner IS NOT NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NOT NULL AND completed_at_ms IS NULL)
        OR (state = 'cleaned' AND lease_owner IS NULL AND lease_expires_at_ms IS NULL
            AND completed_at_ms IS NOT NULL AND last_error_code IS NULL)
    )
);

-- These indexes are created on the new empty queue, not on active application tables.
CREATE INDEX executor_local_cleanup_pending_idx
    ON executor_local_cleanup (next_attempt_at_ms, executor_execution_id) WHERE state = 'pending';
CREATE INDEX executor_local_cleanup_expired_idx
    ON executor_local_cleanup (lease_expires_at_ms, executor_execution_id) WHERE state = 'cleaning';

CREATE FUNCTION enqueue_executor_local_cleanup() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.state = 'completed' AND NEW.resolved_state IN ('succeeded', 'failed') THEN
        INSERT INTO executor_local_cleanup (
            executor_execution_id, submission_id, execution_profile_id,
            resolution_decision_id, resolved_state, eligible_at_ms, next_attempt_at_ms,
            created_at_ms, updated_at_ms
        )
        SELECT execution.executor_execution_id, submission.submission_id,
               submission.execution_profile_id, NEW.resolution_decision_id,
               NEW.resolved_state, now_ms, now_ms, now_ms, now_ms
        FROM executor_executions execution
        JOIN provider_submissions submission
          ON submission.executor_execution_id = execution.executor_execution_id
         AND submission.submission_id = execution.submission_id
        JOIN executor_resolution_decisions decision
          ON decision.decision_id = NEW.resolution_decision_id
         AND decision.executor_execution_id = execution.executor_execution_id
         AND decision.submission_id = submission.submission_id
         AND decision.source = 'active_runner_observation'
        WHERE execution.executor_execution_id = NEW.executor_execution_id
          AND submission.submission_id = NEW.submission_id
          AND execution.resolution_decision_id = NEW.resolution_decision_id
          AND submission.resolution_decision_id = NEW.resolution_decision_id
          AND submission.execution_profile_id IS NOT NULL
          AND execution.state = NEW.resolved_state AND submission.state = NEW.resolved_state
        ON CONFLICT (executor_execution_id) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_terminal_reduction_enqueue_local_cleanup
    AFTER INSERT OR UPDATE OF state ON executor_terminal_reductions
    FOR EACH ROW EXECUTE FUNCTION enqueue_executor_local_cleanup();

INSERT INTO executor_local_cleanup (
    executor_execution_id, submission_id, execution_profile_id, resolution_decision_id,
    resolved_state, eligible_at_ms, next_attempt_at_ms, created_at_ms, updated_at_ms
)
SELECT execution.executor_execution_id, submission.submission_id, submission.execution_profile_id,
       reduction.resolution_decision_id, reduction.resolved_state,
       clock.now_ms, clock.now_ms, clock.now_ms, clock.now_ms
FROM executor_terminal_reductions reduction
JOIN executor_executions execution
  ON execution.executor_execution_id = reduction.executor_execution_id
 AND execution.submission_id = reduction.submission_id
 AND execution.resolution_decision_id = reduction.resolution_decision_id
 AND execution.state = reduction.resolved_state
JOIN provider_submissions submission
  ON submission.executor_execution_id = execution.executor_execution_id
 AND submission.submission_id = execution.submission_id
 AND submission.resolution_decision_id = reduction.resolution_decision_id
 AND submission.state = reduction.resolved_state
JOIN executor_resolution_decisions decision
  ON decision.decision_id = reduction.resolution_decision_id
 AND decision.executor_execution_id = execution.executor_execution_id
 AND decision.submission_id = submission.submission_id
 AND decision.source = 'active_runner_observation'
CROSS JOIN (SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS now_ms) clock
WHERE reduction.state = 'completed' AND reduction.resolved_state IN ('succeeded', 'failed')
  AND submission.execution_profile_id IS NOT NULL;

CREATE FUNCTION protect_executor_local_cleanup() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'executor local cleanup records are durable';
    END IF;
    IF OLD.state = 'cleaned' THEN
        RAISE EXCEPTION 'completed executor local cleanup is immutable';
    END IF;
    IF NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.execution_profile_id IS DISTINCT FROM OLD.execution_profile_id
       OR NEW.resolution_decision_id IS DISTINCT FROM OLD.resolution_decision_id
       OR NEW.resolved_state IS DISTINCT FROM OLD.resolved_state
       OR NEW.eligible_at_ms IS DISTINCT FROM OLD.eligible_at_ms
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms THEN
        RAISE EXCEPTION 'executor local cleanup identity is immutable';
    END IF;
    IF NEW.state = 'cleaning' THEN
        IF (OLD.state = 'pending' AND OLD.next_attempt_at_ms > now_ms)
           OR (OLD.state = 'cleaning' AND OLD.lease_expires_at_ms > now_ms)
           OR NEW.lease_epoch <> OLD.lease_epoch + 1
           OR NEW.attempts <> OLD.attempts + 1
           OR NEW.lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'executor local cleanup claim requires a due or expired lease';
        END IF;
    ELSIF OLD.state = 'cleaning' AND NEW.state IN ('pending', 'cleaned') THEN
        IF OLD.lease_expires_at_ms <= now_ms
           OR NEW.lease_epoch <> OLD.lease_epoch OR NEW.attempts <> OLD.attempts THEN
            RAISE EXCEPTION 'executor local cleanup completion requires its live lease';
        END IF;
    ELSE
        RAISE EXCEPTION 'invalid executor local cleanup transition';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_local_cleanup_guard
    BEFORE UPDATE OR DELETE ON executor_local_cleanup
    FOR EACH ROW EXECUTE FUNCTION protect_executor_local_cleanup();
CREATE TRIGGER executor_local_cleanup_reject_truncate
    BEFORE TRUNCATE ON executor_local_cleanup
    FOR EACH STATEMENT EXECUTE FUNCTION protect_executor_local_cleanup();
