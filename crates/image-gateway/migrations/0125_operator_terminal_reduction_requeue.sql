CREATE TABLE operator_terminal_reduction_requeues (
    submission_id UUID PRIMARY KEY
        REFERENCES executor_terminal_reductions(submission_id) ON DELETE RESTRICT,
    executor_execution_id UUID NOT NULL UNIQUE,
    prior_lease_epoch BIGINT NOT NULL CHECK (prior_lease_epoch > 0),
    prior_claimed_at_ms BIGINT NOT NULL,
    prior_blocked_error_code TEXT NOT NULL CHECK (
        prior_blocked_error_code = 'canonical_conflict'
    ),
    prior_blocked_by TEXT NOT NULL,
    prior_blocked_at_ms BIGINT NOT NULL,
    repair_revision TEXT NOT NULL CHECK (
        repair_revision ~ '^[a-z0-9][a-z0-9._-]{0,127}$'
    ),
    requeued_by TEXT NOT NULL CHECK (
        char_length(requeued_by) BETWEEN 1 AND 255
        AND requeued_by !~ '[[:cntrl:]]'
    ),
    requeued_at_ms BIGINT NOT NULL,
    UNIQUE (submission_id, executor_execution_id)
);

CREATE FUNCTION reject_operator_terminal_reduction_requeue_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'operator terminal reduction requeue evidence is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER operator_terminal_reduction_requeues_reject_mutation
    BEFORE UPDATE OR DELETE ON operator_terminal_reduction_requeues
    FOR EACH ROW EXECUTE FUNCTION reject_operator_terminal_reduction_requeue_mutation();

CREATE TRIGGER operator_terminal_reduction_requeues_reject_truncate
    BEFORE TRUNCATE ON operator_terminal_reduction_requeues
    FOR EACH STATEMENT EXECUTE FUNCTION reject_operator_terminal_reduction_requeue_mutation();

ALTER TABLE executor_terminal_reductions
    DROP CONSTRAINT executor_terminal_reductions_check,
    ADD CONSTRAINT executor_terminal_reductions_check CHECK (
        (
            state = 'ready'
            AND lease_owner IS NULL
            AND lease_epoch >= 0
            AND lease_expires_at_ms IS NULL
            AND claimed_at_ms IS NULL
            AND completed_at_ms IS NULL
            AND blocked_error_code IS NULL
            AND blocked_by IS NULL
            AND blocked_at_ms IS NULL
        )
        OR
        (
            state = 'leased'
            AND lease_owner IS NOT NULL
            AND lease_epoch > 0
            AND lease_expires_at_ms IS NOT NULL
            AND claimed_at_ms IS NOT NULL
            AND completed_at_ms IS NULL
            AND blocked_error_code IS NULL
            AND blocked_by IS NULL
            AND blocked_at_ms IS NULL
        )
        OR
        (
            state = 'completed'
            AND lease_owner IS NULL
            AND lease_epoch > 0
            AND lease_expires_at_ms IS NULL
            AND claimed_at_ms IS NOT NULL
            AND completed_at_ms IS NOT NULL
            AND blocked_error_code IS NULL
            AND blocked_by IS NULL
            AND blocked_at_ms IS NULL
        )
        OR
        (
            state = 'blocked'
            AND lease_owner IS NULL
            AND lease_epoch > 0
            AND lease_expires_at_ms IS NULL
            AND claimed_at_ms IS NOT NULL
            AND completed_at_ms IS NULL
            AND blocked_error_code IS NOT NULL
            AND blocked_by IS NOT NULL
            AND blocked_at_ms IS NOT NULL
        )
    );

CREATE OR REPLACE FUNCTION enforce_executor_terminal_reduction_lease() RETURNS TRIGGER AS $$
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
        RAISE EXCEPTION 'terminal reduction terminal state is immutable';
    END IF;
    IF OLD.state = 'blocked' AND NEW.state = 'ready' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM executor_terminal_reduction_recoveries recovery
            WHERE recovery.submission_id = OLD.submission_id
              AND recovery.executor_execution_id = OLD.executor_execution_id
              AND recovery.prior_lease_epoch = OLD.lease_epoch
              AND recovery.prior_claimed_at_ms = OLD.claimed_at_ms
              AND recovery.prior_blocked_error_code = OLD.blocked_error_code
              AND recovery.prior_blocked_by = OLD.blocked_by
              AND recovery.prior_blocked_at_ms = OLD.blocked_at_ms
              AND recovery.recovered_at_ms = NEW.updated_at_ms
        ) AND NOT EXISTS (
            SELECT 1
            FROM operator_terminal_reduction_requeues requeue
            WHERE requeue.submission_id = OLD.submission_id
              AND requeue.executor_execution_id = OLD.executor_execution_id
              AND requeue.prior_lease_epoch = OLD.lease_epoch
              AND requeue.prior_claimed_at_ms = OLD.claimed_at_ms
              AND requeue.prior_blocked_error_code = OLD.blocked_error_code
              AND requeue.prior_blocked_by = OLD.blocked_by
              AND requeue.prior_blocked_at_ms = OLD.blocked_at_ms
              AND requeue.requeued_at_ms = NEW.updated_at_ms
        ) THEN
            RAISE EXCEPTION 'blocked terminal reduction recovery requires immutable evidence';
        END IF;
    ELSIF OLD.state = 'blocked' THEN
        RAISE EXCEPTION 'terminal reduction terminal state is immutable';
    ELSIF OLD.state = 'ready' AND NEW.state = 'leased' THEN
        IF NEW.lease_epoch <> OLD.lease_epoch + 1
           OR NEW.lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'terminal reduction claim requires its next future lease';
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
    ELSIF OLD.state = 'leased' AND NEW.state IN ('completed', 'blocked') THEN
        IF OLD.lease_expires_at_ms <= now_ms
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.claimed_at_ms IS DISTINCT FROM OLD.claimed_at_ms THEN
            RAISE EXCEPTION 'terminal reduction finalization requires the live lease';
        END IF;
    ELSE
        RAISE EXCEPTION 'invalid terminal reduction state transition';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
