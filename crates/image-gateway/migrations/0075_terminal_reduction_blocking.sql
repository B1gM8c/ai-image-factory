LOCK TABLE executor_terminal_reductions IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE executor_terminal_reductions
    DROP CONSTRAINT executor_terminal_reductions_state_check,
    DROP CONSTRAINT executor_terminal_reductions_check,
    DROP CONSTRAINT executor_terminal_reduction_completion_shape,
    ADD COLUMN blocked_error_code TEXT CHECK (
        blocked_error_code IN (
            'canonical_conflict',
            'invalid_input',
            'artifact_integrity'
        )
    ),
    ADD COLUMN blocked_by TEXT CHECK (
        blocked_by IS NULL
        OR (
            char_length(blocked_by) BETWEEN 1 AND 255
            AND blocked_by !~ '[[:cntrl:]]'
        )
    ),
    ADD COLUMN blocked_at_ms BIGINT,
    ADD CONSTRAINT executor_terminal_reductions_state_check CHECK (
        state IN ('ready', 'leased', 'completed', 'blocked')
    ),
    ADD CONSTRAINT executor_terminal_reductions_check CHECK (
        (
            state = 'ready'
            AND lease_owner IS NULL
            AND lease_epoch = 0
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
    ),
    ADD CONSTRAINT executor_terminal_reduction_completion_shape CHECK (
        (
            state IN ('ready', 'leased', 'blocked')
            AND completion_owner IS NULL
            AND provider_receipt_id IS NULL
            AND customer_artifact_id IS NULL
            AND quota_reservation_id IS NULL
        )
        OR
        (
            state = 'completed'
            AND completion_owner IS NOT NULL
            AND provider_receipt_id IS NOT NULL
            AND quota_reservation_id IS NOT NULL
            AND (
                (resolved_state = 'succeeded') =
                (customer_artifact_id IS NOT NULL)
            )
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
    IF OLD.state IN ('completed', 'blocked') THEN
        RAISE EXCEPTION 'terminal reduction terminal state is immutable';
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

CREATE INDEX executor_terminal_reductions_blocked_idx
    ON executor_terminal_reductions (blocked_at_ms DESC, submission_id)
    WHERE state = 'blocked';
