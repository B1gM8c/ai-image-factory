LOCK TABLE provider_remote_submit_intents, provider_remote_tasks,
    provider_submissions, executor_executions, executor_capacity_allocations
    IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM provider_remote_submit_intents WHERE state <> 'reserved'
    ) OR EXISTS (
        SELECT 1 FROM provider_remote_tasks
    ) THEN
        RAISE EXCEPTION
            'provider recovery migration requires remote submit activity to be drained';
    END IF;
END;
$$;

CREATE TABLE provider_submit_recoveries (
    submission_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_account_id UUID NOT NULL,
    invocation_attempt INTEGER NOT NULL CHECK (invocation_attempt > 0),
    provider_timeout_ms BIGINT NOT NULL CHECK (
        provider_timeout_ms BETWEEN 1 AND 2592000000
    ),
    provider_deadline_at_ms BIGINT NOT NULL,
    next_recovery_at_ms BIGINT,
    state TEXT NOT NULL CHECK (state IN ('active', 'closed')),
    recovery_owner TEXT CHECK (
        recovery_owner IS NULL
        OR (char_length(recovery_owner) BETWEEN 1 AND 255
            AND recovery_owner !~ '[[:cntrl:]]')
    ),
    recovery_lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (recovery_lease_epoch >= 0),
    recovery_lease_expires_at_ms BIGINT,
    recovery_claimed_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    closed_at_ms BIGINT,
    FOREIGN KEY (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) REFERENCES provider_remote_submit_intents (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) ON DELETE RESTRICT,
    UNIQUE (submission_id, executor_execution_id, provider_id, provider_account_id),
    CHECK (
        provider_deadline_at_ms = created_at_ms + provider_timeout_ms
        AND updated_at_ms >= created_at_ms
    ),
    CHECK (
        (state = 'active'
            AND next_recovery_at_ms BETWEEN created_at_ms AND provider_deadline_at_ms
            AND closed_at_ms IS NULL)
        OR
        (state = 'closed'
            AND next_recovery_at_ms IS NULL
            AND closed_at_ms IS NOT NULL AND closed_at_ms >= created_at_ms)
    ),
    CHECK (
        (recovery_owner IS NULL
            AND recovery_lease_expires_at_ms IS NULL
            AND recovery_claimed_at_ms IS NULL)
        OR
        (state = 'active'
            AND recovery_owner IS NOT NULL
            AND recovery_lease_epoch > 0
            AND recovery_claimed_at_ms IS NOT NULL
            AND recovery_lease_expires_at_ms > recovery_claimed_at_ms)
    )
);

CREATE INDEX provider_submit_recoveries_claim_idx
    ON provider_submit_recoveries (
        provider_id,
        provider_account_id,
        GREATEST(
            next_recovery_at_ms,
            COALESCE(recovery_lease_expires_at_ms, next_recovery_at_ms)
        ),
        provider_deadline_at_ms,
        submission_id
    )
    WHERE state = 'active';

DROP INDEX provider_remote_tasks_poll_claim_idx;

CREATE INDEX provider_remote_tasks_poll_claim_idx
    ON provider_remote_tasks (
        provider_id,
        provider_account_id,
        GREATEST(
            next_poll_at_ms,
            COALESCE(poll_lease_expires_at_ms, next_poll_at_ms)
        ),
        submission_id
    )
    WHERE state = 'provider_waiting';

ALTER TABLE provider_remote_tasks
    ADD COLUMN attach_recovery_owner TEXT CHECK (
        attach_recovery_owner IS NULL
        OR (char_length(attach_recovery_owner) BETWEEN 1 AND 255
            AND attach_recovery_owner !~ '[[:cntrl:]]')
    ),
    ADD COLUMN attach_recovery_lease_epoch BIGINT CHECK (
        attach_recovery_lease_epoch IS NULL OR attach_recovery_lease_epoch > 0
    ),
    ADD CONSTRAINT provider_remote_task_attach_recovery_fence_check CHECK (
        (attach_recovery_owner IS NULL) = (attach_recovery_lease_epoch IS NULL)
    );

CREATE FUNCTION validate_provider_submit_recovery_insert() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.state <> 'active'
       OR NEW.invocation_attempt <> 1
       OR NEW.recovery_owner IS NOT NULL
       OR NEW.recovery_lease_epoch <> 0
       OR NEW.recovery_lease_expires_at_ms IS NOT NULL
       OR NEW.recovery_claimed_at_ms IS NOT NULL
       OR NEW.closed_at_ms IS NOT NULL
       OR NEW.created_at_ms IS DISTINCT FROM NEW.updated_at_ms
       OR NOT EXISTS (
            SELECT 1
            FROM provider_remote_submit_intents intent
            JOIN provider_submissions submission
              ON submission.submission_id = intent.submission_id
             AND submission.executor_execution_id = intent.executor_execution_id
            JOIN executor_executions execution
              ON execution.executor_execution_id = intent.executor_execution_id
             AND execution.submission_id = intent.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = intent.executor_execution_id
             AND allocation.submission_id = intent.submission_id
            WHERE intent.submission_id = NEW.submission_id
              AND intent.executor_execution_id = NEW.executor_execution_id
              AND intent.provider_id = NEW.provider_id
              AND intent.provider_account_id = NEW.provider_account_id
              AND intent.state = 'sending'
              AND intent.send_started_at_ms = NEW.created_at_ms
              AND execution.state = 'running'
              AND submission.state = 'running'
              AND allocation.state = 'held'
              AND NEW.created_at_ms <= now_ms
              AND NEW.provider_deadline_at_ms > now_ms
              AND NEW.next_recovery_at_ms = LEAST(
                    execution.lease_expires_at_ms,
                    NEW.provider_deadline_at_ms
                  )
       ) THEN
        RAISE EXCEPTION
            'provider submit recovery requires its frozen live invocation context';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submit_recovery_insert_guard
    BEFORE INSERT ON provider_submit_recoveries
    FOR EACH ROW EXECUTE FUNCTION validate_provider_submit_recovery_insert();

CREATE FUNCTION enforce_provider_submit_recovery_update() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.provider_account_id IS DISTINCT FROM OLD.provider_account_id
       OR NEW.invocation_attempt IS DISTINCT FROM OLD.invocation_attempt
       OR NEW.provider_timeout_ms IS DISTINCT FROM OLD.provider_timeout_ms
       OR NEW.provider_deadline_at_ms IS DISTINCT FROM OLD.provider_deadline_at_ms
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms
       OR NEW.updated_at_ms < OLD.updated_at_ms THEN
        RAISE EXCEPTION 'provider submit recovery identity is immutable';
    END IF;
    IF OLD.state = 'closed' THEN
        RAISE EXCEPTION 'closed provider submit recovery is immutable';
    END IF;

    IF NEW.state = 'closed' THEN
        IF NEW.next_recovery_at_ms IS NOT NULL
           OR NEW.recovery_owner IS NOT NULL
           OR NEW.recovery_lease_expires_at_ms IS NOT NULL
           OR NEW.recovery_claimed_at_ms IS NOT NULL
           OR NEW.recovery_lease_epoch IS DISTINCT FROM OLD.recovery_lease_epoch
           OR NEW.closed_at_ms IS DISTINCT FROM NEW.updated_at_ms THEN
            RAISE EXCEPTION 'invalid provider submit recovery close';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.recovery_owner IS NULL AND NEW.recovery_owner IS NOT NULL THEN
        IF NEW.recovery_lease_epoch <> OLD.recovery_lease_epoch + 1
           OR NEW.recovery_claimed_at_ms IS DISTINCT FROM NEW.updated_at_ms
           OR NEW.recovery_lease_expires_at_ms <= NEW.updated_at_ms
           OR NEW.next_recovery_at_ms IS DISTINCT FROM OLD.next_recovery_at_ms
           OR NEW.closed_at_ms IS NOT NULL THEN
            RAISE EXCEPTION 'invalid provider submit recovery acquisition';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.recovery_owner IS NOT NULL AND NEW.recovery_owner IS NOT NULL THEN
        IF NEW.recovery_lease_epoch = OLD.recovery_lease_epoch + 1 THEN
            IF OLD.recovery_lease_expires_at_ms > NEW.updated_at_ms
               OR NEW.recovery_claimed_at_ms IS DISTINCT FROM NEW.updated_at_ms
               OR NEW.recovery_lease_expires_at_ms <= NEW.updated_at_ms
               OR NEW.next_recovery_at_ms IS DISTINCT FROM OLD.next_recovery_at_ms
               OR NEW.closed_at_ms IS NOT NULL THEN
                RAISE EXCEPTION 'provider submit recovery reclaim requires expiry';
            END IF;
            RETURN NEW;
        END IF;
        IF NEW.recovery_owner IS DISTINCT FROM OLD.recovery_owner
           OR NEW.recovery_lease_epoch IS DISTINCT FROM OLD.recovery_lease_epoch
           OR NEW.recovery_claimed_at_ms IS DISTINCT FROM OLD.recovery_claimed_at_ms
           OR NEW.recovery_lease_expires_at_ms < OLD.recovery_lease_expires_at_ms
           OR NEW.recovery_lease_expires_at_ms <= NEW.updated_at_ms
           OR NEW.next_recovery_at_ms IS DISTINCT FROM OLD.next_recovery_at_ms
           OR NEW.closed_at_ms IS NOT NULL THEN
            RAISE EXCEPTION 'invalid provider submit recovery heartbeat';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.recovery_owner IS NOT NULL AND NEW.recovery_owner IS NULL THEN
        IF NEW.recovery_lease_epoch IS DISTINCT FROM OLD.recovery_lease_epoch
           OR NEW.recovery_lease_expires_at_ms IS NOT NULL
           OR NEW.recovery_claimed_at_ms IS NOT NULL
           OR NEW.next_recovery_at_ms < OLD.next_recovery_at_ms
           OR NEW.closed_at_ms IS NOT NULL THEN
            RAISE EXCEPTION 'invalid provider submit recovery deferral';
        END IF;
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'unsupported provider submit recovery mutation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submit_recovery_update_guard
    BEFORE UPDATE ON provider_submit_recoveries
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submit_recovery_update();

CREATE FUNCTION enforce_provider_submit_recovery_projection() RETURNS TRIGGER AS $$
DECLARE
    intent_state TEXT;
    recovery_state TEXT;
BEGIN
    SELECT intent.state, recovery.state
    INTO intent_state, recovery_state
    FROM provider_remote_submit_intents intent
    LEFT JOIN provider_submit_recoveries recovery
      ON recovery.submission_id = intent.submission_id
    WHERE intent.submission_id = NEW.submission_id;

    IF intent_state = 'reserved' AND recovery_state IS NOT NULL THEN
        RAISE EXCEPTION 'reserved provider submit cannot own recovery state';
    ELSIF intent_state IN ('sending', 'outcome_unknown', 'operation_known')
          AND recovery_state IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION 'active provider submit requires an active recovery lease';
    ELSIF intent_state IN ('attached', 'rejected')
          AND recovery_state IS DISTINCT FROM 'closed' THEN
        RAISE EXCEPTION 'terminal provider submit requires a closed recovery lease';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_submit_intent_recovery_projection_check
    AFTER UPDATE ON provider_remote_submit_intents
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submit_recovery_projection();

CREATE CONSTRAINT TRIGGER provider_submit_recovery_projection_check
    AFTER INSERT OR UPDATE ON provider_submit_recoveries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submit_recovery_projection();

CREATE OR REPLACE FUNCTION validate_provider_remote_task_insert() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
    execution_state TEXT;
    execution_owner TEXT;
    execution_epoch BIGINT;
    execution_expires_at_ms BIGINT;
    execution_launch_owner TEXT;
    execution_launch_epoch BIGINT;
    submission_state TEXT;
    submission_provider TEXT;
    submission_account UUID;
    capacity_state TEXT;
    recovery_state TEXT;
    recovery_owner TEXT;
    recovery_epoch BIGINT;
    recovery_expires_at_ms BIGINT;
    provider_deadline_at_ms BIGINT;
BEGIN
    SELECT execution.state, execution.executor_owner, execution.lease_epoch,
           execution.lease_expires_at_ms, execution.launch_owner,
           execution.launch_lease_epoch, submission.state,
           submission.provider_id, submission.provider_account_id, allocation.state,
           recovery.state, recovery.recovery_owner, recovery.recovery_lease_epoch,
           recovery.recovery_lease_expires_at_ms, recovery.provider_deadline_at_ms
    INTO execution_state, execution_owner, execution_epoch,
         execution_expires_at_ms, execution_launch_owner, execution_launch_epoch,
         submission_state, submission_provider, submission_account, capacity_state,
         recovery_state, recovery_owner, recovery_epoch, recovery_expires_at_ms,
         provider_deadline_at_ms
    FROM executor_executions execution
    JOIN provider_submissions submission
      ON submission.executor_execution_id = execution.executor_execution_id
     AND submission.submission_id = execution.submission_id
    JOIN executor_capacity_allocations allocation
      ON allocation.executor_execution_id = execution.executor_execution_id
     AND allocation.submission_id = execution.submission_id
    JOIN provider_submit_recoveries recovery
      ON recovery.executor_execution_id = execution.executor_execution_id
     AND recovery.submission_id = execution.submission_id
    WHERE execution.executor_execution_id = NEW.executor_execution_id
      AND execution.submission_id = NEW.submission_id
    FOR UPDATE OF execution, submission, allocation, recovery;

    IF NOT FOUND OR execution_state <> 'running' OR submission_state <> 'running'
       OR execution_owner IS DISTINCT FROM NEW.submit_owner
       OR execution_epoch IS DISTINCT FROM NEW.submit_lease_epoch
       OR execution_launch_owner IS DISTINCT FROM NEW.submit_owner
       OR execution_launch_epoch IS DISTINCT FROM NEW.submit_lease_epoch
       OR submission_provider IS DISTINCT FROM NEW.provider_id
       OR submission_account IS DISTINCT FROM NEW.provider_account_id
       OR capacity_state IS DISTINCT FROM 'held'
       OR recovery_state IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION
            'remote task attach requires frozen execution identity and held capacity';
    END IF;

    IF NEW.attach_recovery_owner IS NULL THEN
        IF execution_expires_at_ms <= now_ms
           OR provider_deadline_at_ms <= now_ms
           OR recovery_owner IS NOT NULL THEN
            RAISE EXCEPTION 'remote task attach requires a live submit fence';
        END IF;
    ELSIF recovery_owner IS DISTINCT FROM NEW.attach_recovery_owner
          OR recovery_epoch IS DISTINCT FROM NEW.attach_recovery_lease_epoch
          OR recovery_expires_at_ms <= now_ms THEN
        RAISE EXCEPTION 'remote task attach requires the live recovery fence';
    END IF;

    IF NEW.state <> 'provider_waiting'
       OR NEW.poll_owner IS NOT NULL OR NEW.poll_lease_epoch <> 0
       OR NEW.poll_lease_expires_at_ms IS NOT NULL OR NEW.poll_claimed_at_ms IS NOT NULL
       OR NEW.cancel_requested OR NEW.cancel_requested_at_ms IS NOT NULL
       OR NEW.last_wakeup_observation_id IS NOT NULL
       OR NOT EXISTS (
            SELECT 1
            FROM provider_remote_submit_intents intent
            WHERE intent.submission_id = NEW.submission_id
              AND intent.executor_execution_id = NEW.executor_execution_id
              AND intent.provider_id = NEW.provider_id
              AND intent.provider_account_id = NEW.provider_account_id
              AND intent.submit_owner = NEW.submit_owner
              AND intent.submit_lease_epoch = NEW.submit_lease_epoch
              AND intent.state = 'attached'
              AND intent.remote_operation_id = NEW.remote_operation_id
              AND intent.provider_request_id IS NOT DISTINCT FROM NEW.provider_request_id
       ) THEN
        RAISE EXCEPTION 'remote task must be inserted from its durable submit receipt';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION enforce_provider_remote_task_attach_fence_update() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.attach_recovery_owner IS DISTINCT FROM OLD.attach_recovery_owner
       OR NEW.attach_recovery_lease_epoch IS DISTINCT FROM OLD.attach_recovery_lease_epoch THEN
        RAISE EXCEPTION 'remote task attach authority is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_remote_task_attach_fence_update_guard
    BEFORE UPDATE ON provider_remote_tasks
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_remote_task_attach_fence_update();

CREATE FUNCTION reject_provider_submit_recovery_delete() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider submit recovery history is durable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submit_recoveries_reject_delete
    BEFORE DELETE ON provider_submit_recoveries
    FOR EACH ROW EXECUTE FUNCTION reject_provider_submit_recovery_delete();

CREATE TRIGGER provider_submit_recoveries_reject_truncate
    BEFORE TRUNCATE ON provider_submit_recoveries
    FOR EACH STATEMENT EXECUTE FUNCTION reject_provider_submit_recovery_delete();
