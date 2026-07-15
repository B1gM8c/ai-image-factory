LOCK TABLE provider_submit_recoveries IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_submit_recoveries
        WHERE state = 'active' AND recovery_owner IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'provider recovery command migration requires active recovery workers to be drained';
    END IF;
END;
$$;

CREATE TABLE provider_submit_recovery_commands (
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_account_id UUID NOT NULL,
    command_owner TEXT NOT NULL CHECK (
        octet_length(command_owner) BETWEEN 1 AND 255
        AND command_owner !~ '[[:cntrl:]]'
    ),
    command_id TEXT NOT NULL CHECK (
        octet_length(command_id) BETWEEN 1 AND 255
        AND command_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]*$'
        AND position('://' IN command_id) = 0
    ),
    command_kind TEXT NOT NULL CHECK (command_kind IN ('claim', 'defer')),
    request_duration_ms BIGINT NOT NULL CHECK (
        request_duration_ms BETWEEN 1 AND 86400000
    ),
    submission_id UUID NOT NULL,
    executor_execution_id UUID NOT NULL,
    recovery_lease_epoch BIGINT NOT NULL CHECK (recovery_lease_epoch > 0),
    claim_claimed_at_ms BIGINT,
    claim_lease_expires_at_ms BIGINT,
    intent_state TEXT CHECK (
        intent_state IN ('sending', 'outcome_unknown', 'operation_known')
    ),
    intent_remote_operation_id TEXT,
    intent_provider_request_id TEXT,
    intent_send_started_at_ms BIGINT,
    intent_receipt_event_identity TEXT,
    intent_failure_event_identity TEXT,
    intent_failure_error_code TEXT,
    intent_updated_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (provider_id, provider_account_id, command_owner, command_id),
    FOREIGN KEY (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) REFERENCES provider_submit_recoveries (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) ON DELETE RESTRICT,
    CHECK (
        (
            command_kind = 'claim'
            AND claim_claimed_at_ms IS NOT NULL
            AND claim_lease_expires_at_ms > claim_claimed_at_ms
            AND claim_lease_expires_at_ms
                  <= claim_claimed_at_ms + request_duration_ms
            AND intent_state IS NOT NULL
            AND intent_updated_at_ms IS NOT NULL
            AND created_at_ms = claim_claimed_at_ms
        )
        OR
        (
            command_kind = 'defer'
            AND claim_claimed_at_ms IS NULL
            AND claim_lease_expires_at_ms IS NULL
            AND intent_state IS NULL
            AND intent_remote_operation_id IS NULL
            AND intent_provider_request_id IS NULL
            AND intent_send_started_at_ms IS NULL
            AND intent_receipt_event_identity IS NULL
            AND intent_failure_event_identity IS NULL
            AND intent_failure_error_code IS NULL
            AND intent_updated_at_ms IS NULL
        )
    )
);

CREATE UNIQUE INDEX provider_submit_recovery_commands_transition_uidx
    ON provider_submit_recovery_commands (
        submission_id, recovery_lease_epoch, command_kind
    );

CREATE FUNCTION validate_provider_submit_recovery_command_insert() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.created_at_ms > now_ms THEN
        RAISE EXCEPTION 'provider submit recovery command cannot be future dated';
    END IF;

    IF NEW.command_kind = 'claim' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM provider_submit_recoveries recovery
            JOIN provider_remote_submit_intents intent
              ON intent.submission_id = recovery.submission_id
             AND intent.executor_execution_id = recovery.executor_execution_id
            WHERE recovery.submission_id = NEW.submission_id
              AND recovery.executor_execution_id = NEW.executor_execution_id
              AND recovery.provider_id = NEW.provider_id
              AND recovery.provider_account_id = NEW.provider_account_id
              AND recovery.state = 'active'
              AND recovery.recovery_owner = NEW.command_owner
              AND recovery.recovery_lease_epoch = NEW.recovery_lease_epoch
              AND recovery.recovery_claimed_at_ms = NEW.claim_claimed_at_ms
              AND recovery.recovery_lease_expires_at_ms =
                    NEW.claim_lease_expires_at_ms
              AND NEW.claim_lease_expires_at_ms = LEAST(
                    recovery.provider_deadline_at_ms,
                    NEW.claim_claimed_at_ms + NEW.request_duration_ms
                  )
              AND recovery.provider_deadline_at_ms >=
                    NEW.claim_lease_expires_at_ms
              AND intent.state = NEW.intent_state
              AND intent.remote_operation_id IS NOT DISTINCT FROM
                    NEW.intent_remote_operation_id
              AND intent.provider_request_id IS NOT DISTINCT FROM
                    NEW.intent_provider_request_id
              AND intent.send_started_at_ms IS NOT DISTINCT FROM
                    NEW.intent_send_started_at_ms
              AND intent.receipt_event_identity IS NOT DISTINCT FROM
                    NEW.intent_receipt_event_identity
              AND intent.failure_event_identity IS NOT DISTINCT FROM
                    NEW.intent_failure_event_identity
              AND intent.failure_error_code IS NOT DISTINCT FROM
                    NEW.intent_failure_error_code
              AND intent.updated_at_ms = NEW.intent_updated_at_ms
        ) THEN
            RAISE EXCEPTION
                'provider submit recovery claim command requires its exact live result';
        END IF;
    ELSE
        IF NOT EXISTS (
            SELECT 1
            FROM provider_submit_recoveries recovery
            WHERE recovery.submission_id = NEW.submission_id
              AND recovery.executor_execution_id = NEW.executor_execution_id
              AND recovery.provider_id = NEW.provider_id
              AND recovery.provider_account_id = NEW.provider_account_id
              AND recovery.state = 'active'
              AND recovery.recovery_owner = NEW.command_owner
              AND recovery.recovery_lease_epoch = NEW.recovery_lease_epoch
              AND recovery.recovery_lease_expires_at_ms > NEW.created_at_ms
              AND recovery.provider_deadline_at_ms > NEW.created_at_ms
        ) THEN
            RAISE EXCEPTION
                'provider submit recovery defer command requires its exact live lease';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submit_recovery_command_insert_guard
    BEFORE INSERT ON provider_submit_recovery_commands
    FOR EACH ROW EXECUTE FUNCTION validate_provider_submit_recovery_command_insert();

CREATE FUNCTION enforce_provider_submit_recovery_command_projection() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.state = 'active'
       AND NEW.recovery_owner IS NOT NULL
       AND (
            OLD.recovery_owner IS NULL
            OR NEW.recovery_lease_epoch = OLD.recovery_lease_epoch + 1
       ) THEN
        IF NOT EXISTS (
            SELECT 1
            FROM provider_submit_recovery_commands command
            WHERE command.provider_id = NEW.provider_id
              AND command.provider_account_id = NEW.provider_account_id
              AND command.command_kind = 'claim'
              AND command.command_owner = NEW.recovery_owner
              AND command.submission_id = NEW.submission_id
              AND command.executor_execution_id = NEW.executor_execution_id
              AND command.recovery_lease_epoch = NEW.recovery_lease_epoch
              AND command.claim_claimed_at_ms = NEW.recovery_claimed_at_ms
              AND command.claim_lease_expires_at_ms =
                    NEW.recovery_lease_expires_at_ms
        ) THEN
            RAISE EXCEPTION
                'provider submit recovery acquisition requires durable command evidence';
        END IF;
    ELSIF NEW.state = 'active'
          AND OLD.recovery_owner IS NOT NULL
          AND NEW.recovery_owner IS NULL THEN
        IF NOT EXISTS (
            SELECT 1
            FROM provider_submit_recovery_commands command
            WHERE command.provider_id = NEW.provider_id
              AND command.provider_account_id = NEW.provider_account_id
              AND command.command_kind = 'defer'
              AND command.command_owner = OLD.recovery_owner
              AND command.submission_id = NEW.submission_id
              AND command.executor_execution_id = NEW.executor_execution_id
              AND command.recovery_lease_epoch = OLD.recovery_lease_epoch
              AND command.created_at_ms = NEW.updated_at_ms
              AND NEW.next_recovery_at_ms = LEAST(
                    NEW.provider_deadline_at_ms,
                    command.created_at_ms + command.request_duration_ms
                  )
        ) THEN
            RAISE EXCEPTION
                'provider submit recovery deferral requires durable command evidence';
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_submit_recovery_command_projection_check
    AFTER UPDATE ON provider_submit_recoveries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_provider_submit_recovery_command_projection();

CREATE FUNCTION enforce_provider_submit_recovery_command_result() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.command_kind = 'claim' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM provider_submit_recoveries recovery
            WHERE recovery.submission_id = NEW.submission_id
              AND recovery.executor_execution_id = NEW.executor_execution_id
              AND recovery.provider_id = NEW.provider_id
              AND recovery.provider_account_id = NEW.provider_account_id
              AND recovery.state = 'active'
              AND recovery.recovery_owner = NEW.command_owner
              AND recovery.recovery_lease_epoch = NEW.recovery_lease_epoch
              AND recovery.recovery_claimed_at_ms = NEW.claim_claimed_at_ms
              AND recovery.recovery_lease_expires_at_ms >=
                    NEW.claim_lease_expires_at_ms
              AND NEW.claim_lease_expires_at_ms = LEAST(
                    recovery.provider_deadline_at_ms,
                    NEW.claim_claimed_at_ms + NEW.request_duration_ms
                  )
        ) THEN
            RAISE EXCEPTION
                'provider submit recovery claim command lost its authority result';
        END IF;
    ELSE
        IF NOT EXISTS (
            SELECT 1
            FROM provider_submit_recoveries recovery
            WHERE recovery.submission_id = NEW.submission_id
              AND recovery.executor_execution_id = NEW.executor_execution_id
              AND recovery.provider_id = NEW.provider_id
              AND recovery.provider_account_id = NEW.provider_account_id
              AND recovery.state = 'active'
              AND recovery.recovery_owner IS NULL
              AND recovery.recovery_lease_epoch = NEW.recovery_lease_epoch
              AND recovery.recovery_claimed_at_ms IS NULL
              AND recovery.recovery_lease_expires_at_ms IS NULL
              AND recovery.updated_at_ms = NEW.created_at_ms
              AND recovery.next_recovery_at_ms = LEAST(
                    recovery.provider_deadline_at_ms,
                    NEW.created_at_ms + NEW.request_duration_ms
                  )
        ) THEN
            RAISE EXCEPTION
                'provider submit recovery defer command lost its committed result';
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_submit_recovery_command_result_check
    AFTER INSERT ON provider_submit_recovery_commands
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_provider_submit_recovery_command_result();

CREATE FUNCTION reject_provider_submit_recovery_command_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider submit recovery command history is immutable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submit_recovery_commands_reject_update
    BEFORE UPDATE ON provider_submit_recovery_commands
    FOR EACH ROW
    EXECUTE FUNCTION reject_provider_submit_recovery_command_mutation();

CREATE TRIGGER provider_submit_recovery_commands_reject_delete
    BEFORE DELETE ON provider_submit_recovery_commands
    FOR EACH ROW
    EXECUTE FUNCTION reject_provider_submit_recovery_command_mutation();

CREATE TRIGGER provider_submit_recovery_commands_reject_truncate
    BEFORE TRUNCATE ON provider_submit_recovery_commands
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_provider_submit_recovery_command_mutation();
