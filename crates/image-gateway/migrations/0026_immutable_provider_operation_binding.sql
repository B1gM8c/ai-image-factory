SET LOCAL lock_timeout = '5s';

LOCK TABLE provider_execution_profiles, provider_submissions,
    executor_executions, executor_capacity_allocations,
    provider_remote_submit_intents, provider_submit_recoveries,
    provider_remote_tasks
    IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM provider_remote_submit_intents)
       OR EXISTS (SELECT 1 FROM provider_submit_recoveries)
       OR EXISTS (SELECT 1 FROM provider_remote_tasks) THEN
        RAISE EXCEPTION
            'operation binding migration must run before the first remote provider activation';
    END IF;

    -- Both predicates are covered by the existing partial executor indexes.
    -- provider_waiting is already impossible after the remote-history gate.
    IF EXISTS (
        SELECT 1 FROM executor_executions
        WHERE state IN ('prepared', 'leased')
    ) OR EXISTS (
        SELECT 1 FROM executor_executions
        WHERE state = 'running'
    ) THEN
        RAISE EXCEPTION
            'operation binding migration requires active executor submissions to be drained';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM provider_execution_profiles
        WHERE NOT (
            provider_id = 'openai-codex'
            AND command_schema IN (
                'openai.images.generation.v1',
                'openai.images.edit.v1'
            )
        )
    ) THEN
        RAISE EXCEPTION
            'operation binding migration must run before provisioning non-Codex execution profiles';
    END IF;
END;
$$;

DROP TRIGGER provider_execution_profiles_identity ON provider_execution_profiles;
DROP TRIGGER provider_submission_state_transition ON provider_submissions;

ALTER TABLE provider_execution_profiles
    ADD COLUMN operation_id TEXT CHECK (
        char_length(operation_id) BETWEEN 1 AND 128
        AND operation_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    ADD COLUMN operation_descriptor_revision TEXT CHECK (
        char_length(operation_descriptor_revision) BETWEEN 1 AND 255
        AND operation_descriptor_revision !~ '[[:cntrl:]]'
    ),
    ADD COLUMN operation_descriptor_sha256_v1 TEXT CHECK (
        operation_descriptor_sha256_v1 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN completion_mode TEXT CHECK (
        completion_mode IN ('inline', 'remote_task')
    ),
    ADD COLUMN idempotency_mode TEXT CHECK (
        idempotency_mode IN ('submission_bound', 'provider_token')
    );

UPDATE provider_execution_profiles
SET operation_id = CASE command_schema
        WHEN 'openai.images.generation.v1' THEN 'images.generations'
        WHEN 'openai.images.edit.v1' THEN 'images.edits'
    END,
    operation_descriptor_revision = CASE command_schema
        WHEN 'openai.images.generation.v1'
            THEN 'openai-codex/images.generations/v1'
        WHEN 'openai.images.edit.v1'
            THEN 'openai-codex/images.edits/v1'
    END,
    operation_descriptor_sha256_v1 = CASE command_schema
        WHEN 'openai.images.generation.v1'
            THEN 'f7f3e84594bfda2312d9420aa22108e76b10b3b22c52535ccf768f944d9b7aaa'
        WHEN 'openai.images.edit.v1'
            THEN 'c9a714ae667cab60f8130b841aa8887077232a29a1c3bb59ba7ecb77b8ddb471'
    END,
    completion_mode = 'inline',
    idempotency_mode = 'submission_bound';

ALTER TABLE provider_execution_profiles
    ALTER COLUMN operation_id SET NOT NULL,
    ALTER COLUMN operation_descriptor_revision SET NOT NULL,
    ALTER COLUMN operation_descriptor_sha256_v1 SET NOT NULL,
    ALTER COLUMN completion_mode SET NOT NULL,
    ALTER COLUMN idempotency_mode SET NOT NULL;

CREATE TRIGGER provider_execution_profiles_identity
    BEFORE UPDATE OR DELETE ON provider_execution_profiles
    FOR EACH ROW EXECUTE FUNCTION enforce_execution_binding_identity();

ALTER TABLE provider_submissions
    ADD COLUMN operation_id TEXT,
    ADD COLUMN operation_descriptor_revision TEXT,
    ADD COLUMN operation_descriptor_sha256_v1 TEXT,
    ADD COLUMN completion_mode TEXT,
    ADD COLUMN idempotency_mode TEXT,
    ADD COLUMN operation_binding_version SMALLINT NOT NULL DEFAULT 1;

-- Existing rows are drained terminal evidence and remain version 1. PostgreSQL
-- enforces this NOT VALID constraint for every new row without scanning or
-- rewriting the historical submissions table during the deployment lock.
ALTER TABLE provider_submissions
    ADD CONSTRAINT provider_submissions_operation_binding_v2_check CHECK (
        operation_binding_version = 1
        OR (
            operation_binding_version = 2
            AND operation_id IS NOT NULL
            AND operation_descriptor_revision IS NOT NULL
            AND operation_descriptor_sha256_v1 IS NOT NULL
            AND completion_mode IS NOT NULL
            AND idempotency_mode IS NOT NULL
            AND char_length(operation_id) BETWEEN 1 AND 128
            AND operation_id ~ '^[A-Za-z0-9_.-]+$'
            AND char_length(operation_descriptor_revision) BETWEEN 1 AND 255
            AND operation_descriptor_revision !~ '[[:cntrl:]]'
            AND operation_descriptor_sha256_v1 ~ '^[0-9a-f]{64}$'
            AND completion_mode IN ('inline', 'remote_task')
            AND idempotency_mode IN ('submission_bound', 'provider_token')
        )
    ) NOT VALID;

ALTER TABLE provider_remote_submit_intents
    ADD COLUMN provider_command_sha256 TEXT NOT NULL CHECK (
        provider_command_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN execution_binding_sha256 TEXT NOT NULL CHECK (
        execution_binding_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN provider_timeout_ms BIGINT NOT NULL CHECK (
        provider_timeout_ms BETWEEN 1 AND 2592000000
    );

CREATE OR REPLACE FUNCTION validate_bound_provider_submission() RETURNS TRIGGER AS $$
DECLARE
    profile_enabled BOOLEAN;
BEGIN
    IF NEW.execution_profile_id IS NULL OR NEW.operation_binding_version <> 2 THEN
        RAISE EXCEPTION
            'new provider submissions require an exact operation binding';
    END IF;
    SELECT p.state = 'enabled'
           AND pool.state = 'enabled'
           AND account.state = 'enabled'
           AND policy.state = 'enabled'
    INTO profile_enabled
    FROM provider_execution_profiles p
    JOIN provider_credential_pools pool
      ON pool.credential_pool_id = p.credential_pool_id
     AND pool.provider_id = p.provider_id
    JOIN provider_accounts account
      ON account.provider_account_id = p.provider_account_id
     AND account.credential_pool_id = p.credential_pool_id
     AND account.provider_id = p.provider_id
     AND account.credential_ref = p.credential_ref
     AND account.credential_revision = p.credential_revision
    JOIN executor_resource_policies policy
      ON policy.resource_policy_id = p.resource_policy_id
     AND policy.revision = p.resource_policy_revision
    WHERE p.execution_profile_id = NEW.execution_profile_id
      AND p.provider_id = NEW.provider_id
      AND p.command_schema = NEW.command_schema
      AND p.adapter_revision = NEW.adapter_revision
      AND p.credential_pool_id = NEW.credential_pool_id
      AND p.provider_account_id = NEW.provider_account_id
      AND p.credential_ref = NEW.credential_ref
      AND p.credential_revision = NEW.credential_revision
      AND p.resource_policy_id = NEW.resource_policy_id
      AND p.resource_policy_revision = NEW.resource_policy_revision
      AND p.operation_id = NEW.operation_id
      AND p.operation_descriptor_revision = NEW.operation_descriptor_revision
      AND p.operation_descriptor_sha256_v1 = NEW.operation_descriptor_sha256_v1
      AND p.completion_mode = NEW.completion_mode
      AND p.idempotency_mode = NEW.idempotency_mode;
    IF profile_enabled IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'provider submission execution profile is unavailable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_provider_submission_state_transition() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.output_id IS DISTINCT FROM OLD.output_id
       OR NEW.job_id IS DISTINCT FROM OLD.job_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.model IS DISTINCT FROM OLD.model
       OR NEW.work_item_id IS DISTINCT FROM OLD.work_item_id
       OR NEW.created_by_execution_id IS DISTINCT FROM OLD.created_by_execution_id
       OR NEW.created_by_lease_epoch IS DISTINCT FROM OLD.created_by_lease_epoch
       OR NEW.command_schema IS DISTINCT FROM OLD.command_schema
       OR NEW.command_hash IS DISTINCT FROM OLD.command_hash
       OR NEW.prepared_at_ms IS DISTINCT FROM OLD.prepared_at_ms
       OR NEW.execution_profile_id IS DISTINCT FROM OLD.execution_profile_id
       OR NEW.credential_pool_id IS DISTINCT FROM OLD.credential_pool_id
       OR NEW.provider_account_id IS DISTINCT FROM OLD.provider_account_id
       OR NEW.credential_ref IS DISTINCT FROM OLD.credential_ref
       OR NEW.credential_revision IS DISTINCT FROM OLD.credential_revision
       OR NEW.adapter_revision IS DISTINCT FROM OLD.adapter_revision
       OR NEW.resource_policy_id IS DISTINCT FROM OLD.resource_policy_id
       OR NEW.resource_policy_revision IS DISTINCT FROM OLD.resource_policy_revision
       OR NEW.operation_id IS DISTINCT FROM OLD.operation_id
       OR NEW.operation_descriptor_revision IS DISTINCT FROM OLD.operation_descriptor_revision
       OR NEW.operation_descriptor_sha256_v1 IS DISTINCT FROM OLD.operation_descriptor_sha256_v1
       OR NEW.completion_mode IS DISTINCT FROM OLD.completion_mode
       OR NEW.idempotency_mode IS DISTINCT FROM OLD.idempotency_mode
       OR NEW.operation_binding_version IS DISTINCT FROM OLD.operation_binding_version THEN
        RAISE EXCEPTION 'provider submission identity and command are immutable';
    END IF;
    IF OLD.state IN ('succeeded', 'failed', 'uncertain', 'canceled') THEN
        RAISE EXCEPTION 'terminal provider submission is immutable';
    END IF;
    IF NOT (
        (OLD.state = 'prepared' AND NEW.state IN ('running', 'canceled'))
        OR (OLD.state = 'running'
            AND NEW.state IN ('provider_waiting', 'succeeded', 'failed', 'uncertain'))
        OR (OLD.state = 'provider_waiting'
            AND NEW.state IN ('succeeded', 'failed', 'uncertain', 'canceled'))
    ) THEN
        RAISE EXCEPTION 'invalid provider submission state transition';
    END IF;
    IF OLD.state IN ('running', 'provider_waiting')
       AND NEW.started_at_ms IS DISTINCT FROM OLD.started_at_ms THEN
        RAISE EXCEPTION 'provider submission start history is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submission_state_transition
    BEFORE UPDATE ON provider_submissions
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submission_state_transition();

CREATE OR REPLACE FUNCTION validate_provider_submit_intent_insert() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.state <> 'reserved' OR NEW.remote_operation_id IS NOT NULL
       OR NOT EXISTS (
            SELECT 1
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
             AND allocation.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = NEW.executor_execution_id
              AND execution.submission_id = NEW.submission_id
              AND execution.state = 'running'
              AND submission.state = 'running'
              AND execution.executor_owner = NEW.submit_owner
              AND execution.lease_epoch = NEW.submit_lease_epoch
              AND execution.launch_owner = NEW.submit_owner
              AND execution.launch_lease_epoch = NEW.submit_lease_epoch
              AND execution.lease_expires_at_ms > now_ms
              AND submission.provider_id = NEW.provider_id
              AND submission.provider_account_id = NEW.provider_account_id
              AND submission.operation_binding_version = 2
              AND submission.completion_mode = 'remote_task'
              AND submission.idempotency_mode = 'submission_bound'
              AND allocation.state = 'held'
       ) THEN
        RAISE EXCEPTION
            'provider submit reservation requires an exact remote operation binding';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_provider_submit_intent_update() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.provider_account_id IS DISTINCT FROM OLD.provider_account_id
       OR NEW.submit_owner IS DISTINCT FROM OLD.submit_owner
       OR NEW.submit_lease_epoch IS DISTINCT FROM OLD.submit_lease_epoch
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.provider_command_sha256 IS DISTINCT FROM OLD.provider_command_sha256
       OR NEW.execution_binding_sha256 IS DISTINCT FROM OLD.execution_binding_sha256
       OR NEW.provider_timeout_ms IS DISTINCT FROM OLD.provider_timeout_ms
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms
       OR NEW.updated_at_ms < OLD.updated_at_ms THEN
        RAISE EXCEPTION 'provider submit intent identity and history are immutable';
    END IF;

    IF OLD.state IN ('attached', 'rejected') THEN
        RAISE EXCEPTION 'terminal provider submit intent is immutable';
    END IF;

    IF OLD.state = 'reserved' AND NEW.state = 'sending' THEN
        IF NEW.send_started_at_ms IS NULL
           OR NEW.send_started_at_ms IS DISTINCT FROM NEW.updated_at_ms
           OR NEW.remote_operation_id IS DISTINCT FROM OLD.remote_operation_id
           OR NEW.provider_request_id IS DISTINCT FROM OLD.provider_request_id
           OR NEW.receipt_event_identity IS DISTINCT FROM OLD.receipt_event_identity
           OR NEW.failure_event_identity IS DISTINCT FROM OLD.failure_event_identity
           OR NEW.failure_error_code IS DISTINCT FROM OLD.failure_error_code
           OR NOT EXISTS (
                SELECT 1
                FROM executor_executions execution
                JOIN provider_submissions submission
                  ON submission.executor_execution_id = execution.executor_execution_id
                 AND submission.submission_id = execution.submission_id
                JOIN executor_capacity_allocations allocation
                  ON allocation.executor_execution_id = execution.executor_execution_id
                 AND allocation.submission_id = execution.submission_id
                WHERE execution.executor_execution_id = NEW.executor_execution_id
                  AND execution.submission_id = NEW.submission_id
                  AND execution.state = 'running' AND submission.state = 'running'
                  AND execution.executor_owner = NEW.submit_owner
                  AND execution.lease_epoch = NEW.submit_lease_epoch
                  AND execution.launch_owner = NEW.submit_owner
                  AND execution.launch_lease_epoch = NEW.submit_lease_epoch
                  AND execution.lease_expires_at_ms > now_ms
                  AND allocation.state = 'held'
           ) THEN
            RAISE EXCEPTION 'provider submit start requires the live executor fence';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'sending'
       AND NEW.state IN ('operation_known', 'outcome_unknown', 'rejected') THEN
        IF NEW.send_started_at_ms IS DISTINCT FROM OLD.send_started_at_ms THEN
            RAISE EXCEPTION 'invalid provider submit outcome transition';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'outcome_unknown' AND NEW.state = 'operation_known' THEN
        IF NEW.send_started_at_ms IS DISTINCT FROM OLD.send_started_at_ms
           OR NEW.failure_event_identity IS DISTINCT FROM OLD.failure_event_identity
           OR NEW.failure_error_code IS DISTINCT FROM OLD.failure_error_code THEN
            RAISE EXCEPTION 'invalid provider submit reconciliation transition';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state IN ('sending', 'outcome_unknown', 'operation_known')
       AND NEW.state = 'deadline_quarantined' THEN
        IF NEW.remote_operation_id IS DISTINCT FROM OLD.remote_operation_id
           OR NEW.provider_request_id IS DISTINCT FROM OLD.provider_request_id
           OR NEW.send_started_at_ms IS DISTINCT FROM OLD.send_started_at_ms
           OR NEW.receipt_event_identity IS DISTINCT FROM OLD.receipt_event_identity
           OR NEW.failure_event_identity IS DISTINCT FROM OLD.failure_event_identity
           OR NEW.failure_error_code IS DISTINCT FROM OLD.failure_error_code THEN
            RAISE EXCEPTION 'submit deadline cannot rewrite provider evidence';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'deadline_quarantined'
       AND NEW.state = 'deadline_quarantined'
       AND OLD.remote_operation_id IS NULL
       AND OLD.provider_request_id IS NULL
       AND OLD.receipt_event_identity IS NULL THEN
        IF NEW.remote_operation_id IS NULL OR NEW.receipt_event_identity IS NULL
           OR NEW.send_started_at_ms IS DISTINCT FROM OLD.send_started_at_ms
           OR NEW.failure_event_identity IS DISTINCT FROM OLD.failure_event_identity
           OR NEW.failure_error_code IS DISTINCT FROM OLD.failure_error_code THEN
            RAISE EXCEPTION 'invalid late submit receipt';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'operation_known' AND NEW.state = 'attached' THEN
        IF NEW.remote_operation_id IS DISTINCT FROM OLD.remote_operation_id
           OR NEW.provider_request_id IS DISTINCT FROM OLD.provider_request_id
           OR NEW.send_started_at_ms IS DISTINCT FROM OLD.send_started_at_ms
           OR NEW.receipt_event_identity IS DISTINCT FROM OLD.receipt_event_identity
           OR NEW.failure_event_identity IS DISTINCT FROM OLD.failure_event_identity
           OR NEW.failure_error_code IS DISTINCT FROM OLD.failure_error_code THEN
            RAISE EXCEPTION 'provider submit attach cannot rewrite its receipt';
        END IF;
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'invalid provider submit intent transition';
END;
$$ LANGUAGE plpgsql;
