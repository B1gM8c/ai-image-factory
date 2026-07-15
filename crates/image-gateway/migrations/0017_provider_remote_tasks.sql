LOCK TABLE executor_executions, provider_submissions
    IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM executor_executions execution
        JOIN provider_submissions submission
          ON submission.executor_execution_id = execution.executor_execution_id
         AND submission.submission_id = execution.submission_id
        WHERE execution.state IN ('leased', 'running')
           OR submission.state = 'running'
    ) THEN
        RAISE EXCEPTION
            'remote provider task migration requires active executor submissions to be drained';
    END IF;
END;
$$;

ALTER TABLE provider_submissions
    DROP CONSTRAINT provider_submissions_state_check,
    DROP CONSTRAINT provider_submissions_check1,
    DROP CONSTRAINT provider_submission_resolution_presence_check,
    ADD CONSTRAINT provider_submissions_state_check CHECK (
        state IN (
            'prepared', 'running', 'provider_waiting',
            'succeeded', 'failed', 'uncertain', 'canceled'
        )
    ),
    ADD CONSTRAINT provider_submissions_lifecycle_check CHECK (
        (state = 'prepared' AND started_at_ms IS NULL AND finished_at_ms IS NULL
            AND result_manifest_id IS NULL AND error_code IS NULL)
        OR
        (state IN ('running', 'provider_waiting')
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NULL
            AND result_manifest_id IS NULL AND error_code IS NULL)
        OR
        (state = 'succeeded' AND started_at_ms IS NOT NULL AND finished_at_ms IS NOT NULL
            AND result_manifest_id IS NOT NULL AND error_code IS NULL)
        OR
        (state IN ('failed', 'uncertain')
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NOT NULL
            AND result_manifest_id IS NULL AND error_code IS NOT NULL)
        OR
        (state = 'canceled' AND finished_at_ms IS NOT NULL
            AND result_manifest_id IS NULL AND error_code IS NOT NULL)
    ),
    ADD CONSTRAINT provider_submission_resolution_presence_check CHECK (
        (state IN ('prepared', 'running', 'provider_waiting')
            AND resolution_decision_id IS NULL)
        OR
        (state IN ('succeeded', 'failed', 'uncertain', 'canceled')
            AND resolution_decision_id IS NOT NULL)
    ),
    ADD CONSTRAINT provider_submission_remote_task_identity_unique UNIQUE (
        submission_id, executor_execution_id, provider_id, provider_account_id
    );

ALTER TABLE executor_executions
    DROP CONSTRAINT executor_executions_state_check,
    DROP CONSTRAINT executor_executions_check,
    DROP CONSTRAINT executor_execution_launch_fence_check,
    DROP CONSTRAINT executor_execution_resolution_presence_check,
    ADD CONSTRAINT executor_executions_state_check CHECK (
        state IN (
            'prepared', 'leased', 'running', 'provider_waiting',
            'succeeded', 'failed', 'uncertain', 'canceled'
        )
    ),
    ADD CONSTRAINT executor_executions_lifecycle_check CHECK (
        (state = 'prepared'
            AND executor_owner IS NULL AND lease_epoch = 0
            AND lease_expires_at_ms IS NULL AND leased_at_ms IS NULL
            AND started_at_ms IS NULL AND finished_at_ms IS NULL AND error_code IS NULL)
        OR
        (state = 'leased'
            AND executor_owner IS NOT NULL AND executor_owner <> '' AND lease_epoch > 0
            AND lease_expires_at_ms IS NOT NULL AND leased_at_ms IS NOT NULL
            AND started_at_ms IS NULL AND finished_at_ms IS NULL AND error_code IS NULL)
        OR
        (state = 'running'
            AND executor_owner IS NOT NULL AND executor_owner <> '' AND lease_epoch > 0
            AND lease_expires_at_ms IS NOT NULL AND leased_at_ms IS NOT NULL
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NULL AND error_code IS NULL)
        OR
        (state = 'provider_waiting'
            AND executor_owner IS NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NULL AND leased_at_ms IS NOT NULL
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NULL AND error_code IS NULL)
        OR
        (state = 'succeeded'
            AND executor_owner IS NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NULL AND leased_at_ms IS NOT NULL
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NOT NULL AND error_code IS NULL)
        OR
        (state IN ('failed', 'uncertain')
            AND executor_owner IS NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NULL AND leased_at_ms IS NOT NULL
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NOT NULL
            AND error_code IS NOT NULL)
        OR
        (state = 'canceled'
            AND executor_owner IS NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NULL AND leased_at_ms IS NOT NULL
            AND finished_at_ms IS NOT NULL AND error_code IS NOT NULL)
    ),
    ADD CONSTRAINT executor_execution_launch_fence_check CHECK (
        (state IN ('prepared', 'leased')
            AND launch_owner IS NULL AND launch_lease_epoch IS NULL)
        OR
        (state = 'canceled' AND started_at_ms IS NULL
            AND launch_owner IS NULL AND launch_lease_epoch IS NULL)
        OR
        (state IN ('running', 'provider_waiting', 'succeeded', 'failed', 'uncertain')
            AND launch_owner IS NOT NULL AND launch_owner <> ''
            AND launch_lease_epoch IS NOT NULL AND launch_lease_epoch > 0)
        OR
        (state = 'canceled' AND started_at_ms IS NOT NULL
            AND launch_owner IS NOT NULL AND launch_owner <> ''
            AND launch_lease_epoch IS NOT NULL AND launch_lease_epoch > 0)
    ),
    ADD CONSTRAINT executor_execution_resolution_presence_check CHECK (
        (state IN ('prepared', 'leased', 'running', 'provider_waiting')
            AND resolution_decision_id IS NULL)
        OR
        (state IN ('succeeded', 'failed', 'uncertain', 'canceled')
            AND resolution_decision_id IS NOT NULL)
    );

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
       OR NEW.resource_policy_revision IS DISTINCT FROM OLD.resource_policy_revision THEN
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

CREATE OR REPLACE FUNCTION enforce_executor_lease_updates() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms THEN
        RAISE EXCEPTION 'executor execution identity is immutable';
    END IF;
    IF OLD.state IN ('succeeded', 'failed', 'uncertain', 'canceled') THEN
        RAISE EXCEPTION 'terminal executor execution is immutable';
    END IF;
    IF NOT (
        (OLD.state = 'prepared' AND NEW.state = 'leased')
        OR (OLD.state = 'leased' AND NEW.state IN ('leased', 'running', 'canceled'))
        OR (OLD.state = 'running'
            AND NEW.state IN ('running', 'provider_waiting', 'succeeded', 'failed', 'uncertain'))
        OR (OLD.state = 'provider_waiting'
            AND NEW.state IN ('succeeded', 'failed', 'uncertain', 'canceled'))
    ) THEN
        RAISE EXCEPTION 'invalid executor execution state transition';
    END IF;
    IF OLD.state = 'prepared' AND NEW.state = 'leased' THEN
        IF NEW.lease_epoch <> OLD.lease_epoch + 1
           OR NEW.lease_expires_at_ms IS NULL
           OR NEW.lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'executor claim must create a future monotonic lease fence';
        END IF;
    ELSIF OLD.state = 'leased' AND NEW.state = 'leased' THEN
        IF NEW.executor_owner IS DISTINCT FROM OLD.executor_owner
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch THEN
            IF OLD.lease_expires_at_ms > now_ms
               OR NEW.lease_epoch <> OLD.lease_epoch + 1
               OR NEW.lease_expires_at_ms <= now_ms THEN
                RAISE EXCEPTION 'executor reclaim requires an expired lease and the next epoch';
            END IF;
        ELSIF NEW.lease_expires_at_ms IS DISTINCT FROM OLD.lease_expires_at_ms THEN
            RAISE EXCEPTION 'leased executor cannot renew the same lease fence';
        END IF;
    ELSIF OLD.state = 'leased' AND NEW.state = 'running' THEN
        IF NEW.executor_owner IS DISTINCT FROM OLD.executor_owner
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.lease_expires_at_ms IS DISTINCT FROM OLD.lease_expires_at_ms
           OR NEW.lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'executor start must preserve a live lease fence';
        END IF;
    ELSIF OLD.state = 'running' AND NEW.state = 'running' THEN
        IF NEW.executor_owner IS DISTINCT FROM OLD.executor_owner
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR OLD.lease_expires_at_ms <= now_ms
           OR NEW.lease_expires_at_ms <= now_ms
           OR NEW.lease_expires_at_ms < OLD.lease_expires_at_ms THEN
            RAISE EXCEPTION 'running executor lease fence is immutable and monotonic';
        END IF;
    ELSIF OLD.state = 'running' AND NEW.state = 'provider_waiting' THEN
        IF OLD.lease_expires_at_ms <= now_ms
           OR NEW.executor_owner IS NOT NULL
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.lease_expires_at_ms IS NOT NULL
           OR NEW.leased_at_ms IS DISTINCT FROM OLD.leased_at_ms
           OR NEW.started_at_ms IS DISTINCT FROM OLD.started_at_ms
           OR NEW.finished_at_ms IS NOT NULL OR NEW.error_code IS NOT NULL THEN
            RAISE EXCEPTION 'remote provider handoff requires the live executor fence';
        END IF;
    ELSIF OLD.state = 'provider_waiting' THEN
        IF NEW.executor_owner IS NOT NULL
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.lease_expires_at_ms IS NOT NULL
           OR NEW.leased_at_ms IS DISTINCT FROM OLD.leased_at_ms
           OR NEW.started_at_ms IS DISTINCT FROM OLD.started_at_ms THEN
            RAISE EXCEPTION 'remote provider waiting cannot reacquire executor ownership';
        END IF;
    END IF;
    IF OLD.state = 'running'
       AND NEW.state <> 'provider_waiting'
       AND (NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
            OR NEW.leased_at_ms IS DISTINCT FROM OLD.leased_at_ms
            OR NEW.started_at_ms IS DISTINCT FROM OLD.started_at_ms) THEN
        RAISE EXCEPTION 'running executor history is immutable';
    END IF;
    IF OLD.state = 'leased' AND NEW.state IN ('running', 'canceled')
       AND (NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
            OR NEW.leased_at_ms IS DISTINCT FROM OLD.leased_at_ms) THEN
        RAISE EXCEPTION 'leased executor history is immutable across resolution';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_executor_resolution_projection() RETURNS TRIGGER AS $$
DECLARE
    executor_state TEXT;
    executor_decision UUID;
    submission_state TEXT;
    submission_decision UUID;
    submission_manifest UUID;
    submission_error TEXT;
    executor_error TEXT;
    decision_manifest UUID;
    decision_error TEXT;
BEGIN
    SELECT e.state, e.resolution_decision_id, e.error_code,
           s.state, s.resolution_decision_id, s.result_manifest_id, s.error_code
    INTO executor_state, executor_decision, executor_error,
         submission_state, submission_decision, submission_manifest, submission_error
    FROM executor_executions e
    JOIN provider_submissions s
      ON s.executor_execution_id = e.executor_execution_id
     AND s.submission_id = e.submission_id
    WHERE e.executor_execution_id = NEW.executor_execution_id
      AND e.submission_id = NEW.submission_id;

    IF executor_state IN ('prepared', 'leased') THEN
        IF submission_state <> 'prepared' THEN
            RAISE EXCEPTION 'executor and submission nonterminal projections are inconsistent';
        END IF;
    ELSIF executor_state = 'running' THEN
        IF submission_state <> 'running' THEN
            RAISE EXCEPTION 'executor and submission nonterminal projections are inconsistent';
        END IF;
    ELSIF executor_state = 'provider_waiting' THEN
        IF submission_state <> 'provider_waiting' THEN
            RAISE EXCEPTION 'remote provider projections are inconsistent';
        END IF;
    ELSE
        IF executor_state IS DISTINCT FROM submission_state
           OR executor_decision IS DISTINCT FROM submission_decision
           OR executor_decision IS NULL THEN
            RAISE EXCEPTION
                'executor and submission terminal resolution projections must match';
        END IF;
        SELECT result_manifest_id, error_code
        INTO decision_manifest, decision_error
        FROM executor_resolution_decisions
        WHERE decision_id = executor_decision
          AND executor_execution_id = NEW.executor_execution_id
          AND submission_id = NEW.submission_id;
        IF submission_manifest IS DISTINCT FROM decision_manifest
           OR submission_error IS DISTINCT FROM decision_error
           OR executor_error IS DISTINCT FROM decision_error THEN
            RAISE EXCEPTION
                'canonical terminal payload must match the resolution decision';
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE provider_remote_submit_intents (
    submission_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_account_id UUID NOT NULL,
    submit_owner TEXT NOT NULL CHECK (
        char_length(submit_owner) BETWEEN 1 AND 255
        AND submit_owner !~ '[[:cntrl:]]'
    ),
    submit_lease_epoch BIGINT NOT NULL CHECK (submit_lease_epoch > 0),
    idempotency_key TEXT NOT NULL CHECK (
        char_length(idempotency_key) BETWEEN 1 AND 255
        AND idempotency_key ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
        AND idempotency_key NOT LIKE '%://%'
    ),
    state TEXT NOT NULL CHECK (state IN ('reserved', 'attached')),
    remote_operation_id TEXT CHECK (
        remote_operation_id IS NULL
        OR (
            char_length(remote_operation_id) BETWEEN 1 AND 255
            AND remote_operation_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
            AND remote_operation_id NOT LIKE '%://%'
        )
    ),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) REFERENCES provider_submissions (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) ON DELETE RESTRICT,
    UNIQUE (submission_id, executor_execution_id, provider_id, provider_account_id),
    UNIQUE (provider_id, provider_account_id, idempotency_key),
    CHECK (
        (state = 'reserved' AND remote_operation_id IS NULL)
        OR (state = 'attached' AND remote_operation_id IS NOT NULL)
    )
);

CREATE FUNCTION validate_provider_submit_intent_insert() RETURNS TRIGGER AS $$
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
              AND allocation.state = 'held'
       ) THEN
        RAISE EXCEPTION
            'provider submit reservation requires the live executor fence';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submit_intent_insert_guard
    BEFORE INSERT ON provider_remote_submit_intents
    FOR EACH ROW EXECUTE FUNCTION validate_provider_submit_intent_insert();

CREATE FUNCTION enforce_provider_submit_intent_update() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.state = 'attached'
       OR NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.provider_account_id IS DISTINCT FROM OLD.provider_account_id
       OR NEW.submit_owner IS DISTINCT FROM OLD.submit_owner
       OR NEW.submit_lease_epoch IS DISTINCT FROM OLD.submit_lease_epoch
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms
       OR NEW.state <> 'attached'
       OR NEW.remote_operation_id IS NULL
       OR to_jsonb(NEW) - 'state' - 'remote_operation_id' - 'updated_at_ms'
          IS DISTINCT FROM
          to_jsonb(OLD) - 'state' - 'remote_operation_id' - 'updated_at_ms' THEN
        RAISE EXCEPTION 'invalid provider submit intent transition';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submit_intent_update_guard
    BEFORE UPDATE ON provider_remote_submit_intents
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submit_intent_update();

CREATE TABLE provider_remote_tasks (
    submission_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_account_id UUID NOT NULL,
    remote_operation_id TEXT NOT NULL CHECK (
        char_length(remote_operation_id) BETWEEN 1 AND 255
        AND remote_operation_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
        AND remote_operation_id NOT LIKE '%://%'
    ),
    provider_request_id TEXT CHECK (
        provider_request_id IS NULL
        OR (
            char_length(provider_request_id) BETWEEN 1 AND 255
            AND provider_request_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
            AND provider_request_id NOT LIKE '%://%'
        )
    ),
    submit_owner TEXT NOT NULL CHECK (
        char_length(submit_owner) BETWEEN 1 AND 255
        AND submit_owner !~ '[[:cntrl:]]'
    ),
    submit_lease_epoch BIGINT NOT NULL CHECK (submit_lease_epoch > 0),
    state TEXT NOT NULL CHECK (
        state IN ('provider_waiting', 'artifact_ready', 'failed', 'canceled', 'uncertain')
    ),
    artifact_ref TEXT CHECK (
        artifact_ref IS NULL
        OR (
            char_length(artifact_ref) BETWEEN 1 AND 512
            AND artifact_ref ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]*$'
            AND artifact_ref NOT LIKE '%://%'
        )
    ),
    error_code TEXT CHECK (
        error_code IS NULL OR error_code ~ '^[A-Za-z0-9_.-]{1,128}$'
    ),
    effect_certainty TEXT NOT NULL CHECK (
        effect_certainty IN ('not_applicable', 'confirmed_no_effect', 'unknown_remote_effect')
    ),
    next_poll_at_ms BIGINT,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_requested_at_ms BIGINT,
    poll_owner TEXT CHECK (
        poll_owner IS NULL
        OR (char_length(poll_owner) BETWEEN 1 AND 255 AND poll_owner !~ '[[:cntrl:]]')
    ),
    poll_lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (poll_lease_epoch >= 0),
    poll_lease_expires_at_ms BIGINT,
    poll_claimed_at_ms BIGINT,
    state_observation_id UUID NOT NULL,
    last_wakeup_observation_id UUID,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    terminal_at_ms BIGINT,
    FOREIGN KEY (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) REFERENCES provider_submissions (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) ON DELETE RESTRICT,
    FOREIGN KEY (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) REFERENCES provider_remote_submit_intents (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) ON DELETE RESTRICT,
    UNIQUE (provider_id, provider_account_id, remote_operation_id),
    UNIQUE (submission_id, executor_execution_id),
    UNIQUE (submission_id, executor_execution_id, provider_id, provider_account_id),
    CHECK (
        (state = 'provider_waiting'
            AND artifact_ref IS NULL AND error_code IS NULL
            AND effect_certainty = 'not_applicable'
            AND next_poll_at_ms IS NOT NULL AND terminal_at_ms IS NULL)
        OR
        (state = 'artifact_ready'
            AND artifact_ref IS NOT NULL AND error_code IS NULL
            AND effect_certainty = 'not_applicable'
            AND next_poll_at_ms IS NULL AND terminal_at_ms IS NOT NULL)
        OR
        (state = 'failed'
            AND artifact_ref IS NULL AND error_code IS NOT NULL
            AND effect_certainty = 'not_applicable'
            AND next_poll_at_ms IS NULL AND terminal_at_ms IS NOT NULL)
        OR
        (state = 'canceled'
            AND artifact_ref IS NULL AND error_code IS NOT NULL
            AND effect_certainty = 'confirmed_no_effect'
            AND next_poll_at_ms IS NULL AND terminal_at_ms IS NOT NULL
            AND cancel_requested)
        OR
        (state = 'uncertain'
            AND artifact_ref IS NULL AND error_code IS NOT NULL
            AND effect_certainty = 'unknown_remote_effect'
            AND next_poll_at_ms IS NULL AND terminal_at_ms IS NOT NULL)
    ),
    CHECK (
        (NOT cancel_requested AND cancel_requested_at_ms IS NULL)
        OR (cancel_requested AND cancel_requested_at_ms IS NOT NULL)
    ),
    CHECK (
        (poll_owner IS NULL AND poll_lease_expires_at_ms IS NULL
            AND poll_claimed_at_ms IS NULL)
        OR
        (poll_owner IS NOT NULL AND poll_lease_epoch > 0
            AND poll_lease_expires_at_ms IS NOT NULL
            AND poll_claimed_at_ms IS NOT NULL AND state = 'provider_waiting')
    )
);

CREATE INDEX provider_remote_tasks_poll_claim_idx
    ON provider_remote_tasks (
        provider_id, provider_account_id, next_poll_at_ms, submission_id
    )
    WHERE state = 'provider_waiting';

CREATE TABLE provider_task_observations (
    observation_id UUID PRIMARY KEY,
    submission_id UUID NOT NULL,
    executor_execution_id UUID NOT NULL,
    event_identity TEXT NOT NULL CHECK (
        char_length(event_identity) BETWEEN 1 AND 255
        AND event_identity ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
        AND event_identity NOT LIKE '%://%'
    ),
    source TEXT NOT NULL CHECK (
        source IN ('submit_attach', 'poll', 'cancel', 'verified_callback')
    ),
    observed_state TEXT NOT NULL CHECK (
        observed_state IN (
            'provider_waiting', 'artifact_ready', 'failed', 'canceled', 'uncertain'
        )
    ),
    artifact_ref TEXT CHECK (
        artifact_ref IS NULL
        OR (
            char_length(artifact_ref) BETWEEN 1 AND 512
            AND artifact_ref ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]*$'
            AND artifact_ref NOT LIKE '%://%'
        )
    ),
    error_code TEXT CHECK (
        error_code IS NULL OR error_code ~ '^[A-Za-z0-9_.-]{1,128}$'
    ),
    effect_certainty TEXT NOT NULL CHECK (
        effect_certainty IN ('not_applicable', 'confirmed_no_effect', 'unknown_remote_effect')
    ),
    next_poll_at_ms BIGINT,
    poll_owner TEXT,
    poll_lease_epoch BIGINT,
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    observed_at_ms BIGINT NOT NULL,
    FOREIGN KEY (submission_id, executor_execution_id)
        REFERENCES provider_remote_tasks(submission_id, executor_execution_id)
        ON DELETE RESTRICT,
    UNIQUE (submission_id, event_identity),
    UNIQUE (observation_id, submission_id),
    UNIQUE (observation_id, executor_execution_id, submission_id),
    CHECK (
        (source IN ('poll', 'cancel')
            AND poll_owner IS NOT NULL
            AND char_length(poll_owner) BETWEEN 1 AND 255
            AND poll_owner !~ '[[:cntrl:]]'
            AND poll_lease_epoch IS NOT NULL AND poll_lease_epoch > 0)
        OR
        (source IN ('submit_attach', 'verified_callback')
            AND poll_owner IS NULL AND poll_lease_epoch IS NULL)
    ),
    CHECK (
        (observed_state = 'provider_waiting'
            AND artifact_ref IS NULL AND error_code IS NULL
            AND effect_certainty = 'not_applicable'
            AND next_poll_at_ms BETWEEN observed_at_ms AND observed_at_ms + 86400000)
        OR
        (observed_state = 'artifact_ready'
            AND artifact_ref IS NOT NULL AND error_code IS NULL
            AND effect_certainty = 'not_applicable' AND next_poll_at_ms IS NULL)
        OR
        (observed_state = 'failed'
            AND artifact_ref IS NULL AND error_code IS NOT NULL
            AND effect_certainty = 'not_applicable' AND next_poll_at_ms IS NULL)
        OR
        (observed_state = 'canceled'
            AND artifact_ref IS NULL AND error_code IS NOT NULL
            AND effect_certainty = 'confirmed_no_effect' AND next_poll_at_ms IS NULL)
        OR
        (observed_state = 'uncertain'
            AND artifact_ref IS NULL AND error_code IS NOT NULL
            AND effect_certainty = 'unknown_remote_effect' AND next_poll_at_ms IS NULL)
    ),
    CHECK (
        source <> 'verified_callback'
        OR observed_state = 'provider_waiting'
    ),
    CHECK (
        source <> 'submit_attach'
        OR observed_state = 'provider_waiting'
    )
);

ALTER TABLE executor_resolution_decisions
    DROP CONSTRAINT executor_resolution_decisions_source_check,
    DROP CONSTRAINT executor_resolution_decisions_check2,
    ADD COLUMN provider_task_observation_id UUID,
    ADD CONSTRAINT executor_resolution_decisions_source_check CHECK (
        source IN (
            'active_runner_observation',
            'executor_lease_expired',
            'executor_start_abandoned',
            'remote_provider_observation'
        )
    ),
    ADD CONSTRAINT executor_resolution_decisions_evidence_check CHECK (
        (source = 'active_runner_observation'
            AND observation_id IS NOT NULL
            AND provider_task_observation_id IS NULL
            AND resolved_state IN ('succeeded', 'failed', 'uncertain'))
        OR
        (source = 'executor_lease_expired'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NULL
            AND resolved_state = 'uncertain'
            AND error_code = 'executor_lease_expired')
        OR
        (source = 'executor_start_abandoned'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NULL
            AND resolved_state = 'canceled'
            AND error_code = 'executor_start_abandoned')
        OR
        (source = 'remote_provider_observation'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NOT NULL)
    ),
    ADD CONSTRAINT executor_resolution_decisions_provider_observation_fk
        FOREIGN KEY (
            provider_task_observation_id, executor_execution_id, submission_id
        ) REFERENCES provider_task_observations (
            observation_id, executor_execution_id, submission_id
        ) ON DELETE RESTRICT;

ALTER TABLE executor_capacity_allocations
    DROP CONSTRAINT executor_capacity_allocations_release_reason_check,
    ADD CONSTRAINT executor_capacity_allocations_release_reason_check CHECK (
        release_reason IS NULL
        OR release_reason IN (
            'terminal_evidence',
            'executor_start_abandoned',
            'remote_provider_observation'
        )
    );

ALTER TABLE provider_remote_tasks
    ADD CONSTRAINT provider_remote_task_state_observation_fk FOREIGN KEY (
        state_observation_id, submission_id
    ) REFERENCES provider_task_observations(observation_id, submission_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT provider_remote_task_wakeup_observation_fk FOREIGN KEY (
        last_wakeup_observation_id, submission_id
    ) REFERENCES provider_task_observations(observation_id, submission_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

CREATE OR REPLACE FUNCTION validate_executor_resolution_decision() RETURNS TRIGGER AS $$
DECLARE
    executor_state TEXT;
    executor_owner_value TEXT;
    executor_epoch BIGINT;
    executor_expiry BIGINT;
    launch_owner_value TEXT;
    launch_epoch BIGINT;
    submission_state TEXT;
    work_state TEXT;
    remote_state TEXT;
    remote_observation UUID;
    now_ms BIGINT;
BEGIN
    SELECT e.state, e.executor_owner, e.lease_epoch, e.lease_expires_at_ms,
           e.launch_owner, e.launch_lease_epoch, s.state, w.state,
           floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
    INTO executor_state, executor_owner_value, executor_epoch, executor_expiry,
         launch_owner_value, launch_epoch, submission_state, work_state, now_ms
    FROM executor_executions e
    JOIN provider_submissions s
      ON s.executor_execution_id = e.executor_execution_id
     AND s.submission_id = e.submission_id
    JOIN work_items w ON w.work_item_id = s.work_item_id AND w.job_id = s.job_id
    WHERE e.executor_execution_id = NEW.executor_execution_id
      AND e.submission_id = NEW.submission_id
    FOR UPDATE OF e, s;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'resolution decision does not name a durable execution';
    END IF;
    IF NEW.source = 'active_runner_observation' THEN
        IF executor_state <> 'running' OR submission_state <> 'running'
           OR executor_owner_value IS DISTINCT FROM launch_owner_value
           OR executor_epoch IS DISTINCT FROM launch_epoch
           OR executor_expiry IS NULL OR executor_expiry <= now_ms THEN
            RAISE EXCEPTION 'active resolution requires the live launch fence';
        END IF;
    ELSIF NEW.source = 'executor_lease_expired' THEN
        IF executor_state <> 'running' OR submission_state <> 'running'
           OR executor_expiry IS NULL OR executor_expiry > now_ms THEN
            RAISE EXCEPTION 'expiry resolution requires an expired running execution';
        END IF;
    ELSIF NEW.source = 'executor_start_abandoned' THEN
        IF executor_state <> 'leased' OR submission_state <> 'prepared'
           OR launch_owner_value IS NOT NULL OR launch_epoch IS NOT NULL
           OR executor_expiry IS NULL OR executor_expiry > now_ms
           OR work_state NOT IN ('succeeded', 'failed', 'uncertain') THEN
            RAISE EXCEPTION 'abandoned resolution requires expired unstarted terminal work';
        END IF;
    ELSIF NEW.source = 'remote_provider_observation' THEN
        SELECT task.state, task.state_observation_id
        INTO remote_state, remote_observation
        FROM provider_remote_tasks task
        WHERE task.executor_execution_id = NEW.executor_execution_id
          AND task.submission_id = NEW.submission_id
        FOR UPDATE;
        IF executor_state <> 'provider_waiting'
           OR submission_state <> 'provider_waiting'
           OR executor_owner_value IS NOT NULL
           OR executor_expiry IS NOT NULL
           OR remote_observation IS DISTINCT FROM NEW.provider_task_observation_id
           OR NOT (
                (remote_state = 'artifact_ready' AND NEW.resolved_state = 'succeeded')
                OR (remote_state = 'failed' AND NEW.resolved_state = 'failed')
                OR (remote_state = 'uncertain' AND NEW.resolved_state = 'uncertain')
                OR (remote_state = 'canceled' AND NEW.resolved_state = 'canceled')
           ) THEN
            RAISE EXCEPTION
                'remote resolution requires matching terminal provider evidence';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_executor_capacity_allocation_transition()
RETURNS TRIGGER AS $$
DECLARE
    decision_source TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'executor capacity allocations are durable';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'held' OR NEW.released_at_ms IS NOT NULL
           OR NEW.release_reason IS NOT NULL OR NEW.release_decision_id IS NOT NULL
           OR NEW.released_state IS NOT NULL THEN
            RAISE EXCEPTION 'executor capacity allocation must be inserted held';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.allocation_id IS DISTINCT FROM OLD.allocation_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.execution_profile_id IS DISTINCT FROM OLD.execution_profile_id
       OR NEW.resource_policy_id IS DISTINCT FROM OLD.resource_policy_id
       OR NEW.resource_policy_revision IS DISTINCT FROM OLD.resource_policy_revision
       OR NEW.acquired_at_ms IS DISTINCT FROM OLD.acquired_at_ms THEN
        RAISE EXCEPTION 'executor capacity allocation identity is immutable';
    END IF;
    IF OLD.state = 'released' THEN
        RAISE EXCEPTION 'released executor capacity allocation is immutable';
    END IF;
    IF NEW.last_heartbeat_at_ms < OLD.last_heartbeat_at_ms THEN
        RAISE EXCEPTION 'executor capacity heartbeat cannot move backwards';
    END IF;
    IF NOT (
        (OLD.state = 'held' AND NEW.state = 'held'
            AND NEW.released_at_ms IS NULL AND NEW.release_reason IS NULL
            AND NEW.release_decision_id IS NULL AND NEW.released_state IS NULL)
        OR
        (OLD.state = 'held' AND NEW.state = 'released'
            AND NEW.released_at_ms IS NOT NULL AND NEW.release_reason IS NOT NULL
            AND NEW.release_decision_id IS NOT NULL AND NEW.released_state IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'invalid executor capacity allocation transition';
    END IF;
    IF NEW.state = 'released' THEN
        SELECT source INTO decision_source
        FROM executor_resolution_decisions
        WHERE decision_id = NEW.release_decision_id
          AND executor_execution_id = NEW.executor_execution_id
          AND submission_id = NEW.submission_id
          AND resolved_state = NEW.released_state;
        IF NEW.release_reason = 'terminal_evidence' THEN
            IF decision_source IS NULL OR NOT EXISTS (
                SELECT 1 FROM executor_runner_observations observation
                WHERE observation.executor_execution_id = NEW.executor_execution_id
                  AND observation.submission_id = NEW.submission_id
            ) THEN
                RAISE EXCEPTION 'terminal capacity release requires durable runner evidence';
            END IF;
        ELSIF NEW.release_reason = 'executor_start_abandoned' THEN
            IF decision_source IS DISTINCT FROM 'executor_start_abandoned'
               OR NEW.released_state IS DISTINCT FROM 'canceled' THEN
                RAISE EXCEPTION 'abandoned capacity release requires its fenced decision';
            END IF;
        ELSIF NEW.release_reason = 'remote_provider_observation' THEN
            IF decision_source IS DISTINCT FROM 'remote_provider_observation'
               OR NOT EXISTS (
                    SELECT 1
                    FROM executor_resolution_decisions decision
                    JOIN provider_task_observations observation
                      ON observation.observation_id =
                         decision.provider_task_observation_id
                     AND observation.executor_execution_id =
                         decision.executor_execution_id
                     AND observation.submission_id = decision.submission_id
                    WHERE decision.decision_id = NEW.release_decision_id
               ) THEN
                RAISE EXCEPTION
                    'remote capacity release requires durable provider evidence';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_provider_remote_task_insert() RETURNS TRIGGER AS $$
DECLARE
    execution_state TEXT;
    execution_owner TEXT;
    execution_epoch BIGINT;
    execution_expiry BIGINT;
    execution_launch_owner TEXT;
    execution_launch_epoch BIGINT;
    submission_state TEXT;
    submission_provider TEXT;
    submission_account UUID;
    capacity_state TEXT;
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    SELECT execution.state, execution.executor_owner, execution.lease_epoch,
           execution.lease_expires_at_ms, execution.launch_owner,
           execution.launch_lease_epoch, submission.state,
           submission.provider_id, submission.provider_account_id,
           allocation.state
    INTO execution_state, execution_owner, execution_epoch,
         execution_expiry, execution_launch_owner, execution_launch_epoch,
         submission_state, submission_provider, submission_account, capacity_state
    FROM executor_executions execution
    JOIN provider_submissions submission
      ON submission.executor_execution_id = execution.executor_execution_id
     AND submission.submission_id = execution.submission_id
    JOIN executor_capacity_allocations allocation
      ON allocation.executor_execution_id = execution.executor_execution_id
     AND allocation.submission_id = execution.submission_id
    WHERE execution.executor_execution_id = NEW.executor_execution_id
      AND execution.submission_id = NEW.submission_id
    FOR UPDATE OF execution, submission, allocation;

    IF NOT FOUND OR execution_state <> 'running' OR submission_state <> 'running'
       OR execution_owner IS DISTINCT FROM NEW.submit_owner
       OR execution_epoch IS DISTINCT FROM NEW.submit_lease_epoch
       OR execution_expiry IS NULL OR execution_expiry <= now_ms
       OR execution_launch_owner IS DISTINCT FROM NEW.submit_owner
       OR execution_launch_epoch IS DISTINCT FROM NEW.submit_lease_epoch
       OR submission_provider IS DISTINCT FROM NEW.provider_id
       OR submission_account IS DISTINCT FROM NEW.provider_account_id
       OR capacity_state IS DISTINCT FROM 'held' THEN
        RAISE EXCEPTION 'remote task attach requires a live executor fence and held capacity';
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
       ) THEN
        RAISE EXCEPTION 'remote task must be inserted in its initial waiting shape';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_remote_task_insert_guard
    BEFORE INSERT ON provider_remote_tasks
    FOR EACH ROW EXECUTE FUNCTION validate_provider_remote_task_insert();

CREATE FUNCTION validate_provider_task_observation() RETURNS TRIGGER AS $$
DECLARE
    task provider_remote_tasks%ROWTYPE;
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    SELECT * INTO task
    FROM provider_remote_tasks
    WHERE submission_id = NEW.submission_id
      AND executor_execution_id = NEW.executor_execution_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'provider task observation has no durable remote task';
    END IF;
    IF NEW.source = 'submit_attach' THEN
        IF NEW.observation_id IS DISTINCT FROM task.state_observation_id
           OR task.state <> 'provider_waiting'
           OR EXISTS (
               SELECT 1 FROM provider_task_observations
               WHERE submission_id = NEW.submission_id
                 AND observation_id <> NEW.observation_id
           ) THEN
            RAISE EXCEPTION 'submit attach observation must initialize the remote task once';
        END IF;
    ELSIF NEW.source IN ('poll', 'cancel') THEN
        IF task.state <> 'provider_waiting'
           OR task.poll_owner IS DISTINCT FROM NEW.poll_owner
           OR task.poll_lease_epoch IS DISTINCT FROM NEW.poll_lease_epoch
           OR task.poll_lease_expires_at_ms IS NULL
           OR task.poll_lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'provider task observation requires the live poll fence';
        END IF;
        IF NEW.source = 'cancel' AND NOT task.cancel_requested THEN
            RAISE EXCEPTION 'cancel observation requires a durable cancel request';
        END IF;
        IF NEW.observed_state = 'canceled' AND NOT task.cancel_requested THEN
            RAISE EXCEPTION 'canceled observation requires a durable cancel request';
        END IF;
    ELSIF NEW.source = 'verified_callback' THEN
        IF NEW.observed_state <> 'provider_waiting'
           OR NEW.next_poll_at_ms IS NULL THEN
            RAISE EXCEPTION 'verified callback may only record a poll wakeup';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_task_observation_guard
    AFTER INSERT ON provider_task_observations
    FOR EACH ROW EXECUTE FUNCTION validate_provider_task_observation();

CREATE FUNCTION enforce_provider_remote_task_update() RETURNS TRIGGER AS $$
DECLARE
    observation provider_task_observations%ROWTYPE;
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.provider_account_id IS DISTINCT FROM OLD.provider_account_id
       OR NEW.remote_operation_id IS DISTINCT FROM OLD.remote_operation_id
       OR NEW.provider_request_id IS DISTINCT FROM OLD.provider_request_id
       OR NEW.submit_owner IS DISTINCT FROM OLD.submit_owner
       OR NEW.submit_lease_epoch IS DISTINCT FROM OLD.submit_lease_epoch
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms THEN
        RAISE EXCEPTION 'provider remote task identity is immutable';
    END IF;
    IF OLD.state IN ('artifact_ready', 'failed', 'canceled', 'uncertain') THEN
        RAISE EXCEPTION 'terminal provider remote task is immutable';
    END IF;

    IF NOT OLD.cancel_requested AND NEW.cancel_requested THEN
        IF NEW.cancel_requested_at_ms IS NULL
           OR NEW.cancel_requested_at_ms < OLD.updated_at_ms
           OR NEW.next_poll_at_ms > OLD.next_poll_at_ms
           OR to_jsonb(NEW)
                - 'cancel_requested' - 'cancel_requested_at_ms'
                - 'next_poll_at_ms' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'cancel_requested' - 'cancel_requested_at_ms'
                - 'next_poll_at_ms' - 'updated_at_ms' THEN
            RAISE EXCEPTION 'invalid provider cancel request transition';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.last_wakeup_observation_id IS DISTINCT FROM OLD.last_wakeup_observation_id THEN
        SELECT * INTO observation
        FROM provider_task_observations
        WHERE observation_id = NEW.last_wakeup_observation_id
          AND submission_id = NEW.submission_id
          AND source = 'verified_callback';
        IF NOT FOUND OR NEW.state <> 'provider_waiting'
           OR NEW.next_poll_at_ms <> LEAST(OLD.next_poll_at_ms, observation.next_poll_at_ms)
           OR to_jsonb(NEW)
                - 'last_wakeup_observation_id' - 'next_poll_at_ms' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'last_wakeup_observation_id' - 'next_poll_at_ms' - 'updated_at_ms' THEN
            RAISE EXCEPTION 'verified callback may only advance the next poll';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.poll_owner IS NULL AND NEW.poll_owner IS NOT NULL THEN
        IF OLD.next_poll_at_ms > now_ms
           OR NEW.poll_lease_epoch <> OLD.poll_lease_epoch + 1
           OR NEW.poll_lease_expires_at_ms <= now_ms
           OR NEW.poll_claimed_at_ms IS NULL
           OR NEW.poll_claimed_at_ms > now_ms
           OR NEW.poll_claimed_at_ms < now_ms - 1000
           OR to_jsonb(NEW)
                - 'poll_owner' - 'poll_lease_epoch' - 'poll_lease_expires_at_ms'
                - 'poll_claimed_at_ms' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'poll_owner' - 'poll_lease_epoch' - 'poll_lease_expires_at_ms'
                - 'poll_claimed_at_ms' - 'updated_at_ms' THEN
            RAISE EXCEPTION 'provider poll claim requires a due unowned task';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.poll_owner IS NOT NULL
       AND NEW.poll_owner IS NOT NULL
       AND (NEW.poll_owner IS DISTINCT FROM OLD.poll_owner
            OR NEW.poll_lease_epoch IS DISTINCT FROM OLD.poll_lease_epoch) THEN
        IF OLD.poll_lease_expires_at_ms > now_ms
           OR NEW.poll_lease_epoch <> OLD.poll_lease_epoch + 1
           OR NEW.poll_lease_expires_at_ms <= now_ms
           OR NEW.poll_claimed_at_ms IS NULL
           OR NEW.poll_claimed_at_ms > now_ms
           OR NEW.poll_claimed_at_ms < now_ms - 1000
           OR to_jsonb(NEW)
                - 'poll_owner' - 'poll_lease_epoch' - 'poll_lease_expires_at_ms'
                - 'poll_claimed_at_ms' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'poll_owner' - 'poll_lease_epoch' - 'poll_lease_expires_at_ms'
                - 'poll_claimed_at_ms' - 'updated_at_ms' THEN
            RAISE EXCEPTION 'provider poll reclaim requires an expired fence';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.poll_owner IS NOT NULL
       AND NEW.poll_owner IS NOT DISTINCT FROM OLD.poll_owner
       AND NEW.poll_lease_epoch IS NOT DISTINCT FROM OLD.poll_lease_epoch
       AND NEW.poll_lease_expires_at_ms IS DISTINCT FROM OLD.poll_lease_expires_at_ms THEN
        IF OLD.poll_lease_expires_at_ms <= now_ms
           OR NEW.poll_lease_expires_at_ms < OLD.poll_lease_expires_at_ms
           OR NEW.poll_claimed_at_ms IS DISTINCT FROM OLD.poll_claimed_at_ms
           OR to_jsonb(NEW) - 'poll_lease_expires_at_ms' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD) - 'poll_lease_expires_at_ms' - 'updated_at_ms' THEN
            RAISE EXCEPTION 'provider poll heartbeat requires the live fence';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.state_observation_id IS DISTINCT FROM OLD.state_observation_id THEN
        SELECT * INTO observation
        FROM provider_task_observations
        WHERE observation_id = NEW.state_observation_id
          AND submission_id = NEW.submission_id
          AND source IN ('poll', 'cancel');
        IF NOT FOUND
           OR OLD.poll_owner IS NULL
           OR observation.poll_owner IS DISTINCT FROM OLD.poll_owner
           OR observation.poll_lease_epoch IS DISTINCT FROM OLD.poll_lease_epoch
           OR NEW.state IS DISTINCT FROM observation.observed_state
           OR NEW.artifact_ref IS DISTINCT FROM observation.artifact_ref
           OR NEW.error_code IS DISTINCT FROM observation.error_code
           OR NEW.effect_certainty IS DISTINCT FROM observation.effect_certainty
           OR NEW.next_poll_at_ms IS DISTINCT FROM observation.next_poll_at_ms
           OR NEW.poll_owner IS NOT NULL OR NEW.poll_lease_expires_at_ms IS NOT NULL
           OR NEW.poll_claimed_at_ms IS NOT NULL
           OR NEW.poll_lease_epoch IS DISTINCT FROM OLD.poll_lease_epoch
           OR NEW.terminal_at_ms IS DISTINCT FROM (
                CASE WHEN observation.observed_state = 'provider_waiting'
                     THEN NULL ELSE observation.observed_at_ms END
              )
           OR to_jsonb(NEW)
                - 'state' - 'artifact_ref' - 'error_code' - 'effect_certainty'
                - 'next_poll_at_ms' - 'poll_owner' - 'poll_lease_expires_at_ms'
                - 'poll_claimed_at_ms' - 'state_observation_id'
                - 'updated_at_ms' - 'terminal_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'state' - 'artifact_ref' - 'error_code' - 'effect_certainty'
                - 'next_poll_at_ms' - 'poll_owner' - 'poll_lease_expires_at_ms'
                - 'poll_claimed_at_ms' - 'state_observation_id'
                - 'updated_at_ms' - 'terminal_at_ms' THEN
            RAISE EXCEPTION 'provider task state requires its live fenced observation';
        END IF;
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'unsupported provider remote task mutation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_remote_task_update_guard
    BEFORE UPDATE ON provider_remote_tasks
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_remote_task_update();

CREATE FUNCTION enforce_provider_remote_task_projection() RETURNS TRIGGER AS $$
DECLARE
    named_submission UUID;
    task_state TEXT;
    execution_state TEXT;
    execution_owner TEXT;
    execution_expiry BIGINT;
    submission_state TEXT;
BEGIN
    named_submission := NEW.submission_id;
    SELECT task.state, execution.state, execution.executor_owner,
           execution.lease_expires_at_ms, submission.state
    INTO task_state, execution_state, execution_owner,
         execution_expiry, submission_state
    FROM executor_executions execution
    JOIN provider_submissions submission
      ON submission.executor_execution_id = execution.executor_execution_id
     AND submission.submission_id = execution.submission_id
    LEFT JOIN provider_remote_tasks task
      ON task.executor_execution_id = execution.executor_execution_id
     AND task.submission_id = execution.submission_id
    WHERE execution.submission_id = named_submission;

    IF task_state IS NULL THEN
        IF execution_state = 'provider_waiting' OR submission_state = 'provider_waiting' THEN
            RAISE EXCEPTION 'remote provider waiting requires one durable task';
        END IF;
        RETURN NULL;
    END IF;
    IF execution_state = 'provider_waiting' OR submission_state = 'provider_waiting' THEN
        IF execution_state <> 'provider_waiting'
           OR submission_state <> 'provider_waiting'
           OR execution_owner IS NOT NULL OR execution_expiry IS NOT NULL THEN
            RAISE EXCEPTION 'remote provider waiting must release executor ownership atomically';
        END IF;
        RETURN NULL;
    END IF;
    IF task_state = 'provider_waiting'
       OR (task_state = 'artifact_ready' AND execution_state <> 'succeeded')
       OR (task_state IN ('failed', 'uncertain', 'canceled')
           AND execution_state IS DISTINCT FROM task_state)
       OR submission_state IS DISTINCT FROM execution_state THEN
        RAISE EXCEPTION 'canonical terminal projection conflicts with remote task evidence';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_remote_task_projection_check
    AFTER INSERT OR UPDATE ON provider_remote_tasks
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_remote_task_projection();

CREATE CONSTRAINT TRIGGER provider_submission_remote_task_projection_check
    AFTER UPDATE ON provider_submissions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_remote_task_projection();

CREATE CONSTRAINT TRIGGER executor_execution_remote_task_projection_check
    AFTER UPDATE ON executor_executions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_remote_task_projection();

CREATE FUNCTION reject_provider_remote_task_delete() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider remote tasks and observations are durable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_remote_tasks_reject_delete
    BEFORE DELETE ON provider_remote_tasks
    FOR EACH ROW EXECUTE FUNCTION reject_provider_remote_task_delete();

CREATE TRIGGER provider_remote_tasks_reject_truncate
    BEFORE TRUNCATE ON provider_remote_tasks
    FOR EACH STATEMENT EXECUTE FUNCTION reject_provider_remote_task_delete();

CREATE TRIGGER provider_submit_intents_reject_delete
    BEFORE DELETE ON provider_remote_submit_intents
    FOR EACH ROW EXECUTE FUNCTION reject_provider_remote_task_delete();

CREATE TRIGGER provider_submit_intents_reject_truncate
    BEFORE TRUNCATE ON provider_remote_submit_intents
    FOR EACH STATEMENT EXECUTE FUNCTION reject_provider_remote_task_delete();

CREATE TRIGGER provider_task_observations_reject_mutation
    BEFORE UPDATE OR DELETE ON provider_task_observations
    FOR EACH ROW EXECUTE FUNCTION reject_provider_remote_task_delete();

CREATE TRIGGER provider_task_observations_reject_truncate
    BEFORE TRUNCATE ON provider_task_observations
    FOR EACH STATEMENT EXECUTE FUNCTION reject_provider_remote_task_delete();
