LOCK TABLE provider_remote_submit_intents, provider_remote_tasks,
    provider_task_observations, executor_resolution_decisions,
    executor_executions, provider_submissions, executor_capacity_allocations
    IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE provider_remote_submit_intents
    ADD COLUMN provider_request_id TEXT CHECK (
        provider_request_id IS NULL
        OR (
            char_length(provider_request_id) BETWEEN 1 AND 255
            AND provider_request_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
            AND provider_request_id NOT LIKE '%://%'
        )
    ),
    ADD COLUMN send_started_at_ms BIGINT,
    ADD COLUMN receipt_event_identity TEXT,
    ADD COLUMN failure_event_identity TEXT,
    ADD COLUMN failure_error_code TEXT;

ALTER TABLE executor_resolution_decisions
    ADD COLUMN provider_submit_intent_id UUID REFERENCES
        provider_remote_submit_intents(submission_id) ON DELETE RESTRICT;

DROP TRIGGER provider_submit_intent_update_guard
    ON provider_remote_submit_intents;

UPDATE provider_remote_submit_intents intent
SET provider_request_id = task.provider_request_id,
    send_started_at_ms = intent.created_at_ms,
    receipt_event_identity = observation.event_identity
FROM provider_remote_tasks task, provider_task_observations observation
WHERE intent.state = 'attached'
  AND task.submission_id = intent.submission_id
  AND task.executor_execution_id = intent.executor_execution_id
  AND observation.submission_id = intent.submission_id
  AND observation.executor_execution_id = intent.executor_execution_id
  AND observation.source = 'submit_attach';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_remote_submit_intents
        WHERE state = 'attached'
          AND (send_started_at_ms IS NULL OR receipt_event_identity IS NULL)
    ) THEN
        RAISE EXCEPTION
            'attached provider submit intent has no durable submit observation';
    END IF;
END;
$$;

ALTER TABLE provider_remote_submit_intents
    DROP CONSTRAINT provider_remote_submit_intents_state_check,
    DROP CONSTRAINT provider_remote_submit_intents_check,
    ADD CONSTRAINT provider_remote_submit_intents_state_check CHECK (
        state IN (
            'reserved', 'sending', 'outcome_unknown', 'operation_known',
            'attached', 'rejected'
        )
    ),
    ADD CONSTRAINT provider_remote_submit_intents_receipt_event_check CHECK (
        receipt_event_identity IS NULL
        OR (
            char_length(receipt_event_identity) BETWEEN 1 AND 255
            AND receipt_event_identity ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
            AND receipt_event_identity NOT LIKE '%://%'
        )
    ),
    ADD CONSTRAINT provider_remote_submit_intents_failure_event_check CHECK (
        failure_event_identity IS NULL
        OR (
            char_length(failure_event_identity) BETWEEN 1 AND 255
            AND failure_event_identity ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
            AND failure_event_identity NOT LIKE '%://%'
        )
    ),
    ADD CONSTRAINT provider_remote_submit_intents_failure_error_check CHECK (
        failure_error_code IS NULL
        OR failure_error_code ~ '^[A-Za-z0-9_.-]{1,128}$'
    ),
    ADD CONSTRAINT provider_remote_submit_intents_time_check CHECK (
        updated_at_ms >= created_at_ms
        AND (send_started_at_ms IS NULL OR send_started_at_ms >= created_at_ms)
    ),
    ADD CONSTRAINT provider_remote_submit_intents_lifecycle_check CHECK (
        (state = 'reserved'
            AND remote_operation_id IS NULL AND provider_request_id IS NULL
            AND send_started_at_ms IS NULL
            AND receipt_event_identity IS NULL
            AND failure_event_identity IS NULL AND failure_error_code IS NULL)
        OR
        (state = 'sending'
            AND remote_operation_id IS NULL AND provider_request_id IS NULL
            AND send_started_at_ms IS NOT NULL
            AND receipt_event_identity IS NULL
            AND failure_event_identity IS NULL AND failure_error_code IS NULL)
        OR
        (state IN ('operation_known', 'attached')
            AND remote_operation_id IS NOT NULL AND send_started_at_ms IS NOT NULL
            AND receipt_event_identity IS NOT NULL
            AND (
                (failure_event_identity IS NULL AND failure_error_code IS NULL)
                OR (failure_event_identity IS NOT NULL AND failure_error_code IS NOT NULL)
            ))
        OR
        (state IN ('outcome_unknown', 'rejected')
            AND remote_operation_id IS NULL AND provider_request_id IS NULL
            AND send_started_at_ms IS NOT NULL
            AND receipt_event_identity IS NULL
            AND failure_event_identity IS NOT NULL AND failure_error_code IS NOT NULL)
    );

CREATE UNIQUE INDEX provider_submit_intents_remote_operation_uidx
    ON provider_remote_submit_intents (
        provider_id, provider_account_id, remote_operation_id
    )
    WHERE remote_operation_id IS NOT NULL;

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
           OR to_jsonb(NEW) - 'state' - 'send_started_at_ms' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD) - 'state' - 'send_started_at_ms' - 'updated_at_ms'
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
        IF NEW.send_started_at_ms IS DISTINCT FROM OLD.send_started_at_ms
           OR to_jsonb(NEW)
                - 'state' - 'remote_operation_id' - 'provider_request_id'
                - 'receipt_event_identity' - 'failure_event_identity'
                - 'failure_error_code' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'state' - 'remote_operation_id' - 'provider_request_id'
                - 'receipt_event_identity' - 'failure_event_identity'
                - 'failure_error_code' - 'updated_at_ms' THEN
            RAISE EXCEPTION 'invalid provider submit outcome transition';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'outcome_unknown' AND NEW.state = 'operation_known' THEN
        IF NEW.send_started_at_ms IS DISTINCT FROM OLD.send_started_at_ms
           OR to_jsonb(NEW)
                - 'state' - 'remote_operation_id' - 'provider_request_id'
                - 'receipt_event_identity' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'state' - 'remote_operation_id' - 'provider_request_id'
                - 'receipt_event_identity' - 'updated_at_ms' THEN
            RAISE EXCEPTION 'invalid provider submit reconciliation transition';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'operation_known' AND NEW.state = 'attached' THEN
        IF to_jsonb(NEW) - 'state' - 'updated_at_ms'
           IS DISTINCT FROM to_jsonb(OLD) - 'state' - 'updated_at_ms' THEN
            RAISE EXCEPTION 'provider submit attach cannot rewrite its receipt';
        END IF;
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'invalid provider submit intent transition';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submit_intent_update_guard
    BEFORE UPDATE ON provider_remote_submit_intents
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submit_intent_update();

ALTER TABLE executor_resolution_decisions
    DROP CONSTRAINT executor_resolution_decisions_source_check,
    DROP CONSTRAINT executor_resolution_decisions_evidence_check,
    ADD CONSTRAINT executor_resolution_decisions_source_check CHECK (
        source IN (
            'active_runner_observation', 'executor_lease_expired',
            'executor_start_abandoned', 'remote_provider_observation',
            'remote_submit_outcome'
        )
    ),
    ADD CONSTRAINT executor_resolution_decisions_evidence_check CHECK (
        (source = 'active_runner_observation'
            AND observation_id IS NOT NULL
            AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NULL
            AND resolved_state IN ('succeeded', 'failed', 'uncertain'))
        OR
        (source = 'executor_lease_expired'
            AND observation_id IS NULL AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NULL
            AND resolved_state = 'uncertain'
            AND error_code = 'executor_lease_expired')
        OR
        (source = 'executor_start_abandoned'
            AND observation_id IS NULL AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NULL
            AND resolved_state = 'canceled'
            AND error_code = 'executor_start_abandoned')
        OR
        (source = 'remote_provider_observation'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NOT NULL
            AND provider_submit_intent_id IS NULL)
        OR
        (source = 'remote_submit_outcome'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NOT NULL
            AND resolved_state = 'failed')
    );

ALTER TABLE executor_capacity_allocations
    DROP CONSTRAINT executor_capacity_allocations_release_reason_check,
    ADD CONSTRAINT executor_capacity_allocations_release_reason_check CHECK (
        release_reason IS NULL
        OR release_reason IN (
            'terminal_evidence', 'executor_start_abandoned',
            'remote_provider_observation', 'remote_submit_outcome'
        )
    );

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
    submit_state TEXT;
    submit_owner_value TEXT;
    submit_epoch BIGINT;
    submit_error TEXT;
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
           OR executor_expiry IS NULL OR executor_expiry <= now_ms
           OR EXISTS (
                SELECT 1
                FROM provider_remote_submit_intents intent
                WHERE intent.executor_execution_id = NEW.executor_execution_id
                  AND intent.submission_id = NEW.submission_id
                  AND intent.state IN (
                    'sending', 'outcome_unknown', 'operation_known'
                  )
           ) THEN
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
    ELSIF NEW.source = 'remote_submit_outcome' THEN
        SELECT intent.state, intent.submit_owner, intent.submit_lease_epoch,
               intent.failure_error_code
        INTO submit_state, submit_owner_value, submit_epoch, submit_error
        FROM provider_remote_submit_intents intent
        WHERE intent.submission_id = NEW.provider_submit_intent_id
          AND intent.executor_execution_id = NEW.executor_execution_id
          AND intent.submission_id = NEW.submission_id
        FOR UPDATE;
        IF executor_state <> 'running' OR submission_state <> 'running'
           OR executor_owner_value IS DISTINCT FROM submit_owner_value
           OR executor_epoch IS DISTINCT FROM submit_epoch
           OR launch_owner_value IS DISTINCT FROM submit_owner_value
           OR launch_epoch IS DISTINCT FROM submit_epoch
           OR submit_error IS DISTINCT FROM NEW.error_code
           OR submit_state <> 'rejected' OR NEW.resolved_state <> 'failed' THEN
            RAISE EXCEPTION
                'submit resolution requires matching durable provider outcome';
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
                      ON observation.observation_id = decision.provider_task_observation_id
                     AND observation.executor_execution_id = decision.executor_execution_id
                     AND observation.submission_id = decision.submission_id
                    WHERE decision.decision_id = NEW.release_decision_id
               ) THEN
                RAISE EXCEPTION
                    'remote capacity release requires durable provider evidence';
            END IF;
        ELSIF NEW.release_reason = 'remote_submit_outcome' THEN
            IF decision_source IS DISTINCT FROM 'remote_submit_outcome'
               OR NOT EXISTS (
                    SELECT 1
                    FROM executor_resolution_decisions decision
                    JOIN provider_remote_submit_intents intent
                      ON intent.submission_id = decision.provider_submit_intent_id
                     AND intent.executor_execution_id = decision.executor_execution_id
                    WHERE decision.decision_id = NEW.release_decision_id
                      AND intent.state = 'rejected'
                      AND decision.resolved_state = 'failed'
               ) THEN
                RAISE EXCEPTION
                    'submit capacity release requires durable provider outcome';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_provider_remote_task_insert() RETURNS TRIGGER AS $$
DECLARE
    execution_state TEXT;
    execution_owner TEXT;
    execution_epoch BIGINT;
    execution_launch_owner TEXT;
    execution_launch_epoch BIGINT;
    submission_state TEXT;
    submission_provider TEXT;
    submission_account UUID;
    capacity_state TEXT;
BEGIN
    SELECT execution.state, execution.executor_owner, execution.lease_epoch,
           execution.launch_owner, execution.launch_lease_epoch, submission.state,
           submission.provider_id, submission.provider_account_id, allocation.state
    INTO execution_state, execution_owner, execution_epoch,
         execution_launch_owner, execution_launch_epoch, submission_state,
         submission_provider, submission_account, capacity_state
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
       OR execution_launch_owner IS DISTINCT FROM NEW.submit_owner
       OR execution_launch_epoch IS DISTINCT FROM NEW.submit_lease_epoch
       OR submission_provider IS DISTINCT FROM NEW.provider_id
       OR submission_account IS DISTINCT FROM NEW.provider_account_id
       OR capacity_state IS DISTINCT FROM 'held' THEN
        RAISE EXCEPTION
            'remote task attach requires the frozen executor identity and held capacity';
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

CREATE OR REPLACE FUNCTION enforce_executor_lease_updates() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
    durable_remote_handoff BOOLEAN;
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
        SELECT EXISTS (
            SELECT 1
            FROM provider_remote_submit_intents intent
            JOIN provider_remote_tasks task
              ON task.submission_id = intent.submission_id
             AND task.executor_execution_id = intent.executor_execution_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = intent.executor_execution_id
             AND allocation.submission_id = intent.submission_id
            WHERE intent.executor_execution_id = OLD.executor_execution_id
              AND intent.submission_id = OLD.submission_id
              AND intent.state = 'attached'
              AND intent.submit_owner = OLD.launch_owner
              AND intent.submit_lease_epoch = OLD.launch_lease_epoch
              AND task.remote_operation_id = intent.remote_operation_id
              AND allocation.state = 'held'
        ) INTO durable_remote_handoff;
        IF (OLD.lease_expires_at_ms <= now_ms AND NOT durable_remote_handoff)
           OR NEW.executor_owner IS NOT NULL
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.lease_expires_at_ms IS NOT NULL
           OR NEW.leased_at_ms IS DISTINCT FROM OLD.leased_at_ms
           OR NEW.started_at_ms IS DISTINCT FROM OLD.started_at_ms
           OR NEW.finished_at_ms IS NOT NULL OR NEW.error_code IS NOT NULL THEN
            RAISE EXCEPTION
                'remote provider handoff requires a live fence or durable submit receipt';
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

CREATE FUNCTION enforce_provider_submit_intent_projection() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.state = 'attached' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM provider_remote_tasks task
            JOIN provider_task_observations observation
              ON observation.observation_id = task.state_observation_id
             AND observation.executor_execution_id = task.executor_execution_id
             AND observation.submission_id = task.submission_id
            JOIN executor_executions execution
              ON execution.executor_execution_id = task.executor_execution_id
             AND execution.submission_id = task.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = task.executor_execution_id
             AND submission.submission_id = task.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = task.executor_execution_id
             AND allocation.submission_id = task.submission_id
            WHERE task.executor_execution_id = NEW.executor_execution_id
              AND task.submission_id = NEW.submission_id
              AND task.provider_id = NEW.provider_id
              AND task.provider_account_id = NEW.provider_account_id
              AND task.submit_owner = NEW.submit_owner
              AND task.submit_lease_epoch = NEW.submit_lease_epoch
              AND task.remote_operation_id = NEW.remote_operation_id
              AND task.provider_request_id IS NOT DISTINCT FROM NEW.provider_request_id
              AND task.state = 'provider_waiting'
              AND observation.source = 'submit_attach'
              AND observation.observed_state = 'provider_waiting'
              AND execution.state = 'provider_waiting'
              AND execution.executor_owner IS NULL
              AND execution.lease_epoch = NEW.submit_lease_epoch
              AND execution.lease_expires_at_ms IS NULL
              AND execution.launch_owner = NEW.submit_owner
              AND execution.launch_lease_epoch = NEW.submit_lease_epoch
              AND submission.state = 'provider_waiting'
              AND allocation.state = 'held'
        ) THEN
            RAISE EXCEPTION
                'attached provider submit intent requires its complete remote handoff';
        END IF;
    ELSIF NEW.state = 'rejected' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM executor_resolution_decisions decision
            JOIN executor_executions execution
              ON execution.executor_execution_id = decision.executor_execution_id
             AND execution.submission_id = decision.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = decision.executor_execution_id
             AND submission.submission_id = decision.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = decision.executor_execution_id
             AND allocation.submission_id = decision.submission_id
            WHERE decision.executor_execution_id = NEW.executor_execution_id
              AND decision.submission_id = NEW.submission_id
              AND decision.provider_submit_intent_id = NEW.submission_id
              AND decision.source = 'remote_submit_outcome'
              AND decision.resolved_state = 'failed'
              AND decision.error_code = NEW.failure_error_code
              AND execution.state = 'failed'
              AND execution.executor_owner IS NULL
              AND execution.lease_expires_at_ms IS NULL
              AND execution.resolution_decision_id = decision.decision_id
              AND submission.state = 'failed'
              AND submission.resolution_decision_id = decision.decision_id
              AND allocation.state = 'released'
              AND allocation.release_reason = 'remote_submit_outcome'
              AND allocation.release_decision_id = decision.decision_id
              AND allocation.released_state = 'failed'
        ) THEN
            RAISE EXCEPTION
                'rejected provider submit intent requires its complete terminal projection';
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_submit_intent_projection_check
    AFTER UPDATE ON provider_remote_submit_intents
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submit_intent_projection();
