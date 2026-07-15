LOCK TABLE executor_executions, provider_submissions,
    executor_capacity_allocations, provider_remote_submit_intents,
    provider_submit_recoveries, executor_resolution_decisions
    IN ACCESS EXCLUSIVE MODE;

ALTER TABLE provider_remote_submit_intents
    DROP CONSTRAINT provider_remote_submit_intents_state_check,
    DROP CONSTRAINT provider_remote_submit_intents_lifecycle_check,
    ADD CONSTRAINT provider_remote_submit_intents_state_check CHECK (
        state IN (
            'reserved', 'sending', 'outcome_unknown', 'operation_known',
            'attached', 'rejected', 'deadline_quarantined'
        )
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
        OR
        (state = 'deadline_quarantined'
            AND send_started_at_ms IS NOT NULL
            AND (
                (remote_operation_id IS NULL AND provider_request_id IS NULL
                    AND receipt_event_identity IS NULL)
                OR (remote_operation_id IS NOT NULL AND receipt_event_identity IS NOT NULL)
            )
            AND (
                (failure_event_identity IS NULL AND failure_error_code IS NULL)
                OR (failure_event_identity IS NOT NULL AND failure_error_code IS NOT NULL)
            ))
    );

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

    IF OLD.state IN ('sending', 'outcome_unknown', 'operation_known')
       AND NEW.state = 'deadline_quarantined' THEN
        IF to_jsonb(NEW) - 'state' - 'updated_at_ms'
           IS DISTINCT FROM to_jsonb(OLD) - 'state' - 'updated_at_ms' THEN
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
           OR to_jsonb(NEW)
                - 'remote_operation_id' - 'provider_request_id'
                - 'receipt_event_identity' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'remote_operation_id' - 'provider_request_id'
                - 'receipt_event_identity' - 'updated_at_ms' THEN
            RAISE EXCEPTION 'invalid late submit receipt';
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

ALTER TABLE executor_resolution_decisions
    DROP CONSTRAINT executor_resolution_decisions_source_check,
    DROP CONSTRAINT executor_resolution_decisions_evidence_check,
    ADD CONSTRAINT executor_resolution_decisions_source_check CHECK (
        source IN (
            'active_runner_observation', 'executor_lease_expired',
            'executor_start_abandoned', 'remote_provider_observation',
            'remote_submit_outcome', 'remote_submit_deadline'
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
        OR
        (source = 'remote_submit_deadline'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NOT NULL
            AND resolved_state = 'uncertain'
            AND error_code = 'provider_submit_deadline')
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
    recovery_state TEXT;
    provider_deadline BIGINT;
    capacity_state TEXT;
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
                    'sending', 'outcome_unknown', 'operation_known',
                    'deadline_quarantined'
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
    ELSIF NEW.source = 'remote_submit_deadline' THEN
        SELECT allocation.state
        INTO capacity_state
        FROM executor_capacity_allocations allocation
        WHERE allocation.executor_execution_id = NEW.executor_execution_id
          AND allocation.submission_id = NEW.submission_id
        FOR UPDATE;
        SELECT intent.state, intent.submit_owner, intent.submit_lease_epoch,
               recovery.state, recovery.provider_deadline_at_ms
        INTO submit_state, submit_owner_value, submit_epoch,
             recovery_state, provider_deadline
        FROM provider_remote_submit_intents intent
        JOIN provider_submit_recoveries recovery
          ON recovery.submission_id = intent.submission_id
         AND recovery.executor_execution_id = intent.executor_execution_id
        WHERE intent.submission_id = NEW.provider_submit_intent_id
          AND intent.executor_execution_id = NEW.executor_execution_id
          AND intent.submission_id = NEW.submission_id
        FOR UPDATE OF intent, recovery;
        IF executor_state <> 'running' OR submission_state <> 'running'
           OR executor_owner_value IS DISTINCT FROM submit_owner_value
           OR executor_epoch IS DISTINCT FROM submit_epoch
           OR launch_owner_value IS DISTINCT FROM submit_owner_value
           OR launch_epoch IS DISTINCT FROM submit_epoch
           OR submit_state <> 'deadline_quarantined'
           OR recovery_state <> 'closed'
           OR capacity_state <> 'held'
           OR provider_deadline > NEW.decided_at_ms
           OR NEW.decided_at_ms > now_ms
           OR EXISTS (
                SELECT 1 FROM provider_remote_tasks task
                WHERE task.submission_id = NEW.submission_id
           ) THEN
            RAISE EXCEPTION
                'submit deadline requires due unknown-effect evidence and held capacity';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE INDEX provider_submit_recoveries_deadline_idx
    ON provider_submit_recoveries (
        provider_account_id, provider_deadline_at_ms, submission_id
    )
    WHERE state = 'active';

CREATE OR REPLACE FUNCTION enforce_provider_submit_recovery_projection() RETURNS TRIGGER AS $$
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
    ELSIF intent_state IN ('attached', 'rejected', 'deadline_quarantined')
          AND recovery_state IS DISTINCT FROM 'closed' THEN
        RAISE EXCEPTION 'terminal provider submit requires a closed recovery lease';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_provider_submit_intent_projection() RETURNS TRIGGER AS $$
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
    ELSIF NEW.state = 'deadline_quarantined' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM provider_submit_recoveries recovery
            JOIN executor_resolution_decisions decision
              ON decision.provider_submit_intent_id = recovery.submission_id
             AND decision.executor_execution_id = recovery.executor_execution_id
             AND decision.submission_id = recovery.submission_id
            JOIN executor_executions execution
              ON execution.executor_execution_id = decision.executor_execution_id
             AND execution.submission_id = decision.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = decision.executor_execution_id
             AND submission.submission_id = decision.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = decision.executor_execution_id
             AND allocation.submission_id = decision.submission_id
            WHERE recovery.submission_id = NEW.submission_id
              AND recovery.state = 'closed'
              AND decision.source = 'remote_submit_deadline'
              AND decision.resolved_state = 'uncertain'
              AND decision.error_code = 'provider_submit_deadline'
              AND decision.decided_at_ms >= recovery.provider_deadline_at_ms
              AND execution.state = 'uncertain'
              AND execution.executor_owner IS NULL
              AND execution.lease_expires_at_ms IS NULL
              AND execution.resolution_decision_id = decision.decision_id
              AND submission.state = 'uncertain'
              AND submission.resolution_decision_id = decision.decision_id
              AND allocation.state = 'held'
        ) THEN
            RAISE EXCEPTION
                'deadline-quarantined submit requires terminal uncertainty and held capacity';
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION enforce_provider_submit_deadline_capacity_hold() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.state = 'held' AND NEW.state = 'released'
       AND EXISTS (
            SELECT 1
            FROM executor_executions execution
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
             AND decision.executor_execution_id = execution.executor_execution_id
             AND decision.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = NEW.executor_execution_id
              AND execution.submission_id = NEW.submission_id
              AND decision.source = 'remote_submit_deadline'
       ) THEN
        RAISE EXCEPTION
            'deadline-quarantined provider capacity requires separate release evidence';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_capacity_submit_deadline_hold_guard
    BEFORE UPDATE ON executor_capacity_allocations
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submit_deadline_capacity_hold();
