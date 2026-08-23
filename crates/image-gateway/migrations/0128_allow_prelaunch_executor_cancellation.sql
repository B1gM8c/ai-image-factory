-- A handed-off output may be canceled before any executor acquires launch
-- authority. Preserve the existing expired-launch policy while also allowing
-- the exact prepared/no-launch state used by the audited cancellation command.
CREATE OR REPLACE FUNCTION validate_executor_resolution_decision()
RETURNS TRIGGER AS $$
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
    quarantine provider_remote_task_quarantines%ROWTYPE;
    now_ms BIGINT;
BEGIN
    SELECT execution.state, execution.executor_owner, execution.lease_epoch,
           execution.lease_expires_at_ms, execution.launch_owner,
           execution.launch_lease_epoch, submission.state, work.state,
           floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
    INTO executor_state, executor_owner_value, executor_epoch,
         executor_expiry, launch_owner_value, launch_epoch,
         submission_state, work_state, now_ms
    FROM executor_executions execution
    JOIN provider_submissions submission
      ON submission.executor_execution_id = execution.executor_execution_id
     AND submission.submission_id = execution.submission_id
    JOIN work_items work
      ON work.work_item_id = submission.work_item_id
     AND work.job_id = submission.job_id
    WHERE execution.executor_execution_id = NEW.executor_execution_id
      AND execution.submission_id = NEW.submission_id
    FOR UPDATE OF execution, submission;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'resolution decision does not name a durable execution';
    END IF;
    IF NEW.source = 'active_runner_observation' THEN
        IF executor_state <> 'running'
           OR submission_state <> 'running'
           OR executor_owner_value IS DISTINCT FROM launch_owner_value
           OR executor_epoch IS DISTINCT FROM launch_epoch
           OR executor_expiry IS NULL
           OR executor_expiry <= now_ms
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
            RAISE EXCEPTION
                'active resolution requires the live launch fence';
        END IF;
    ELSIF NEW.source = 'executor_lease_expired' THEN
        IF executor_state <> 'running'
           OR submission_state <> 'running'
           OR executor_expiry IS NULL
           OR executor_expiry > now_ms THEN
            RAISE EXCEPTION
                'expiry resolution requires an expired running execution';
        END IF;
    ELSIF NEW.source = 'executor_start_abandoned' THEN
        IF NOT (
            (
                executor_state = 'leased'
                AND submission_state = 'prepared'
                AND launch_owner_value IS NULL
                AND launch_epoch IS NULL
                AND executor_expiry IS NOT NULL
                AND executor_expiry <= now_ms
                AND work_state IN ('succeeded', 'failed', 'uncertain')
            )
            OR
            (
                executor_state = 'prepared'
                AND submission_state = 'prepared'
                AND executor_owner_value IS NULL
                AND executor_epoch = 0
                AND executor_expiry IS NULL
                AND launch_owner_value IS NULL
                AND launch_epoch IS NULL
                AND work_state = 'awaiting_executor'
            )
        ) THEN
            RAISE EXCEPTION
                'abandoned resolution requires an unlaunched execution';
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
           OR remote_observation IS DISTINCT FROM
                NEW.provider_task_observation_id
           OR NOT (
                (remote_state = 'artifact_ready'
                    AND NEW.resolved_state = 'succeeded')
                OR (remote_state = 'failed'
                    AND NEW.resolved_state = 'failed')
                OR (remote_state = 'uncertain'
                    AND NEW.resolved_state = 'uncertain')
                OR (remote_state = 'canceled'
                    AND NEW.resolved_state = 'canceled')
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
        IF executor_state <> 'running'
           OR submission_state <> 'running'
           OR executor_owner_value IS DISTINCT FROM submit_owner_value
           OR executor_epoch IS DISTINCT FROM submit_epoch
           OR launch_owner_value IS DISTINCT FROM submit_owner_value
           OR launch_epoch IS DISTINCT FROM submit_epoch
           OR submit_error IS DISTINCT FROM NEW.error_code
           OR submit_state <> 'rejected'
           OR NEW.resolved_state <> 'failed' THEN
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
        IF executor_state <> 'running'
           OR submission_state <> 'running'
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
                SELECT 1
                FROM provider_remote_tasks task
                WHERE task.submission_id = NEW.submission_id
           ) THEN
            RAISE EXCEPTION
                'submit deadline requires due unknown-effect evidence and held capacity';
        END IF;
    ELSIF NEW.source = 'remote_task_deadline' THEN
        SELECT * INTO quarantine
        FROM provider_remote_task_quarantines
        WHERE quarantine_id = NEW.provider_remote_task_quarantine_id
          AND executor_execution_id = NEW.executor_execution_id
          AND submission_id = NEW.submission_id;
        SELECT allocation.state
        INTO capacity_state
        FROM executor_capacity_allocations allocation
        WHERE allocation.executor_execution_id = NEW.executor_execution_id
          AND allocation.submission_id = NEW.submission_id
        FOR UPDATE;
        IF quarantine.quarantine_id IS NULL
           OR executor_state <> 'provider_waiting'
           OR submission_state <> 'provider_waiting'
           OR executor_owner_value IS NOT NULL
           OR executor_expiry IS NOT NULL
           OR capacity_state <> 'held'
           OR NEW.decided_at_ms IS DISTINCT FROM quarantine.quarantined_at_ms
           OR NEW.decided_at_ms > now_ms
           OR NOT EXISTS (
                SELECT 1
                FROM provider_remote_tasks task
                WHERE task.submission_id = NEW.submission_id
                  AND task.executor_execution_id = NEW.executor_execution_id
                  AND task.state = 'uncertain'
                  AND task.deadline_quarantine_id = quarantine.quarantine_id
                  AND task.provider_deadline_at_ms =
                        quarantine.provider_deadline_at_ms
                  AND task.error_code = NEW.error_code
           ) THEN
            RAISE EXCEPTION
                'remote task deadline requires exact quarantine authority and held capacity';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER TABLE executor_executions
    DROP CONSTRAINT executor_executions_lifecycle_check,
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
            AND executor_owner IS NULL AND lease_expires_at_ms IS NULL
            AND finished_at_ms IS NOT NULL AND error_code IS NOT NULL
            AND (
                (lease_epoch > 0 AND leased_at_ms IS NOT NULL)
                OR
                (lease_epoch = 0 AND leased_at_ms IS NULL AND started_at_ms IS NULL)
            ))
    );

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
        (OLD.state = 'prepared' AND NEW.state IN ('leased', 'canceled'))
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
    ELSIF OLD.state = 'prepared' AND NEW.state = 'canceled' THEN
        IF OLD.executor_owner IS NOT NULL
           OR OLD.lease_epoch <> 0
           OR OLD.lease_expires_at_ms IS NOT NULL
           OR OLD.leased_at_ms IS NOT NULL
           OR OLD.started_at_ms IS NOT NULL
           OR OLD.launch_owner IS NOT NULL
           OR OLD.launch_lease_epoch IS NOT NULL
           OR NEW.executor_owner IS NOT NULL
           OR NEW.lease_epoch <> 0
           OR NEW.lease_expires_at_ms IS NOT NULL
           OR NEW.leased_at_ms IS NOT NULL
           OR NEW.started_at_ms IS NOT NULL
           OR NEW.launch_owner IS NOT NULL
           OR NEW.launch_lease_epoch IS NOT NULL
           OR NEW.finished_at_ms IS NULL
           OR NEW.resolution_decision_id IS NULL
           OR NEW.error_code <> 'executor_start_abandoned' THEN
            RAISE EXCEPTION
                'prepared cancellation requires exact no-launch evidence';
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
