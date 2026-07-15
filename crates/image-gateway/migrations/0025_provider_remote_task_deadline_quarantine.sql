LOCK TABLE provider_remote_tasks, provider_task_observations,
    provider_remote_submit_intents, provider_submit_recoveries,
    executor_artifact_authorities, executor_result_manifests,
    executor_resolution_decisions, executor_executions,
    provider_submissions, executor_capacity_allocations
    IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_task_observations observation
        WHERE observation.event_identity =
              'internal:artifact-authority-recovery-v1'
    ) THEN
        RAISE EXCEPTION
            'remote task deadline migration found a reserved internal event identity';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM provider_remote_tasks task
        WHERE task.poll_owner IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'remote task deadline migration requires all poll claimants to be drained';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM provider_remote_tasks task
        LEFT JOIN provider_submit_recoveries recovery
          ON recovery.submission_id = task.submission_id
         AND recovery.executor_execution_id = task.executor_execution_id
         AND recovery.provider_id = task.provider_id
         AND recovery.provider_account_id = task.provider_account_id
        LEFT JOIN executor_executions execution
          ON execution.executor_execution_id = task.executor_execution_id
         AND execution.submission_id = task.submission_id
        LEFT JOIN provider_submissions submission
          ON submission.executor_execution_id = task.executor_execution_id
         AND submission.submission_id = task.submission_id
        LEFT JOIN executor_capacity_allocations allocation
          ON allocation.executor_execution_id = task.executor_execution_id
         AND allocation.submission_id = task.submission_id
        WHERE recovery.submission_id IS NULL
           OR recovery.state <> 'closed'
           OR recovery.provider_deadline_at_ms <= task.created_at_ms
           OR (
                task.state = 'provider_waiting'
                AND (
                    recovery.provider_deadline_at_ms <= now_ms
                    OR task.next_poll_at_ms > recovery.provider_deadline_at_ms
                    OR execution.state <> 'provider_waiting'
                    OR submission.state <> 'provider_waiting'
                    OR allocation.state <> 'held'
                )
              )
    ) THEN
        RAISE EXCEPTION
            'remote task deadline migration found unsafe legacy state';
    END IF;
END;
$$;

DROP TRIGGER provider_remote_task_update_guard ON provider_remote_tasks;

ALTER TABLE provider_remote_tasks
    ADD COLUMN provider_deadline_at_ms BIGINT,
    ADD COLUMN deadline_quarantine_id UUID;

UPDATE provider_remote_tasks task
SET provider_deadline_at_ms = recovery.provider_deadline_at_ms
FROM provider_submit_recoveries recovery
WHERE recovery.submission_id = task.submission_id
  AND recovery.executor_execution_id = task.executor_execution_id
  AND recovery.provider_id = task.provider_id
  AND recovery.provider_account_id = task.provider_account_id
  AND recovery.state = 'closed';

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

ALTER TABLE provider_remote_tasks
    ALTER COLUMN provider_deadline_at_ms SET NOT NULL,
    DROP CONSTRAINT provider_remote_tasks_check2,
    ADD CONSTRAINT provider_remote_tasks_poll_fence_check CHECK (
        (
            poll_owner IS NULL
            AND poll_lease_expires_at_ms IS NULL
            AND poll_claimed_at_ms IS NULL
        )
        OR
        (
            poll_owner IS NOT NULL
            AND poll_lease_epoch > 0
            AND poll_lease_expires_at_ms IS NOT NULL
            AND poll_claimed_at_ms IS NOT NULL
            AND poll_claimed_at_ms < poll_lease_expires_at_ms
            AND poll_lease_expires_at_ms <= provider_deadline_at_ms
            AND state = 'provider_waiting'
        )
    ),
    ADD CONSTRAINT provider_remote_tasks_deadline_shape_check CHECK (
        provider_deadline_at_ms > created_at_ms
        AND (
            state <> 'provider_waiting'
            OR next_poll_at_ms <= provider_deadline_at_ms
        )
        AND (
            -- The error text is descriptive; only this FK marks quarantine authority.
            deadline_quarantine_id IS NULL
            OR
            (deadline_quarantine_id IS NOT NULL
                AND state = 'uncertain'
                AND artifact_ref IS NULL
                AND error_code = 'provider_remote_task_deadline'
                AND effect_certainty = 'unknown_remote_effect'
                AND next_poll_at_ms IS NULL
                AND poll_owner IS NULL
                AND poll_lease_expires_at_ms IS NULL
                AND poll_claimed_at_ms IS NULL
                AND terminal_at_ms >= provider_deadline_at_ms)
        )
    );

CREATE TABLE provider_remote_task_quarantines (
    quarantine_id UUID PRIMARY KEY,
    submission_id UUID NOT NULL UNIQUE,
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
    provider_deadline_at_ms BIGINT NOT NULL,
    error_code TEXT NOT NULL CHECK (
        error_code = 'provider_remote_task_deadline'
    ),
    quarantined_at_ms BIGINT NOT NULL CHECK (
        quarantined_at_ms >= provider_deadline_at_ms
    ),
    CHECK (quarantine_id = executor_execution_id),
    FOREIGN KEY (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) REFERENCES provider_remote_tasks (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (quarantine_id, executor_execution_id, submission_id)
);

ALTER TABLE provider_remote_tasks
    ADD CONSTRAINT provider_remote_tasks_deadline_quarantine_fk
        FOREIGN KEY (
            deadline_quarantine_id, executor_execution_id, submission_id
        ) REFERENCES provider_remote_task_quarantines (
            quarantine_id, executor_execution_id, submission_id
        ) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX provider_remote_tasks_deadline_claim_idx
    ON provider_remote_tasks (
        provider_id, provider_account_id,
        provider_deadline_at_ms, submission_id
    )
    WHERE state = 'provider_waiting';

ALTER TABLE provider_task_observations
    DROP CONSTRAINT provider_task_observations_source_check,
    DROP CONSTRAINT provider_task_observations_check,
    ADD CONSTRAINT provider_task_observations_source_check CHECK (
        source IN (
            'submit_attach', 'poll', 'cancel',
            'verified_callback', 'artifact_recovery'
        )
    ),
    ADD CONSTRAINT provider_task_observations_source_fence_check CHECK (
        (
            source IN ('poll', 'cancel')
            AND poll_owner IS NOT NULL
            AND char_length(poll_owner) BETWEEN 1 AND 255
            AND poll_owner !~ '[[:cntrl:]]'
            AND poll_lease_epoch IS NOT NULL
            AND poll_lease_epoch > 0
        )
        OR
        (
            source IN (
                'submit_attach', 'verified_callback', 'artifact_recovery'
            )
            AND poll_owner IS NULL
            AND poll_lease_epoch IS NULL
        )
    ),
    ADD CONSTRAINT provider_task_observations_artifact_recovery_check CHECK (
        (source = 'artifact_recovery') = (
            event_identity = 'internal:artifact-authority-recovery-v1'
        )
        AND (source <> 'artifact_recovery'
             OR observed_state = 'artifact_ready')
    );

ALTER TABLE executor_resolution_decisions
    ADD COLUMN provider_remote_task_quarantine_id UUID,
    DROP CONSTRAINT executor_resolution_decisions_source_check,
    DROP CONSTRAINT executor_resolution_decisions_evidence_check,
    ADD CONSTRAINT executor_resolution_decisions_source_check CHECK (
        source IN (
            'active_runner_observation', 'executor_lease_expired',
            'executor_start_abandoned', 'remote_provider_observation',
            'remote_submit_outcome', 'remote_submit_deadline',
            'remote_task_deadline'
        )
    ),
    ADD CONSTRAINT executor_resolution_decisions_evidence_check CHECK (
        (source = 'active_runner_observation'
            AND observation_id IS NOT NULL
            AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NULL
            AND provider_remote_task_quarantine_id IS NULL
            AND resolved_state IN ('succeeded', 'failed', 'uncertain'))
        OR
        (source = 'executor_lease_expired'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NULL
            AND provider_remote_task_quarantine_id IS NULL
            AND resolved_state = 'uncertain'
            AND error_code = 'executor_lease_expired')
        OR
        (source = 'executor_start_abandoned'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NULL
            AND provider_remote_task_quarantine_id IS NULL
            AND resolved_state = 'canceled'
            AND error_code = 'executor_start_abandoned')
        OR
        (source = 'remote_provider_observation'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NOT NULL
            AND provider_submit_intent_id IS NULL
            AND provider_remote_task_quarantine_id IS NULL)
        OR
        (source = 'remote_submit_outcome'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NOT NULL
            AND provider_remote_task_quarantine_id IS NULL
            AND resolved_state = 'failed')
        OR
        (source = 'remote_submit_deadline'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NOT NULL
            AND provider_remote_task_quarantine_id IS NULL
            AND resolved_state = 'uncertain'
            AND error_code = 'provider_submit_deadline')
        OR
        (source = 'remote_task_deadline'
            AND observation_id IS NULL
            AND provider_task_observation_id IS NULL
            AND provider_submit_intent_id IS NULL
            AND provider_remote_task_quarantine_id IS NOT NULL
            AND resolved_state = 'uncertain'
            AND error_code = 'provider_remote_task_deadline')
    ),
    ADD CONSTRAINT executor_resolution_decisions_remote_task_quarantine_fk
        FOREIGN KEY (
            provider_remote_task_quarantine_id,
            executor_execution_id, submission_id
        ) REFERENCES provider_remote_task_quarantines (
            quarantine_id, executor_execution_id, submission_id
        ) ON DELETE RESTRICT;

CREATE OR REPLACE FUNCTION validate_provider_remote_task_insert()
RETURNS TRIGGER AS $$
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
    frozen_provider_deadline_at_ms BIGINT;
BEGIN
    SELECT execution.state, execution.executor_owner, execution.lease_epoch,
           execution.lease_expires_at_ms, execution.launch_owner,
           execution.launch_lease_epoch, submission.state,
           submission.provider_id, submission.provider_account_id,
           allocation.state, recovery.state, recovery.recovery_owner,
           recovery.recovery_lease_epoch,
           recovery.recovery_lease_expires_at_ms,
           recovery.provider_deadline_at_ms
    INTO execution_state, execution_owner, execution_epoch,
         execution_expires_at_ms, execution_launch_owner,
         execution_launch_epoch, submission_state, submission_provider,
         submission_account, capacity_state, recovery_state,
         recovery_owner, recovery_epoch, recovery_expires_at_ms,
         frozen_provider_deadline_at_ms
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

    IF NOT FOUND
       OR execution_state <> 'running'
       OR submission_state <> 'running'
       OR execution_owner IS DISTINCT FROM NEW.submit_owner
       OR execution_epoch IS DISTINCT FROM NEW.submit_lease_epoch
       OR execution_launch_owner IS DISTINCT FROM NEW.submit_owner
       OR execution_launch_epoch IS DISTINCT FROM NEW.submit_lease_epoch
       OR submission_provider IS DISTINCT FROM NEW.provider_id
       OR submission_account IS DISTINCT FROM NEW.provider_account_id
       OR capacity_state IS DISTINCT FROM 'held'
       OR recovery_state IS DISTINCT FROM 'active'
       OR NEW.provider_deadline_at_ms IS DISTINCT FROM
            frozen_provider_deadline_at_ms
       OR NEW.provider_deadline_at_ms <= now_ms THEN
        RAISE EXCEPTION
            'remote task attach requires frozen execution identity, deadline, and held capacity';
    END IF;

    IF NEW.attach_recovery_owner IS NULL THEN
        IF execution_expires_at_ms <= now_ms OR recovery_owner IS NOT NULL THEN
            RAISE EXCEPTION 'remote task attach requires a live submit fence';
        END IF;
    ELSIF recovery_owner IS DISTINCT FROM NEW.attach_recovery_owner
          OR recovery_epoch IS DISTINCT FROM NEW.attach_recovery_lease_epoch
          OR recovery_expires_at_ms <= now_ms THEN
        RAISE EXCEPTION 'remote task attach requires the live recovery fence';
    END IF;

    IF NEW.state <> 'provider_waiting'
       OR NEW.deadline_quarantine_id IS NOT NULL
       OR NEW.poll_owner IS NOT NULL
       OR NEW.poll_lease_epoch <> 0
       OR NEW.poll_lease_expires_at_ms IS NOT NULL
       OR NEW.poll_claimed_at_ms IS NOT NULL
       OR NEW.cancel_requested
       OR NEW.cancel_requested_at_ms IS NOT NULL
       OR NEW.last_wakeup_observation_id IS NOT NULL
       OR NEW.next_poll_at_ms > NEW.provider_deadline_at_ms
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
              AND intent.provider_request_id IS NOT DISTINCT FROM
                    NEW.provider_request_id
       ) THEN
        RAISE EXCEPTION
            'remote task must be inserted from its durable submit receipt';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_provider_task_observation()
RETURNS TRIGGER AS $$
DECLARE
    task provider_remote_tasks%ROWTYPE;
    now_ms BIGINT;
BEGIN
    SELECT * INTO task
    FROM provider_remote_tasks
    WHERE submission_id = NEW.submission_id
      AND executor_execution_id = NEW.executor_execution_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'provider task observation has no durable remote task';
    END IF;
    now_ms := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;

    IF NEW.source = 'submit_attach' THEN
        IF NEW.observation_id IS DISTINCT FROM task.state_observation_id
           OR task.state <> 'provider_waiting'
           OR task.provider_deadline_at_ms <= now_ms
           OR NEW.next_poll_at_ms > task.provider_deadline_at_ms
           OR EXISTS (
               SELECT 1
               FROM provider_task_observations
               WHERE submission_id = NEW.submission_id
                 AND observation_id <> NEW.observation_id
           ) THEN
            RAISE EXCEPTION
                'submit attach observation must initialize the remote task once';
        END IF;
    ELSIF NEW.source IN ('poll', 'cancel') THEN
        IF task.state <> 'provider_waiting'
           OR task.provider_deadline_at_ms <= now_ms
           OR task.poll_owner IS DISTINCT FROM NEW.poll_owner
           OR task.poll_lease_epoch IS DISTINCT FROM NEW.poll_lease_epoch
           OR task.poll_lease_expires_at_ms IS NULL
           OR task.poll_lease_expires_at_ms <= now_ms
           OR NEW.next_poll_at_ms > task.provider_deadline_at_ms THEN
            RAISE EXCEPTION
                'provider task observation requires the live bounded poll fence';
        END IF;
        IF NEW.source = 'cancel' AND NOT task.cancel_requested THEN
            RAISE EXCEPTION
                'cancel observation requires a durable cancel request';
        END IF;
        IF NEW.observed_state = 'canceled' AND NOT task.cancel_requested THEN
            RAISE EXCEPTION
                'canceled observation requires a durable cancel request';
        END IF;
        IF NEW.observed_state IN ('failed', 'canceled', 'uncertain')
           AND EXISTS (
                SELECT 1
                FROM executor_result_manifests manifest
                WHERE manifest.executor_execution_id = NEW.executor_execution_id
                  AND manifest.submission_id = NEW.submission_id
           ) THEN
            RAISE EXCEPTION
                'published artifact evidence must resolve before contradictory terminal evidence';
        END IF;
    ELSIF NEW.source = 'verified_callback' THEN
        IF task.state <> 'provider_waiting'
           OR task.provider_deadline_at_ms <= now_ms
           OR NEW.observed_state <> 'provider_waiting'
           OR NEW.next_poll_at_ms IS NULL
           OR NEW.next_poll_at_ms > task.provider_deadline_at_ms THEN
            RAISE EXCEPTION
                'verified callback may only wake a live bounded task';
        END IF;
    ELSIF NEW.source = 'artifact_recovery' THEN
        IF task.state <> 'provider_waiting'
           OR task.provider_deadline_at_ms > NEW.observed_at_ms
           OR NEW.observed_state <> 'artifact_ready'
           OR NEW.artifact_ref IS DISTINCT FROM
                ('manifest:' || replace(NEW.result_manifest_id::TEXT, '-', '')) THEN
            RAISE EXCEPTION
                'artifact recovery requires due immutable artifact authority';
        END IF;
    END IF;

    IF NEW.observed_state = 'artifact_ready' AND NOT EXISTS (
        SELECT 1
        FROM executor_result_manifests manifest
        JOIN executor_artifact_authorities authority
          ON authority.authority_id = manifest.artifact_authority_id
         AND authority.executor_execution_id = manifest.executor_execution_id
         AND authority.submission_id = manifest.submission_id
        WHERE manifest.manifest_id = NEW.result_manifest_id
          AND manifest.artifact_authority_id = NEW.executor_execution_id
          AND manifest.executor_execution_id = NEW.executor_execution_id
          AND manifest.submission_id = NEW.submission_id
          AND authority.sha256_hex = NEW.artifact_sha256_hex
          AND authority.byte_size = NEW.artifact_byte_size
          AND authority.media_type = NEW.artifact_media_type
    ) THEN
        RAISE EXCEPTION
            'artifact_ready observation requires its immutable artifact manifest';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_provider_remote_task_update()
RETURNS TRIGGER AS $$
DECLARE
    observation provider_task_observations%ROWTYPE;
    quarantine provider_remote_task_quarantines%ROWTYPE;
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
       OR NEW.provider_deadline_at_ms IS DISTINCT FROM OLD.provider_deadline_at_ms
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms THEN
        RAISE EXCEPTION 'provider remote task identity is immutable';
    END IF;
    IF OLD.state IN ('artifact_ready', 'failed', 'canceled', 'uncertain') THEN
        RAISE EXCEPTION 'terminal provider remote task is immutable';
    END IF;

    IF NOT OLD.cancel_requested AND NEW.cancel_requested THEN
        IF OLD.provider_deadline_at_ms <= now_ms
           OR NEW.cancel_requested_at_ms IS NULL
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

    IF NEW.last_wakeup_observation_id IS DISTINCT FROM
          OLD.last_wakeup_observation_id THEN
        SELECT * INTO observation
        FROM provider_task_observations
        WHERE observation_id = NEW.last_wakeup_observation_id
          AND submission_id = NEW.submission_id
          AND source = 'verified_callback';
        IF NOT FOUND
           OR OLD.provider_deadline_at_ms <= now_ms
           OR NEW.state <> 'provider_waiting'
           OR NEW.next_poll_at_ms <> LEAST(
                OLD.next_poll_at_ms, observation.next_poll_at_ms
              )
           OR to_jsonb(NEW)
                - 'last_wakeup_observation_id'
                - 'next_poll_at_ms' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'last_wakeup_observation_id'
                - 'next_poll_at_ms' - 'updated_at_ms' THEN
            RAISE EXCEPTION
                'verified callback may only advance a live bounded poll';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.poll_owner IS NULL AND NEW.poll_owner IS NOT NULL THEN
        IF OLD.provider_deadline_at_ms <= now_ms
           OR OLD.next_poll_at_ms > now_ms
           OR NEW.poll_lease_epoch <> OLD.poll_lease_epoch + 1
           OR NEW.poll_lease_expires_at_ms <= now_ms
           OR NEW.poll_lease_expires_at_ms > NEW.provider_deadline_at_ms
           OR NEW.poll_claimed_at_ms IS NULL
           OR NEW.poll_claimed_at_ms > now_ms
           OR NEW.poll_claimed_at_ms < now_ms - 1000
           OR to_jsonb(NEW)
                - 'poll_owner' - 'poll_lease_epoch'
                - 'poll_lease_expires_at_ms'
                - 'poll_claimed_at_ms' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'poll_owner' - 'poll_lease_epoch'
                - 'poll_lease_expires_at_ms'
                - 'poll_claimed_at_ms' - 'updated_at_ms' THEN
            RAISE EXCEPTION
                'provider poll claim requires a due task before its deadline';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.poll_owner IS NOT NULL
       AND NEW.poll_owner IS NOT NULL
       AND (
            NEW.poll_owner IS DISTINCT FROM OLD.poll_owner
            OR NEW.poll_lease_epoch IS DISTINCT FROM OLD.poll_lease_epoch
       ) THEN
        IF OLD.provider_deadline_at_ms <= now_ms
           OR OLD.poll_lease_expires_at_ms > now_ms
           OR NEW.poll_lease_epoch <> OLD.poll_lease_epoch + 1
           OR NEW.poll_lease_expires_at_ms <= now_ms
           OR NEW.poll_lease_expires_at_ms > NEW.provider_deadline_at_ms
           OR NEW.poll_claimed_at_ms IS NULL
           OR NEW.poll_claimed_at_ms > now_ms
           OR NEW.poll_claimed_at_ms < now_ms - 1000
           OR to_jsonb(NEW)
                - 'poll_owner' - 'poll_lease_epoch'
                - 'poll_lease_expires_at_ms'
                - 'poll_claimed_at_ms' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'poll_owner' - 'poll_lease_epoch'
                - 'poll_lease_expires_at_ms'
                - 'poll_claimed_at_ms' - 'updated_at_ms' THEN
            RAISE EXCEPTION
                'provider poll reclaim requires an expired bounded fence';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.poll_owner IS NOT NULL
       AND NEW.poll_owner IS NOT DISTINCT FROM OLD.poll_owner
       AND NEW.poll_lease_epoch IS NOT DISTINCT FROM OLD.poll_lease_epoch
       AND (
            NEW.poll_lease_expires_at_ms IS DISTINCT FROM
                OLD.poll_lease_expires_at_ms
            OR NEW.updated_at_ms IS DISTINCT FROM OLD.updated_at_ms
       ) THEN
        IF OLD.provider_deadline_at_ms <= now_ms
           OR OLD.poll_lease_expires_at_ms <= now_ms
           OR NEW.poll_lease_expires_at_ms < OLD.poll_lease_expires_at_ms
           OR NEW.poll_lease_expires_at_ms > NEW.provider_deadline_at_ms
           OR NEW.updated_at_ms < OLD.updated_at_ms
           OR NEW.updated_at_ms > now_ms
           OR NEW.updated_at_ms < now_ms - 1000
           OR NEW.poll_claimed_at_ms IS DISTINCT FROM OLD.poll_claimed_at_ms
           OR to_jsonb(NEW) - 'poll_lease_expires_at_ms' - 'updated_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD) - 'poll_lease_expires_at_ms' - 'updated_at_ms' THEN
            RAISE EXCEPTION
                'provider poll heartbeat requires the live bounded fence';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.deadline_quarantine_id IS NULL
       AND NEW.deadline_quarantine_id IS NOT NULL THEN
        SELECT * INTO quarantine
        FROM provider_remote_task_quarantines
        WHERE quarantine_id = NEW.deadline_quarantine_id
          AND submission_id = NEW.submission_id
          AND executor_execution_id = NEW.executor_execution_id;
        IF NOT FOUND
           OR OLD.provider_deadline_at_ms > now_ms
           OR quarantine.provider_id IS DISTINCT FROM NEW.provider_id
           OR quarantine.provider_account_id IS DISTINCT FROM
                NEW.provider_account_id
           OR quarantine.remote_operation_id IS DISTINCT FROM
                NEW.remote_operation_id
           OR quarantine.provider_deadline_at_ms IS DISTINCT FROM
                NEW.provider_deadline_at_ms
           OR NEW.state <> 'uncertain'
           OR NEW.error_code <> 'provider_remote_task_deadline'
           OR NEW.effect_certainty <> 'unknown_remote_effect'
           OR NEW.state_observation_id IS DISTINCT FROM OLD.state_observation_id
           OR NEW.poll_owner IS NOT NULL
           OR NEW.poll_lease_expires_at_ms IS NOT NULL
           OR NEW.poll_claimed_at_ms IS NOT NULL
           OR NEW.terminal_at_ms IS DISTINCT FROM quarantine.quarantined_at_ms
           OR to_jsonb(NEW)
                - 'state' - 'error_code' - 'effect_certainty'
                - 'next_poll_at_ms' - 'poll_owner'
                - 'poll_lease_expires_at_ms' - 'poll_claimed_at_ms'
                - 'deadline_quarantine_id' - 'updated_at_ms' - 'terminal_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'state' - 'error_code' - 'effect_certainty'
                - 'next_poll_at_ms' - 'poll_owner'
                - 'poll_lease_expires_at_ms' - 'poll_claimed_at_ms'
                - 'deadline_quarantine_id' - 'updated_at_ms' - 'terminal_at_ms' THEN
            RAISE EXCEPTION
                'remote task deadline requires exact quarantine authority';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.state_observation_id IS DISTINCT FROM OLD.state_observation_id THEN
        SELECT * INTO observation
        FROM provider_task_observations
        WHERE observation_id = NEW.state_observation_id
          AND submission_id = NEW.submission_id
          AND source IN ('poll', 'cancel', 'artifact_recovery');
        IF NOT FOUND
           OR NEW.state IS DISTINCT FROM observation.observed_state
           OR NEW.artifact_ref IS DISTINCT FROM observation.artifact_ref
           OR NEW.error_code IS DISTINCT FROM observation.error_code
           OR NEW.effect_certainty IS DISTINCT FROM observation.effect_certainty
           OR NEW.next_poll_at_ms IS DISTINCT FROM observation.next_poll_at_ms
           OR NEW.poll_owner IS NOT NULL
           OR NEW.poll_lease_expires_at_ms IS NOT NULL
           OR NEW.poll_claimed_at_ms IS NOT NULL
           OR NEW.poll_lease_epoch IS DISTINCT FROM OLD.poll_lease_epoch
           OR NEW.deadline_quarantine_id IS DISTINCT FROM
                OLD.deadline_quarantine_id
           OR NEW.terminal_at_ms IS DISTINCT FROM (
                CASE
                    WHEN observation.observed_state = 'provider_waiting'
                    THEN NULL
                    ELSE observation.observed_at_ms
                END
              )
           OR (
                observation.source IN ('poll', 'cancel')
                AND (
                    OLD.provider_deadline_at_ms <= now_ms
                    OR OLD.poll_owner IS NULL
                    OR observation.poll_owner IS DISTINCT FROM OLD.poll_owner
                    OR observation.poll_lease_epoch IS DISTINCT FROM
                        OLD.poll_lease_epoch
                )
              )
           OR (
                observation.source = 'artifact_recovery'
                AND OLD.provider_deadline_at_ms > observation.observed_at_ms
              )
           OR to_jsonb(NEW)
                - 'state' - 'artifact_ref' - 'error_code' - 'effect_certainty'
                - 'next_poll_at_ms' - 'poll_owner'
                - 'poll_lease_expires_at_ms' - 'poll_claimed_at_ms'
                - 'state_observation_id' - 'updated_at_ms' - 'terminal_at_ms'
              IS DISTINCT FROM
              to_jsonb(OLD)
                - 'state' - 'artifact_ref' - 'error_code' - 'effect_certainty'
                - 'next_poll_at_ms' - 'poll_owner'
                - 'poll_lease_expires_at_ms' - 'poll_claimed_at_ms'
                - 'state_observation_id' - 'updated_at_ms' - 'terminal_at_ms' THEN
            RAISE EXCEPTION
                'provider task state requires exact bounded evidence';
        END IF;
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'unsupported provider remote task mutation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_remote_task_update_guard
    BEFORE UPDATE ON provider_remote_tasks
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_remote_task_update();

CREATE FUNCTION validate_provider_remote_task_quarantine_insert()
RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
    task provider_remote_tasks%ROWTYPE;
    execution_state TEXT;
    submission_state TEXT;
    allocation_state TEXT;
BEGIN
    SELECT * INTO task
    FROM provider_remote_tasks
    WHERE submission_id = NEW.submission_id
      AND executor_execution_id = NEW.executor_execution_id
    FOR UPDATE;
    SELECT execution.state, submission.state, allocation.state
    INTO execution_state, submission_state, allocation_state
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

    IF NOT FOUND
       OR task.submission_id IS NULL
       OR task.state <> 'provider_waiting'
       OR task.provider_id IS DISTINCT FROM NEW.provider_id
       OR task.provider_account_id IS DISTINCT FROM NEW.provider_account_id
       OR task.remote_operation_id IS DISTINCT FROM NEW.remote_operation_id
       OR task.provider_deadline_at_ms IS DISTINCT FROM
            NEW.provider_deadline_at_ms
       OR NEW.quarantined_at_ms > now_ms
       OR execution_state <> 'provider_waiting'
       OR submission_state <> 'provider_waiting'
       OR allocation_state <> 'held'
       OR EXISTS (
            SELECT 1
            FROM executor_result_manifests manifest
            WHERE manifest.executor_execution_id = NEW.executor_execution_id
              AND manifest.submission_id = NEW.submission_id
       ) THEN
        RAISE EXCEPTION
            'remote task quarantine requires due unresolved authority and held capacity';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_remote_task_quarantine_insert_guard
    BEFORE INSERT ON provider_remote_task_quarantines
    FOR EACH ROW
    EXECUTE FUNCTION validate_provider_remote_task_quarantine_insert();

CREATE FUNCTION reject_provider_remote_task_quarantine_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider remote task quarantine history is immutable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_remote_task_quarantines_reject_update
    BEFORE UPDATE ON provider_remote_task_quarantines
    FOR EACH ROW
    EXECUTE FUNCTION reject_provider_remote_task_quarantine_mutation();

CREATE TRIGGER provider_remote_task_quarantines_reject_delete
    BEFORE DELETE ON provider_remote_task_quarantines
    FOR EACH ROW
    EXECUTE FUNCTION reject_provider_remote_task_quarantine_mutation();

CREATE TRIGGER provider_remote_task_quarantines_reject_truncate
    BEFORE TRUNCATE ON provider_remote_task_quarantines
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_provider_remote_task_quarantine_mutation();

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
        IF executor_state <> 'leased'
           OR submission_state <> 'prepared'
           OR launch_owner_value IS NOT NULL
           OR launch_epoch IS NOT NULL
           OR executor_expiry IS NULL
           OR executor_expiry > now_ms
           OR work_state NOT IN ('succeeded', 'failed', 'uncertain') THEN
            RAISE EXCEPTION
                'abandoned resolution requires expired unstarted terminal work';
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

CREATE FUNCTION enforce_precise_terminal_capacity_release()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.state = 'held'
       AND NEW.state = 'released'
       AND NEW.release_reason = 'terminal_evidence'
       AND NOT (
            EXISTS (
                SELECT 1
                FROM executor_resolution_decisions decision
                JOIN executor_runner_observations observation
                  ON observation.observation_id = decision.observation_id
                 AND observation.executor_execution_id =
                        decision.executor_execution_id
                 AND observation.submission_id = decision.submission_id
                 AND observation.observed_state = decision.resolved_state
                WHERE decision.decision_id = NEW.release_decision_id
                  AND decision.executor_execution_id =
                        NEW.executor_execution_id
                  AND decision.submission_id = NEW.submission_id
                  AND decision.source = 'active_runner_observation'
                  AND decision.resolved_state = NEW.released_state
            )
            OR EXISTS (
                SELECT 1
                FROM executor_resolution_decisions decision
                JOIN executor_runner_observations observation
                  ON observation.observation_id = NEW.executor_execution_id
                 AND observation.executor_execution_id =
                        decision.executor_execution_id
                 AND observation.submission_id = decision.submission_id
                WHERE decision.decision_id = NEW.release_decision_id
                  AND decision.executor_execution_id =
                        NEW.executor_execution_id
                  AND decision.submission_id = NEW.submission_id
                  AND decision.source = 'executor_lease_expired'
                  AND decision.resolved_state = 'uncertain'
                  AND NEW.released_state = 'uncertain'
                  AND observation.observed_at_ms <= NEW.released_at_ms
            )
       ) THEN
        RAISE EXCEPTION
            'terminal capacity release requires exact active or late runner evidence';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_capacity_precise_terminal_release_guard
    BEFORE UPDATE ON executor_capacity_allocations
    FOR EACH ROW
    EXECUTE FUNCTION enforce_precise_terminal_capacity_release();

CREATE OR REPLACE FUNCTION enforce_provider_submit_deadline_capacity_hold()
RETURNS TRIGGER AS $$
DECLARE
    decision_source TEXT;
BEGIN
    IF OLD.state = 'held' AND NEW.state = 'released' THEN
        SELECT decision.source INTO decision_source
        FROM executor_executions execution
        JOIN executor_resolution_decisions decision
          ON decision.decision_id = execution.resolution_decision_id
         AND decision.executor_execution_id = execution.executor_execution_id
         AND decision.submission_id = execution.submission_id
        WHERE execution.executor_execution_id = NEW.executor_execution_id
          AND execution.submission_id = NEW.submission_id;

        IF decision_source = 'remote_task_deadline' THEN
            RAISE EXCEPTION
                'remote task deadline capacity remains held pending strong reconciliation';
        END IF;
        IF decision_source = 'remote_submit_deadline'
           AND NOT (
                NEW.release_reason = 'provider_capacity_reconciliation'
                AND NEW.release_reconciliation_id IS NOT NULL
           ) THEN
            RAISE EXCEPTION
                'submit deadline capacity requires reconciliation evidence';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION enforce_provider_remote_task_quarantine_projection()
RETURNS TRIGGER AS $$
DECLARE
    target_submission UUID := COALESCE(NEW.submission_id, OLD.submission_id);
    marker_exists BOOLEAN;
    projection_valid BOOLEAN;
BEGIN
    SELECT
        quarantine.quarantine_id IS NOT NULL
            OR task.deadline_quarantine_id IS NOT NULL
            OR decision.source = 'remote_task_deadline',
        quarantine.quarantine_id IS NOT NULL
            AND task.deadline_quarantine_id = quarantine.quarantine_id
            AND task.state = 'uncertain'
            AND task.error_code = 'provider_remote_task_deadline'
            AND task.effect_certainty = 'unknown_remote_effect'
            AND task.provider_id = quarantine.provider_id
            AND task.provider_account_id = quarantine.provider_account_id
            AND task.remote_operation_id = quarantine.remote_operation_id
            AND task.provider_deadline_at_ms =
                quarantine.provider_deadline_at_ms
            AND task.terminal_at_ms = quarantine.quarantined_at_ms
            AND decision.source = 'remote_task_deadline'
            AND decision.provider_remote_task_quarantine_id =
                quarantine.quarantine_id
            AND decision.resolved_state = 'uncertain'
            AND decision.error_code = 'provider_remote_task_deadline'
            AND decision.decided_at_ms = quarantine.quarantined_at_ms
            AND execution.state = 'uncertain'
            AND execution.executor_owner IS NULL
            AND execution.lease_expires_at_ms IS NULL
            AND execution.resolution_decision_id = decision.decision_id
            AND submission.state = 'uncertain'
            AND submission.resolution_decision_id = decision.decision_id
            AND allocation.state = 'held'
            AND allocation.release_reason IS NULL
            AND allocation.release_decision_id IS NULL
            AND allocation.release_reconciliation_id IS NULL
    INTO marker_exists, projection_valid
    FROM provider_remote_tasks task
    JOIN executor_executions execution
      ON execution.executor_execution_id = task.executor_execution_id
     AND execution.submission_id = task.submission_id
    JOIN provider_submissions submission
      ON submission.executor_execution_id = task.executor_execution_id
     AND submission.submission_id = task.submission_id
    JOIN executor_capacity_allocations allocation
      ON allocation.executor_execution_id = task.executor_execution_id
     AND allocation.submission_id = task.submission_id
    LEFT JOIN provider_remote_task_quarantines quarantine
      ON quarantine.executor_execution_id = task.executor_execution_id
     AND quarantine.submission_id = task.submission_id
    LEFT JOIN executor_resolution_decisions decision
      ON decision.executor_execution_id = task.executor_execution_id
     AND decision.submission_id = task.submission_id
    WHERE task.submission_id = target_submission;

    IF marker_exists IS TRUE AND projection_valid IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION
            'remote task deadline quarantine projection is inconsistent';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_remote_task_quarantine_projection_check
    AFTER INSERT OR UPDATE ON provider_remote_tasks
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_provider_remote_task_quarantine_projection();

CREATE CONSTRAINT TRIGGER provider_remote_task_quarantine_row_projection_check
    AFTER INSERT ON provider_remote_task_quarantines
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_provider_remote_task_quarantine_projection();

CREATE CONSTRAINT TRIGGER provider_remote_task_quarantine_decision_check
    AFTER INSERT ON executor_resolution_decisions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_provider_remote_task_quarantine_projection();

CREATE CONSTRAINT TRIGGER provider_remote_task_quarantine_execution_check
    AFTER UPDATE ON executor_executions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_provider_remote_task_quarantine_projection();

CREATE CONSTRAINT TRIGGER provider_remote_task_quarantine_submission_check
    AFTER UPDATE ON provider_submissions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_provider_remote_task_quarantine_projection();

CREATE CONSTRAINT TRIGGER provider_remote_task_quarantine_capacity_check
    AFTER UPDATE ON executor_capacity_allocations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_provider_remote_task_quarantine_projection();
