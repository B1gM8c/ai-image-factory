LOCK TABLE provider_remote_tasks, provider_task_observations,
    executor_artifact_authorities, executor_result_manifests,
    executor_resolution_decisions, executor_executions,
    provider_submissions, executor_capacity_allocations
    IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_remote_tasks task
        JOIN executor_executions execution
          ON execution.executor_execution_id = task.executor_execution_id
         AND execution.submission_id = task.submission_id
        JOIN provider_submissions submission
          ON submission.executor_execution_id = task.executor_execution_id
         AND submission.submission_id = task.submission_id
        WHERE task.state = 'artifact_ready'
          AND (
            execution.state <> 'succeeded'
            OR submission.state <> 'succeeded'
            OR execution.resolution_decision_id IS NULL
            OR submission.resolution_decision_id IS DISTINCT FROM
               execution.resolution_decision_id
            OR submission.result_manifest_id IS NULL
          )
    ) THEN
        RAISE EXCEPTION
            'atomic provider artifact migration requires unresolved artifact_ready tasks to be drained';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM provider_remote_tasks task
        JOIN executor_result_manifests manifest
          ON manifest.executor_execution_id = task.executor_execution_id
         AND manifest.submission_id = task.submission_id
        WHERE task.state IN ('failed', 'canceled', 'uncertain')
    ) THEN
        RAISE EXCEPTION
            'atomic provider artifact migration found contradictory artifact and failure evidence';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM provider_task_observations observation
        JOIN provider_remote_tasks task
          ON task.executor_execution_id = observation.executor_execution_id
         AND task.submission_id = observation.submission_id
        JOIN executor_executions execution
          ON execution.executor_execution_id = observation.executor_execution_id
         AND execution.submission_id = observation.submission_id
        JOIN provider_submissions submission
          ON submission.executor_execution_id = observation.executor_execution_id
         AND submission.submission_id = observation.submission_id
        WHERE observation.observed_state <> 'provider_waiting'
          AND (
            task.state_observation_id IS DISTINCT FROM observation.observation_id
            OR task.state IS DISTINCT FROM observation.observed_state
            OR submission.state IS DISTINCT FROM execution.state
            OR (
              observation.observed_state = 'artifact_ready'
              AND execution.state <> 'succeeded'
            )
            OR (
              observation.observed_state IN ('failed', 'canceled', 'uncertain')
              AND execution.state IS DISTINCT FROM observation.observed_state
            )
          )
    ) THEN
        RAISE EXCEPTION
            'atomic provider artifact migration found an unprojected terminal observation';
    END IF;
END;
$$;

ALTER TABLE provider_task_observations
    ADD COLUMN result_manifest_id UUID,
    ADD COLUMN artifact_sha256_hex TEXT,
    ADD COLUMN artifact_byte_size BIGINT,
    ADD COLUMN artifact_media_type TEXT;

DROP TRIGGER provider_task_observations_reject_mutation
    ON provider_task_observations;

UPDATE provider_task_observations observation
SET result_manifest_id = manifest.manifest_id,
    artifact_sha256_hex = authority.sha256_hex,
    artifact_byte_size = authority.byte_size,
    artifact_media_type = authority.media_type
FROM executor_result_manifests manifest
JOIN executor_artifact_authorities authority
  ON authority.authority_id = manifest.artifact_authority_id
 AND authority.executor_execution_id = manifest.executor_execution_id
 AND authority.submission_id = manifest.submission_id
WHERE observation.observed_state = 'artifact_ready'
  AND observation.executor_execution_id = manifest.executor_execution_id
  AND observation.submission_id = manifest.submission_id;

CREATE TRIGGER provider_task_observations_reject_mutation
    BEFORE UPDATE OR DELETE ON provider_task_observations
    FOR EACH ROW EXECUTE FUNCTION reject_provider_remote_task_delete();

ALTER TABLE provider_task_observations
    ADD CONSTRAINT provider_task_observations_artifact_manifest_check CHECK (
        (observed_state = 'artifact_ready'
            AND result_manifest_id IS NOT NULL
            AND artifact_sha256_hex ~ '^[0-9a-f]{64}$'
            AND artifact_byte_size BETWEEN 1 AND 268435456
            AND artifact_media_type IN ('image/png', 'image/jpeg', 'image/webp'))
        OR
        (observed_state <> 'artifact_ready'
            AND result_manifest_id IS NULL
            AND artifact_sha256_hex IS NULL
            AND artifact_byte_size IS NULL
            AND artifact_media_type IS NULL)
    ),
    ADD CONSTRAINT provider_task_observations_result_manifest_fk
        FOREIGN KEY (result_manifest_id, executor_execution_id, submission_id)
        REFERENCES executor_result_manifests (
            manifest_id, executor_execution_id, submission_id
        ) ON DELETE RESTRICT;

CREATE UNIQUE INDEX provider_task_observations_manifest_uidx
    ON provider_task_observations (result_manifest_id)
    WHERE result_manifest_id IS NOT NULL;

CREATE OR REPLACE FUNCTION validate_provider_task_observation() RETURNS TRIGGER AS $$
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
        IF NEW.observed_state IN ('failed', 'canceled', 'uncertain')
           AND EXISTS (
                SELECT 1
                FROM executor_result_manifests manifest
                WHERE manifest.executor_execution_id = NEW.executor_execution_id
                  AND manifest.submission_id = NEW.submission_id
           ) THEN
            RAISE EXCEPTION
                'published artifact evidence must resolve before a contradictory terminal state';
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

CREATE OR REPLACE FUNCTION enforce_provider_remote_task_projection() RETURNS TRIGGER AS $$
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
    IF task_state = 'provider_waiting' THEN
        IF execution_state <> 'provider_waiting'
           OR submission_state <> 'provider_waiting'
           OR execution_owner IS NOT NULL OR execution_expiry IS NOT NULL THEN
            RAISE EXCEPTION 'remote provider waiting must release executor ownership atomically';
        END IF;
        RETURN NULL;
    END IF;
    IF (task_state = 'artifact_ready' AND execution_state <> 'succeeded')
       OR (task_state IN ('failed', 'uncertain', 'canceled')
           AND execution_state IS DISTINCT FROM task_state)
       OR submission_state IS DISTINCT FROM execution_state THEN
        RAISE EXCEPTION 'canonical terminal projection conflicts with remote task evidence';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION enforce_provider_terminal_observation_projection() RETURNS TRIGGER AS $$
DECLARE
    task_state TEXT;
    task_observation UUID;
    execution_state TEXT;
    submission_state TEXT;
    submission_manifest UUID;
    decision_manifest UUID;
BEGIN
    IF NEW.observed_state = 'provider_waiting' THEN
        RETURN NULL;
    END IF;

    SELECT task.state, task.state_observation_id,
           execution.state, submission.state, submission.result_manifest_id,
           decision.result_manifest_id
    INTO task_state, task_observation, execution_state, submission_state,
         submission_manifest, decision_manifest
    FROM provider_remote_tasks task
    JOIN executor_executions execution
      ON execution.executor_execution_id = task.executor_execution_id
     AND execution.submission_id = task.submission_id
    JOIN provider_submissions submission
      ON submission.executor_execution_id = task.executor_execution_id
     AND submission.submission_id = task.submission_id
    LEFT JOIN executor_resolution_decisions decision
      ON decision.decision_id = execution.resolution_decision_id
     AND decision.executor_execution_id = execution.executor_execution_id
     AND decision.submission_id = execution.submission_id
    WHERE task.executor_execution_id = NEW.executor_execution_id
      AND task.submission_id = NEW.submission_id;

    IF task_observation IS DISTINCT FROM NEW.observation_id
       OR task_state IS DISTINCT FROM NEW.observed_state
       OR submission_state IS DISTINCT FROM execution_state
       OR (NEW.observed_state = 'artifact_ready' AND (
            execution_state <> 'succeeded'
            OR submission_manifest IS DISTINCT FROM NEW.result_manifest_id
            OR decision_manifest IS DISTINCT FROM NEW.result_manifest_id
       ))
       OR (NEW.observed_state IN ('failed', 'canceled', 'uncertain')
           AND execution_state IS DISTINCT FROM NEW.observed_state) THEN
        RAISE EXCEPTION
            'terminal provider observation must project canonical resolution atomically';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_terminal_observation_projection_check
    AFTER INSERT ON provider_task_observations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_terminal_observation_projection();
