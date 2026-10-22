-- Dreamina video URL evidence is a separate delivery contract. It never creates
-- a media manifest, claims a media checksum, or inherits the blob retention TTL.
CREATE TABLE provider_remote_video_results (
    remote_result_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    submission_id UUID NOT NULL UNIQUE,
    video_url TEXT NOT NULL CHECK (
        octet_length(video_url) BETWEEN 1 AND 8192
        AND video_url ~* '^https://[^/?#@[:space:]]+([/?][^#]*)?$'
        AND video_url !~ '[[:cntrl:][:space:]]'
        AND position(chr(92) in video_url) = 0
    ),
    expires_at_ms BIGINT,
    evidence_sha256_hex TEXT NOT NULL CHECK (evidence_sha256_hex ~ '^[0-9a-f]{64}$'),
    observed_at_ms BIGINT NOT NULL CHECK (observed_at_ms > 0),
    CHECK (remote_result_id = executor_execution_id),
    CHECK (evidence_sha256_hex = encode(sha256(
        convert_to('dreamina-remote-video-result-v1', 'UTF8')
        || int8send(octet_length(video_url)::BIGINT) || convert_to(video_url, 'UTF8')
        || int8send(COALESCE(expires_at_ms, -1))
    ), 'hex')),
    CHECK (expires_at_ms IS NULL OR expires_at_ms > observed_at_ms),
    UNIQUE (remote_result_id, executor_execution_id, submission_id),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES executor_executions(executor_execution_id, submission_id) ON DELETE RESTRICT
);

ALTER TABLE provider_task_observations
    ADD COLUMN remote_result_id UUID,
    ADD CONSTRAINT provider_task_observations_remote_video_identity_fk
        FOREIGN KEY (remote_result_id, executor_execution_id, submission_id)
        REFERENCES provider_remote_video_results(remote_result_id, executor_execution_id, submission_id)
        ON DELETE RESTRICT;

ALTER TABLE executor_resolution_decisions
    ADD COLUMN remote_result_id UUID,
    ADD CONSTRAINT executor_resolution_decisions_remote_video_identity_fk
        FOREIGN KEY (remote_result_id, executor_execution_id, submission_id)
        REFERENCES provider_remote_video_results(remote_result_id, executor_execution_id, submission_id)
        ON DELETE RESTRICT;

ALTER TABLE provider_submissions
    ADD COLUMN remote_result_id UUID,
    ADD CONSTRAINT provider_submissions_remote_video_identity_fk
        FOREIGN KEY (remote_result_id, executor_execution_id, submission_id)
        REFERENCES provider_remote_video_results(remote_result_id, executor_execution_id, submission_id)
        ON DELETE RESTRICT;

ALTER TABLE executor_terminal_reductions
    ADD COLUMN remote_result_id UUID,
    ADD CONSTRAINT executor_terminal_reductions_remote_video_identity_fk
        FOREIGN KEY (remote_result_id, executor_execution_id, submission_id)
        REFERENCES provider_remote_video_results(remote_result_id, executor_execution_id, submission_id)
        ON DELETE RESTRICT;

ALTER TABLE provider_submissions DROP CONSTRAINT provider_submissions_lifecycle_check,
    ADD CONSTRAINT provider_submissions_lifecycle_check CHECK ((((state = 'prepared'::text) AND (started_at_ms IS NULL) AND (finished_at_ms IS NULL) AND (result_manifest_id IS NULL AND remote_result_id IS NULL) AND (error_code IS NULL)) OR ((state = ANY (ARRAY['running'::text, 'provider_waiting'::text])) AND (started_at_ms IS NOT NULL) AND (finished_at_ms IS NULL) AND (result_manifest_id IS NULL AND remote_result_id IS NULL) AND (error_code IS NULL)) OR ((state = 'succeeded'::text) AND (started_at_ms IS NOT NULL) AND (finished_at_ms IS NOT NULL) AND ((result_manifest_id IS NOT NULL) <> (remote_result_id IS NOT NULL)) AND (error_code IS NULL)) OR ((state = ANY (ARRAY['failed'::text, 'uncertain'::text])) AND (started_at_ms IS NOT NULL) AND (finished_at_ms IS NOT NULL) AND (result_manifest_id IS NULL AND remote_result_id IS NULL) AND (error_code IS NOT NULL)) OR ((state = 'canceled'::text) AND (finished_at_ms IS NOT NULL) AND (result_manifest_id IS NULL AND remote_result_id IS NULL) AND (error_code IS NOT NULL))));

ALTER TABLE executor_resolution_decisions DROP CONSTRAINT executor_resolution_decisions_check1,
    ADD CONSTRAINT executor_resolution_decisions_check1 CHECK ((((resolved_state = 'succeeded'::text) AND ((result_manifest_id IS NOT NULL) <> (remote_result_id IS NOT NULL)) AND (error_code IS NULL)) OR ((resolved_state = ANY (ARRAY['failed'::text, 'uncertain'::text, 'canceled'::text])) AND (result_manifest_id IS NULL AND remote_result_id IS NULL) AND (error_code IS NOT NULL))));

ALTER TABLE provider_remote_tasks DROP CONSTRAINT provider_remote_tasks_state_check,
    ADD CONSTRAINT provider_remote_tasks_state_check CHECK ((state = ANY (ARRAY['provider_waiting'::text, 'artifact_ready'::text, 'remote_ready'::text, 'failed'::text, 'canceled'::text, 'uncertain'::text])));

ALTER TABLE provider_task_observations DROP CONSTRAINT provider_task_observations_observed_state_check,
    ADD CONSTRAINT provider_task_observations_observed_state_check CHECK ((observed_state = ANY (ARRAY['provider_waiting'::text, 'artifact_ready'::text, 'remote_ready'::text, 'failed'::text, 'canceled'::text, 'uncertain'::text])));

ALTER TABLE provider_remote_tasks DROP CONSTRAINT provider_remote_tasks_check,
    ADD CONSTRAINT provider_remote_tasks_check CHECK ((((state = 'provider_waiting'::text) AND (artifact_ref IS NULL) AND (error_code IS NULL) AND (effect_certainty = 'not_applicable'::text) AND (next_poll_at_ms IS NOT NULL) AND (terminal_at_ms IS NULL)) OR ((state = 'artifact_ready'::text) AND (artifact_ref IS NOT NULL) AND (error_code IS NULL) AND (effect_certainty = 'not_applicable'::text) AND (next_poll_at_ms IS NULL) AND (terminal_at_ms IS NOT NULL)) OR ((state = 'failed'::text) AND (artifact_ref IS NULL) AND (error_code IS NOT NULL) AND (effect_certainty = 'not_applicable'::text) AND (next_poll_at_ms IS NULL) AND (terminal_at_ms IS NOT NULL)) OR ((state = 'canceled'::text) AND (artifact_ref IS NULL) AND (error_code IS NOT NULL) AND (effect_certainty = 'confirmed_no_effect'::text) AND (next_poll_at_ms IS NULL) AND (terminal_at_ms IS NOT NULL) AND cancel_requested) OR ((state = 'uncertain'::text) AND (artifact_ref IS NULL) AND (error_code IS NOT NULL) AND (effect_certainty = 'unknown_remote_effect'::text) AND (next_poll_at_ms IS NULL) AND (terminal_at_ms IS NOT NULL))) OR (state = 'remote_ready' AND artifact_ref IS NULL AND error_code IS NULL AND effect_certainty = 'not_applicable' AND next_poll_at_ms IS NULL AND terminal_at_ms IS NOT NULL));

ALTER TABLE provider_task_observations DROP CONSTRAINT provider_task_observations_check1,
    ADD CONSTRAINT provider_task_observations_check1 CHECK ((((observed_state = 'provider_waiting'::text) AND (artifact_ref IS NULL) AND (error_code IS NULL) AND (effect_certainty = 'not_applicable'::text) AND ((next_poll_at_ms >= observed_at_ms) AND (next_poll_at_ms <= (observed_at_ms + 86400000)))) OR ((observed_state = 'artifact_ready'::text) AND (artifact_ref IS NOT NULL) AND (error_code IS NULL) AND (effect_certainty = 'not_applicable'::text) AND (next_poll_at_ms IS NULL)) OR ((observed_state = 'failed'::text) AND (artifact_ref IS NULL) AND (error_code IS NOT NULL) AND (effect_certainty = 'not_applicable'::text) AND (next_poll_at_ms IS NULL)) OR ((observed_state = 'canceled'::text) AND (artifact_ref IS NULL) AND (error_code IS NOT NULL) AND (effect_certainty = 'confirmed_no_effect'::text) AND (next_poll_at_ms IS NULL)) OR ((observed_state = 'uncertain'::text) AND (artifact_ref IS NULL) AND (error_code IS NOT NULL) AND (effect_certainty = 'unknown_remote_effect'::text) AND (next_poll_at_ms IS NULL))) OR (observed_state = 'remote_ready' AND artifact_ref IS NULL AND error_code IS NULL AND effect_certainty = 'not_applicable' AND next_poll_at_ms IS NULL));

ALTER TABLE executor_terminal_reductions DROP CONSTRAINT executor_terminal_reduction_completion_shape,
    ADD CONSTRAINT executor_terminal_reduction_completion_shape CHECK ((((state = ANY (ARRAY['ready'::text, 'leased'::text, 'blocked'::text])) AND (completion_owner IS NULL) AND (provider_receipt_id IS NULL) AND (customer_artifact_id IS NULL AND remote_result_id IS NULL) AND (quota_reservation_id IS NULL)) OR ((state = 'completed'::text) AND (completion_owner IS NOT NULL) AND (provider_receipt_id IS NOT NULL) AND (quota_reservation_id IS NOT NULL) AND ((resolved_state = 'succeeded'::text) = ((customer_artifact_id IS NOT NULL) <> (remote_result_id IS NOT NULL))))));


ALTER TABLE provider_task_observations ADD CONSTRAINT provider_task_observations_remote_video_shape
    CHECK ((observed_state = 'remote_ready') = (remote_result_id IS NOT NULL));
ALTER TABLE executor_resolution_decisions ADD CONSTRAINT executor_resolution_remote_video_source
    CHECK (remote_result_id IS NULL OR source = 'remote_provider_observation');
ALTER TABLE executor_terminal_reductions ADD CONSTRAINT terminal_remote_video_success
    CHECK (remote_result_id IS NULL OR (state = 'completed' AND resolved_state = 'succeeded'));
-- The existing fingerprint describes local runner manifest/error evidence and
-- remains unchanged (including its PostgreSQL 16-compatible generated expression).
-- Remote success has no local fingerprint; its bound remote_result_id is canonical.
ALTER TABLE job_response_projections
    ADD COLUMN remote_result_count INTEGER NOT NULL DEFAULT 0 CHECK (remote_result_count BETWEEN 0 AND 1),
    DROP CONSTRAINT job_response_projections_artifact_count_check,
    ADD CONSTRAINT job_response_projections_delivery_count CHECK (
        artifact_count >= 0 AND artifact_count + remote_result_count > 0
        AND (remote_result_count = 0 OR (artifact_count = 0 AND operation = 'video_generation'))
    );

CREATE FUNCTION validate_provider_remote_video_result() RETURNS TRIGGER AS $$
DECLARE
    task provider_remote_tasks%ROWTYPE;
BEGIN
    SELECT * INTO STRICT task FROM provider_remote_tasks
    WHERE submission_id = NEW.submission_id AND executor_execution_id = NEW.executor_execution_id
    FOR UPDATE;
    IF task.provider_id <> 'dreamina-cli' OR task.state <> 'provider_waiting'
       OR task.poll_owner IS NULL OR task.poll_lease_expires_at_ms <= NEW.observed_at_ms
       OR task.provider_deadline_at_ms <= NEW.observed_at_ms
       OR NOT EXISTS (
            SELECT 1 FROM provider_submissions submission JOIN jobs job USING (job_id)
            WHERE submission.submission_id = NEW.submission_id
              AND submission.executor_execution_id = NEW.executor_execution_id
              AND submission.provider_id = 'dreamina-cli' AND job.operation = 'video_generation'
              AND submission.state = 'provider_waiting'
       )
       OR EXISTS (SELECT 1 FROM executor_result_manifests WHERE executor_execution_id = NEW.executor_execution_id)
       OR EXISTS (SELECT 1 FROM executor_artifact_authorities WHERE executor_execution_id = NEW.executor_execution_id)
    THEN RAISE EXCEPTION 'remote video result requires a live Dreamina video poll without local artifact evidence';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER provider_remote_video_result_insert_guard BEFORE INSERT ON provider_remote_video_results
    FOR EACH ROW EXECUTE FUNCTION validate_provider_remote_video_result();
CREATE TRIGGER provider_remote_video_result_immutable BEFORE UPDATE OR DELETE ON provider_remote_video_results
    FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation();
CREATE TRIGGER provider_remote_video_result_no_truncate BEFORE TRUNCATE ON provider_remote_video_results
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();

CREATE FUNCTION enforce_remote_video_result_projection() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM executor_result_manifests WHERE executor_execution_id = NEW.executor_execution_id)
       OR EXISTS (SELECT 1 FROM executor_artifact_authorities WHERE executor_execution_id = NEW.executor_execution_id)
       OR NOT EXISTS (
        SELECT 1 FROM provider_remote_tasks task
        JOIN provider_task_observations observation ON observation.observation_id = task.state_observation_id
        JOIN provider_submissions submission ON submission.submission_id = task.submission_id
          AND submission.executor_execution_id = task.executor_execution_id
        JOIN executor_resolution_decisions decision ON decision.decision_id = submission.resolution_decision_id
        WHERE task.executor_execution_id = NEW.executor_execution_id AND task.submission_id = NEW.submission_id
          AND task.state = 'remote_ready' AND observation.observed_state = 'remote_ready'
          AND observation.remote_result_id = NEW.remote_result_id
          AND submission.state = 'succeeded' AND submission.remote_result_id = NEW.remote_result_id
          AND decision.resolved_state = 'succeeded' AND decision.remote_result_id = NEW.remote_result_id
    ) THEN RAISE EXCEPTION 'remote video evidence must resolve atomically without a local media manifest';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER provider_remote_video_result_projection
    AFTER INSERT ON provider_remote_video_results DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_remote_video_result_projection();

-- Serialize both delivery contracts on the same remote task row. This also
-- protects direct SQL inserts and the ordering of concurrent local/remote writers.
CREATE FUNCTION prevent_local_artifact_after_remote_video() RETURNS TRIGGER AS $$
BEGIN
    PERFORM 1 FROM provider_remote_tasks
    WHERE executor_execution_id = NEW.executor_execution_id
      AND submission_id = NEW.submission_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM provider_remote_video_results
               WHERE executor_execution_id = NEW.executor_execution_id)
    THEN RAISE EXCEPTION 'local artifact evidence conflicts with canonical remote video'; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER executor_artifact_authorities_remote_video_guard
    BEFORE INSERT ON executor_artifact_authorities
    FOR EACH ROW EXECUTE FUNCTION prevent_local_artifact_after_remote_video();
CREATE TRIGGER executor_result_manifests_remote_video_guard
    BEFORE INSERT ON executor_result_manifests
    FOR EACH ROW EXECUTE FUNCTION prevent_local_artifact_after_remote_video();

-- Preserve all existing fences while extending validate_provider_task_observation.
DO $migration$
DECLARE
    definition TEXT := pg_get_functiondef('validate_provider_task_observation()'::REGPROCEDURE);
    patched TEXT;
BEGIN
    patched := replace(definition, $old$    IF NEW.observed_state = 'artifact_ready' AND NOT EXISTS ($old$, $new$    IF NEW.observed_state = 'remote_ready' AND (
        NEW.source <> 'poll' OR task.provider_id <> 'dreamina-cli'
        OR NOT EXISTS (
            SELECT 1 FROM provider_remote_video_results remote
            WHERE remote.remote_result_id = NEW.remote_result_id
              AND remote.executor_execution_id = NEW.executor_execution_id
              AND remote.submission_id = NEW.submission_id
        ) OR EXISTS (SELECT 1 FROM executor_result_manifests WHERE executor_execution_id = NEW.executor_execution_id)
       OR EXISTS (SELECT 1 FROM executor_artifact_authorities WHERE executor_execution_id = NEW.executor_execution_id)
    ) THEN RAISE EXCEPTION 'remote_ready requires canonical Dreamina video URL evidence'; END IF;
    IF NEW.observed_state = 'artifact_ready' AND NOT EXISTS ($new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected validate_provider_task_observation guard 1'; END IF;
    definition := patched;
    EXECUTE definition;
END;
$migration$;

-- Preserve all existing fences while extending enforce_provider_remote_task_update.
DO $migration$
DECLARE
    definition TEXT := pg_get_functiondef('enforce_provider_remote_task_update()'::REGPROCEDURE);
    patched TEXT;
BEGIN
    patched := replace(definition, $old$OLD.state IN ('artifact_ready', 'failed', 'canceled', 'uncertain')$old$, $new$OLD.state IN ('artifact_ready', 'remote_ready', 'failed', 'canceled', 'uncertain')$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_provider_remote_task_update guard 1'; END IF;
    definition := patched;
    EXECUTE definition;
END;
$migration$;

-- Preserve all existing fences while extending enforce_provider_remote_task_projection.
DO $migration$
DECLARE
    definition TEXT := pg_get_functiondef('enforce_provider_remote_task_projection()'::REGPROCEDURE);
    patched TEXT;
BEGIN
    patched := replace(definition, $old$task_state = 'artifact_ready' AND execution_state <> 'succeeded'$old$, $new$task_state IN ('artifact_ready', 'remote_ready') AND execution_state <> 'succeeded'$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_provider_remote_task_projection guard 1'; END IF;
    definition := patched;
    EXECUTE definition;
END;
$migration$;

-- Preserve all existing fences while extending enforce_provider_terminal_observation_projection.
DO $migration$
DECLARE
    definition TEXT := pg_get_functiondef('enforce_provider_terminal_observation_projection()'::REGPROCEDURE);
    patched TEXT;
BEGIN
    patched := replace(definition, $old$    decision_manifest UUID;$old$, $new$    decision_manifest UUID;
    submission_remote UUID;
    decision_remote UUID;$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_provider_terminal_observation_projection guard 1'; END IF;
    definition := patched;
    patched := replace(definition, $old$           decision.result_manifest_id$old$, $new$           decision.result_manifest_id, submission.remote_result_id, decision.remote_result_id$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_provider_terminal_observation_projection guard 2'; END IF;
    definition := patched;
    patched := replace(definition, $old$         submission_manifest, decision_manifest$old$, $new$         submission_manifest, decision_manifest, submission_remote, decision_remote$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_provider_terminal_observation_projection guard 3'; END IF;
    definition := patched;
    patched := replace(definition, $old$       OR (NEW.observed_state IN ('failed', 'canceled', 'uncertain')$old$, $new$       OR (NEW.observed_state = 'remote_ready' AND (
            execution_state <> 'succeeded'
            OR submission_remote IS DISTINCT FROM NEW.remote_result_id
            OR decision_remote IS DISTINCT FROM NEW.remote_result_id
       ))
       OR (NEW.observed_state IN ('failed', 'canceled', 'uncertain')$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_provider_terminal_observation_projection guard 4'; END IF;
    definition := patched;
    EXECUTE definition;
END;
$migration$;

-- Preserve all existing fences while extending enforce_executor_resolution_projection.
DO $migration$
DECLARE
    definition TEXT := pg_get_functiondef('enforce_executor_resolution_projection()'::REGPROCEDURE);
    patched TEXT;
BEGIN
    patched := replace(definition, $old$    decision_manifest UUID;$old$, $new$    decision_manifest UUID;
    submission_remote UUID;
    decision_remote UUID;$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_executor_resolution_projection guard 1'; END IF;
    definition := patched;
    patched := replace(definition, $old$s.result_manifest_id, s.error_code$old$, $new$s.result_manifest_id, s.error_code, s.remote_result_id$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_executor_resolution_projection guard 2'; END IF;
    definition := patched;
    patched := replace(definition, $old$submission_manifest, submission_error
$old$, $new$submission_manifest, submission_error, submission_remote
$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_executor_resolution_projection guard 3'; END IF;
    definition := patched;
    patched := replace(definition, $old$SELECT result_manifest_id, error_code
$old$, $new$SELECT result_manifest_id, error_code, remote_result_id
$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_executor_resolution_projection guard 4'; END IF;
    definition := patched;
    patched := replace(definition, $old$INTO decision_manifest, decision_error
$old$, $new$INTO decision_manifest, decision_error, decision_remote
$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_executor_resolution_projection guard 5'; END IF;
    definition := patched;
    patched := replace(definition, $old$IF submission_manifest IS DISTINCT FROM decision_manifest$old$, $new$IF submission_remote IS DISTINCT FROM decision_remote
           OR submission_manifest IS DISTINCT FROM decision_manifest$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected enforce_executor_resolution_projection guard 6'; END IF;
    definition := patched;
    EXECUTE definition;
END;
$migration$;

-- Preserve all existing fences while extending validate_executor_resolution_decision.
DO $migration$
DECLARE
    definition TEXT := pg_get_functiondef('validate_executor_resolution_decision()'::REGPROCEDURE);
    patched TEXT;
BEGIN
    patched := replace(definition, $old$remote_state = 'artifact_ready'$old$, $new$remote_state IN ('artifact_ready', 'remote_ready')$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected validate_executor_resolution_decision guard 1'; END IF;
    definition := patched;
    EXECUTE definition;
END;
$migration$;

-- Preserve all existing fences while extending validate_executor_terminal_reduction_completion.
DO $migration$
DECLARE
    definition TEXT := pg_get_functiondef('validate_executor_terminal_reduction_completion()'::REGPROCEDURE);
    patched TEXT;
BEGIN
    patched := replace(definition, $old$    IF NEW.resolved_state = 'succeeded' THEN$old$, $new$    IF NEW.resolved_state = 'succeeded' AND NEW.remote_result_id IS NOT NULL THEN
        IF NEW.customer_artifact_id IS NOT NULL
           OR submission_row.remote_result_id IS DISTINCT FROM NEW.remote_result_id
           OR submission_row.result_manifest_id IS NOT NULL
           OR NOT EXISTS (
                SELECT 1 FROM provider_remote_video_results remote
                JOIN executor_resolution_decisions decision
                  ON decision.decision_id = NEW.resolution_decision_id
                 AND decision.remote_result_id = remote.remote_result_id
                WHERE remote.remote_result_id = NEW.remote_result_id
                  AND remote.executor_execution_id = NEW.executor_execution_id
                  AND remote.submission_id = NEW.submission_id
                  AND receipt_row.evidence -> 'remote_video' ->> 'remote_result_id' = remote.remote_result_id::TEXT
                  AND receipt_row.evidence -> 'remote_video' ->> 'evidence_sha256_hex' = remote.evidence_sha256_hex
                  AND NOT (receipt_row.evidence ? 'artifact')
           ) THEN RAISE EXCEPTION 'completed remote video reduction is not canonical'; END IF;
    ELSIF NEW.resolved_state = 'succeeded' THEN$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected validate_executor_terminal_reduction_completion guard 1'; END IF;
    definition := patched;
    EXECUTE definition;
END;
$migration$;

-- Preserve all existing fences while extending assert_terminal_parent_completion.
DO $migration$
DECLARE
    definition TEXT := pg_get_functiondef('assert_terminal_parent_completion(uuid)'::REGPROCEDURE);
    patched TEXT;
BEGIN
    patched := replace(definition, $old$    artifact_count BIGINT;$old$, $new$    local_artifact_count_value BIGINT;
    remote_result_count_value BIGINT;$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected assert_terminal_parent_completion guard 1'; END IF;
    definition := patched;
    patched := replace(definition, $old$INTO artifact_count FROM artifacts$old$, $new$INTO local_artifact_count_value FROM artifacts$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected local artifact count selection'; END IF;
    definition := patched;
    patched := replace(definition, $old$    SELECT COUNT(*) INTO projection_count$old$, $new$    SELECT COUNT(*) INTO remote_result_count_value
    FROM executor_terminal_reductions reduction
    JOIN provider_submissions submission USING (submission_id, executor_execution_id)
    WHERE submission.job_id = target_job_id AND reduction.state = 'completed'
      AND reduction.resolved_state = 'succeeded' AND reduction.remote_result_id IS NOT NULL;
    IF EXISTS (SELECT 1 FROM job_response_projections projection WHERE projection.job_id = target_job_id
        AND (projection.artifact_count <> local_artifact_count_value OR projection.remote_result_count <> remote_result_count_value))
    THEN RAISE EXCEPTION 'response delivery counts must match completed results'; END IF;
    SELECT COUNT(*) INTO projection_count$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected assert_terminal_parent_completion guard 2'; END IF;
    definition := patched;
    patched := replace(definition, $old$artifact_count <> succeeded_count$old$, $new$local_artifact_count_value + remote_result_count_value <> succeeded_count$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected assert_terminal_parent_completion guard 3'; END IF;
    definition := patched;
    EXECUTE definition;
END;
$migration$;

-- Preserve all existing fences while extending create_job_artifact_retention.
DO $migration$
DECLARE
    definition TEXT := pg_get_functiondef('create_job_artifact_retention()'::REGPROCEDURE);
    patched TEXT;
BEGIN
    patched := replace(definition, $old$BEGIN
    SELECT * INTO STRICT policy$old$, $new$BEGIN
    IF NEW.artifact_count = 0 THEN RETURN NEW; END IF;
    SELECT * INTO STRICT policy$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected create_job_artifact_retention guard 1'; END IF;
    definition := patched;
    EXECUTE definition;
END;
$migration$;
