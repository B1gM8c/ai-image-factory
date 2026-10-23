-- Native image batches keep one real provider operation and N existing output
-- partitions. Followers never acquire launch authority or synthetic remote IDs.
CREATE TABLE dreamina_native_batches (
    leader_submission_id UUID PRIMARY KEY REFERENCES provider_submissions(submission_id) ON DELETE RESTRICT,
    job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE RESTRICT,
    output_count INTEGER NOT NULL CHECK (output_count BETWEEN 2 AND 10),
    command_hash TEXT NOT NULL CHECK (command_hash ~ '^[0-9a-f]{64}$'),
    created_at_ms BIGINT NOT NULL
);
CREATE TABLE dreamina_native_batch_members (
    submission_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    leader_submission_id UUID NOT NULL REFERENCES dreamina_native_batches(leader_submission_id) ON DELETE RESTRICT,
    output_index INTEGER NOT NULL CHECK (output_index BETWEEN 0 AND 9),
    UNIQUE (leader_submission_id, output_index),
    UNIQUE (submission_id, executor_execution_id),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES executor_executions(executor_execution_id, submission_id) ON DELETE RESTRICT,
    CHECK ((output_index = 0) = (submission_id = leader_submission_id))
);
CREATE TABLE dreamina_native_batch_results (
    submission_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    resolved_state TEXT NOT NULL CHECK (resolved_state IN ('succeeded', 'failed', 'uncertain', 'canceled')),
    result_manifest_id UUID,
    error_code TEXT,
    observed_at_ms BIGINT NOT NULL,
    FOREIGN KEY (submission_id, executor_execution_id)
        REFERENCES dreamina_native_batch_members(submission_id, executor_execution_id) ON DELETE RESTRICT,
    FOREIGN KEY (result_manifest_id, executor_execution_id, submission_id)
        REFERENCES executor_result_manifests(manifest_id, executor_execution_id, submission_id) ON DELETE RESTRICT,
    CHECK ((resolved_state = 'succeeded' AND result_manifest_id IS NOT NULL AND error_code IS NULL)
        OR (resolved_state <> 'succeeded' AND result_manifest_id IS NULL AND error_code IS NOT NULL))
);

CREATE FUNCTION validate_dreamina_native_batch() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM provider_submissions leader
        JOIN job_outputs output ON output.output_id = leader.output_id AND output.job_id = leader.job_id
        JOIN jobs job ON job.job_id = leader.job_id
        JOIN job_payloads payload ON payload.job_id = job.job_id
        JOIN executor_executions execution USING (submission_id, executor_execution_id)
        WHERE leader.submission_id = NEW.leader_submission_id AND leader.job_id = NEW.job_id
          AND leader.provider_id = 'dreamina-cli' AND leader.state = 'prepared'
          AND execution.state = 'prepared' AND output.output_index = 0
          AND job.operation IN ('generation', 'edit') AND job.output_count = NEW.output_count
          AND payload.command_schema = 'dreamina-cli.submit.v1'
          AND payload.command_json ->> 'schema_version' = '2'
          AND payload.command_json ->> 'operation' IN ('text2image', 'image2image')
          AND payload.command_json ->> 'generate_num' = NEW.output_count::TEXT
          AND leader.command_hash = NEW.command_hash
    ) THEN RAISE EXCEPTION 'native batch requires a frozen version 2 Dreamina image command'; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_native_batch_insert_guard BEFORE INSERT ON dreamina_native_batches
    FOR EACH ROW EXECUTE FUNCTION validate_dreamina_native_batch();

CREATE FUNCTION validate_dreamina_native_batch_member() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM dreamina_native_batches batch
        JOIN provider_submissions leader ON leader.submission_id = batch.leader_submission_id
        JOIN provider_submissions member ON member.submission_id = NEW.submission_id
        JOIN executor_executions execution ON execution.executor_execution_id = NEW.executor_execution_id
          AND execution.submission_id = member.submission_id
        JOIN job_outputs output ON output.output_id = member.output_id AND output.job_id = batch.job_id
        WHERE batch.leader_submission_id = NEW.leader_submission_id
          AND NEW.output_index < batch.output_count AND output.output_index = NEW.output_index
          AND member.job_id = batch.job_id AND member.provider_id = 'dreamina-cli'
          AND member.command_hash = batch.command_hash AND member.state = 'prepared' AND execution.state = 'prepared'
          AND member.execution_profile_id = leader.execution_profile_id
          AND member.provider_account_id = leader.provider_account_id
          AND member.work_item_id = leader.work_item_id
    ) THEN RAISE EXCEPTION 'native batch member must match its frozen job and execution profile'; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_native_batch_member_insert_guard BEFORE INSERT ON dreamina_native_batch_members
    FOR EACH ROW EXECUTE FUNCTION validate_dreamina_native_batch_member();

CREATE FUNCTION assert_dreamina_native_batch_membership() RETURNS TRIGGER AS $$
DECLARE batch_id UUID; expected INTEGER; actual BIGINT;
BEGIN
    batch_id := NEW.leader_submission_id;
    SELECT output_count INTO STRICT expected FROM dreamina_native_batches WHERE leader_submission_id = batch_id;
    SELECT count(*) INTO actual FROM dreamina_native_batch_members WHERE leader_submission_id = batch_id;
    IF actual <> expected THEN RAISE EXCEPTION 'native batch membership must be complete atomically'; END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER dreamina_native_batch_complete AFTER INSERT ON dreamina_native_batches
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION assert_dreamina_native_batch_membership();
CREATE CONSTRAINT TRIGGER dreamina_native_batch_members_complete AFTER INSERT ON dreamina_native_batch_members
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION assert_dreamina_native_batch_membership();

-- Results may be published under a live leader poll fence, or synthesized from
-- the leader's already canonical failure. Their immutable manifest is per image.
CREATE FUNCTION validate_dreamina_native_batch_result() RETURNS TRIGGER AS $$
DECLARE leader provider_submissions%ROWTYPE;
BEGIN
    SELECT submission.* INTO STRICT leader FROM dreamina_native_batch_members member
    JOIN provider_submissions submission ON submission.submission_id = member.leader_submission_id
    WHERE member.submission_id = NEW.submission_id AND member.executor_execution_id = NEW.executor_execution_id
      AND member.output_index > 0;
    IF NEW.resolved_state <> 'succeeded' AND EXISTS (SELECT 1 FROM executor_result_manifests WHERE submission_id = NEW.submission_id) THEN
        RAISE EXCEPTION 'native failure must not discard a published member artifact';
    END IF;
    IF leader.state IN ('succeeded', 'failed', 'uncertain', 'canceled') THEN
        IF NEW.resolved_state <> leader.state OR NEW.error_code IS DISTINCT FROM leader.error_code
           OR NEW.resolved_state = 'succeeded' THEN
            RAISE EXCEPTION 'post-terminal native result requires exact leader failure';
        END IF;
    ELSIF NOT EXISTS (
        SELECT 1 FROM provider_remote_tasks task WHERE task.submission_id = leader.submission_id
          AND task.state = 'provider_waiting' AND task.poll_owner IS NOT NULL
          AND task.poll_lease_expires_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
          AND task.provider_deadline_at_ms > NEW.observed_at_ms
          AND task.provider_deadline_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
    ) THEN RAISE EXCEPTION 'native result requires a live leader poll fence'; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_native_batch_result_insert_guard BEFORE INSERT ON dreamina_native_batch_results
    FOR EACH ROW EXECUTE FUNCTION validate_dreamina_native_batch_result();

CREATE FUNCTION validate_dreamina_native_member_artifact() RETURNS TRIGGER AS $$
BEGIN
    -- Publish the leader last. Once its immutable artifact exists, deadline
    -- recovery can always resolve the entire batch without another provider call.
    IF EXISTS (SELECT 1 FROM dreamina_native_batches WHERE leader_submission_id = NEW.submission_id)
       AND EXISTS (
        SELECT 1 FROM dreamina_native_batches batch
        WHERE batch.leader_submission_id = NEW.submission_id
          AND (SELECT count(*) FROM dreamina_native_batch_members member
               JOIN dreamina_native_batch_results result USING (submission_id, executor_execution_id)
               WHERE member.leader_submission_id = batch.leader_submission_id AND member.output_index > 0)
              <> batch.output_count - 1
    ) THEN RAISE EXCEPTION 'native leader artifact requires every follower result first'; END IF;
    IF EXISTS (SELECT 1 FROM dreamina_native_batch_members WHERE submission_id = NEW.submission_id AND output_index > 0)
       AND NOT EXISTS (
        SELECT 1 FROM dreamina_native_batch_members member
        JOIN provider_remote_tasks task ON task.submission_id = member.leader_submission_id
        WHERE member.submission_id = NEW.submission_id AND member.executor_execution_id = NEW.executor_execution_id
          AND task.state = 'provider_waiting' AND task.poll_owner IS NOT NULL
          AND task.poll_lease_expires_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
          AND task.provider_deadline_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
    ) THEN RAISE EXCEPTION 'native member artifact requires a live leader poll'; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER executor_native_member_artifact_guard BEFORE INSERT ON executor_artifact_authorities
    FOR EACH ROW EXECUTE FUNCTION validate_dreamina_native_member_artifact();
CREATE TRIGGER executor_native_member_manifest_guard BEFORE INSERT ON executor_result_manifests
    FOR EACH ROW EXECUTE FUNCTION validate_dreamina_native_member_artifact();
CREATE FUNCTION assert_dreamina_native_member_artifact_result() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM dreamina_native_batch_members WHERE submission_id = NEW.submission_id AND output_index > 0)
       AND NOT EXISTS (SELECT 1 FROM dreamina_native_batch_results result
            WHERE result.submission_id = NEW.submission_id AND result.executor_execution_id = NEW.executor_execution_id
              AND result.resolved_state = 'succeeded' AND result.result_manifest_id = NEW.submission_id)
    THEN RAISE EXCEPTION 'native member artifact requires its atomic immutable success result'; END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER executor_native_member_artifact_result AFTER INSERT ON executor_artifact_authorities
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION assert_dreamina_native_member_artifact_result();

DO $migration$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['dreamina_native_batches', 'dreamina_native_batch_members', 'dreamina_native_batch_results'] LOOP
        EXECUTE format('CREATE TRIGGER %I_reject_mutation BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation()', table_name, table_name);
        EXECUTE format('CREATE TRIGGER %I_reject_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation()', table_name, table_name);
    END LOOP;
END;
$migration$;

ALTER TABLE executor_resolution_decisions ADD COLUMN dreamina_batch_result_id UUID
    REFERENCES dreamina_native_batch_results(submission_id) ON DELETE RESTRICT;
-- Extend the existing source/evidence constraints without changing any old arm.
DO $migration$
DECLARE definition TEXT;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO definition FROM pg_constraint
    WHERE conrelid = 'executor_resolution_decisions'::REGCLASS AND conname = 'executor_resolution_decisions_source_check';
    ALTER TABLE executor_resolution_decisions DROP CONSTRAINT executor_resolution_decisions_source_check;
    EXECUTE 'ALTER TABLE executor_resolution_decisions ADD CONSTRAINT executor_resolution_decisions_source_check '
        || regexp_replace(definition, '^CHECK \((.*)\)$', 'CHECK ((\1) OR source = ''dreamina_native_batch'')');
    SELECT pg_get_constraintdef(oid) INTO definition FROM pg_constraint
    WHERE conrelid = 'executor_resolution_decisions'::REGCLASS AND conname = 'executor_resolution_decisions_evidence_check';
    ALTER TABLE executor_resolution_decisions DROP CONSTRAINT executor_resolution_decisions_evidence_check;
    EXECUTE 'ALTER TABLE executor_resolution_decisions ADD CONSTRAINT executor_resolution_decisions_evidence_check '
        || regexp_replace(definition, '^CHECK \((.*)\)$', 'CHECK (((\1) AND dreamina_batch_result_id IS NULL) OR (source = ''dreamina_native_batch'' AND dreamina_batch_result_id = submission_id AND observation_id IS NULL AND provider_task_observation_id IS NULL AND provider_submit_intent_id IS NULL AND provider_remote_task_quarantine_id IS NULL))');
END;
$migration$;

CREATE FUNCTION validate_dreamina_native_batch_decision() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.source <> 'dreamina_native_batch' THEN RETURN NEW; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM dreamina_native_batch_members member
        JOIN dreamina_native_batch_results result USING (submission_id, executor_execution_id)
        JOIN provider_submissions leader ON leader.submission_id = member.leader_submission_id
        JOIN executor_resolution_decisions decision ON decision.decision_id = leader.resolution_decision_id
        JOIN provider_submissions submission ON submission.submission_id = member.submission_id
        JOIN executor_executions execution ON execution.executor_execution_id = member.executor_execution_id
        WHERE member.submission_id = NEW.submission_id AND member.executor_execution_id = NEW.executor_execution_id
          AND member.output_index > 0 AND NEW.dreamina_batch_result_id = member.submission_id
          AND leader.state IN ('succeeded', 'failed', 'uncertain', 'canceled')
          AND result.resolved_state = NEW.resolved_state
          AND result.result_manifest_id IS NOT DISTINCT FROM NEW.result_manifest_id
          AND result.error_code IS NOT DISTINCT FROM NEW.error_code
          AND submission.state = 'prepared' AND execution.state = 'prepared'
          AND execution.executor_owner IS NULL AND execution.launch_owner IS NULL AND execution.lease_epoch = 0
          AND NOT EXISTS (SELECT 1 FROM provider_remote_submit_intents WHERE submission_id = member.submission_id)
          AND NOT EXISTS (SELECT 1 FROM provider_remote_tasks WHERE submission_id = member.submission_id)
          AND NOT EXISTS (SELECT 1 FROM executor_capacity_allocations WHERE submission_id = member.submission_id)
    ) THEN RAISE EXCEPTION 'native follower decision requires its exact immutable result and terminal leader'; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_native_batch_resolution_guard BEFORE INSERT ON executor_resolution_decisions
    FOR EACH ROW EXECUTE FUNCTION validate_dreamina_native_batch_decision();

-- No new launch fence is invented for a follower. The narrow terminal shape is
-- reachable only with the canonical result/decision guarded above and below.
DO $migration$
DECLARE definition TEXT;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO definition FROM pg_constraint
    WHERE conrelid = 'executor_executions'::REGCLASS AND conname = 'executor_executions_lifecycle_check';
    ALTER TABLE executor_executions DROP CONSTRAINT executor_executions_lifecycle_check;
    EXECUTE 'ALTER TABLE executor_executions ADD CONSTRAINT executor_executions_lifecycle_check '
      || regexp_replace(definition, '^CHECK \((.*)\)$', 'CHECK ((\1) OR (state IN (''succeeded'', ''failed'', ''uncertain'', ''canceled'') AND executor_owner IS NULL AND lease_epoch = 0 AND lease_expires_at_ms IS NULL AND leased_at_ms IS NULL AND started_at_ms IS NULL AND finished_at_ms IS NOT NULL AND resolution_decision_id IS NOT NULL AND ((state = ''succeeded'') = (error_code IS NULL))))');
    SELECT pg_get_constraintdef(oid) INTO definition FROM pg_constraint
    WHERE conrelid = 'executor_executions'::REGCLASS AND conname = 'executor_execution_launch_fence_check';
    ALTER TABLE executor_executions DROP CONSTRAINT executor_execution_launch_fence_check;
    EXECUTE 'ALTER TABLE executor_executions ADD CONSTRAINT executor_execution_launch_fence_check '
      || regexp_replace(definition, '^CHECK \((.*)\)$', 'CHECK ((\1) OR (state IN (''succeeded'', ''failed'', ''uncertain'', ''canceled'') AND launch_owner IS NULL AND launch_lease_epoch IS NULL AND lease_epoch = 0 AND resolution_decision_id IS NOT NULL))');
END;
$migration$;

DO $migration$
DECLARE definition TEXT; patched TEXT;
BEGIN
    definition := pg_get_functiondef('enforce_executor_lease_updates()'::REGPROCEDURE);
    patched := replace(definition, $old$BEGIN
$old$, $new$BEGIN
    IF EXISTS (SELECT 1 FROM dreamina_native_batch_members member WHERE member.submission_id = OLD.submission_id AND member.output_index > 0) THEN
        IF OLD.state <> 'prepared' OR NEW.state NOT IN ('succeeded', 'failed', 'uncertain', 'canceled')
           OR (to_jsonb(NEW) - ARRAY['state','finished_at_ms','updated_at_ms','error_code','resolution_decision_id']) IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['state','finished_at_ms','updated_at_ms','error_code','resolution_decision_id'])
           OR NOT EXISTS (SELECT 1 FROM executor_resolution_decisions decision WHERE decision.decision_id = NEW.resolution_decision_id AND decision.submission_id = OLD.submission_id AND decision.executor_execution_id = OLD.executor_execution_id AND decision.source = 'dreamina_native_batch' AND decision.resolved_state = NEW.state AND decision.error_code IS NOT DISTINCT FROM NEW.error_code)
        THEN RAISE EXCEPTION 'native batch follower cannot acquire or launch independently'; END IF;
        RETURN NEW;
    END IF;
$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected executor lease guard'; END IF;
    EXECUTE patched;
    definition := pg_get_functiondef('enforce_provider_submission_state_transition()'::REGPROCEDURE);
    patched := replace(definition, $old$    IF NOT (
        (OLD.state = 'prepared'$old$, $new$    IF EXISTS (SELECT 1 FROM dreamina_native_batch_members member WHERE member.submission_id = OLD.submission_id AND member.output_index > 0) THEN
        IF OLD.state <> 'prepared' OR NEW.state NOT IN ('succeeded', 'failed', 'uncertain', 'canceled')
           OR NOT EXISTS (SELECT 1 FROM executor_resolution_decisions decision WHERE decision.decision_id = NEW.resolution_decision_id AND decision.submission_id = OLD.submission_id AND decision.executor_execution_id = OLD.executor_execution_id AND decision.source = 'dreamina_native_batch' AND decision.resolved_state = NEW.state AND decision.error_code IS NOT DISTINCT FROM NEW.error_code AND decision.result_manifest_id IS NOT DISTINCT FROM NEW.result_manifest_id)
        THEN RAISE EXCEPTION 'native batch follower requires canonical terminal evidence'; END IF;
        RETURN NEW;
    END IF;
    IF NOT (
        (OLD.state = 'prepared'$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected provider submission guard'; END IF;
    EXECUTE patched;
END;
$migration$;

CREATE FUNCTION project_dreamina_native_batch_followers() RETURNS TRIGGER AS $$
DECLARE member RECORD; result dreamina_native_batch_results%ROWTYPE;
BEGIN
    IF NEW.state NOT IN ('succeeded', 'failed', 'uncertain', 'canceled') OR OLD.state = NEW.state
       OR NOT EXISTS (SELECT 1 FROM dreamina_native_batches WHERE leader_submission_id = NEW.submission_id)
    THEN RETURN NEW; END IF;
    FOR member IN SELECT * FROM dreamina_native_batch_members WHERE leader_submission_id = NEW.submission_id AND output_index > 0 ORDER BY output_index LOOP
        SELECT * INTO result FROM dreamina_native_batch_results WHERE submission_id = member.submission_id;
        IF NOT FOUND THEN
            IF NEW.state = 'succeeded' THEN RAISE EXCEPTION 'native success requires a resolved result for every image'; END IF;
            INSERT INTO dreamina_native_batch_results (submission_id, executor_execution_id, resolved_state, error_code, observed_at_ms)
                VALUES (member.submission_id, member.executor_execution_id, NEW.state, NEW.error_code, NEW.finished_at_ms)
                RETURNING * INTO result;
        END IF;
        INSERT INTO executor_resolution_decisions (decision_id, executor_execution_id, submission_id, source, resolved_state, result_manifest_id, error_code, decided_at_ms, dreamina_batch_result_id)
            VALUES (member.executor_execution_id, member.executor_execution_id, member.submission_id, 'dreamina_native_batch', result.resolved_state, result.result_manifest_id, result.error_code, NEW.finished_at_ms, member.submission_id);
        UPDATE executor_executions SET state = result.resolved_state, resolution_decision_id = member.executor_execution_id,
            finished_at_ms = NEW.finished_at_ms, updated_at_ms = NEW.finished_at_ms, error_code = result.error_code
            WHERE executor_execution_id = member.executor_execution_id AND state = 'prepared';
        IF NOT FOUND THEN RAISE EXCEPTION 'native follower execution must be unlaunched'; END IF;
        UPDATE provider_submissions SET state = result.resolved_state, resolution_decision_id = member.executor_execution_id,
            result_manifest_id = result.result_manifest_id, started_at_ms = COALESCE(NEW.started_at_ms, NEW.finished_at_ms),
            finished_at_ms = NEW.finished_at_ms, updated_at_ms = NEW.finished_at_ms, error_code = result.error_code
            WHERE submission_id = member.submission_id AND state = 'prepared';
        IF NOT FOUND THEN RAISE EXCEPTION 'native follower submission must be unlaunched'; END IF;
    END LOOP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER provider_submission_native_batch_terminal AFTER UPDATE ON provider_submissions
    FOR EACH ROW EXECUTE FUNCTION project_dreamina_native_batch_followers();

-- Only the new frozen native image batches join the existing partial-success policy.
DO $migration$
DECLARE definition TEXT; patched TEXT;
BEGIN
    definition := pg_get_functiondef('assert_terminal_parent_completion(uuid)'::REGPROCEDURE);
    patched := replace(definition, $old$              AND payload.command_schema IN (
                  'openai.images.generation.v1', 'openai.images.edit.v1'
              )$old$, $new$              AND (payload.command_schema IN (
                  'openai.images.generation.v1', 'openai.images.edit.v1'
              ) OR EXISTS (SELECT 1 FROM dreamina_native_batches batch WHERE batch.job_id = target_job_id))$new$);
    IF patched = definition THEN RAISE EXCEPTION 'expected partial image terminal policy'; END IF;
    EXECUTE patched;
END;
$migration$;
