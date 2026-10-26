-- A new human authorization creates a new one-shot attempt. Old nonces,
-- process evidence and unknown outcomes are never reset or rewritten.
ALTER TABLE dreamina_operator_video_supplement_authorizations
 ADD COLUMN previous_authorization_id UUID,
 ADD CONSTRAINT dreamina_video_supplement_previous_unique UNIQUE(previous_authorization_id),
 ADD CONSTRAINT dreamina_video_supplement_previous_same_job
  FOREIGN KEY(previous_authorization_id,job_id)
  REFERENCES dreamina_operator_video_supplement_authorizations(authorization_id,job_id),
 ADD CONSTRAINT dreamina_video_supplement_not_self
  CHECK(previous_authorization_id IS DISTINCT FROM authorization_id);

-- Only roots use the original six identities as unique keys. A successor must
-- prove every one of those identities equals its one existing parent below.
DO $migration$
DECLARE item RECORD; removed INTEGER:=0;
BEGIN
 FOR item IN
  SELECT constraint_row.conname FROM pg_constraint constraint_row
  JOIN pg_attribute column_row ON column_row.attrelid=constraint_row.conrelid
   AND constraint_row.conkey=ARRAY[column_row.attnum]::SMALLINT[]
  WHERE constraint_row.conrelid='dreamina_operator_video_supplement_authorizations'::REGCLASS
   AND constraint_row.contype='u' AND column_row.attname IN
    ('job_id','submission_id','executor_execution_id','output_id','original_receipt_id','hold_id')
 LOOP
  EXECUTE format('ALTER TABLE dreamina_operator_video_supplement_authorizations DROP CONSTRAINT %I',item.conname);
  removed:=removed+1;
 END LOOP;
 IF removed<>6 THEN RAISE EXCEPTION 'expected six original supplement identity constraints'; END IF;
END; $migration$;
CREATE UNIQUE INDEX dreamina_video_supplement_root_job ON dreamina_operator_video_supplement_authorizations(job_id) WHERE previous_authorization_id IS NULL;
CREATE UNIQUE INDEX dreamina_video_supplement_root_submission ON dreamina_operator_video_supplement_authorizations(submission_id) WHERE previous_authorization_id IS NULL;
CREATE UNIQUE INDEX dreamina_video_supplement_root_execution ON dreamina_operator_video_supplement_authorizations(executor_execution_id) WHERE previous_authorization_id IS NULL;
CREATE UNIQUE INDEX dreamina_video_supplement_root_output ON dreamina_operator_video_supplement_authorizations(output_id) WHERE previous_authorization_id IS NULL;
CREATE UNIQUE INDEX dreamina_video_supplement_root_receipt ON dreamina_operator_video_supplement_authorizations(original_receipt_id) WHERE previous_authorization_id IS NULL;
CREATE UNIQUE INDEX dreamina_video_supplement_root_hold ON dreamina_operator_video_supplement_authorizations(hold_id) WHERE previous_authorization_id IS NULL;
CREATE INDEX dreamina_video_supplement_attempt_job ON dreamina_operator_video_supplement_authorizations(job_id);

CREATE FUNCTION assert_dreamina_video_supplement_successor(value dreamina_operator_video_supplement_authorizations) RETURNS VOID AS $$
DECLARE parent dreamina_operator_video_supplement_authorizations%ROWTYPE;
BEGIN
 IF value.previous_authorization_id IS NULL THEN RETURN; END IF;
 SELECT * INTO STRICT parent FROM dreamina_operator_video_supplement_authorizations
  WHERE authorization_id=value.previous_authorization_id FOR UPDATE;
 IF parent.state<>'uncertain' OR parent.remote_operation_id IS NOT NULL
  OR parent.submit_stdout_sha256 IS NOT NULL OR parent.dispatched_at_ms IS NULL
  OR parent.operator_reference=value.operator_reference
  OR parent.runtime_binding_json->>'upstream_identity_sha256' IS DISTINCT FROM value.runtime_binding_json->>'upstream_identity_sha256'
  OR parent.runtime_binding_json->>'account_home_sha256' IS DISTINCT FROM value.runtime_binding_json->>'account_home_sha256'
  OR value.created_at_ms<parent.updated_at_ms
  OR EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations
    WHERE previous_authorization_id=parent.authorization_id AND authorization_id<>value.authorization_id)
  OR EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_resolutions WHERE job_id=parent.job_id)
  OR to_jsonb(value)-ARRAY['authorization_id','dispatch_nonce','runtime_binding_json','operator_reference','state','remote_operation_id','submit_stdout_sha256','error_code','created_at_ms','updated_at_ms','dispatched_at_ms','previous_authorization_id']
   IS DISTINCT FROM to_jsonb(parent)-ARRAY['authorization_id','dispatch_nonce','runtime_binding_json','operator_reference','state','remote_operation_id','submit_stdout_sha256','error_code','created_at_ms','updated_at_ms','dispatched_at_ms','previous_authorization_id']
 THEN RAISE EXCEPTION 'successor requires a new human authorization for the same unresolved unknown attempt'; END IF;
END; $$ LANGUAGE plpgsql;

-- The existing insert trigger writes a new jobs tuple before any authority is
-- inserted. That remains the common lock and stale REPEATABLE READ fence.
DO $migration$
DECLARE definition TEXT; patched TEXT; target REGPROCEDURE;
BEGIN
 target:='validate_dreamina_video_supplement_authorization_insert()'::REGPROCEDURE;
 definition:=pg_get_functiondef(target);
 patched:=replace(definition,' RETURN NEW;',' PERFORM assert_dreamina_video_supplement_successor(NEW); RETURN NEW;');
 IF patched=definition THEN RAISE EXCEPTION 'missing supplement insert guard'; END IF;
 EXECUTE patched;
 FOREACH target IN ARRAY ARRAY[
  'assert_operator_dreamina_video_supplement_original(uuid)'::REGPROCEDURE,
  'assert_operator_dreamina_video_supplement(uuid)'::REGPROCEDURE
 ] LOOP
  definition:=pg_get_functiondef(target);
  patched:=replace(definition,
   'FROM dreamina_operator_video_supplement_authorizations WHERE job_id=target_job_id;',
   'FROM dreamina_operator_video_supplement_authorizations current_attempt WHERE job_id=target_job_id AND NOT EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations child WHERE child.previous_authorization_id=current_attempt.authorization_id);');
  IF patched=definition THEN RAISE EXCEPTION 'missing supplement active-head assertion'; END IF;
  EXECUTE patched;
 END LOOP;
 target:='protect_dreamina_video_supplement_authorization()'::REGPROCEDURE;
 definition:=pg_get_functiondef(target);
 patched:=regexp_replace(definition,E'\\mBEGIN\\M',$guard$BEGIN
 PERFORM 1 FROM jobs WHERE job_id=OLD.job_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE previous_authorization_id=OLD.authorization_id)
 THEN RAISE EXCEPTION 'superseded supplement authority is permanently frozen'; END IF;$guard$);
 IF patched=definition THEN RAISE EXCEPTION 'missing supplement immutable guard'; END IF;
 EXECUTE patched;
 target:='assert_dreamina_supplement_observation()'::REGPROCEDURE;
 definition:=pg_get_functiondef(target);
 patched:=replace(definition,'AND supplement.state IN (''accepted'', ''uncertain'')',
  'AND supplement.state IN (''accepted'', ''uncertain'') AND NOT EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations child WHERE child.previous_authorization_id=supplement.authorization_id)');
 IF patched=definition THEN RAISE EXCEPTION 'missing supplement observation guard'; END IF;
 EXECUTE patched;
END; $migration$;
