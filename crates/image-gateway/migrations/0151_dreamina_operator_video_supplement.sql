-- Independent, one-shot operator attempts. They never enter provider_submissions.
CREATE TABLE dreamina_operator_video_supplement_authorizations (
 authorization_id UUID PRIMARY KEY, job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id),
 submission_id UUID NOT NULL UNIQUE REFERENCES provider_submissions(submission_id),
 executor_execution_id UUID NOT NULL UNIQUE, output_id UUID NOT NULL UNIQUE REFERENCES job_outputs(output_id),
 original_receipt_id UUID NOT NULL UNIQUE REFERENCES provider_receipts(receipt_id),
 hold_id UUID NOT NULL UNIQUE REFERENCES customer_billing_holds(hold_id),
 provider_account_id UUID NOT NULL, execution_profile_id UUID NOT NULL, dispatch_nonce UUID NOT NULL UNIQUE,
 command_hash TEXT NOT NULL CHECK(command_hash ~ '^[a-f0-9]{64}$'),
 quote_hash TEXT NOT NULL CHECK(quote_hash ~ '^[a-f0-9]{64}$'),
 execution_binding_sha256 TEXT NOT NULL CHECK(execution_binding_sha256 ~ '^[a-f0-9]{64}$'),
 runtime_binding_json JSONB NOT NULL CHECK(jsonb_typeof(runtime_binding_json)='object'),
 command_json JSONB NOT NULL, api_profile TEXT NOT NULL, billable_units INTEGER NOT NULL CHECK(billable_units > 0),
 operator_reference TEXT NOT NULL CHECK(length(operator_reference) BETWEEN 1 AND 255 AND operator_reference !~ '[[:cntrl:]]'),
 original_evidence_json JSONB NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('authorized','dispatching','accepted','uncertain','succeeded')),
 remote_operation_id TEXT CHECK(remote_operation_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$' AND remote_operation_id NOT LIKE '%://%'),
 submit_stdout_sha256 TEXT CHECK(submit_stdout_sha256 ~ '^[a-f0-9]{64}$'),
 error_code TEXT CHECK(error_code ~ '^[A-Za-z0-9_.-]{1,128}$'),
 created_at_ms BIGINT NOT NULL, updated_at_ms BIGINT NOT NULL, dispatched_at_ms BIGINT,
 CHECK((state='authorized' AND dispatched_at_ms IS NULL AND remote_operation_id IS NULL AND submit_stdout_sha256 IS NULL)
 OR (state='dispatching' AND dispatched_at_ms IS NOT NULL AND remote_operation_id IS NULL AND submit_stdout_sha256 IS NULL)
 OR (state IN ('accepted','succeeded') AND dispatched_at_ms IS NOT NULL AND remote_operation_id IS NOT NULL AND submit_stdout_sha256 IS NOT NULL)
 OR (state='uncertain' AND dispatched_at_ms IS NOT NULL)),
 CHECK((remote_operation_id IS NULL) = (submit_stdout_sha256 IS NULL)),
 UNIQUE(authorization_id,job_id)
);
CREATE TABLE dreamina_operator_video_supplement_resolutions (
 resolution_id UUID PRIMARY KEY, authorization_id UUID NOT NULL UNIQUE,
 artifact_id UUID NOT NULL UNIQUE, storage_backend TEXT NOT NULL, storage_namespace TEXT NOT NULL, object_key TEXT NOT NULL,
 job_id UUID NOT NULL UNIQUE, submission_id UUID NOT NULL UNIQUE, executor_execution_id UUID NOT NULL UNIQUE,
 output_id UUID NOT NULL UNIQUE, original_receipt_id UUID NOT NULL UNIQUE, hold_id UUID NOT NULL UNIQUE,
 remote_operation_id TEXT NOT NULL, command_hash TEXT NOT NULL, quote_hash TEXT NOT NULL,
 execution_binding_sha256 TEXT NOT NULL, billable_units INTEGER NOT NULL CHECK(billable_units > 0),
 reviewed_stdout_sha256 TEXT NOT NULL CHECK(reviewed_stdout_sha256 ~ '^[a-f0-9]{64}$'),
 submit_stdout_sha256 TEXT NOT NULL CHECK(submit_stdout_sha256 ~ '^[a-f0-9]{64}$'),
 execution_audit_sha256 TEXT NOT NULL CHECK(execution_audit_sha256 ~ '^[a-f0-9]{64}$'),
 review_sha256 TEXT NOT NULL CHECK(review_sha256 ~ '^[a-f0-9]{64}$'),
 input_sha256 TEXT NOT NULL CHECK(input_sha256 ~ '^[a-f0-9]{64}$'),
 video_url TEXT NOT NULL CHECK(octet_length(video_url) BETWEEN 1 AND 8192 AND video_url ~* '^https://[^/?#@[:space:]]+([/?][^#]*)?$' AND video_url !~ '[[:cntrl:][:space:]]' AND position(chr(92) in video_url)=0),
 video_url_sha256 TEXT NOT NULL CHECK(video_url_sha256=encode(sha256(convert_to(video_url,'UTF8')),'hex')),
 retrieved_video_sha256 TEXT NOT NULL CHECK(retrieved_video_sha256 ~ '^[a-f0-9]{64}$'),
 retrieved_video_byte_size BIGINT NOT NULL CHECK(retrieved_video_byte_size BETWEEN 12 AND 134217728),
 video_width INTEGER NOT NULL CHECK(video_width>0), video_height INTEGER NOT NULL CHECK(video_height>0),
 video_duration_ms BIGINT NOT NULL CHECK(video_duration_ms BETWEEN billable_units::BIGINT*1000-100 AND billable_units::BIGINT*1000+100), expires_at_ms BIGINT,
 operator_reference TEXT NOT NULL, created_at_ms BIGINT NOT NULL,
 FOREIGN KEY(authorization_id,job_id) REFERENCES dreamina_operator_video_supplement_authorizations(authorization_id,job_id)
);
CREATE TRIGGER dreamina_video_supplement_resolution_immutable BEFORE UPDATE OR DELETE ON dreamina_operator_video_supplement_resolutions FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation();
CREATE TRIGGER dreamina_video_supplement_resolution_no_truncate BEFORE TRUNCATE ON dreamina_operator_video_supplement_resolutions FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();
CREATE TRIGGER dreamina_video_supplement_authorization_no_truncate BEFORE TRUNCATE ON dreamina_operator_video_supplement_authorizations FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();

-- Capture all old execution and payload evidence byte-for-byte. New attempts have
-- their own authority and cannot falsify NoRemoteEffect on the old execution.
CREATE FUNCTION dreamina_video_supplement_original_snapshot(target_job_id UUID) RETURNS JSONB AS $$
DECLARE result JSONB := '{}'::JSONB; target TEXT; rows JSONB; mutable_columns TEXT[];
BEGIN
 FOREACH target IN ARRAY ARRAY['provider_submissions','executor_executions','provider_remote_submit_intents','provider_submit_recoveries','provider_remote_tasks','executor_resolution_decisions','executor_terminal_reductions','executor_capacity_allocations','provider_receipts','provider_task_observations','accepted_completion_budgets','executor_result_manifests','executor_artifact_authorities','provider_remote_video_results','job_payloads','customer_price_quotes','customer_price_quote_lines','job_auth_attributions','job_input_objects','job_input_manifests','provider_submit_quarantined_receipts'] LOOP
  EXECUTE format('SELECT COALESCE(jsonb_agg(to_jsonb(item) ORDER BY to_jsonb(item)::TEXT),''[]''::JSONB) FROM %I item WHERE COALESCE(to_jsonb(item)->>''job_id'',(SELECT submission.job_id::TEXT FROM provider_submissions submission WHERE submission.submission_id::TEXT=to_jsonb(item)->>''submission_id''))=$1::TEXT',target) INTO rows USING target_job_id;
  result := result || jsonb_build_object(target, rows);
 END LOOP;
 FOREACH target IN ARRAY ARRAY['job_events','outbox_events'] LOOP
  EXECUTE format('SELECT COALESCE(jsonb_agg(to_jsonb(item) ORDER BY to_jsonb(item)::TEXT),''[]''::JSONB) FROM %I item WHERE job_id=$1 AND semantic_key<>''job.operator_dreamina_video_supplement_success''',target) INTO rows USING target_job_id;
  result:=result || jsonb_build_object(target || '_original',rows);
 END LOOP;
 SELECT COALESCE(jsonb_agg(to_jsonb(item) ORDER BY to_jsonb(item)::TEXT),'[]'::JSONB) INTO rows FROM metering_events item WHERE job_id=target_job_id AND event_type<>'operator_video_supplement_success';
 result:=result || jsonb_build_object('metering_events_original',rows);
 -- Parent/API identity is frozen while only this overlay's terminal accounting
 -- fields may change. In particular model, request ID, actor scope and input
 -- session identity cannot drift behind an otherwise unchanged submission.
 FOREACH target IN ARRAY ARRAY['jobs','work_items','job_attempts','job_outputs','quota_reservations','customer_billing_holds','idempotency_requests','admission_sessions'] LOOP
  mutable_columns:=CASE target
   WHEN 'jobs' THEN ARRAY['state','charged_units','last_error_code','updated_at_ms']
   WHEN 'work_items' THEN ARRAY['state','updated_at_ms']
   WHEN 'job_attempts' THEN ARRAY['state','error_code','updated_at_ms']
   WHEN 'job_outputs' THEN ARRAY['state','error_code','finished_at_ms','updated_at_ms']
   WHEN 'quota_reservations' THEN ARRAY['state','committed_units','released_units','updated_at_ms']
   WHEN 'customer_billing_holds' THEN ARRAY['state','captured_micros','released_micros','grant_captured_micros','account_captured_micros','grant_released_micros','account_released_micros','updated_at_ms']
   WHEN 'idempotency_requests' THEN ARRAY['state','terminal_outcome','updated_at_ms']
   WHEN 'admission_sessions' THEN ARRAY['input_cleanup_state','input_cleanup_owner','input_cleanup_lease_expires_at_ms','input_cleanup_completed_at_ms','updated_at_ms']
  END;
  EXECUTE format('SELECT COALESCE(jsonb_agg(to_jsonb(item)-$2 ORDER BY (to_jsonb(item)-$2)::TEXT),''[]''::JSONB) FROM %I item WHERE COALESCE(to_jsonb(item)->>''job_id'',(SELECT work.job_id::TEXT FROM work_items work WHERE work.work_item_id::TEXT=to_jsonb(item)->>''work_item_id''))=$1::TEXT',target) INTO rows USING target_job_id,mutable_columns;
  result:=result || jsonb_build_object(target || '_identity',rows);
 END LOOP;
 RETURN result;
END; $$ LANGUAGE plpgsql;
-- Runtime pins are the operator's frozen reviewed process identity. The service
-- verifies actual executable/keychain bytes; the DB independently checks the
-- active account/profile/credential head while dispatch is still prohibited.
CREATE FUNCTION assert_dreamina_video_supplement_runtime(value JSONB, profile_id UUID, account_id UUID) RETURNS VOID AS $$
DECLARE key TEXT;
BEGIN
 IF jsonb_typeof(value) IS DISTINCT FROM 'object'
 OR (SELECT count(*) FROM jsonb_object_keys(value))<>11
 OR NOT(value ?& ARRAY['execution_profile_id','provider_account_id','upstream_identity_sha256','credential_revision','credential_ref_sha256','credential_auth_sha256','account_home_sha256','cli_path','cli_sha256','runner_path','runner_sha256'])
 OR value->>'execution_profile_id' IS DISTINCT FROM profile_id::TEXT
 OR value->>'provider_account_id' IS DISTINCT FROM account_id::TEXT
 OR jsonb_typeof(value->'credential_revision') IS DISTINCT FROM 'number'
 OR value->>'credential_revision' !~ '^[1-9][0-9]{0,18}$'
 THEN RAISE EXCEPTION 'invalid frozen supplement runtime identity'; END IF;
 FOREACH key IN ARRAY ARRAY['upstream_identity_sha256','credential_ref_sha256','credential_auth_sha256','account_home_sha256','cli_sha256','runner_sha256'] LOOP
  IF jsonb_typeof(value->key) IS DISTINCT FROM 'string' OR value->>key !~ '^[a-f0-9]{64}$' THEN RAISE EXCEPTION 'invalid frozen supplement runtime digest'; END IF;
 END LOOP;
 FOREACH key IN ARRAY ARRAY['cli_path','runner_path'] LOOP
  IF jsonb_typeof(value->key) IS DISTINCT FROM 'string' OR value->>key NOT LIKE '/%' OR value->>key ~ '[[:cntrl:]]' OR value->>key ~ '/\.\.?(/|$)' THEN RAISE EXCEPTION 'invalid frozen supplement executable path'; END IF;
 END LOOP;
 PERFORM profile.execution_profile_id FROM provider_execution_profiles profile
 JOIN provider_accounts account USING(provider_account_id)
 JOIN provider_account_credential_heads head USING(provider_account_id)
 JOIN provider_account_environments environment USING(provider_account_id)
 WHERE profile.execution_profile_id=profile_id AND account.provider_account_id=account_id
 AND profile.state='enabled' AND profile.provider_id='dreamina-cli' AND profile.operation_id='videos.generations'
 AND account.state='enabled' AND account.credential_revision=(value->>'credential_revision')::BIGINT
 AND profile.credential_revision=account.credential_revision AND profile.credential_ref=account.credential_ref
 AND encode(sha256(convert_to(account.credential_ref,'UTF8')),'hex')=value->>'credential_ref_sha256'
 AND account.credential_auth_sha256=value->>'credential_auth_sha256'
 AND head.active_revision=account.credential_revision AND head.lifecycle_state IN ('active','refresh_due')
 AND environment.state='active' AND environment.environment_kind='dreamina_home_v1'
 AND environment.upstream_identity_sha256=value->>'upstream_identity_sha256'
 AND encode(sha256(convert_to(environment.environment_ref,'UTF8')),'hex')=value->>'account_home_sha256'
 FOR SHARE OF profile,account,head,environment;
 IF NOT FOUND THEN RAISE EXCEPTION 'frozen supplement runtime no longer matches active account'; END IF;
END; $$ LANGUAGE plpgsql;
CREATE FUNCTION validate_dreamina_video_supplement_authorization_insert() RETURNS TRIGGER AS $$
BEGIN
 -- A row lock alone is invisible to a consumer's earlier REPEATABLE READ
 -- snapshot. Create a new parent tuple version so its later lock/write must
 -- serialize against this authorization instead of missing the new fence.
 UPDATE jobs SET updated_at_ms=updated_at_ms WHERE job_id=NEW.job_id;
 PERFORM assert_dreamina_video_supplement_runtime(NEW.runtime_binding_json,NEW.execution_profile_id,NEW.provider_account_id);
 IF NEW.state<>'authorized' OR NEW.dispatched_at_ms IS NOT NULL OR NEW.remote_operation_id IS NOT NULL OR NEW.submit_stdout_sha256 IS NOT NULL OR NEW.error_code IS NOT NULL OR NEW.created_at_ms<>NEW.updated_at_ms
 OR NEW.original_evidence_json IS DISTINCT FROM dreamina_video_supplement_original_snapshot(NEW.job_id)
 THEN RAISE EXCEPTION 'video supplement supplement_auth must precede launch and freeze original evidence'; END IF;
 RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_video_supplement_authorization_insert BEFORE INSERT ON dreamina_operator_video_supplement_authorizations FOR EACH ROW EXECUTE FUNCTION validate_dreamina_video_supplement_authorization_insert();
CREATE FUNCTION assert_operator_dreamina_video_supplement_original(target_job_id UUID) RETURNS VOID AS $$
DECLARE supplement_auth dreamina_operator_video_supplement_authorizations%ROWTYPE;
BEGIN
 SELECT * INTO STRICT supplement_auth FROM dreamina_operator_video_supplement_authorizations WHERE job_id=target_job_id;
 IF supplement_auth.original_evidence_json IS DISTINCT FROM dreamina_video_supplement_original_snapshot(target_job_id)
 OR EXISTS(SELECT 1 FROM artifacts WHERE job_id=target_job_id)
 OR NOT EXISTS(SELECT 1 FROM jobs job JOIN job_outputs output USING(job_id) JOIN provider_submissions submission USING(job_id) JOIN executor_executions execution USING(submission_id,executor_execution_id) JOIN provider_remote_submit_intents intent USING(submission_id,executor_execution_id) JOIN executor_terminal_reductions reduction USING(submission_id,executor_execution_id) JOIN provider_receipts receipt ON receipt.receipt_id=reduction.provider_receipt_id JOIN customer_billing_holds hold ON hold.job_id=job.job_id JOIN customer_price_quotes quote ON quote.quote_id=hold.quote_id
 WHERE job.job_id=target_job_id AND job.provider_id='dreamina-cli' AND job.operation='video_generation' AND job.economics_contract_version=4 AND job.output_count=1
 AND job.billable_units=supplement_auth.billable_units AND job.requested_units=supplement_auth.billable_units AND job.model=supplement_auth.command_json->>'model_version'
 AND output.output_id=supplement_auth.output_id AND output.output_index=0 AND output.billable_units=supplement_auth.billable_units
 AND submission.submission_id=supplement_auth.submission_id AND execution.executor_execution_id=supplement_auth.executor_execution_id
 AND submission.provider_account_id=supplement_auth.provider_account_id AND submission.execution_profile_id=supplement_auth.execution_profile_id AND submission.command_hash=supplement_auth.command_hash
 AND EXISTS(SELECT 1 FROM job_payloads payload JOIN admission_sessions admission ON admission.session_id=payload.admission_session_id WHERE payload.job_id=target_job_id AND payload.command_schema=submission.command_schema AND payload.command_json=supplement_auth.command_json AND admission.api_profile=supplement_auth.api_profile)
 AND submission.state='uncertain' AND submission.error_code='provider_submit_deadline' AND submission.remote_result_id IS NULL AND submission.result_manifest_id IS NULL
 AND execution.state='uncertain' AND execution.error_code='provider_submit_deadline' AND execution.executor_owner IS NULL AND execution.lease_expires_at_ms IS NULL
 AND intent.state='deadline_quarantined' AND intent.remote_operation_id IS NULL AND intent.execution_binding_sha256=supplement_auth.execution_binding_sha256
 AND reduction.state='completed' AND reduction.resolved_state='uncertain' AND reduction.customer_artifact_id IS NULL AND reduction.remote_result_id IS NULL
 AND receipt.receipt_id=supplement_auth.original_receipt_id AND receipt.outcome='uncertain'
 AND hold.hold_id=supplement_auth.hold_id AND hold.tenant_id=job.tenant_id AND hold.held_micros=quote.max_total_micros
 AND quote.quote_hash=supplement_auth.quote_hash AND quote.provider_id='dreamina-cli' AND quote.provider_model_id=supplement_auth.command_json->>'model_version'
 AND quote.media_kind='video' AND quote.execution_surface='provider_cli')
 OR (SELECT count(*) FROM provider_submissions WHERE job_id=target_job_id)<>1
 OR (SELECT count(*) FROM job_outputs WHERE job_id=target_job_id)<>1
 OR (SELECT count(*) FROM work_items WHERE job_id=target_job_id)<>1
 OR EXISTS(SELECT 1 FROM provider_remote_tasks WHERE submission_id=supplement_auth.submission_id)
 OR EXISTS(SELECT 1 FROM provider_remote_video_results WHERE submission_id=supplement_auth.submission_id)
 OR EXISTS(SELECT 1 FROM executor_result_manifests WHERE submission_id=supplement_auth.submission_id)
 OR EXISTS(SELECT 1 FROM executor_artifact_authorities WHERE submission_id=supplement_auth.submission_id)
 OR EXISTS(SELECT 1 FROM provider_submit_quarantined_receipts WHERE submission_id=supplement_auth.submission_id)
 OR EXISTS(SELECT 1 FROM provider_submit_recoveries WHERE submission_id=supplement_auth.submission_id AND (state<>'closed' OR recovery_owner IS NOT NULL OR recovery_lease_expires_at_ms IS NOT NULL))
 THEN RAISE EXCEPTION 'operator video supplement original evidence changed or original result exists'; END IF;
END; $$ LANGUAGE plpgsql;
CREATE FUNCTION validate_dreamina_video_supplement_resolution_insert() RETURNS TRIGGER AS $$
BEGIN
 PERFORM 1 FROM jobs WHERE job_id=NEW.job_id FOR UPDATE;
 IF NEW.storage_backend NOT IN ('filesystem-v1','memory-v1') OR length(NEW.storage_namespace)=0 OR length(NEW.object_key)=0 THEN RAISE EXCEPTION 'supplement requires immutable local video artifact'; END IF;
 IF NOT EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations supplement_auth WHERE supplement_auth.authorization_id=NEW.authorization_id AND supplement_auth.job_id=NEW.job_id
 AND NEW.artifact_id=supplement_auth.dispatch_nonce
 AND LEAST(NEW.video_width,NEW.video_height)=CASE supplement_auth.command_json->>'video_resolution' WHEN '720p' THEN 720 WHEN '1080p' THEN 1080 ELSE -1 END
 AND NEW.video_width::BIGINT*split_part(supplement_auth.command_json->>'ratio',':',2)::BIGINT=NEW.video_height::BIGINT*split_part(supplement_auth.command_json->>'ratio',':',1)::BIGINT
 AND supplement_auth.state IN ('accepted','uncertain') AND supplement_auth.submission_id=NEW.submission_id AND supplement_auth.executor_execution_id=NEW.executor_execution_id AND supplement_auth.output_id=NEW.output_id AND supplement_auth.original_receipt_id=NEW.original_receipt_id AND supplement_auth.hold_id=NEW.hold_id AND supplement_auth.remote_operation_id=NEW.remote_operation_id AND supplement_auth.submit_stdout_sha256=NEW.submit_stdout_sha256 AND supplement_auth.command_hash=NEW.command_hash AND supplement_auth.quote_hash=NEW.quote_hash AND supplement_auth.billable_units=NEW.billable_units AND supplement_auth.execution_binding_sha256=NEW.execution_binding_sha256 AND supplement_auth.operator_reference=NEW.operator_reference) THEN RAISE EXCEPTION 'supplement result does not bind authorized new attempt'; END IF;
 RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_video_supplement_resolution_insert BEFORE INSERT ON dreamina_operator_video_supplement_resolutions FOR EACH ROW EXECUTE FUNCTION validate_dreamina_video_supplement_resolution_insert();
CREATE FUNCTION assert_operator_dreamina_video_supplement_success(target_job_id UUID) RETURNS VOID AS $$
DECLARE
    resolution dreamina_operator_video_supplement_resolutions%ROWTYPE;
    rating customer_rated_usage%ROWTYPE;
    hold customer_billing_holds%ROWTYPE;
    charge_id UUID;
BEGIN
    SELECT * INTO STRICT resolution FROM dreamina_operator_video_supplement_resolutions
    WHERE job_id = target_job_id;
    PERFORM assert_operator_dreamina_video_supplement_original(target_job_id);
    SELECT * INTO STRICT rating FROM customer_rated_usage WHERE job_id = target_job_id;
    SELECT * INTO STRICT hold FROM customer_billing_holds WHERE hold_id = resolution.hold_id;
    IF NOT EXISTS (
        SELECT 1 FROM jobs job
        JOIN work_items work ON work.job_id = job.job_id
        JOIN job_attempts attempt ON attempt.execution_id = work.execution_id
          AND attempt.work_item_id = work.work_item_id AND attempt.lease_epoch = work.lease_epoch
        JOIN job_outputs output ON output.output_id = resolution.output_id AND output.job_id = job.job_id
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
        JOIN job_response_projections projection ON projection.job_id = job.job_id
        WHERE job.job_id = target_job_id AND job.state = 'succeeded' AND job.charged_units = resolution.billable_units
          AND job.last_error_code IS NULL AND work.state = 'succeeded' AND attempt.state = 'succeeded'
          AND attempt.error_code IS NULL AND output.state = 'succeeded' AND output.error_code IS NULL
          AND quota.state = 'committed' AND quota.committed_units = resolution.billable_units AND quota.released_units = 0
          AND projection.artifact_count = 1 AND projection.remote_result_count = 0
          AND (SELECT COUNT(*) FROM job_events WHERE job_id = target_job_id
              AND semantic_key = 'work.' || work.work_item_id::TEXT || '.executor-terminal'
              AND event_type = 'job.uncertain' AND payload_json ->> 'terminal_state' = 'uncertain'
              AND payload_json ->> 'error_code' = 'provider_submit_deadline') = 1
          AND (SELECT COUNT(*) FROM outbox_events WHERE job_id = target_job_id
              AND semantic_key = 'work.' || work.work_item_id::TEXT || '.executor-terminal'
              AND event_type = 'job.uncertain' AND payload_json ->> 'terminal_state' = 'uncertain'
              AND payload_json ->> 'error_code' = 'provider_submit_deadline') = 1
    ) OR EXISTS (SELECT 1 FROM artifacts WHERE job_id = target_job_id)
      OR (SELECT COUNT(*) FROM job_response_projections WHERE job_id = target_job_id) <> 1
      OR NOT EXISTS (SELECT 1 FROM idempotency_requests WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM idempotency_requests WHERE job_id = target_job_id
          AND (state <> 'succeeded' OR terminal_outcome IS DISTINCT FROM 'succeeded'))
      OR (SELECT COUNT(*) FROM provider_usage_facts WHERE job_id = target_job_id) <> 1
      OR NOT EXISTS (
          SELECT 1 FROM provider_usage_facts fact
          JOIN provider_submissions submission ON submission.submission_id = fact.submission_id
          WHERE fact.job_id = target_job_id AND fact.output_id = resolution.output_id
            AND fact.submission_id = resolution.submission_id AND fact.receipt_id = resolution.original_receipt_id
            AND fact.attempt_execution_id IS NULL AND fact.provider_id = 'dreamina-cli'
            AND fact.provider_account_id IS NOT DISTINCT FROM submission.provider_account_id
            AND fact.fact_domain = 'customer_billable' AND fact.terminal_outcome = 'succeeded'
            AND fact.execution_surface = 'provider_cli' AND fact.metric = 'video_requested_second'
            AND fact.quantity = resolution.billable_units AND fact.unit = 'second' AND fact.quantity_source = 'request_derived'
            AND fact.confidence = 'exact'
            AND fact.metadata_json ->> 'operator_resolution_id' = resolution.resolution_id::TEXT)
      OR (SELECT COUNT(*) FROM usage_events WHERE job_id = target_job_id) <> 1
      OR NOT EXISTS (SELECT 1 FROM usage_events usage JOIN jobs job USING (job_id)
          WHERE usage.job_id = target_job_id AND usage.tenant_id = job.tenant_id
            AND usage.operation = job.operation AND usage.request_id = job.request_id
            AND usage.units = resolution.billable_units AND usage.billing_metric = 'video_second' AND usage.billing_unit = 'second' AND usage.outcome = 'charged')
      OR (SELECT COUNT(*) FROM customer_rated_usage_lines WHERE job_id = target_job_id) <> 1
      OR NOT EXISTS (
          SELECT 1 FROM customer_rated_usage_lines line
          JOIN customer_price_quote_lines quoted ON quoted.quote_line_id = line.quote_line_id
            AND quoted.quote_id = line.quote_id AND quoted.job_id = line.job_id
          JOIN customer_rated_usage_fact_links link ON link.rated_usage_line_id = line.rated_usage_line_id
          JOIN provider_usage_facts fact ON fact.usage_fact_id = link.usage_fact_id
          WHERE line.rated_usage_id = rating.rated_usage_id AND line.quote_id = hold.quote_id
            AND line.job_id = target_job_id AND quoted.terminal_outcome = 'succeeded'
            AND line.actual_quantity = resolution.billable_units AND line.amount_micros = quoted.max_amount_micros
            AND rating.total_amount_micros = line.amount_micros AND rating.currency = hold.currency
            AND rating.quote_id = hold.quote_id AND fact.job_id = target_job_id
            AND fact.receipt_id = resolution.original_receipt_id
            AND fact.metadata_json ->> 'operator_resolution_id' = resolution.resolution_id::TEXT)
      OR hold.state <> 'settled' OR hold.captured_micros <> rating.total_amount_micros
      OR hold.released_micros <> hold.held_micros - hold.captured_micros
      OR hold.grant_captured_micros + hold.grant_released_micros <> hold.grant_held_micros
      OR hold.account_captured_micros + hold.account_released_micros <> hold.account_held_micros
      OR EXISTS (SELECT 1 FROM customer_billing_hold_grant_reservations WHERE hold_id = hold.hold_id
          AND (state = 'reserved' OR consumed_micros + released_micros <> reserved_micros))
      OR (SELECT COUNT(*) FROM metering_events WHERE job_id = target_job_id
          AND event_type = 'operator_video_supplement_success') <> 1
      OR NOT EXISTS (SELECT 1 FROM metering_events WHERE job_id = target_job_id
          AND event_type = 'operator_video_supplement_success' AND outcome = 'succeeded' AND units = resolution.billable_units
          AND billing_metric = 'video_second' AND billing_unit = 'second')
      OR (SELECT COUNT(*) FROM job_events WHERE job_id = target_job_id
          AND semantic_key = 'job.operator_dreamina_video_supplement_success' AND event_type = 'job.succeeded'
          AND payload_json ->> 'resolution_id' = resolution.resolution_id::TEXT) <> 1
      OR (SELECT COUNT(*) FROM job_events WHERE job_id = target_job_id AND event_type = 'job.succeeded') <> 1
      OR (SELECT COUNT(*) FROM outbox_events WHERE job_id = target_job_id
          AND semantic_key = 'job.operator_dreamina_video_supplement_success' AND event_type = 'job.succeeded'
          AND payload_json ->> 'resolution_id' = resolution.resolution_id::TEXT) <> 1
      OR (SELECT COUNT(*) FROM outbox_events WHERE job_id = target_job_id AND event_type = 'job.succeeded') <> 1
    THEN
        RAISE EXCEPTION 'Dreamina operator video success requires one reviewed video URL, one frozen-price charge, and consistent success projections';
    END IF;
    PERFORM validate_customer_billing_hold_grant_split(hold.hold_id);
    IF rating.total_amount_micros = 0 THEN
        IF EXISTS (SELECT 1 FROM ledger_transactions WHERE source_job_id = target_job_id) THEN
            RAISE EXCEPTION 'free operator video success cannot create a charge';
        END IF;
    ELSE
        SELECT transaction_id INTO STRICT charge_id FROM ledger_transactions
        WHERE source_job_id = target_job_id AND transaction_type = 'customer_job_charge'
          AND currency = rating.currency;
        IF (SELECT COUNT(*) FROM ledger_transactions WHERE source_job_id = target_job_id) <> 1
          OR (SELECT COUNT(*) FROM ledger_postings WHERE transaction_id = charge_id) <> 2
          OR NOT EXISTS (SELECT 1 FROM ledger_transaction_seals WHERE transaction_id = charge_id)
          OR NOT EXISTS (SELECT 1 FROM ledger_postings posting JOIN ledger_accounts account USING (account_id)
              WHERE posting.transaction_id = charge_id AND posting.currency = rating.currency
                AND account.owner_type = 'tenant' AND account.owner_id = hold.tenant_id
                AND account.account_type = 'receivable' AND account.currency = rating.currency
                AND posting.amount_micros = rating.total_amount_micros)
          OR NOT EXISTS (SELECT 1 FROM ledger_postings posting JOIN ledger_accounts account USING (account_id)
              WHERE posting.transaction_id = charge_id AND posting.currency = rating.currency
                AND account.owner_type = 'platform' AND account.owner_id = 'platform'
                AND account.account_type = 'revenue' AND account.currency = rating.currency
                AND posting.amount_micros = -rating.total_amount_micros)
          OR (SELECT SUM(amount_micros::NUMERIC) FROM ledger_postings WHERE transaction_id = charge_id) <> 0
        THEN
            RAISE EXCEPTION 'Dreamina operator video success requires one sealed balanced customer ledger charge';
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION assert_operator_dreamina_video_supplement(target_job_id UUID) RETURNS VOID AS $$
DECLARE supplement_auth dreamina_operator_video_supplement_authorizations%ROWTYPE;
BEGIN
 SELECT * INTO STRICT supplement_auth FROM dreamina_operator_video_supplement_authorizations WHERE job_id=target_job_id;
 PERFORM assert_operator_dreamina_video_supplement_original(target_job_id);
 IF supplement_auth.state='succeeded' THEN
  PERFORM assert_operator_dreamina_video_supplement_success(target_job_id); RETURN;
 END IF;
 IF EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_resolutions WHERE job_id=target_job_id)
 OR NOT EXISTS(SELECT 1 FROM jobs job JOIN work_items work USING(job_id) JOIN job_attempts attempt ON attempt.execution_id=work.execution_id AND attempt.work_item_id=work.work_item_id AND attempt.lease_epoch=work.lease_epoch JOIN job_outputs output USING(job_id) JOIN customer_billing_holds hold USING(job_id) JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id
 WHERE job.job_id=target_job_id AND job.state='uncertain' AND job.last_error_code='provider_submit_deadline' AND job.charged_units=0 AND work.state='uncertain' AND work.lease_owner IS NULL AND work.lease_expires_at_ms IS NULL AND attempt.state='uncertain' AND output.state='uncertain'
 AND hold.state='held' AND hold.captured_micros=0 AND hold.released_micros=0 AND hold.grant_captured_micros=0 AND hold.account_captured_micros=0 AND hold.grant_released_micros=0 AND hold.account_released_micros=0
 AND quota.state IN ('reserved','expired') AND quota.committed_units=0 AND quota.released_units=0)
 OR NOT EXISTS(SELECT 1 FROM idempotency_requests WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM idempotency_requests WHERE job_id=target_job_id AND (state<>'uncertain' OR terminal_outcome IS DISTINCT FROM 'uncertain'))
 OR EXISTS(SELECT 1 FROM provider_usage_facts WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM usage_events WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM customer_rated_usage WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM ledger_transactions WHERE source_job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM job_response_projections WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM customer_billing_hold_grant_reservations WHERE hold_id=supplement_auth.hold_id AND (state<>'reserved' OR consumed_micros<>0 OR released_micros<>0))
 THEN RAISE EXCEPTION 'pending operator supplement must retain original uncertain status and untouched hold'; END IF;
END; $$ LANGUAGE plpgsql;
CREATE FUNCTION protect_dreamina_video_supplement_authorization() RETURNS TRIGGER AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'video supplement authorization is permanent'; END IF;
 IF OLD.state='authorized' AND NEW.state='dispatching' THEN PERFORM assert_dreamina_video_supplement_runtime(NEW.runtime_binding_json,NEW.execution_profile_id,NEW.provider_account_id); END IF;
 IF to_jsonb(NEW)-'state'-'remote_operation_id'-'submit_stdout_sha256'-'error_code'-'updated_at_ms'-'dispatched_at_ms' IS DISTINCT FROM to_jsonb(OLD)-'state'-'remote_operation_id'-'submit_stdout_sha256'-'error_code'-'updated_at_ms'-'dispatched_at_ms'
 OR OLD.state='succeeded' OR (OLD.dispatched_at_ms IS NOT NULL AND NEW.dispatched_at_ms IS DISTINCT FROM OLD.dispatched_at_ms)
 OR (OLD.remote_operation_id IS NOT NULL AND (NEW.remote_operation_id IS DISTINCT FROM OLD.remote_operation_id OR NEW.submit_stdout_sha256 IS DISTINCT FROM OLD.submit_stdout_sha256))
 OR NOT ((OLD.state='authorized' AND NEW.state='dispatching') OR (OLD.state IN ('dispatching','accepted','uncertain') AND NEW.state IN ('accepted','uncertain','succeeded')))
 THEN RAISE EXCEPTION 'video supplement identity and one-shot dispatch fence are immutable'; END IF;
 RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_video_supplement_authorization_update BEFORE UPDATE OR DELETE ON dreamina_operator_video_supplement_authorizations FOR EACH ROW EXECUTE FUNCTION protect_dreamina_video_supplement_authorization();

-- Resolve a table mutation's job before taking its common parent lock. This
-- serializes supplement_auth against late callbacks and upload publication.
CREATE FUNCTION dreamina_video_supplement_mutation_job(row_data JSONB) RETURNS UUID AS $$
DECLARE result UUID;
BEGIN
 result:=COALESCE(row_data->>'job_id',row_data->>'source_job_id')::UUID;
 IF result IS NULL AND row_data ? 'submission_id' THEN SELECT job_id INTO result FROM provider_submissions WHERE submission_id=(row_data->>'submission_id')::UUID; END IF;
 IF result IS NULL AND row_data ? 'work_item_id' THEN SELECT job_id INTO result FROM work_items WHERE work_item_id=(row_data->>'work_item_id')::UUID; END IF;
 IF result IS NULL AND row_data ? 'hold_id' THEN SELECT job_id INTO result FROM customer_billing_holds WHERE hold_id=(row_data->>'hold_id')::UUID; END IF;
 IF result IS NULL AND row_data ? 'transaction_id' THEN SELECT source_job_id INTO result FROM ledger_transactions WHERE transaction_id=(row_data->>'transaction_id')::UUID; END IF;
 IF result IS NULL AND row_data ? 'rated_usage_line_id' THEN SELECT job_id INTO result FROM customer_rated_usage_lines WHERE rated_usage_line_id=(row_data->>'rated_usage_line_id')::UUID; END IF;
 RETURN result;
END; $$ LANGUAGE plpgsql;
CREATE FUNCTION fence_dreamina_video_supplement_original() RETURNS TRIGGER AS $$
DECLARE target UUID; row_data JSONB; affected JSONB[];
BEGIN
 IF TG_OP='INSERT' THEN affected:=ARRAY[to_jsonb(NEW)]; ELSIF TG_OP='DELETE' THEN affected:=ARRAY[to_jsonb(OLD)]; ELSE affected:=ARRAY[to_jsonb(OLD),to_jsonb(NEW)]; END IF;
 FOREACH row_data IN ARRAY affected LOOP
  target:=dreamina_video_supplement_mutation_job(row_data);
  IF target IS NOT NULL THEN
   PERFORM 1 FROM jobs WHERE job_id=target AND provider_id='dreamina-cli' AND operation='video_generation' FOR UPDATE;
   IF EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE job_id=target) THEN RAISE EXCEPTION 'original execution is permanently fenced by operator supplement'; END IF;
  END IF;
 END LOOP;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE FUNCTION validate_dreamina_video_supplement_mutation() RETURNS TRIGGER AS $$
DECLARE target UUID; row_data JSONB; affected JSONB[];
BEGIN
 IF TG_OP='INSERT' THEN affected:=ARRAY[to_jsonb(NEW)]; ELSIF TG_OP='DELETE' THEN affected:=ARRAY[to_jsonb(OLD)]; ELSE affected:=ARRAY[to_jsonb(OLD),to_jsonb(NEW)]; END IF;
 FOREACH row_data IN ARRAY affected LOOP
  target:=dreamina_video_supplement_mutation_job(row_data);
  IF EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE job_id=target) THEN PERFORM assert_operator_dreamina_video_supplement(target); END IF;
 END LOOP;
 RETURN NULL;
END; $$ LANGUAGE plpgsql;
DO $migration$
DECLARE target TEXT;
BEGIN
 FOREACH target IN ARRAY ARRAY['provider_submissions','executor_executions','provider_remote_submit_intents','provider_submit_recoveries','provider_remote_tasks','executor_resolution_decisions','executor_terminal_reductions','executor_capacity_allocations','provider_receipts','provider_task_observations','accepted_completion_budgets','executor_result_manifests','executor_artifact_authorities','provider_remote_video_results','job_payloads','customer_price_quotes','customer_price_quote_lines','job_auth_attributions','job_input_objects','job_input_manifests','artifacts','provider_submit_quarantined_receipts'] LOOP
  EXECUTE format('CREATE TRIGGER dreamina_video_supplement_original_fence BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION fence_dreamina_video_supplement_original()',target);
 END LOOP;
 FOREACH target IN ARRAY ARRAY['dreamina_operator_video_supplement_authorizations','dreamina_operator_video_supplement_resolutions','admission_sessions','jobs','work_items','job_attempts','job_outputs','idempotency_requests','quota_reservations','customer_billing_holds','customer_billing_hold_grant_reservations','job_response_projections','provider_usage_facts','usage_events','metering_events','customer_rated_usage','customer_rated_usage_lines','customer_rated_usage_fact_links','ledger_transactions','ledger_postings','ledger_transaction_seals','job_events','outbox_events'] LOOP
  EXECUTE format('CREATE CONSTRAINT TRIGGER dreamina_video_supplement_consistency AFTER INSERT OR UPDATE OR DELETE ON %I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_dreamina_video_supplement_mutation()',target);
 END LOOP;
END; $migration$;
DO $migration$
DECLARE target REGPROCEDURE; definition TEXT; patched TEXT;
BEGIN
 FOREACH target IN ARRAY ARRAY['assert_terminal_parent_completion(uuid)'::REGPROCEDURE,'assert_terminal_quota_accounting(uuid)'::REGPROCEDURE] LOOP
  definition:=pg_get_functiondef(target);
  patched:=regexp_replace(definition,E'\\mBEGIN\\M',$guard$BEGIN
   IF EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE job_id=target_job_id) THEN PERFORM assert_operator_dreamina_video_supplement(target_job_id); RETURN; END IF;$guard$);
  IF patched=definition THEN RAISE EXCEPTION 'missing terminal assertion'; END IF; EXECUTE patched;
 END LOOP;
 definition:=pg_get_functiondef('validate_v4_provider_usage_fact_insert()'::REGPROCEDURE);
 patched:=replace(definition,'SELECT CASE WHEN contract_version = 4',$guard$SELECT CASE WHEN contract_version = 4
 AND NEW.fact_domain='customer_billable' AND NEW.attempt_execution_id IS NULL AND NEW.terminal_outcome='succeeded'
 AND NEW.metric='video_requested_second' AND NEW.unit='second' AND NEW.quantity_source='request_derived' AND NEW.confidence='exact' AND NEW.execution_surface='provider_cli' AND receipt.outcome='uncertain'
 AND EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_resolutions resolution WHERE resolution.resolution_id::TEXT=NEW.metadata_json->>'operator_resolution_id' AND resolution.job_id=NEW.job_id AND resolution.submission_id=NEW.submission_id AND resolution.original_receipt_id=NEW.receipt_id AND resolution.output_id=NEW.output_id AND NEW.quantity=resolution.billable_units AND NEW.provider_id='dreamina-cli')
 THEN 'succeeded' WHEN contract_version = 4$guard$);
 IF patched=definition THEN RAISE EXCEPTION 'missing usage authority guard'; END IF; EXECUTE patched;
END; $migration$;

-- Taking the parent lock before financial/projection writes prevents concurrent
-- authorization and release checks from missing each other's uncommitted rows.
CREATE FUNCTION lock_dreamina_video_supplement_parent() RETURNS TRIGGER AS $$
DECLARE target UUID; row_data JSONB; affected JSONB[];
BEGIN
 IF TG_OP='INSERT' THEN affected:=ARRAY[to_jsonb(NEW)]; ELSIF TG_OP='DELETE' THEN affected:=ARRAY[to_jsonb(OLD)]; ELSE affected:=ARRAY[to_jsonb(OLD),to_jsonb(NEW)]; END IF;
 FOREACH row_data IN ARRAY affected LOOP
  target:=dreamina_video_supplement_mutation_job(row_data);
  IF target IS NOT NULL THEN PERFORM 1 FROM jobs WHERE job_id=target AND provider_id='dreamina-cli' AND operation='video_generation' FOR UPDATE; END IF;
 END LOOP;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE FUNCTION fence_dreamina_video_supplement_truncate() RETURNS TRIGGER AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations) THEN RAISE EXCEPTION 'operator video supplement evidence and settlement cannot be truncated'; END IF;
 RETURN NULL;
END; $$ LANGUAGE plpgsql;
DO $migration$
DECLARE target TEXT;
BEGIN
 FOREACH target IN ARRAY ARRAY['admission_sessions','jobs','work_items','job_attempts','job_outputs','idempotency_requests','quota_reservations','customer_billing_holds','customer_billing_hold_grant_reservations','job_response_projections','provider_usage_facts','usage_events','metering_events','customer_rated_usage','customer_rated_usage_lines','customer_rated_usage_fact_links','ledger_transactions','ledger_postings','ledger_transaction_seals','job_events','outbox_events'] LOOP
  EXECUTE format('CREATE TRIGGER dreamina_video_supplement_parent_lock BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION lock_dreamina_video_supplement_parent()',target);
 END LOOP;
 FOREACH target IN ARRAY ARRAY['admission_sessions','jobs','work_items','job_attempts','job_outputs','idempotency_requests','quota_reservations','customer_billing_holds','customer_billing_hold_grant_reservations','job_response_projections','provider_usage_facts','usage_events','metering_events','customer_rated_usage','customer_rated_usage_lines','customer_rated_usage_fact_links','ledger_transactions','ledger_postings','ledger_transaction_seals','job_events','outbox_events','provider_submissions','executor_executions','provider_remote_submit_intents','provider_submit_recoveries','provider_remote_tasks','executor_resolution_decisions','executor_terminal_reductions','executor_capacity_allocations','provider_receipts','provider_task_observations','accepted_completion_budgets','executor_result_manifests','executor_artifact_authorities','provider_remote_video_results','job_payloads','customer_price_quotes','customer_price_quote_lines','job_auth_attributions','job_input_objects','job_input_manifests','artifacts','provider_submit_quarantined_receipts'] LOOP
  EXECUTE format('CREATE TRIGGER dreamina_video_supplement_truncate_fence BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION fence_dreamina_video_supplement_truncate()',target);
 END LOOP;
END; $migration$;
