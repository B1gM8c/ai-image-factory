-- A customer-only economic waiver after a real accepted head was cancelled.
-- Unknown provider submissions, receipts and ancestor attempts are never rewritten.
CREATE TABLE dreamina_operator_video_supplement_cancellations (
 cancellation_id UUID PRIMARY KEY, job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id),
 authorization_id UUID NOT NULL UNIQUE REFERENCES dreamina_operator_video_supplement_authorizations(authorization_id),
 dispatch_nonce UUID NOT NULL UNIQUE, submission_id UUID NOT NULL, executor_execution_id UUID NOT NULL,
 output_id UUID NOT NULL, original_receipt_id UUID NOT NULL, hold_id UUID NOT NULL UNIQUE REFERENCES customer_billing_holds(hold_id),
 quote_id UUID NOT NULL UNIQUE REFERENCES customer_price_quotes(quote_id), request_id TEXT NOT NULL,
 provider_account_id UUID NOT NULL, upstream_task_id TEXT NOT NULL,
 quote_hash TEXT NOT NULL, command_hash TEXT NOT NULL, submit_stdout_sha256 TEXT NOT NULL,
 query_stdout_sha256 TEXT NOT NULL, execution_audit_sha256 TEXT NOT NULL, review_sha256 TEXT NOT NULL,
 website_confirmation_sha256 TEXT NOT NULL, head_review_sha256 TEXT NOT NULL,
 upstream_identity_sha256 TEXT NOT NULL, account_home_sha256 TEXT NOT NULL,
 reason TEXT NOT NULL CHECK(reason='operator_supplement_cancelled'),
 original_effect_certainty TEXT NOT NULL CHECK(original_effect_certainty='unknown_remote_effect'),
 customer_charge_waived BOOLEAN NOT NULL CHECK(customer_charge_waived),
 operator_reference TEXT NOT NULL CHECK(length(operator_reference) BETWEEN 1 AND 255 AND operator_reference !~ '[[:cntrl:]]'),
 queried_at_ms BIGINT NOT NULL, website_confirmed_at_ms BIGINT NOT NULL,
 confirmed_at_ms BIGINT NOT NULL CHECK(confirmed_at_ms>=queried_at_ms AND queried_at_ms>=website_confirmed_at_ms AND website_confirmed_at_ms>0),
 release_event_id UUID NOT NULL UNIQUE,
 finalized_snapshot JSONB CHECK(finalized_snapshot IS NULL OR jsonb_typeof(finalized_snapshot)='object'),
 FOREIGN KEY(original_receipt_id,submission_id,output_id,job_id) REFERENCES provider_receipts(receipt_id,submission_id,output_id,job_id),
 FOREIGN KEY(executor_execution_id,submission_id) REFERENCES provider_submissions(executor_execution_id,submission_id)
);
DO $migration$ DECLARE column_name TEXT; BEGIN
 FOREACH column_name IN ARRAY ARRAY['quote_hash','command_hash','submit_stdout_sha256','query_stdout_sha256','execution_audit_sha256','review_sha256','website_confirmation_sha256','head_review_sha256','upstream_identity_sha256','account_home_sha256'] LOOP
 EXECUTE format('ALTER TABLE dreamina_operator_video_supplement_cancellations ADD CHECK (%I ~ ''^[a-f0-9]{64}$'')',column_name); END LOOP;
END; $migration$;

-- Own terminal audit is separate from the frozen original executor history.
DO $migration$ DECLARE definition TEXT; patched TEXT; BEGIN
 definition:=pg_get_functiondef('dreamina_video_supplement_original_snapshot(uuid)'::REGPROCEDURE);
 patched:=replace(definition,$old$semantic_key<>''job.operator_dreamina_video_supplement_success''$old$,$new$semantic_key NOT IN (''job.operator_dreamina_video_supplement_success'',''job.operator_dreamina_video_supplement_cancelled'')$new$);
 IF patched=definition THEN RAISE EXCEPTION 'missing original snapshot event guard'; END IF; EXECUTE patched;
END; $migration$;
CREATE FUNCTION dreamina_supplement_cancel_snapshot(target_job_id UUID) RETURNS JSONB AS $$
DECLARE result JSONB; observations JSONB;
BEGIN
 result:=dreamina_manual_cancellation_snapshot(target_job_id);
 SELECT COALESCE(jsonb_agg(to_jsonb(observation) ORDER BY observation.observation_id),'[]'::JSONB) INTO observations
 FROM dreamina_operator_video_supplement_observations observation JOIN dreamina_operator_video_supplement_authorizations head USING(authorization_id) WHERE head.job_id=target_job_id;
 RETURN result || jsonb_build_object('supplement_observations',observations);
END; $$ LANGUAGE plpgsql;

CREATE FUNCTION prepare_dreamina_supplement_cancel() RETURNS TRIGGER AS $$
BEGIN
 PERFORM 1 FROM jobs WHERE job_id=NEW.job_id FOR UPDATE;
 UPDATE jobs SET updated_at_ms=updated_at_ms WHERE job_id=NEW.job_id;
 PERFORM assert_operator_dreamina_video_supplement(NEW.job_id);
 IF NEW.finalized_snapshot IS NOT NULL OR NOT EXISTS(
 SELECT 1 FROM dreamina_operator_video_supplement_authorizations head
 JOIN jobs job USING(job_id) JOIN customer_billing_holds hold ON hold.hold_id=head.hold_id
 JOIN customer_price_quotes quote ON quote.quote_id=hold.quote_id
 JOIN provider_account_environments environment ON environment.provider_account_id=head.provider_account_id
 WHERE head.authorization_id=NEW.authorization_id AND head.dispatch_nonce=NEW.dispatch_nonce AND head.job_id=NEW.job_id
 AND head.state IN ('accepted','uncertain') AND head.remote_operation_id=NEW.upstream_task_id AND head.submit_stdout_sha256=NEW.submit_stdout_sha256
 AND head.submission_id=NEW.submission_id AND head.executor_execution_id=NEW.executor_execution_id AND head.output_id=NEW.output_id
 AND head.original_receipt_id=NEW.original_receipt_id AND head.hold_id=NEW.hold_id AND head.provider_account_id=NEW.provider_account_id
 AND head.quote_hash=NEW.quote_hash AND head.command_hash=NEW.command_hash AND quote.quote_id=NEW.quote_id AND quote.quote_hash=NEW.quote_hash
 AND job.request_id=NEW.request_id AND job.state='uncertain' AND job.charged_units=0
 AND hold.currency='USD' AND hold.held_micros>0 AND hold.state='held' AND hold.captured_micros=0 AND hold.released_micros=0
 AND environment.provider_id='dreamina-cli' AND environment.upstream_identity_sha256=NEW.upstream_identity_sha256
 AND encode(sha256(convert_to(environment.environment_ref,'UTF8')),'hex')=NEW.account_home_sha256
 AND head.runtime_binding_json->>'upstream_identity_sha256'=NEW.upstream_identity_sha256
 AND head.runtime_binding_json->>'account_home_sha256'=NEW.account_home_sha256
 AND NEW.queried_at_ms>=head.dispatched_at_ms AND NEW.website_confirmed_at_ms>=head.dispatched_at_ms
 AND NOT EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations child WHERE child.previous_authorization_id=head.authorization_id))
 OR EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_resolutions WHERE job_id=NEW.job_id)
 OR EXISTS(SELECT 1 FROM customer_rated_usage WHERE job_id=NEW.job_id)
 OR EXISTS(SELECT 1 FROM provider_usage_facts WHERE job_id=NEW.job_id)
 OR EXISTS(SELECT 1 FROM ledger_transactions WHERE source_job_id=NEW.job_id)
 OR EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_observations observation JOIN dreamina_operator_video_supplement_authorizations head USING(authorization_id) WHERE head.job_id=NEW.job_id AND observation.query_status='success')
 THEN RAISE EXCEPTION 'supplement cancellation requires the same accepted active head and untouched original hold'; END IF;
 RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_supplement_cancel_prepare BEFORE INSERT ON dreamina_operator_video_supplement_cancellations FOR EACH ROW EXECUTE FUNCTION prepare_dreamina_supplement_cancel();
CREATE FUNCTION protect_dreamina_supplement_cancel() RETURNS TRIGGER AS $$
BEGIN
 IF TG_OP='DELETE' OR OLD.finalized_snapshot IS NOT NULL OR NEW.finalized_snapshot IS NULL
 OR to_jsonb(OLD)-'finalized_snapshot' IS DISTINCT FROM to_jsonb(NEW)-'finalized_snapshot'
 THEN RAISE EXCEPTION 'supplement cancellation authority is permanent'; END IF;
 RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_supplement_cancel_immutable BEFORE UPDATE OR DELETE ON dreamina_operator_video_supplement_cancellations FOR EACH ROW EXECUTE FUNCTION protect_dreamina_supplement_cancel();
CREATE TRIGGER dreamina_supplement_cancel_no_truncate BEFORE TRUNCATE ON dreamina_operator_video_supplement_cancellations FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();

CREATE FUNCTION assert_dreamina_supplement_cancel(target_job_id UUID) RETURNS VOID AS $$
DECLARE authority dreamina_operator_video_supplement_cancellations%ROWTYPE;
BEGIN
 SELECT * INTO STRICT authority FROM dreamina_operator_video_supplement_cancellations WHERE job_id=target_job_id;
 PERFORM assert_operator_dreamina_video_supplement_original(target_job_id);
 IF authority.finalized_snapshot IS NULL OR authority.finalized_snapshot IS DISTINCT FROM dreamina_supplement_cancel_snapshot(target_job_id)
 OR NOT EXISTS(SELECT 1 FROM jobs job JOIN work_items work USING(job_id) JOIN job_attempts attempt ON attempt.execution_id=work.execution_id JOIN job_outputs output ON output.job_id=job.job_id JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id JOIN customer_billing_holds hold ON hold.hold_id=authority.hold_id JOIN customer_rated_usage rating ON rating.job_id=job.job_id
 WHERE job.job_id=target_job_id AND job.state='uncertain' AND job.charged_units=0 AND work.state='uncertain' AND attempt.state='uncertain' AND output.state='uncertain'
 AND quota.state='released' AND quota.committed_units=0 AND quota.released_units=quota.requested_units AND quota.requested_units=job.billable_units
 AND hold.state IN ('settled','released') AND hold.captured_micros=0 AND hold.released_micros=hold.held_micros AND hold.held_micros>0
 AND hold.grant_captured_micros=0 AND hold.account_captured_micros=0 AND hold.grant_released_micros=hold.grant_held_micros AND hold.account_released_micros=hold.account_held_micros
 AND rating.quote_id=authority.quote_id AND rating.total_amount_micros=0 AND rating.currency='USD')
 OR EXISTS(SELECT 1 FROM customer_rated_usage_lines WHERE job_id=target_job_id AND (amount_micros<>0 OR actual_quantity<>0))
 OR NOT EXISTS(SELECT 1 FROM provider_usage_facts WHERE job_id=target_job_id AND fact_domain='customer_billable')
 OR EXISTS(SELECT 1 FROM provider_usage_facts WHERE job_id=target_job_id AND (fact_domain<>'customer_billable' OR terminal_outcome<>'failed' OR quantity<>0 OR metadata_json->>'operator_supplement_cancellation_id' IS DISTINCT FROM authority.cancellation_id::TEXT OR metadata_json->>'reason' IS DISTINCT FROM 'operator_supplement_cancelled'))
 OR EXISTS(SELECT 1 FROM customer_billing_hold_grant_reservations WHERE hold_id=authority.hold_id AND (state<>'released' OR consumed_micros<>0 OR released_micros<>reserved_micros))
 OR EXISTS(SELECT 1 FROM ledger_transactions WHERE source_job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM artifacts WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM job_response_projections WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_resolutions WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE previous_authorization_id=authority.authorization_id)
 OR NOT EXISTS(SELECT 1 FROM outbox_events WHERE job_id=target_job_id AND semantic_key='job.operator_dreamina_video_supplement_cancelled' AND payload_json->>'cancellation_id'=authority.cancellation_id::TEXT AND payload_json->>'reason'='operator_supplement_cancelled')
 OR NOT EXISTS(SELECT 1 FROM job_events WHERE event_id=authority.release_event_id AND job_id=target_job_id AND semantic_key='job.operator_dreamina_video_supplement_cancelled' AND payload_json->>'cancellation_id'=authority.cancellation_id::TEXT AND payload_json->>'reason'='operator_supplement_cancelled')
 THEN RAISE EXCEPTION 'supplement cancellation requires one immutable zero settlement and full original release'; END IF;
END; $$ LANGUAGE plpgsql;
DO $migration$ DECLARE definition TEXT; patched TEXT; BEGIN
 definition:=pg_get_functiondef('assert_operator_dreamina_video_supplement(uuid)'::REGPROCEDURE);
 patched:=regexp_replace(definition,E'\\mBEGIN\\M',$guard$BEGIN
 IF EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_cancellations WHERE job_id=target_job_id) THEN PERFORM assert_dreamina_supplement_cancel(target_job_id); RETURN; END IF;$guard$);
 IF patched=definition THEN RAISE EXCEPTION 'missing supplement assertion'; END IF; EXECUTE patched;
 definition:=pg_get_functiondef('validate_v4_provider_usage_fact_insert()'::REGPROCEDURE);
 patched:=replace(definition,'SELECT CASE WHEN contract_version = 4',$guard$SELECT CASE WHEN contract_version = 4
 AND NEW.fact_domain='customer_billable' AND NEW.attempt_execution_id IS NULL AND NEW.terminal_outcome='failed' AND NEW.quantity=0
 AND NEW.metric='video_requested_second' AND NEW.unit='second' AND NEW.quantity_source='request_derived' AND NEW.confidence='exact' AND NEW.execution_surface='provider_cli' AND receipt.outcome='uncertain'
 AND NEW.evidence_path='dreamina_operator_video_supplement_cancellations.cancellation_id' AND NOT (NEW.metadata_json ? 'operator_resolution_id')
 AND NEW.provider_id='dreamina-cli' AND NEW.metadata_json->>'reason'='operator_supplement_cancelled'
 AND NEW.metadata_json->>'original_effect_certainty'='unknown_remote_effect' AND NEW.metadata_json->'customer_charge_waived'='true'::JSONB
 AND EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_cancellations authority WHERE authority.cancellation_id::TEXT=NEW.metadata_json->>'operator_supplement_cancellation_id' AND authority.job_id=NEW.job_id AND authority.submission_id=NEW.submission_id AND authority.original_receipt_id=NEW.receipt_id AND authority.output_id=NEW.output_id AND authority.provider_account_id=NEW.provider_account_id)
 THEN 'failed' WHEN contract_version = 4$guard$);
 IF patched=definition THEN RAISE EXCEPTION 'missing V4 usage authority guard'; END IF; EXECUTE patched;
END; $migration$;

CREATE FUNCTION fence_dreamina_supplement_cancel() RETURNS TRIGGER AS $$
DECLARE target UUID; row_data JSONB; affected JSONB[];
BEGIN
 IF TG_OP='UPDATE' AND TG_TABLE_NAME='outbox_events' AND to_jsonb(NEW)-'published_at_ms'=to_jsonb(OLD)-'published_at_ms' THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND TG_TABLE_NAME='admission_sessions' AND to_jsonb(NEW)-ARRAY['input_cleanup_state','input_cleanup_owner','input_cleanup_lease_expires_at_ms','input_cleanup_completed_at_ms','updated_at_ms']=to_jsonb(OLD)-ARRAY['input_cleanup_state','input_cleanup_owner','input_cleanup_lease_expires_at_ms','input_cleanup_completed_at_ms','updated_at_ms'] THEN RETURN NEW; END IF;
 IF TG_OP='INSERT' THEN affected:=ARRAY[to_jsonb(NEW)]; ELSIF TG_OP='DELETE' THEN affected:=ARRAY[to_jsonb(OLD)]; ELSE affected:=ARRAY[to_jsonb(OLD),to_jsonb(NEW)]; END IF;
 FOREACH row_data IN ARRAY affected LOOP
  target:=dreamina_video_supplement_mutation_job(row_data);
  IF target IS NULL AND row_data ? 'authorization_id' THEN SELECT job_id INTO target FROM dreamina_operator_video_supplement_authorizations WHERE authorization_id=(row_data->>'authorization_id')::UUID; END IF;
  IF target IS NOT NULL THEN
   PERFORM 1 FROM jobs WHERE job_id=target AND provider_id='dreamina-cli' AND operation='video_generation' FOR UPDATE;
   IF FOUND AND EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_cancellations WHERE job_id=target AND finalized_snapshot IS NOT NULL) THEN RAISE EXCEPTION 'cancelled supplement chain and original zero settlement are permanently frozen'; END IF;
  END IF;
 END LOOP;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE FUNCTION validate_dreamina_supplement_cancel_mutation() RETURNS TRIGGER AS $$
DECLARE target UUID; row_data JSONB; affected JSONB[];
BEGIN
 IF TG_OP='INSERT' THEN affected:=ARRAY[to_jsonb(NEW)]; ELSIF TG_OP='DELETE' THEN affected:=ARRAY[to_jsonb(OLD)]; ELSE affected:=ARRAY[to_jsonb(OLD),to_jsonb(NEW)]; END IF;
 FOREACH row_data IN ARRAY affected LOOP
  target:=dreamina_video_supplement_mutation_job(row_data);
  IF target IS NULL AND row_data ? 'authorization_id' THEN SELECT job_id INTO target FROM dreamina_operator_video_supplement_authorizations WHERE authorization_id=(row_data->>'authorization_id')::UUID; END IF;
  IF EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_cancellations WHERE job_id=target) THEN PERFORM assert_dreamina_supplement_cancel(target); END IF;
 END LOOP;
 RETURN NULL;
END; $$ LANGUAGE plpgsql;
CREATE FUNCTION fence_dreamina_supplement_cancel_truncate() RETURNS TRIGGER AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_cancellations) THEN RAISE EXCEPTION 'cancelled supplement evidence cannot be truncated'; END IF; RETURN NULL;
END; $$ LANGUAGE plpgsql;
DO $migration$ DECLARE target TEXT; BEGIN
 FOREACH target IN ARRAY ARRAY['dreamina_operator_video_supplement_cancellations','dreamina_operator_video_supplement_authorizations','dreamina_operator_video_supplement_resolutions','dreamina_operator_video_supplement_observations','provider_submissions','executor_executions','provider_remote_submit_intents','provider_submit_recoveries','provider_remote_tasks','executor_resolution_decisions','executor_terminal_reductions','executor_capacity_allocations','provider_receipts','provider_task_observations','accepted_completion_budgets','executor_result_manifests','executor_artifact_authorities','provider_remote_video_results','job_payloads','customer_price_quotes','customer_price_quote_lines','job_auth_attributions','job_input_objects','job_input_manifests','artifacts','provider_submit_quarantined_receipts','admission_sessions','jobs','work_items','job_attempts','job_outputs','idempotency_requests','quota_reservations','customer_billing_holds','customer_billing_hold_grant_reservations','credit_grant_events','job_response_projections','provider_usage_facts','usage_events','metering_events','economic_metering_events','customer_rated_usage','customer_rated_usage_lines','customer_rated_usage_fact_links','ledger_transactions','ledger_postings','ledger_transaction_seals','job_events','outbox_events','dreamina_operator_video_success_resolutions','dreamina_operator_rejection_resolutions','dreamina_manual_video_cancellations'] LOOP
  IF target<>'dreamina_operator_video_supplement_cancellations' THEN EXECUTE format('CREATE TRIGGER dreamina_supplement_cancel_fence BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION fence_dreamina_supplement_cancel()',target); END IF;
  EXECUTE format('CREATE CONSTRAINT TRIGGER dreamina_supplement_cancel_consistency AFTER INSERT OR UPDATE OR DELETE ON %I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_dreamina_supplement_cancel_mutation()',target);
  EXECUTE format('CREATE TRIGGER dreamina_supplement_cancel_truncate_fence BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION fence_dreamina_supplement_cancel_truncate()',target);
 END LOOP;
END; $migration$;
