-- Classification only: the original Failed reduction and its completed zero
-- customer settlement remain the only execution and financial authorities.
CREATE TABLE dreamina_manual_video_cancellations (
 authority_id UUID PRIMARY KEY,
 job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE RESTRICT,
 request_id TEXT NOT NULL,
 submission_id UUID NOT NULL UNIQUE REFERENCES provider_submissions(submission_id) ON DELETE RESTRICT,
 executor_execution_id UUID NOT NULL UNIQUE,
 provider_account_id UUID NOT NULL,
 upstream_task_id TEXT NOT NULL,
 hold_id UUID NOT NULL UNIQUE REFERENCES customer_billing_holds(hold_id) ON DELETE RESTRICT,
 quote_id UUID NOT NULL UNIQUE REFERENCES customer_price_quotes(quote_id) ON DELETE RESTRICT,
 original_receipt_id UUID NOT NULL UNIQUE REFERENCES provider_receipts(receipt_id) ON DELETE RESTRICT,
 rated_usage_id UUID NOT NULL UNIQUE REFERENCES customer_rated_usage(rated_usage_id) ON DELETE RESTRICT,
 release_event_id UUID NOT NULL UNIQUE REFERENCES job_events(event_id) ON DELETE RESTRICT,
 quote_hash TEXT NOT NULL CHECK(quote_hash ~ '^[a-f0-9]{64}$'),
 command_hash TEXT NOT NULL CHECK(command_hash ~ '^[a-f0-9]{64}$'),
 execution_binding_sha256 TEXT NOT NULL CHECK(execution_binding_sha256 ~ '^[a-f0-9]{64}$'),
 query_stdout_sha256 TEXT NOT NULL CHECK(query_stdout_sha256 ~ '^[a-f0-9]{64}$'),
 execution_audit_sha256 TEXT NOT NULL CHECK(execution_audit_sha256 ~ '^[a-f0-9]{64}$'),
 review_sha256 TEXT NOT NULL CHECK(review_sha256 ~ '^[a-f0-9]{64}$'),
 website_confirmation_sha256 TEXT NOT NULL CHECK(website_confirmation_sha256 ~ '^[a-f0-9]{64}$'),
 upstream_identity_sha256 TEXT NOT NULL CHECK(upstream_identity_sha256 ~ '^[a-f0-9]{64}$'),
 account_home_sha256 TEXT NOT NULL CHECK(account_home_sha256 ~ '^[a-f0-9]{64}$'),
 operator_reference TEXT NOT NULL CHECK(length(operator_reference) BETWEEN 1 AND 255 AND operator_reference !~ '[[:cntrl:]]'),
 queried_at_ms BIGINT NOT NULL CHECK(queried_at_ms>0),
 website_confirmed_at_ms BIGINT NOT NULL CHECK(website_confirmed_at_ms>0),
 confirmed_at_ms BIGINT NOT NULL CHECK(confirmed_at_ms>=queried_at_ms AND confirmed_at_ms>=website_confirmed_at_ms),
 original_snapshot JSONB NOT NULL CHECK(jsonb_typeof(original_snapshot)='object')
);
CREATE TRIGGER dreamina_manual_cancellation_immutable BEFORE UPDATE OR DELETE ON dreamina_manual_video_cancellations FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation();
CREATE TRIGGER dreamina_manual_cancellation_no_truncate BEFORE TRUNCATE ON dreamina_manual_video_cancellations FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();

CREATE FUNCTION dreamina_manual_cancellation_snapshot(target_job_id UUID) RETURNS JSONB AS $$
DECLARE result JSONB; target TEXT; rows JSONB; predicate TEXT;
BEGIN
 result:=dreamina_video_supplement_original_snapshot(target_job_id);
 FOREACH target IN ARRAY ARRAY['jobs','work_items','job_attempts','job_outputs','quota_reservations','customer_billing_holds','idempotency_requests','customer_billing_hold_grant_reservations','credit_grant_events','customer_rated_usage','customer_rated_usage_lines','customer_rated_usage_fact_links','ledger_transactions','ledger_postings','ledger_transaction_seals','artifacts','job_response_projections','provider_usage_facts','usage_events','metering_events','economic_metering_events','dreamina_operator_video_supplement_authorizations','dreamina_operator_video_success_resolutions','dreamina_operator_rejection_resolutions'] LOOP
  predicate:=CASE target
   WHEN 'job_attempts' THEN 'item.work_item_id IN (SELECT work_item_id FROM work_items WHERE job_id=$1)'
   WHEN 'credit_grant_events' THEN 'item.hold_id IN (SELECT hold_id FROM customer_billing_holds WHERE job_id=$1)'
   WHEN 'customer_billing_hold_grant_reservations' THEN 'item.hold_id IN (SELECT hold_id FROM customer_billing_holds WHERE job_id=$1)'
   WHEN 'customer_rated_usage_fact_links' THEN 'item.rated_usage_line_id IN (SELECT rated_usage_line_id FROM customer_rated_usage_lines WHERE job_id=$1)'
   WHEN 'ledger_transactions' THEN 'item.source_job_id=$1'
   WHEN 'ledger_postings' THEN 'item.transaction_id IN (SELECT transaction_id FROM ledger_transactions WHERE source_job_id=$1)'
   WHEN 'ledger_transaction_seals' THEN 'item.transaction_id IN (SELECT transaction_id FROM ledger_transactions WHERE source_job_id=$1)'
   ELSE 'item.job_id=$1'
  END;
  EXECUTE format('SELECT COALESCE(jsonb_agg(to_jsonb(item)-''updated_at_ms'' ORDER BY (to_jsonb(item)-''updated_at_ms'')::TEXT),''[]''::JSONB) FROM %I item WHERE %s',target,predicate) INTO rows USING target_job_id;
  result:=result || jsonb_build_object(target || '_final',rows);
 END LOOP;
 -- Publication acknowledgements do not change the original terminal event.
 SELECT COALESCE(jsonb_agg(to_jsonb(item)-'published_at_ms' ORDER BY (to_jsonb(item)-'published_at_ms')::TEXT),'[]'::JSONB) INTO rows FROM outbox_events item WHERE job_id=target_job_id;
 RETURN result || jsonb_build_object('outbox_events_original',rows);
END; $$ LANGUAGE plpgsql;

CREATE FUNCTION assert_dreamina_manual_cancellation(target_job_id UUID) RETURNS VOID AS $$
DECLARE authority dreamina_manual_video_cancellations%ROWTYPE;
BEGIN
 SELECT * INTO STRICT authority FROM dreamina_manual_video_cancellations WHERE job_id=target_job_id;
 IF authority.original_snapshot IS DISTINCT FROM dreamina_manual_cancellation_snapshot(target_job_id)
 OR NOT EXISTS(
  SELECT 1 FROM jobs job
  JOIN provider_submissions submission USING(job_id)
  JOIN provider_remote_tasks remote USING(submission_id,executor_execution_id)
  JOIN provider_remote_submit_intents intent USING(submission_id,executor_execution_id)
  JOIN executor_terminal_reductions reduction USING(submission_id,executor_execution_id)
  JOIN provider_receipts receipt ON receipt.receipt_id=reduction.provider_receipt_id AND receipt.submission_id=submission.submission_id AND receipt.job_id=job.job_id
  JOIN customer_billing_holds hold ON hold.job_id=job.job_id
  JOIN customer_price_quotes quote ON quote.quote_id=hold.quote_id AND quote.job_id=job.job_id
  JOIN customer_rated_usage rating ON rating.job_id=job.job_id AND rating.quote_id=quote.quote_id
  JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id AND quota.job_id=job.job_id
  JOIN work_items work ON work.job_id=job.job_id
  JOIN job_attempts attempt ON attempt.execution_id=work.execution_id AND attempt.work_item_id=work.work_item_id AND attempt.lease_epoch=work.lease_epoch
  JOIN job_outputs output ON output.job_id=job.job_id AND output.output_id=submission.output_id
  JOIN job_events event ON event.event_id=authority.release_event_id AND event.job_id=job.job_id
  WHERE job.job_id=target_job_id AND job.provider_id='dreamina-cli' AND job.operation='video_generation'
   AND job.economics_contract_version=4 AND job.state='failed' AND job.charged_units=0
   AND job.request_id=authority.request_id AND job.output_count=1
   AND submission.submission_id=authority.submission_id AND submission.executor_execution_id=authority.executor_execution_id
   AND submission.provider_account_id=authority.provider_account_id AND submission.state='failed'
   AND submission.command_hash=authority.command_hash AND intent.execution_binding_sha256=authority.execution_binding_sha256
   AND intent.state='attached' AND intent.remote_operation_id=authority.upstream_task_id
   AND remote.provider_id='dreamina-cli' AND remote.provider_account_id=authority.provider_account_id
   AND remote.remote_operation_id=authority.upstream_task_id AND remote.state='failed'
   AND remote.artifact_ref IS NULL AND remote.poll_owner IS NULL AND remote.poll_lease_expires_at_ms IS NULL
   AND reduction.state='completed' AND reduction.resolved_state='failed'
   AND reduction.customer_artifact_id IS NULL AND reduction.remote_result_id IS NULL
   AND reduction.quota_reservation_id=job.reservation_id
   AND receipt.receipt_id=authority.original_receipt_id AND receipt.outcome='failed'
   AND hold.hold_id=authority.hold_id AND hold.quote_id=authority.quote_id
   AND hold.currency='USD' AND hold.held_micros>0 AND hold.state IN ('settled','released')
   AND hold.captured_micros=0 AND hold.released_micros=hold.held_micros
   AND hold.grant_captured_micros=0 AND hold.account_captured_micros=0
   AND hold.grant_released_micros=hold.grant_held_micros AND hold.account_released_micros=hold.account_held_micros
   AND quote.quote_hash=authority.quote_hash AND quote.currency='USD' AND quote.operation='video_generation' AND quote.media_kind='video'
   AND rating.rated_usage_id=authority.rated_usage_id AND rating.total_amount_micros=0 AND rating.currency=hold.currency
   AND quota.state='released' AND quota.committed_units=0 AND quota.released_units=quota.requested_units AND quota.requested_units=job.billable_units
   AND work.state='failed' AND attempt.state='failed' AND output.state='failed'
   AND event.event_type='job.failed' AND event.semantic_key='work.' || work.work_item_id::TEXT || '.executor-terminal'
   AND event.payload_json->>'terminal_state'='failed' AND (event.payload_json->>'succeeded_outputs')::INTEGER=0 AND (event.payload_json->>'uncertain_outputs')::INTEGER=0
 )
 OR (SELECT count(*) FROM provider_submissions WHERE job_id=target_job_id)<>1
 OR EXISTS(SELECT 1 FROM customer_rated_usage_lines WHERE job_id=target_job_id AND amount_micros<>0)
 OR EXISTS(SELECT 1 FROM customer_billing_hold_grant_reservations WHERE hold_id=authority.hold_id AND (state<>'released' OR consumed_micros<>0 OR released_micros<>reserved_micros))
 OR (SELECT COALESCE(sum(reserved_micros),0) FROM customer_billing_hold_grant_reservations WHERE hold_id=authority.hold_id)<>(SELECT grant_held_micros FROM customer_billing_holds WHERE hold_id=authority.hold_id)
 OR EXISTS(SELECT 1 FROM ledger_transactions WHERE source_job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM artifacts WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM provider_remote_video_results result JOIN provider_submissions submission USING(submission_id) WHERE submission.job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM job_response_projections WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM executor_result_manifests result JOIN provider_submissions submission USING(submission_id) WHERE submission.job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM executor_artifact_authorities WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM provider_usage_facts WHERE job_id=target_job_id AND terminal_outcome='succeeded')
 OR EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM dreamina_operator_video_success_resolutions WHERE job_id=target_job_id)
 OR EXISTS(SELECT 1 FROM dreamina_operator_rejection_resolutions WHERE job_id=target_job_id)
 THEN RAISE EXCEPTION 'manual cancellation requires unchanged accepted failed video and completed zero-charge release'; END IF;
END; $$ LANGUAGE plpgsql;

CREATE FUNCTION prepare_dreamina_manual_cancellation() RETURNS TRIGGER AS $$
BEGIN
 PERFORM 1 FROM jobs WHERE job_id=NEW.job_id FOR UPDATE;
 -- Create a new tuple even though classification changes no job fields. An old
 -- REPEATABLE READ writer must serialize, rather than miss the new authority.
 UPDATE jobs SET updated_at_ms=updated_at_ms WHERE job_id=NEW.job_id;
 IF NOT EXISTS(SELECT 1 FROM provider_account_environments environment WHERE environment.provider_account_id=NEW.provider_account_id AND environment.provider_id='dreamina-cli' AND environment.environment_kind='dreamina_home_v1' AND environment.upstream_identity_sha256=NEW.upstream_identity_sha256 AND encode(sha256(convert_to(environment.environment_ref,'UTF8')),'hex')=NEW.account_home_sha256)
 THEN RAISE EXCEPTION 'manual cancellation account evidence mismatch'; END IF;
 NEW.original_snapshot:=dreamina_manual_cancellation_snapshot(NEW.job_id);
 RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_manual_cancellation_prepare BEFORE INSERT ON dreamina_manual_video_cancellations FOR EACH ROW EXECUTE FUNCTION prepare_dreamina_manual_cancellation();

CREATE FUNCTION fence_dreamina_manual_cancellation() RETURNS TRIGGER AS $$
DECLARE target UUID; row_data JSONB; affected JSONB[];
BEGIN
 IF TG_OP='UPDATE' AND TG_TABLE_NAME='outbox_events' AND to_jsonb(NEW)-'published_at_ms'=to_jsonb(OLD)-'published_at_ms' THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND TG_TABLE_NAME='admission_sessions' AND to_jsonb(NEW)-ARRAY['input_cleanup_state','input_cleanup_owner','input_cleanup_lease_expires_at_ms','input_cleanup_completed_at_ms','updated_at_ms']=to_jsonb(OLD)-ARRAY['input_cleanup_state','input_cleanup_owner','input_cleanup_lease_expires_at_ms','input_cleanup_completed_at_ms','updated_at_ms'] THEN RETURN NEW; END IF;
 IF TG_OP='INSERT' THEN affected:=ARRAY[to_jsonb(NEW)]; ELSIF TG_OP='DELETE' THEN affected:=ARRAY[to_jsonb(OLD)]; ELSE affected:=ARRAY[to_jsonb(OLD),to_jsonb(NEW)]; END IF;
 FOREACH row_data IN ARRAY affected LOOP
  target:=dreamina_video_supplement_mutation_job(row_data);
  IF target IS NOT NULL THEN
   PERFORM 1 FROM jobs WHERE job_id=target AND provider_id='dreamina-cli' AND operation='video_generation' FOR UPDATE;
   IF EXISTS(SELECT 1 FROM dreamina_manual_video_cancellations WHERE job_id=target) THEN RAISE EXCEPTION 'original failed video and zero settlement are permanently fenced by manual cancellation'; END IF;
  END IF;
 END LOOP;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END; $$ LANGUAGE plpgsql;
CREATE FUNCTION validate_dreamina_manual_cancellation() RETURNS TRIGGER AS $$
DECLARE target UUID; row_data JSONB; affected JSONB[];
BEGIN
 IF TG_OP='INSERT' THEN affected:=ARRAY[to_jsonb(NEW)]; ELSIF TG_OP='DELETE' THEN affected:=ARRAY[to_jsonb(OLD)]; ELSE affected:=ARRAY[to_jsonb(OLD),to_jsonb(NEW)]; END IF;
 FOREACH row_data IN ARRAY affected LOOP
  target:=dreamina_video_supplement_mutation_job(row_data);
  IF EXISTS(SELECT 1 FROM dreamina_manual_video_cancellations WHERE job_id=target) THEN PERFORM assert_dreamina_manual_cancellation(target); END IF;
 END LOOP;
 RETURN NULL;
END; $$ LANGUAGE plpgsql;
CREATE FUNCTION fence_dreamina_manual_cancellation_truncate() RETURNS TRIGGER AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM dreamina_manual_video_cancellations) THEN RAISE EXCEPTION 'manual cancellation evidence cannot be truncated'; END IF;
 RETURN NULL;
END; $$ LANGUAGE plpgsql;
DO $migration$
DECLARE target TEXT;
BEGIN
 FOREACH target IN ARRAY ARRAY['provider_submissions','executor_executions','provider_remote_submit_intents','provider_submit_recoveries','provider_remote_tasks','executor_resolution_decisions','executor_terminal_reductions','executor_capacity_allocations','provider_receipts','provider_task_observations','accepted_completion_budgets','executor_result_manifests','executor_artifact_authorities','provider_remote_video_results','job_payloads','customer_price_quotes','customer_price_quote_lines','job_auth_attributions','job_input_objects','job_input_manifests','artifacts','provider_submit_quarantined_receipts','admission_sessions','jobs','work_items','job_attempts','job_outputs','idempotency_requests','quota_reservations','customer_billing_holds','customer_billing_hold_grant_reservations','credit_grant_events','job_response_projections','provider_usage_facts','usage_events','metering_events','economic_metering_events','customer_rated_usage','customer_rated_usage_lines','customer_rated_usage_fact_links','ledger_transactions','ledger_postings','ledger_transaction_seals','job_events','outbox_events','dreamina_operator_video_supplement_authorizations','dreamina_operator_video_supplement_resolutions','dreamina_operator_video_success_resolutions','dreamina_operator_rejection_resolutions'] LOOP
  EXECUTE format('CREATE TRIGGER dreamina_manual_cancellation_fence BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION fence_dreamina_manual_cancellation()',target);
  EXECUTE format('CREATE CONSTRAINT TRIGGER dreamina_manual_cancellation_consistency AFTER INSERT OR UPDATE OR DELETE ON %I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_dreamina_manual_cancellation()',target);
  EXECUTE format('CREATE TRIGGER dreamina_manual_cancellation_truncate_fence BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION fence_dreamina_manual_cancellation_truncate()',target);
 END LOOP;
END; $migration$;
CREATE CONSTRAINT TRIGGER dreamina_manual_cancellation_consistency AFTER INSERT ON dreamina_manual_video_cancellations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_dreamina_manual_cancellation();
