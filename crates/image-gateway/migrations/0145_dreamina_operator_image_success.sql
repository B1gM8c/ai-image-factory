-- A reviewed single-image success is a separate authority. The original poll,
-- executor decision, reduction, and uncertain receipt remain immutable evidence.
CREATE TABLE dreamina_operator_image_success_resolutions (
    resolution_id UUID PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE RESTRICT,
    submission_id UUID NOT NULL UNIQUE,
    executor_execution_id UUID NOT NULL UNIQUE,
    output_id UUID NOT NULL UNIQUE,
    original_receipt_id UUID NOT NULL UNIQUE,
    hold_id UUID NOT NULL UNIQUE REFERENCES customer_billing_holds(hold_id) ON DELETE RESTRICT,
    artifact_id UUID NOT NULL UNIQUE REFERENCES artifacts(artifact_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    remote_operation_id TEXT NOT NULL CHECK (
        remote_operation_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
        AND remote_operation_id NOT LIKE '%://%'
    ),
    command_hash TEXT NOT NULL CHECK (command_hash ~ '^[a-f0-9]{64}$'),
    execution_binding_sha256 TEXT NOT NULL CHECK (execution_binding_sha256 ~ '^[a-f0-9]{64}$'),
    quote_hash TEXT NOT NULL CHECK (quote_hash ~ '^[a-f0-9]{64}$'),
    reviewed_stdout_sha256 TEXT NOT NULL CHECK (reviewed_stdout_sha256 ~ '^[a-f0-9]{64}$'),
    execution_audit_sha256 TEXT NOT NULL CHECK (execution_audit_sha256 ~ '^[a-f0-9]{64}$'),
    review_sha256 TEXT NOT NULL CHECK (review_sha256 ~ '^[a-f0-9]{64}$'),
    input_sha256 TEXT NOT NULL CHECK (input_sha256 ~ '^[a-f0-9]{64}$'),
    selected_source_index INTEGER NOT NULL CHECK (selected_source_index = 0),
    selected_item_id TEXT NOT NULL CHECK (
        char_length(selected_item_id) BETWEEN 1 AND 255
        AND selected_item_id !~ '[[:cntrl:]]'
    ),
    upstream_result_count INTEGER NOT NULL CHECK (upstream_result_count = 4),
    artifact_sha256 TEXT NOT NULL CHECK (artifact_sha256 ~ '^[a-f0-9]{64}$'),
    artifact_byte_size BIGINT NOT NULL CHECK (artifact_byte_size BETWEEN 1 AND 33554432),
    artifact_media_type TEXT NOT NULL CHECK (artifact_media_type = 'image/png'),
    artifact_width INTEGER NOT NULL CHECK (artifact_width BETWEEN 1 AND 8192),
    artifact_height INTEGER NOT NULL CHECK (artifact_height BETWEEN 1 AND 8192),
    CHECK (artifact_width::BIGINT * artifact_height::BIGINT <= 16777216),
    operator_reference TEXT NOT NULL CHECK (
        char_length(operator_reference) BETWEEN 1 AND 255
        AND operator_reference !~ '[[:cntrl:]]'
    ),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms > 0),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES provider_submissions(executor_execution_id, submission_id) ON DELETE RESTRICT,
    FOREIGN KEY (original_receipt_id, submission_id, output_id, job_id)
        REFERENCES provider_receipts(receipt_id, submission_id, output_id, job_id) ON DELETE RESTRICT
);

CREATE TRIGGER dreamina_operator_image_success_immutable
    BEFORE UPDATE OR DELETE ON dreamina_operator_image_success_resolutions
    FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation();
CREATE TRIGGER dreamina_operator_image_success_reject_truncate
    BEFORE TRUNCATE ON dreamina_operator_image_success_resolutions
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();

-- Shared by the insert fence, effective usage authority, and final assertion.
-- This checks original evidence only; it never changes its effective outcome.
CREATE FUNCTION assert_operator_dreamina_image_success_evidence(
    resolution dreamina_operator_image_success_resolutions
) RETURNS VOID AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM jobs job
        JOIN job_payloads payload ON payload.job_id = job.job_id
        JOIN provider_submissions submission ON submission.job_id = job.job_id
        JOIN executor_executions execution
          ON execution.executor_execution_id = submission.executor_execution_id
         AND execution.submission_id = submission.submission_id
        JOIN executor_resolution_decisions decision
          ON decision.decision_id = execution.resolution_decision_id
         AND decision.decision_id = submission.resolution_decision_id
         AND decision.executor_execution_id = execution.executor_execution_id
         AND decision.submission_id = submission.submission_id
        JOIN executor_terminal_reductions reduction
          ON reduction.submission_id = submission.submission_id
         AND reduction.executor_execution_id = execution.executor_execution_id
         AND reduction.resolution_decision_id = decision.decision_id
        JOIN provider_receipts receipt ON receipt.receipt_id = reduction.provider_receipt_id
        JOIN provider_remote_submit_intents intent
          ON intent.submission_id = submission.submission_id
         AND intent.executor_execution_id = execution.executor_execution_id
        JOIN provider_submit_recoveries recovery
          ON recovery.submission_id = submission.submission_id
         AND recovery.executor_execution_id = execution.executor_execution_id
        JOIN provider_remote_tasks task
          ON task.submission_id = submission.submission_id
         AND task.executor_execution_id = execution.executor_execution_id
        JOIN provider_task_observations observation
          ON observation.observation_id = task.state_observation_id
         AND observation.observation_id = decision.provider_task_observation_id
         AND observation.submission_id = task.submission_id
         AND observation.executor_execution_id = task.executor_execution_id
        JOIN executor_capacity_allocations allocation
          ON allocation.submission_id = submission.submission_id
         AND allocation.executor_execution_id = execution.executor_execution_id
        JOIN work_items work ON work.work_item_id = submission.work_item_id AND work.job_id = job.job_id
        JOIN job_attempts attempt
          ON attempt.work_item_id = work.work_item_id
         AND attempt.execution_id = work.execution_id
         AND attempt.lease_epoch = work.lease_epoch
         AND attempt.execution_id = submission.created_by_execution_id
         AND attempt.lease_epoch = submission.created_by_lease_epoch
        JOIN job_outputs output ON output.output_id = submission.output_id AND output.job_id = job.job_id
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
          AND quota.job_id = job.job_id AND quota.tenant_id = job.tenant_id
        JOIN customer_billing_holds hold ON hold.hold_id = resolution.hold_id
          AND hold.job_id = job.job_id AND hold.tenant_id = job.tenant_id
        JOIN customer_price_quotes quote ON quote.quote_id = hold.quote_id
          AND quote.job_id = job.job_id AND quote.tenant_id = job.tenant_id
          AND quote.currency = hold.currency
        WHERE job.job_id = resolution.job_id
          AND submission.submission_id = resolution.submission_id
          AND execution.executor_execution_id = resolution.executor_execution_id
          AND receipt.receipt_id = resolution.original_receipt_id
          AND output.output_id = resolution.output_id
          AND job.provider_id = 'dreamina-cli' AND submission.provider_id = job.provider_id
          AND submission.operation_id = 'images.generations'
          AND submission.completion_mode = 'remote_task'
          AND job.operation = 'generation' AND job.output_count = 1
          AND job.requested_units = 1 AND job.billable_units = 1
          AND job.economics_contract_version = 4
          AND payload.command_schema = submission.command_schema
          AND payload.command_json ->> 'operation' = 'text2image'
          AND payload.command_json -> 'schema_version' = '1'::JSONB
          AND payload.command_json -> 'generate_num' = '1'::JSONB
          AND submission.command_hash = resolution.command_hash
          AND output.output_index = 0 AND output.billable_units = 1
          AND quota.requested_units = 1
          AND submission.state = 'uncertain' AND execution.state = 'uncertain'
          AND submission.error_code = 'dreamina_poll_artifact_invalid'
          AND execution.error_code = 'dreamina_poll_artifact_invalid'
          AND submission.result_manifest_id IS NULL
          AND execution.executor_owner IS NULL AND execution.lease_expires_at_ms IS NULL
          AND decision.source = 'remote_provider_observation'
          AND decision.resolved_state = 'uncertain' AND decision.error_code = 'dreamina_poll_artifact_invalid'
          AND decision.result_manifest_id IS NULL
          AND reduction.state = 'completed' AND reduction.resolved_state = 'uncertain'
          AND reduction.customer_artifact_id IS NULL
          AND reduction.quota_reservation_id = quota.reservation_id
          AND receipt.outcome = 'uncertain' AND receipt.receipt_schema = 'executor.resolution.v1'
          AND receipt.submission_id = submission.submission_id AND receipt.job_id = job.job_id
          AND receipt.output_id = output.output_id AND receipt.provider_id = job.provider_id
          AND receipt.evidence ->> 'submission_id' = submission.submission_id::TEXT
          AND receipt.evidence ->> 'executor_execution_id' = execution.executor_execution_id::TEXT
          AND receipt.evidence ->> 'resolved_state' = 'uncertain'
          AND receipt.evidence ->> 'resolution_decision_id' = decision.decision_id::TEXT
          AND receipt.evidence ->> 'error_code' = 'dreamina_poll_artifact_invalid'
          AND intent.state = 'attached' AND intent.submit_lease_epoch = 1
          AND intent.remote_operation_id = resolution.remote_operation_id
          AND intent.execution_binding_sha256 = resolution.execution_binding_sha256
          AND intent.receipt_event_identity IS NOT NULL
          AND recovery.state = 'closed' AND recovery.invocation_attempt = 1
          AND recovery.recovery_owner IS NULL AND recovery.recovery_lease_expires_at_ms IS NULL
          AND task.state = 'uncertain' AND task.error_code = 'dreamina_poll_artifact_invalid'
          AND task.remote_operation_id = resolution.remote_operation_id
          AND task.artifact_ref IS NULL AND task.next_poll_at_ms IS NULL
          AND task.effect_certainty = 'unknown_remote_effect'
          AND task.poll_owner IS NULL AND task.poll_lease_expires_at_ms IS NULL
          AND task.poll_claimed_at_ms IS NULL AND task.deadline_quarantine_id IS NULL
          AND observation.source = 'poll' AND observation.observed_state = 'uncertain'
          AND observation.error_code = 'dreamina_poll_artifact_invalid'
          AND observation.artifact_ref IS NULL AND observation.result_manifest_id IS NULL
          AND allocation.state = 'released' AND allocation.released_state = 'uncertain'
          AND allocation.release_reason = 'remote_provider_observation'
          AND allocation.release_decision_id = decision.decision_id
          AND work.kind = 'image_batch'
          AND work.lease_owner IS NULL AND work.lease_expires_at_ms IS NULL
          AND quote.quote_hash = resolution.quote_hash
          AND quote.operation = job.operation AND quote.media_kind = 'image'
          AND quote.execution_surface = 'provider_cli'
          AND hold.held_micros = quote.max_total_micros
          AND resolution.created_at_ms >= receipt.created_at_ms
    ) OR (SELECT COUNT(*) FROM provider_submissions WHERE job_id = resolution.job_id) <> 1
      OR (SELECT COUNT(*) FROM job_outputs WHERE job_id = resolution.job_id) <> 1
      OR (SELECT COUNT(*) FROM provider_receipts WHERE job_id = resolution.job_id) <> 1
      OR (SELECT COUNT(*) FROM job_attempts attempt JOIN work_items work USING (work_item_id)
          WHERE work.job_id = resolution.job_id) <> 1
      OR (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = resolution.job_id
          AND terminal_outcome = 'succeeded') <> 1
      OR NOT EXISTS (SELECT 1 FROM customer_price_quote_lines WHERE job_id = resolution.job_id
          AND terminal_outcome = 'succeeded' AND metric = 'image_output' AND unit = 'image'
          AND max_quantity = 1 AND quantity_source = 'request_derived')
      OR EXISTS (SELECT 1 FROM executor_result_manifests WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM executor_artifact_authorities WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM provider_submit_quarantined_receipts WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM provider_remote_video_results WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM dreamina_native_batch_members WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM accepted_completion_budgets WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM dreamina_operator_rejection_resolutions WHERE job_id = resolution.job_id)
    THEN
        RAISE EXCEPTION 'Dreamina operator image success requires one original uncertain single-image execution';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_operator_dreamina_image_success_insert() RETURNS TRIGGER AS $$
BEGIN
    PERFORM 1 FROM jobs WHERE job_id = NEW.job_id FOR UPDATE;
    PERFORM 1 FROM customer_billing_holds WHERE hold_id = NEW.hold_id FOR UPDATE;
    PERFORM assert_operator_dreamina_image_success_evidence(NEW);
    IF NOT EXISTS (
        SELECT 1 FROM jobs job
        JOIN work_items work ON work.job_id = job.job_id
        JOIN job_attempts attempt ON attempt.execution_id = work.execution_id
          AND attempt.work_item_id = work.work_item_id AND attempt.lease_epoch = work.lease_epoch
        JOIN job_outputs output ON output.job_id = job.job_id
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
        JOIN customer_billing_holds hold ON hold.hold_id = NEW.hold_id
        WHERE job.job_id = NEW.job_id AND job.state = 'uncertain'
          AND job.charged_units = 0 AND job.last_error_code = 'dreamina_poll_artifact_invalid'
          AND work.state = 'uncertain' AND attempt.state = 'uncertain'
          AND output.state = 'uncertain' AND output.error_code = 'dreamina_poll_artifact_invalid'
          AND quota.state IN ('reserved', 'expired')
          AND quota.committed_units = 0 AND quota.released_units = 0
          AND hold.state = 'held' AND hold.captured_micros = 0 AND hold.released_micros = 0
          AND hold.grant_captured_micros = 0 AND hold.account_captured_micros = 0
          AND hold.grant_released_micros = 0 AND hold.account_released_micros = 0
    ) OR EXISTS (SELECT 1 FROM artifacts WHERE job_id = NEW.job_id)
      OR EXISTS (SELECT 1 FROM job_response_projections WHERE job_id = NEW.job_id)
      OR EXISTS (SELECT 1 FROM provider_usage_facts WHERE job_id = NEW.job_id)
      OR EXISTS (SELECT 1 FROM usage_events WHERE job_id = NEW.job_id)
      OR EXISTS (SELECT 1 FROM customer_rated_usage WHERE job_id = NEW.job_id)
      OR EXISTS (SELECT 1 FROM ledger_transactions WHERE source_job_id = NEW.job_id)
      OR EXISTS (SELECT 1 FROM customer_billing_hold_grant_reservations WHERE hold_id = NEW.hold_id
          AND (state <> 'reserved' OR consumed_micros <> 0 OR released_micros <> 0))
      OR EXISTS (SELECT 1 FROM idempotency_requests WHERE job_id = NEW.job_id
          AND (state <> 'uncertain' OR terminal_outcome IS DISTINCT FROM 'uncertain'))
      OR NOT EXISTS (SELECT 1 FROM idempotency_requests WHERE job_id = NEW.job_id)
    THEN
        RAISE EXCEPTION 'Dreamina operator image success requires untouched held funds and no prior settlement';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_operator_image_success_insert_guard
    BEFORE INSERT ON dreamina_operator_image_success_resolutions
    FOR EACH ROW EXECUTE FUNCTION validate_operator_dreamina_image_success_insert();

CREATE FUNCTION assert_operator_dreamina_image_success(target_job_id UUID) RETURNS VOID AS $$
DECLARE
    resolution dreamina_operator_image_success_resolutions%ROWTYPE;
    rating customer_rated_usage%ROWTYPE;
    hold customer_billing_holds%ROWTYPE;
    charge_id UUID;
BEGIN
    SELECT * INTO STRICT resolution FROM dreamina_operator_image_success_resolutions
    WHERE job_id = target_job_id;
    PERFORM assert_operator_dreamina_image_success_evidence(resolution);
    SELECT * INTO STRICT rating FROM customer_rated_usage WHERE job_id = target_job_id;
    SELECT * INTO STRICT hold FROM customer_billing_holds WHERE hold_id = resolution.hold_id;
    IF NOT EXISTS (
        SELECT 1 FROM jobs job
        JOIN work_items work ON work.job_id = job.job_id
        JOIN job_attempts attempt ON attempt.execution_id = work.execution_id
          AND attempt.work_item_id = work.work_item_id AND attempt.lease_epoch = work.lease_epoch
        JOIN job_outputs output ON output.output_id = resolution.output_id AND output.job_id = job.job_id
        JOIN artifacts artifact ON artifact.artifact_id = resolution.artifact_id
          AND artifact.job_id = job.job_id AND artifact.tenant_id = job.tenant_id
          AND artifact.work_item_id = work.work_item_id AND artifact.execution_id = attempt.execution_id
          AND artifact.lease_epoch = attempt.lease_epoch
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
        JOIN job_response_projections projection ON projection.job_id = job.job_id
        WHERE job.job_id = target_job_id AND job.state = 'succeeded' AND job.charged_units = 1
          AND job.last_error_code IS NULL AND work.state = 'succeeded' AND attempt.state = 'succeeded'
          AND attempt.error_code IS NULL AND output.state = 'succeeded' AND output.error_code IS NULL
          AND quota.state = 'committed' AND quota.committed_units = 1 AND quota.released_units = 0
          AND artifact.state = 'ready' AND artifact.output_index = 0
          AND artifact.object_key = 'objects/'
              || substring(replace(resolution.artifact_id::TEXT, '-', '') FROM 1 FOR 2)
              || '/' || replace(resolution.artifact_id::TEXT, '-', '')
          AND artifact.sha256_hex = resolution.artifact_sha256
          AND artifact.byte_size = resolution.artifact_byte_size
          AND artifact.media_type = resolution.artifact_media_type
          AND projection.artifact_count = 1
          AND (SELECT COUNT(*) FROM job_events WHERE job_id = target_job_id
              AND semantic_key = 'work.' || work.work_item_id::TEXT || '.executor-terminal'
              AND event_type = 'job.uncertain' AND payload_json ->> 'terminal_state' = 'uncertain'
              AND payload_json ->> 'error_code' = 'dreamina_poll_artifact_invalid') = 1
          AND (SELECT COUNT(*) FROM outbox_events WHERE job_id = target_job_id
              AND semantic_key = 'work.' || work.work_item_id::TEXT || '.executor-terminal'
              AND event_type = 'job.uncertain' AND payload_json ->> 'terminal_state' = 'uncertain'
              AND payload_json ->> 'error_code' = 'dreamina_poll_artifact_invalid') = 1
    ) OR (SELECT COUNT(*) FROM artifacts WHERE job_id = target_job_id) <> 1
      OR (SELECT COUNT(*) FROM job_response_projections WHERE job_id = target_job_id) <> 1
      OR EXISTS (SELECT 1 FROM idempotency_requests WHERE job_id = target_job_id
          AND (state <> 'succeeded' OR terminal_outcome IS DISTINCT FROM 'succeeded'))
      OR NOT EXISTS (SELECT 1 FROM idempotency_requests WHERE job_id = target_job_id)
      OR (SELECT COUNT(*) FROM provider_usage_facts WHERE job_id = target_job_id) <> 1
      OR NOT EXISTS (
          SELECT 1 FROM provider_usage_facts fact
          JOIN provider_submissions submission ON submission.submission_id = fact.submission_id
          WHERE fact.job_id = target_job_id AND fact.output_id = resolution.output_id
            AND fact.submission_id = resolution.submission_id AND fact.receipt_id = resolution.original_receipt_id
            AND fact.attempt_execution_id IS NULL AND fact.provider_id = 'dreamina-cli'
            AND fact.provider_account_id IS NOT DISTINCT FROM submission.provider_account_id
            AND fact.fact_domain = 'customer_billable' AND fact.terminal_outcome = 'succeeded'
            AND fact.execution_surface = 'provider_cli' AND fact.metric = 'image_output'
            AND fact.quantity = 1 AND fact.unit = 'image' AND fact.quantity_source = 'request_derived'
            AND fact.confidence = 'exact'
            AND fact.metadata_json ->> 'operator_resolution_id' = resolution.resolution_id::TEXT)
      OR (SELECT COUNT(*) FROM usage_events WHERE job_id = target_job_id) <> 1
      OR NOT EXISTS (SELECT 1 FROM usage_events usage JOIN jobs job USING (job_id)
          WHERE usage.job_id = target_job_id AND usage.tenant_id = job.tenant_id
            AND usage.operation = job.operation AND usage.request_id = job.request_id
            AND usage.units = 1 AND usage.outcome = 'charged'
            AND usage.billing_metric = 'output' AND usage.billing_unit = 'output')
      OR (SELECT COUNT(*) FROM customer_rated_usage_lines WHERE job_id = target_job_id) <> 1
      OR NOT EXISTS (
          SELECT 1 FROM customer_rated_usage_lines line
          JOIN customer_price_quote_lines quoted ON quoted.quote_line_id = line.quote_line_id
            AND quoted.quote_id = line.quote_id AND quoted.job_id = line.job_id
          JOIN customer_rated_usage_fact_links link ON link.rated_usage_line_id = line.rated_usage_line_id
          JOIN provider_usage_facts fact ON fact.usage_fact_id = link.usage_fact_id
          WHERE line.rated_usage_id = rating.rated_usage_id AND line.quote_id = hold.quote_id
            AND line.job_id = target_job_id AND quoted.terminal_outcome = 'succeeded'
            AND line.actual_quantity = 1 AND line.amount_micros = quoted.max_amount_micros
            AND rating.total_amount_micros = line.amount_micros AND rating.currency = hold.currency
            AND rating.quote_id = hold.quote_id AND fact.job_id = target_job_id)
      OR hold.state <> 'settled' OR hold.captured_micros <> rating.total_amount_micros
      OR hold.released_micros <> hold.held_micros - hold.captured_micros
      OR hold.grant_captured_micros + hold.grant_released_micros <> hold.grant_held_micros
      OR hold.account_captured_micros + hold.account_released_micros <> hold.account_held_micros
      OR EXISTS (SELECT 1 FROM customer_billing_hold_grant_reservations grant_reservation
          WHERE grant_reservation.hold_id = hold.hold_id
          AND (grant_reservation.state = 'reserved'
              OR grant_reservation.consumed_micros + grant_reservation.released_micros
                  <> grant_reservation.reserved_micros))
      OR (SELECT COUNT(*) FROM metering_events WHERE job_id = target_job_id
          AND event_type = 'operator_image_success') <> 1
      OR NOT EXISTS (SELECT 1 FROM metering_events WHERE job_id = target_job_id
          AND event_type = 'operator_image_success' AND outcome = 'succeeded' AND units = 1)
      OR (SELECT COUNT(*) FROM job_events WHERE job_id = target_job_id
          AND semantic_key = 'job.operator_dreamina_image_success' AND event_type = 'job.succeeded'
          AND payload_json ->> 'resolution_id' = resolution.resolution_id::TEXT) <> 1
      OR (SELECT COUNT(*) FROM job_events WHERE job_id = target_job_id AND event_type = 'job.succeeded') <> 1
      OR (SELECT COUNT(*) FROM outbox_events WHERE job_id = target_job_id
          AND semantic_key = 'job.operator_dreamina_image_success' AND event_type = 'job.succeeded'
          AND payload_json ->> 'resolution_id' = resolution.resolution_id::TEXT) <> 1
      OR (SELECT COUNT(*) FROM outbox_events WHERE job_id = target_job_id AND event_type = 'job.succeeded') <> 1
    THEN
        RAISE EXCEPTION 'Dreamina operator image success requires one artifact, one frozen-price charge, and consistent success projections';
    END IF;
    PERFORM validate_customer_billing_hold_grant_split(hold.hold_id);
    IF rating.total_amount_micros = 0 THEN
        IF EXISTS (SELECT 1 FROM ledger_transactions WHERE source_job_id = target_job_id) THEN
            RAISE EXCEPTION 'free operator image success cannot create a charge';
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
            RAISE EXCEPTION 'Dreamina operator image success requires one sealed balanced customer ledger charge';
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_operator_dreamina_image_success() RETURNS TRIGGER AS $$
BEGIN
    PERFORM assert_operator_dreamina_image_success(NEW.job_id);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER dreamina_operator_image_success_consistency
    AFTER INSERT ON dreamina_operator_image_success_resolutions DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_operator_dreamina_image_success();

-- Preserve every historical guard, including the 0141 refusal overlay.
DO $migration$
DECLARE target REGPROCEDURE; definition TEXT; patched TEXT;
BEGIN
    FOREACH target IN ARRAY ARRAY[
        'assert_terminal_parent_completion(uuid)'::REGPROCEDURE,
        'assert_terminal_quota_accounting(uuid)'::REGPROCEDURE
    ] LOOP
        definition := pg_get_functiondef(target);
        IF strpos(definition, 'assert_operator_dreamina_rejection(target_job_id)') = 0 THEN
            RAISE EXCEPTION 'expected existing operator refusal guard in %', target;
        END IF;
        patched := regexp_replace(definition, E'\\mBEGIN\\M', $guard$BEGIN
    IF EXISTS (SELECT 1 FROM dreamina_operator_image_success_resolutions WHERE job_id = target_job_id) THEN
        PERFORM assert_operator_dreamina_image_success(target_job_id);
        RETURN;
    END IF;$guard$);
        IF patched = definition THEN RAISE EXCEPTION 'expected terminal entry point %', target; END IF;
        EXECUTE patched;
    END LOOP;

    definition := pg_get_functiondef('validate_v4_provider_usage_fact_insert()'::REGPROCEDURE);
    patched := replace(definition,
        'SELECT receipt.outcome, submission.provider_account_id,',
        $guard$SELECT CASE WHEN contract_version = 4
                 AND NEW.fact_domain = 'customer_billable'
                 AND NEW.attempt_execution_id IS NULL
                 AND receipt.outcome = 'uncertain'
                 AND EXISTS (
                     SELECT 1 FROM dreamina_operator_image_success_resolutions resolution
                     WHERE resolution.resolution_id::TEXT = NEW.metadata_json ->> 'operator_resolution_id'
                       AND resolution.job_id = NEW.job_id
                       AND resolution.submission_id = NEW.submission_id
                       AND resolution.original_receipt_id = NEW.receipt_id
                       AND resolution.output_id = NEW.output_id
                       AND NEW.provider_id = 'dreamina-cli'
                 ) THEN 'succeeded' ELSE receipt.outcome END, submission.provider_account_id,$guard$);
    IF patched = definition THEN RAISE EXCEPTION 'expected durable usage authority guard'; END IF;
    EXECUTE patched;

    definition := pg_get_functiondef('protect_completed_terminal_artifact()'::REGPROCEDURE);
    patched := regexp_replace(definition, E'\\mBEGIN\\M', $guard$BEGIN
    IF EXISTS (SELECT 1 FROM dreamina_operator_image_success_resolutions WHERE artifact_id = OLD.artifact_id) THEN
        RAISE EXCEPTION 'operator-resolved Dreamina image artifact is immutable';
    END IF;$guard$);
    IF patched = definition THEN RAISE EXCEPTION 'expected completed artifact guard'; END IF;
    EXECUTE patched;
END;
$migration$;

-- Subsequent facts or mutations must continue to satisfy the complete overlay,
-- even when they do not touch an existing parent-projection trigger.
CREATE FUNCTION validate_existing_operator_dreamina_image_success() RETURNS TRIGGER AS $$
DECLARE
    row_data JSONB;
    affected_rows JSONB[];
    target_job_id UUID;
BEGIN
    IF TG_OP = 'INSERT' THEN affected_rows := ARRAY[to_jsonb(NEW)];
    ELSIF TG_OP = 'DELETE' THEN affected_rows := ARRAY[to_jsonb(OLD)];
    ELSE affected_rows := ARRAY[to_jsonb(OLD), to_jsonb(NEW)]; END IF;
    FOREACH row_data IN ARRAY affected_rows LOOP
        target_job_id := COALESCE(row_data ->> 'job_id', row_data ->> 'source_job_id')::UUID;
        IF target_job_id IS NULL AND row_data ? 'submission_id' THEN
            SELECT job_id INTO target_job_id FROM provider_submissions
            WHERE submission_id = (row_data ->> 'submission_id')::UUID;
        END IF;
        IF target_job_id IS NULL AND row_data ? 'work_item_id' THEN
            SELECT job_id INTO target_job_id FROM work_items
            WHERE work_item_id = (row_data ->> 'work_item_id')::UUID;
        END IF;
        IF target_job_id IS NULL AND row_data ? 'hold_id' THEN
            SELECT job_id INTO target_job_id FROM customer_billing_holds
            WHERE hold_id = (row_data ->> 'hold_id')::UUID;
        END IF;
        IF target_job_id IS NULL AND row_data ? 'transaction_id' THEN
            SELECT source_job_id INTO target_job_id FROM ledger_transactions
            WHERE transaction_id = (row_data ->> 'transaction_id')::UUID;
        END IF;
        IF target_job_id IS NULL AND row_data ? 'rated_usage_line_id' THEN
            SELECT job_id INTO target_job_id FROM customer_rated_usage_lines
            WHERE rated_usage_line_id = (row_data ->> 'rated_usage_line_id')::UUID;
        END IF;
        IF target_job_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM dreamina_operator_image_success_resolutions WHERE job_id = target_job_id
        ) THEN
            PERFORM assert_operator_dreamina_image_success(target_job_id);
        END IF;
        IF TG_TABLE_NAME = 'ledger_accounts' THEN
            FOR target_job_id IN
                SELECT DISTINCT transaction.source_job_id
                FROM ledger_postings posting
                JOIN ledger_transactions transaction USING (transaction_id)
                JOIN dreamina_operator_image_success_resolutions resolution
                  ON resolution.job_id = transaction.source_job_id
                WHERE posting.account_id = (row_data ->> 'account_id')::UUID
            LOOP
                PERFORM assert_operator_dreamina_image_success(target_job_id);
            END LOOP;
        END IF;
    END LOOP;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- The original canonical JSON serialization cannot be reconstructed from jsonb
-- for a byte-identical command hash. Preserve the reviewed payload itself.
CREATE FUNCTION protect_operator_dreamina_image_success_payload() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM dreamina_operator_image_success_resolutions WHERE job_id = OLD.job_id) THEN
        RAISE EXCEPTION 'operator-resolved Dreamina image payload is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER operator_dreamina_image_success_payload_immutable
    BEFORE UPDATE OR DELETE ON job_payloads
    FOR EACH ROW EXECUTE FUNCTION protect_operator_dreamina_image_success_payload();

DO $migration$
DECLARE target TEXT;
BEGIN
    FOREACH target IN ARRAY ARRAY[
        'job_outputs', 'provider_usage_facts', 'usage_events', 'metering_events',
        'customer_billing_holds', 'customer_rated_usage', 'customer_rated_usage_lines',
        'ledger_transactions', 'provider_remote_submit_intents', 'provider_remote_tasks',
        'provider_submit_recoveries', 'executor_capacity_allocations', 'accepted_completion_budgets',
        'jobs', 'work_items', 'job_attempts', 'idempotency_requests', 'quota_reservations',
        'job_response_projections', 'job_payloads', 'customer_price_quotes',
        'customer_price_quote_lines', 'provider_submissions', 'executor_executions',
        'executor_resolution_decisions', 'executor_terminal_reductions', 'provider_receipts',
        'provider_task_observations', 'provider_submit_quarantined_receipts',
        'provider_remote_video_results', 'executor_result_manifests', 'executor_artifact_authorities',
        'dreamina_native_batch_members', 'dreamina_operator_rejection_resolutions',
        'job_events', 'outbox_events', 'artifacts', 'customer_billing_hold_grant_reservations',
        'customer_rated_usage_fact_links', 'ledger_postings', 'ledger_transaction_seals', 'ledger_accounts'
    ] LOOP
        EXECUTE format('CREATE CONSTRAINT TRIGGER operator_dreamina_image_success_check '
            || 'AFTER INSERT OR UPDATE OR DELETE ON %I DEFERRABLE INITIALLY DEFERRED '
            || 'FOR EACH ROW EXECUTE FUNCTION validate_existing_operator_dreamina_image_success()', target);
    END LOOP;
END;
$migration$;
