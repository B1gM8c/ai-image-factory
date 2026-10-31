-- Preserve original accepted/deadline evidence. Only a reviewed same-ID terminal
-- copyright failure can release this single-video reservation and capacity.
CREATE TABLE dreamina_accepted_terminal_resolutions (
    resolution_id UUID PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE RESTRICT,
    submission_id UUID NOT NULL UNIQUE,
    executor_execution_id UUID NOT NULL UNIQUE,
    hold_id UUID NOT NULL UNIQUE REFERENCES customer_billing_holds(hold_id) ON DELETE RESTRICT,
    reason TEXT NOT NULL CHECK (reason = 'dreamina_copyright_rejected'),
    review_sha256 TEXT NOT NULL CHECK(review_sha256 ~ '^[a-f0-9]{64}$'),
    reviewed_stdout_sha256 TEXT NOT NULL CHECK (reviewed_stdout_sha256 ~ '^[a-f0-9]{64}$'),
    execution_binding_sha256 TEXT NOT NULL CHECK (execution_binding_sha256 ~ '^[a-f0-9]{64}$'),
    provider_account_id UUID NOT NULL,
    remote_operation_id TEXT NOT NULL,
    quarantine_id UUID NOT NULL REFERENCES provider_remote_task_quarantines(quarantine_id),
    command_hash TEXT NOT NULL CHECK(command_hash ~ '^[a-f0-9]{64}$'),
    provider_command_sha256 TEXT NOT NULL CHECK(provider_command_sha256 ~ '^[a-f0-9]{64}$'),
    request_hash TEXT NOT NULL CHECK(request_hash ~ '^[a-f0-9]{64}$'),
    quote_hash TEXT NOT NULL CHECK(quote_hash ~ '^[a-f0-9]{64}$'),
    query_audit_sha256 TEXT NOT NULL CHECK(query_audit_sha256 ~ '^[a-f0-9]{64}$'),
    process_request_sha256 TEXT NOT NULL CHECK(process_request_sha256 ~ '^[a-f0-9]{64}$'),
    process_terminal_sha256 TEXT NOT NULL CHECK(process_terminal_sha256 ~ '^[a-f0-9]{64}$'),
    prompt_sha256 TEXT NOT NULL CHECK(prompt_sha256 ~ '^[a-f0-9]{64}$'),
    queried_at_ms BIGINT NOT NULL CHECK(queried_at_ms > 0),
    provider_deadline_at_ms BIGINT NOT NULL CHECK (provider_deadline_at_ms > 0),
    released_units INTEGER NOT NULL CHECK (released_units > 0),
    released_micros BIGINT NOT NULL CHECK (released_micros >= 0),
    operator_reference TEXT NOT NULL CHECK (
        char_length(operator_reference) BETWEEN 1 AND 255
        AND operator_reference !~ '[[:cntrl:]]'
    ),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms >= queried_at_ms AND queried_at_ms >= provider_deadline_at_ms),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES provider_submissions(executor_execution_id, submission_id) ON DELETE RESTRICT
);

CREATE FUNCTION protect_dreamina_accepted_terminal_resolution() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Dreamina accepted terminal resolutions are append-only';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_accepted_terminal_immutable
    BEFORE UPDATE OR DELETE ON dreamina_accepted_terminal_resolutions
    FOR EACH ROW EXECUTE FUNCTION protect_dreamina_accepted_terminal_resolution();

CREATE TRIGGER dreamina_accepted_terminal_reject_truncate
    BEFORE TRUNCATE ON dreamina_accepted_terminal_resolutions
    FOR EACH STATEMENT EXECUTE FUNCTION protect_dreamina_accepted_terminal_resolution();

CREATE FUNCTION assert_dreamina_accepted_terminal(target_job_id UUID) RETURNS VOID AS $$
DECLARE
    resolution dreamina_accepted_terminal_resolutions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT resolution FROM dreamina_accepted_terminal_resolutions
    WHERE job_id = target_job_id;
    IF NOT EXISTS (
        SELECT 1
        FROM jobs job
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
        JOIN provider_remote_tasks task ON task.submission_id = submission.submission_id
          AND task.executor_execution_id = execution.executor_execution_id
        JOIN provider_remote_task_quarantines quarantine ON quarantine.quarantine_id = task.deadline_quarantine_id
          AND quarantine.submission_id = submission.submission_id
        JOIN accepted_completion_budgets budget ON budget.submission_id = submission.submission_id
          AND budget.executor_execution_id = execution.executor_execution_id
        JOIN job_payloads payload ON payload.job_id = job.job_id
        JOIN executor_capacity_allocations allocation
          ON allocation.submission_id = submission.submission_id
         AND allocation.executor_execution_id = execution.executor_execution_id
        JOIN work_items work ON work.work_item_id = submission.work_item_id AND work.job_id = job.job_id
        JOIN job_attempts attempt
          ON attempt.work_item_id = work.work_item_id
         AND attempt.execution_id = work.execution_id
         AND attempt.lease_epoch = work.lease_epoch
        JOIN job_outputs output ON output.output_id = submission.output_id AND output.job_id = job.job_id
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
          AND quota.job_id = job.job_id AND quota.tenant_id = job.tenant_id
        JOIN customer_billing_holds hold ON hold.hold_id = resolution.hold_id
          AND hold.job_id = job.job_id AND hold.tenant_id = job.tenant_id
        JOIN customer_price_quotes quote ON quote.quote_id = hold.quote_id AND quote.job_id = job.job_id
        WHERE job.job_id = target_job_id
          AND submission.submission_id = resolution.submission_id
          AND execution.executor_execution_id = resolution.executor_execution_id
          AND job.provider_id = 'dreamina-cli' AND submission.provider_id = job.provider_id
          AND job.operation = 'video_generation' AND job.output_count = 1
          AND job.economics_contract_version = 4 AND job.charged_units = 0
          AND submission.state = 'uncertain' AND execution.state = 'uncertain'
          AND submission.error_code = 'provider_remote_task_deadline'
          AND execution.error_code = 'provider_remote_task_deadline'
          AND submission.result_manifest_id IS NULL
          AND execution.executor_owner IS NULL AND execution.lease_expires_at_ms IS NULL
          AND decision.source = 'remote_task_deadline'
          AND decision.resolved_state = 'uncertain' AND decision.error_code = 'provider_remote_task_deadline'
          AND reduction.state = 'completed' AND reduction.resolved_state = 'uncertain'
          AND reduction.customer_artifact_id IS NULL
          AND receipt.outcome = 'uncertain' AND receipt.receipt_schema = 'executor.resolution.v1'
          AND receipt.submission_id = submission.submission_id AND receipt.job_id = job.job_id
          AND receipt.provider_cost_micros IS NULL AND receipt.provider_cost_currency IS NULL
          AND receipt.output_id = output.output_id AND receipt.provider_id = job.provider_id
          AND receipt.evidence ->> 'resolved_state' = 'uncertain'
          AND receipt.evidence ->> 'resolution_decision_id' = decision.decision_id::TEXT
          AND reduction.quota_reservation_id = quota.reservation_id
          AND (SELECT COUNT(*) FROM job_events WHERE job_id = target_job_id
               AND semantic_key = 'work.' || work.work_item_id::TEXT || '.executor-terminal') = 1
          AND (SELECT COUNT(*) FROM outbox_events WHERE job_id = target_job_id
               AND semantic_key = 'work.' || work.work_item_id::TEXT || '.executor-terminal') = 1
          AND submission.provider_account_id = resolution.provider_account_id
          AND submission.command_hash = resolution.command_hash
          AND payload.request_hash = resolution.request_hash AND quote.quote_hash = resolution.quote_hash
          AND intent.state = 'attached' AND intent.submit_lease_epoch = 1
          AND intent.remote_operation_id = resolution.remote_operation_id
          AND intent.receipt_event_identity IS NOT NULL
          AND intent.execution_binding_sha256 = resolution.execution_binding_sha256
          AND intent.provider_command_sha256 = resolution.provider_command_sha256
          AND intent.provider_account_id = resolution.provider_account_id
          AND task.state = 'uncertain' AND task.error_code = 'provider_remote_task_deadline'
          AND task.effect_certainty = 'unknown_remote_effect' AND task.artifact_ref IS NULL
          AND task.remote_operation_id = resolution.remote_operation_id
          AND task.provider_account_id = resolution.provider_account_id
          AND task.poll_owner IS NULL AND task.poll_lease_expires_at_ms IS NULL AND task.next_poll_at_ms IS NULL
          AND NOT task.cancel_requested
          AND quarantine.quarantine_id = resolution.quarantine_id
          AND quarantine.executor_execution_id = execution.executor_execution_id
          AND quarantine.remote_operation_id = resolution.remote_operation_id
          AND quarantine.provider_account_id = resolution.provider_account_id
          AND quarantine.error_code = 'provider_remote_task_deadline'
          AND quarantine.quarantined_at_ms <= resolution.queried_at_ms
          AND decision.provider_remote_task_quarantine_id = resolution.quarantine_id
          AND budget.remote_operation_id = resolution.remote_operation_id AND budget.policy_version = 1
          AND budget.completion_deadline_at_ms = resolution.provider_deadline_at_ms
          AND budget.original_submit_deadline_at_ms = quarantine.provider_deadline_at_ms
          AND recovery.state = 'closed' AND recovery.invocation_attempt = 1
          AND recovery.recovery_owner IS NULL AND recovery.recovery_lease_expires_at_ms IS NULL
          AND allocation.state = 'released' AND allocation.released_state = 'uncertain'
          AND allocation.release_reason = 'operator_accepted_terminal'
          AND allocation.release_decision_id = execution.executor_execution_id
          AND allocation.release_reconciliation_id IS NULL
          AND job.state = 'failed' AND job.last_error_code = resolution.reason
          AND work.kind = 'video_single' AND work.state = 'failed'
          AND work.lease_owner IS NULL AND work.lease_expires_at_ms IS NULL
          AND attempt.state = 'failed' AND attempt.error_code = resolution.reason
          AND output.output_index = 0 AND output.state = 'failed' AND output.error_code = resolution.reason
          AND quota.state = 'released' AND quota.committed_units = 0
          AND quota.requested_units = job.billable_units AND quota.released_units = quota.requested_units
          AND quota.released_units = resolution.released_units
          AND hold.state = 'released' AND hold.captured_micros = 0
          AND hold.released_micros = hold.held_micros AND hold.released_micros = resolution.released_micros
          AND hold.grant_captured_micros = 0 AND hold.account_captured_micros = 0
          AND hold.grant_released_micros = hold.grant_held_micros
          AND hold.account_released_micros = hold.account_held_micros
    ) OR (SELECT COUNT(*) FROM provider_submissions WHERE job_id = target_job_id) <> 1
      OR (SELECT COUNT(*) FROM job_outputs WHERE job_id = target_job_id) <> 1
      OR (SELECT COUNT(*) FROM provider_receipts WHERE job_id = target_job_id) <> 1
      OR (SELECT COUNT(*) FROM metering_events WHERE job_id = target_job_id
          AND outcome = resolution.reason AND event_type IN ('quota_released', 'job_failed')
          AND units = resolution.released_units) <> 2
      OR (SELECT COUNT(*) FROM job_attempts attempt JOIN work_items work USING (work_item_id)
          WHERE work.job_id = target_job_id) <> 1
      OR NOT EXISTS (SELECT 1 FROM idempotency_requests WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM idempotency_requests WHERE job_id = target_job_id
                 AND (state <> 'failed' OR terminal_outcome IS DISTINCT FROM 'failed'))
      OR EXISTS (SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM dreamina_manual_video_cancellations WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM dreamina_operator_video_success_resolutions WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM dreamina_operator_rejection_resolutions WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM artifacts WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM job_response_projections WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM provider_usage_facts WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM usage_events WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM customer_rated_usage WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM ledger_transactions WHERE source_job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM provider_submit_quarantined_receipts WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM executor_result_manifests WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM dreamina_native_batch_members WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM customer_billing_hold_grant_reservations WHERE hold_id = resolution.hold_id
                 AND (state <> 'released' OR consumed_micros <> 0 OR released_micros <> reserved_micros))
      OR (SELECT COUNT(*) FROM job_events WHERE job_id = target_job_id
          AND semantic_key = 'job.operator_dreamina_accepted_terminal'
          AND event_type = 'job.failed' AND payload_json ->> 'resolution_id' = resolution.resolution_id::TEXT) <> 1
      OR (SELECT COUNT(*) FROM outbox_events WHERE job_id = target_job_id
          AND semantic_key = 'job.operator_dreamina_accepted_terminal'
          AND event_type = 'job.failed' AND payload_json ->> 'resolution_id' = resolution.resolution_id::TEXT) <> 1
    THEN
        RAISE EXCEPTION 'Dreamina accepted terminal requires canonical refusal evidence and full zero-charge release';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_dreamina_accepted_terminal() RETURNS TRIGGER AS $$
BEGIN
    PERFORM assert_dreamina_accepted_terminal(NEW.job_id);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER dreamina_accepted_terminal_consistency
    AFTER INSERT ON dreamina_accepted_terminal_resolutions DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_dreamina_accepted_terminal();

-- Existing jobs take their existing guards unchanged. Only a fully validated
-- operator resolution has a different effective parent and quota projection.
DO $migration$
DECLARE target REGPROCEDURE; definition TEXT; patched TEXT;
BEGIN
    FOREACH target IN ARRAY ARRAY[
        'assert_terminal_parent_completion(uuid)'::REGPROCEDURE,
        'assert_terminal_quota_accounting(uuid)'::REGPROCEDURE
    ] LOOP
        definition := pg_get_functiondef(target);
        patched := regexp_replace(definition, E'\\mBEGIN\\M', $guard$BEGIN
    IF EXISTS (SELECT 1 FROM dreamina_accepted_terminal_resolutions WHERE job_id = target_job_id) THEN
        PERFORM assert_dreamina_accepted_terminal(target_job_id);
        RETURN;
    END IF;$guard$);
        IF patched = definition THEN RAISE EXCEPTION 'expected terminal accounting entry point %', target; END IF;
        EXECUTE patched;
    END LOOP;
END;
$migration$;


ALTER TABLE executor_capacity_allocations DROP CONSTRAINT executor_capacity_allocations_release_reason_check;
ALTER TABLE executor_capacity_allocations ADD CONSTRAINT executor_capacity_allocations_release_reason_check CHECK (
    release_reason IS NULL OR release_reason IN ('terminal_evidence','executor_start_abandoned',
    'remote_provider_observation','remote_submit_outcome','provider_capacity_reconciliation','operator_accepted_terminal'));

DO $migration$
DECLARE definition TEXT; patched TEXT; old TEXT;
BEGIN
    definition := pg_get_functiondef('enforce_executor_capacity_allocation_transition()'::REGPROCEDURE);
    old := 'IF NEW.release_reason = ''terminal_evidence'' THEN';
    patched := replace(definition, old, $body$IF NEW.release_reason = 'operator_accepted_terminal' THEN
            IF decision_source IS DISTINCT FROM 'remote_task_deadline'
               OR NEW.released_state IS DISTINCT FROM 'uncertain'
               OR NEW.release_reconciliation_id IS NOT NULL
               OR NOT EXISTS (SELECT 1 FROM dreamina_accepted_terminal_resolutions r
                    WHERE r.submission_id=NEW.submission_id AND r.executor_execution_id=NEW.executor_execution_id
                      AND r.executor_execution_id=NEW.release_decision_id) THEN
                RAISE EXCEPTION 'accepted terminal capacity requires exact immutable resolution';
            END IF;
        ELSIF NEW.release_reason = 'terminal_evidence' THEN$body$);
    IF patched=definition THEN RAISE EXCEPTION 'missing capacity transition anchor'; END IF;
    EXECUTE patched;
    definition := pg_get_functiondef('enforce_provider_submit_deadline_capacity_hold()'::REGPROCEDURE);
    old := 'IF decision_source = ''remote_task_deadline'' THEN';
    patched := replace(definition, old, $body$IF decision_source = 'remote_task_deadline' AND NOT (
            NEW.release_reason = 'operator_accepted_terminal' AND EXISTS (
                SELECT 1 FROM dreamina_accepted_terminal_resolutions r
                WHERE r.submission_id=NEW.submission_id AND r.executor_execution_id=NEW.executor_execution_id
                  AND r.executor_execution_id=NEW.release_decision_id)) THEN$body$);
    IF patched=definition THEN RAISE EXCEPTION 'missing accepted deadline hold anchor'; END IF;
    EXECUTE patched;
    definition := pg_get_functiondef('enforce_provider_remote_task_quarantine_projection()'::REGPROCEDURE);
    patched := regexp_replace(definition, E'\\mBEGIN\\M', $body$BEGIN
    IF EXISTS (SELECT 1 FROM dreamina_accepted_terminal_resolutions WHERE submission_id=target_submission) THEN
        PERFORM assert_dreamina_accepted_terminal((SELECT job_id FROM dreamina_accepted_terminal_resolutions WHERE submission_id=target_submission));
        RETURN NULL;
    END IF;$body$);
    IF patched=definition THEN RAISE EXCEPTION 'missing quarantine projection anchor'; END IF;
    EXECUTE patched;
END;
$migration$;

CREATE FUNCTION validate_existing_dreamina_accepted_terminal() RETURNS TRIGGER AS $$
DECLARE target_job UUID;
BEGIN
    SELECT job_id INTO target_job FROM dreamina_accepted_terminal_resolutions
    WHERE submission_id=COALESCE(NEW.submission_id,OLD.submission_id);
    IF FOUND THEN PERFORM assert_dreamina_accepted_terminal(target_job); END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
DO $migration$
DECLARE target TEXT;
BEGIN
    FOREACH target IN ARRAY ARRAY['provider_remote_tasks','provider_remote_task_quarantines',
       'provider_remote_submit_intents','provider_submissions','executor_capacity_allocations'] LOOP
        EXECUTE format('CREATE CONSTRAINT TRIGGER accepted_terminal_resolution_projection_check AFTER INSERT OR UPDATE OR DELETE ON %I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_existing_dreamina_accepted_terminal()', target);
    END LOOP;
END;
$migration$;
