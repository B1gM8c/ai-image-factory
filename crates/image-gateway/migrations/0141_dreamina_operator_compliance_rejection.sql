-- Preserve the original immutable uncertain decision. This separately audited,
-- single-output overlay resolves only a proven pre-acceptance compliance refusal.
CREATE TABLE dreamina_operator_rejection_resolutions (
    resolution_id UUID PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE RESTRICT,
    submission_id UUID NOT NULL UNIQUE,
    executor_execution_id UUID NOT NULL UNIQUE,
    hold_id UUID NOT NULL UNIQUE REFERENCES customer_billing_holds(hold_id) ON DELETE RESTRICT,
    reason TEXT NOT NULL CHECK (reason = 'dreamina_compliance_confirmation_required'),
    reviewed_stdout_sha256 TEXT NOT NULL CHECK (reviewed_stdout_sha256 ~ '^[a-f0-9]{64}$'),
    execution_binding_sha256 TEXT NOT NULL CHECK (execution_binding_sha256 ~ '^[a-f0-9]{64}$'),
    launch_nonce UUID NOT NULL CHECK (launch_nonce <> '00000000-0000-0000-0000-000000000000'),
    provider_deadline_at_ms BIGINT NOT NULL CHECK (provider_deadline_at_ms > 0),
    released_units INTEGER NOT NULL CHECK (released_units > 0),
    released_micros BIGINT NOT NULL CHECK (released_micros >= 0),
    operator_reference TEXT NOT NULL CHECK (
        char_length(operator_reference) BETWEEN 1 AND 255
        AND operator_reference !~ '[[:cntrl:]]'
    ),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms >= provider_deadline_at_ms),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES provider_submissions(executor_execution_id, submission_id) ON DELETE RESTRICT
);

CREATE FUNCTION protect_dreamina_operator_rejection_resolution() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Dreamina operator rejection resolutions are append-only';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_operator_rejection_immutable
    BEFORE UPDATE OR DELETE ON dreamina_operator_rejection_resolutions
    FOR EACH ROW EXECUTE FUNCTION protect_dreamina_operator_rejection_resolution();

CREATE TRIGGER dreamina_operator_rejection_reject_truncate
    BEFORE TRUNCATE ON dreamina_operator_rejection_resolutions
    FOR EACH STATEMENT EXECUTE FUNCTION protect_dreamina_operator_rejection_resolution();

CREATE FUNCTION assert_operator_dreamina_rejection(target_job_id UUID) RETURNS VOID AS $$
DECLARE
    resolution dreamina_operator_rejection_resolutions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT resolution FROM dreamina_operator_rejection_resolutions
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
        WHERE job.job_id = target_job_id
          AND submission.submission_id = resolution.submission_id
          AND execution.executor_execution_id = resolution.executor_execution_id
          AND job.provider_id = 'dreamina-cli' AND submission.provider_id = job.provider_id
          AND job.operation = 'video_generation' AND job.output_count = 1
          AND job.economics_contract_version = 4 AND job.charged_units = 0
          AND submission.state = 'uncertain' AND execution.state = 'uncertain'
          AND submission.error_code = 'provider_submit_deadline'
          AND execution.error_code = 'provider_submit_deadline'
          AND submission.result_manifest_id IS NULL
          AND execution.executor_owner IS NULL AND execution.lease_expires_at_ms IS NULL
          AND decision.source = 'remote_submit_deadline'
          AND decision.resolved_state = 'uncertain' AND decision.error_code = 'provider_submit_deadline'
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
          AND intent.state = 'deadline_quarantined' AND intent.submit_lease_epoch = 1
          AND intent.remote_operation_id IS NULL AND intent.provider_request_id IS NULL
          AND intent.receipt_event_identity IS NULL
          AND intent.failure_error_code = 'dreamina_submit_rejected'
          AND intent.execution_binding_sha256 = resolution.execution_binding_sha256
          AND recovery.state = 'closed' AND recovery.invocation_attempt = 1
          AND recovery.recovery_owner IS NULL AND recovery.recovery_lease_expires_at_ms IS NULL
          AND recovery.provider_deadline_at_ms = resolution.provider_deadline_at_ms
          AND allocation.state = 'released' AND allocation.released_state = 'uncertain'
          AND allocation.release_reason = 'provider_capacity_reconciliation'
          AND EXISTS (SELECT 1 FROM provider_capacity_reconciliations capacity
              WHERE capacity.reconciliation_id = allocation.release_reconciliation_id
                AND capacity.submission_id = submission.submission_id
                AND capacity.state = 'released' AND capacity.evidence_kind = 'confirmed_no_effect'
                AND capacity.event_identity = 'operator:' || resolution.resolution_id::TEXT)
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
      OR EXISTS (SELECT 1 FROM artifacts WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM job_response_projections WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM provider_usage_facts WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM usage_events WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM customer_rated_usage WHERE job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM ledger_transactions WHERE source_job_id = target_job_id)
      OR EXISTS (SELECT 1 FROM provider_remote_tasks WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM provider_submit_quarantined_receipts WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM executor_result_manifests WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM dreamina_native_batch_members WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM customer_billing_hold_grant_reservations WHERE hold_id = resolution.hold_id
                 AND (state <> 'released' OR consumed_micros <> 0 OR released_micros <> reserved_micros))
      OR (SELECT COUNT(*) FROM job_events WHERE job_id = target_job_id
          AND semantic_key = 'job.operator_dreamina_compliance_rejection'
          AND event_type = 'job.failed' AND payload_json ->> 'resolution_id' = resolution.resolution_id::TEXT) <> 1
      OR (SELECT COUNT(*) FROM outbox_events WHERE job_id = target_job_id
          AND semantic_key = 'job.operator_dreamina_compliance_rejection'
          AND event_type = 'job.failed' AND payload_json ->> 'resolution_id' = resolution.resolution_id::TEXT) <> 1
    THEN
        RAISE EXCEPTION 'Dreamina operator rejection requires canonical refusal evidence and full zero-charge release';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_operator_dreamina_rejection() RETURNS TRIGGER AS $$
BEGIN
    PERFORM assert_operator_dreamina_rejection(NEW.job_id);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER dreamina_operator_rejection_consistency
    AFTER INSERT ON dreamina_operator_rejection_resolutions DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_operator_dreamina_rejection();

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
    IF EXISTS (SELECT 1 FROM dreamina_operator_rejection_resolutions WHERE job_id = target_job_id) THEN
        PERFORM assert_operator_dreamina_rejection(target_job_id);
        RETURN;
    END IF;$guard$);
        IF patched = definition THEN RAISE EXCEPTION 'expected terminal accounting entry point %', target; END IF;
        EXECUTE patched;
    END LOOP;
END;
$migration$;

-- A late accepted provider receipt must not silently contradict a settled
-- operator refusal. The existing canonical evidence stays immutable.
CREATE FUNCTION validate_operator_dreamina_intent_update() RETURNS TRIGGER AS $$
DECLARE target_job_id UUID;
BEGIN
    SELECT job_id INTO target_job_id FROM dreamina_operator_rejection_resolutions
    WHERE submission_id = NEW.submission_id;
    IF FOUND THEN PERFORM assert_operator_dreamina_rejection(target_job_id); END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER dreamina_operator_rejection_late_receipt_check
    AFTER UPDATE ON provider_remote_submit_intents DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_operator_dreamina_intent_update();
