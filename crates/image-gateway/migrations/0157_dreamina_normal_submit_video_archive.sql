-- A reviewed original archive is an operator authority, never an invented
-- submit receipt. Original execution evidence remains deadline-quarantined.
ALTER TABLE dreamina_operator_video_success_resolutions
    ADD COLUMN evidence_mode TEXT NOT NULL DEFAULT 'legacy_poll_v1',
    ADD COLUMN archive_artifact_id UUID,
    ADD COLUMN archive_sha256 TEXT,
    ADD COLUMN archive_byte_size BIGINT,
    ADD COLUMN provider_command_sha256 TEXT,
    ADD CONSTRAINT dreamina_video_success_evidence_mode CHECK (
        (evidence_mode = 'legacy_poll_v1' AND archive_artifact_id IS NULL
         AND archive_sha256 IS NULL AND archive_byte_size IS NULL
         AND provider_command_sha256 IS NULL)
        OR
        (evidence_mode = 'normal_submit_archive_v1' AND archive_artifact_id IS NOT NULL
         AND archive_sha256 IS NOT NULL AND archive_sha256 ~ '^[a-f0-9]{64}$'
         AND archive_byte_size IS NOT NULL AND archive_byte_size > 0
         AND provider_command_sha256 IS NOT NULL AND provider_command_sha256 ~ '^[a-f0-9]{64}$')
    ),
    ADD CONSTRAINT dreamina_video_success_archive_fk FOREIGN KEY (archive_artifact_id)
        REFERENCES artifacts(artifact_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX dreamina_video_success_native_lookup
    ON dreamina_operator_video_success_resolutions(remote_operation_id);

CREATE OR REPLACE FUNCTION assert_operator_dreamina_normal_video_archive_evidence(
    resolution dreamina_operator_video_success_resolutions
) RETURNS VOID AS $$
BEGIN
    IF resolution.evidence_mode <> 'normal_submit_archive_v1' OR NOT EXISTS (
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
          AND submission.operation_id = 'videos.generations'
          AND submission.completion_mode = 'remote_task'
          AND job.operation = 'video_generation' AND job.output_count = 1
          AND job.requested_units = 10 AND job.billable_units = 10
          AND job.economics_contract_version = 4
          AND payload.command_schema = submission.command_schema
          AND payload.command_json ->> 'operation' = 'multimodal2video'
          AND payload.command_json -> 'schema_version' = '1'::JSONB
          AND payload.command_json -> 'duration' = '10'::JSONB
          AND payload.command_json ->> 'model_version' = job.model
          AND payload.command_json ->> 'video_resolution' = '720p'
          AND submission.command_hash = resolution.command_hash
          AND output.output_index = 0 AND output.billable_units = 10
          AND quota.requested_units = 10
          AND submission.state = 'uncertain' AND execution.state = 'uncertain'
          AND submission.error_code = 'provider_submit_deadline'
          AND execution.error_code = 'provider_submit_deadline'
          AND submission.result_manifest_id IS NULL AND submission.remote_result_id IS NULL
          AND execution.executor_owner IS NULL AND execution.lease_expires_at_ms IS NULL
          AND decision.source = 'remote_submit_deadline'
          AND decision.provider_submit_intent_id = submission.submission_id
          AND decision.provider_task_observation_id IS NULL
          AND decision.observation_id IS NULL
          AND decision.resolved_state = 'uncertain' AND decision.error_code = 'provider_submit_deadline'
          AND decision.result_manifest_id IS NULL AND decision.remote_result_id IS NULL
          AND reduction.state = 'completed' AND reduction.resolved_state = 'uncertain'
          AND reduction.customer_artifact_id IS NULL AND reduction.remote_result_id IS NULL
          AND reduction.quota_reservation_id = quota.reservation_id
          AND receipt.outcome = 'uncertain' AND receipt.receipt_schema = 'executor.resolution.v1'
          AND receipt.submission_id = submission.submission_id AND receipt.job_id = job.job_id
          AND receipt.output_id = output.output_id AND receipt.provider_id = job.provider_id
          AND receipt.evidence ->> 'submission_id' = submission.submission_id::TEXT
          AND receipt.evidence ->> 'executor_execution_id' = execution.executor_execution_id::TEXT
          AND receipt.evidence ->> 'resolved_state' = 'uncertain'
          AND receipt.evidence ->> 'resolution_decision_id' = decision.decision_id::TEXT
          AND receipt.evidence ->> 'error_code' = 'provider_submit_deadline'
          AND intent.state = 'deadline_quarantined' AND intent.submit_lease_epoch = 1
          AND intent.provider_id = submission.provider_id
          AND intent.provider_account_id = submission.provider_account_id
          AND intent.provider_command_sha256 = resolution.provider_command_sha256
          AND intent.execution_binding_sha256 = resolution.execution_binding_sha256
          AND intent.remote_operation_id IS NULL AND intent.provider_request_id IS NULL
          AND intent.receipt_event_identity IS NULL
          AND intent.failure_error_code = 'dreamina_submit_receipt_invalid'
          AND recovery.state = 'closed' AND recovery.invocation_attempt = 1
          AND recovery.provider_timeout_ms = intent.provider_timeout_ms
          AND recovery.recovery_owner IS NULL AND recovery.recovery_lease_expires_at_ms IS NULL
          AND recovery.recovery_claimed_at_ms IS NULL
          AND resolution.created_at_ms >= recovery.provider_deadline_at_ms
          AND (
              (allocation.state = 'held' AND allocation.release_decision_id IS NULL
               AND allocation.release_reconciliation_id IS NULL AND allocation.release_reason IS NULL
               AND allocation.released_state IS NULL
               AND EXISTS (SELECT 1 FROM provider_capacity_reconciliations capacity
                   WHERE capacity.submission_id = submission.submission_id
                     AND capacity.executor_execution_id = execution.executor_execution_id
                     AND capacity.provider_id = submission.provider_id
                     AND capacity.provider_account_id = submission.provider_account_id
                     AND capacity.state = 'active' AND capacity.evidence_revision = 0))
              OR
              (allocation.state = 'released' AND allocation.released_state = 'uncertain'
               AND allocation.release_reason = 'provider_capacity_reconciliation'
               AND allocation.release_decision_id = decision.decision_id
               AND EXISTS (SELECT 1 FROM provider_capacity_reconciliations capacity
                   WHERE capacity.reconciliation_id = allocation.release_reconciliation_id
                     AND capacity.submission_id = submission.submission_id
                     AND capacity.executor_execution_id = execution.executor_execution_id
                     AND capacity.provider_id = submission.provider_id
                     AND capacity.provider_account_id = submission.provider_account_id
                     AND capacity.state = 'released' AND capacity.evidence_revision = 0
                     AND capacity.evidence_kind = 'remote_terminal'
                     AND capacity.remote_terminal_state = 'succeeded'
                     AND capacity.remote_operation_id = resolution.remote_operation_id
                     AND capacity.event_identity = 'operator:' || resolution.resolution_id::TEXT))
          )
          AND work.kind = 'video_single'
          AND work.lease_owner IS NULL AND work.lease_expires_at_ms IS NULL
          AND quote.quote_hash = resolution.quote_hash
          AND quote.operation = job.operation AND quote.media_kind = 'video'
          AND quote.execution_surface = 'provider_cli'
          AND quote.provider_id = 'dreamina-cli'
          AND quote.provider_model_id = payload.command_json ->> 'model_version'
          AND quote.request_dimensions_json = jsonb_build_object(
              'duration', '10', 'resolution', payload.command_json ->> 'video_resolution',
              'ratio', payload.command_json ->> 'ratio',
              'processing_mode', 'synchronous',
              'workflow', 'multimodal2video',
              'input_image_count', jsonb_array_length(payload.command_json -> 'images')::TEXT,
              'input_video_count', jsonb_array_length(payload.command_json -> 'videos')::TEXT,
              'input_audio_count', jsonb_array_length(payload.command_json -> 'audios')::TEXT)
          AND hold.held_micros = quote.max_total_micros
          AND resolution.created_at_ms >= receipt.created_at_ms
    ) OR (SELECT COUNT(*) FROM provider_submissions WHERE job_id = resolution.job_id) <> 1
      OR (SELECT COUNT(*) FROM work_items WHERE job_id = resolution.job_id) <> 1
      OR (SELECT COUNT(*) FROM job_outputs WHERE job_id = resolution.job_id) <> 1
      OR (SELECT COUNT(*) FROM provider_receipts WHERE job_id = resolution.job_id) <> 1
      OR (SELECT COUNT(*) FROM job_attempts attempt JOIN work_items work USING (work_item_id)
          WHERE work.job_id = resolution.job_id) <> 1
      OR (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = resolution.job_id
          AND terminal_outcome = 'succeeded') <> 1
      OR NOT EXISTS (SELECT 1 FROM customer_price_quote_lines WHERE job_id = resolution.job_id
          AND terminal_outcome = 'succeeded' AND metric = 'video_requested_second' AND unit = 'second'
          AND max_quantity = 10 AND quantity_source = 'request_derived'
          AND required_confidence = 'exact')
      OR EXISTS (SELECT 1 FROM provider_remote_tasks WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM provider_task_observations WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM accepted_completion_budgets WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM executor_runner_observations WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE job_id = resolution.job_id)
      OR EXISTS (SELECT 1 FROM dreamina_operator_video_supplement_resolutions WHERE job_id = resolution.job_id)
      OR EXISTS (SELECT 1 FROM dreamina_manual_video_cancellations WHERE job_id = resolution.job_id)
      OR EXISTS (SELECT 1 FROM dreamina_operator_video_supplement_cancellations WHERE job_id = resolution.job_id)
      OR EXISTS (SELECT 1 FROM executor_result_manifests WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM executor_artifact_authorities WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM provider_submit_quarantined_receipts WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM provider_remote_video_results WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM dreamina_native_batch_members WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM dreamina_operator_image_success_resolutions WHERE job_id = resolution.job_id)
      OR EXISTS (SELECT 1 FROM dreamina_operator_rejection_resolutions WHERE job_id = resolution.job_id)
    THEN
        RAISE EXCEPTION 'normal video archive requires the original unaccepted deadline quarantine and untouched execution evidence';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION assert_operator_dreamina_normal_video_archive_publication(
    resolution dreamina_operator_video_success_resolutions
) RETURNS VOID AS $$
BEGIN
    IF resolution.evidence_mode <> 'normal_submit_archive_v1'
       OR (SELECT COUNT(*) FROM artifacts WHERE job_id = resolution.job_id) <> 1
       OR NOT EXISTS (
           SELECT 1 FROM artifacts artifact
           JOIN jobs job ON job.job_id = artifact.job_id AND job.tenant_id = artifact.tenant_id
           JOIN provider_submissions submission ON submission.submission_id = resolution.submission_id
             AND submission.job_id = job.job_id AND submission.work_item_id = artifact.work_item_id
             AND submission.created_by_execution_id = artifact.execution_id
             AND submission.created_by_lease_epoch = artifact.lease_epoch
           WHERE artifact.artifact_id = resolution.archive_artifact_id
             AND artifact.job_id = resolution.job_id AND artifact.output_index = 0
             AND artifact.state = 'ready' AND artifact.media_type = 'video/mp4'
             AND artifact.sha256_hex = resolution.archive_sha256
             AND artifact.byte_size = resolution.archive_byte_size
             AND artifact.storage_backend IN ('filesystem-v1', 'memory-v1')
       ) THEN
        RAISE EXCEPTION 'normal video archive must publish exactly the reviewed original bytes';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Serialize all modes by original account/native identity, including old-mode
-- inserts. Existing per-job/submission/output/hold uniqueness remains intact.
CREATE FUNCTION guard_operator_dreamina_video_native_identity(
    resolution dreamina_operator_video_success_resolutions
) RETURNS VOID AS $$
DECLARE account_id UUID;
BEGIN
    SELECT provider_account_id INTO STRICT account_id FROM provider_submissions
    WHERE submission_id = resolution.submission_id AND job_id = resolution.job_id;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'dreamina-video-success:' || account_id::TEXT || ':' || resolution.remote_operation_id, 0));
    IF EXISTS (
        SELECT 1 FROM dreamina_operator_video_success_resolutions other
        JOIN provider_submissions submission ON submission.submission_id = other.submission_id
        WHERE other.remote_operation_id = resolution.remote_operation_id
          AND submission.provider_account_id = account_id
          AND other.resolution_id <> resolution.resolution_id
    ) OR (resolution.evidence_mode = 'normal_submit_archive_v1' AND (
        EXISTS (SELECT 1 FROM provider_remote_tasks task
            WHERE task.provider_id = 'dreamina-cli' AND task.provider_account_id = account_id
              AND task.remote_operation_id = resolution.remote_operation_id)
        OR EXISTS (SELECT 1 FROM dreamina_operator_video_supplement_authorizations supplement
            WHERE supplement.provider_account_id = account_id
              AND supplement.remote_operation_id = resolution.remote_operation_id)
        OR EXISTS (SELECT 1 FROM provider_remote_submit_intents intent
            JOIN provider_submissions submission USING (submission_id, executor_execution_id)
            WHERE intent.provider_id = 'dreamina-cli' AND intent.provider_account_id = account_id
              AND submission.operation_id = 'videos.generations'
              AND submission.completion_mode = 'remote_task'
              AND intent.remote_operation_id = resolution.remote_operation_id)
    )) THEN
        RAISE EXCEPTION 'native video is already bound to another delivery authority';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Retain the legacy function bodies and all common financial guards. Only the
-- explicit new mode selects the original-deadline and local-archive predicates.
DO $migration$
DECLARE definition TEXT; patched TEXT; old TEXT; replacement TEXT;
BEGIN
    definition := pg_get_functiondef('assert_operator_dreamina_video_success_evidence(dreamina_operator_video_success_resolutions)'::REGPROCEDURE);
    IF strpos(definition, 'JOIN provider_remote_tasks task') = 0
       OR strpos(definition, 'dreamina_poll_protocol_invalid') = 0 THEN
        RAISE EXCEPTION 'expected unchanged legacy video evidence assertion';
    END IF;
    patched := regexp_replace(definition, E'\\mBEGIN\\M', $body$BEGIN
    IF resolution.evidence_mode = 'normal_submit_archive_v1' THEN
        PERFORM assert_operator_dreamina_normal_video_archive_evidence(resolution);
        RETURN;
    END IF;$body$);
    IF patched = definition THEN RAISE EXCEPTION 'missing legacy evidence entry point'; END IF;
    EXECUTE patched;

    definition := pg_get_functiondef('validate_operator_dreamina_video_success_insert()'::REGPROCEDURE);
    patched := regexp_replace(definition, E'\\mBEGIN\\M', $body$BEGIN
    PERFORM guard_operator_dreamina_video_native_identity(NEW);$body$);
    old := '''dreamina_poll_protocol_invalid''';
    replacement := '(CASE NEW.evidence_mode WHEN ''normal_submit_archive_v1'' THEN ''provider_submit_deadline'' ELSE ''dreamina_poll_protocol_invalid'' END)';
    IF (length(patched) - length(replace(patched, old, ''))) / length(old) <> 2 THEN
        RAISE EXCEPTION 'expected two legacy untouched-state error checks';
    END IF;
    patched := replace(patched, old, replacement);
    EXECUTE patched;

    definition := pg_get_functiondef('assert_operator_dreamina_video_success(uuid)'::REGPROCEDURE);
    old := 'PERFORM assert_operator_dreamina_video_success_evidence(resolution);';
    replacement := $body$PERFORM assert_operator_dreamina_video_success_evidence(resolution);
    IF resolution.evidence_mode = 'normal_submit_archive_v1' THEN
        PERFORM assert_operator_dreamina_normal_video_archive_publication(resolution);
        IF NOT EXISTS (
            SELECT 1 FROM executor_capacity_allocations allocation
            JOIN provider_capacity_reconciliations capacity
              ON capacity.reconciliation_id = allocation.release_reconciliation_id
             AND capacity.submission_id = allocation.submission_id
             AND capacity.executor_execution_id = allocation.executor_execution_id
            WHERE allocation.submission_id = resolution.submission_id
              AND allocation.executor_execution_id = resolution.executor_execution_id
              AND allocation.state = 'released' AND allocation.released_state = 'uncertain'
              AND allocation.release_reason = 'provider_capacity_reconciliation'
              AND capacity.state = 'released' AND capacity.evidence_kind = 'remote_terminal'
              AND capacity.remote_terminal_state = 'succeeded'
              AND capacity.remote_operation_id = resolution.remote_operation_id
              AND capacity.event_identity = 'operator:' || resolution.resolution_id::TEXT
        ) THEN
            RAISE EXCEPTION 'normal video archive requires fenced terminal capacity closure';
        END IF;
    END IF;$body$;
    patched := replace(definition, old, replacement);
    IF patched = definition THEN RAISE EXCEPTION 'missing common video evidence assertion'; END IF;
    old := 'projection.artifact_count = 0 AND projection.remote_result_count = 1';
    replacement := $body$((resolution.evidence_mode = 'legacy_poll_v1' AND projection.artifact_count = 0 AND projection.remote_result_count = 1)
            OR (resolution.evidence_mode = 'normal_submit_archive_v1' AND projection.artifact_count = 1 AND projection.remote_result_count = 0))$body$;
    IF strpos(patched, old) = 0 THEN RAISE EXCEPTION 'missing legacy URL projection'; END IF;
    patched := replace(patched, old, replacement);
    old := '''dreamina_poll_protocol_invalid''';
    replacement := '(CASE resolution.evidence_mode WHEN ''normal_submit_archive_v1'' THEN ''provider_submit_deadline'' ELSE ''dreamina_poll_protocol_invalid'' END)';
    IF (length(patched) - length(replace(patched, old, ''))) / length(old) <> 2 THEN
        RAISE EXCEPTION 'expected two original terminal error events';
    END IF;
    patched := replace(patched, old, replacement);
    old := 'OR EXISTS (SELECT 1 FROM artifacts WHERE job_id = target_job_id)';
    replacement := 'OR (resolution.evidence_mode = ''legacy_poll_v1'' AND EXISTS (SELECT 1 FROM artifacts WHERE job_id = target_job_id))';
    IF strpos(patched, old) = 0 THEN RAISE EXCEPTION 'missing legacy no-artifact assertion'; END IF;
    patched := replace(patched, old, replacement);
    EXECUTE patched;
END;
$migration$;

-- A positive reviewed query plus the exact published archive may close capacity
-- under the existing lease fence. It does not create a submit receipt or task.
DO $migration$
DECLARE definition TEXT; patched TEXT; old TEXT; replacement TEXT;
BEGIN
    definition := pg_get_functiondef('enforce_provider_capacity_reconciliation_update()'::REGPROCEDURE);
    old := $body$                  AND intent.receipt_event_identity IS NOT NULL
           ) THEN$body$;
    replacement := $body$                  AND intent.receipt_event_identity IS NOT NULL
           ) AND NOT EXISTS (
                SELECT 1 FROM dreamina_operator_video_success_resolutions resolution
                JOIN provider_submissions submission ON submission.submission_id = resolution.submission_id
                  AND submission.executor_execution_id = resolution.executor_execution_id
                JOIN provider_remote_submit_intents intent ON intent.submission_id = submission.submission_id
                  AND intent.executor_execution_id = submission.executor_execution_id
                JOIN artifacts artifact ON artifact.artifact_id = resolution.archive_artifact_id
                  AND artifact.job_id = resolution.job_id AND artifact.state = 'ready'
                  AND artifact.media_type = 'video/mp4' AND artifact.sha256_hex = resolution.archive_sha256
                  AND artifact.byte_size = resolution.archive_byte_size
                WHERE resolution.evidence_mode = 'normal_submit_archive_v1'
                  AND resolution.submission_id = NEW.submission_id
                  AND resolution.executor_execution_id = NEW.executor_execution_id
                  AND submission.provider_id = NEW.provider_id
                  AND submission.provider_account_id = NEW.provider_account_id
                  AND submission.command_hash = resolution.command_hash
                  AND intent.state = 'deadline_quarantined' AND intent.remote_operation_id IS NULL
                  AND intent.receipt_event_identity IS NULL
                  AND intent.execution_binding_sha256 = resolution.execution_binding_sha256
                  AND intent.provider_command_sha256 = resolution.provider_command_sha256
                  AND NEW.remote_operation_id = resolution.remote_operation_id
                  AND NEW.remote_terminal_state = 'succeeded'
                  AND NEW.event_identity = 'operator:' || resolution.resolution_id::TEXT
                  AND NEW.reconciliation_owner = 'factoryctl.operator-normal-archive'
                  AND NEW.last_command_id = resolution.resolution_id::TEXT
                  AND NEW.evidence_revision = 0 AND NEW.claimed_evidence_revision = 0
           ) THEN$body$;
    patched := replace(definition, old, replacement);
    IF patched = definition THEN RAISE EXCEPTION 'missing durable-receipt capacity evidence gate'; END IF;
    EXECUTE patched;
END;
$migration$;
CREATE CONSTRAINT TRIGGER operator_dreamina_video_success_capacity_check
    AFTER INSERT OR UPDATE OR DELETE ON provider_capacity_reconciliations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    EXECUTE FUNCTION validate_existing_operator_dreamina_video_success();

-- These authority writers must see the winner after waiting for the shared
-- native-identity lock. Older transaction snapshots are refused only for this
-- Dreamina video boundary; other providers and image operations are untouched.
CREATE FUNCTION fence_normal_video_archive_native_claim() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.remote_operation_id IS NULL THEN RETURN NEW; END IF;
    IF TG_TABLE_NAME IN ('provider_remote_tasks', 'provider_remote_submit_intents') AND NOT EXISTS (
        SELECT 1 FROM provider_submissions submission
        WHERE submission.submission_id = NEW.submission_id
          AND submission.executor_execution_id = NEW.executor_execution_id
          AND submission.provider_id = 'dreamina-cli'
          AND submission.operation_id = 'videos.generations'
          AND submission.completion_mode = 'remote_task'
    ) THEN RETURN NEW; END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'Dreamina video native authority requires READ COMMITTED';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'dreamina-video-success:' || NEW.provider_account_id::TEXT || ':' || NEW.remote_operation_id, 0));
    IF EXISTS (
        SELECT 1 FROM dreamina_operator_video_success_resolutions resolution
        JOIN provider_submissions submission ON submission.submission_id = resolution.submission_id
        WHERE resolution.evidence_mode = 'normal_submit_archive_v1'
          AND resolution.remote_operation_id = NEW.remote_operation_id
          AND submission.provider_account_id = NEW.provider_account_id
    ) THEN
        RAISE EXCEPTION 'native video is already delivered by its original archive authority';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER normal_video_archive_native_task_fence
    BEFORE INSERT OR UPDATE OF remote_operation_id, provider_account_id, submission_id, executor_execution_id
    ON provider_remote_tasks
    FOR EACH ROW EXECUTE FUNCTION fence_normal_video_archive_native_claim();
CREATE TRIGGER normal_video_archive_native_intent_fence
    BEFORE INSERT OR UPDATE OF remote_operation_id, provider_account_id, submission_id, executor_execution_id
    ON provider_remote_submit_intents
    FOR EACH ROW EXECUTE FUNCTION fence_normal_video_archive_native_claim();
CREATE TRIGGER normal_video_archive_native_supplement_fence
    BEFORE INSERT OR UPDATE OF remote_operation_id, provider_account_id, submission_id, executor_execution_id
    ON dreamina_operator_video_supplement_authorizations
    FOR EACH ROW EXECUTE FUNCTION fence_normal_video_archive_native_claim();

DO $migration$
DECLARE definition TEXT; patched TEXT;
BEGIN
    definition := pg_get_functiondef('guard_operator_dreamina_video_native_identity(dreamina_operator_video_success_resolutions)'::REGPROCEDURE);
    patched := regexp_replace(definition, E'\\mBEGIN\\M', $body$BEGIN
    IF resolution.evidence_mode = 'normal_submit_archive_v1'
       AND current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'normal video archive authority requires READ COMMITTED';
    END IF;$body$);
    IF patched = definition THEN RAISE EXCEPTION 'missing native identity guard entry'; END IF;
    EXECUTE patched;
END;
$migration$;
