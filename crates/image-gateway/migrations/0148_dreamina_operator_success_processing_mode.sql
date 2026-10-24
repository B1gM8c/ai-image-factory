-- Normal synchronous admission freezes processing_mode alongside command-derived
-- pricing dimensions. Match that complete shape without changing original quotes.
-- Replace only the evidence validators; retain their identities and privileges,
-- all original execution/settlement fences, and every existing trigger.
CREATE OR REPLACE FUNCTION assert_operator_dreamina_image_success_evidence(
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
          AND quote.request_dimensions_json = (
              jsonb_build_object(
                  'resolution_type', payload.command_json ->> 'resolution_type',
                  'processing_mode', 'synchronous')
              || CASE WHEN payload.command_json ->> 'ratio' IS NOT NULL
                  THEN jsonb_build_object('ratio', payload.command_json ->> 'ratio')
                  ELSE jsonb_build_object(
                      'width', payload.command_json ->> 'width',
                      'height', payload.command_json ->> 'height')
                 END
              || CASE WHEN payload.command_json ->> 'resolution_type' = '1.5k'
                  THEN jsonb_build_object('workflow', 'text2image')
                  ELSE '{}'::JSONB
                 END
          )
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

CREATE OR REPLACE FUNCTION assert_operator_dreamina_video_success_evidence(
    resolution dreamina_operator_video_success_resolutions
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
        JOIN accepted_completion_budgets budget
          ON budget.submission_id = submission.submission_id
         AND budget.executor_execution_id = submission.executor_execution_id
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
          AND payload.command_json ->> 'operation' = 'text2video'
          AND payload.command_json -> 'schema_version' = '1'::JSONB
          AND payload.command_json -> 'duration' = '10'::JSONB
          AND payload.command_json ->> 'model_version' = 'seedance2.0fast'
          AND payload.command_json ->> 'video_resolution' = '720p'
          AND submission.command_hash = resolution.command_hash
          AND output.output_index = 0 AND output.billable_units = 10
          AND quota.requested_units = 10
          AND submission.state = 'uncertain' AND execution.state = 'uncertain'
          AND submission.error_code = 'dreamina_poll_protocol_invalid'
          AND execution.error_code = 'dreamina_poll_protocol_invalid'
          AND submission.result_manifest_id IS NULL AND submission.remote_result_id IS NULL
          AND execution.executor_owner IS NULL AND execution.lease_expires_at_ms IS NULL
          AND decision.source = 'remote_provider_observation'
          AND decision.resolved_state = 'uncertain' AND decision.error_code = 'dreamina_poll_protocol_invalid'
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
          AND receipt.evidence ->> 'error_code' = 'dreamina_poll_protocol_invalid'
          AND intent.state = 'attached' AND intent.submit_lease_epoch = 1
          AND intent.remote_operation_id = resolution.remote_operation_id
          AND intent.execution_binding_sha256 = resolution.execution_binding_sha256
          AND intent.receipt_event_identity IS NOT NULL
          AND recovery.state = 'closed' AND recovery.invocation_attempt = 1
          AND recovery.provider_timeout_ms = 600000
          AND recovery.provider_deadline_at_ms = task.provider_deadline_at_ms
          AND recovery.recovery_owner IS NULL AND recovery.recovery_lease_expires_at_ms IS NULL
          AND recovery.recovery_claimed_at_ms IS NULL
          AND task.state = 'uncertain' AND task.error_code = 'dreamina_poll_protocol_invalid'
          AND task.provider_id = job.provider_id
          AND task.provider_account_id = submission.provider_account_id
          AND budget.remote_operation_id = resolution.remote_operation_id
          AND budget.original_submit_deadline_at_ms = task.provider_deadline_at_ms
          AND budget.accepted_at_ms = task.created_at_ms
          AND budget.completion_deadline_at_ms = task.created_at_ms + 86400000
          AND budget.policy_version = 1
          AND task.remote_operation_id = resolution.remote_operation_id
          AND task.artifact_ref IS NULL AND task.next_poll_at_ms IS NULL
          AND task.effect_certainty = 'unknown_remote_effect'
          AND task.poll_owner IS NULL AND task.poll_lease_expires_at_ms IS NULL
          AND task.poll_claimed_at_ms IS NULL AND task.deadline_quarantine_id IS NULL
          AND observation.source = 'poll' AND observation.observed_state = 'uncertain'
          AND observation.error_code = 'dreamina_poll_protocol_invalid'
          AND observation.artifact_ref IS NULL AND observation.result_manifest_id IS NULL
          AND observation.remote_result_id IS NULL
          AND allocation.state = 'released' AND allocation.released_state = 'uncertain'
          AND allocation.release_reason = 'remote_provider_observation'
          AND allocation.release_decision_id = decision.decision_id
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
              'processing_mode', 'synchronous')
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
      OR EXISTS (SELECT 1 FROM executor_result_manifests WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM executor_artifact_authorities WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM provider_submit_quarantined_receipts WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM provider_remote_video_results WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM dreamina_native_batch_members WHERE submission_id = resolution.submission_id)
      OR EXISTS (SELECT 1 FROM dreamina_operator_image_success_resolutions WHERE job_id = resolution.job_id)
      OR EXISTS (SELECT 1 FROM dreamina_operator_rejection_resolutions WHERE job_id = resolution.job_id)
    THEN
        RAISE EXCEPTION 'Dreamina operator video success requires one original uncertain single-video execution';
    END IF;
END;
$$ LANGUAGE plpgsql;
