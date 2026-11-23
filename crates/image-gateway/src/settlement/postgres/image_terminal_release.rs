use image_api_contracts::dreamina::{DREAMINA_IMAGES_API_PROFILE, DreaminaReleasedSettlement};
use uuid::Uuid;

use super::{PostgresExecutionSettlementStore, settlement_unavailable};
use crate::ImageGatewayError;

// Read evidence only. A failed status or zero charged units alone never proves release.
pub(super) async fn project(
    store: &PostgresExecutionSettlementStore,
    tenant_id: &str,
    project_id: &str,
    actor_user_id: Option<Uuid>,
    job_id: Uuid,
) -> Result<Option<DreaminaReleasedSettlement>, ImageGatewayError> {
    let row: Option<(Uuid, Uuid, String, Uuid, String, String, String, String, String)> =
        sqlx::query_as(
        r#"
        SELECT decision.decision_id, job.job_id, job.request_id,
               hold.hold_id, hold.currency, hold.held_micros::TEXT,
               hold.released_micros::TEXT, hold.captured_micros::TEXT, job.last_error_code
        FROM jobs job
        JOIN job_auth_attributions auth ON auth.job_id = job.job_id AND auth.tenant_id = job.tenant_id
        JOIN job_payloads payload ON payload.job_id = job.job_id
        JOIN admission_sessions admission ON admission.session_id = payload.admission_session_id
          AND admission.job_id = job.job_id AND admission.tenant_id = job.tenant_id
          AND admission.project_id = auth.project_id AND admission.operation = job.operation
        JOIN provider_submissions submission ON submission.job_id = job.job_id
          AND submission.tenant_id = job.tenant_id AND submission.provider_id = job.provider_id
        JOIN job_outputs output ON output.output_id = submission.output_id AND output.job_id = job.job_id
        JOIN executor_executions execution USING (submission_id, executor_execution_id)
        JOIN provider_remote_submit_intents intent USING (submission_id, executor_execution_id)
        LEFT JOIN provider_remote_tasks task
          ON task.submission_id = submission.submission_id
         AND task.executor_execution_id = submission.executor_execution_id
        LEFT JOIN provider_task_observations observation
          ON observation.observation_id = task.state_observation_id
         AND observation.submission_id = task.submission_id
         AND observation.executor_execution_id = task.executor_execution_id
        JOIN executor_resolution_decisions decision
          ON decision.decision_id = submission.resolution_decision_id
         AND decision.submission_id = submission.submission_id
         AND decision.executor_execution_id = submission.executor_execution_id
        JOIN executor_terminal_reductions reduction
          ON reduction.submission_id = submission.submission_id
         AND reduction.executor_execution_id = submission.executor_execution_id
         AND reduction.resolution_decision_id = decision.decision_id
        JOIN customer_billing_holds hold ON hold.job_id = job.job_id AND hold.tenant_id = job.tenant_id
        JOIN customer_price_quotes quote ON quote.quote_id = hold.quote_id AND quote.job_id = job.job_id
          AND quote.tenant_id = job.tenant_id AND quote.project_id = auth.project_id
          AND quote.currency = hold.currency AND quote.operation = job.operation
        JOIN customer_rated_usage rated ON rated.quote_id = hold.quote_id AND rated.job_id = job.job_id
          AND rated.currency = hold.currency
        JOIN quota_reservations quota ON quota.job_id = job.job_id AND quota.tenant_id = job.tenant_id
          AND quota.request_id = job.request_id
        WHERE job.job_id = $1 AND job.tenant_id = $2 AND auth.project_id = $3
          AND ($4::UUID IS NULL OR auth.actor_user_id = $4)
          AND admission.api_profile = $5 AND quote.api_profile = $5
          AND job.provider_id = 'dreamina-cli' AND job.operation IN ('generation', 'edit')
          AND job.economics_contract_version = 4 AND job.output_count = 1
          AND job.state = 'failed' AND job.charged_units = 0
          AND payload.command_schema = 'dreamina-cli.submit.v1'
          AND submission.command_schema = payload.command_schema
          AND submission.operation_id = 'images.generations'
          AND submission.operation_binding_version = 2 AND submission.completion_mode = 'remote_task'
          AND submission.state = 'failed' AND execution.state = 'failed' AND output.state = 'failed'
          AND output.output_index = 0 AND output.error_code = job.last_error_code
          AND (SELECT count(*) FROM provider_submissions WHERE job_id = job.job_id) = 1
          AND decision.resolved_state = 'failed'
          AND decision.error_code = job.last_error_code AND decision.result_manifest_id IS NULL
          AND (
            (
              job.last_error_code IN ('dreamina_generation_failed', 'dreamina_copyright_rejected')
              AND task.state = 'failed' AND task.provider_id = submission.provider_id
              AND task.provider_account_id = submission.provider_account_id
              AND task.poll_owner IS NULL AND task.poll_lease_expires_at_ms IS NULL
              AND task.next_poll_at_ms IS NULL AND task.deadline_quarantine_id IS NULL
              AND intent.state = 'attached' AND intent.remote_operation_id = task.remote_operation_id
              AND intent.provider_id = task.provider_id AND intent.provider_account_id = task.provider_account_id
              AND observation.source = 'poll' AND observation.observed_state = 'failed'
              AND observation.effect_certainty = 'not_applicable' AND observation.error_code = job.last_error_code
              AND decision.source = 'remote_provider_observation'
              AND decision.provider_task_observation_id = observation.observation_id
            ) OR (
              job.last_error_code = 'dreamina_material_upload_failed'
              AND payload.command_json ->> 'operation' = 'image2image'
              AND intent.state = 'rejected' AND intent.failure_error_code = job.last_error_code
              AND intent.failure_event_identity IS NOT NULL
              AND intent.provider_id = submission.provider_id
              AND intent.provider_account_id = submission.provider_account_id
              AND intent.remote_operation_id IS NULL AND intent.provider_request_id IS NULL
              AND intent.receipt_event_identity IS NULL AND submission.result_manifest_id IS NULL
              AND decision.source = 'remote_submit_outcome'
              AND decision.provider_submit_intent_id = intent.submission_id
              AND decision.provider_task_observation_id IS NULL
              AND quota.reservation_id = job.reservation_id
              AND NOT EXISTS (SELECT 1 FROM provider_remote_tasks accepted
                  WHERE accepted.submission_id = submission.submission_id)
              AND NOT EXISTS (SELECT 1 FROM provider_submit_quarantined_receipts receipt
                  WHERE receipt.submission_id = submission.submission_id)
            )
          )
          AND reduction.state = 'completed' AND reduction.resolved_state = 'failed'
          AND hold.state = 'settled' AND hold.captured_micros = 0
          AND hold.released_micros = hold.held_micros
          AND hold.grant_captured_micros = 0 AND hold.account_captured_micros = 0
          AND hold.grant_released_micros = hold.grant_held_micros
          AND hold.account_released_micros = hold.account_held_micros
          AND rated.total_amount_micros = 0
          AND quota.state = 'released' AND quota.committed_units = 0 AND quota.started_units = 0
          AND quota.released_units = quota.requested_units AND quota.requested_units = 1
          AND NOT EXISTS (SELECT 1 FROM ledger_transactions ledger WHERE ledger.source_job_id = job.job_id
              AND ledger.transaction_type IN ('customer_charge', 'customer_job_charge'))
          AND NOT EXISTS (SELECT 1 FROM customer_rated_usage_lines line
              WHERE line.rated_usage_id = rated.rated_usage_id AND line.amount_micros <> 0)
          AND NOT EXISTS (SELECT 1 FROM artifacts WHERE job_id = job.job_id)
          AND NOT EXISTS (SELECT 1 FROM executor_result_manifests WHERE submission_id = submission.submission_id)
          AND NOT EXISTS (SELECT 1 FROM dreamina_image_result_sets WHERE submission_id = submission.submission_id)
          AND NOT EXISTS (SELECT 1 FROM dreamina_native_batches WHERE job_id = job.job_id)
          AND NOT EXISTS (SELECT 1 FROM provider_remote_task_quarantines WHERE submission_id = submission.submission_id)
          AND NOT EXISTS (SELECT 1 FROM executor_capacity_risk_waivers WHERE submission_id = submission.submission_id)
          AND NOT EXISTS (SELECT 1 FROM dreamina_operator_image_supplement_authorizations WHERE job_id = job.job_id)
          AND NOT EXISTS (SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE job_id = job.job_id)

        UNION ALL

        SELECT resolution.resolution_id, job.job_id, job.request_id,
               hold.hold_id, hold.currency, hold.held_micros::TEXT,
               hold.released_micros::TEXT, hold.captured_micros::TEXT, resolution.reason
        FROM dreamina_operator_rejection_resolutions resolution
        JOIN jobs job ON job.job_id = resolution.job_id
        JOIN job_auth_attributions auth ON auth.job_id = job.job_id AND auth.tenant_id = job.tenant_id
        JOIN customer_billing_holds hold
          ON hold.hold_id = resolution.hold_id AND hold.job_id = job.job_id AND hold.tenant_id = job.tenant_id
        JOIN customer_price_quotes quote ON quote.quote_id = hold.quote_id AND quote.job_id = job.job_id
          AND quote.tenant_id = job.tenant_id AND quote.project_id = auth.project_id
          AND quote.currency = hold.currency AND quote.operation = job.operation
        JOIN quota_reservations quota
          ON quota.reservation_id = job.reservation_id AND quota.job_id = job.job_id
         AND quota.tenant_id = job.tenant_id AND quota.request_id = job.request_id
        JOIN provider_remote_submit_intents intent
          ON intent.submission_id = resolution.submission_id
         AND intent.executor_execution_id = resolution.executor_execution_id
        WHERE job.job_id = $1 AND job.tenant_id = $2 AND auth.project_id = $3
          AND ($4::UUID IS NULL OR auth.actor_user_id = $4) AND quote.api_profile = $5
          AND resolution.reason = 'dreamina_material_upload_failed'
          AND job.state = 'failed' AND job.last_error_code = resolution.reason AND job.charged_units = 0
          AND is_operator_dreamina_image_upload_rejection(job.job_id, resolution.submission_id)
          AND intent.state = 'deadline_quarantined' AND intent.failure_event_identity IS NOT NULL
          AND intent.execution_binding_sha256 = resolution.execution_binding_sha256
          AND intent.remote_operation_id IS NULL AND intent.provider_request_id IS NULL
          AND intent.receipt_event_identity IS NULL
          AND hold.state = 'released' AND hold.captured_micros = 0
          AND hold.released_micros = hold.held_micros AND hold.released_micros = resolution.released_micros
          AND hold.grant_captured_micros = 0 AND hold.account_captured_micros = 0
          AND hold.grant_released_micros = hold.grant_held_micros
          AND hold.account_released_micros = hold.account_held_micros
          AND quota.state = 'released' AND quota.committed_units = 0 AND quota.started_units = 0
          AND quota.released_units = quota.requested_units AND quota.released_units = resolution.released_units
          AND NOT EXISTS (SELECT 1 FROM provider_remote_tasks WHERE submission_id = resolution.submission_id)
          AND NOT EXISTS (SELECT 1 FROM provider_submit_quarantined_receipts WHERE submission_id = resolution.submission_id)
          AND NOT EXISTS (SELECT 1 FROM executor_result_manifests WHERE submission_id = resolution.submission_id)
          AND NOT EXISTS (SELECT 1 FROM artifacts WHERE job_id = job.job_id)
          AND NOT EXISTS (SELECT 1 FROM customer_rated_usage WHERE job_id = job.job_id)
          AND NOT EXISTS (SELECT 1 FROM ledger_transactions WHERE source_job_id = job.job_id)
        "#,
    )
    .bind(job_id).bind(tenant_id).bind(project_id).bind(actor_user_id).bind(DREAMINA_IMAGES_API_PROFILE)
    .fetch_optional(&store.pool).await.map_err(settlement_unavailable)?;
    Ok(row.map(
        |(audit, job, request, hold, currency, held, released, captured, error_code)| {
            let basis = if error_code == "dreamina_material_upload_failed" {
                "dreamina_image_material_upload_rejection_v1"
            } else {
                "native_image_terminal_reduction_v1"
            };
            DreaminaReleasedSettlement {
                basis: Some(basis.into()),
                state: "released".into(),
                resolution_audit_id: audit.to_string(),
                job_id: job.to_string(),
                request_id: request,
                hold_id: hold.to_string(),
                currency,
                held_micros: held,
                released_micros: released,
                captured_micros: captured,
            }
        },
    ))
}
