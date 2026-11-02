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
    let row: Option<(Uuid, Uuid, String, Uuid, String, String, String, String)> = sqlx::query_as(
        r#"
        SELECT decision.decision_id, job.job_id, job.request_id,
               hold.hold_id, hold.currency, hold.held_micros::TEXT,
               hold.released_micros::TEXT, hold.captured_micros::TEXT
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
        JOIN provider_remote_tasks task USING (submission_id, executor_execution_id)
        JOIN provider_remote_submit_intents intent USING (submission_id, executor_execution_id)
        JOIN provider_task_observations observation
          ON observation.observation_id = task.state_observation_id
         AND observation.submission_id = task.submission_id
         AND observation.executor_execution_id = task.executor_execution_id
        JOIN executor_resolution_decisions decision
          ON decision.decision_id = submission.resolution_decision_id
         AND decision.submission_id = submission.submission_id
         AND decision.executor_execution_id = submission.executor_execution_id
         AND decision.provider_task_observation_id = observation.observation_id
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
          AND job.last_error_code IN ('dreamina_generation_failed', 'dreamina_copyright_rejected')
          AND payload.command_schema = 'dreamina-cli.submit.v1'
          AND submission.command_schema = payload.command_schema
          AND submission.operation_id = 'images.generations'
          AND submission.operation_binding_version = 2 AND submission.completion_mode = 'remote_task'
          AND submission.state = 'failed' AND execution.state = 'failed' AND output.state = 'failed'
          AND output.output_index = 0 AND output.error_code = job.last_error_code
          AND (SELECT count(*) FROM provider_submissions WHERE job_id = job.job_id) = 1
          AND task.state = 'failed' AND task.provider_id = submission.provider_id
          AND task.provider_account_id = submission.provider_account_id
          AND task.poll_owner IS NULL AND task.poll_lease_expires_at_ms IS NULL
          AND task.next_poll_at_ms IS NULL AND task.deadline_quarantine_id IS NULL
          AND intent.state = 'attached' AND intent.remote_operation_id = task.remote_operation_id
          AND intent.provider_id = task.provider_id AND intent.provider_account_id = task.provider_account_id
          AND observation.source = 'poll' AND observation.observed_state = 'failed'
          AND observation.effect_certainty = 'not_applicable' AND observation.error_code = job.last_error_code
          AND decision.source = 'remote_provider_observation' AND decision.resolved_state = 'failed'
          AND decision.error_code = observation.error_code AND decision.result_manifest_id IS NULL
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
        "#,
    )
    .bind(job_id).bind(tenant_id).bind(project_id).bind(actor_user_id).bind(DREAMINA_IMAGES_API_PROFILE)
    .fetch_optional(&store.pool).await.map_err(settlement_unavailable)?;
    Ok(row.map(
        |(audit, job, request, hold, currency, held, released, captured)| {
            DreaminaReleasedSettlement {
                basis: Some("native_image_terminal_reduction_v1".into()),
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
