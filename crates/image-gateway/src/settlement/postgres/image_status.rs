use image_api_contracts::dreamina::{DREAMINA_IMAGES_API_PROFILE, DreaminaImageRequestMetadata};
use image_provider_dreamina_cli::DREAMINA_SUBMIT_COMMAND_SCHEMA;
use sqlx::FromRow;
use uuid::Uuid;

use super::{PostgresExecutionSettlementStore, settlement_unavailable, video_provider_progress};
use crate::{
    ImageGatewayError,
    settlement::{
        ExecutionSettlementStore, GenerationResultLookup, ImageResultStatus, ImageTaskProjection,
        VideoPendingStage,
    },
};

#[derive(FromRow)]
struct ImageStatusRow {
    job_state: String,
    work_state: String,
    error_code: Option<String>,
    model: String,
    request_id: String,
    output_count: i32,
    model_version: Option<String>,
    workflow: Option<String>,
    resolution_type: Option<String>,
    ratio: Option<String>,
    width: Option<i64>,
    height: Option<i64>,
    progress_json: Option<serde_json::Value>,
    observed_at_ms: Option<i64>,
}

// The only pending read is a scoped target row and its indexed latest valid observation.
// Neither the frozen command (which may contain input references) nor historical observations
// leave the database. Artifacts are hydrated only for this target's successful result.
pub(super) async fn project(
    store: &PostgresExecutionSettlementStore,
    tenant_id: &str,
    project_id: &str,
    actor_user_id: Option<Uuid>,
    job_id: Uuid,
) -> Result<Option<ImageTaskProjection>, ImageGatewayError> {
    let row: Option<ImageStatusRow> = sqlx::query_as(
        r#"
        SELECT j.state AS job_state, w.state AS work_state, j.last_error_code AS error_code,
               j.model, j.request_id, j.output_count,
               payload.command_json ->> 'model_version' AS model_version,
               payload.command_json ->> 'operation' AS workflow,
               payload.command_json ->> 'resolution_type' AS resolution_type,
               payload.command_json ->> 'ratio' AS ratio,
               (payload.command_json ->> 'width')::BIGINT AS width,
               (payload.command_json ->> 'height')::BIGINT AS height,
               observation.progress_json, observation.observed_at_ms
        FROM jobs j
        JOIN work_items w ON w.job_id = j.job_id
        JOIN job_auth_attributions attribution ON attribution.job_id = j.job_id
        JOIN job_payloads payload ON payload.job_id = j.job_id AND payload.command_schema = $5
        JOIN admission_sessions session ON session.session_id = payload.admission_session_id
          AND session.job_id = j.job_id AND session.tenant_id = j.tenant_id
          AND session.project_id = attribution.project_id AND session.api_profile = $6
          AND session.operation = j.operation
        LEFT JOIN job_outputs output ON output.job_id = j.job_id AND output.output_index = 0
        LEFT JOIN provider_submissions submission
          ON submission.output_id = output.output_id AND submission.job_id = j.job_id
         AND submission.tenant_id = j.tenant_id AND submission.work_item_id = w.work_item_id
        LEFT JOIN provider_remote_tasks task
          ON task.submission_id = submission.submission_id
         AND task.executor_execution_id = submission.executor_execution_id
         AND task.provider_id = j.provider_id
        LEFT JOIN LATERAL (
            SELECT progress.progress_json, progress.observed_at_ms
            FROM provider_task_observations progress
            WHERE progress.submission_id = task.submission_id
              AND progress.executor_execution_id = task.executor_execution_id
              AND progress.source = 'poll' AND progress.observed_state = 'provider_waiting'
              AND progress.progress_json IS NOT NULL
            ORDER BY progress.observed_at_ms DESC,
                     progress.poll_lease_epoch DESC, progress.observation_id DESC
            LIMIT 1
        ) observation ON TRUE
        WHERE j.job_id = $1 AND j.tenant_id = $2 AND attribution.project_id = $3
          AND ($4::UUID IS NULL OR attribution.actor_user_id = $4)
          AND j.provider_id = 'dreamina-cli' AND j.operation IN ('generation', 'edit')
          AND j.economics_contract_version IN (2, 3, 4)
        "#,
    )
    .bind(job_id)
    .bind(tenant_id)
    .bind(project_id)
    .bind(actor_user_id)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(DREAMINA_IMAGES_API_PROFILE)
    .fetch_optional(&store.pool)
    .await
    .map_err(settlement_unavailable)?;
    let Some(row) = row else { return Ok(None) };
    let count = u32::try_from(row.output_count)
        .ok()
        .filter(|count| (1..=10).contains(count))
        .ok_or_else(ImageGatewayError::artifact_integrity)?;
    let status = if row.job_state == "succeeded" {
        match store.load_generation_result(job_id).await? {
            GenerationResultLookup::Available(result) if result.images.len() == count as usize => {
                ImageResultStatus::Succeeded(result)
            }
            GenerationResultLookup::Expired => ImageResultStatus::Expired,
            _ => return Err(ImageGatewayError::artifact_integrity()),
        }
    } else if row.job_state == "failed" || row.work_state == "failed" {
        ImageResultStatus::Failed {
            error_code: row.error_code,
        }
    } else if row.work_state == "uncertain" {
        ImageResultStatus::Uncertain
    } else {
        let stage = match row.work_state.as_str() {
            "ready" => VideoPendingStage::Queued,
            "leased" | "running" => VideoPendingStage::Dispatching,
            _ => VideoPendingStage::Processing,
        };
        ImageResultStatus::Pending {
            stage,
            provider_progress: video_provider_progress(row.progress_json, row.observed_at_ms),
        }
    };
    Ok(Some(ImageTaskProjection {
        model: row.model,
        request_id: row.request_id,
        request: DreaminaImageRequestMetadata {
            generate_num: Some(count),
            model_version: row.model_version,
            workflow: row.workflow,
            resolution_type: row.resolution_type,
            ratio: row.ratio,
            width: positive_dimension(row.width)?,
            height: positive_dimension(row.height)?,
        },
        status,
    }))
}

fn positive_dimension(value: Option<i64>) -> Result<Option<u32>, ImageGatewayError> {
    value
        .map(|value| {
            u32::try_from(value)
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(ImageGatewayError::artifact_integrity)
        })
        .transpose()
}

pub(super) async fn lookup(
    store: &PostgresExecutionSettlementStore,
    tenant_id: &str,
    project_id: &str,
    actor_user_id: Option<Uuid>,
    operation: &str,
    key_digest: &str,
) -> Result<Option<Uuid>, ImageGatewayError> {
    let row: Option<(Option<Uuid>, String, bool)> = sqlx::query_as(
        r#"
        SELECT identity.job_id, identity.state,
               identity.request_hash = session.request_hash
                 AND identity.job_id IS NOT DISTINCT FROM session.job_id AS consistent
        FROM idempotency_requests identity
        JOIN admission_sessions session ON session.session_id = identity.session_id
          AND session.tenant_id = identity.tenant_id
          AND session.project_id = identity.project_id
          AND session.api_profile = identity.api_profile AND session.operation = identity.operation
        WHERE identity.tenant_id = $1 AND identity.project_id = $2
          AND identity.api_profile = $3 AND identity.operation = $4 AND identity.key_digest = $5
          AND (identity.job_id IS NULL OR EXISTS (
              SELECT 1 FROM job_auth_attributions attribution
              JOIN jobs job ON job.job_id = attribution.job_id
              WHERE attribution.job_id = identity.job_id AND attribution.project_id = $2
                AND job.tenant_id = $1 AND job.provider_id = 'dreamina-cli'
                AND ($6::UUID IS NULL OR attribution.actor_user_id = $6)
          ))
        "#,
    )
    .bind(tenant_id)
    .bind(project_id)
    .bind(DREAMINA_IMAGES_API_PROFILE)
    .bind(operation)
    .bind(key_digest)
    .bind(actor_user_id)
    .fetch_optional(&store.pool)
    .await
    .map_err(settlement_unavailable)?;
    match row {
        None => Ok(None),
        Some((_, _, false)) => Err(ImageGatewayError::idempotency_conflict()),
        Some((Some(job_id), _, true)) => Ok(Some(job_id)),
        Some((None, state, true)) if state == "receiving" => {
            Err(ImageGatewayError::idempotency_in_progress())
        }
        Some(_) => Err(ImageGatewayError::idempotency_conflict()),
    }
}
