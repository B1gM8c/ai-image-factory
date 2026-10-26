use super::*;
use crate::settlement::{VideoPendingStage, VideoResultStatus};
use image_provider_dreamina_cli::{DreaminaQueryStatusV1, parse_query_receipt};

impl PostgresExecutionSettlementStore {
    /// Called only after a bound query's raw evidence has been saved privately.
    /// Parsing the original receipt here prevents caller-supplied queue fields.
    pub async fn record_dreamina_video_supplement_observation(
        &self,
        authorization_id: Uuid,
        dispatch_nonce: Uuid,
        stdout: &[u8],
        observed_at_ms: i64,
    ) -> Result<(), ImageGatewayError> {
        let query = parse_query_receipt(stdout).map_err(|_| conflict())?;
        let (status, progress) = match query.status() {
            DreaminaQueryStatusV1::Querying => {
                let (_, progress) = crate::providers::dreamina_cli::pending_progress(&query);
                progress.validate().map_err(|_| conflict())?;
                (
                    "querying",
                    Some(json!({
                        "provider_status": progress.provider_status,
                        "queue_position": progress.queue_position,
                        "queue_total": progress.queue_total,
                        "estimated_remaining_seconds": progress.estimated_remaining_seconds,
                    })),
                )
            }
            DreaminaQueryStatusV1::Failed => ("failed", None),
            DreaminaQueryStatusV1::Success => ("success", None),
        };
        sqlx::query("INSERT INTO dreamina_operator_video_supplement_observations (observation_id,authorization_id,dispatch_nonce,remote_operation_id,stdout_sha256,observed_at_ms,query_status,progress_json) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (authorization_id,observed_at_ms,stdout_sha256) DO NOTHING")
            .bind(Uuid::new_v4()).bind(authorization_id).bind(dispatch_nonce)
            .bind(query.submit_id()).bind(sha256_hex(stdout)).bind(observed_at_ms)
            .bind(status).bind(progress).execute(&self.pool).await.map_err(storage_conflict)?;
        Ok(())
    }
}

pub(in crate::settlement::postgres) async fn project(
    store: &PostgresExecutionSettlementStore,
    tenant_id: &str,
    project_id: Option<&str>,
    actor_user_id: Option<Uuid>,
    job_id: Uuid,
    status: VideoResultStatus,
) -> Result<VideoResultStatus, ImageGatewayError> {
    let VideoResultStatus::Uncertain {
        model, duration_ms, ..
    } = &status
    else {
        return Ok(status);
    };
    let observation: Option<(Option<String>, Option<Value>, Option<i64>, bool)> = sqlx::query_as(
        r#"
        SELECT observation.query_status, observation.progress_json, observation.observed_at_ms, cancellation.cancellation_id IS NOT NULL
        FROM dreamina_operator_video_supplement_authorizations supplement
        JOIN jobs job ON job.job_id = supplement.job_id
        JOIN job_auth_attributions attribution
          ON attribution.job_id = job.job_id AND attribution.tenant_id = job.tenant_id
        JOIN customer_billing_holds hold ON hold.hold_id = supplement.hold_id
          AND hold.job_id = job.job_id AND hold.tenant_id = job.tenant_id
        LEFT JOIN dreamina_operator_video_supplement_cancellations cancellation ON cancellation.authorization_id=supplement.authorization_id AND cancellation.finalized_snapshot IS NOT NULL
        LEFT JOIN LATERAL (
            SELECT query_status, progress_json, observed_at_ms
            FROM dreamina_operator_video_supplement_observations
            WHERE authorization_id = supplement.authorization_id
              AND remote_operation_id = supplement.remote_operation_id
              AND dispatch_nonce = supplement.dispatch_nonce
            ORDER BY observed_at_ms DESC, observation_id DESC LIMIT 1
        ) observation ON TRUE
        WHERE job.job_id = $1 AND job.tenant_id = $2
          AND ($3::TEXT IS NULL OR attribution.project_id = $3)
          AND ($4::UUID IS NULL OR attribution.actor_user_id = $4)
          AND job.provider_id = 'dreamina-cli' AND job.operation = 'video_generation'
          AND job.state = 'uncertain' AND job.economics_contract_version = 4
          AND supplement.state IN ('accepted', 'uncertain')
          AND supplement.remote_operation_id IS NOT NULL
          AND supplement.submit_stdout_sha256 IS NOT NULL
          AND ((hold.state = 'held' AND hold.captured_micros = 0 AND hold.released_micros = 0) OR (cancellation.cancellation_id IS NOT NULL AND hold.captured_micros=0 AND hold.released_micros=hold.held_micros))
    "#,
    )
    .bind(job_id)
    .bind(tenant_id)
    .bind(project_id)
    .bind(actor_user_id)
    .fetch_optional(&store.pool)
    .await
    .map_err(unavailable)?;
    match observation {
        Some((_, _, _, true)) => Ok(VideoResultStatus::Failed {
            model: model.clone(),
            duration_ms: *duration_ms,
            error_code: Some("operator_supplement_cancelled".to_owned()),
        }),
        Some((Some(query_status), _, _, false)) if query_status == "failed" => Ok(status),
        Some((_, progress, observed_at_ms, false)) => Ok(VideoResultStatus::Pending {
            model: model.clone(),
            duration_ms: *duration_ms,
            stage: VideoPendingStage::Processing,
            provider_progress: super::super::video_provider_progress(progress, observed_at_ms),
        }),
        None => Ok(status),
    }
}
