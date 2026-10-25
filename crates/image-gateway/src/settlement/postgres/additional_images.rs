use super::*;
use crate::{
    artifacts::{executor_object_key, media_type_from_bytes},
    provider_tasks::additional_image_artifact_id,
    settlement::AdditionalImageResult,
};

#[derive(sqlx::FromRow)]
struct CandidateRow {
    source_index: i32,
    artifact_id: Uuid,
    executor_execution_id: Uuid,
    work_item_id: Uuid,
    execution_id: Uuid,
    lease_epoch: i64,
    storage_backend: String,
    storage_namespace: String,
    object_key: String,
    sha256_hex: String,
    byte_size: i64,
    media_type: String,
}

pub(super) async fn project(
    store: &PostgresExecutionSettlementStore,
    tenant_id: &str,
    project_id: &str,
    actor_user_id: Option<Uuid>,
    job_id: Uuid,
) -> Result<Vec<AdditionalImageResult>, ImageGatewayError> {
    // Scope and original retention are checked before fetching any private blobs.
    let count: Option<(i32, bool, Uuid, Uuid)> = sqlx::query_as(
        r#"SELECT result_set.returned_count,
             (result_set.cleanup_state = 'available' AND retention.state = 'available'
              AND retention.expires_at_ms > floor(extract(epoch FROM statement_timestamp()) * 1000)::BIGINT) AS readable,
             result_set.submission_id, result_set.executor_execution_id
           FROM jobs job
           JOIN job_auth_attributions attribution USING (job_id)
           JOIN job_artifact_retention retention USING (job_id)
           JOIN artifacts primary_artifact ON primary_artifact.job_id = job.job_id AND primary_artifact.output_index = 0
           JOIN provider_submissions primary_submission ON primary_submission.output_id = primary_artifact.artifact_id
             AND primary_submission.job_id = job.job_id
           JOIN executor_artifact_authorities primary_authority ON primary_authority.submission_id = primary_submission.submission_id
             AND primary_authority.executor_execution_id = primary_submission.executor_execution_id
             AND primary_authority.output_id = primary_artifact.artifact_id
             AND primary_authority.sha256_hex = primary_artifact.sha256_hex
             AND primary_authority.byte_size = primary_artifact.byte_size
             AND primary_authority.media_type = primary_artifact.media_type
           JOIN dreamina_image_result_sets result_set ON result_set.job_id = job.job_id
             AND result_set.submission_id = primary_authority.submission_id
             AND result_set.executor_execution_id = primary_authority.executor_execution_id
           JOIN job_payloads payload ON payload.job_id = job.job_id
           JOIN admission_sessions session ON session.session_id = payload.admission_session_id
             AND session.api_profile = $5 AND session.job_id = job.job_id
             AND session.project_id = attribution.project_id AND session.tenant_id = job.tenant_id
           WHERE job.job_id = $1 AND job.tenant_id = $2 AND attribution.project_id = $3
             AND ($4::UUID IS NULL OR attribution.actor_user_id = $4)
             AND job.state = 'succeeded' AND job.provider_id = 'dreamina-cli'
             AND job.operation IN ('generation','edit') AND job.output_count = 1
             "#,
    ).bind(job_id).bind(tenant_id).bind(project_id).bind(actor_user_id)
        .bind(image_api_contracts::dreamina::DREAMINA_IMAGES_API_PROFILE)
        .fetch_optional(&store.pool).await.map_err(settlement_unavailable)?;
    let Some((count, readable, submission_id, execution_id)) = count else {
        return Ok(Vec::new()); // Older completed jobs have no physical result set.
    };
    if !readable {
        return Err(ImageGatewayError::artifact_expired());
    }
    let expected = usize::try_from(count)
        .ok()
        .filter(|count| (1..=10).contains(count))
        .ok_or_else(ImageGatewayError::artifact_integrity)?
        - 1;
    let rows: Vec<CandidateRow> = sqlx::query_as(
        r#"SELECT artifact.source_index, artifact.artifact_id, artifact.executor_execution_id,
                  submission.work_item_id, attempt.execution_id, attempt.lease_epoch,
                  artifact.storage_backend, artifact.storage_namespace, artifact.object_key,
                  artifact.sha256_hex, artifact.byte_size, artifact.media_type
           FROM dreamina_image_additional_artifacts artifact
           JOIN provider_submissions submission ON submission.submission_id = artifact.submission_id
             AND submission.executor_execution_id = artifact.executor_execution_id
             AND submission.job_id = artifact.job_id
           JOIN job_attempts attempt ON attempt.execution_id = submission.created_by_execution_id
           WHERE artifact.job_id = $1 AND artifact.submission_id = $2
             AND artifact.executor_execution_id = $3 ORDER BY artifact.source_index"#,
    )
    .bind(job_id)
    .bind(submission_id)
    .bind(execution_id)
    .fetch_all(&store.pool)
    .await
    .map_err(settlement_unavailable)?;
    if rows.len() != expected {
        return Err(ImageGatewayError::artifact_integrity());
    }
    let mut results = Vec::with_capacity(rows.len());
    let mut total = 0_u64;
    for (offset, row) in rows.into_iter().enumerate() {
        let source_index = offset + 1;
        let byte_size = u64::try_from(row.byte_size)
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(ImageGatewayError::artifact_integrity)?;
        total = total
            .checked_add(byte_size)
            .filter(|value| *value <= crate::artifacts::MAX_ARTIFACT_BYTES)
            .ok_or_else(ImageGatewayError::artifact_integrity)?;
        if usize::try_from(row.source_index).ok() != Some(source_index)
            || Some(row.artifact_id)
                != additional_image_artifact_id(row.executor_execution_id, source_index)
            || row.object_key != executor_object_key(row.artifact_id)
        {
            return Err(ImageGatewayError::artifact_integrity());
        }
        let metadata = ArtifactMetadata {
            identity: ArtifactIdentity {
                artifact_id: row.artifact_id,
                tenant_id: tenant_id.to_owned(),
                job_id,
                work_item_id: row.work_item_id,
                execution_id: row.execution_id,
                lease_epoch: row.lease_epoch,
                output_index: row.source_index as u32,
                media_type: row.media_type.clone(),
            },
            storage_backend: row.storage_backend,
            object_key: row.object_key,
            sha256_hex: row.sha256_hex.clone(),
            byte_size,
        };
        let bytes = store
            .artifact_store
            .get_additional_image(&metadata, &row.storage_namespace)
            .await
            .map_err(|error| match error {
                ArtifactReadError::Integrity => ImageGatewayError::artifact_integrity(),
                ArtifactReadError::Unavailable => {
                    ImageGatewayError::service_unavailable("artifact storage unavailable")
                }
            })?;
        if bytes.len() as u64 != byte_size
            || hex::encode(Sha256::digest(&bytes)) != row.sha256_hex
            || media_type_from_bytes(&bytes).ok() != Some(row.media_type.as_str())
        {
            return Err(ImageGatewayError::artifact_integrity());
        }
        results.push(AdditionalImageResult {
            source_index,
            bytes,
            sha256: row.sha256_hex,
            byte_size,
            mime_type: row.media_type,
        });
    }
    Ok(results)
}

pub(super) async fn validate_budget(
    store: &PostgresExecutionSettlementStore,
    tenant_id: &str,
    project_id: &str,
    actor_user_id: Option<Uuid>,
    job_id: Uuid,
) -> Result<(), ImageGatewayError> {
    let size: Option<i64> = sqlx::query_scalar(
        r#"SELECT (COALESCE((SELECT sum(byte_size) FROM artifacts WHERE job_id = job.job_id),0)
                 + COALESCE((SELECT sum(extra.byte_size) FROM dreamina_image_additional_artifacts extra
                    JOIN provider_submissions primary_submission ON primary_submission.submission_id = extra.submission_id
                      AND primary_submission.executor_execution_id = extra.executor_execution_id
                    JOIN artifacts primary_artifact ON primary_artifact.artifact_id = primary_submission.output_id
                      AND primary_artifact.job_id = job.job_id AND primary_artifact.output_index = 0
                    JOIN executor_artifact_authorities primary_authority ON primary_authority.submission_id = extra.submission_id
                      AND primary_authority.executor_execution_id = extra.executor_execution_id
                      AND primary_authority.sha256_hex = primary_artifact.sha256_hex
                      AND primary_authority.byte_size = primary_artifact.byte_size
                      AND primary_authority.media_type = primary_artifact.media_type
                    WHERE extra.job_id = job.job_id),0))::BIGINT
           FROM jobs job JOIN job_auth_attributions attribution USING (job_id)
           JOIN job_artifact_retention retention USING (job_id)
           JOIN job_payloads payload ON payload.job_id = job.job_id
           JOIN admission_sessions session ON session.session_id = payload.admission_session_id
             AND session.api_profile = $5 AND session.job_id = job.job_id
             AND session.project_id = attribution.project_id AND session.tenant_id = job.tenant_id
           WHERE job.job_id = $1 AND job.tenant_id = $2 AND attribution.project_id = $3
             AND ($4::UUID IS NULL OR attribution.actor_user_id = $4)
             AND job.state = 'succeeded' AND job.provider_id = 'dreamina-cli'
             AND job.operation IN ('generation','edit') AND retention.state = 'available'
             AND retention.expires_at_ms > floor(extract(epoch FROM statement_timestamp()) * 1000)::BIGINT"#,
    ).bind(job_id).bind(tenant_id).bind(project_id).bind(actor_user_id)
        .bind(image_api_contracts::dreamina::DREAMINA_IMAGES_API_PROFILE)
        .fetch_optional(&store.pool).await.map_err(settlement_unavailable)?;
    if size.is_some_and(|size| size <= 0 || size as u64 > crate::artifacts::MAX_ARTIFACT_BYTES) {
        return Err(ImageGatewayError::artifact_integrity());
    }
    Ok(())
}
