//! Additional nonbillable images from the separately authorized one-shot attempt.
use super::*;
use crate::settlement::AdditionalImageResult;

#[derive(sqlx::FromRow)]
struct Scope {
    authorization_id: Uuid,
    dispatch_nonce: Uuid,
    returned_count: i32,
    work_item_id: Uuid,
    execution_id: Uuid,
    lease_epoch: i64,
    readable: bool,
}
#[derive(sqlx::FromRow)]
struct ImageRow {
    source_index: i32,
    artifact_id: Uuid,
    storage_backend: String,
    storage_namespace: String,
    object_key: String,
    sha256_hex: String,
    byte_size: i64,
}
async fn scope(
    store: &PostgresExecutionSettlementStore,
    tenant: &str,
    project: &str,
    actor: Option<Uuid>,
    job: Uuid,
) -> Result<Option<Scope>, ImageGatewayError> {
    sqlx::query_as(r#"SELECT auth.authorization_id,auth.dispatch_nonce,resolution.returned_count,work.work_item_id,attempt.execution_id,attempt.lease_epoch,
       (retention.state='available' AND retention.expires_at_ms > floor(extract(epoch FROM statement_timestamp())*1000)::BIGINT) AS readable
       FROM jobs job JOIN job_auth_attributions attribution USING(job_id)
       JOIN dreamina_operator_image_supplement_authorizations auth USING(job_id)
       JOIN dreamina_operator_image_supplement_resolutions resolution USING(authorization_id,job_id)
       JOIN job_artifact_retention retention USING(job_id)
       JOIN work_items work USING(job_id) JOIN job_attempts attempt ON attempt.execution_id=work.execution_id AND attempt.lease_epoch=work.lease_epoch
       JOIN artifacts primary_artifact ON primary_artifact.artifact_id=resolution.artifact_id AND primary_artifact.job_id=job.job_id
         AND primary_artifact.sha256_hex=resolution.image_sha256 AND primary_artifact.byte_size=resolution.image_byte_size
       JOIN job_payloads payload ON payload.job_id=job.job_id JOIN admission_sessions admission ON admission.session_id=payload.admission_session_id
       WHERE job.job_id=$1 AND job.tenant_id=$2 AND attribution.project_id=$3 AND ($4::UUID IS NULL OR attribution.actor_user_id=$4)
         AND job.state='succeeded' AND auth.state='succeeded' AND job.operation='generation' AND job.provider_id='dreamina-cli'
         AND admission.api_profile=$5 AND admission.job_id=job.job_id AND admission.project_id=attribution.project_id AND admission.tenant_id=job.tenant_id"#)
       .bind(job).bind(tenant).bind(project).bind(actor).bind(image_api_contracts::dreamina::DREAMINA_IMAGES_API_PROFILE)
       .fetch_optional(&store.pool).await.map_err(unavailable)
}
pub(in crate::settlement::postgres) async fn project(
    store: &PostgresExecutionSettlementStore,
    tenant: &str,
    project: &str,
    actor: Option<Uuid>,
    job: Uuid,
) -> Result<Option<Vec<AdditionalImageResult>>, ImageGatewayError> {
    let Some(scope) = scope(store, tenant, project, actor, job).await? else {
        return Ok(None);
    };
    if !scope.readable {
        return Err(ImageGatewayError::artifact_expired());
    }
    let rows:Vec<ImageRow>=sqlx::query_as("SELECT source_index,artifact_id,storage_backend,storage_namespace,object_key,sha256_hex,byte_size FROM dreamina_operator_image_supplement_artifacts WHERE authorization_id=$1 AND job_id=$2 ORDER BY source_index")
       .bind(scope.authorization_id).bind(job).fetch_all(&store.pool).await.map_err(unavailable)?;
    if rows.len() != scope.returned_count as usize || !(1..=4).contains(&rows.len()) {
        return Err(ImageGatewayError::artifact_integrity());
    }
    let mut output = Vec::new();
    let mut total = 0u64;
    for (index, row) in rows.into_iter().enumerate() {
        let size =
            u64::try_from(row.byte_size).map_err(|_| ImageGatewayError::artifact_integrity())?;
        total = total
            .checked_add(size)
            .filter(|v| *v <= crate::artifacts::MAX_ARTIFACT_BYTES)
            .ok_or_else(ImageGatewayError::artifact_integrity)?;
        let expected = if index == 0 {
            scope.dispatch_nonce
        } else {
            image_artifact_id(scope.authorization_id, index)?
        };
        if row.source_index != index as i32
            || row.artifact_id != expected
            || size == 0
            || row.storage_namespace != store.artifact_store.storage_identity()
        {
            return Err(ImageGatewayError::artifact_integrity());
        }
        if index == 0 {
            continue;
        }
        let artifact = ArtifactMetadata {
            identity: ArtifactIdentity {
                artifact_id: row.artifact_id,
                tenant_id: tenant.into(),
                job_id: job,
                work_item_id: scope.work_item_id,
                execution_id: scope.execution_id,
                lease_epoch: scope.lease_epoch,
                output_index: index as u32,
                media_type: "image/png".into(),
            },
            storage_backend: row.storage_backend,
            object_key: row.object_key,
            sha256_hex: row.sha256_hex.clone(),
            byte_size: size,
        };
        let bytes = store
            .artifact_store
            .get(&artifact)
            .await
            .map_err(|e| match e {
                ArtifactReadError::Integrity => ImageGatewayError::artifact_integrity(),
                ArtifactReadError::Unavailable => {
                    ImageGatewayError::service_unavailable("supplement image storage unavailable")
                }
            })?;
        if bytes.len() as u64 != size
            || sha256_hex(&bytes) != row.sha256_hex
            || image::guess_format(&bytes).ok() != Some(image::ImageFormat::Png)
        {
            return Err(ImageGatewayError::artifact_integrity());
        }
        output.push(AdditionalImageResult {
            source_index: index,
            bytes,
            sha256: row.sha256_hex,
            byte_size: size,
            mime_type: "image/png".into(),
        });
    }
    Ok(Some(output))
}
pub(in crate::settlement::postgres) async fn validate_budget(
    store: &PostgresExecutionSettlementStore,
    tenant: &str,
    project: &str,
    actor: Option<Uuid>,
    job: Uuid,
) -> Result<bool, ImageGatewayError> {
    let Some(scope) = scope(store, tenant, project, actor, job).await? else {
        return Ok(false);
    };
    if !scope.readable {
        return Err(ImageGatewayError::artifact_expired());
    }
    let (count,size):(i64,i64)=sqlx::query_as("SELECT count(*),COALESCE(sum(byte_size),0)::BIGINT FROM dreamina_operator_image_supplement_artifacts WHERE authorization_id=$1 AND job_id=$2")
      .bind(scope.authorization_id).bind(job).fetch_one(&store.pool).await.map_err(unavailable)?;
    if count != i64::from(scope.returned_count)
        || !(1..=4).contains(&count)
        || size <= 0
        || size as u64 > crate::artifacts::MAX_ARTIFACT_BYTES
    {
        return Err(ImageGatewayError::artifact_integrity());
    }
    Ok(true)
}
