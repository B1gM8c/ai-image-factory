use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    artifacts::{
        ArtifactIdentity, ArtifactMetadata, GenerationResponseProjection, GenerationResultManifest,
    },
    usage::UsageSnapshot,
};

#[derive(sqlx::FromRow)]
struct ProjectionRow {
    job_id: Uuid,
    tenant_id: String,
    api_profile: String,
    operation: String,
    response_schema: String,
    created_at_seconds: i64,
    output_format: String,
    quality: String,
    size: String,
    background: String,
    stream: bool,
    limit_5h: i32,
    remaining_5h: i32,
    limit_7d: i32,
    remaining_7d: i32,
    artifact_count: i32,
    retention_state: String,
}

pub(super) enum GenerationManifestLookup {
    Available(GenerationResultManifest),
    Expired,
    Missing,
}

#[derive(sqlx::FromRow)]
struct ArtifactRow {
    artifact_id: Uuid,
    tenant_id: String,
    job_id: Uuid,
    work_item_id: Uuid,
    execution_id: Uuid,
    lease_epoch: i64,
    output_index: i32,
    media_type: String,
    storage_backend: String,
    object_key: String,
    sha256_hex: String,
    byte_size: i64,
}

pub(super) async fn persist_generation_result(
    tx: &mut Transaction<'_, Postgres>,
    result: &GenerationResultManifest,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let projection = &result.projection;
    sqlx::query(
        r#"
        INSERT INTO job_response_projections
          (job_id, api_profile, operation, response_schema, created_at_seconds, output_format,
           quality, size, background, stream, limit_5h, remaining_5h, limit_7d,
           remaining_7d, artifact_count, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        "#,
    )
    .bind(result.job_id)
    .bind(&projection.api_profile)
    .bind(&projection.operation)
    .bind(&projection.response_schema)
    .bind(projection.created_at_seconds)
    .bind(&projection.output_format)
    .bind(&projection.quality)
    .bind(&projection.size)
    .bind(&projection.background)
    .bind(projection.stream)
    .bind(i32::try_from(projection.usage.limit_5h).map_err(invalid_result_number)?)
    .bind(i32::try_from(projection.usage.remaining_5h).map_err(invalid_result_number)?)
    .bind(i32::try_from(projection.usage.limit_7d).map_err(invalid_result_number)?)
    .bind(i32::try_from(projection.usage.remaining_7d).map_err(invalid_result_number)?)
    .bind(i32::try_from(result.artifacts.len()).map_err(invalid_result_number)?)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(result_storage_unavailable)?;

    for artifact in &result.artifacts {
        sqlx::query(
            r#"
            INSERT INTO artifacts
              (artifact_id, tenant_id, job_id, work_item_id, execution_id, lease_epoch,
               output_index, state, storage_backend, object_key, sha256_hex, byte_size,
               media_type, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'ready', $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(artifact.identity.artifact_id)
        .bind(&artifact.identity.tenant_id)
        .bind(artifact.identity.job_id)
        .bind(artifact.identity.work_item_id)
        .bind(artifact.identity.execution_id)
        .bind(artifact.identity.lease_epoch)
        .bind(i32::try_from(artifact.identity.output_index).map_err(invalid_result_number)?)
        .bind(&artifact.storage_backend)
        .bind(&artifact.object_key)
        .bind(&artifact.sha256_hex)
        .bind(i64::try_from(artifact.byte_size).map_err(invalid_result_number)?)
        .bind(&artifact.identity.media_type)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(result_storage_unavailable)?;
    }
    Ok(())
}

pub(super) async fn validate_completed_result(
    tx: &mut Transaction<'_, Postgres>,
    expected: &GenerationResultManifest,
) -> Result<(), ImageGatewayError> {
    let stored = load_generation_manifest_tx(tx, expected.job_id)
        .await?
        .ok_or_else(|| ImageGatewayError::internal("completed job has no result projection"))?;
    if stored == *expected {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "generation result differs from committed projection",
        ))
    }
}

pub(super) async fn load_generation_manifest(
    pool: &PgPool,
    job_id: Uuid,
) -> Result<GenerationManifestLookup, ImageGatewayError> {
    let projection: Option<ProjectionRow> = sqlx::query_as(
        r#"
        SELECT p.job_id, j.tenant_id, p.api_profile, p.operation, p.response_schema,
               p.created_at_seconds, p.output_format, p.quality, p.size, p.background,
               p.stream, p.limit_5h, p.remaining_5h, p.limit_7d, p.remaining_7d,
               p.artifact_count,
               CASE
                 WHEN retention.state = 'available'
                  AND retention.expires_at_ms >
                      (EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::BIGINT
                 THEN 'available'
                 ELSE 'expired'
               END AS retention_state
        FROM job_response_projections p
        JOIN jobs j ON j.job_id = p.job_id
        JOIN job_artifact_retention retention ON retention.job_id = p.job_id
        WHERE p.job_id = $1 AND j.state = 'succeeded'
        "#,
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await
    .map_err(result_storage_unavailable)?;
    let Some(projection) = projection else {
        return Ok(GenerationManifestLookup::Missing);
    };
    if projection.retention_state != "available" {
        return Ok(GenerationManifestLookup::Expired);
    }
    let artifacts: Vec<ArtifactRow> = sqlx::query_as(
        r#"
        SELECT artifact_id, tenant_id, job_id, work_item_id, execution_id, lease_epoch,
               output_index, media_type, storage_backend, object_key, sha256_hex, byte_size
        FROM artifacts
        WHERE job_id = $1 AND state = 'ready'
        ORDER BY output_index
        "#,
    )
    .bind(job_id)
    .fetch_all(pool)
    .await
    .map_err(result_storage_unavailable)?;
    manifest_from_rows(projection, artifacts).map(GenerationManifestLookup::Available)
}

async fn load_generation_manifest_tx(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<Option<GenerationResultManifest>, ImageGatewayError> {
    let projection: Option<ProjectionRow> = sqlx::query_as(
        r#"
        SELECT p.job_id, j.tenant_id, p.api_profile, p.operation, p.response_schema,
               p.created_at_seconds, p.output_format, p.quality, p.size, p.background,
               p.stream, p.limit_5h, p.remaining_5h, p.limit_7d, p.remaining_7d,
               p.artifact_count, 'available'::TEXT AS retention_state
        FROM job_response_projections p
        JOIN jobs j ON j.job_id = p.job_id
        WHERE p.job_id = $1
        FOR UPDATE OF p
        "#,
    )
    .bind(job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(result_storage_unavailable)?;
    let Some(projection) = projection else {
        return Ok(None);
    };
    let artifacts: Vec<ArtifactRow> = sqlx::query_as(
        r#"
        SELECT artifact_id, tenant_id, job_id, work_item_id, execution_id, lease_epoch,
               output_index, media_type, storage_backend, object_key, sha256_hex, byte_size
        FROM artifacts
        WHERE job_id = $1 AND state = 'ready'
        ORDER BY output_index
        FOR UPDATE
        "#,
    )
    .bind(job_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(result_storage_unavailable)?;
    manifest_from_rows(projection, artifacts).map(Some)
}

fn manifest_from_rows(
    projection: ProjectionRow,
    artifacts: Vec<ArtifactRow>,
) -> Result<GenerationResultManifest, ImageGatewayError> {
    if usize::try_from(projection.artifact_count).ok() != Some(artifacts.len()) {
        return Err(ImageGatewayError::artifact_integrity());
    }
    let usage = UsageSnapshot {
        limit_5h: u32::try_from(projection.limit_5h).map_err(invalid_projection_number)?,
        remaining_5h: u32::try_from(projection.remaining_5h).map_err(invalid_projection_number)?,
        limit_7d: u32::try_from(projection.limit_7d).map_err(invalid_projection_number)?,
        remaining_7d: u32::try_from(projection.remaining_7d).map_err(invalid_projection_number)?,
    };
    let artifacts = artifacts
        .into_iter()
        .map(|row| {
            Ok(ArtifactMetadata {
                identity: ArtifactIdentity {
                    artifact_id: row.artifact_id,
                    tenant_id: row.tenant_id,
                    job_id: row.job_id,
                    work_item_id: row.work_item_id,
                    execution_id: row.execution_id,
                    lease_epoch: row.lease_epoch,
                    output_index: u32::try_from(row.output_index)
                        .map_err(invalid_projection_number)?,
                    media_type: row.media_type,
                },
                storage_backend: row.storage_backend,
                object_key: row.object_key,
                sha256_hex: row.sha256_hex,
                byte_size: u64::try_from(row.byte_size).map_err(invalid_projection_number)?,
            })
        })
        .collect::<Result<Vec<_>, ImageGatewayError>>()?;
    Ok(GenerationResultManifest {
        job_id: projection.job_id,
        tenant_id: projection.tenant_id,
        projection: GenerationResponseProjection {
            api_profile: projection.api_profile,
            operation: projection.operation,
            response_schema: projection.response_schema,
            created_at_seconds: projection.created_at_seconds,
            output_format: projection.output_format,
            quality: projection.quality,
            size: projection.size,
            background: projection.background,
            stream: projection.stream,
            usage,
        },
        artifacts,
    })
}

pub(super) async fn generation_result_is_expired(
    pool: &PgPool,
    job_id: Uuid,
) -> Result<bool, ImageGatewayError> {
    sqlx::query_scalar(
        r#"
        SELECT state <> 'available'
            OR expires_at_ms <=
               (EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::BIGINT
        FROM job_artifact_retention
        WHERE job_id = $1
        "#,
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await
    .map_err(result_storage_unavailable)?
    .ok_or_else(ImageGatewayError::artifact_integrity)
}

fn invalid_result_number(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::internal("generation result number is out of range")
}

fn invalid_projection_number(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::artifact_integrity()
}

fn result_storage_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("execution settlement unavailable")
}
