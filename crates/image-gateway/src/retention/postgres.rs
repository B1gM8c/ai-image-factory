use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    ArtifactRetentionClaim, ArtifactRetentionLease, ArtifactRetentionStore, RetainedArtifactPair,
    RetainedExecutorArtifact,
};
use crate::{
    ImageGatewayError,
    artifacts::{ArtifactIdentity, ArtifactMetadata},
};

#[derive(Clone)]
pub struct PostgresArtifactRetentionStore {
    pool: PgPool,
}

impl PostgresArtifactRetentionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct RetainedArtifactRow {
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
    authority_id: Option<Uuid>,
    authority_storage_backend: Option<String>,
    authority_storage_namespace: Option<String>,
    authority_object_key: Option<String>,
    authority_sha256_hex: Option<String>,
    authority_byte_size: Option<i64>,
}

#[async_trait]
impl ArtifactRetentionStore for PostgresArtifactRetentionStore {
    async fn expire_due(&self, limit: u32) -> Result<u32, ImageGatewayError> {
        let limit = i64::from(limit);
        let affected = sqlx::query(
            r#"
            WITH clock AS (
                SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            ), due AS (
                SELECT retention.job_id
                FROM job_artifact_retention retention, clock
                WHERE retention.state = 'available'
                  AND retention.expires_at_ms <= clock.now_ms
                ORDER BY retention.expires_at_ms, retention.job_id
                FOR UPDATE OF retention SKIP LOCKED
                LIMIT $1
            )
            UPDATE job_artifact_retention retention
            SET state = 'expired', expired_at_ms = clock.now_ms,
                purge_after_ms = retention.expires_at_ms + retention.read_drain_ms,
                last_error_code = NULL, updated_at_ms = clock.now_ms
            FROM due, clock
            WHERE retention.job_id = due.job_id
            "#,
        )
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(retention_unavailable)?
        .rows_affected();
        Ok(u32::try_from(affected).unwrap_or(u32::MAX))
    }

    async fn claim_due(
        &self,
        owner: &str,
        lease_ms: u64,
    ) -> Result<Option<ArtifactRetentionClaim>, ImageGatewayError> {
        if owner.is_empty() || owner.len() > 255 || owner.chars().any(char::is_control) {
            return Err(ImageGatewayError::internal(
                "artifact retention owner is invalid",
            ));
        }
        let lease_ms = i64::try_from(lease_ms)
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| ImageGatewayError::internal("artifact retention lease is invalid"))?;
        let mut tx = self.pool.begin().await.map_err(retention_unavailable)?;
        let job_id: Option<Uuid> = sqlx::query_scalar(
            r#"
            WITH clock AS (
                SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            )
            SELECT retention.job_id
            FROM job_artifact_retention retention, clock
            WHERE (retention.state = 'expired'
                   AND retention.purge_after_ms <= clock.now_ms)
               OR (retention.state = 'deleting'
                   AND retention.lease_expires_at_ms <= clock.now_ms)
            ORDER BY CASE
                         WHEN retention.state = 'expired' THEN retention.purge_after_ms
                         ELSE retention.lease_expires_at_ms
                     END,
                     retention.job_id
            FOR UPDATE OF retention SKIP LOCKED
            LIMIT 1
            "#,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(retention_unavailable)?;
        let Some(job_id) = job_id else {
            tx.commit().await.map_err(retention_unavailable)?;
            return Ok(None);
        };
        let rows: Vec<RetainedArtifactRow> = sqlx::query_as(
            r#"
            SELECT artifact.artifact_id, artifact.tenant_id, artifact.job_id,
                   artifact.work_item_id, artifact.execution_id, artifact.lease_epoch,
                   artifact.output_index, artifact.media_type, artifact.storage_backend,
                   artifact.object_key, artifact.sha256_hex, artifact.byte_size,
                   authority.authority_id,
                   authority.storage_backend AS authority_storage_backend,
                   authority.storage_namespace AS authority_storage_namespace,
                   authority.object_key AS authority_object_key,
                   authority.sha256_hex AS authority_sha256_hex,
                   authority.byte_size AS authority_byte_size
            FROM artifacts artifact
            LEFT JOIN executor_artifact_authorities authority
              ON authority.job_id = artifact.job_id
             AND authority.output_id = artifact.artifact_id
            WHERE artifact.job_id = $1
            ORDER BY artifact.output_index
            "#,
        )
        .bind(job_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(retention_unavailable)?;
        let artifacts = if rows.is_empty() {
            None
        } else {
            rows.into_iter()
                .map(retained_pair_from_row)
                .collect::<Result<Vec<_>, _>>()
                .ok()
        };
        let Some(artifacts) = artifacts else {
            defer_invalid_manifest(&mut tx, job_id).await?;
            tx.commit().await.map_err(retention_unavailable)?;
            tracing::error!(%job_id, "artifact retention manifest quarantined for retry");
            return Ok(Some(ArtifactRetentionClaim::Deferred));
        };
        let epoch: i64 = sqlx::query_scalar(
            r#"
            UPDATE job_artifact_retention
            SET state = 'deleting', lease_owner = $1,
                lease_epoch = lease_epoch + 1,
                lease_expires_at_ms =
                    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT + $2,
                delete_attempts = delete_attempts + 1,
                updated_at_ms =
                    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
            WHERE job_id = $3
            RETURNING lease_epoch
            "#,
        )
        .bind(owner)
        .bind(lease_ms)
        .bind(job_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(retention_unavailable)?;
        tx.commit().await.map_err(retention_unavailable)?;
        Ok(Some(ArtifactRetentionClaim::Lease(
            ArtifactRetentionLease {
                job_id,
                owner: owner.to_owned(),
                epoch,
                artifacts,
            },
        )))
    }

    async fn complete(&self, lease: &ArtifactRetentionLease) -> Result<(), ImageGatewayError> {
        let affected = sqlx::query(
            r#"
            UPDATE job_artifact_retention
            SET state = 'deleted', lease_owner = NULL, lease_expires_at_ms = NULL,
                last_error_code = NULL,
                deleted_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
                updated_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
            WHERE job_id = $1 AND state = 'deleting'
              AND lease_owner = $2 AND lease_epoch = $3
            "#,
        )
        .bind(lease.job_id)
        .bind(&lease.owner)
        .bind(lease.epoch)
        .execute(&self.pool)
        .await
        .map_err(retention_unavailable)?
        .rows_affected();
        require_fenced_update(affected)
    }

    async fn retry(
        &self,
        lease: &ArtifactRetentionLease,
        error_code: &'static str,
    ) -> Result<(), ImageGatewayError> {
        let affected = sqlx::query(
            r#"
            UPDATE job_artifact_retention
            SET state = 'expired', lease_owner = NULL, lease_expires_at_ms = NULL,
                purge_after_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
                                 + retry_delay_ms,
                last_error_code = $4,
                updated_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
            WHERE job_id = $1 AND state = 'deleting'
              AND lease_owner = $2 AND lease_epoch = $3
            "#,
        )
        .bind(lease.job_id)
        .bind(&lease.owner)
        .bind(lease.epoch)
        .bind(error_code)
        .execute(&self.pool)
        .await
        .map_err(retention_unavailable)?
        .rows_affected();
        require_fenced_update(affected)
    }
}

async fn defer_invalid_manifest(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job_id: Uuid,
) -> Result<(), ImageGatewayError> {
    let affected = sqlx::query(
        r#"
        UPDATE job_artifact_retention
        SET state = 'expired', lease_owner = NULL, lease_expires_at_ms = NULL,
            purge_after_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
                             + retry_delay_ms,
            last_error_code = 'artifact_manifest_invalid',
            updated_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
        WHERE job_id = $1
        "#,
    )
    .bind(job_id)
    .execute(&mut **tx)
    .await
    .map_err(retention_unavailable)?
    .rows_affected();
    require_fenced_update(affected)
}

fn retained_pair_from_row(
    row: RetainedArtifactRow,
) -> Result<RetainedArtifactPair, ImageGatewayError> {
    let byte_size =
        u64::try_from(row.byte_size).map_err(|_| ImageGatewayError::artifact_integrity())?;
    let customer = ArtifactMetadata {
        identity: ArtifactIdentity {
            artifact_id: row.artifact_id,
            tenant_id: row.tenant_id,
            job_id: row.job_id,
            work_item_id: row.work_item_id,
            execution_id: row.execution_id,
            lease_epoch: row.lease_epoch,
            output_index: u32::try_from(row.output_index)
                .map_err(|_| ImageGatewayError::artifact_integrity())?,
            media_type: row.media_type,
        },
        storage_backend: row.storage_backend,
        object_key: row.object_key,
        sha256_hex: row.sha256_hex,
        byte_size,
    };
    let executor = match (
        row.authority_id,
        row.authority_storage_backend,
        row.authority_storage_namespace,
        row.authority_object_key,
        row.authority_sha256_hex,
        row.authority_byte_size,
    ) {
        (None, None, None, None, None, None) => None,
        (
            Some(authority_id),
            Some(storage_backend),
            Some(storage_namespace),
            Some(object_key),
            Some(sha256_hex),
            Some(byte_size),
        ) => {
            let byte_size =
                u64::try_from(byte_size).map_err(|_| ImageGatewayError::artifact_integrity())?;
            if storage_backend != customer.storage_backend
                || sha256_hex != customer.sha256_hex
                || byte_size != customer.byte_size
            {
                return Err(ImageGatewayError::artifact_integrity());
            }
            Some(RetainedExecutorArtifact {
                authority_id,
                storage_backend,
                storage_namespace,
                object_key,
                sha256_hex,
                byte_size,
            })
        }
        _ => return Err(ImageGatewayError::artifact_integrity()),
    };
    Ok(RetainedArtifactPair { customer, executor })
}

fn require_fenced_update(affected: u64) -> Result<(), ImageGatewayError> {
    if affected == 1 {
        Ok(())
    } else {
        Err(ImageGatewayError::service_unavailable(
            "artifact retention lease is stale",
        ))
    }
}

fn retention_unavailable(error: impl std::fmt::Display) -> ImageGatewayError {
    tracing::error!(error = %error, "artifact retention storage unavailable");
    ImageGatewayError::service_unavailable("artifact retention unavailable")
}
