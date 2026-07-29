use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    ExecutionSettlementStore, GenerationResultLookup, GenerationResultStatus, StoredVideoArtifact,
    VideoPendingStage, VideoResultStatus, validate_generation_result,
};
use crate::{
    ImageGatewayError,
    admission::WorkLease,
    artifacts::{
        ArtifactBlobStore, ArtifactIdentity, ArtifactMetadata, ArtifactReadError,
        GenerationResultManifest, hydrate_generation_result,
    },
    usage::{UsageReservation, UsageSnapshot},
};

mod failure;
mod results;

use results::{
    GenerationManifestLookup, generation_result_is_expired, load_generation_manifest,
    persist_generation_result, validate_completed_result,
};

#[derive(Clone)]
pub struct PostgresExecutionSettlementStore {
    pool: PgPool,
    artifact_store: Arc<dyn ArtifactBlobStore>,
}

impl PostgresExecutionSettlementStore {
    pub fn new(pool: PgPool, artifact_store: Arc<dyn ArtifactBlobStore>) -> Self {
        Self {
            pool,
            artifact_store,
        }
    }
}

#[async_trait]
impl ExecutionSettlementStore for PostgresExecutionSettlementStore {
    fn artifact_storage_identity(&self) -> String {
        self.artifact_store.storage_identity()
    }

    async fn succeed(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        result: &GenerationResultManifest,
    ) -> Result<UsageSnapshot, ImageGatewayError> {
        validate_generation_result(lease, reservation, result)?;
        let mut tx = self.pool.begin().await.map_err(settlement_unavailable)?;
        lock_tenant_quota(&mut tx, &reservation.charge.tenant_id).await?;
        let quota_job = lock_quota_and_job(&mut tx, reservation).await?;
        validate_reservation_handle(&quota_job, reservation)?;
        let work_attempt = lock_work_and_attempt(&mut tx, lease).await?;
        validate_lease_identity(&work_attempt, lease)?;
        let idempotency = lock_idempotency(&mut tx, lease.job_id).await?;
        let now = database_now(&mut tx).await?;

        if is_completed(&quota_job, &work_attempt) {
            validate_completed_state(&quota_job, &work_attempt, &idempotency)?;
            validate_completed_result(&mut tx, result).await?;
            tx.commit().await.map_err(settlement_unavailable)?;
            return Ok(reservation.snapshot.clone());
        }

        validate_active_state(&quota_job, &work_attempt, &idempotency, lease, now)?;
        persist_generation_result(&mut tx, result, now).await?;
        transition_work(&mut tx, lease, now).await?;
        transition_attempt(&mut tx, lease, now).await?;
        transition_idempotency(&mut tx, lease.job_id, now, idempotency.len()).await?;
        insert_usage_event(&mut tx, &quota_job, now).await?;
        commit_quota(&mut tx, &quota_job, now).await?;
        succeed_job(&mut tx, &quota_job, now).await?;
        insert_metering_events(&mut tx, &quota_job, now).await?;
        append_succeeded_events(&mut tx, lease, now).await?;

        tx.commit().await.map_err(settlement_unavailable)?;
        Ok(reservation.snapshot.clone())
    }

    async fn fail(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        error_code: &'static str,
    ) -> Result<(), ImageGatewayError> {
        failure::settle(&self.pool, lease, reservation, error_code).await
    }

    async fn load_generation_result(
        &self,
        job_id: Uuid,
    ) -> Result<GenerationResultLookup, ImageGatewayError> {
        let manifest = match load_generation_manifest(&self.pool, job_id).await? {
            GenerationManifestLookup::Available(manifest) => manifest,
            GenerationManifestLookup::Expired => return Ok(GenerationResultLookup::Expired),
            GenerationManifestLookup::Missing => return Ok(GenerationResultLookup::Missing),
        };
        match hydrate_generation_result(self.artifact_store.as_ref(), manifest).await {
            Ok(result) => Ok(GenerationResultLookup::Available(result)),
            Err(error) => {
                if generation_result_is_expired(&self.pool, job_id).await? {
                    Ok(GenerationResultLookup::Expired)
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn generation_status(
        &self,
        job_id: Uuid,
    ) -> Result<GenerationResultStatus, ImageGatewayError> {
        let state: Option<(String, String, Option<String>)> = sqlx::query_as(
            r#"
            SELECT j.state AS job_state, w.state AS work_state, j.last_error_code
            FROM jobs j
            JOIN work_items w ON w.job_id = j.job_id
            WHERE j.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        match state {
            Some((job_state, _, _)) if job_state == "succeeded" => {
                match self.load_generation_result(job_id).await? {
                    GenerationResultLookup::Available(result) => {
                        Ok(GenerationResultStatus::Succeeded(result))
                    }
                    GenerationResultLookup::Expired => Ok(GenerationResultStatus::Expired),
                    GenerationResultLookup::Missing => Err(ImageGatewayError::artifact_integrity()),
                }
            }
            Some((job_state, work_state, error_code))
                if job_state == "failed" || work_state == "failed" =>
            {
                Ok(GenerationResultStatus::Failed { error_code })
            }
            Some((_, work_state, _)) if work_state == "uncertain" => {
                Ok(GenerationResultStatus::Uncertain)
            }
            Some(_) => Ok(GenerationResultStatus::Pending),
            None => Err(ImageGatewayError::internal("generation job not found")),
        }
    }

    async fn video_status(
        &self,
        tenant_id: &str,
        job_id: Uuid,
    ) -> Result<Option<VideoResultStatus>, ImageGatewayError> {
        let row: Option<VideoStatusRow> = sqlx::query_as(
            r#"
            SELECT j.state, w.state, j.last_error_code, j.model, j.billable_units,
                   artifact.artifact_id
            FROM jobs j
            JOIN work_items w ON w.job_id = j.job_id
            LEFT JOIN artifacts artifact
              ON artifact.job_id = j.job_id
             AND artifact.output_index = 0
             AND artifact.state = 'ready'
             AND artifact.media_type = 'video/mp4'
            WHERE j.job_id = $1 AND j.tenant_id = $2
              AND j.operation = 'video_generation'
              AND j.economics_contract_version IN (3, 4)
            "#,
        )
        .bind(job_id)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        row.map(video_result_status).transpose()
    }

    async fn project_video_status(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        job_id: Uuid,
    ) -> Result<Option<VideoResultStatus>, ImageGatewayError> {
        let row: Option<VideoStatusRow> = sqlx::query_as(
            r#"
                SELECT j.state, w.state, j.last_error_code, j.model, j.billable_units,
                       artifact.artifact_id
                FROM jobs j
                JOIN work_items w ON w.job_id = j.job_id
                JOIN job_auth_attributions attribution
                  ON attribution.job_id = j.job_id
                LEFT JOIN artifacts artifact
                  ON artifact.job_id = j.job_id
                 AND artifact.output_index = 0
                 AND artifact.state = 'ready'
                 AND artifact.media_type = 'video/mp4'
                WHERE j.job_id = $1 AND j.tenant_id = $2
                  AND attribution.project_id = $3
                  AND ($4::UUID IS NULL OR attribution.actor_user_id = $4)
                  AND j.operation = 'video_generation'
                  AND j.economics_contract_version IN (3, 4)
                "#,
        )
        .bind(job_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(actor_user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        row.map(video_result_status).transpose()
    }

    async fn load_video_artifact(
        &self,
        tenant_id: &str,
        artifact_id: Uuid,
    ) -> Result<Option<StoredVideoArtifact>, ImageGatewayError> {
        let row: Option<(
            Uuid,
            String,
            Uuid,
            Uuid,
            Uuid,
            i64,
            i32,
            String,
            String,
            String,
            String,
            i64,
            bool,
        )> = sqlx::query_as(
            r#"
                SELECT artifact.artifact_id, artifact.tenant_id, artifact.job_id,
                       artifact.work_item_id, artifact.execution_id, artifact.lease_epoch,
                       artifact.output_index, artifact.media_type, artifact.storage_backend,
                       artifact.object_key, artifact.sha256_hex, artifact.byte_size,
                       retention.state = 'available'
                         AND retention.expires_at_ms >
                           (EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::BIGINT
                         AS retention_available
                FROM artifacts artifact
                JOIN jobs job ON job.job_id = artifact.job_id
                JOIN job_artifact_retention retention ON retention.job_id = artifact.job_id
                WHERE artifact.artifact_id = $1 AND artifact.tenant_id = $2
                  AND artifact.state = 'ready'
                  AND job.operation = 'video_generation'
                  AND job.economics_contract_version IN (3, 4)
                "#,
        )
        .bind(artifact_id)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        let Some((
            artifact_id,
            tenant_id,
            job_id,
            work_item_id,
            execution_id,
            lease_epoch,
            output_index,
            media_type,
            storage_backend,
            object_key,
            sha256_hex,
            byte_size,
            retention_available,
        )) = row
        else {
            return Ok(None);
        };
        if !retention_available {
            return Err(ImageGatewayError::artifact_expired());
        }
        if media_type != "video/mp4" {
            return Err(ImageGatewayError::artifact_integrity());
        }
        let metadata = ArtifactMetadata {
            identity: ArtifactIdentity {
                artifact_id,
                tenant_id,
                job_id,
                work_item_id,
                execution_id,
                lease_epoch,
                output_index: u32::try_from(output_index)
                    .map_err(|_| ImageGatewayError::artifact_integrity())?,
                media_type: media_type.clone(),
            },
            storage_backend,
            object_key,
            sha256_hex,
            byte_size: u64::try_from(byte_size)
                .map_err(|_| ImageGatewayError::artifact_integrity())?,
        };
        let bytes = match self.artifact_store.get(&metadata).await {
            Ok(bytes) => bytes,
            Err(error) => {
                if generation_result_is_expired(&self.pool, job_id).await? {
                    return Err(ImageGatewayError::artifact_expired());
                }
                return Err(match error {
                    ArtifactReadError::Integrity => ImageGatewayError::artifact_integrity(),
                    ArtifactReadError::Unavailable => {
                        ImageGatewayError::service_unavailable("artifact storage unavailable")
                    }
                });
            }
        };
        Ok(Some(StoredVideoArtifact { media_type, bytes }))
    }

    async fn load_project_video_artifact(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        artifact_id: Uuid,
    ) -> Result<Option<StoredVideoArtifact>, ImageGatewayError> {
        let belongs_to_project: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM artifacts artifact
                JOIN jobs job ON job.job_id = artifact.job_id
                JOIN job_auth_attributions attribution
                  ON attribution.job_id = job.job_id
                WHERE artifact.artifact_id = $1
                  AND artifact.tenant_id = $2
                  AND attribution.project_id = $3
                  AND ($4::UUID IS NULL OR attribution.actor_user_id = $4)
                  AND artifact.state = 'ready'
                  AND job.operation = 'video_generation'
                  AND job.economics_contract_version IN (3, 4)
            )
            "#,
        )
        .bind(artifact_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(actor_user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        if !belongs_to_project {
            return Ok(None);
        }
        self.load_video_artifact(tenant_id, artifact_id).await
    }
}

type VideoStatusRow = (String, String, Option<String>, String, i32, Option<Uuid>);

fn video_result_status(row: VideoStatusRow) -> Result<VideoResultStatus, ImageGatewayError> {
    let (job_state, work_state, error_code, model, billable_units, artifact_id) = row;
    let duration = u8::try_from(billable_units)
        .ok()
        .filter(|duration| *duration > 0)
        .ok_or_else(ImageGatewayError::artifact_integrity)?;
    if job_state == "succeeded" {
        Ok(VideoResultStatus::Succeeded {
            model,
            duration,
            artifact_id: artifact_id.ok_or_else(ImageGatewayError::artifact_integrity)?,
        })
    } else if job_state == "failed" || work_state == "failed" {
        Ok(VideoResultStatus::Failed {
            model,
            duration,
            error_code,
        })
    } else if work_state == "uncertain" {
        Ok(VideoResultStatus::Uncertain { model, duration })
    } else {
        let stage = match work_state.as_str() {
            "ready" => VideoPendingStage::Queued,
            "leased" | "running" => VideoPendingStage::Dispatching,
            _ => VideoPendingStage::Processing,
        };
        Ok(VideoResultStatus::Pending {
            model,
            duration,
            stage,
        })
    }
}

#[derive(sqlx::FromRow)]
struct LockedQuotaJob {
    reservation_id: Uuid,
    quota_tenant_id: String,
    quota_request_id: String,
    quota_job_id: Uuid,
    job_job_id: Uuid,
    requested_units: i32,
    committed_units: i32,
    released_units: i32,
    quota_state: String,
    expires_at_ms: i64,
    job_tenant_id: String,
    job_request_id: String,
    operation: String,
    provider_id: String,
    model: String,
    job_state: String,
    job_requested_units: i32,
    charged_units: i32,
    job_reservation_id: Uuid,
    last_error_code: Option<String>,
}

#[derive(sqlx::FromRow)]
struct LockedWorkAttempt {
    work_item_id: Uuid,
    job_id: Uuid,
    work_state: String,
    work_lease_epoch: i64,
    lease_owner: Option<String>,
    lease_expires_at_ms: Option<i64>,
    work_execution_id: Option<Uuid>,
    attempt_execution_id: Uuid,
    attempt_work_item_id: Uuid,
    attempt_lease_epoch: i64,
    worker_id: String,
    attempt_state: String,
    attempt_error_code: Option<String>,
}

#[derive(sqlx::FromRow)]
struct LockedIdempotency {
    state: String,
    terminal_outcome: Option<String>,
}

async fn lock_tenant_quota(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
) -> Result<(), ImageGatewayError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quota_lock_id(tenant_id))
        .execute(&mut **tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota lock unavailable"))?;
    Ok(())
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ImageGatewayError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(settlement_unavailable)
}

async fn lock_quota_and_job(
    tx: &mut Transaction<'_, Postgres>,
    reservation: &UsageReservation,
) -> Result<LockedQuotaJob, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT
          qr.reservation_id,
          qr.tenant_id AS quota_tenant_id,
          qr.request_id AS quota_request_id,
          qr.job_id AS quota_job_id,
          j.job_id AS job_job_id,
          qr.requested_units,
          qr.committed_units,
          qr.released_units,
          qr.state AS quota_state,
          qr.expires_at_ms,
          j.tenant_id AS job_tenant_id,
          j.request_id AS job_request_id,
          j.operation,
          j.provider_id,
          j.model,
          j.state AS job_state,
          j.requested_units AS job_requested_units,
          j.charged_units,
          j.reservation_id AS job_reservation_id,
          j.last_error_code
        FROM quota_reservations qr
        JOIN jobs j
          ON j.job_id = qr.job_id
         AND j.tenant_id = qr.tenant_id
         AND j.reservation_id = qr.reservation_id
        WHERE qr.reservation_id = $1
        FOR UPDATE OF qr, j
        "#,
    )
    .bind(reservation.reservation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(settlement_unavailable)?
    .ok_or_else(|| ImageGatewayError::internal("reservation not found"))
}

fn validate_reservation_handle(
    locked: &LockedQuotaJob,
    reservation: &UsageReservation,
) -> Result<(), ImageGatewayError> {
    let units_match = u32::try_from(locked.requested_units)
        .is_ok_and(|units| units == reservation.charge.billable_units);
    if locked.reservation_id != reservation.reservation_id
        || locked.quota_tenant_id != reservation.charge.tenant_id
        || locked.job_tenant_id != reservation.charge.tenant_id
        || locked.quota_job_id != reservation.job_id
        || locked.job_job_id != reservation.job_id
        || locked.quota_request_id != reservation.charge.request_id
        || locked.job_request_id != reservation.charge.request_id
        || locked.operation != reservation.charge.operation
        || locked.provider_id != reservation.charge.provider_id
        || locked.model != reservation.charge.model
        || locked.job_reservation_id != reservation.reservation_id
        || locked.job_requested_units != locked.requested_units
        || !units_match
    {
        return Err(ImageGatewayError::internal(
            "reservation handle does not match stored settlement state",
        ));
    }
    Ok(())
}

async fn lock_work_and_attempt(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
) -> Result<LockedWorkAttempt, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT
          w.work_item_id,
          w.job_id,
          w.state AS work_state,
          w.lease_epoch AS work_lease_epoch,
          w.lease_owner,
          w.lease_expires_at_ms,
          w.execution_id AS work_execution_id,
          a.execution_id AS attempt_execution_id,
          a.work_item_id AS attempt_work_item_id,
          a.lease_epoch AS attempt_lease_epoch,
          a.worker_id,
          a.state AS attempt_state,
          a.error_code AS attempt_error_code
        FROM work_items w
        JOIN job_attempts a
          ON a.work_item_id = w.work_item_id
         AND a.execution_id = $2
        WHERE w.work_item_id = $1
        FOR UPDATE OF w, a
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.execution_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(settlement_unavailable)?
    .ok_or_else(|| ImageGatewayError::internal("work lease is stale or invalid"))
}

fn validate_lease_identity(
    locked: &LockedWorkAttempt,
    lease: &WorkLease,
) -> Result<(), ImageGatewayError> {
    if locked.work_item_id != lease.work_item_id
        || locked.job_id != lease.job_id
        || locked.work_lease_epoch != lease.lease_epoch
        || locked.work_execution_id != Some(lease.execution_id)
        || locked.attempt_execution_id != lease.execution_id
        || locked.attempt_work_item_id != lease.work_item_id
        || locked.attempt_lease_epoch != lease.lease_epoch
        || locked.worker_id != lease.worker_id
    {
        return Err(ImageGatewayError::internal(
            "work lease is stale or invalid",
        ));
    }
    Ok(())
}

async fn lock_idempotency(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<Vec<LockedIdempotency>, ImageGatewayError> {
    sqlx::query_as(
        "SELECT state, terminal_outcome FROM idempotency_requests WHERE job_id = $1 FOR UPDATE",
    )
    .bind(job_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(settlement_unavailable)
}

fn is_completed(quota_job: &LockedQuotaJob, work: &LockedWorkAttempt) -> bool {
    quota_job.quota_state == "committed"
        && quota_job.job_state == "succeeded"
        && work.work_state == "succeeded"
}

fn validate_completed_state(
    quota_job: &LockedQuotaJob,
    work: &LockedWorkAttempt,
    idempotency: &[LockedIdempotency],
) -> Result<(), ImageGatewayError> {
    let units = quota_job.requested_units;
    let work_complete = work.lease_owner.is_none()
        && work.lease_expires_at_ms.is_none()
        && work.attempt_state == "succeeded";
    let quota_complete = quota_job.committed_units == units
        && quota_job.released_units == 0
        && quota_job.charged_units == units;
    let idempotency_complete = idempotency.iter().all(|row| {
        row.state == "succeeded" && row.terminal_outcome.as_deref() == Some("succeeded")
    });
    if work_complete && quota_complete && idempotency_complete {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "settlement state is partially completed",
        ))
    }
}

fn validate_active_state(
    quota_job: &LockedQuotaJob,
    work: &LockedWorkAttempt,
    idempotency: &[LockedIdempotency],
    lease: &WorkLease,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let lease_active = work.work_state == "running"
        && work.lease_owner.as_deref() == Some(lease.worker_id.as_str())
        && work
            .lease_expires_at_ms
            .is_some_and(|expires| expires > now)
        && work.attempt_state == "running";
    let quota_active = quota_job.quota_state == "reserved"
        && quota_job.committed_units == 0
        && quota_job.released_units == 0
        && quota_job.expires_at_ms > now;
    let job_active = matches!(
        quota_job.job_state.as_str(),
        "reserved" | "running" | "artifact_ready"
    ) && quota_job.charged_units == 0;
    let idempotency_active = idempotency
        .iter()
        .all(|row| row.state == "accepted" && row.terminal_outcome.is_none());
    if lease_active && quota_active && job_active && idempotency_active {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "settlement state is stale or invalid",
        ))
    }
}

async fn transition_work(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE work_items
        SET state = 'succeeded', lease_owner = NULL, lease_expires_at_ms = NULL,
            updated_at_ms = $6
        WHERE work_item_id = $1 AND job_id = $2 AND lease_epoch = $3
          AND lease_owner = $4 AND execution_id = $5
          AND state = 'running' AND lease_expires_at_ms > $6
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.job_id)
    .bind(lease.lease_epoch)
    .bind(&lease.worker_id)
    .bind(lease.execution_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "work settlement")
}

async fn transition_attempt(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE job_attempts
        SET state = 'succeeded', finished_at_ms = $5, error_code = NULL, updated_at_ms = $5
        WHERE execution_id = $1 AND work_item_id = $2 AND lease_epoch = $3
          AND worker_id = $4 AND state = 'running'
        "#,
    )
    .bind(lease.execution_id)
    .bind(lease.work_item_id)
    .bind(lease.lease_epoch)
    .bind(&lease.worker_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "attempt settlement")
}

async fn transition_idempotency(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    now: i64,
    expected_rows: usize,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE idempotency_requests
        SET state = 'succeeded', terminal_outcome = 'succeeded', updated_at_ms = $2
        WHERE job_id = $1 AND state = 'accepted'
        "#,
    )
    .bind(job_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    if result.rows_affected() == expected_rows as u64 {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "idempotency settlement did not update every row",
        ))
    }
}

async fn insert_usage_event(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO usage_events
          (event_id, tenant_id, job_id, request_id, operation, units, outcome, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, 'charged', $7)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(&locked.quota_tenant_id)
    .bind(locked.quota_job_id)
    .bind(&locked.quota_request_id)
    .bind(&locked.operation)
    .bind(locked.requested_units)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    Ok(())
}

async fn commit_quota(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE quota_reservations
        SET committed_units = requested_units, state = 'committed', updated_at_ms = $2
        WHERE reservation_id = $1 AND tenant_id = $3 AND job_id = $4 AND state = 'reserved'
        "#,
    )
    .bind(locked.reservation_id)
    .bind(now)
    .bind(&locked.quota_tenant_id)
    .bind(locked.quota_job_id)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "quota settlement")
}

async fn succeed_job(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET state = 'succeeded', charged_units = $5, finished_at_ms = $4, updated_at_ms = $4
        WHERE job_id = $1 AND tenant_id = $2 AND reservation_id = $3 AND state = $6
        "#,
    )
    .bind(locked.quota_job_id)
    .bind(&locked.quota_tenant_id)
    .bind(locked.reservation_id)
    .bind(now)
    .bind(locked.requested_units)
    .bind(&locked.job_state)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "job settlement")
}

async fn insert_metering_events(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    now: i64,
) -> Result<(), ImageGatewayError> {
    for event_type in ["quota_committed", "job_succeeded"] {
        sqlx::query(
            r#"
            INSERT INTO metering_events
              (event_id, tenant_id, job_id, reservation_id, request_id, operation,
               event_type, units, outcome, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'succeeded', $9)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&locked.quota_tenant_id)
        .bind(locked.quota_job_id)
        .bind(locked.reservation_id)
        .bind(&locked.quota_request_id)
        .bind(&locked.operation)
        .bind(event_type)
        .bind(locked.requested_units)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(settlement_unavailable)?;
    }
    Ok(())
}

async fn append_succeeded_events(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let semantic_key = format!("work.{}.succeeded", lease.work_item_id);
    let payload = json!({
        "execution_id": lease.execution_id.to_string(),
        "lease_epoch": lease.lease_epoch,
    });
    sqlx::query(
        r#"
        INSERT INTO job_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, 'job.succeeded', $3, $4, $5)
        ON CONFLICT (job_id, semantic_key) DO NOTHING
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(lease.job_id)
    .bind(&semantic_key)
    .bind(&payload)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO outbox_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, 'job.succeeded', $3, $4, $5)
        ON CONFLICT (job_id, semantic_key) DO NOTHING
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(lease.job_id)
    .bind(&semantic_key)
    .bind(&payload)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    Ok(())
}

fn require_one_row(
    result: sqlx::postgres::PgQueryResult,
    transition: &str,
) -> Result<(), ImageGatewayError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(format!(
            "{transition} did not update exactly one row"
        )))
    }
}

fn quota_lock_id(tenant_id: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(b"quota:");
    hasher.update(tenant_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
}

fn settlement_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("execution settlement unavailable")
}
