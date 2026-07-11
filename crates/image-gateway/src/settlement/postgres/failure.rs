use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    LockedIdempotency, LockedQuotaJob, LockedWorkAttempt, database_now, lock_idempotency,
    lock_quota_and_job, lock_tenant_quota, lock_work_and_attempt, require_one_row,
    settlement_unavailable, validate_lease_identity, validate_reservation_handle,
};
use crate::{ImageGatewayError, admission::WorkLease, usage::UsageReservation};

pub(super) async fn settle(
    pool: &PgPool,
    lease: &WorkLease,
    reservation: &UsageReservation,
    error_code: &'static str,
) -> Result<(), ImageGatewayError> {
    if error_code.is_empty() {
        return Err(ImageGatewayError::internal(
            "failure settlement requires an error code",
        ));
    }
    let mut tx = pool.begin().await.map_err(settlement_unavailable)?;
    lock_tenant_quota(&mut tx, &reservation.charge.tenant_id).await?;
    let quota_job = lock_quota_and_job(&mut tx, reservation).await?;
    validate_reservation_handle(&quota_job, reservation)?;
    let work_attempt = lock_work_and_attempt(&mut tx, lease).await?;
    validate_lease_identity(&work_attempt, lease)?;
    let idempotency = lock_idempotency(&mut tx, lease.job_id).await?;
    let now = database_now(&mut tx).await?;

    if is_failed(&quota_job, &work_attempt) {
        validate_failed_state(&quota_job, &work_attempt, &idempotency, error_code)?;
        tx.commit().await.map_err(settlement_unavailable)?;
        return Ok(());
    }

    validate_failure_active_state(&quota_job, &work_attempt, &idempotency, lease, now)?;
    fail_work(&mut tx, lease, now).await?;
    fail_attempt(&mut tx, lease, error_code, now).await?;
    fail_idempotency(&mut tx, lease.job_id, now, idempotency.len()).await?;
    release_quota(&mut tx, &quota_job, now).await?;
    fail_job(&mut tx, &quota_job, error_code, now).await?;
    insert_metering_events(&mut tx, &quota_job, error_code, now).await?;
    append_events(&mut tx, lease, error_code, now).await?;

    tx.commit().await.map_err(settlement_unavailable)
}

fn validate_failure_active_state(
    quota_job: &LockedQuotaJob,
    work: &LockedWorkAttempt,
    idempotency: &[LockedIdempotency],
    lease: &WorkLease,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let state_pair = (work.work_state == "leased" && work.attempt_state == "claimed")
        || (work.work_state == "running" && work.attempt_state == "running");
    let lease_active = state_pair
        && work.lease_owner.as_deref() == Some(lease.worker_id.as_str())
        && work
            .lease_expires_at_ms
            .is_some_and(|expires| expires > now);
    let quota_active = matches!(quota_job.quota_state.as_str(), "reserved" | "expired")
        && quota_job.committed_units == 0
        && quota_job.released_units == 0;
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
            "failure settlement state is stale or invalid",
        ))
    }
}

fn is_failed(quota_job: &LockedQuotaJob, work: &LockedWorkAttempt) -> bool {
    quota_job.quota_state == "released"
        && quota_job.job_state == "failed"
        && work.work_state == "failed"
}

fn validate_failed_state(
    quota_job: &LockedQuotaJob,
    work: &LockedWorkAttempt,
    idempotency: &[LockedIdempotency],
    error_code: &str,
) -> Result<(), ImageGatewayError> {
    let work_failed = work.lease_owner.is_none()
        && work.lease_expires_at_ms.is_none()
        && work.attempt_state == "failed"
        && work.attempt_error_code.as_deref() == Some(error_code);
    let quota_released = quota_job.committed_units == 0
        && quota_job.released_units == quota_job.requested_units
        && quota_job.charged_units == 0;
    let job_failed = quota_job.last_error_code.as_deref() == Some(error_code);
    let idempotency_failed = idempotency
        .iter()
        .all(|row| row.state == "failed" && row.terminal_outcome.as_deref() == Some("failed"));
    if work_failed && quota_released && job_failed && idempotency_failed {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "failure settlement state is partially completed or conflicts",
        ))
    }
}

async fn fail_work(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE work_items
        SET state = 'failed', lease_owner = NULL, lease_expires_at_ms = NULL,
            updated_at_ms = $6
        WHERE work_item_id = $1 AND job_id = $2 AND lease_epoch = $3
          AND lease_owner = $4 AND execution_id = $5
          AND state IN ('leased', 'running') AND lease_expires_at_ms > $6
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
    require_one_row(result, "work failure settlement")
}

async fn fail_attempt(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    error_code: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE job_attempts
        SET state = 'failed', finished_at_ms = $5, error_code = $6, updated_at_ms = $5
        WHERE execution_id = $1 AND work_item_id = $2 AND lease_epoch = $3
          AND worker_id = $4 AND state IN ('claimed', 'running')
        "#,
    )
    .bind(lease.execution_id)
    .bind(lease.work_item_id)
    .bind(lease.lease_epoch)
    .bind(&lease.worker_id)
    .bind(now)
    .bind(error_code)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "attempt failure settlement")
}

async fn fail_idempotency(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    now: i64,
    expected_rows: usize,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE idempotency_requests
        SET state = 'failed', terminal_outcome = 'failed', updated_at_ms = $2
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
            "idempotency failure settlement did not update every row",
        ))
    }
}

async fn release_quota(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE quota_reservations
        SET released_units = requested_units, state = 'released', updated_at_ms = $2
        WHERE reservation_id = $1 AND tenant_id = $3 AND job_id = $4
          AND state IN ('reserved', 'expired')
        "#,
    )
    .bind(locked.reservation_id)
    .bind(now)
    .bind(&locked.quota_tenant_id)
    .bind(locked.quota_job_id)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "quota failure settlement")
}

async fn fail_job(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    error_code: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET state = 'failed', charged_units = 0, finished_at_ms = $4,
            updated_at_ms = $4, last_error_code = $5
        WHERE job_id = $1 AND tenant_id = $2 AND reservation_id = $3 AND state = $6
        "#,
    )
    .bind(locked.quota_job_id)
    .bind(&locked.quota_tenant_id)
    .bind(locked.reservation_id)
    .bind(now)
    .bind(error_code)
    .bind(&locked.job_state)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "job failure settlement")
}

async fn insert_metering_events(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    error_code: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    for event_type in ["quota_released", "job_failed"] {
        sqlx::query(
            r#"
            INSERT INTO metering_events
              (event_id, tenant_id, job_id, reservation_id, request_id, operation,
               event_type, units, outcome, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
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
        .bind(error_code)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(settlement_unavailable)?;
    }
    Ok(())
}

async fn append_events(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    error_code: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let semantic_key = format!("work.{}.failed", lease.work_item_id);
    let payload = json!({
        "error_code": error_code,
        "execution_id": lease.execution_id.to_string(),
        "lease_epoch": lease.lease_epoch,
    });
    sqlx::query(
        r#"
        INSERT INTO job_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, 'job.failed', $3, $4, $5)
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
        VALUES ($1, $2, 'job.failed', $3, $4, $5)
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
