use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{ReconciliationOutcome, ReconciliationStore};
use crate::ImageGatewayError;

const SERIALIZABLE_RETRY_ATTEMPTS: usize = 3;

#[derive(Clone)]
pub struct PostgresReconciliationStore {
    pool: PgPool,
}

impl PostgresReconciliationStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct ExpiredWork {
    work_item_id: Uuid,
    job_id: Uuid,
    execution_id: Uuid,
    lease_epoch: i64,
    work_state: String,
    attempt_state: String,
}

#[derive(sqlx::FromRow)]
struct OrphanCandidate {
    reservation_id: Uuid,
    tenant_id: String,
    session_id: Uuid,
}

#[derive(sqlx::FromRow)]
struct LockedOrphanSession {
    tenant_id: String,
    request_id: String,
    operation: String,
    state: String,
    job_id: Option<Uuid>,
    created_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct LockedOrphanReservation {
    reservation_id: Uuid,
    admission_session_id: Option<Uuid>,
    tenant_id: String,
    request_id: String,
    requested_units: i32,
    released_units: i32,
    quota_state: String,
    quota_created_at_ms: i64,
    job_id: Uuid,
    operation: String,
    job_state: String,
    charged_units: i32,
    job_created_at_ms: i64,
}

#[async_trait]
impl ReconciliationStore for PostgresReconciliationStore {
    async fn reconcile_expired_work(
        &self,
        limit: u32,
    ) -> Result<ReconciliationOutcome, ImageGatewayError> {
        if limit == 0 {
            return Ok(ReconciliationOutcome::default());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(reconciliation_unavailable)?;
        let expired: Vec<ExpiredWork> = sqlx::query_as(
            r#"
            SELECT w.work_item_id, w.job_id, w.execution_id, w.lease_epoch,
                   w.state AS work_state, a.state AS attempt_state
            FROM work_items w
            JOIN job_attempts a
              ON a.work_item_id = w.work_item_id
             AND a.execution_id = w.execution_id
             AND a.lease_epoch = w.lease_epoch
            WHERE w.state IN ('leased', 'running')
              AND w.lease_expires_at_ms <= floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            ORDER BY w.lease_expires_at_ms, w.work_item_id
            FOR UPDATE OF w, a SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await
        .map_err(reconciliation_unavailable)?;
        let now: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(reconciliation_unavailable)?;

        let mut outcome = ReconciliationOutcome::default();
        for work in expired {
            if work.work_state == "leased" && work.attempt_state == "claimed" {
                requeue_unstarted(&mut tx, &work, now).await?;
                outcome.requeued += 1;
            } else if work.work_state == "running" && work.attempt_state == "running" {
                mark_running_uncertain(&mut tx, &work, now).await?;
                outcome.uncertain += 1;
            } else {
                return Err(ImageGatewayError::internal(
                    "expired work and attempt states are inconsistent",
                ));
            }
        }
        tx.commit().await.map_err(reconciliation_unavailable)?;
        Ok(outcome)
    }

    async fn reconcile_orphan_reservations(
        &self,
        grace_ms: u64,
        limit: u32,
    ) -> Result<ReconciliationOutcome, ImageGatewayError> {
        if limit == 0 {
            return Ok(ReconciliationOutcome::default());
        }
        let grace_ms = i64::try_from(grace_ms).unwrap_or(i64::MAX);
        let now = database_now_pool(&self.pool).await?;
        let cutoff = now.saturating_sub(grace_ms);
        let candidates = find_orphan_candidates(&self.pool, cutoff, limit).await?;
        let mut outcome = ReconciliationOutcome::default();
        for candidate in candidates {
            if reconcile_orphan_candidate(&self.pool, &candidate, grace_ms).await? {
                outcome.orphaned += 1;
            }
        }
        Ok(outcome)
    }

    async fn claim_input_cleanup(
        &self,
        owner: &str,
        grace_ms: u64,
        lease_ms: u64,
        limit: u32,
    ) -> Result<Vec<Uuid>, ImageGatewayError> {
        if owner.is_empty() || limit == 0 || lease_ms == 0 {
            return Ok(Vec::new());
        }
        let grace_ms = i64::try_from(grace_ms).unwrap_or(i64::MAX);
        let lease_ms = i64::try_from(lease_ms).unwrap_or(i64::MAX);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(reconciliation_unavailable)?;
        let now = database_now(&mut tx).await?;
        let cutoff = now.saturating_sub(grace_ms);
        let lease_expires_at_ms = now.saturating_add(lease_ms);
        abort_expired_unreserved_input_sessions(&mut tx, cutoff, now, limit).await?;
        let sessions: Vec<Uuid> = sqlx::query_scalar(
            r#"
            WITH candidates AS (
              SELECT s.session_id
              FROM admission_sessions s
              WHERE s.operation IN ('edit', 'video_generation')
                AND (
                  s.input_cleanup_state = 'pending'
                  OR (
                    s.input_cleanup_state = 'leased'
                    AND s.input_cleanup_lease_expires_at_ms <= $1
                  )
                )
                AND (
                  (
                    s.job_id IS NULL
                    AND s.state = 'aborted'
                    AND s.updated_at_ms <= $2
                  )
                  OR EXISTS (
                    SELECT 1
                    FROM jobs j
                    WHERE j.job_id = s.job_id
                      AND j.tenant_id = s.tenant_id
                      AND s.state = 'attached'
                      AND j.state IN ('succeeded', 'failed')
                      AND j.finished_at_ms <= $2
                  )
                )
              ORDER BY s.updated_at_ms, s.session_id
              FOR UPDATE OF s SKIP LOCKED
              LIMIT $3
            )
            UPDATE admission_sessions s
            SET input_cleanup_state = 'leased',
                input_cleanup_owner = $4,
                input_cleanup_lease_expires_at_ms = $5,
                input_cleanup_completed_at_ms = NULL
            FROM candidates c
            WHERE s.session_id = c.session_id
            RETURNING s.session_id
            "#,
        )
        .bind(now)
        .bind(cutoff)
        .bind(i64::from(limit))
        .bind(owner)
        .bind(lease_expires_at_ms)
        .fetch_all(&mut *tx)
        .await
        .map_err(reconciliation_unavailable)?;
        tx.commit().await.map_err(reconciliation_unavailable)?;
        Ok(sessions)
    }

    async fn complete_input_cleanup(
        &self,
        owner: &str,
        session_id: Uuid,
    ) -> Result<(), ImageGatewayError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(reconciliation_unavailable)?;
        let now = database_now(&mut tx).await?;
        let updated = sqlx::query(
            r#"
            UPDATE admission_sessions
            SET input_cleanup_state = 'complete', input_cleanup_owner = NULL,
                input_cleanup_lease_expires_at_ms = NULL,
                input_cleanup_completed_at_ms = $3
            WHERE session_id = $1 AND input_cleanup_state = 'leased'
              AND input_cleanup_owner = $2
            "#,
        )
        .bind(session_id)
        .bind(owner)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(reconciliation_unavailable)?;
        if updated.rows_affected() == 0 {
            let state: Option<String> = sqlx::query_scalar(
                "SELECT input_cleanup_state FROM admission_sessions WHERE session_id = $1 FOR UPDATE",
            )
            .bind(session_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(reconciliation_unavailable)?;
            if state.as_deref() != Some("complete") {
                return Err(ImageGatewayError::service_unavailable(
                    "input cleanup lease is unavailable",
                ));
            }
        }
        tx.commit().await.map_err(reconciliation_unavailable)?;
        Ok(())
    }
}

async fn abort_expired_unreserved_input_sessions(
    tx: &mut Transaction<'_, Postgres>,
    cutoff: i64,
    now: i64,
    limit: u32,
) -> Result<(), ImageGatewayError> {
    let sessions: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT s.session_id
        FROM admission_sessions s
        WHERE s.operation IN ('edit', 'video_generation')
          AND s.state = 'receiving' AND s.job_id IS NULL
          AND s.deadline_at_ms <= $1
          AND NOT EXISTS (
            SELECT 1
            FROM quota_reservations qr
            JOIN jobs j
              ON j.job_id = qr.job_id
             AND j.tenant_id = qr.tenant_id
             AND j.reservation_id = qr.reservation_id
            WHERE qr.state = 'reserved' AND j.state = 'reserved'
              AND (
                qr.admission_session_id = s.session_id
                OR (
                  qr.admission_session_id IS NULL
                  AND qr.tenant_id = s.tenant_id
                  AND qr.request_id = s.request_id
                  AND j.operation = s.operation
                )
              )
          )
        ORDER BY s.deadline_at_ms, s.session_id
        FOR UPDATE OF s SKIP LOCKED
        LIMIT $2
        "#,
    )
    .bind(cutoff)
    .bind(i64::from(limit))
    .fetch_all(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)?;
    for session_id in sessions {
        sqlx::query(
            "UPDATE admission_sessions SET state = 'aborted', updated_at_ms = $2 WHERE session_id = $1 AND state = 'receiving' AND job_id IS NULL",
        )
        .bind(session_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?;
        sqlx::query(
            "UPDATE idempotency_requests SET state = 'aborted', terminal_outcome = NULL, updated_at_ms = $2 WHERE session_id = $1 AND state = 'receiving'",
        )
        .bind(session_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?;
    }
    Ok(())
}

async fn find_orphan_candidates(
    pool: &PgPool,
    cutoff: i64,
    limit: u32,
) -> Result<Vec<OrphanCandidate>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT qr.reservation_id, qr.tenant_id,
               COALESCE(qr.admission_session_id, (
                 SELECT s.session_id
                 FROM admission_sessions s
                 WHERE qr.admission_session_id IS NULL
                   AND s.tenant_id = qr.tenant_id
                   AND s.request_id = qr.request_id
                   AND s.operation = j.operation
                   AND s.state IN ('receiving', 'aborted')
                   AND s.job_id IS NULL
                   AND s.created_at_ms <= $1
                 ORDER BY s.created_at_ms, s.session_id
                 LIMIT 1
               )) AS session_id
        FROM quota_reservations qr
        JOIN jobs j
          ON j.job_id = qr.job_id
         AND j.tenant_id = qr.tenant_id
         AND j.reservation_id = qr.reservation_id
        WHERE qr.state = 'reserved'
          AND j.state = 'reserved'
          AND qr.created_at_ms <= $1
          AND j.created_at_ms <= $1
          AND NOT EXISTS (SELECT 1 FROM work_items w WHERE w.job_id = j.job_id)
          AND (
            (
              qr.admission_session_id IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM admission_sessions s
                WHERE s.session_id = qr.admission_session_id
                  AND s.tenant_id = qr.tenant_id
                  AND s.request_id = qr.request_id
                  AND s.operation = j.operation
                  AND s.state IN ('receiving', 'aborted')
                  AND s.job_id IS NULL
                  AND s.created_at_ms <= $1
              )
            )
            OR (
              qr.admission_session_id IS NULL
              AND (
                SELECT COUNT(*)
                FROM admission_sessions s
                WHERE s.tenant_id = qr.tenant_id
                  AND s.request_id = qr.request_id
                  AND s.operation = j.operation
                  AND s.state IN ('receiving', 'aborted')
                  AND s.job_id IS NULL
                  AND s.created_at_ms <= $1
              ) = 1
              AND (
                SELECT COUNT(*)
                FROM quota_reservations other_qr
                JOIN jobs other_j
                  ON other_j.job_id = other_qr.job_id
                 AND other_j.tenant_id = other_qr.tenant_id
                 AND other_j.reservation_id = other_qr.reservation_id
                WHERE other_qr.admission_session_id IS NULL
                  AND other_qr.tenant_id = qr.tenant_id
                  AND other_qr.request_id = qr.request_id
                  AND other_j.operation = j.operation
                  AND other_qr.state = 'reserved'
                  AND other_j.state = 'reserved'
                  AND other_qr.created_at_ms <= $1
                  AND other_j.created_at_ms <= $1
                  AND NOT EXISTS (
                    SELECT 1 FROM work_items other_w WHERE other_w.job_id = other_j.job_id
                  )
              ) = 1
            )
          )
        ORDER BY qr.created_at_ms, qr.reservation_id
        LIMIT $2
        "#,
    )
    .bind(cutoff)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .map_err(reconciliation_unavailable)
}

async fn reconcile_orphan_candidate(
    pool: &PgPool,
    candidate: &OrphanCandidate,
    grace_ms: i64,
) -> Result<bool, ImageGatewayError> {
    for attempt in 0..SERIALIZABLE_RETRY_ATTEMPTS {
        match reconcile_orphan_candidate_once(pool, candidate, grace_ms).await {
            Err(error)
                if error.error_code() == Some("service_unavailable")
                    && attempt + 1 < SERIALIZABLE_RETRY_ATTEMPTS =>
            {
                continue;
            }
            result => return result,
        }
    }
    Err(ImageGatewayError::service_unavailable(
        "reconciliation retry budget exhausted",
    ))
}

async fn reconcile_orphan_candidate_once(
    pool: &PgPool,
    candidate: &OrphanCandidate,
    grace_ms: i64,
) -> Result<bool, ImageGatewayError> {
    let mut tx = pool.begin().await.map_err(reconciliation_unavailable)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .execute(&mut *tx)
        .await
        .map_err(reconciliation_unavailable)?;
    lock_tenant_quota(&mut tx, &candidate.tenant_id).await?;
    let now = database_now(&mut tx).await?;
    let cutoff = now.saturating_sub(grace_ms);

    let Some(session) = lock_orphan_session(&mut tx, candidate.session_id).await? else {
        tx.commit().await.map_err(reconciliation_unavailable)?;
        return Ok(false);
    };
    if session.tenant_id != candidate.tenant_id
        || !matches!(session.state.as_str(), "receiving" | "aborted")
        || session.job_id.is_some()
        || session.created_at_ms > cutoff
    {
        tx.commit().await.map_err(reconciliation_unavailable)?;
        return Ok(false);
    }

    let Some(reservation) = lock_orphan_reservation(&mut tx, candidate.reservation_id).await?
    else {
        tx.commit().await.map_err(reconciliation_unavailable)?;
        return Ok(false);
    };
    if !orphan_still_reclaimable(&mut tx, candidate, &session, &reservation, cutoff).await? {
        tx.commit().await.map_err(reconciliation_unavailable)?;
        return Ok(false);
    }
    lock_active_idempotency(&mut tx, candidate.session_id).await?;
    terminalize_orphan(&mut tx, candidate.session_id, &reservation, now).await?;
    tx.commit().await.map_err(reconciliation_unavailable)?;
    Ok(true)
}

async fn lock_tenant_quota(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
) -> Result<(), ImageGatewayError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quota_lock_id(tenant_id))
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?;
    Ok(())
}

async fn lock_orphan_session(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
) -> Result<Option<LockedOrphanSession>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT tenant_id, request_id, operation, state, job_id, created_at_ms
        FROM admission_sessions
        WHERE session_id = $1
        FOR UPDATE
        "#,
    )
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)
}

async fn lock_orphan_reservation(
    tx: &mut Transaction<'_, Postgres>,
    reservation_id: Uuid,
) -> Result<Option<LockedOrphanReservation>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT qr.reservation_id, qr.admission_session_id, qr.tenant_id, qr.request_id,
               qr.requested_units, qr.released_units, qr.state AS quota_state,
               qr.created_at_ms AS quota_created_at_ms,
               j.job_id, j.operation, j.state AS job_state,
               j.charged_units, j.created_at_ms AS job_created_at_ms
        FROM quota_reservations qr
        JOIN jobs j
          ON j.job_id = qr.job_id
         AND j.tenant_id = qr.tenant_id
         AND j.reservation_id = qr.reservation_id
        WHERE qr.reservation_id = $1
        FOR UPDATE OF qr, j
        "#,
    )
    .bind(reservation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)
}

async fn orphan_still_reclaimable(
    tx: &mut Transaction<'_, Postgres>,
    candidate: &OrphanCandidate,
    session: &LockedOrphanSession,
    reservation: &LockedOrphanReservation,
    cutoff: i64,
) -> Result<bool, ImageGatewayError> {
    let has_work: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM work_items WHERE job_id = $1)")
            .bind(reservation.job_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(reconciliation_unavailable)?;
    let matching_sessions: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM admission_sessions s
        WHERE s.tenant_id = $1 AND s.request_id = $2 AND s.operation = $3
          AND s.state IN ('receiving', 'aborted')
          AND s.job_id IS NULL AND s.created_at_ms <= $4
        "#,
    )
    .bind(&session.tenant_id)
    .bind(&session.request_id)
    .bind(&session.operation)
    .bind(cutoff)
    .fetch_one(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)?;
    let matching_reservations: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM quota_reservations qr
        JOIN jobs j
          ON j.job_id = qr.job_id
         AND j.tenant_id = qr.tenant_id
         AND j.reservation_id = qr.reservation_id
        WHERE qr.tenant_id = $1 AND qr.request_id = $2 AND j.operation = $3
          AND qr.state = 'reserved' AND j.state = 'reserved'
          AND qr.created_at_ms <= $4 AND j.created_at_ms <= $4
          AND NOT EXISTS (SELECT 1 FROM work_items w WHERE w.job_id = j.job_id)
        "#,
    )
    .bind(&session.tenant_id)
    .bind(&session.request_id)
    .bind(&session.operation)
    .bind(cutoff)
    .fetch_one(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)?;
    let binding_matches = match reservation.admission_session_id {
        Some(session_id) => session_id == candidate.session_id,
        None => matching_sessions == 1 && matching_reservations == 1,
    };
    Ok(binding_matches
        && reservation.reservation_id == candidate.reservation_id
        && reservation.tenant_id == candidate.tenant_id
        && reservation.tenant_id == session.tenant_id
        && reservation.request_id == session.request_id
        && reservation.operation == session.operation
        && reservation.quota_state == "reserved"
        && reservation.job_state == "reserved"
        && reservation.released_units == 0
        && reservation.charged_units == 0
        && reservation.quota_created_at_ms <= cutoff
        && reservation.job_created_at_ms <= cutoff
        && !has_work)
}

async fn lock_active_idempotency(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
) -> Result<(), ImageGatewayError> {
    let states: Vec<String> = sqlx::query_scalar(
        "SELECT state FROM idempotency_requests WHERE session_id = $1 FOR UPDATE",
    )
    .bind(session_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)?;
    if states
        .iter()
        .all(|state| matches!(state.as_str(), "receiving" | "aborted"))
    {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "orphan idempotency state is inconsistent",
        ))
    }
}

async fn terminalize_orphan(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    reservation: &LockedOrphanReservation,
    now: i64,
) -> Result<(), ImageGatewayError> {
    require_one(
        sqlx::query(
            r#"
            UPDATE admission_sessions
            SET state = 'aborted', updated_at_ms = $2
            WHERE session_id = $1 AND state IN ('receiving', 'aborted') AND job_id IS NULL
            "#,
        )
        .bind(session_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?,
        "orphan admission termination",
    )?;
    sqlx::query(
        r#"
        UPDATE idempotency_requests
        SET state = 'aborted', terminal_outcome = NULL, updated_at_ms = $2
        WHERE session_id = $1 AND state = 'receiving'
        "#,
    )
    .bind(session_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE quota_reservations
            SET released_units = requested_units, state = 'released', updated_at_ms = $2
            WHERE reservation_id = $1 AND tenant_id = $3 AND job_id = $4
              AND state = 'reserved' AND released_units = 0
            "#,
        )
        .bind(reservation.reservation_id)
        .bind(now)
        .bind(&reservation.tenant_id)
        .bind(reservation.job_id)
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?,
        "orphan quota release",
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE jobs
            SET state = 'failed', finished_at_ms = $2, updated_at_ms = $2,
                last_error_code = 'orphaned_admission'
            WHERE job_id = $1 AND tenant_id = $3 AND reservation_id = $4
              AND state = 'reserved' AND charged_units = 0
            "#,
        )
        .bind(reservation.job_id)
        .bind(now)
        .bind(&reservation.tenant_id)
        .bind(reservation.reservation_id)
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?,
        "orphan job termination",
    )?;
    insert_orphan_metering_events(tx, reservation, now).await?;
    append_orphan_event_pair(tx, reservation, now).await
}

async fn insert_orphan_metering_events(
    tx: &mut Transaction<'_, Postgres>,
    reservation: &LockedOrphanReservation,
    now: i64,
) -> Result<(), ImageGatewayError> {
    for event_type in ["quota_released", "job_failed"] {
        sqlx::query(
            r#"
            INSERT INTO metering_events
              (event_id, tenant_id, job_id, reservation_id, request_id, operation,
               event_type, units, outcome, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'orphaned_admission', $9)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&reservation.tenant_id)
        .bind(reservation.job_id)
        .bind(reservation.reservation_id)
        .bind(&reservation.request_id)
        .bind(&reservation.operation)
        .bind(event_type)
        .bind(reservation.requested_units)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?;
    }
    Ok(())
}

async fn append_orphan_event_pair(
    tx: &mut Transaction<'_, Postgres>,
    reservation: &LockedOrphanReservation,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let payload = json!({
        "reservation_id": reservation.reservation_id.to_string(),
        "reason": "orphaned_admission",
    });
    sqlx::query(
        r#"
        INSERT INTO job_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, 'job.failed', 'job.orphaned_reservation', $3, $4)
        ON CONFLICT (job_id, semantic_key) DO NOTHING
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(reservation.job_id)
    .bind(&payload)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO outbox_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, 'job.failed', 'job.orphaned_reservation', $3, $4)
        ON CONFLICT (job_id, semantic_key) DO NOTHING
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(reservation.job_id)
    .bind(&payload)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)?;
    Ok(())
}

async fn database_now_pool(pool: &PgPool) -> Result<i64, ImageGatewayError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(reconciliation_unavailable)
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ImageGatewayError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)
}

fn quota_lock_id(tenant_id: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(b"quota:");
    hasher.update(tenant_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
}

async fn requeue_unstarted(
    tx: &mut Transaction<'_, Postgres>,
    work: &ExpiredWork,
    now: i64,
) -> Result<(), ImageGatewayError> {
    require_one(
        sqlx::query(
            r#"
            UPDATE work_items
            SET state = 'ready', available_at_ms = $5, lease_owner = NULL,
                lease_expires_at_ms = NULL, execution_id = NULL, updated_at_ms = $5
            WHERE work_item_id = $1 AND job_id = $2 AND execution_id = $3
              AND lease_epoch = $4 AND state = 'leased'
            "#,
        )
        .bind(work.work_item_id)
        .bind(work.job_id)
        .bind(work.execution_id)
        .bind(work.lease_epoch)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?,
        "unstarted work requeue",
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE job_attempts
            SET state = 'failed', finished_at_ms = $3,
                error_code = 'lease_expired_before_start', updated_at_ms = $3
            WHERE execution_id = $1 AND lease_epoch = $2 AND state = 'claimed'
            "#,
        )
        .bind(work.execution_id)
        .bind(work.lease_epoch)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?,
        "unstarted attempt expiry",
    )?;
    append_event_pair(tx, work, "work.requeued", now).await
}

async fn mark_running_uncertain(
    tx: &mut Transaction<'_, Postgres>,
    work: &ExpiredWork,
    now: i64,
) -> Result<(), ImageGatewayError> {
    require_one(
        sqlx::query(
            r#"
            UPDATE work_items
            SET state = 'uncertain', lease_owner = NULL, lease_expires_at_ms = NULL,
                updated_at_ms = $5
            WHERE work_item_id = $1 AND job_id = $2 AND execution_id = $3
              AND lease_epoch = $4 AND state = 'running'
            "#,
        )
        .bind(work.work_item_id)
        .bind(work.job_id)
        .bind(work.execution_id)
        .bind(work.lease_epoch)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?,
        "running work uncertainty",
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE job_attempts
            SET state = 'uncertain', finished_at_ms = $3,
                error_code = 'lease_expired_after_start', updated_at_ms = $3
            WHERE execution_id = $1 AND lease_epoch = $2 AND state = 'running'
            "#,
        )
        .bind(work.execution_id)
        .bind(work.lease_epoch)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(reconciliation_unavailable)?,
        "running attempt uncertainty",
    )?;
    sqlx::query(
        r#"
        UPDATE idempotency_requests
        SET state = 'uncertain', terminal_outcome = 'uncertain', updated_at_ms = $2
        WHERE job_id = $1 AND state = 'accepted'
        "#,
    )
    .bind(work.job_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)?;
    append_event_pair(tx, work, "job.uncertain", now).await
}

async fn append_event_pair(
    tx: &mut Transaction<'_, Postgres>,
    work: &ExpiredWork,
    event_type: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let semantic_key = format!(
        "work.{}.lease.{}.expired",
        work.work_item_id, work.lease_epoch
    );
    let payload = json!({
        "execution_id": work.execution_id.to_string(),
        "lease_epoch": work.lease_epoch,
        "previous_state": work.work_state,
    });
    sqlx::query(
        r#"
        INSERT INTO job_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (job_id, semantic_key) DO NOTHING
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(work.job_id)
    .bind(event_type)
    .bind(&semantic_key)
    .bind(&payload)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO outbox_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (job_id, semantic_key) DO NOTHING
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(work.job_id)
    .bind(event_type)
    .bind(&semantic_key)
    .bind(&payload)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(reconciliation_unavailable)?;
    Ok(())
}

fn require_one(
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

fn reconciliation_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("reconciliation unavailable")
}
