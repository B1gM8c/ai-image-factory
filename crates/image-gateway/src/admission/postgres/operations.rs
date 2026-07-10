use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{AttachedRunningWork, LockedAdmissionSession};
use crate::admission::{AdmissionError, AttachJob, WorkLease};

pub(super) async fn attach_and_start_work(
    pool: &PgPool,
    request: AttachJob,
    worker_id: &str,
    lease_duration_ms: i64,
) -> Result<WorkLease, AdmissionError> {
    if !request.command_json.is_object() {
        return Err(AdmissionError::InvalidCommand);
    }
    let mut tx = pool.begin().await.map_err(unavailable)?;
    let now = database_now(&mut tx).await?;
    let session: Option<LockedAdmissionSession> = sqlx::query_as(
        r#"
            SELECT tenant_id, state, idempotency_key_digest, request_hash, deadline_at_ms, job_id
            FROM admission_sessions
            WHERE session_id = $1 AND owner_token = $2
            FOR UPDATE
            "#,
    )
    .bind(request.ticket.session_id)
    .bind(request.ticket.owner_token)
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    let Some(session) = session else {
        return Err(AdmissionError::InvalidOwner);
    };
    if session.request_hash != request.ticket.request_hash
        || !job_belongs_to_tenant(&mut tx, request.job_id, &session.tenant_id).await?
    {
        return Err(AdmissionError::InvalidOwner);
    }

    if session.state == "attached" && session.job_id == Some(request.job_id) {
        let existing: Option<AttachedRunningWork> = sqlx::query_as(
            r#"
            SELECT p.command_schema, p.command_json, p.request_hash AS payload_hash,
                   w.work_item_id, w.state AS work_state, w.lease_epoch, w.lease_owner,
                   w.lease_expires_at_ms, w.execution_id
            FROM job_payloads p
            JOIN work_items w ON w.job_id = p.job_id
            WHERE p.job_id = $1 AND p.admission_session_id = $2
            FOR UPDATE OF w
            "#,
        )
        .bind(request.job_id)
        .bind(request.ticket.session_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some(existing) = existing else {
            return Err(AdmissionError::InvalidOwner);
        };
        if existing.command_schema != request.command_schema
            || existing.command_json != request.command_json
            || existing.payload_hash != request.ticket.request_hash
            || existing.work_state != "running"
            || existing.lease_owner.as_deref() != Some(worker_id)
            || existing
                .lease_expires_at_ms
                .is_none_or(|deadline| deadline <= now)
        {
            return Err(AdmissionError::InvalidOwner);
        }
        let execution_id = existing.execution_id.ok_or(AdmissionError::InvalidOwner)?;
        tx.commit().await.map_err(unavailable)?;
        return Ok(WorkLease {
            work_item_id: existing.work_item_id,
            job_id: request.job_id,
            execution_id,
            lease_epoch: existing.lease_epoch,
            worker_id: worker_id.to_string(),
            command_schema: existing.command_schema,
            command_json: existing.command_json,
        });
    }

    if session.state != "receiving" || session.job_id.is_some() {
        return Err(AdmissionError::InvalidOwner);
    }
    if session.deadline_at_ms <= now {
        abort_receiving_session(&mut tx, request.ticket.session_id, now).await?;
        tx.commit().await.map_err(unavailable)?;
        return Err(AdmissionError::Expired);
    }

    sqlx::query(
        r#"
        INSERT INTO job_payloads
          (job_id, admission_session_id, command_schema, command_json, request_hash, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(request.job_id)
    .bind(request.ticket.session_id)
    .bind(&request.command_schema)
    .bind(&request.command_json)
    .bind(&request.ticket.request_hash)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;

    let work_item_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let lease_epoch = 1_i64;
    let lease_expires_at_ms = now.saturating_add(lease_duration_ms.max(1));
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms, lease_epoch,
           lease_owner, lease_expires_at_ms, execution_id, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 'running', $4, $5, $6, $7, $8, $4, $4)
        "#,
    )
    .bind(work_item_id)
    .bind(request.job_id)
    .bind(&request.work_kind)
    .bind(now)
    .bind(lease_epoch)
    .bind(worker_id)
    .bind(lease_expires_at_ms)
    .bind(execution_id)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts
          (attempt_id, execution_id, work_item_id, lease_epoch, worker_id, state,
           started_at_ms, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, $5, 'running', $6, $6, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(execution_id)
    .bind(work_item_id)
    .bind(lease_epoch)
    .bind(worker_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;

    sqlx::query(
        "UPDATE admission_sessions SET state = 'attached', job_id = $2, updated_at_ms = $3 WHERE session_id = $1",
    )
    .bind(request.ticket.session_id)
    .bind(request.job_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    if session.idempotency_key_digest.is_some() {
        sqlx::query(
            "UPDATE idempotency_requests SET state = 'accepted', job_id = $2, updated_at_ms = $3 WHERE session_id = $1",
        )
        .bind(request.ticket.session_id)
        .bind(request.job_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    }
    append_event_pair(
        &mut tx,
        request.job_id,
        "job.accepted",
        "job.accepted",
        json!({}),
        now,
    )
    .await?;
    tx.commit().await.map_err(unavailable)?;
    Ok(WorkLease {
        work_item_id,
        job_id: request.job_id,
        execution_id,
        lease_epoch,
        worker_id: worker_id.to_string(),
        command_schema: request.command_schema,
        command_json: request.command_json,
    })
}

pub(super) async fn claim_work(
    pool: &PgPool,
    target_job_id: Option<Uuid>,
    worker_id: &str,
    lease_duration_ms: i64,
) -> Result<Option<WorkLease>, AdmissionError> {
    let mut tx = pool.begin().await.map_err(unavailable)?;
    let now = database_now(&mut tx).await?;
    let row: Option<(Uuid, Uuid, i64, String, Value)> = sqlx::query_as(
        r#"
        SELECT w.work_item_id, w.job_id, w.lease_epoch, p.command_schema, p.command_json
        FROM work_items w JOIN job_payloads p ON p.job_id = w.job_id
        WHERE w.state = 'ready' AND w.available_at_ms <= $1
          AND ($2::UUID IS NULL OR w.job_id = $2)
        ORDER BY w.available_at_ms, w.created_at_ms, w.work_item_id
        FOR UPDATE OF w SKIP LOCKED LIMIT 1
        "#,
    )
    .bind(now)
    .bind(target_job_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    let Some((work_item_id, job_id, previous_epoch, command_schema, command_json)) = row else {
        tx.commit().await.map_err(unavailable)?;
        return Ok(None);
    };
    let lease_epoch = previous_epoch + 1;
    let execution_id = Uuid::new_v4();
    sqlx::query(
        r#"
        UPDATE work_items SET state = 'leased', lease_epoch = $2, lease_owner = $3,
          lease_expires_at_ms = $4, execution_id = $5, updated_at_ms = $6
        WHERE work_item_id = $1
        "#,
    )
    .bind(work_item_id)
    .bind(lease_epoch)
    .bind(worker_id)
    .bind(now.saturating_add(lease_duration_ms.max(1)))
    .bind(execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts
          (attempt_id, execution_id, work_item_id, lease_epoch, worker_id, state,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, $5, 'claimed', $6, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(execution_id)
    .bind(work_item_id)
    .bind(lease_epoch)
    .bind(worker_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    tx.commit().await.map_err(unavailable)?;
    Ok(Some(WorkLease {
        work_item_id,
        job_id,
        execution_id,
        lease_epoch,
        worker_id: worker_id.to_string(),
        command_schema,
        command_json,
    }))
}

pub(super) async fn transition_active(
    pool: &PgPool,
    lease: &WorkLease,
    from: &str,
    to: &str,
    error_code: Option<&str>,
) -> Result<(), AdmissionError> {
    let mut tx = pool.begin().await.map_err(unavailable)?;
    let now = database_now(&mut tx).await?;
    let terminal = matches!(to, "succeeded" | "failed" | "uncertain");
    let changed = if terminal {
        sqlx::query(
            r#"
            UPDATE work_items SET state = $5, lease_owner = NULL, lease_expires_at_ms = NULL,
              updated_at_ms = $6
            WHERE work_item_id = $1 AND lease_epoch = $2 AND lease_owner = $3
              AND execution_id = $4 AND state = $7 AND lease_expires_at_ms > $6
              AND job_id = $8
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .bind(lease.execution_id)
        .bind(to)
        .bind(now)
        .bind(from)
        .bind(lease.job_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected()
    } else {
        sqlx::query(
            r#"
            UPDATE work_items SET state = $5, updated_at_ms = $6
            WHERE work_item_id = $1 AND lease_epoch = $2 AND lease_owner = $3
              AND execution_id = $4 AND state = $7 AND lease_expires_at_ms > $6
              AND job_id = $8
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .bind(lease.execution_id)
        .bind(to)
        .bind(now)
        .bind(from)
        .bind(lease.job_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected()
    };
    if changed != 1 {
        return Err(AdmissionError::StaleLease);
    }
    sqlx::query(
        r#"
        UPDATE job_attempts SET state = $2,
          started_at_ms = CASE WHEN $2 = 'running' THEN COALESCE(started_at_ms, $3) ELSE started_at_ms END,
          finished_at_ms = CASE WHEN $2 IN ('succeeded', 'failed', 'uncertain') THEN $3 ELSE NULL END,
          error_code = $4, updated_at_ms = $3
        WHERE execution_id = $1 AND lease_epoch = $5
        "#,
    )
    .bind(lease.execution_id).bind(to).bind(now).bind(error_code).bind(lease.lease_epoch)
    .execute(&mut *tx).await.map_err(unavailable)?;
    if terminal {
        sqlx::query(
            "UPDATE idempotency_requests SET state = $2, terminal_outcome = $2, updated_at_ms = $3 WHERE job_id = $1",
        )
        .bind(lease.job_id).bind(to).bind(now)
        .execute(&mut *tx).await.map_err(unavailable)?;
        let semantic_key = format!("work.{}.{}", lease.work_item_id, to);
        append_event_pair(
            &mut tx,
            lease.job_id,
            &format!("job.{to}"),
            &semantic_key,
            json!({"execution_id": lease.execution_id.to_string(), "lease_epoch": lease.lease_epoch}),
            now,
        )
        .await?;
    }
    tx.commit().await.map_err(unavailable)?;
    Ok(())
}

pub(super) async fn append_event_pair(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    event_type: &str,
    semantic_key: &str,
    payload: Value,
    now: i64,
) -> Result<(), AdmissionError> {
    sqlx::query(
        "INSERT INTO job_events (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (job_id, semantic_key) DO NOTHING",
    )
    .bind(Uuid::new_v4()).bind(job_id).bind(event_type).bind(semantic_key)
    .bind(&payload).bind(now).execute(&mut **tx).await.map_err(unavailable)?;
    sqlx::query(
        "INSERT INTO outbox_events (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (job_id, semantic_key) DO NOTHING",
    )
    .bind(Uuid::new_v4()).bind(job_id).bind(event_type).bind(semantic_key)
    .bind(&payload).bind(now).execute(&mut **tx).await.map_err(unavailable)?;
    Ok(())
}

pub(super) async fn job_belongs_to_tenant(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    tenant_id: &str,
) -> Result<bool, AdmissionError> {
    let found: Option<String> =
        sqlx::query_scalar("SELECT tenant_id FROM jobs WHERE job_id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unavailable)?;
    Ok(found.as_deref() == Some(tenant_id))
}

pub(super) async fn abort_receiving_session(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    now: i64,
) -> Result<(), AdmissionError> {
    sqlx::query(
        "UPDATE admission_sessions SET state = 'aborted', updated_at_ms = $2 WHERE session_id = $1 AND state = 'receiving'",
    )
    .bind(session_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    sqlx::query(
        "UPDATE idempotency_requests SET state = 'aborted', updated_at_ms = $2 WHERE session_id = $1 AND state = 'receiving'",
    )
    .bind(session_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub(super) async fn database_now(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<i64, AdmissionError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

pub(super) fn unavailable(_: impl std::fmt::Display) -> AdmissionError {
    AdmissionError::Unavailable
}
