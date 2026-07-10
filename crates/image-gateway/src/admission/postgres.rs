use async_trait::async_trait;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    AdmissionClaim, AdmissionError, AdmissionStore, AdmissionTicket, AttachJob, AttachedWork,
    ClaimAdmission, WorkLease, WorkOutcome,
};

#[derive(Clone)]
pub struct PostgresAdmissionStore {
    pool: PgPool,
}

impl PostgresAdmissionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AdmissionStore for PostgresAdmissionStore {
    async fn claim(&self, request: ClaimAdmission) -> Result<AdmissionClaim, AdmissionError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;

        if let Some(key_digest) = request.idempotency_key_digest.as_deref() {
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(key_digest)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;

            let existing_session_id: Option<Uuid> = sqlx::query_scalar(
                r#"
                SELECT session_id
                FROM idempotency_requests
                WHERE project_id = $1 AND api_profile = $2
                  AND operation = $3 AND key_digest = $4
                "#,
            )
            .bind(&request.project_id)
            .bind(&request.api_profile)
            .bind(&request.operation)
            .bind(key_digest)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;

            if let Some(session_id) = existing_session_id {
                let deadline_at_ms: i64 = sqlx::query_scalar(
                    "SELECT deadline_at_ms FROM admission_sessions WHERE session_id = $1 FOR UPDATE",
                )
                .bind(session_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
                let (request_hash, job_id, state): (String, Option<Uuid>, String) = sqlx::query_as(
                    r#"
                        SELECT request_hash, job_id, state
                        FROM idempotency_requests
                        WHERE project_id = $1 AND api_profile = $2
                          AND operation = $3 AND key_digest = $4
                        FOR UPDATE
                        "#,
                )
                .bind(&request.project_id)
                .bind(&request.api_profile)
                .bind(&request.operation)
                .bind(key_digest)
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
                if request_hash == request.request_hash
                    && state == "receiving"
                    && deadline_at_ms <= now
                {
                    abort_receiving_session(&mut tx, session_id, now).await?;
                    tx.commit().await.map_err(unavailable)?;
                    return Err(AdmissionError::Expired);
                }
                tx.commit().await.map_err(unavailable)?;
                if request_hash != request.request_hash {
                    return Ok(AdmissionClaim::Conflict { job_id });
                }
                return if state == "receiving" {
                    Ok(AdmissionClaim::InProgress { session_id })
                } else if let Some(job_id) = job_id {
                    Ok(AdmissionClaim::Existing { job_id, state })
                } else {
                    Ok(AdmissionClaim::Conflict { job_id: None })
                };
            }
        }

        if request.deadline_at_ms <= now {
            return Err(AdmissionError::Expired);
        }
        let ticket = AdmissionTicket {
            session_id: Uuid::new_v4(),
            owner_token: Uuid::new_v4(),
            request_hash: request.request_hash.clone(),
        };
        sqlx::query(
            r#"
            INSERT INTO admission_sessions
              (session_id, owner_token, tenant_id, project_id, api_profile, operation,
               request_id, idempotency_key_digest, request_hash, state, deadline_at_ms,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'receiving', $10, $11, $11)
            "#,
        )
        .bind(ticket.session_id)
        .bind(ticket.owner_token)
        .bind(&request.tenant_id)
        .bind(&request.project_id)
        .bind(&request.api_profile)
        .bind(&request.operation)
        .bind(&request.request_id)
        .bind(&request.idempotency_key_digest)
        .bind(&request.request_hash)
        .bind(request.deadline_at_ms)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;

        if let Some(key_digest) = request.idempotency_key_digest {
            sqlx::query(
                r#"
                INSERT INTO idempotency_requests
                  (project_id, api_profile, operation, key_digest, tenant_id, request_hash,
                   session_id, state, created_at_ms, updated_at_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'receiving', $8, $8)
                "#,
            )
            .bind(&request.project_id)
            .bind(&request.api_profile)
            .bind(&request.operation)
            .bind(key_digest)
            .bind(&request.tenant_id)
            .bind(&request.request_hash)
            .bind(ticket.session_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }

        tx.commit().await.map_err(unavailable)?;
        Ok(AdmissionClaim::Owner(ticket))
    }

    async fn attach(&self, request: AttachJob) -> Result<AttachedWork, AdmissionError> {
        if !request.command_json.is_object() {
            return Err(AdmissionError::InvalidCommand);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let session: Option<(String, String, Option<String>, String, i64)> = sqlx::query_as(
            r#"
            SELECT tenant_id, state, idempotency_key_digest, request_hash, deadline_at_ms
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
        let Some((tenant_id, state, key_digest, request_hash, deadline_at_ms)) = session else {
            return Err(AdmissionError::InvalidOwner);
        };
        if state != "receiving" || request_hash != request.ticket.request_hash {
            return Err(AdmissionError::InvalidOwner);
        }
        if deadline_at_ms <= now {
            abort_receiving_session(&mut tx, request.ticket.session_id, now).await?;
            tx.commit().await.map_err(unavailable)?;
            return Err(AdmissionError::Expired);
        }
        if !job_belongs_to_tenant(&mut tx, request.job_id, &tenant_id).await? {
            return Err(AdmissionError::InvalidOwner);
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
        sqlx::query(
            r#"
            INSERT INTO work_items
              (work_item_id, job_id, kind, state, available_at_ms, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, 'ready', $4, $4, $4)
            "#,
        )
        .bind(work_item_id)
        .bind(request.job_id)
        .bind(&request.work_kind)
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
        if key_digest.is_some() {
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
        Ok(AttachedWork {
            work_item_id,
            job_id: request.job_id,
        })
    }

    async fn abort(&self, ticket: &AdmissionTicket) -> Result<(), AdmissionError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let changed = sqlx::query(
            "UPDATE admission_sessions SET state = 'aborted', updated_at_ms = $3 WHERE session_id = $1 AND owner_token = $2 AND state = 'receiving'",
        )
        .bind(ticket.session_id)
        .bind(ticket.owner_token)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if changed != 1 {
            return Err(AdmissionError::InvalidOwner);
        }
        sqlx::query(
            "UPDATE idempotency_requests SET state = 'aborted', updated_at_ms = $2 WHERE session_id = $1 AND state = 'receiving'",
        )
        .bind(ticket.session_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }

    async fn claim_ready(
        &self,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<Option<WorkLease>, AdmissionError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let row: Option<(Uuid, Uuid, i64, String, Value)> = sqlx::query_as(
            r#"
            SELECT w.work_item_id, w.job_id, w.lease_epoch, p.command_schema, p.command_json
            FROM work_items w JOIN job_payloads p ON p.job_id = w.job_id
            WHERE w.state = 'ready' AND w.available_at_ms <= $1
            ORDER BY w.available_at_ms, w.created_at_ms, w.work_item_id
            FOR UPDATE OF w SKIP LOCKED LIMIT 1
            "#,
        )
        .bind(now)
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
        .bind(now + lease_duration_ms.max(1))
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

    async fn start(&self, lease: &WorkLease) -> Result<(), AdmissionError> {
        transition_active(&self.pool, lease, "leased", "running", None).await
    }

    async fn heartbeat(
        &self,
        lease: &WorkLease,
        lease_duration_ms: i64,
    ) -> Result<(), AdmissionError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let changed = sqlx::query(
            r#"
            UPDATE work_items SET lease_expires_at_ms = $5, updated_at_ms = $6
            WHERE work_item_id = $1 AND lease_epoch = $2 AND lease_owner = $3
              AND execution_id = $4 AND state IN ('leased', 'running') AND lease_expires_at_ms > $6
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .bind(lease.execution_id)
        .bind(now + lease_duration_ms.max(1))
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if changed != 1 {
            return Err(AdmissionError::StaleLease);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }

    async fn settle(
        &self,
        lease: &WorkLease,
        outcome: WorkOutcome,
        error_code: Option<&str>,
    ) -> Result<(), AdmissionError> {
        transition_active(&self.pool, lease, "running", outcome.as_str(), error_code).await
    }
}

async fn transition_active(
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
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .bind(lease.execution_id)
        .bind(to)
        .bind(now)
        .bind(from)
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
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .bind(lease.execution_id)
        .bind(to)
        .bind(now)
        .bind(from)
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

async fn append_event_pair(
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

async fn job_belongs_to_tenant(
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

async fn abort_receiving_session(
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

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, AdmissionError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn unavailable(_: impl std::fmt::Display) -> AdmissionError {
    AdmissionError::Unavailable
}
