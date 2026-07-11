use async_trait::async_trait;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

mod operations;

use super::{
    AdmissionClaim, AdmissionError, AdmissionStore, AdmissionTicket, AttachJob, AttachedWork,
    ClaimAdmission, WorkLease, WorkOutcome, attach_operation, validate_attach_request,
};
use operations::{
    abort_receiving_session, append_event_pair, attach_and_start_work, bind_quota_reservation,
    claim_work, database_now, job_matches_admission_identity, persist_inputs, replay_attached_work,
    reserve_schedule_slot, transition_active, unavailable,
};

#[derive(Clone)]
pub struct PostgresAdmissionStore {
    pool: PgPool,
}

#[derive(sqlx::FromRow)]
struct LockedAdmissionSession {
    tenant_id: String,
    operation: String,
    request_id: String,
    state: String,
    idempotency_key_digest: Option<String>,
    request_hash: String,
    deadline_at_ms: i64,
    job_id: Option<Uuid>,
}

#[derive(sqlx::FromRow)]
struct AttachedRunningWork {
    command_schema: String,
    command_json: Value,
    payload_hash: String,
    work_item_id: Uuid,
    work_state: String,
    lease_epoch: i64,
    lease_owner: Option<String>,
    lease_expires_at_ms: Option<i64>,
    execution_id: Option<Uuid>,
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
        let mut replace_aborted_identity = false;

        let recovered: Option<(
            Uuid,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
            Option<Uuid>,
        )> = sqlx::query_as(
            r#"
                SELECT session_id, tenant_id, project_id, api_profile, operation,
                       request_id, request_hash, state, deadline_at_ms, job_id
                FROM admission_sessions
                WHERE owner_token = $1
                FOR UPDATE
                "#,
        )
        .bind(request.owner_token)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if let Some((
            session_id,
            tenant_id,
            project_id,
            api_profile,
            operation,
            request_id,
            request_hash,
            state,
            deadline_at_ms,
            job_id,
        )) = recovered
        {
            if tenant_id != request.tenant_id
                || project_id != request.project_id
                || api_profile != request.api_profile
                || operation != request.operation
                || request_id != request.request_id
                || request_hash != request.request_hash
            {
                return Err(AdmissionError::InvalidOwner);
            }
            if state == "receiving" {
                if job_id.is_some() {
                    return Err(AdmissionError::InvalidOwner);
                }
                if deadline_at_ms <= now {
                    abort_receiving_session(&mut tx, session_id, now).await?;
                    tx.commit().await.map_err(unavailable)?;
                    return Err(AdmissionError::Expired);
                }
                tx.commit().await.map_err(unavailable)?;
                return Ok(AdmissionClaim::Owner(AdmissionTicket {
                    session_id,
                    owner_token: request.owner_token,
                    request_hash,
                }));
            }
        }

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
                let (deadline_at_ms, owner_token, session_request_hash): (i64, Uuid, String) =
                    sqlx::query_as(
                    "SELECT deadline_at_ms, owner_token, request_hash FROM admission_sessions WHERE session_id = $1 FOR UPDATE",
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
                if request_hash == request.request_hash && state == "aborted" && job_id.is_none() {
                    replace_aborted_identity = true;
                } else {
                    tx.commit().await.map_err(unavailable)?;
                    if request_hash != request.request_hash {
                        return Ok(AdmissionClaim::Conflict { job_id });
                    }
                    if state == "receiving"
                        && owner_token == request.owner_token
                        && session_request_hash == request.request_hash
                    {
                        return Ok(AdmissionClaim::Owner(AdmissionTicket {
                            session_id,
                            owner_token,
                            request_hash,
                        }));
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
        }

        if request.deadline_at_ms <= now {
            return Err(AdmissionError::Expired);
        }
        let ticket = AdmissionTicket {
            session_id: Uuid::new_v4(),
            owner_token: request.owner_token,
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
            if replace_aborted_identity {
                let changed = sqlx::query(
                    r#"
                    UPDATE idempotency_requests
                    SET tenant_id = $5, request_hash = $6, session_id = $7,
                        job_id = NULL, state = 'receiving', terminal_outcome = NULL,
                        updated_at_ms = $8
                    WHERE project_id = $1 AND api_profile = $2
                      AND operation = $3 AND key_digest = $4
                      AND state = 'aborted' AND job_id IS NULL
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
                .map_err(unavailable)?
                .rows_affected();
                if changed != 1 {
                    return Err(AdmissionError::Unavailable);
                }
            } else {
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
        }

        tx.commit().await.map_err(unavailable)?;
        Ok(AdmissionClaim::Owner(ticket))
    }

    async fn attach(&self, request: AttachJob) -> Result<AttachedWork, AdmissionError> {
        validate_attach_request(&request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let session: Option<LockedAdmissionSession> = sqlx::query_as(
            r#"
            SELECT tenant_id, operation, request_id, state, idempotency_key_digest,
                   request_hash, deadline_at_ms, job_id
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
        let expected_operation = attach_operation(&request)?;
        if session.request_hash != request.ticket.request_hash
            || session.operation != expected_operation
            || !job_matches_admission_identity(
                &mut tx,
                request.job_id,
                &session.tenant_id,
                expected_operation,
                &session.request_id,
            )
            .await?
        {
            return Err(AdmissionError::InvalidOwner);
        }
        if session.state == "attached" && session.job_id == Some(request.job_id) {
            bind_quota_reservation(&mut tx, &request, &session, now).await?;
            let attached = replay_attached_work(&mut tx, &request).await?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(attached);
        }
        if session.state != "receiving" || session.job_id.is_some() {
            return Err(AdmissionError::InvalidOwner);
        }
        if session.deadline_at_ms <= now {
            abort_receiving_session(&mut tx, request.ticket.session_id, now).await?;
            tx.commit().await.map_err(unavailable)?;
            return Err(AdmissionError::Expired);
        }
        bind_quota_reservation(&mut tx, &request, &session, now).await?;
        persist_inputs(&mut tx, &request, now).await?;

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

        let schedule = reserve_schedule_slot(&mut tx, &request, now).await?;
        let work_item_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO work_items
              (work_item_id, job_id, kind, state, available_at_ms,
               schedule_scope, schedule_weight, schedule_priority, schedule_cost,
               schedule_finish_tag, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, 'ready', $4, $5, $6, $7, $8, $9, $4, $4)
            "#,
        )
        .bind(work_item_id)
        .bind(request.job_id)
        .bind(&request.work_kind)
        .bind(now)
        .bind(&schedule.scope)
        .bind(schedule.weight)
        .bind(schedule.priority)
        .bind(schedule.cost)
        .bind(schedule.finish_tag)
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
        Ok(AttachedWork {
            work_item_id,
            job_id: request.job_id,
        })
    }

    async fn attach_and_start(
        &self,
        request: AttachJob,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<WorkLease, AdmissionError> {
        attach_and_start_work(&self.pool, request, worker_id, lease_duration_ms).await
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
        claim_work(&self.pool, None, worker_id, lease_duration_ms).await
    }

    async fn claim_job(
        &self,
        job_id: Uuid,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<Option<WorkLease>, AdmissionError> {
        claim_work(&self.pool, Some(job_id), worker_id, lease_duration_ms).await
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
