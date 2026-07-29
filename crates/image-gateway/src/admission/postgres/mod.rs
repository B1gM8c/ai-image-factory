use async_trait::async_trait;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

use crate::economics::{admit_job_outputs, validate_admitted_job_outputs};
use crate::pricing::admission::{admit_customer_pricing_v4, validate_customer_pricing_v4};

mod operations;

use super::{
    AdmissionClaim, AdmissionContract, AdmissionError, AdmissionStore, AdmissionTicket, AttachJob,
    AttachedWork, ClaimAdmission, WorkLease, WorkOutcome, attach_operation, provider_command_hash,
    validate_attach_request,
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
    api_profile: String,
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
        let payload_hash = provider_command_hash(&request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let session: Option<LockedAdmissionSession> = sqlx::query_as(
            r#"
            SELECT tenant_id, api_profile, operation, request_id, state, idempotency_key_digest,
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
            match request.contract {
                AdmissionContract::LegacyV1 => {
                    let version: Option<i16> = sqlx::query_scalar(
                        "SELECT economics_contract_version FROM jobs WHERE job_id = $1",
                    )
                    .bind(request.job_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(unavailable)?;
                    if version != Some(1) {
                        return Err(AdmissionError::InvalidOwner);
                    }
                }
                AdmissionContract::OutputEconomicsV2 | AdmissionContract::MediaEconomicsV3 => {
                    validate_admitted_job_outputs(&mut tx, &request).await?;
                }
                AdmissionContract::CustomerPricingV4 => {
                    validate_customer_pricing_v4(&mut tx, &request, &session.api_profile).await?;
                }
            }
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
        if matches!(
            request.contract,
            AdmissionContract::OutputEconomicsV2 | AdmissionContract::MediaEconomicsV3
        ) {
            admit_job_outputs(&mut tx, &request, &session.api_profile, now).await?;
        }
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
        .bind(&payload_hash)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;

        bind_provider_route_attribution(&mut tx, &request, now)
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    ?error,
                    job.id = %request.job_id,
                    admission.session.id = %request.ticket.session_id,
                    "provider route attribution failed during admission"
                );
            })?;
        if request.contract == AdmissionContract::CustomerPricingV4 {
            admit_customer_pricing_v4(&mut tx, &request, &session.api_profile)
                .await
                .inspect_err(|error| {
                    tracing::warn!(
                        ?error,
                        job.id = %request.job_id,
                        admission.session.id = %request.ticket.session_id,
                        "customer pricing admission failed"
                    );
                })?;
        }

        let schedule = reserve_schedule_slot(&mut tx, &request, now)
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    ?error,
                    job.id = %request.job_id,
                    admission.session.id = %request.ticket.session_id,
                    "scheduler reservation failed during admission"
                );
            })?;
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
        contract: AdmissionContract,
    ) -> Result<Option<WorkLease>, AdmissionError> {
        claim_work(
            &self.pool,
            None,
            worker_id,
            lease_duration_ms,
            Some(contract),
            None,
            None,
        )
        .await
    }

    async fn claim_ready_for_schema(
        &self,
        worker_id: &str,
        lease_duration_ms: i64,
        contract: AdmissionContract,
        command_schema: &str,
    ) -> Result<Option<WorkLease>, AdmissionError> {
        if command_schema.is_empty() {
            return Err(AdmissionError::InvalidCommand);
        }
        claim_work(
            &self.pool,
            None,
            worker_id,
            lease_duration_ms,
            Some(contract),
            Some(command_schema),
            None,
        )
        .await
    }

    async fn claim_ready_for_profile(
        &self,
        worker_id: &str,
        lease_duration_ms: i64,
        contract: AdmissionContract,
        command_schema: &str,
        execution_profile_id: Uuid,
    ) -> Result<Option<WorkLease>, AdmissionError> {
        if command_schema.is_empty() || execution_profile_id.is_nil() {
            return Err(AdmissionError::InvalidCommand);
        }
        claim_work(
            &self.pool,
            None,
            worker_id,
            lease_duration_ms,
            Some(contract),
            Some(command_schema),
            Some(execution_profile_id),
        )
        .await
    }

    async fn claim_job(
        &self,
        job_id: Uuid,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<Option<WorkLease>, AdmissionError> {
        claim_work(
            &self.pool,
            Some(job_id),
            worker_id,
            lease_duration_ms,
            None,
            None,
            None,
        )
        .await
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

async fn bind_provider_route_attribution(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &AttachJob,
    now: i64,
) -> Result<(), AdmissionError> {
    let binding: Option<(String, String, String, Uuid, i64, String)> = sqlx::query_as(
        r#"
        SELECT binding.provider_id, binding.operation_id, binding.command_schema,
               binding.route_id, binding.route_revision, head.state
        FROM jobs job
        JOIN job_payloads job_command
          ON job_command.job_id = job.job_id
        JOIN admission_sessions session
          ON session.session_id = job_command.admission_session_id
        JOIN job_auth_attributions attribution
          ON attribution.job_id = job.job_id
         AND attribution.tenant_id = job.tenant_id
        JOIN gateway_api_keys api_key
          ON api_key.id = attribution.api_key_id
         AND api_key.project_id = attribution.project_id
         AND api_key.service_account_id = attribution.service_account_id
         AND api_key.tenant_id = attribution.tenant_id
         AND api_key.authz_version = attribution.credential_authz_version
         AND api_key.deleted_at IS NULL
        JOIN gateway_api_key_provider_routes binding
          ON binding.api_key_id = attribution.api_key_id
         AND binding.tenant_id = attribution.tenant_id
         AND binding.project_id = attribution.project_id
         AND binding.service_account_id = attribution.service_account_id
         AND binding.provider_id = job.provider_id
         AND (
           attribution.route_id IS NULL
           OR (
             binding.route_id = attribution.route_id
             AND binding.route_revision = attribution.route_revision
             AND binding.provider_id = attribution.route_provider_id
             AND binding.operation_id = attribution.route_operation_id
             AND binding.command_schema = attribution.route_command_schema
           )
         )
        JOIN provider_route_heads head
          ON head.route_id = binding.route_id
         AND head.provider_id = binding.provider_id
         AND head.operation_id = binding.operation_id
         AND head.command_schema = binding.command_schema
        WHERE job.job_id = $1 AND attribution.auth_kind = 'api_key'
          AND EXISTS (
            SELECT 1
            FROM provider_route_model_mappings mapping
            WHERE mapping.route_id = binding.route_id
              AND mapping.route_revision = binding.route_revision
              AND mapping.provider_id = binding.provider_id
              AND mapping.operation_id = binding.operation_id
              AND mapping.command_schema = binding.command_schema
              AND mapping.api_profile = session.api_profile
              AND mapping.execution_model_id = job.model
          )
        FOR SHARE OF binding, head
        "#,
    )
    .bind(request.job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some((provider_id, operation_id, command_schema, route_id, route_revision, route_state)) =
        binding
    else {
        let api_key_attributed: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
              SELECT 1 FROM job_auth_attributions
              WHERE job_id = $1 AND auth_kind = 'api_key'
            )
            "#,
        )
        .bind(request.job_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)?;
        if api_key_attributed {
            return Err(AdmissionError::InvalidOwner);
        }
        let user_binding: Option<(String, String, String, Uuid, i64, String)> = sqlx::query_as(
            r#"
                SELECT attribution.route_provider_id, attribution.route_operation_id,
                       attribution.route_command_schema, attribution.route_id,
                       attribution.route_revision, head.state
                FROM jobs job
                JOIN job_payloads job_command
                  ON job_command.job_id = job.job_id
                JOIN admission_sessions session
                  ON session.session_id = job_command.admission_session_id
                JOIN job_auth_attributions attribution
                  ON attribution.job_id = job.job_id
                 AND attribution.tenant_id = job.tenant_id
                JOIN provider_route_heads head
                  ON head.route_id = attribution.route_id
                 AND head.current_revision = attribution.route_revision
                 AND head.provider_id = attribution.route_provider_id
                 AND head.operation_id = attribution.route_operation_id
                 AND head.command_schema = attribution.route_command_schema
                WHERE job.job_id = $1 AND attribution.auth_kind = 'user_session'
                  AND attribution.route_provider_id = job.provider_id
                  AND EXISTS (
                    SELECT 1
                    FROM provider_route_model_mappings mapping
                    WHERE mapping.route_id = attribution.route_id
                      AND mapping.route_revision = attribution.route_revision
                      AND mapping.provider_id = attribution.route_provider_id
                      AND mapping.operation_id = attribution.route_operation_id
                      AND mapping.command_schema = attribution.route_command_schema
                      AND mapping.api_profile = session.api_profile
                      AND mapping.execution_model_id = job.model
                  )
                FOR SHARE OF head
                "#,
        )
        .bind(request.job_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unavailable)?;
        let Some((
            provider_id,
            operation_id,
            command_schema,
            route_id,
            route_revision,
            route_state,
        )) = user_binding
        else {
            let user_attributed: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                  SELECT 1 FROM job_auth_attributions
                  WHERE job_id = $1 AND auth_kind = 'user_session'
                )
                "#,
            )
            .bind(request.job_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(unavailable)?;
            return if user_attributed {
                Err(AdmissionError::InvalidOwner)
            } else {
                Ok(())
            };
        };
        if route_state != "enabled" || command_schema != request.command_schema {
            return Err(AdmissionError::InvalidCommand);
        }
        let inserted = sqlx::query(
            r#"
            INSERT INTO job_provider_route_attributions
              (job_id, tenant_id, api_key_id, provider_id, operation_id,
               command_schema, route_id, route_revision, attributed_at_ms)
            SELECT job.job_id, job.tenant_id, NULL,
                   $2, $3, $4, $5, $6, $7
            FROM jobs job
            JOIN job_auth_attributions attribution ON attribution.job_id = job.job_id
            WHERE job.job_id = $1 AND attribution.auth_kind = 'user_session'
            ON CONFLICT (job_id) DO NOTHING
            "#,
        )
        .bind(request.job_id)
        .bind(provider_id)
        .bind(operation_id)
        .bind(command_schema)
        .bind(route_id)
        .bind(route_revision)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if inserted != 1 {
            return Err(AdmissionError::InvalidOwner);
        }
        return Ok(());
    };
    if route_state != "enabled" || command_schema != request.command_schema {
        return Err(AdmissionError::InvalidCommand);
    }
    let inserted = sqlx::query(
        r#"
        INSERT INTO job_provider_route_attributions
          (job_id, tenant_id, api_key_id, provider_id, operation_id,
           command_schema, route_id, route_revision, attributed_at_ms)
        SELECT job.job_id, job.tenant_id, attribution.api_key_id,
               $2, $3, $4, $5, $6, $7
        FROM jobs job
        JOIN job_auth_attributions attribution ON attribution.job_id = job.job_id
        WHERE job.job_id = $1 AND attribution.auth_kind = 'api_key'
        ON CONFLICT (job_id) DO NOTHING
        "#,
    )
    .bind(request.job_id)
    .bind(provider_id)
    .bind(operation_id)
    .bind(command_schema)
    .bind(route_id)
    .bind(route_revision)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    if inserted != 1 {
        return Err(AdmissionError::InvalidOwner);
    }
    Ok(())
}
