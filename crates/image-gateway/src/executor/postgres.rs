use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    ExecutorArtifactAuthority, ExecutorArtifactAuthorityStore, ExecutorClaimScope,
    ExecutorLaunchContext, ExecutorLaunchContextStore, ExecutorResultManifest,
    ExecutorRunnerObservation, ExecutorSubmissionError, ExecutorSubmissionLease,
    ExecutorSubmissionOutcome, ExecutorSubmissionStore, PreparedExecutorSubmission,
};
use crate::admission::WorkLease;

const MAX_RECONCILE_BATCH: u32 = 1_000;
const EXECUTOR_LEASE_EXPIRED: &str = "executor_lease_expired";
const EXECUTOR_START_ABANDONED: &str = "executor_start_abandoned";

mod validation;

use validation::{
    command_hash, command_output_count, distinct_execution_id, validate_artifact_authority,
    validate_claim_scope, validate_executor_lease, validate_lease_duration, validate_outcome,
    validate_owner, validate_owner_and_duration, validate_work_lease,
};

#[derive(Clone)]
pub struct PostgresExecutorSubmissionStore {
    pool: PgPool,
}

impl PostgresExecutorSubmissionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct DurableCommandRow {
    requested_units: i32,
    economics_contract_version: i16,
    tenant_id: String,
    provider_id: String,
    model: String,
    command_schema: String,
    command_json: Value,
}

#[derive(sqlx::FromRow)]
struct ExistingIdentityRow {
    output_id: Uuid,
    output_index: i32,
    submission_id: Option<Uuid>,
    executor_execution_id: Option<Uuid>,
    tenant_id: Option<String>,
    provider_id: Option<String>,
    model: Option<String>,
    work_item_id: Option<Uuid>,
    command_schema: Option<String>,
    command_hash: Option<String>,
    executor_row_id: Option<Uuid>,
}

#[derive(sqlx::FromRow)]
struct ClaimableRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    output_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    provider_id: String,
    model: String,
    work_item_id: Uuid,
    output_index: i32,
    command_schema: String,
    command_hash: String,
    lease_epoch: i64,
}

#[derive(sqlx::FromRow)]
struct ResumableRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    output_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    provider_id: String,
    model: String,
    work_item_id: Uuid,
    output_index: i32,
    command_schema: String,
    command_hash: String,
    lease_epoch: i64,
    lease_expires_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct TerminalOutcomeRow {
    execution_state: String,
    submission_state: String,
    execution_error_code: Option<String>,
    submission_error_code: Option<String>,
    manifest_id: Option<Uuid>,
    artifact_authority_id: Option<Uuid>,
    resolution_source: Option<String>,
    decision_observation_id: Option<Uuid>,
    observation_payload_hash: Option<String>,
}

#[derive(sqlx::FromRow)]
struct ArtifactAuthorityRow {
    authority_id: Uuid,
    storage_backend: String,
    storage_namespace: String,
    object_key: String,
    sha256_hex: String,
    byte_size: i64,
    media_type: String,
}

#[derive(sqlx::FromRow)]
struct ExpiredExecutionRow {
    executor_execution_id: Uuid,
    submission_id: Uuid,
    executor_state: String,
}

#[derive(sqlx::FromRow)]
struct LockedExecutorRow {
    state: String,
    executor_owner: Option<String>,
    lease_epoch: i64,
    lease_expires_at_ms: Option<i64>,
    launch_owner: Option<String>,
    launch_lease_epoch: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct StoredObservationRow {
    observation_id: Uuid,
    observed_state: String,
    result_manifest_id: Option<Uuid>,
    error_code: Option<String>,
    payload_hash: String,
}

#[derive(sqlx::FromRow)]
struct LaunchContextRow {
    request_id: String,
    api_profile: String,
    output_index: i32,
    command_schema: String,
    command_hash: String,
    command_json: Value,
}

#[async_trait]
impl ExecutorLaunchContextStore for PostgresExecutorSubmissionStore {
    async fn load_launch_context(
        &self,
        lease: &ExecutorSubmissionLease,
    ) -> Result<ExecutorLaunchContext, ExecutorSubmissionError> {
        validate_executor_lease(lease)?;
        let row: LaunchContextRow = sqlx::query_as(
            r#"
            WITH db_clock AS (
              SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            )
            SELECT j.request_id, a.api_profile, o.output_index,
                   p.command_schema, s.command_hash, p.command_json
            FROM executor_executions e
            JOIN provider_submissions s
              ON s.executor_execution_id = e.executor_execution_id
             AND s.submission_id = e.submission_id
            JOIN job_outputs o ON o.output_id = s.output_id AND o.job_id = s.job_id
            JOIN jobs j
              ON j.job_id = s.job_id
             AND j.tenant_id = s.tenant_id
             AND j.provider_id = s.provider_id
             AND j.model = s.model
            JOIN job_payloads p ON p.job_id = s.job_id
            JOIN admission_sessions a
              ON a.session_id = p.admission_session_id
             AND a.job_id = s.job_id
             AND a.tenant_id = s.tenant_id
             AND a.request_id = j.request_id
            CROSS JOIN db_clock
            WHERE e.executor_execution_id = $1 AND e.submission_id = $2
              AND s.output_id = $3 AND s.job_id = $4 AND s.tenant_id = $5
              AND s.provider_id = $6 AND s.model = $7 AND s.work_item_id = $8
              AND o.output_index = $9
              AND s.command_schema = $10 AND s.command_hash = $11
              AND p.command_schema = s.command_schema
              AND e.state = 'running' AND s.state = 'running'
              AND e.executor_owner = $12 AND e.lease_epoch = $13
              AND e.lease_expires_at_ms > db_clock.now_ms
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .bind(lease.output_id)
        .bind(lease.job_id)
        .bind(&lease.tenant_id)
        .bind(&lease.provider_id)
        .bind(&lease.model)
        .bind(lease.work_item_id)
        .bind(lease.output_index)
        .bind(&lease.command_schema)
        .bind(&lease.command_hash)
        .bind(&lease.executor_owner)
        .bind(lease.executor_lease_epoch)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?
        .ok_or(ExecutorSubmissionError::StaleLease)?;
        if row.output_index != lease.output_index
            || row.command_schema != lease.command_schema
            || row.command_hash != lease.command_hash
            || command_hash(&row.command_json)? != lease.command_hash
        {
            return Err(ExecutorSubmissionError::Conflict);
        }
        Ok(ExecutorLaunchContext {
            request_id: row.request_id,
            api_profile: row.api_profile,
            output_index: row.output_index,
            command_schema: row.command_schema,
            command_hash: row.command_hash,
            command_json: row.command_json,
        })
    }
}

#[async_trait]
impl ExecutorArtifactAuthorityStore for PostgresExecutorSubmissionStore {
    async fn publish_artifact_authority(
        &self,
        lease: &ExecutorSubmissionLease,
        authority: &ExecutorArtifactAuthority,
    ) -> Result<(), ExecutorSubmissionError> {
        validate_executor_lease(lease)?;
        validate_artifact_authority(authority)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let locked = lock_executor_execution(&mut tx, lease).await?;
        if let Some(existing) = lock_artifact_authority(&mut tx, lease).await? {
            if artifact_authority_matches(&existing, authority) {
                tx.commit().await.map_err(unavailable)?;
                return Ok(());
            }
            return Err(ExecutorSubmissionError::Conflict);
        }
        if !matches!(
            locked.state.as_str(),
            "running" | "succeeded" | "failed" | "uncertain"
        ) || !launch_fence_matches(&locked, lease)
        {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        if !matches!(
            lock_submission_state(&mut tx, lease).await?.as_str(),
            "running" | "succeeded" | "failed" | "uncertain"
        ) {
            return Err(ExecutorSubmissionError::Conflict);
        }
        let now = database_now(&mut tx).await?;
        insert_artifact_authority(&mut tx, lease, authority, now).await?;
        tx.commit().await.map_err(unavailable)
    }
}

#[async_trait]
impl ExecutorSubmissionStore for PostgresExecutorSubmissionStore {
    async fn prepare_for_lease(
        &self,
        lease: &WorkLease,
    ) -> Result<Vec<PreparedExecutorSubmission>, ExecutorSubmissionError> {
        validate_work_lease(lease)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let command = lock_durable_command(&mut tx, lease).await?;
        if command.command_schema != lease.command_schema
            || command.command_json != lease.command_json
        {
            return Err(ExecutorSubmissionError::Conflict);
        }
        let output_count = command_output_count(command.requested_units, &command.command_json)?;
        let command_hash = command_hash(&command.command_json)?;

        let existing = load_existing(&mut tx, lease.job_id).await?;
        if !existing.is_empty() {
            if existing.iter().all(|row| row.submission_id.is_none()) {
                if command.economics_contract_version != 2 {
                    return Err(ExecutorSubmissionError::Conflict);
                }
                let prepared = prepare_admission_outputs(
                    &mut tx,
                    existing,
                    lease,
                    output_count,
                    &command_hash,
                    &command,
                )
                .await?;
                tx.commit().await.map_err(unavailable)?;
                return Ok(prepared);
            }
            let prepared =
                rebuild_existing(existing, lease, output_count, &command_hash, &command)?;
            attach_attempts(&mut tx, lease, &prepared).await?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(prepared);
        }
        if command.economics_contract_version == 2 {
            return Err(ExecutorSubmissionError::Conflict);
        }

        let now = database_now(&mut tx).await?;
        let mut prepared = Vec::with_capacity(output_count as usize);
        for output_index in 0..output_count {
            let output_id = Uuid::new_v4();
            let submission_id = Uuid::new_v4();
            let executor_execution_id = distinct_execution_id(lease.execution_id, submission_id);
            sqlx::query(
                r#"
                INSERT INTO job_outputs
                  (output_id, job_id, output_index, state, created_at_ms, updated_at_ms)
                VALUES ($1, $2, $3, 'pending', $4, $4)
                "#,
            )
            .bind(output_id)
            .bind(lease.job_id)
            .bind(output_index)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            sqlx::query(
                r#"
                INSERT INTO provider_submissions
                  (submission_id, executor_execution_id, output_id, job_id,
                   tenant_id, provider_id, model, work_item_id,
                   created_by_execution_id, created_by_lease_epoch, command_schema, command_hash,
                   state, prepared_at_ms, updated_at_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        'prepared', $13, $13)
                "#,
            )
            .bind(submission_id)
            .bind(executor_execution_id)
            .bind(output_id)
            .bind(lease.job_id)
            .bind(&command.tenant_id)
            .bind(&command.provider_id)
            .bind(&command.model)
            .bind(lease.work_item_id)
            .bind(lease.execution_id)
            .bind(lease.lease_epoch)
            .bind(&lease.command_schema)
            .bind(&command_hash)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            sqlx::query(
                r#"
                INSERT INTO executor_executions
                  (executor_execution_id, submission_id, state, created_at_ms, updated_at_ms)
                VALUES ($1, $2, 'prepared', $3, $3)
                "#,
            )
            .bind(executor_execution_id)
            .bind(submission_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            let item = PreparedExecutorSubmission {
                submission_id,
                executor_execution_id,
                output_id,
                job_id: lease.job_id,
                tenant_id: command.tenant_id.clone(),
                provider_id: command.provider_id.clone(),
                model: command.model.clone(),
                work_item_id: lease.work_item_id,
                output_index,
                command_schema: lease.command_schema.clone(),
                command_hash: command_hash.clone(),
            };
            insert_attachment(&mut tx, lease, submission_id, now).await?;
            prepared.push(item);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(prepared)
    }

    async fn resume_running(
        &self,
        scope: &ExecutorClaimScope,
        owner: &str,
    ) -> Result<Option<ExecutorSubmissionLease>, ExecutorSubmissionError> {
        validate_claim_scope(scope)?;
        validate_owner(owner)?;
        let row: Option<ResumableRow> = sqlx::query_as(
            r#"
            WITH db_clock AS (
              SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            )
            SELECT s.submission_id, e.executor_execution_id, s.output_id, s.job_id,
                   s.tenant_id, s.provider_id, s.model, s.work_item_id,
                   o.output_index, s.command_schema, s.command_hash,
                   e.lease_epoch, e.lease_expires_at_ms
            FROM executor_executions e
            JOIN provider_submissions s
              ON s.executor_execution_id = e.executor_execution_id
             AND s.submission_id = e.submission_id
            JOIN job_outputs o ON o.output_id = s.output_id AND o.job_id = s.job_id
            CROSS JOIN db_clock
            WHERE e.state = 'running' AND s.state = 'running'
              AND e.executor_owner = $1 AND e.lease_expires_at_ms > db_clock.now_ms
              AND s.provider_id = $2 AND s.command_schema = $3
            ORDER BY e.started_at_ms, e.created_at_ms, e.executor_execution_id
            LIMIT 1
            "#,
        )
        .bind(owner)
        .bind(&scope.provider_id)
        .bind(&scope.command_schema)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(row.map(|row| ExecutorSubmissionLease {
            submission_id: row.submission_id,
            executor_execution_id: row.executor_execution_id,
            output_id: row.output_id,
            job_id: row.job_id,
            tenant_id: row.tenant_id,
            provider_id: row.provider_id,
            model: row.model,
            work_item_id: row.work_item_id,
            output_index: row.output_index,
            command_schema: row.command_schema,
            command_hash: row.command_hash,
            executor_owner: owner.to_string(),
            executor_lease_epoch: row.lease_epoch,
            executor_lease_expires_at_ms: row.lease_expires_at_ms,
        }))
    }

    async fn claim_prepared(
        &self,
        scope: &ExecutorClaimScope,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ExecutorSubmissionLease>, ExecutorSubmissionError> {
        validate_owner_and_duration(owner, lease_ms)?;
        validate_claim_scope(scope)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let row: Option<ClaimableRow> = sqlx::query_as(
            r#"
            SELECT s.submission_id, e.executor_execution_id, s.output_id, s.job_id,
                   s.tenant_id, s.provider_id, s.model,
                   s.work_item_id, o.output_index, s.command_schema, s.command_hash,
                   e.lease_epoch
            FROM executor_executions e
            JOIN provider_submissions s
              ON s.executor_execution_id = e.executor_execution_id
             AND s.submission_id = e.submission_id
            JOIN job_outputs o ON o.output_id = s.output_id AND o.job_id = s.job_id
            JOIN work_items w ON w.work_item_id = s.work_item_id AND w.job_id = s.job_id
            JOIN job_attempts a
              ON a.work_item_id = w.work_item_id
             AND a.execution_id = w.execution_id
             AND a.lease_epoch = w.lease_epoch
            JOIN provider_submission_attachments pa
              ON pa.submission_id = s.submission_id
             AND pa.job_id = s.job_id
             AND pa.work_item_id = w.work_item_id
             AND pa.attempt_execution_id = a.execution_id
             AND pa.lease_epoch = a.lease_epoch
            JOIN jobs j ON j.job_id = s.job_id
            WHERE (e.state = 'prepared'
               OR (e.state = 'leased' AND e.lease_expires_at_ms <= $1))
              AND s.state = 'prepared'
              AND w.state = 'running' AND w.lease_expires_at_ms > $1
              AND a.state = 'running'
              AND j.state IN ('reserved', 'queued', 'running')
              AND s.provider_id = $2 AND s.command_schema = $3
            ORDER BY e.created_at_ms, s.job_id, o.output_index
            FOR UPDATE OF e, s SKIP LOCKED
            LIMIT 1
            "#,
        )
        .bind(now)
        .bind(&scope.provider_id)
        .bind(&scope.command_schema)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some(row) = row else {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        };
        let executor_lease_epoch = row
            .lease_epoch
            .checked_add(1)
            .ok_or(ExecutorSubmissionError::Unavailable)?;
        let executor_lease_expires_at_ms = now + lease_ms;
        let changed = sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = 'leased', executor_owner = $2, lease_epoch = $3,
                lease_expires_at_ms = $4, leased_at_ms = $5, updated_at_ms = $5
            WHERE executor_execution_id = $1 AND submission_id = $6
              AND (state = 'prepared'
                OR (state = 'leased' AND lease_expires_at_ms <= $5))
            "#,
        )
        .bind(row.executor_execution_id)
        .bind(owner)
        .bind(executor_lease_epoch)
        .bind(executor_lease_expires_at_ms)
        .bind(now)
        .bind(row.submission_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if changed != 1 {
            return Err(ExecutorSubmissionError::Unavailable);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(Some(ExecutorSubmissionLease {
            submission_id: row.submission_id,
            executor_execution_id: row.executor_execution_id,
            output_id: row.output_id,
            job_id: row.job_id,
            tenant_id: row.tenant_id,
            provider_id: row.provider_id,
            model: row.model,
            work_item_id: row.work_item_id,
            output_index: row.output_index,
            command_schema: row.command_schema,
            command_hash: row.command_hash,
            executor_owner: owner.to_string(),
            executor_lease_epoch,
            executor_lease_expires_at_ms,
        }))
    }

    async fn start(&self, lease: &ExecutorSubmissionLease) -> Result<(), ExecutorSubmissionError> {
        validate_executor_lease(lease)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        match lock_current_running_work(&mut tx, lease).await {
            Ok(()) => {}
            Err(ExecutorSubmissionError::StaleLease) => {
                tx.rollback().await.map_err(unavailable)?;
                return replay_started(&self.pool, lease).await;
            }
            Err(error) => return Err(error),
        }
        let locked = lock_executor_execution(&mut tx, lease).await?;
        let now = database_now(&mut tx).await?;
        if !locked_executor_matches(&locked, lease, now) {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        if locked.state == "running" {
            if !launch_fence_matches(&locked, lease)
                || lock_submission_state(&mut tx, lease).await? != "running"
            {
                return Err(ExecutorSubmissionError::Conflict);
            }
            tx.commit().await.map_err(unavailable)?;
            return Ok(());
        }
        if locked.state != "leased" {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        if lock_submission_state(&mut tx, lease).await? != "prepared" {
            return Err(ExecutorSubmissionError::Conflict);
        }
        let changed = sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = 'running', launch_owner = $3, launch_lease_epoch = $4,
                started_at_ms = $5, updated_at_ms = $5
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND executor_owner = $3 AND lease_epoch = $4 AND state = 'leased'
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .bind(&lease.executor_owner)
        .bind(lease.executor_lease_epoch)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if changed != 1 {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        let submission_changed = sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = 'running', started_at_ms = $6, updated_at_ms = $6
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND output_id = $3 AND job_id = $4 AND work_item_id = $5
              AND state = 'prepared'
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.executor_execution_id)
        .bind(lease.output_id)
        .bind(lease.job_id)
        .bind(lease.work_item_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if submission_changed != 1 {
            return Err(ExecutorSubmissionError::Unavailable);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }

    async fn heartbeat(
        &self,
        lease: &ExecutorSubmissionLease,
        lease_ms: i64,
    ) -> Result<ExecutorSubmissionLease, ExecutorSubmissionError> {
        validate_executor_lease(lease)?;
        validate_lease_duration(lease_ms)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let locked = lock_executor_execution(&mut tx, lease).await?;
        let now = database_now(&mut tx).await?;
        if locked.state != "running"
            || locked.executor_owner.as_deref() != Some(lease.executor_owner.as_str())
            || locked.lease_epoch != lease.executor_lease_epoch
            || !launch_fence_matches(&locked, lease)
            || locked
                .lease_expires_at_ms
                .is_none_or(|expires| expires <= now)
        {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        let executor_lease_expires_at_ms = locked
            .lease_expires_at_ms
            .unwrap_or_default()
            .max(now + lease_ms);
        let changed = sqlx::query(
            r#"
            UPDATE executor_executions
            SET lease_expires_at_ms = $5, updated_at_ms = $6
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND executor_owner = $3 AND lease_epoch = $4
              AND state = 'running'
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .bind(&lease.executor_owner)
        .bind(lease.executor_lease_epoch)
        .bind(executor_lease_expires_at_ms)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if changed != 1 {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(ExecutorSubmissionLease {
            executor_lease_expires_at_ms,
            ..lease.clone()
        })
    }

    async fn append_runner_observation(
        &self,
        lease: &ExecutorSubmissionLease,
        outcome: &ExecutorSubmissionOutcome,
    ) -> Result<ExecutorRunnerObservation, ExecutorSubmissionError> {
        validate_executor_lease(lease)?;
        validate_outcome(outcome)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let locked = lock_executor_execution(&mut tx, lease).await?;
        if !matches!(
            locked.state.as_str(),
            "running" | "succeeded" | "failed" | "uncertain"
        ) || !launch_fence_matches(&locked, lease)
        {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        let observation = runner_observation(lease, outcome)?;
        let payload_hash = observation_payload_hash(lease, outcome);
        if let Some(existing) = lock_runner_observation(&mut tx, lease).await? {
            if stored_observation_matches(&existing, &observation, &payload_hash) {
                tx.commit().await.map_err(unavailable)?;
                return Ok(observation);
            }
            return Err(ExecutorSubmissionError::Conflict);
        }
        if let Some(manifest) = outcome.manifest() {
            let authority = lock_artifact_authority(&mut tx, lease)
                .await?
                .ok_or(ExecutorSubmissionError::Conflict)?;
            if !manifest_matches_artifact_authority(manifest, &authority) {
                return Err(ExecutorSubmissionError::Conflict);
            }
        }
        let now = database_now(&mut tx).await?;
        if let Some(manifest) = outcome.manifest() {
            insert_result_manifest(&mut tx, lease, manifest, now).await?;
        }
        insert_runner_observation(&mut tx, lease, outcome, &payload_hash, now).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(observation)
    }

    async fn resolve_runner_observation(
        &self,
        lease: &ExecutorSubmissionLease,
        observation: &ExecutorRunnerObservation,
    ) -> Result<(), ExecutorSubmissionError> {
        validate_executor_lease(lease)?;
        validate_outcome(&observation.outcome)?;
        if observation.observation_id != lease.executor_execution_id
            || observation.executor_execution_id != lease.executor_execution_id
            || observation.submission_id != lease.submission_id
        {
            return Err(ExecutorSubmissionError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let outcome = &observation.outcome;
        let state = outcome.state();
        let error_code = outcome.error_code();
        let locked = lock_executor_execution(&mut tx, lease).await?;
        if matches!(locked.state.as_str(), "succeeded" | "failed" | "uncertain") {
            if !launch_fence_matches(&locked, lease) {
                return Err(ExecutorSubmissionError::StaleLease);
            }
            return match terminal_outcome_matches(&mut tx, lease, outcome).await? {
                Some(true) => {
                    tx.commit().await.map_err(unavailable)?;
                    Ok(())
                }
                Some(false) => Err(ExecutorSubmissionError::Conflict),
                None => Err(ExecutorSubmissionError::StaleLease),
            };
        }
        let now = database_now(&mut tx).await?;
        if locked.state != "running"
            || locked.executor_owner.as_deref() != Some(lease.executor_owner.as_str())
            || locked.lease_epoch != lease.executor_lease_epoch
            || !launch_fence_matches(&locked, lease)
            || locked
                .lease_expires_at_ms
                .is_none_or(|expires| expires <= now)
        {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        if lock_submission_state(&mut tx, lease).await? != "running" {
            return Err(ExecutorSubmissionError::Conflict);
        }
        let stored = lock_runner_observation(&mut tx, lease)
            .await?
            .ok_or(ExecutorSubmissionError::Conflict)?;
        let payload_hash = observation_payload_hash(lease, outcome);
        if !stored_observation_matches(&stored, observation, &payload_hash) {
            return Err(ExecutorSubmissionError::Conflict);
        }
        insert_resolution_decision(
            &mut tx,
            lease.executor_execution_id,
            lease.submission_id,
            "active_runner_observation",
            Some(observation.observation_id),
            state,
            outcome.manifest().map(|manifest| manifest.manifest_id),
            error_code,
            now,
        )
        .await?;
        let execution_changed = sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = $5, executor_owner = NULL, lease_expires_at_ms = NULL,
                resolution_decision_id = $1,
                finished_at_ms = $6, updated_at_ms = $6, error_code = $7
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND executor_owner = $3 AND lease_epoch = $4
              AND state = 'running'
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .bind(&lease.executor_owner)
        .bind(lease.executor_lease_epoch)
        .bind(state)
        .bind(now)
        .bind(error_code)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if execution_changed != 1 {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        let submission_changed = sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = $6, result_manifest_id = $7, finished_at_ms = $8,
                updated_at_ms = $8, error_code = $9,
                resolution_decision_id = $2
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND output_id = $3 AND job_id = $4 AND work_item_id = $5
              AND state = 'running'
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.executor_execution_id)
        .bind(lease.output_id)
        .bind(lease.job_id)
        .bind(lease.work_item_id)
        .bind(state)
        .bind(outcome.manifest().map(|manifest| manifest.manifest_id))
        .bind(now)
        .bind(error_code)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if submission_changed != 1 {
            return Err(ExecutorSubmissionError::Unavailable);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }

    async fn reconcile_expired(&self, limit: u32) -> Result<u64, ExecutorSubmissionError> {
        if limit == 0 || limit > MAX_RECONCILE_BATCH {
            return Err(ExecutorSubmissionError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let rows: Vec<ExpiredExecutionRow> = sqlx::query_as(
            r#"
            SELECT e.executor_execution_id, e.submission_id, e.state AS executor_state
            FROM executor_executions e
            JOIN provider_submissions s
             ON s.executor_execution_id = e.executor_execution_id
             AND s.submission_id = e.submission_id
            JOIN work_items w ON w.work_item_id = s.work_item_id AND w.job_id = s.job_id
            WHERE e.lease_expires_at_ms <= $1
              AND (
                (e.state = 'running' AND s.state = 'running')
                OR
                (e.state = 'leased' AND s.state = 'prepared'
                  AND w.state IN ('succeeded', 'failed', 'uncertain'))
              )
            ORDER BY e.lease_expires_at_ms, e.executor_execution_id
            FOR UPDATE OF e, s SKIP LOCKED
            LIMIT $2
            "#,
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
        for row in &rows {
            update_expired_execution(&mut tx, row, now).await?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(rows.len() as u64)
    }
}

async fn lock_durable_command(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
) -> Result<DurableCommandRow, ExecutorSubmissionError> {
    let job: Option<(i32, i16, String, String, String)> = sqlx::query_as(
        r#"
        SELECT requested_units, economics_contract_version, tenant_id, provider_id, model
        FROM jobs
        WHERE job_id = $1 AND state IN ('reserved', 'queued', 'running')
        FOR UPDATE
        "#,
    )
    .bind(lease.job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some((requested_units, economics_contract_version, tenant_id, provider_id, model)) = job
    else {
        return Err(ExecutorSubmissionError::StaleLease);
    };
    let payload: Option<(String, Value)> = sqlx::query_as(
        r#"
        SELECT p.command_schema, p.command_json
        FROM work_items w
        JOIN job_attempts a
          ON a.work_item_id = w.work_item_id
         AND a.execution_id = w.execution_id
         AND a.lease_epoch = w.lease_epoch
        JOIN job_payloads p ON p.job_id = w.job_id
        WHERE w.work_item_id = $1 AND w.job_id = $2
          AND w.execution_id = $3 AND w.lease_epoch = $4 AND w.lease_owner = $5
          AND w.state = 'leased' AND w.lease_expires_at_ms >
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
          AND a.worker_id = $5 AND a.state = 'claimed'
        FOR UPDATE OF w, a
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.job_id)
    .bind(lease.execution_id)
    .bind(lease.lease_epoch)
    .bind(&lease.worker_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some((command_schema, command_json)) = payload else {
        return Err(ExecutorSubmissionError::StaleLease);
    };
    Ok(DurableCommandRow {
        requested_units,
        economics_contract_version,
        tenant_id,
        provider_id,
        model,
        command_schema,
        command_json,
    })
}

async fn prepare_admission_outputs(
    tx: &mut Transaction<'_, Postgres>,
    rows: Vec<ExistingIdentityRow>,
    lease: &WorkLease,
    output_count: i32,
    command_hash: &str,
    command: &DurableCommandRow,
) -> Result<Vec<PreparedExecutorSubmission>, ExecutorSubmissionError> {
    if rows.len() != output_count as usize
        || rows
            .iter()
            .enumerate()
            .any(|(index, row)| row.output_index != index as i32 || row.submission_id.is_some())
    {
        return Err(ExecutorSubmissionError::Conflict);
    }
    let now = database_now(tx).await?;
    let mut prepared = Vec::with_capacity(rows.len());
    for row in rows {
        let submission_id = Uuid::new_v4();
        let executor_execution_id = distinct_execution_id(lease.execution_id, submission_id);
        sqlx::query(
            r#"
            INSERT INTO provider_submissions
              (submission_id, executor_execution_id, output_id, job_id,
               tenant_id, provider_id, model, work_item_id,
               created_by_execution_id, created_by_lease_epoch, command_schema, command_hash,
               state, prepared_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                    'prepared', $13, $13)
            "#,
        )
        .bind(submission_id)
        .bind(executor_execution_id)
        .bind(row.output_id)
        .bind(lease.job_id)
        .bind(&command.tenant_id)
        .bind(&command.provider_id)
        .bind(&command.model)
        .bind(lease.work_item_id)
        .bind(lease.execution_id)
        .bind(lease.lease_epoch)
        .bind(&lease.command_schema)
        .bind(command_hash)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            r#"
            INSERT INTO executor_executions
              (executor_execution_id, submission_id, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'prepared', $3, $3)
            "#,
        )
        .bind(executor_execution_id)
        .bind(submission_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
        let submission = PreparedExecutorSubmission {
            submission_id,
            executor_execution_id,
            output_id: row.output_id,
            job_id: lease.job_id,
            tenant_id: command.tenant_id.clone(),
            provider_id: command.provider_id.clone(),
            model: command.model.clone(),
            work_item_id: lease.work_item_id,
            output_index: row.output_index,
            command_schema: lease.command_schema.clone(),
            command_hash: command_hash.to_string(),
        };
        insert_attachment(tx, lease, submission_id, now).await?;
        prepared.push(submission);
    }
    Ok(prepared)
}

async fn load_existing(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<Vec<ExistingIdentityRow>, ExecutorSubmissionError> {
    sqlx::query_as(
        r#"
        SELECT o.output_id, o.output_index, s.submission_id, s.executor_execution_id,
               s.tenant_id, s.provider_id, s.model,
               s.work_item_id, s.command_schema, s.command_hash,
               e.executor_execution_id AS executor_row_id
        FROM job_outputs o
        LEFT JOIN provider_submissions s
          ON s.output_id = o.output_id AND s.job_id = o.job_id
        LEFT JOIN executor_executions e
          ON e.executor_execution_id = s.executor_execution_id
         AND e.submission_id = s.submission_id
        WHERE o.job_id = $1
        ORDER BY o.output_index
        "#,
    )
    .bind(job_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)
}

fn rebuild_existing(
    rows: Vec<ExistingIdentityRow>,
    lease: &WorkLease,
    output_count: i32,
    command_hash: &str,
    command: &DurableCommandRow,
) -> Result<Vec<PreparedExecutorSubmission>, ExecutorSubmissionError> {
    if rows.len() != output_count as usize {
        return Err(ExecutorSubmissionError::Conflict);
    }
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| {
            let submission_id = row.submission_id.ok_or(ExecutorSubmissionError::Conflict)?;
            let executor_execution_id = row
                .executor_execution_id
                .ok_or(ExecutorSubmissionError::Conflict)?;
            if row.output_index != index as i32
                || row.work_item_id != Some(lease.work_item_id)
                || row.tenant_id.as_deref() != Some(command.tenant_id.as_str())
                || row.provider_id.as_deref() != Some(command.provider_id.as_str())
                || row.model.as_deref() != Some(command.model.as_str())
                || row.command_schema.as_deref() != Some(lease.command_schema.as_str())
                || row.command_hash.as_deref() != Some(command_hash)
                || row.executor_row_id != Some(executor_execution_id)
            {
                return Err(ExecutorSubmissionError::Conflict);
            }
            Ok(PreparedExecutorSubmission {
                submission_id,
                executor_execution_id,
                output_id: row.output_id,
                job_id: lease.job_id,
                tenant_id: row.tenant_id.ok_or(ExecutorSubmissionError::Conflict)?,
                provider_id: row.provider_id.ok_or(ExecutorSubmissionError::Conflict)?,
                model: row.model.ok_or(ExecutorSubmissionError::Conflict)?,
                work_item_id: lease.work_item_id,
                output_index: row.output_index,
                command_schema: lease.command_schema.clone(),
                command_hash: command_hash.to_string(),
            })
        })
        .collect()
}

async fn attach_attempts(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    submissions: &[PreparedExecutorSubmission],
) -> Result<(), ExecutorSubmissionError> {
    let now = database_now(tx).await?;
    for submission in submissions {
        insert_attachment(tx, lease, submission.submission_id, now).await?;
    }
    Ok(())
}

async fn insert_attachment(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    submission_id: Uuid,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    sqlx::query(
        r#"
        INSERT INTO provider_submission_attachments
          (submission_id, job_id, attempt_execution_id, work_item_id, lease_epoch, attached_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (submission_id, attempt_execution_id) DO NOTHING
        "#,
    )
    .bind(submission_id)
    .bind(lease.job_id)
    .bind(lease.execution_id)
    .bind(lease.work_item_id)
    .bind(lease.lease_epoch)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn lock_current_running_work(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
) -> Result<(), ExecutorSubmissionError> {
    let active: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT TRUE
        FROM work_items w
        JOIN job_attempts a
          ON a.work_item_id = w.work_item_id
         AND a.execution_id = w.execution_id
         AND a.lease_epoch = w.lease_epoch
        JOIN jobs j ON j.job_id = w.job_id
        JOIN provider_submission_attachments pa
          ON pa.submission_id = $3
         AND pa.job_id = w.job_id
         AND pa.work_item_id = w.work_item_id
         AND pa.attempt_execution_id = a.execution_id
         AND pa.lease_epoch = a.lease_epoch
        WHERE w.work_item_id = $1 AND w.job_id = $2
          AND w.state = 'running' AND w.lease_expires_at_ms >
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
          AND a.state = 'running'
          AND j.state IN ('reserved', 'queued', 'running')
        FOR UPDATE OF w, a
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.job_id)
    .bind(lease.submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    active.ok_or(ExecutorSubmissionError::StaleLease).map(drop)
}

async fn lock_executor_execution(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
) -> Result<LockedExecutorRow, ExecutorSubmissionError> {
    sqlx::query_as(
        r#"
        SELECT state, executor_owner, lease_epoch, lease_expires_at_ms,
               launch_owner, launch_lease_epoch
        FROM executor_executions
        WHERE executor_execution_id = $1 AND submission_id = $2
        FOR UPDATE
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorSubmissionError::StaleLease)
}

async fn replay_started(
    pool: &PgPool,
    lease: &ExecutorSubmissionLease,
) -> Result<(), ExecutorSubmissionError> {
    let mut tx = pool.begin().await.map_err(unavailable)?;
    let locked = lock_executor_execution(&mut tx, lease).await?;
    let now = database_now(&mut tx).await?;
    if locked.state != "running"
        || !locked_executor_matches(&locked, lease, now)
        || !launch_fence_matches(&locked, lease)
    {
        return Err(ExecutorSubmissionError::StaleLease);
    }
    if lock_submission_state(&mut tx, lease).await? != "running" {
        return Err(ExecutorSubmissionError::Conflict);
    }
    tx.commit().await.map_err(unavailable)
}

fn locked_executor_matches(
    locked: &LockedExecutorRow,
    lease: &ExecutorSubmissionLease,
    now: i64,
) -> bool {
    locked.executor_owner.as_deref() == Some(lease.executor_owner.as_str())
        && locked.lease_epoch == lease.executor_lease_epoch
        && locked
            .lease_expires_at_ms
            .is_some_and(|expires| expires > now)
}

fn launch_fence_matches(locked: &LockedExecutorRow, lease: &ExecutorSubmissionLease) -> bool {
    locked.launch_owner.as_deref() == Some(lease.executor_owner.as_str())
        && locked.launch_lease_epoch == Some(lease.executor_lease_epoch)
}

async fn lock_submission_state(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
) -> Result<String, ExecutorSubmissionError> {
    sqlx::query_scalar(
        r#"
        SELECT s.state
        FROM provider_submissions s
        JOIN job_outputs o ON o.output_id = s.output_id AND o.job_id = s.job_id
        WHERE s.submission_id = $1 AND s.executor_execution_id = $2
          AND s.output_id = $3 AND s.job_id = $4 AND s.work_item_id = $5
          AND s.tenant_id = $6 AND s.provider_id = $7 AND s.model = $8
          AND s.command_schema = $9 AND s.command_hash = $10
          AND o.output_index = $11
        FOR UPDATE OF s
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(lease.output_id)
    .bind(lease.job_id)
    .bind(lease.work_item_id)
    .bind(&lease.tenant_id)
    .bind(&lease.provider_id)
    .bind(&lease.model)
    .bind(&lease.command_schema)
    .bind(&lease.command_hash)
    .bind(lease.output_index)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorSubmissionError::Conflict)
}

async fn lock_artifact_authority(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
) -> Result<Option<ArtifactAuthorityRow>, ExecutorSubmissionError> {
    sqlx::query_as(
        r#"
        SELECT a.authority_id, a.storage_backend, a.storage_namespace,
               a.object_key, a.sha256_hex, a.byte_size, a.media_type
        FROM executor_artifact_authorities a
        JOIN provider_submissions s
          ON s.submission_id = a.submission_id
         AND s.executor_execution_id = a.executor_execution_id
         AND s.output_id = a.output_id
         AND s.job_id = a.job_id
        JOIN job_outputs o ON o.output_id = s.output_id AND o.job_id = s.job_id
        WHERE a.executor_execution_id = $1 AND a.submission_id = $2
          AND s.output_id = $3 AND s.job_id = $4 AND s.work_item_id = $5
          AND s.tenant_id = $6 AND s.provider_id = $7 AND s.model = $8
          AND s.command_schema = $9 AND s.command_hash = $10
          AND o.output_index = $11
        FOR UPDATE OF a
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(lease.output_id)
    .bind(lease.job_id)
    .bind(lease.work_item_id)
    .bind(&lease.tenant_id)
    .bind(&lease.provider_id)
    .bind(&lease.model)
    .bind(&lease.command_schema)
    .bind(&lease.command_hash)
    .bind(lease.output_index)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn insert_artifact_authority(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
    authority: &ExecutorArtifactAuthority,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    sqlx::query(
        r#"
        INSERT INTO executor_artifact_authorities
          (authority_id, executor_execution_id, submission_id, output_id, job_id,
           storage_backend, storage_namespace, object_key, sha256_hex, byte_size,
           media_type, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        "#,
    )
    .bind(authority.authority_id)
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(lease.output_id)
    .bind(lease.job_id)
    .bind(&authority.storage_backend)
    .bind(&authority.storage_namespace)
    .bind(&authority.object_key)
    .bind(&authority.sha256_hex)
    .bind(i64::try_from(authority.byte_size).map_err(|_| ExecutorSubmissionError::InvalidInput)?)
    .bind(&authority.media_type)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

fn artifact_authority_matches(
    stored: &ArtifactAuthorityRow,
    expected: &ExecutorArtifactAuthority,
) -> bool {
    stored.authority_id == expected.authority_id
        && stored.storage_backend == expected.storage_backend
        && stored.storage_namespace == expected.storage_namespace
        && stored.object_key == expected.object_key
        && stored.sha256_hex == expected.sha256_hex
        && u64::try_from(stored.byte_size).ok() == Some(expected.byte_size)
        && stored.media_type == expected.media_type
}

fn manifest_matches_artifact_authority(
    manifest: &ExecutorResultManifest,
    authority: &ArtifactAuthorityRow,
) -> bool {
    manifest.artifact_authority_id == authority.authority_id
}

fn runner_observation(
    lease: &ExecutorSubmissionLease,
    outcome: &ExecutorSubmissionOutcome,
) -> Result<ExecutorRunnerObservation, ExecutorSubmissionError> {
    ExecutorRunnerObservation::new(
        lease.executor_execution_id,
        lease.submission_id,
        outcome.clone(),
    )
    .ok_or(ExecutorSubmissionError::InvalidInput)
}

fn observation_payload_hash(
    lease: &ExecutorSubmissionLease,
    outcome: &ExecutorSubmissionOutcome,
) -> String {
    let mut hash = Sha256::new();
    for value in [
        "executor-runner-observation-v1".to_string(),
        lease.executor_execution_id.to_string(),
        lease.submission_id.to_string(),
        lease.executor_owner.clone(),
        lease.executor_lease_epoch.to_string(),
        outcome.state().to_string(),
        outcome
            .manifest()
            .map(|manifest| manifest.manifest_id.to_string())
            .unwrap_or_default(),
        outcome.error_code().unwrap_or_default().to_string(),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hex::encode(hash.finalize())
}

async fn lock_runner_observation(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
) -> Result<Option<StoredObservationRow>, ExecutorSubmissionError> {
    sqlx::query_as(
        r#"
        SELECT observation_id, observed_state, result_manifest_id, error_code, payload_hash
        FROM executor_runner_observations
        WHERE executor_execution_id = $1 AND submission_id = $2
        FOR UPDATE
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

fn stored_observation_matches(
    stored: &StoredObservationRow,
    observation: &ExecutorRunnerObservation,
    payload_hash: &str,
) -> bool {
    stored.observation_id == observation.observation_id
        && stored.observed_state == observation.outcome.state()
        && stored.result_manifest_id
            == observation
                .outcome
                .manifest()
                .map(|manifest| manifest.manifest_id)
        && stored.error_code.as_deref() == observation.outcome.error_code()
        && stored.payload_hash == payload_hash
}

async fn insert_runner_observation(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
    outcome: &ExecutorSubmissionOutcome,
    payload_hash: &str,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    sqlx::query(
        r#"
        INSERT INTO executor_runner_observations
          (observation_id, executor_execution_id, submission_id, launch_owner,
           launch_lease_epoch, observed_state, result_manifest_id, error_code,
           payload_hash, observed_at_ms)
        VALUES ($1, $1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(&lease.executor_owner)
    .bind(lease.executor_lease_epoch)
    .bind(outcome.state())
    .bind(outcome.manifest().map(|manifest| manifest.manifest_id))
    .bind(outcome.error_code())
    .bind(payload_hash)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_resolution_decision(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
    source: &str,
    observation_id: Option<Uuid>,
    resolved_state: &str,
    result_manifest_id: Option<Uuid>,
    error_code: Option<&str>,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    sqlx::query(
        r#"
        INSERT INTO executor_resolution_decisions
          (decision_id, executor_execution_id, submission_id, source,
           observation_id, resolved_state, result_manifest_id, error_code,
           decided_at_ms)
        VALUES ($1, $1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .bind(source)
    .bind(observation_id)
    .bind(resolved_state)
    .bind(result_manifest_id)
    .bind(error_code)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn insert_result_manifest(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
    manifest: &ExecutorResultManifest,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    sqlx::query(
        r#"
        INSERT INTO executor_result_manifests
          (manifest_id, artifact_authority_id, executor_execution_id, submission_id, created_at_ms)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(manifest.manifest_id)
    .bind(manifest.artifact_authority_id)
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn terminal_outcome_matches(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
    outcome: &ExecutorSubmissionOutcome,
) -> Result<Option<bool>, ExecutorSubmissionError> {
    let row: Option<TerminalOutcomeRow> = sqlx::query_as(
        r#"
        SELECT e.state AS execution_state, s.state AS submission_state,
               e.error_code AS execution_error_code,
               s.error_code AS submission_error_code,
               m.manifest_id, m.artifact_authority_id,
               d.source AS resolution_source,
               d.observation_id AS decision_observation_id,
               ro.payload_hash AS observation_payload_hash
        FROM executor_executions e
        JOIN provider_submissions s
          ON s.executor_execution_id = e.executor_execution_id
         AND s.submission_id = e.submission_id
        LEFT JOIN executor_result_manifests m
          ON m.manifest_id = s.result_manifest_id
         AND m.executor_execution_id = e.executor_execution_id
         AND m.submission_id = s.submission_id
        LEFT JOIN executor_artifact_authorities aa
          ON aa.authority_id = m.artifact_authority_id
         AND aa.executor_execution_id = m.executor_execution_id
         AND aa.submission_id = m.submission_id
        LEFT JOIN executor_resolution_decisions d
          ON d.decision_id = e.resolution_decision_id
         AND d.executor_execution_id = e.executor_execution_id
         AND d.submission_id = e.submission_id
        LEFT JOIN executor_runner_observations ro
          ON ro.observation_id = d.observation_id
         AND ro.executor_execution_id = d.executor_execution_id
         AND ro.submission_id = d.submission_id
        WHERE e.executor_execution_id = $1 AND e.submission_id = $2
          AND s.output_id = $3 AND s.job_id = $4 AND s.work_item_id = $5
        FOR UPDATE OF e, s
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(lease.output_id)
    .bind(lease.job_id)
    .bind(lease.work_item_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if !matches!(
        row.execution_state.as_str(),
        "succeeded" | "failed" | "uncertain"
    ) || !matches!(
        row.submission_state.as_str(),
        "succeeded" | "failed" | "uncertain"
    ) {
        return Ok(None);
    }
    let base_matches = row.execution_state == outcome.state()
        && row.submission_state == outcome.state()
        && row.execution_error_code.as_deref() == outcome.error_code()
        && row.submission_error_code.as_deref() == outcome.error_code();
    let manifest_matches = match outcome.manifest() {
        Some(manifest) => {
            row.manifest_id == Some(manifest.manifest_id)
                && row.artifact_authority_id == Some(manifest.artifact_authority_id)
        }
        None => row.manifest_id.is_none(),
    };
    let active_decision_matches = row.resolution_source.as_deref()
        == Some("active_runner_observation")
        && row.decision_observation_id == Some(lease.executor_execution_id)
        && row.observation_payload_hash.as_deref()
            == Some(observation_payload_hash(lease, outcome).as_str());
    Ok(Some(
        base_matches && manifest_matches && active_decision_matches,
    ))
}

async fn update_expired_execution(
    tx: &mut Transaction<'_, Postgres>,
    row: &ExpiredExecutionRow,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    let (executor_from, submission_from, target, error_code) = if row.executor_state == "running" {
        ("running", "running", "uncertain", EXECUTOR_LEASE_EXPIRED)
    } else {
        ("leased", "prepared", "canceled", EXECUTOR_START_ABANDONED)
    };
    insert_resolution_decision(
        tx,
        row.executor_execution_id,
        row.submission_id,
        if row.executor_state == "running" {
            "executor_lease_expired"
        } else {
            "executor_start_abandoned"
        },
        None,
        target,
        None,
        Some(error_code),
        now,
    )
    .await?;
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = $3, executor_owner = NULL, lease_expires_at_ms = NULL,
                resolution_decision_id = $1,
                finished_at_ms = $4, updated_at_ms = $4, error_code = $5
            WHERE executor_execution_id = $1 AND submission_id = $2 AND state = $6
            "#,
        )
        .bind(row.executor_execution_id)
        .bind(row.submission_id)
        .bind(target)
        .bind(now)
        .bind(error_code)
        .bind(executor_from)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = $3, finished_at_ms = $4,
                updated_at_ms = $4, error_code = $5,
                resolution_decision_id = $1
            WHERE executor_execution_id = $1 AND submission_id = $2 AND state = $6
            "#,
        )
        .bind(row.executor_execution_id)
        .bind(row.submission_id)
        .bind(target)
        .bind(now)
        .bind(error_code)
        .bind(submission_from)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ExecutorSubmissionError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn require_one(result: sqlx::postgres::PgQueryResult) -> Result<(), ExecutorSubmissionError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(ExecutorSubmissionError::Unavailable)
    }
}

fn unavailable(_: sqlx::Error) -> ExecutorSubmissionError {
    ExecutorSubmissionError::Unavailable
}
