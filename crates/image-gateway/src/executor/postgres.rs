use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    ExecutorArtifactAuthority, ExecutorArtifactAuthorityStore, ExecutorClaimScope,
    ExecutorEvidenceStore, ExecutorExecutionProfile, ExecutorExecutionProfileStore,
    ExecutorHandoffStore, ExecutorLaunchContext, ExecutorLaunchContextStore,
    ExecutorResultManifest, ExecutorRunnerObservation, ExecutorSubmissionError,
    ExecutorSubmissionLease, ExecutorSubmissionOutcome, ExecutorSubmissionStore,
    PreparedExecutorSubmission,
};
use crate::admission::WorkLease;

const MAX_RECONCILE_BATCH: u32 = 1_000;
const EXECUTOR_LEASE_EXPIRED: &str = "executor_lease_expired";
const EXECUTOR_START_ABANDONED: &str = "executor_start_abandoned";
const FINALIZATION_FENCE_GRACE_MS: i64 = 5_000;

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

#[derive(Clone, Copy)]
enum HandoffParentState {
    Leased,
    HandedOff { execution_profile_id: Uuid },
}

struct LockedHandoffCommand {
    command: DurableCommandRow,
    parent_state: HandoffParentState,
}

#[derive(sqlx::FromRow)]
struct HandoffParentRow {
    command_schema: String,
    command_json: Value,
    work_state: String,
    lease_owner: Option<String>,
    lease_expires_at_ms: Option<i64>,
    execution_profile_id: Option<Uuid>,
    work_handed_off_at_ms: Option<i64>,
    attempt_state: String,
    worker_id: String,
    attempt_handed_off_at_ms: Option<i64>,
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
    execution_profile_id: Option<Uuid>,
    adapter_revision: Option<String>,
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
    execution_profile_id: Uuid,
    adapter_revision: String,
    executor_state: String,
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
    execution_profile_id: Uuid,
    adapter_revision: String,
    lease_epoch: i64,
    lease_expires_at_ms: i64,
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
    has_observation: bool,
}

#[derive(sqlx::FromRow)]
struct ExecutionProfileRow {
    execution_profile_id: Uuid,
    profile_key: String,
    provider_id: String,
    command_schema: String,
    adapter_revision: String,
    credential_pool_id: Uuid,
    provider_account_id: Uuid,
    credential_ref: String,
    credential_revision: i64,
    credential_auth_sha256: String,
    resource_policy_id: Uuid,
    resource_policy_revision: i64,
    max_concurrency: i32,
}

impl From<ExecutionProfileRow> for ExecutorExecutionProfile {
    fn from(row: ExecutionProfileRow) -> Self {
        Self {
            execution_profile_id: row.execution_profile_id,
            profile_key: row.profile_key,
            provider_id: row.provider_id,
            command_schema: row.command_schema,
            adapter_revision: row.adapter_revision,
            credential_pool_id: row.credential_pool_id,
            provider_account_id: row.provider_account_id,
            credential_ref: row.credential_ref,
            credential_revision: row.credential_revision,
            credential_auth_sha256: row.credential_auth_sha256,
            resource_policy_id: row.resource_policy_id,
            resource_policy_revision: row.resource_policy_revision,
            max_concurrency: row.max_concurrency,
        }
    }
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
struct CapacityAllocationRow {
    state: String,
    resource_policy_id: Uuid,
    resource_policy_revision: i64,
    release_decision_id: Option<Uuid>,
    released_state: Option<String>,
    release_reason: Option<String>,
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
impl ExecutorExecutionProfileStore for PostgresExecutorSubmissionStore {
    async fn load_execution_profile(
        &self,
        profile_key: &str,
    ) -> Result<ExecutorExecutionProfile, ExecutorSubmissionError> {
        if profile_key.is_empty() || profile_key.len() > 128 {
            return Err(ExecutorSubmissionError::InvalidInput);
        }
        load_execution_profile_by_key(&self.pool, profile_key)
            .await
            .map(Into::into)
    }
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
              AND s.execution_profile_id = $14 AND s.adapter_revision = $15
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
        .bind(lease.execution_profile_id)
        .bind(&lease.adapter_revision)
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
impl ExecutorEvidenceStore for PostgresExecutorSubmissionStore {
    async fn load_pending_evidence(
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
                   s.execution_profile_id, s.adapter_revision,
                   e.launch_lease_epoch AS lease_epoch,
                   COALESCE(e.lease_expires_at_ms, 0)::BIGINT AS lease_expires_at_ms
            FROM executor_executions e
            JOIN provider_submissions s
              ON s.executor_execution_id = e.executor_execution_id
             AND s.submission_id = e.submission_id
            JOIN job_outputs o ON o.output_id = s.output_id AND o.job_id = s.job_id
            LEFT JOIN executor_runner_observations observation
              ON observation.executor_execution_id = e.executor_execution_id
             AND observation.submission_id = e.submission_id
            CROSS JOIN db_clock
            WHERE e.launch_owner = $1 AND e.launch_lease_epoch IS NOT NULL
              AND s.execution_profile_id = $2
              AND s.provider_id = $3 AND s.command_schema = $4
              AND s.adapter_revision = $5
              AND observation.observation_id IS NULL
              AND (
                (e.state = 'running' AND s.state = 'running'
                    AND e.lease_expires_at_ms <= db_clock.now_ms)
                OR
                (e.state IN ('succeeded', 'failed', 'uncertain') AND s.state = e.state)
              )
            ORDER BY e.updated_at_ms, e.executor_execution_id
            LIMIT 1
            "#,
        )
        .bind(owner)
        .bind(scope.execution_profile_id)
        .bind(&scope.provider_id)
        .bind(&scope.command_schema)
        .bind(&scope.adapter_revision)
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
            execution_profile_id: row.execution_profile_id,
            adapter_revision: row.adapter_revision,
            executor_owner: owner.to_string(),
            executor_lease_epoch: row.lease_epoch,
            executor_lease_expires_at_ms: row.lease_expires_at_ms,
        }))
    }
}

#[async_trait]
impl ExecutorHandoffStore for PostgresExecutorSubmissionStore {
    async fn prepare_and_handoff(
        &self,
        lease: &WorkLease,
        execution_profile_id: Uuid,
    ) -> Result<Vec<PreparedExecutorSubmission>, ExecutorSubmissionError> {
        validate_work_lease(lease)?;
        if execution_profile_id.is_nil() {
            return Err(ExecutorSubmissionError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let locked = lock_durable_command(&mut tx, lease).await?;
        let command = &locked.command;
        if command.command_schema != lease.command_schema
            || command.command_json != lease.command_json
        {
            return Err(ExecutorSubmissionError::Conflict);
        }
        if command.economics_contract_version != 2 {
            return Err(ExecutorSubmissionError::Conflict);
        }
        let profile = match locked.parent_state {
            HandoffParentState::Leased => {
                lock_active_execution_profile(&mut tx, execution_profile_id).await?
            }
            HandoffParentState::HandedOff {
                execution_profile_id: bound_profile_id,
            } => {
                if bound_profile_id != execution_profile_id {
                    return Err(ExecutorSubmissionError::Conflict);
                }
                lock_bound_execution_profile(&mut tx, execution_profile_id).await?
            }
        };
        if profile.provider_id != command.provider_id
            || profile.command_schema != command.command_schema
        {
            return Err(ExecutorSubmissionError::Conflict);
        }
        if matches!(locked.parent_state, HandoffParentState::Leased) {
            bind_work_execution_profile(&mut tx, lease, execution_profile_id).await?;
        }
        let output_count = command_output_count(command.requested_units, &command.command_json)?;
        let command_hash = command_hash(&command.command_json)?;

        let existing = load_existing(&mut tx, lease.job_id).await?;
        let prepared = if existing.is_empty() {
            return Err(ExecutorSubmissionError::Conflict);
        } else if existing.iter().all(|row| row.submission_id.is_none()) {
            prepare_admission_outputs(
                &mut tx,
                existing,
                lease,
                output_count,
                &command_hash,
                command,
                &profile,
            )
            .await?
        } else {
            let prepared = rebuild_existing(
                existing,
                lease,
                output_count,
                &command_hash,
                command,
                &profile,
            )?;
            attach_attempts(&mut tx, lease, &prepared).await?;
            prepared
        };
        if matches!(locked.parent_state, HandoffParentState::Leased) {
            transition_to_executor_handoff(&mut tx, lease).await?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(prepared)
    }
}

#[async_trait]
impl ExecutorSubmissionStore for PostgresExecutorSubmissionStore {
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
                   s.execution_profile_id, s.adapter_revision,
                   e.lease_epoch, e.lease_expires_at_ms
            FROM executor_executions e
            JOIN provider_submissions s
              ON s.executor_execution_id = e.executor_execution_id
             AND s.submission_id = e.submission_id
            JOIN job_outputs o ON o.output_id = s.output_id AND o.job_id = s.job_id
            CROSS JOIN db_clock
            WHERE e.state = 'running' AND s.state = 'running'
              AND e.executor_owner = $1 AND e.lease_expires_at_ms > db_clock.now_ms
              AND s.execution_profile_id = $2
              AND s.provider_id = $3 AND s.command_schema = $4
              AND s.adapter_revision = $5
            ORDER BY e.started_at_ms, e.created_at_ms, e.executor_execution_id
            LIMIT 1
            "#,
        )
        .bind(owner)
        .bind(scope.execution_profile_id)
        .bind(&scope.provider_id)
        .bind(&scope.command_schema)
        .bind(&scope.adapter_revision)
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
            execution_profile_id: row.execution_profile_id,
            adapter_revision: row.adapter_revision,
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
        let profile = lock_active_execution_profile(&mut tx, scope.execution_profile_id).await?;
        if profile.provider_id != scope.provider_id
            || profile.command_schema != scope.command_schema
            || profile.adapter_revision != scope.adapter_revision
        {
            return Err(ExecutorSubmissionError::Conflict);
        }
        let row: Option<ClaimableRow> = sqlx::query_as(
            r#"
            SELECT s.submission_id, e.executor_execution_id, s.output_id, s.job_id,
                   s.tenant_id, s.provider_id, s.model,
                   s.work_item_id, o.output_index, s.command_schema, s.command_hash,
                   s.execution_profile_id, s.adapter_revision,
                   e.state AS executor_state, e.lease_epoch
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
              AND w.state = 'awaiting_executor'
              AND w.lease_owner IS NULL AND w.lease_expires_at_ms IS NULL
              AND w.execution_profile_id = s.execution_profile_id
              AND w.handed_off_at_ms IS NOT NULL
              AND a.state = 'handed_off'
              AND a.handed_off_at_ms = w.handed_off_at_ms
              AND j.state IN ('reserved', 'queued', 'running')
              AND s.execution_profile_id = $2
              AND s.provider_id = $3 AND s.command_schema = $4
              AND s.adapter_revision = $5
            ORDER BY (e.state = 'leased') DESC, e.created_at_ms, s.job_id, o.output_index
            FOR UPDATE OF e, s SKIP LOCKED
            LIMIT 1
            "#,
        )
        .bind(now)
        .bind(scope.execution_profile_id)
        .bind(&scope.provider_id)
        .bind(&scope.command_schema)
        .bind(&scope.adapter_revision)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some(row) = row else {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        };
        if !ensure_capacity_allocation(&mut tx, &row, &profile, now).await? {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        }
        let executor_lease_epoch = row
            .lease_epoch
            .checked_add(1)
            .ok_or(ExecutorSubmissionError::Unavailable)?;
        let executor_lease_expires_at_ms: i64 = sqlx::query_scalar(
            r#"
            WITH claim_clock AS (
              SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            )
            UPDATE executor_executions
            SET state = 'leased', executor_owner = $2, lease_epoch = $3,
                lease_expires_at_ms = claim_clock.now_ms + $4,
                leased_at_ms = claim_clock.now_ms, updated_at_ms = claim_clock.now_ms
            FROM claim_clock
            WHERE executor_execution_id = $1 AND submission_id = $5
              AND (state = 'prepared'
                OR (state = 'leased' AND lease_expires_at_ms <= claim_clock.now_ms))
            RETURNING executor_executions.lease_expires_at_ms
            "#,
        )
        .bind(row.executor_execution_id)
        .bind(owner)
        .bind(executor_lease_epoch)
        .bind(lease_ms)
        .bind(row.submission_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .ok_or(ExecutorSubmissionError::Unavailable)?;
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
            execution_profile_id: row.execution_profile_id,
            adapter_revision: row.adapter_revision,
            executor_owner: owner.to_string(),
            executor_lease_epoch,
            executor_lease_expires_at_ms,
        }))
    }

    async fn start(&self, lease: &ExecutorSubmissionLease) -> Result<(), ExecutorSubmissionError> {
        validate_executor_lease(lease)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        match lock_handed_off_work(&mut tx, lease).await {
            Ok(()) => {}
            Err(ExecutorSubmissionError::StaleLease) => {
                tx.rollback().await.map_err(unavailable)?;
                return replay_started(&self.pool, lease).await;
            }
            Err(error) => return Err(error),
        }
        let locked = lock_executor_execution(&mut tx, lease).await?;
        lock_held_capacity_allocation(&mut tx, lease).await?;
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
        heartbeat_capacity_allocation(&mut tx, lease, now).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(ExecutorSubmissionLease {
            executor_lease_expires_at_ms,
            ..lease.clone()
        })
    }

    async fn record_outcome(
        &self,
        lease: &ExecutorSubmissionLease,
        outcome: &ExecutorSubmissionOutcome,
    ) -> Result<(), ExecutorSubmissionError> {
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
        let now = database_now(&mut tx).await?;
        let active = locked.state == "running"
            && locked.executor_owner.as_deref() == Some(lease.executor_owner.as_str())
            && locked.lease_epoch == lease.executor_lease_epoch
            && locked
                .lease_expires_at_ms
                .is_some_and(|expires| expires > now);
        if active {
            if lock_submission_state(&mut tx, lease).await? != "running" {
                return Err(ExecutorSubmissionError::Conflict);
            }
            lock_held_capacity_allocation(&mut tx, lease).await?;
            require_one(
                sqlx::query(
                    r#"
                    UPDATE executor_executions
                    SET lease_expires_at_ms = GREATEST(lease_expires_at_ms, $5),
                        updated_at_ms = $6
                    WHERE executor_execution_id = $1 AND submission_id = $2
                      AND executor_owner = $3 AND lease_epoch = $4
                      AND state = 'running' AND lease_expires_at_ms > $6
                    "#,
                )
                .bind(lease.executor_execution_id)
                .bind(lease.submission_id)
                .bind(&lease.executor_owner)
                .bind(lease.executor_lease_epoch)
                .bind(now + FINALIZATION_FENCE_GRACE_MS)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?,
            )?;
            heartbeat_capacity_allocation(&mut tx, lease, now).await?;
        }
        let observation = runner_observation(lease, outcome)?;
        let payload_hash = observation_payload_hash(lease, outcome);
        if let Some(existing) = lock_runner_observation(&mut tx, lease).await? {
            if !stored_observation_matches(&existing, &observation, &payload_hash) {
                return Err(ExecutorSubmissionError::Conflict);
            }
        } else {
            if let Some(manifest) = outcome.manifest() {
                let authority = lock_artifact_authority(&mut tx, lease)
                    .await?
                    .ok_or(ExecutorSubmissionError::Conflict)?;
                if !manifest_matches_artifact_authority(manifest, &authority) {
                    return Err(ExecutorSubmissionError::Conflict);
                }
                insert_result_manifest(&mut tx, lease, manifest, now).await?;
            }
            insert_runner_observation(&mut tx, lease, outcome, &payload_hash, now).await?;
        }
        if matches!(locked.state.as_str(), "succeeded" | "failed" | "uncertain") {
            release_capacity_allocation(
                &mut tx,
                lease.executor_execution_id,
                lease.submission_id,
                locked.state.as_str(),
                "terminal_evidence",
                now,
            )
            .await?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(());
        }
        if !active {
            tx.commit().await.map_err(unavailable)?;
            return Ok(());
        }
        let state = outcome.state();
        let error_code = outcome.error_code();
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
        release_capacity_allocation(
            &mut tx,
            lease.executor_execution_id,
            lease.submission_id,
            state,
            "terminal_evidence",
            now,
        )
        .await?;
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
            SELECT e.executor_execution_id, e.submission_id, e.state AS executor_state,
                   observation.observation_id IS NOT NULL AS has_observation
            FROM executor_executions e
            JOIN provider_submissions s
             ON s.executor_execution_id = e.executor_execution_id
             AND s.submission_id = e.submission_id
            JOIN work_items w ON w.work_item_id = s.work_item_id AND w.job_id = s.job_id
            LEFT JOIN executor_runner_observations observation
              ON observation.executor_execution_id = e.executor_execution_id
             AND observation.submission_id = e.submission_id
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
) -> Result<LockedHandoffCommand, ExecutorSubmissionError> {
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
    let parent: Option<HandoffParentRow> = sqlx::query_as(
        r#"
        SELECT p.command_schema, p.command_json,
               w.state AS work_state, w.lease_owner, w.lease_expires_at_ms,
               w.execution_profile_id, w.handed_off_at_ms AS work_handed_off_at_ms,
               a.state AS attempt_state, a.worker_id,
               a.handed_off_at_ms AS attempt_handed_off_at_ms
        FROM work_items w
        JOIN job_attempts a
          ON a.work_item_id = w.work_item_id
         AND a.execution_id = w.execution_id
         AND a.lease_epoch = w.lease_epoch
        JOIN job_payloads p ON p.job_id = w.job_id
        WHERE w.work_item_id = $1 AND w.job_id = $2
          AND w.execution_id = $3 AND w.lease_epoch = $4
        FOR UPDATE OF w, a
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.job_id)
    .bind(lease.execution_id)
    .bind(lease.lease_epoch)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some(parent) = parent else {
        return Err(ExecutorSubmissionError::StaleLease);
    };
    if parent.worker_id != lease.worker_id {
        return Err(ExecutorSubmissionError::StaleLease);
    }
    let now = database_now(tx).await?;
    let parent_state = if parent.work_state == "leased"
        && parent.attempt_state == "claimed"
        && parent.lease_owner.as_deref() == Some(lease.worker_id.as_str())
        && parent
            .lease_expires_at_ms
            .is_some_and(|expires| expires > now)
        && parent.execution_profile_id.is_none()
        && parent.work_handed_off_at_ms.is_none()
        && parent.attempt_handed_off_at_ms.is_none()
    {
        HandoffParentState::Leased
    } else if parent.work_state == "awaiting_executor"
        && parent.attempt_state == "handed_off"
        && parent.lease_owner.is_none()
        && parent.lease_expires_at_ms.is_none()
        && parent.work_handed_off_at_ms.is_some()
        && parent.work_handed_off_at_ms == parent.attempt_handed_off_at_ms
    {
        HandoffParentState::HandedOff {
            execution_profile_id: parent
                .execution_profile_id
                .ok_or(ExecutorSubmissionError::Conflict)?,
        }
    } else {
        return Err(ExecutorSubmissionError::StaleLease);
    };
    Ok(LockedHandoffCommand {
        command: DurableCommandRow {
            requested_units,
            economics_contract_version,
            tenant_id,
            provider_id,
            model,
            command_schema: parent.command_schema,
            command_json: parent.command_json,
        },
        parent_state,
    })
}

async fn load_execution_profile_by_key(
    pool: &PgPool,
    profile_key: &str,
) -> Result<ExecutionProfileRow, ExecutorSubmissionError> {
    sqlx::query_as(
        r#"
        SELECT profile.execution_profile_id, profile.profile_key,
               profile.provider_id, profile.command_schema, profile.adapter_revision,
               profile.credential_pool_id, profile.provider_account_id,
               profile.credential_ref, profile.credential_revision,
               account.credential_auth_sha256,
               profile.resource_policy_id, profile.resource_policy_revision,
               policy.max_concurrency
        FROM provider_execution_profiles profile
        JOIN provider_credential_pools pool
          ON pool.credential_pool_id = profile.credential_pool_id
         AND pool.provider_id = profile.provider_id
        JOIN provider_accounts account
          ON account.provider_account_id = profile.provider_account_id
         AND account.credential_pool_id = profile.credential_pool_id
         AND account.provider_id = profile.provider_id
         AND account.credential_ref = profile.credential_ref
         AND account.credential_revision = profile.credential_revision
        JOIN executor_resource_policies policy
          ON policy.resource_policy_id = profile.resource_policy_id
         AND policy.revision = profile.resource_policy_revision
        WHERE profile.profile_key = $1
        "#,
    )
    .bind(profile_key)
    .fetch_optional(pool)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorSubmissionError::Conflict)
}

async fn lock_active_execution_profile(
    tx: &mut Transaction<'_, Postgres>,
    execution_profile_id: Uuid,
) -> Result<ExecutionProfileRow, ExecutorSubmissionError> {
    sqlx::query_as(
        r#"
        SELECT profile.execution_profile_id, profile.profile_key,
               profile.provider_id, profile.command_schema, profile.adapter_revision,
               profile.credential_pool_id, profile.provider_account_id,
               profile.credential_ref, profile.credential_revision,
               account.credential_auth_sha256,
               profile.resource_policy_id, profile.resource_policy_revision,
               policy.max_concurrency
        FROM provider_execution_profiles profile
        JOIN provider_credential_pools pool
          ON pool.credential_pool_id = profile.credential_pool_id
         AND pool.provider_id = profile.provider_id
        JOIN provider_accounts account
          ON account.provider_account_id = profile.provider_account_id
         AND account.credential_pool_id = profile.credential_pool_id
         AND account.provider_id = profile.provider_id
         AND account.credential_ref = profile.credential_ref
         AND account.credential_revision = profile.credential_revision
        JOIN executor_resource_policies policy
          ON policy.resource_policy_id = profile.resource_policy_id
         AND policy.revision = profile.resource_policy_revision
        WHERE profile.execution_profile_id = $1
          AND profile.state = 'enabled'
          AND pool.state = 'enabled'
          AND account.state = 'enabled'
          AND policy.state = 'enabled'
        FOR UPDATE OF profile, pool, account, policy
        "#,
    )
    .bind(execution_profile_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorSubmissionError::Conflict)
}

async fn lock_bound_execution_profile(
    tx: &mut Transaction<'_, Postgres>,
    execution_profile_id: Uuid,
) -> Result<ExecutionProfileRow, ExecutorSubmissionError> {
    sqlx::query_as(
        r#"
        SELECT profile.execution_profile_id, profile.profile_key,
               profile.provider_id, profile.command_schema, profile.adapter_revision,
               profile.credential_pool_id, profile.provider_account_id,
               profile.credential_ref, profile.credential_revision,
               account.credential_auth_sha256,
               profile.resource_policy_id, profile.resource_policy_revision,
               policy.max_concurrency
        FROM provider_execution_profiles profile
        JOIN provider_credential_pools pool
          ON pool.credential_pool_id = profile.credential_pool_id
         AND pool.provider_id = profile.provider_id
        JOIN provider_accounts account
          ON account.provider_account_id = profile.provider_account_id
         AND account.credential_pool_id = profile.credential_pool_id
         AND account.provider_id = profile.provider_id
         AND account.credential_ref = profile.credential_ref
         AND account.credential_revision = profile.credential_revision
        JOIN executor_resource_policies policy
          ON policy.resource_policy_id = profile.resource_policy_id
         AND policy.revision = profile.resource_policy_revision
        WHERE profile.execution_profile_id = $1
        FOR UPDATE OF profile, pool, account, policy
        "#,
    )
    .bind(execution_profile_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorSubmissionError::Conflict)
}

async fn bind_work_execution_profile(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    execution_profile_id: Uuid,
) -> Result<(), ExecutorSubmissionError> {
    let changed = sqlx::query(
        r#"
        UPDATE work_items
        SET execution_profile_id = $6
        WHERE work_item_id = $1 AND job_id = $2
          AND execution_id = $3 AND lease_epoch = $4 AND lease_owner = $5
          AND state = 'leased'
          AND (execution_profile_id IS NULL OR execution_profile_id = $6)
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.job_id)
    .bind(lease.execution_id)
    .bind(lease.lease_epoch)
    .bind(&lease.worker_id)
    .bind(execution_profile_id)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    if changed == 1 {
        Ok(())
    } else {
        Err(ExecutorSubmissionError::Conflict)
    }
}

async fn transition_to_executor_handoff(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
) -> Result<(), ExecutorSubmissionError> {
    let now = database_now(tx).await?;
    require_one(
        sqlx::query(
            r#"
            UPDATE job_attempts
            SET state = 'handed_off', handed_off_at_ms = $6, updated_at_ms = $6
            WHERE execution_id = $1 AND work_item_id = $2 AND lease_epoch = $3
              AND worker_id = $4 AND state = 'claimed'
              AND handed_off_at_ms IS NULL
              AND EXISTS (
                SELECT 1 FROM work_items work
                WHERE work.work_item_id = $2 AND work.job_id = $5
                  AND work.execution_id = $1 AND work.lease_epoch = $3
                  AND work.lease_owner = $4 AND work.state = 'leased'
                  AND work.lease_expires_at_ms > $6
              )
            "#,
        )
        .bind(lease.execution_id)
        .bind(lease.work_item_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .bind(lease.job_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE work_items
            SET state = 'awaiting_executor', lease_owner = NULL,
                lease_expires_at_ms = NULL, handed_off_at_ms = $6,
                updated_at_ms = $6
            WHERE work_item_id = $1 AND job_id = $2
              AND execution_id = $3 AND lease_epoch = $4 AND lease_owner = $5
              AND state = 'leased' AND lease_expires_at_ms > $6
              AND execution_profile_id IS NOT NULL
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.job_id)
        .bind(lease.execution_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )
}

async fn prepare_admission_outputs(
    tx: &mut Transaction<'_, Postgres>,
    rows: Vec<ExistingIdentityRow>,
    lease: &WorkLease,
    output_count: i32,
    command_hash: &str,
    command: &DurableCommandRow,
    profile: &ExecutionProfileRow,
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
               execution_profile_id, credential_pool_id, provider_account_id,
               credential_ref, credential_revision, adapter_revision,
               resource_policy_id, resource_policy_revision,
               state, prepared_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, $17, $18, $19, $20,
                    'prepared', $21, $21)
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
        .bind(profile.execution_profile_id)
        .bind(profile.credential_pool_id)
        .bind(profile.provider_account_id)
        .bind(&profile.credential_ref)
        .bind(profile.credential_revision)
        .bind(&profile.adapter_revision)
        .bind(profile.resource_policy_id)
        .bind(profile.resource_policy_revision)
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
            execution_profile_id: profile.execution_profile_id,
            adapter_revision: profile.adapter_revision.clone(),
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
               s.execution_profile_id, s.adapter_revision,
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
    profile: &ExecutionProfileRow,
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
                || row.execution_profile_id != Some(profile.execution_profile_id)
                || row.adapter_revision.as_deref() != Some(profile.adapter_revision.as_str())
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
                execution_profile_id: profile.execution_profile_id,
                adapter_revision: profile.adapter_revision.clone(),
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

async fn ensure_capacity_allocation(
    tx: &mut Transaction<'_, Postgres>,
    row: &ClaimableRow,
    profile: &ExecutionProfileRow,
    now: i64,
) -> Result<bool, ExecutorSubmissionError> {
    let existing: Option<(String, Uuid, Uuid, i64)> = sqlx::query_as(
        r#"
        SELECT state, execution_profile_id, resource_policy_id, resource_policy_revision
        FROM executor_capacity_allocations
        WHERE executor_execution_id = $1 AND submission_id = $2
        FOR UPDATE
        "#,
    )
    .bind(row.executor_execution_id)
    .bind(row.submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    if let Some((state, profile_id, policy_id, policy_revision)) = existing {
        if row.executor_state != "leased"
            || state != "held"
            || profile_id != profile.execution_profile_id
            || policy_id != profile.resource_policy_id
            || policy_revision != profile.resource_policy_revision
        {
            return Err(ExecutorSubmissionError::Conflict);
        }
        sqlx::query(
            r#"
            UPDATE executor_capacity_allocations
            SET last_heartbeat_at_ms = GREATEST(last_heartbeat_at_ms, $3)
            WHERE executor_execution_id = $1 AND submission_id = $2 AND state = 'held'
            "#,
        )
        .bind(row.executor_execution_id)
        .bind(row.submission_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
        return Ok(true);
    }
    if row.executor_state != "prepared" {
        return Err(ExecutorSubmissionError::Conflict);
    }
    let acquired = sqlx::query(
        r#"
        UPDATE executor_resource_policies
        SET allocated_count = allocated_count + 1
        WHERE resource_policy_id = $1 AND revision = $2
          AND state = 'enabled' AND allocated_count < max_concurrency
        "#,
    )
    .bind(profile.resource_policy_id)
    .bind(profile.resource_policy_revision)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    if acquired == 0 {
        return Ok(false);
    }
    require_one(
        sqlx::query(
            r#"
            INSERT INTO executor_capacity_allocations
              (allocation_id, executor_execution_id, submission_id, execution_profile_id,
               resource_policy_id, resource_policy_revision, state,
               acquired_at_ms, last_heartbeat_at_ms)
            VALUES ($1, $1, $2, $3, $4, $5, 'held', $6, $6)
            "#,
        )
        .bind(row.executor_execution_id)
        .bind(row.submission_id)
        .bind(profile.execution_profile_id)
        .bind(profile.resource_policy_id)
        .bind(profile.resource_policy_revision)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )?;
    Ok(true)
}

async fn lock_held_capacity_allocation(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
) -> Result<(), ExecutorSubmissionError> {
    let held: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT TRUE
        FROM executor_capacity_allocations
        WHERE executor_execution_id = $1 AND submission_id = $2
          AND execution_profile_id = $3 AND state = 'held'
        FOR UPDATE
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(lease.execution_profile_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    held.ok_or(ExecutorSubmissionError::Conflict).map(drop)
}

async fn heartbeat_capacity_allocation(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_capacity_allocations
            SET last_heartbeat_at_ms = GREATEST(last_heartbeat_at_ms, $4)
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND execution_profile_id = $3 AND state = 'held'
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .bind(lease.execution_profile_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )
}

pub(crate) async fn release_capacity_allocation(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
    released_state: &str,
    release_reason: &str,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    let allocation: Option<CapacityAllocationRow> = sqlx::query_as(
        r#"
            SELECT state, resource_policy_id, resource_policy_revision,
                   release_decision_id, released_state, release_reason
            FROM executor_capacity_allocations
            WHERE executor_execution_id = $1 AND submission_id = $2
            FOR UPDATE
            "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some(allocation) = allocation else {
        return Err(ExecutorSubmissionError::Conflict);
    };
    if allocation.state == "released" {
        return if allocation.release_decision_id == Some(executor_execution_id)
            && allocation.released_state.as_deref() == Some(released_state)
            && allocation.release_reason.as_deref() == Some(release_reason)
        {
            Ok(())
        } else {
            Err(ExecutorSubmissionError::Conflict)
        };
    }
    if allocation.state != "held" {
        return Err(ExecutorSubmissionError::Conflict);
    }
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_capacity_allocations
            SET state = 'released', released_at_ms = $5,
                release_decision_id = $1, released_state = $3, release_reason = $4,
                last_heartbeat_at_ms = GREATEST(last_heartbeat_at_ms, $5)
            WHERE executor_execution_id = $1 AND submission_id = $2 AND state = 'held'
            "#,
        )
        .bind(executor_execution_id)
        .bind(submission_id)
        .bind(released_state)
        .bind(release_reason)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_resource_policies
            SET allocated_count = allocated_count - 1
            WHERE resource_policy_id = $1 AND revision = $2 AND allocated_count > 0
            "#,
        )
        .bind(allocation.resource_policy_id)
        .bind(allocation.resource_policy_revision)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )?;
    Ok(())
}

async fn lock_handed_off_work(
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
          AND w.state = 'awaiting_executor'
          AND w.lease_owner IS NULL AND w.lease_expires_at_ms IS NULL
          AND w.execution_profile_id = $4
          AND w.handed_off_at_ms IS NOT NULL
          AND a.state = 'handed_off'
          AND a.handed_off_at_ms = w.handed_off_at_ms
          AND j.state IN ('reserved', 'queued', 'running')
        FOR UPDATE OF w, a
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.job_id)
    .bind(lease.submission_id)
    .bind(lease.execution_profile_id)
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
    lock_held_capacity_allocation(&mut tx, lease).await?;
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
          AND s.execution_profile_id = $12 AND s.adapter_revision = $13
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
    .bind(lease.execution_profile_id)
    .bind(&lease.adapter_revision)
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
          AND s.execution_profile_id = $12 AND s.adapter_revision = $13
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
    .bind(lease.execution_profile_id)
    .bind(&lease.adapter_revision)
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
        "executor-runner-observation-v2".to_string(),
        lease.executor_execution_id.to_string(),
        lease.submission_id.to_string(),
        lease.execution_profile_id.to_string(),
        lease.adapter_revision.clone(),
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
    )?;
    if target == "canceled" {
        release_capacity_allocation(
            tx,
            row.executor_execution_id,
            row.submission_id,
            target,
            "executor_start_abandoned",
            now,
        )
        .await?;
    } else if row.has_observation {
        release_capacity_allocation(
            tx,
            row.executor_execution_id,
            row.submission_id,
            target,
            "terminal_evidence",
            now,
        )
        .await?;
    }
    Ok(())
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
