use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::executor::{ExecutorSubmissionError, release_capacity_allocation};

use super::{
    PostgresProviderTaskStore, ProviderExecutionContext, ProviderTaskClaimScope,
    ProviderTaskStoreError,
};

const MAX_LEASE_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_RETRY_AFTER_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCapacityReconciliationState {
    Active,
    Released,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCapacityTerminalState {
    Succeeded,
    Failed,
    Canceled,
}

impl ProviderCapacityTerminalState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderCapacityEvidenceOutcome {
    ConfirmedNoEffect,
    RemoteTerminal {
        remote_operation_id: String,
        terminal_state: ProviderCapacityTerminalState,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCapacityEvidence {
    pub event_identity: String,
    pub outcome: ProviderCapacityEvidenceOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCapacityReconciliation {
    pub reconciliation_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub provider_id: String,
    pub provider_account_id: Uuid,
    pub provider_deadline_at_ms: i64,
    pub state: ProviderCapacityReconciliationState,
    pub available_at_ms: i64,
    pub reconciliation_owner: Option<String>,
    pub reconciliation_lease_epoch: i64,
    pub evidence_revision: i64,
    pub evidence: Option<ProviderCapacityEvidence>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub released_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCapacityReconciliationLease {
    pub reconciliation: ProviderCapacityReconciliation,
    context: ProviderExecutionContext,
    pub reconciliation_owner: String,
    pub reconciliation_lease_epoch: i64,
    pub reconciliation_lease_expires_at_ms: i64,
    pub claimed_evidence_revision: i64,
}

impl ProviderCapacityReconciliationLease {
    pub fn context(&self) -> &ProviderExecutionContext {
        &self.context
    }
}

#[async_trait]
pub trait ProviderCapacityReconciliationStore: Send + Sync + 'static {
    async fn load_capacity_reconciliation(
        &self,
        submission_id: Uuid,
    ) -> Result<Option<ProviderCapacityReconciliation>, ProviderTaskStoreError>;

    async fn claim_due_capacity_reconciliation(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        command_id: &str,
        lease_ms: i64,
    ) -> Result<Option<ProviderCapacityReconciliationLease>, ProviderTaskStoreError>;

    async fn heartbeat_capacity_reconciliation(
        &self,
        lease: &ProviderCapacityReconciliationLease,
        lease_ms: i64,
    ) -> Result<ProviderCapacityReconciliationLease, ProviderTaskStoreError>;

    async fn defer_capacity_reconciliation(
        &self,
        lease: &ProviderCapacityReconciliationLease,
        command_id: &str,
        retry_after_ms: i64,
    ) -> Result<(), ProviderTaskStoreError>;

    async fn record_capacity_evidence(
        &self,
        lease: &ProviderCapacityReconciliationLease,
        evidence: &ProviderCapacityEvidence,
    ) -> Result<ProviderCapacityReconciliation, ProviderTaskStoreError>;
}

#[derive(sqlx::FromRow)]
struct ReconciliationRow {
    reconciliation_id: Uuid,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    provider_deadline_at_ms: i64,
    state: String,
    available_at_ms: i64,
    reconciliation_owner: Option<String>,
    reconciliation_lease_epoch: i64,
    evidence_revision: i64,
    claimed_evidence_revision: Option<i64>,
    last_command_kind: Option<String>,
    last_command_id: Option<String>,
    last_command_owner: Option<String>,
    last_command_lease_epoch: Option<i64>,
    claim_command_claimed_at_ms: Option<i64>,
    claim_command_lease_expires_at_ms: Option<i64>,
    evidence_kind: Option<String>,
    remote_operation_id: Option<String>,
    remote_terminal_state: Option<String>,
    event_identity: Option<String>,
    payload_hash: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
    released_at_ms: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct ContextRow {
    model: String,
    command_schema: String,
    command_hash: String,
    operation_id: String,
    operation_descriptor_revision: String,
    operation_descriptor_sha256_v1: String,
    completion_mode: String,
    idempotency_mode: String,
    operation_binding_version: i16,
    execution_profile_id: Uuid,
    adapter_revision: String,
    credential_pool_id: Uuid,
    credential_ref: String,
    credential_revision: i64,
    credential_auth_sha256: String,
    resource_policy_id: Uuid,
    resource_policy_revision: i64,
    idempotency_key: String,
    provider_command_sha256: String,
    execution_binding_sha256: String,
    invocation_attempt: i32,
    provider_timeout_ms: i64,
    provider_deadline_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct ReleaseParentRow {
    provider_id: String,
    provider_account_id: Uuid,
    execution_state: String,
    submission_state: String,
    resolution_decision_id: Option<Uuid>,
    allocation_state: String,
}

#[derive(sqlx::FromRow)]
struct LockedIntentRow {
    state: String,
    remote_operation_id: Option<String>,
}

#[async_trait]
impl ProviderCapacityReconciliationStore for PostgresProviderTaskStore {
    async fn load_capacity_reconciliation(
        &self,
        submission_id: Uuid,
    ) -> Result<Option<ProviderCapacityReconciliation>, ProviderTaskStoreError> {
        if submission_id.is_nil() {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        load_row(&self.pool, submission_id)
            .await?
            .map(reconciliation_from_row)
            .transpose()
    }

    async fn claim_due_capacity_reconciliation(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        command_id: &str,
        lease_ms: i64,
    ) -> Result<Option<ProviderCapacityReconciliationLease>, ProviderTaskStoreError> {
        validate_scope(scope)?;
        validate_owner_and_duration(owner, lease_ms, MAX_LEASE_MS)?;
        validate_command_id(command_id)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let replay: Option<ReconciliationRow> = sqlx::query_as(
            r#"
            SELECT reconciliation.*
            FROM provider_capacity_reconciliations reconciliation
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = reconciliation.executor_execution_id
             AND allocation.submission_id = reconciliation.submission_id
             AND allocation.state = 'held'
            WHERE reconciliation.provider_id = $1
              AND reconciliation.provider_account_id = $2
              AND reconciliation.state = 'active'
              AND reconciliation.reconciliation_owner = $3
              AND reconciliation.last_command_kind = 'claim'
              AND reconciliation.last_command_id = $4
              AND reconciliation.last_command_owner = $3
              AND reconciliation.last_command_lease_epoch =
                    reconciliation.reconciliation_lease_epoch
            FOR UPDATE OF allocation
            "#,
        )
        .bind(&scope.provider_id)
        .bind(scope.provider_account_id)
        .bind(owner)
        .bind(command_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        if let Some(row) = replay {
            let context = load_context(&mut tx, row.submission_id).await?;
            let lease = replay_lease_from_row(row, context)?;
            tx.commit().await.map_err(storage_conflict)?;
            return Ok(Some(lease));
        }
        let row: Option<ReconciliationRow> = sqlx::query_as(
            r#"
            WITH queue_candidates AS MATERIALIZED (
              SELECT reconciliation.submission_id,
                     reconciliation.executor_execution_id,
                     reconciliation.available_at_ms,
                     reconciliation.provider_deadline_at_ms
              FROM provider_capacity_reconciliations reconciliation
              WHERE reconciliation.provider_id = $1
                AND reconciliation.provider_account_id = $2
                AND reconciliation.state = 'active'
                AND reconciliation.available_at_ms <= floor(
                      extract(epoch FROM statement_timestamp()) * 1000
                    )::BIGINT
              ORDER BY reconciliation.available_at_ms,
                       reconciliation.provider_deadline_at_ms,
                       reconciliation.submission_id
              LIMIT 64
            ), capacity_candidate AS MATERIALIZED (
              SELECT candidate.submission_id,
                     candidate.executor_execution_id,
                     floor(extract(epoch FROM statement_timestamp()) * 1000)::BIGINT AS now_ms
              FROM queue_candidates candidate
              JOIN executor_capacity_allocations allocation
                ON allocation.executor_execution_id = candidate.executor_execution_id
               AND allocation.submission_id = candidate.submission_id
               AND allocation.state = 'held'
              ORDER BY candidate.available_at_ms,
                       candidate.provider_deadline_at_ms,
                       candidate.submission_id
              FOR UPDATE OF allocation SKIP LOCKED
              LIMIT 1
            )
            UPDATE provider_capacity_reconciliations reconciliation
            SET reconciliation_owner = $3,
                reconciliation_lease_epoch = reconciliation.reconciliation_lease_epoch + 1,
                available_at_ms = candidate.now_ms + $5,
                claimed_evidence_revision = reconciliation.evidence_revision,
                last_command_kind = 'claim', last_command_id = $4,
                last_command_owner = $3,
                last_command_lease_epoch = reconciliation.reconciliation_lease_epoch + 1,
                claim_command_claimed_at_ms = candidate.now_ms,
                claim_command_lease_expires_at_ms = candidate.now_ms + $5,
                updated_at_ms = candidate.now_ms
            FROM capacity_candidate candidate
            WHERE reconciliation.submission_id = candidate.submission_id
              AND reconciliation.executor_execution_id = candidate.executor_execution_id
              AND reconciliation.state = 'active'
              AND reconciliation.available_at_ms <= candidate.now_ms
            RETURNING reconciliation.*
            "#,
        )
        .bind(&scope.provider_id)
        .bind(scope.provider_account_id)
        .bind(owner)
        .bind(command_id)
        .bind(lease_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        let Some(row) = row else {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        };
        let context = load_context(&mut tx, row.submission_id).await?;
        heartbeat_capacity(
            &mut tx,
            row.executor_execution_id,
            row.submission_id,
            row.updated_at_ms,
        )
        .await?;
        let lease = lease_from_row(row, context)?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(Some(lease))
    }

    async fn heartbeat_capacity_reconciliation(
        &self,
        lease: &ProviderCapacityReconciliationLease,
        lease_ms: i64,
    ) -> Result<ProviderCapacityReconciliationLease, ProviderTaskStoreError> {
        validate_lease(lease)?;
        validate_owner_and_duration(&lease.reconciliation_owner, lease_ms, MAX_LEASE_MS)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        lock_held_capacity(
            &mut tx,
            lease.reconciliation.executor_execution_id,
            lease.reconciliation.submission_id,
        )
        .await?;
        let now = database_now(&mut tx).await?;
        let row: ReconciliationRow = sqlx::query_as(
            r#"
            UPDATE provider_capacity_reconciliations
            SET available_at_ms = $5 + $6, updated_at_ms = $5
            WHERE reconciliation_id = $1 AND submission_id = $2
              AND reconciliation_owner = $3
              AND reconciliation_lease_epoch = $4
              AND state = 'active' AND available_at_ms > $5
              AND evidence_revision = $7
              AND claimed_evidence_revision = $7
            RETURNING *
            "#,
        )
        .bind(lease.reconciliation.reconciliation_id)
        .bind(lease.reconciliation.submission_id)
        .bind(&lease.reconciliation_owner)
        .bind(lease.reconciliation_lease_epoch)
        .bind(now)
        .bind(lease_ms)
        .bind(lease.claimed_evidence_revision)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_conflict)?
        .ok_or(ProviderTaskStoreError::StaleLease)?;
        heartbeat_capacity(&mut tx, row.executor_execution_id, row.submission_id, now).await?;
        let renewed = lease_from_row(row, lease.context.clone())?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(renewed)
    }

    async fn defer_capacity_reconciliation(
        &self,
        lease: &ProviderCapacityReconciliationLease,
        command_id: &str,
        retry_after_ms: i64,
    ) -> Result<(), ProviderTaskStoreError> {
        validate_lease(lease)?;
        validate_command_id(command_id)?;
        if !(1..=MAX_RETRY_AFTER_MS).contains(&retry_after_ms) {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        lock_held_capacity(
            &mut tx,
            lease.reconciliation.executor_execution_id,
            lease.reconciliation.submission_id,
        )
        .await?;
        let now = database_now(&mut tx).await?;
        let current = load_row_in(&mut tx, lease.reconciliation.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        let exact_replay = current.state == "active"
            && current.reconciliation_owner.is_none()
            && current.last_command_kind.as_deref() == Some("defer")
            && current.last_command_id.as_deref() == Some(command_id)
            && current.last_command_owner.as_deref() == Some(lease.reconciliation_owner.as_str())
            && current.last_command_lease_epoch == Some(lease.reconciliation_lease_epoch);
        if exact_replay {
            tx.commit().await.map_err(storage_conflict)?;
            return Ok(());
        }
        let live = current.state == "active"
            && current.reconciliation_owner.as_deref() == Some(lease.reconciliation_owner.as_str())
            && current.reconciliation_lease_epoch == lease.reconciliation_lease_epoch
            && current.available_at_ms > now;
        if !live {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_capacity_reconciliations
                SET reconciliation_owner = NULL,
                    available_at_ms = CASE
                      WHEN evidence_revision = $7
                       AND claimed_evidence_revision = $7
                      THEN $5 + $6 ELSE $5 END,
                    claimed_evidence_revision = NULL,
                    last_command_kind = 'defer', last_command_id = $8,
                    last_command_owner = $3, last_command_lease_epoch = $4,
                    claim_command_claimed_at_ms = NULL,
                    claim_command_lease_expires_at_ms = NULL,
                    updated_at_ms = $5
                WHERE reconciliation_id = $1 AND submission_id = $2
                  AND reconciliation_owner = $3
                  AND reconciliation_lease_epoch = $4
                  AND state = 'active' AND available_at_ms > $5
                "#,
            )
            .bind(lease.reconciliation.reconciliation_id)
            .bind(lease.reconciliation.submission_id)
            .bind(&lease.reconciliation_owner)
            .bind(lease.reconciliation_lease_epoch)
            .bind(now)
            .bind(retry_after_ms)
            .bind(lease.claimed_evidence_revision)
            .bind(command_id)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::StaleLease,
        )?;
        heartbeat_capacity(
            &mut tx,
            lease.reconciliation.executor_execution_id,
            lease.reconciliation.submission_id,
            now,
        )
        .await?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(())
    }

    async fn record_capacity_evidence(
        &self,
        lease: &ProviderCapacityReconciliationLease,
        evidence: &ProviderCapacityEvidence,
    ) -> Result<ProviderCapacityReconciliation, ProviderTaskStoreError> {
        validate_lease(lease)?;
        validate_evidence(evidence)?;
        let payload_hash = evidence_hash(
            lease.reconciliation.reconciliation_id,
            lease.reconciliation.submission_id,
            evidence,
        );
        let (kind, remote_operation_id, remote_terminal_state) = evidence_values(evidence);
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let parent = lock_release_parent(
            &mut tx,
            lease.reconciliation.executor_execution_id,
            lease.reconciliation.submission_id,
        )
        .await?;
        let intent: LockedIntentRow = sqlx::query_as(
            r#"
            SELECT state, remote_operation_id
            FROM provider_remote_submit_intents
            WHERE submission_id = $1 AND executor_execution_id = $2
            FOR UPDATE
            "#,
        )
        .bind(lease.reconciliation.submission_id)
        .bind(lease.reconciliation.executor_execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .ok_or(ProviderTaskStoreError::NotFound)?;
        let now = database_now(&mut tx).await?;
        let current: ReconciliationRow = load_row_in(&mut tx, lease.reconciliation.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;

        if current.state == "released" {
            let exact = current.reconciliation_owner.as_deref()
                == Some(lease.reconciliation_owner.as_str())
                && current.reconciliation_lease_epoch == lease.reconciliation_lease_epoch
                && current.evidence_kind.as_deref() == Some(kind)
                && current.remote_operation_id.as_deref() == remote_operation_id
                && current.remote_terminal_state.as_deref() == remote_terminal_state
                && current.event_identity.as_deref() == Some(evidence.event_identity.as_str())
                && current.payload_hash.as_deref() == Some(payload_hash.as_str());
            if exact {
                tx.commit().await.map_err(storage_conflict)?;
                return reconciliation_from_row(current);
            }
            return Err(ProviderTaskStoreError::Conflict);
        }

        let live_lease = current.state == "active"
            && current.reconciliation_owner.as_deref() == Some(lease.reconciliation_owner.as_str())
            && current.reconciliation_lease_epoch == lease.reconciliation_lease_epoch
            && current.available_at_ms > now
            && current.evidence_revision == lease.claimed_evidence_revision
            && current.claimed_evidence_revision == Some(lease.claimed_evidence_revision);
        let canonical_parent = parent.provider_id == current.provider_id
            && parent.provider_account_id == current.provider_account_id
            && parent.execution_state == "uncertain"
            && parent.submission_state == "uncertain"
            && parent.resolution_decision_id == Some(lease.reconciliation.executor_execution_id)
            && parent.allocation_state == "held"
            && intent.state == "deadline_quarantined";
        if !live_lease || !canonical_parent {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        if remote_operation_id.is_some()
            && intent.remote_operation_id.as_deref() != remote_operation_id
        {
            return Err(ProviderTaskStoreError::Conflict);
        }

        let released: ReconciliationRow = sqlx::query_as(
            r#"
            UPDATE provider_capacity_reconciliations
            SET state = 'released', evidence_kind = $5,
                remote_operation_id = $6, remote_terminal_state = $7,
                event_identity = $8, payload_hash = $9,
                updated_at_ms = $10, released_at_ms = $10
            WHERE reconciliation_id = $1 AND submission_id = $2
              AND reconciliation_owner = $3
              AND reconciliation_lease_epoch = $4
              AND state = 'active' AND available_at_ms > $10
              AND evidence_revision = $11
              AND claimed_evidence_revision = $11
            RETURNING *
            "#,
        )
        .bind(lease.reconciliation.reconciliation_id)
        .bind(lease.reconciliation.submission_id)
        .bind(&lease.reconciliation_owner)
        .bind(lease.reconciliation_lease_epoch)
        .bind(kind)
        .bind(remote_operation_id)
        .bind(remote_terminal_state)
        .bind(&evidence.event_identity)
        .bind(&payload_hash)
        .bind(now)
        .bind(lease.claimed_evidence_revision)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_conflict)?
        .ok_or(ProviderTaskStoreError::StaleLease)?;

        release_capacity_allocation(
            &mut tx,
            lease.reconciliation.executor_execution_id,
            lease.reconciliation.submission_id,
            "uncertain",
            "provider_capacity_reconciliation",
            Some(lease.reconciliation.reconciliation_id),
            now,
        )
        .await
        .map_err(map_executor_error)?;
        tx.commit().await.map_err(storage_conflict)?;
        reconciliation_from_row(released)
    }
}

pub(super) async fn insert_capacity_reconciliation(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    require_one(
        sqlx::query(
            r#"
            INSERT INTO provider_capacity_reconciliations (
                reconciliation_id, submission_id, executor_execution_id,
                provider_id, provider_account_id, provider_deadline_at_ms,
                state, available_at_ms, reconciliation_owner,
                reconciliation_lease_epoch, evidence_revision,
                created_at_ms, updated_at_ms
            )
            SELECT $1, intent.submission_id, intent.executor_execution_id,
                   intent.provider_id, intent.provider_account_id,
                   recovery.provider_deadline_at_ms, 'active', $3, NULL, 0,
                   CASE WHEN intent.receipt_event_identity IS NULL THEN 0 ELSE 1 END,
                   $3, $3
            FROM provider_remote_submit_intents intent
            JOIN provider_submit_recoveries recovery
              ON recovery.submission_id = intent.submission_id
             AND recovery.executor_execution_id = intent.executor_execution_id
            WHERE intent.executor_execution_id = $1 AND intent.submission_id = $2
              AND intent.state = 'deadline_quarantined'
              AND recovery.state = 'closed'
            ON CONFLICT (reconciliation_id) DO NOTHING
            "#,
        )
        .bind(executor_execution_id)
        .bind(submission_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )
}

async fn load_row(
    pool: &sqlx::PgPool,
    submission_id: Uuid,
) -> Result<Option<ReconciliationRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT *
        FROM provider_capacity_reconciliations
        WHERE submission_id = $1
        "#,
    )
    .bind(submission_id)
    .fetch_optional(pool)
    .await
    .map_err(unavailable)
}

async fn load_row_in(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<Option<ReconciliationRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT *
        FROM provider_capacity_reconciliations
        WHERE submission_id = $1
        FOR UPDATE
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn load_context(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<ProviderExecutionContext, ProviderTaskStoreError> {
    let row: ContextRow = sqlx::query_as(
        r#"
        SELECT submission.model, submission.command_schema,
               submission.command_hash, submission.operation_id,
               submission.operation_descriptor_revision,
               submission.operation_descriptor_sha256_v1,
               submission.completion_mode, submission.idempotency_mode,
               submission.operation_binding_version,
               submission.execution_profile_id,
               submission.adapter_revision, submission.credential_pool_id,
               submission.credential_ref, submission.credential_revision,
               account.credential_auth_sha256,
               submission.resource_policy_id, submission.resource_policy_revision,
               intent.idempotency_key, intent.provider_command_sha256,
               intent.execution_binding_sha256, recovery.invocation_attempt,
               recovery.provider_timeout_ms, recovery.provider_deadline_at_ms
        FROM provider_capacity_reconciliations reconciliation
        JOIN provider_submissions submission
          ON submission.submission_id = reconciliation.submission_id
         AND submission.executor_execution_id = reconciliation.executor_execution_id
        JOIN provider_remote_submit_intents intent
          ON intent.submission_id = reconciliation.submission_id
         AND intent.executor_execution_id = reconciliation.executor_execution_id
        JOIN provider_submit_recoveries recovery
          ON recovery.submission_id = reconciliation.submission_id
         AND recovery.executor_execution_id = reconciliation.executor_execution_id
        JOIN provider_accounts account
          ON account.provider_account_id = submission.provider_account_id
         AND account.credential_pool_id = submission.credential_pool_id
         AND account.provider_id = submission.provider_id
         AND account.credential_ref = submission.credential_ref
         AND account.credential_revision = submission.credential_revision
        WHERE reconciliation.submission_id = $1
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ProviderTaskStoreError::Conflict)?;
    Ok(context_from_row(row))
}

async fn lock_release_parent(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
) -> Result<ReleaseParentRow, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission.provider_id, submission.provider_account_id,
               execution.state AS execution_state,
               submission.state AS submission_state,
               execution.resolution_decision_id,
               allocation.state AS allocation_state
        FROM executor_executions execution
        JOIN provider_submissions submission
          ON submission.executor_execution_id = execution.executor_execution_id
         AND submission.submission_id = execution.submission_id
        JOIN executor_capacity_allocations allocation
          ON allocation.executor_execution_id = execution.executor_execution_id
         AND allocation.submission_id = execution.submission_id
        WHERE execution.executor_execution_id = $1
          AND execution.submission_id = $2
        FOR UPDATE OF execution, submission, allocation
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ProviderTaskStoreError::NotFound)
}

async fn lock_held_capacity(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
) -> Result<(), ProviderTaskStoreError> {
    let held: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT TRUE FROM executor_capacity_allocations
        WHERE executor_execution_id = $1 AND submission_id = $2
          AND state = 'held'
        FOR UPDATE
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    held.ok_or(ProviderTaskStoreError::StaleLease).map(drop)
}

async fn heartbeat_capacity(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_capacity_allocations
            SET last_heartbeat_at_ms = GREATEST(last_heartbeat_at_ms, $3)
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'held'
            "#,
        )
        .bind(executor_execution_id)
        .bind(submission_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
        ProviderTaskStoreError::StaleLease,
    )
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ProviderTaskStoreError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn reconciliation_from_row(
    row: ReconciliationRow,
) -> Result<ProviderCapacityReconciliation, ProviderTaskStoreError> {
    let state = match row.state.as_str() {
        "active" => ProviderCapacityReconciliationState::Active,
        "released" => ProviderCapacityReconciliationState::Released,
        _ => return Err(ProviderTaskStoreError::Conflict),
    };
    let evidence = match row.evidence_kind.as_deref() {
        None => None,
        Some("confirmed_no_effect") => Some(ProviderCapacityEvidence {
            event_identity: row
                .event_identity
                .clone()
                .ok_or(ProviderTaskStoreError::Conflict)?,
            outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
        }),
        Some("remote_terminal") => Some(ProviderCapacityEvidence {
            event_identity: row
                .event_identity
                .clone()
                .ok_or(ProviderTaskStoreError::Conflict)?,
            outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                remote_operation_id: row
                    .remote_operation_id
                    .clone()
                    .ok_or(ProviderTaskStoreError::Conflict)?,
                terminal_state: terminal_state_from_str(
                    row.remote_terminal_state
                        .as_deref()
                        .ok_or(ProviderTaskStoreError::Conflict)?,
                )?,
            },
        }),
        Some(_) => return Err(ProviderTaskStoreError::Conflict),
    };
    Ok(ProviderCapacityReconciliation {
        reconciliation_id: row.reconciliation_id,
        submission_id: row.submission_id,
        executor_execution_id: row.executor_execution_id,
        provider_id: row.provider_id,
        provider_account_id: row.provider_account_id,
        provider_deadline_at_ms: row.provider_deadline_at_ms,
        state,
        available_at_ms: row.available_at_ms,
        reconciliation_owner: row.reconciliation_owner,
        reconciliation_lease_epoch: row.reconciliation_lease_epoch,
        evidence_revision: row.evidence_revision,
        evidence,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
        released_at_ms: row.released_at_ms,
    })
}

fn lease_from_row(
    row: ReconciliationRow,
    context: ProviderExecutionContext,
) -> Result<ProviderCapacityReconciliationLease, ProviderTaskStoreError> {
    let owner = row
        .reconciliation_owner
        .clone()
        .ok_or(ProviderTaskStoreError::Conflict)?;
    let epoch = row.reconciliation_lease_epoch;
    let expires_at_ms = row.available_at_ms;
    let claimed_evidence_revision = row
        .claimed_evidence_revision
        .ok_or(ProviderTaskStoreError::Conflict)?;
    let reconciliation = reconciliation_from_row(row)?;
    Ok(ProviderCapacityReconciliationLease {
        reconciliation,
        context,
        reconciliation_owner: owner,
        reconciliation_lease_epoch: epoch,
        reconciliation_lease_expires_at_ms: expires_at_ms,
        claimed_evidence_revision,
    })
}

fn replay_lease_from_row(
    mut row: ReconciliationRow,
    context: ProviderExecutionContext,
) -> Result<ProviderCapacityReconciliationLease, ProviderTaskStoreError> {
    row.evidence_revision = row
        .claimed_evidence_revision
        .ok_or(ProviderTaskStoreError::Conflict)?;
    row.updated_at_ms = row
        .claim_command_claimed_at_ms
        .ok_or(ProviderTaskStoreError::Conflict)?;
    row.available_at_ms = row
        .claim_command_lease_expires_at_ms
        .ok_or(ProviderTaskStoreError::Conflict)?;
    lease_from_row(row, context)
}

fn context_from_row(row: ContextRow) -> ProviderExecutionContext {
    ProviderExecutionContext {
        model: row.model,
        command_schema: row.command_schema,
        command_hash: row.command_hash,
        operation_id: row.operation_id,
        operation_descriptor_revision: row.operation_descriptor_revision,
        operation_descriptor_sha256_v1: row.operation_descriptor_sha256_v1,
        completion_mode: row.completion_mode,
        idempotency_mode: row.idempotency_mode,
        operation_binding_version: row.operation_binding_version,
        execution_profile_id: row.execution_profile_id,
        adapter_revision: row.adapter_revision,
        credential_pool_id: row.credential_pool_id,
        credential_ref: row.credential_ref,
        credential_revision: row.credential_revision,
        credential_auth_sha256: row.credential_auth_sha256,
        resource_policy_id: row.resource_policy_id,
        resource_policy_revision: row.resource_policy_revision,
        submission_idempotency_key: row.idempotency_key,
        provider_command_sha256: row.provider_command_sha256,
        execution_binding_sha256: row.execution_binding_sha256,
        invocation_attempt: row.invocation_attempt,
        provider_timeout_ms: row.provider_timeout_ms,
        provider_deadline_at_ms: row.provider_deadline_at_ms,
    }
}

fn terminal_state_from_str(
    value: &str,
) -> Result<ProviderCapacityTerminalState, ProviderTaskStoreError> {
    match value {
        "succeeded" => Ok(ProviderCapacityTerminalState::Succeeded),
        "failed" => Ok(ProviderCapacityTerminalState::Failed),
        "canceled" => Ok(ProviderCapacityTerminalState::Canceled),
        _ => Err(ProviderTaskStoreError::Conflict),
    }
}

fn evidence_values(
    evidence: &ProviderCapacityEvidence,
) -> (&'static str, Option<&str>, Option<&'static str>) {
    match &evidence.outcome {
        ProviderCapacityEvidenceOutcome::ConfirmedNoEffect => ("confirmed_no_effect", None, None),
        ProviderCapacityEvidenceOutcome::RemoteTerminal {
            remote_operation_id,
            terminal_state,
        } => (
            "remote_terminal",
            Some(remote_operation_id.as_str()),
            Some(terminal_state.as_str()),
        ),
    }
}

fn evidence_hash(
    reconciliation_id: Uuid,
    submission_id: Uuid,
    evidence: &ProviderCapacityEvidence,
) -> String {
    let (kind, operation, terminal) = evidence_values(evidence);
    let mut hash = Sha256::new();
    for value in [
        reconciliation_id.to_string(),
        submission_id.to_string(),
        kind.to_string(),
        operation.unwrap_or("").to_string(),
        terminal.unwrap_or("").to_string(),
        evidence.event_identity.clone(),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hex::encode(hash.finalize())
}

fn validate_scope(scope: &ProviderTaskClaimScope) -> Result<(), ProviderTaskStoreError> {
    if scope.provider_account_id.is_nil() || !valid_simple_identifier(&scope.provider_id, 128) {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    Ok(())
}

fn validate_owner_and_duration(
    owner: &str,
    duration_ms: i64,
    max_ms: i64,
) -> Result<(), ProviderTaskStoreError> {
    if !valid_identifier(owner, 255) || !(1..=max_ms).contains(&duration_ms) {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    Ok(())
}

fn validate_lease(
    lease: &ProviderCapacityReconciliationLease,
) -> Result<(), ProviderTaskStoreError> {
    if lease.reconciliation.reconciliation_id.is_nil()
        || lease.reconciliation.submission_id.is_nil()
        || lease.reconciliation.executor_execution_id.is_nil()
        || lease.reconciliation.reconciliation_id != lease.reconciliation.executor_execution_id
        || lease.reconciliation.state != ProviderCapacityReconciliationState::Active
        || lease.reconciliation_lease_epoch <= 0
        || lease.reconciliation.reconciliation_lease_epoch != lease.reconciliation_lease_epoch
        || lease.reconciliation.reconciliation_owner.as_deref()
            != Some(lease.reconciliation_owner.as_str())
        || lease.reconciliation.evidence_revision != lease.claimed_evidence_revision
        || !valid_identifier(&lease.reconciliation_owner, 255)
    {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    Ok(())
}

fn validate_evidence(evidence: &ProviderCapacityEvidence) -> Result<(), ProviderTaskStoreError> {
    if !valid_identifier(&evidence.event_identity, 255) {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    if let ProviderCapacityEvidenceOutcome::RemoteTerminal {
        remote_operation_id,
        ..
    } = &evidence.outcome
        && !valid_identifier(remote_operation_id, 255)
    {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    Ok(())
}

fn valid_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && !value.chars().any(char::is_control)
        && value.trim() == value
}

fn validate_command_id(value: &str) -> Result<(), ProviderTaskStoreError> {
    if valid_identifier(value, 255) {
        Ok(())
    } else {
        Err(ProviderTaskStoreError::InvalidInput)
    }
}

fn valid_simple_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn require_one(
    result: sqlx::postgres::PgQueryResult,
    error: ProviderTaskStoreError,
) -> Result<(), ProviderTaskStoreError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(error)
    }
}

fn unavailable(_: sqlx::Error) -> ProviderTaskStoreError {
    ProviderTaskStoreError::Unavailable
}

fn storage_conflict(error: sqlx::Error) -> ProviderTaskStoreError {
    match error.as_database_error().and_then(|error| error.code()) {
        Some(code) if matches!(code.as_ref(), "23503" | "23505" | "23514" | "P0001") => {
            ProviderTaskStoreError::Conflict
        }
        _ => ProviderTaskStoreError::Unavailable,
    }
}

fn map_executor_error(error: ExecutorSubmissionError) -> ProviderTaskStoreError {
    match error {
        ExecutorSubmissionError::Unavailable => ProviderTaskStoreError::Unavailable,
        ExecutorSubmissionError::InvalidInput => ProviderTaskStoreError::InvalidInput,
        ExecutorSubmissionError::Conflict | ExecutorSubmissionError::StaleLease => {
            ProviderTaskStoreError::Conflict
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_hash_is_stable_and_domain_separated() {
        let reconciliation_id = Uuid::from_u128(1);
        let submission_id = Uuid::from_u128(2);
        let first = ProviderCapacityEvidence {
            event_identity: "event-1".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
        };
        let second = ProviderCapacityEvidence {
            event_identity: "event-1".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                remote_operation_id: "operation-1".to_string(),
                terminal_state: ProviderCapacityTerminalState::Failed,
            },
        };
        assert_eq!(
            evidence_hash(reconciliation_id, submission_id, &first),
            evidence_hash(reconciliation_id, submission_id, &first)
        );
        assert_ne!(
            evidence_hash(reconciliation_id, submission_id, &first),
            evidence_hash(reconciliation_id, submission_id, &second)
        );
    }
}
