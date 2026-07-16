use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::artifacts::executor_object_key;
use crate::executor::{
    ExecutorExecutionProfile, ExecutorResultManifest, ExecutorSubmissionError,
    release_capacity_allocation,
};

use super::capacity::insert_capacity_reconciliation;
use super::{
    ProviderArtifactAuthority, ProviderArtifactPublication, ProviderExecutionContext,
    ProviderPollRuntimeProfile, ProviderPollRuntimeProfileStore, ProviderRemoteTask,
    ProviderSubmitAcquire, ProviderSubmitAttachAuthority, ProviderSubmitBusyAuthority,
    ProviderSubmitDispatchAuthority, ProviderSubmitFailureKind, ProviderSubmitIntent,
    ProviderSubmitIntentState, ProviderSubmitInvocation, ProviderSubmitRecoveryLease,
    ProviderSubmitStart, ProviderTaskClaimScope, ProviderTaskDeadlineStore, ProviderTaskLease,
    ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskObservationSource,
    ProviderTaskState, ProviderTaskStore, ProviderTaskStoreError, RemoteTaskAttach,
    RemoteTaskQuarantinedReceipt, RemoteTaskSubmitFailure, RemoteTaskSubmitReceipt,
    RemoteTaskSubmitReservation, VerifiedCallbackWakeup,
};

const MAX_POLL_AFTER_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_LEASE_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_PROVIDER_TIMEOUT_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const SUBMIT_DEADLINE_ERROR: &str = "provider_submit_deadline";
const REMOTE_TASK_DEADLINE_ERROR: &str = "provider_remote_task_deadline";
const ARTIFACT_RECOVERY_EVENT: &str = "internal:artifact-authority-recovery-v1";

#[derive(Clone)]
pub struct PostgresProviderTaskStore {
    pub(super) pool: PgPool,
}

impl PostgresProviderTaskStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct PollRuntimeProfileRow {
    execution_profile_id: Uuid,
    profile_key: String,
    provider_id: String,
    command_schema: String,
    operation_id: String,
    operation_descriptor_revision: String,
    operation_descriptor_sha256_v1: String,
    completion_mode: String,
    idempotency_mode: String,
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

impl From<PollRuntimeProfileRow> for ExecutorExecutionProfile {
    fn from(row: PollRuntimeProfileRow) -> Self {
        Self {
            execution_profile_id: row.execution_profile_id,
            profile_key: row.profile_key,
            provider_id: row.provider_id,
            command_schema: row.command_schema,
            operation_id: row.operation_id,
            operation_descriptor_revision: row.operation_descriptor_revision,
            operation_descriptor_sha256_v1: row.operation_descriptor_sha256_v1,
            completion_mode: row.completion_mode,
            idempotency_mode: row.idempotency_mode,
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

impl ProviderPollRuntimeProfileStore for PostgresProviderTaskStore {
    async fn load_active_poll_runtime_profile(
        &self,
        profile_key: &str,
    ) -> Result<ProviderPollRuntimeProfile, ProviderTaskStoreError> {
        if !valid_simple_identifier(profile_key, 128) {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let row: PollRuntimeProfileRow = sqlx::query_as(
            r#"
            SELECT profile.execution_profile_id, profile.profile_key,
                   profile.provider_id, profile.command_schema,
                   profile.operation_id, profile.operation_descriptor_revision,
                   profile.operation_descriptor_sha256_v1, profile.completion_mode,
                   profile.idempotency_mode, profile.adapter_revision,
                   profile.credential_pool_id, profile.provider_account_id,
                   profile.credential_ref, profile.credential_revision,
                   account.credential_auth_sha256,
                   profile.resource_policy_id, profile.resource_policy_revision,
                   policy.max_concurrency
            FROM provider_execution_profiles profile
            JOIN provider_credential_pools pool
              ON pool.credential_pool_id = profile.credential_pool_id
             AND pool.provider_id = profile.provider_id
             AND pool.state = 'enabled'
            JOIN provider_accounts account
              ON account.provider_account_id = profile.provider_account_id
             AND account.credential_pool_id = profile.credential_pool_id
             AND account.provider_id = profile.provider_id
             AND account.credential_ref = profile.credential_ref
             AND account.credential_revision = profile.credential_revision
             AND account.state = 'enabled'
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = profile.resource_policy_id
             AND policy.revision = profile.resource_policy_revision
             AND policy.credential_pool_id = profile.credential_pool_id
             AND policy.provider_account_id = profile.provider_account_id
             AND policy.provider_id = profile.provider_id
             AND policy.state = 'enabled'
            WHERE profile.profile_key = $1
              AND profile.state = 'enabled'
              AND profile.completion_mode = 'remote_task'
            "#,
        )
        .bind(profile_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?
        .ok_or(ProviderTaskStoreError::NotFound)?;
        ProviderPollRuntimeProfile::new(row.into()).map_err(|_| ProviderTaskStoreError::Conflict)
    }
}

#[derive(sqlx::FromRow)]
struct TaskRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    remote_operation_id: String,
    provider_request_id: Option<String>,
    provider_deadline_at_ms: i64,
    state: String,
    artifact_ref: Option<String>,
    error_code: Option<String>,
    next_poll_at_ms: Option<i64>,
    cancel_requested: bool,
    poll_owner: Option<String>,
    poll_lease_epoch: i64,
    poll_lease_expires_at_ms: Option<i64>,
    state_observation_id: Uuid,
    deadline_quarantine_id: Option<Uuid>,
    attach_recovery_owner: Option<String>,
    attach_recovery_lease_epoch: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct ClaimRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    remote_operation_id: String,
    provider_request_id: Option<String>,
    state: String,
    artifact_ref: Option<String>,
    error_code: Option<String>,
    next_poll_at_ms: Option<i64>,
    cancel_requested: bool,
    poll_lease_epoch: i64,
    state_observation_id: Uuid,
    attach_recovery_owner: Option<String>,
    attach_recovery_lease_epoch: Option<i64>,
    poll_owner: String,
    poll_lease_expires_at_ms: i64,
    claim_updated_at_ms: i64,
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
    committed_manifest_id: Option<Uuid>,
    committed_authority_id: Option<Uuid>,
    committed_sha256_hex: Option<String>,
    committed_byte_size: Option<i64>,
    committed_media_type: Option<String>,
}

#[derive(sqlx::FromRow)]
struct ProviderContextRow {
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
struct RecoveryClaimRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    submit_owner: String,
    submit_lease_epoch: i64,
    idempotency_key: String,
    output_index: i32,
    output_total: i32,
    provider_command_sha256: String,
    execution_binding_sha256: String,
    intent_provider_timeout_ms: i64,
    state: String,
    remote_operation_id: Option<String>,
    provider_request_id: Option<String>,
    send_started_at_ms: Option<i64>,
    receipt_event_identity: Option<String>,
    failure_event_identity: Option<String>,
    failure_error_code: Option<String>,
    updated_at_ms: i64,
    recovery_owner: String,
    recovery_lease_epoch: i64,
    recovery_lease_expires_at_ms: i64,
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
    invocation_attempt: i32,
    provider_timeout_ms: i64,
    provider_deadline_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct RecoveryCommandRow {
    command_kind: String,
    request_duration_ms: i64,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    recovery_lease_epoch: i64,
}

#[derive(sqlx::FromRow)]
struct SubmitDeadlineCandidateRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    database_now_ms: i64,
}

#[derive(sqlx::FromRow)]
struct ExistingObservation {
    observation_id: Uuid,
    source: String,
    observed_state: String,
    artifact_ref: Option<String>,
    result_manifest_id: Option<Uuid>,
    artifact_sha256_hex: Option<String>,
    artifact_byte_size: Option<i64>,
    artifact_media_type: Option<String>,
    error_code: Option<String>,
    effect_certainty: String,
    next_poll_at_ms: Option<i64>,
    poll_owner: Option<String>,
    poll_lease_epoch: Option<i64>,
    payload_hash: String,
    observed_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct ResolutionDecisionRow {
    resolved_state: String,
    result_manifest_id: Option<Uuid>,
    error_code: Option<String>,
    provider_task_observation_id: Option<Uuid>,
    provider_submit_intent_id: Option<Uuid>,
}

#[derive(sqlx::FromRow)]
struct SubmitIntentRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    submit_owner: String,
    submit_lease_epoch: i64,
    idempotency_key: String,
    output_index: i32,
    output_total: i32,
    provider_command_sha256: String,
    execution_binding_sha256: String,
    provider_timeout_ms: i64,
    state: String,
    remote_operation_id: Option<String>,
    provider_request_id: Option<String>,
    send_started_at_ms: Option<i64>,
    receipt_event_identity: Option<String>,
    failure_event_identity: Option<String>,
    failure_error_code: Option<String>,
    updated_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct SubmitBindingRow {
    output_id: Uuid,
    output_index: i32,
    output_total: i32,
    provider_id: String,
    provider_account_id: Uuid,
    submission_state: String,
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
    executor_owner: Option<String>,
    executor_lease_epoch: i64,
    launch_lease_epoch: Option<i64>,
    lease_expires_at_ms: Option<i64>,
    allocation_state: String,
}

#[derive(sqlx::FromRow)]
struct SubmitParentRow {
    provider_id: String,
    provider_account_id: Uuid,
    execution_state: String,
    submission_state: String,
    executor_owner: Option<String>,
    lease_epoch: i64,
    lease_expires_at_ms: Option<i64>,
    launch_owner: Option<String>,
    launch_lease_epoch: Option<i64>,
    allocation_state: String,
    allocation_release_reason: Option<String>,
    allocation_release_reconciliation_id: Option<Uuid>,
    reconciliation_state: Option<String>,
    reconciliation_evidence_kind: Option<String>,
    reconciliation_remote_operation_id: Option<String>,
    execution_resolution_decision_id: Option<Uuid>,
    submission_resolution_decision_id: Option<Uuid>,
    resolution_source: Option<String>,
}

#[derive(sqlx::FromRow)]
struct SubmitRecoveryRow {
    state: String,
    recovery_owner: Option<String>,
    recovery_lease_epoch: i64,
    recovery_lease_expires_at_ms: Option<i64>,
    provider_deadline_at_ms: i64,
}

#[async_trait]
impl ProviderTaskStore for PostgresProviderTaskStore {
    async fn acquire_submit(
        &self,
        request: &RemoteTaskSubmitReservation,
    ) -> Result<ProviderSubmitAcquire, ProviderTaskStoreError> {
        validate_reservation(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let binding: Option<SubmitBindingRow> = sqlx::query_as(
            r#"
                SELECT submission.output_id, job_output.output_index,
                       job.requested_units AS output_total, submission.provider_id,
                       submission.provider_account_id,
                       submission.state AS submission_state, submission.model,
                       submission.command_schema, submission.command_hash,
                       submission.operation_id,
                       submission.operation_descriptor_revision,
                       submission.operation_descriptor_sha256_v1,
                       submission.completion_mode, submission.idempotency_mode,
                       submission.operation_binding_version,
                       submission.execution_profile_id, submission.adapter_revision,
                       submission.credential_pool_id, submission.credential_ref,
                       submission.credential_revision, account.credential_auth_sha256,
                       submission.resource_policy_id, submission.resource_policy_revision,
                       execution.executor_owner,
                       execution.lease_epoch AS executor_lease_epoch,
                       execution.launch_lease_epoch, execution.lease_expires_at_ms,
                       allocation.state AS allocation_state
                FROM executor_executions execution
                JOIN provider_submissions submission
                  ON submission.executor_execution_id = execution.executor_execution_id
                 AND submission.submission_id = execution.submission_id
                JOIN job_outputs job_output
                  ON job_output.output_id = submission.output_id
                 AND job_output.job_id = submission.job_id
                JOIN jobs job ON job.job_id = submission.job_id
                JOIN provider_accounts account
                  ON account.provider_account_id = submission.provider_account_id
                 AND account.credential_pool_id = submission.credential_pool_id
                 AND account.provider_id = submission.provider_id
                 AND account.credential_ref = submission.credential_ref
                 AND account.credential_revision = submission.credential_revision
                JOIN executor_capacity_allocations allocation
                  ON allocation.executor_execution_id = execution.executor_execution_id
                 AND allocation.submission_id = execution.submission_id
                WHERE execution.executor_execution_id = $1
                  AND execution.submission_id = $2
                FOR UPDATE OF execution, submission, allocation
                "#,
        )
        .bind(request.executor_execution_id)
        .bind(request.submission_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some(binding) = binding else {
            return Err(ProviderTaskStoreError::NotFound);
        };
        if !submit_output_matches_binding(&binding, request) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let provider_command_sha256 = provider_command_sha256(request);

        if let Some(row) = load_submit_intent_in(&mut tx, request.submission_id).await? {
            let frozen_timeout_ms = row.provider_timeout_ms;
            let execution_binding_sha256 = execution_binding_sha256(
                &binding,
                request,
                &provider_command_sha256,
                frozen_timeout_ms,
            );
            if !submit_intent_matches_acquire(&row, request)
                || row.execution_binding_sha256 != execution_binding_sha256
            {
                return Err(ProviderTaskStoreError::Conflict);
            }
            if row.state == "reserved" {
                let now = database_now(&mut tx).await?;
                if !submit_binding_is_live(&binding, request, now) {
                    return Err(ProviderTaskStoreError::StaleLease);
                }
                transition_submit_to_sending(&mut tx, request, now).await?;
                insert_submit_recovery(&mut tx, request, frozen_timeout_ms, now).await?;
                let invocation =
                    load_submit_invocation_in(&mut tx, request, frozen_timeout_ms).await?;
                let remaining_budget_ms = submit_remaining_budget(&mut tx, &invocation).await?;
                tx.commit().await.map_err(unavailable)?;
                return Ok(ProviderSubmitAcquire::Dispatch(
                    ProviderSubmitDispatchAuthority {
                        invocation,
                        remaining_budget_ms,
                    },
                ));
            }

            let intent = submit_intent_from_row(row)?;
            let acquired = match intent.state {
                ProviderSubmitIntentState::Sending => {
                    let invocation = load_submit_observation_in(&mut tx, intent).await?;
                    let remaining_budget_ms = submit_remaining_budget(&mut tx, &invocation).await?;
                    ProviderSubmitAcquire::Busy(ProviderSubmitBusyAuthority {
                        invocation,
                        remaining_budget_ms,
                    })
                }
                ProviderSubmitIntentState::OutcomeUnknown => ProviderSubmitAcquire::ObserveOnly(
                    load_submit_observation_in(&mut tx, intent).await?,
                ),
                ProviderSubmitIntentState::OperationKnown => {
                    let context = load_provider_context_in(&mut tx, request.submission_id)
                        .await?
                        .ok_or(ProviderTaskStoreError::Conflict)?;
                    ProviderSubmitAcquire::AttachOnly(ProviderSubmitAttachAuthority {
                        invocation: ProviderSubmitInvocation {
                            intent,
                            context: provider_context_from_row(context),
                        },
                    })
                }
                ProviderSubmitIntentState::Attached
                | ProviderSubmitIntentState::Rejected
                | ProviderSubmitIntentState::DeadlineQuarantined => {
                    ProviderSubmitAcquire::Terminal(intent)
                }
                ProviderSubmitIntentState::Reserved => unreachable!(),
            };
            tx.commit().await.map_err(unavailable)?;
            return Ok(acquired);
        }

        let now = database_now(&mut tx).await?;
        if !submit_binding_is_live(&binding, request, now) {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let execution_binding_sha256 = execution_binding_sha256(
            &binding,
            request,
            &provider_command_sha256,
            request.provider_timeout_ms,
        );
        sqlx::query(
            r#"
            INSERT INTO provider_remote_submit_intents
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               submit_owner, submit_lease_epoch, idempotency_key, state,
               output_index, output_total,
               provider_command_sha256, execution_binding_sha256,
               provider_timeout_ms, send_started_at_ms, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'sending', $8, $9,
                    $10, $11, $12, $13, $13, $13)
            "#,
        )
        .bind(request.submission_id)
        .bind(request.executor_execution_id)
        .bind(&binding.provider_id)
        .bind(binding.provider_account_id)
        .bind(&request.executor_owner)
        .bind(request.executor_lease_epoch)
        .bind(&request.idempotency_key)
        .bind(
            i32::try_from(request.output_index())
                .map_err(|_| ProviderTaskStoreError::InvalidInput)?,
        )
        .bind(
            i32::try_from(request.output_total())
                .map_err(|_| ProviderTaskStoreError::InvalidInput)?,
        )
        .bind(&provider_command_sha256)
        .bind(&execution_binding_sha256)
        .bind(request.provider_timeout_ms)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        insert_submit_recovery(&mut tx, request, request.provider_timeout_ms, now).await?;
        let invocation =
            load_submit_invocation_in(&mut tx, request, request.provider_timeout_ms).await?;
        let remaining_budget_ms = submit_remaining_budget(&mut tx, &invocation).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(ProviderSubmitAcquire::Dispatch(
            ProviderSubmitDispatchAuthority {
                invocation,
                remaining_budget_ms,
            },
        ))
    }

    async fn reserve_submit(
        &self,
        request: &RemoteTaskSubmitReservation,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
        validate_reservation(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let binding: Option<SubmitBindingRow> = sqlx::query_as(
            r#"
                SELECT submission.output_id, job_output.output_index,
                       job.requested_units AS output_total, submission.provider_id,
                       submission.provider_account_id,
                       submission.state AS submission_state, submission.model,
                       submission.command_schema, submission.command_hash,
                       submission.operation_id,
                       submission.operation_descriptor_revision,
                       submission.operation_descriptor_sha256_v1,
                       submission.completion_mode, submission.idempotency_mode,
                       submission.operation_binding_version,
                       submission.execution_profile_id, submission.adapter_revision,
                       submission.credential_pool_id, submission.credential_ref,
                       submission.credential_revision, account.credential_auth_sha256,
                       submission.resource_policy_id, submission.resource_policy_revision,
                       execution.executor_owner,
                       execution.lease_epoch AS executor_lease_epoch,
                       execution.launch_lease_epoch, execution.lease_expires_at_ms,
                       allocation.state AS allocation_state
                FROM executor_executions execution
                JOIN provider_submissions submission
                  ON submission.executor_execution_id = execution.executor_execution_id
                 AND submission.submission_id = execution.submission_id
                JOIN job_outputs job_output
                  ON job_output.output_id = submission.output_id
                 AND job_output.job_id = submission.job_id
                JOIN jobs job ON job.job_id = submission.job_id
                JOIN provider_accounts account
                  ON account.provider_account_id = submission.provider_account_id
                 AND account.credential_pool_id = submission.credential_pool_id
                 AND account.provider_id = submission.provider_id
                 AND account.credential_ref = submission.credential_ref
                 AND account.credential_revision = submission.credential_revision
                JOIN executor_capacity_allocations allocation
                  ON allocation.executor_execution_id = execution.executor_execution_id
                 AND allocation.submission_id = execution.submission_id
                WHERE execution.executor_execution_id = $1
                  AND execution.submission_id = $2
                FOR UPDATE OF execution, submission, allocation
                "#,
        )
        .bind(request.executor_execution_id)
        .bind(request.submission_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some(binding) = binding else {
            return Err(ProviderTaskStoreError::NotFound);
        };
        if !submit_output_matches_binding(&binding, request) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let provider_command_sha256 = provider_command_sha256(request);
        let execution_binding_sha256 = execution_binding_sha256(
            &binding,
            request,
            &provider_command_sha256,
            request.provider_timeout_ms,
        );
        if let Some(existing) = load_submit_intent_in(&mut tx, request.submission_id).await? {
            let matches = existing.executor_execution_id == request.executor_execution_id
                && existing.submit_owner == request.executor_owner
                && existing.submit_lease_epoch == request.executor_lease_epoch
                && existing.idempotency_key == request.idempotency_key
                && existing.provider_command_sha256 == provider_command_sha256
                && existing.execution_binding_sha256 == execution_binding_sha256
                && existing.provider_timeout_ms == request.provider_timeout_ms;
            if !matches {
                return Err(ProviderTaskStoreError::Conflict);
            }
            let intent = submit_intent_from_row(existing)?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(intent);
        }
        let now = database_now(&mut tx).await?;
        if binding.submission_state != "running"
            || binding.executor_owner.as_deref() != Some(request.executor_owner.as_str())
            || binding.executor_lease_epoch != request.executor_lease_epoch
            || binding.launch_lease_epoch != Some(request.executor_lease_epoch)
            || binding.lease_expires_at_ms.is_none_or(|value| value <= now)
            || binding.allocation_state != "held"
        {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        sqlx::query(
            r#"
            INSERT INTO provider_remote_submit_intents
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               submit_owner, submit_lease_epoch, idempotency_key, state,
               output_index, output_total,
               provider_command_sha256, execution_binding_sha256,
               provider_timeout_ms, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'reserved', $8, $9,
                    $10, $11, $12, $13, $13)
            "#,
        )
        .bind(request.submission_id)
        .bind(request.executor_execution_id)
        .bind(&binding.provider_id)
        .bind(binding.provider_account_id)
        .bind(&request.executor_owner)
        .bind(request.executor_lease_epoch)
        .bind(&request.idempotency_key)
        .bind(
            i32::try_from(request.output_index())
                .map_err(|_| ProviderTaskStoreError::InvalidInput)?,
        )
        .bind(
            i32::try_from(request.output_total())
                .map_err(|_| ProviderTaskStoreError::InvalidInput)?,
        )
        .bind(&provider_command_sha256)
        .bind(&execution_binding_sha256)
        .bind(request.provider_timeout_ms)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        let intent = submit_intent_from_row(
            load_submit_intent_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::NotFound)?,
        )?;
        tx.commit().await.map_err(unavailable)?;
        Ok(intent)
    }

    async fn start_submit(
        &self,
        request: &RemoteTaskSubmitReservation,
    ) -> Result<ProviderSubmitStart, ProviderTaskStoreError> {
        validate_reservation(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        if !lock_live_submit_fence(&mut tx, request).await? {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let row = load_submit_intent_in(&mut tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if !submit_intent_matches_reservation(&row, request) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let intent = submit_intent_from_row(row)?;
        if intent.state != ProviderSubmitIntentState::Reserved {
            let context = load_provider_context_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::Conflict)?;
            if context.provider_timeout_ms != request.provider_timeout_ms {
                return Err(ProviderTaskStoreError::Conflict);
            }
            tx.commit().await.map_err(unavailable)?;
            return Ok(ProviderSubmitStart::Existing(ProviderSubmitInvocation {
                intent,
                context: provider_context_from_row(context),
            }));
        }
        let now = database_now(&mut tx).await?;
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET state = 'sending', send_started_at_ms = $3, updated_at_ms = $3
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND state = 'reserved'
                "#,
            )
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        sqlx::query(
            r#"
            INSERT INTO provider_submit_recoveries
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               invocation_attempt, provider_timeout_ms, provider_deadline_at_ms,
               next_recovery_at_ms, state, created_at_ms, updated_at_ms)
            SELECT intent.submission_id, intent.executor_execution_id,
                   intent.provider_id, intent.provider_account_id,
                   1, $3, $4 + $3,
                   LEAST(execution.lease_expires_at_ms, $4 + $3),
                   'active', $4, $4
            FROM provider_remote_submit_intents intent
            JOIN executor_executions execution
              ON execution.executor_execution_id = intent.executor_execution_id
             AND execution.submission_id = intent.submission_id
            WHERE intent.submission_id = $1
              AND intent.executor_execution_id = $2
              AND intent.state = 'sending'
            "#,
        )
        .bind(request.submission_id)
        .bind(request.executor_execution_id)
        .bind(request.provider_timeout_ms)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        let intent = submit_intent_from_row(
            load_submit_intent_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::NotFound)?,
        )?;
        let context = provider_context_from_row(
            load_provider_context_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::Conflict)?,
        );
        tx.commit().await.map_err(unavailable)?;
        Ok(ProviderSubmitStart::Acquired(ProviderSubmitInvocation {
            intent,
            context,
        }))
    }

    async fn record_submit_failure(
        &self,
        request: &RemoteTaskSubmitFailure,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
        validate_submit_failure(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let parent = lock_submit_parent(
            &mut tx,
            request.executor_execution_id,
            request.submission_id,
        )
        .await?;
        let row = load_submit_intent_in(&mut tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if row.executor_execution_id != request.executor_execution_id
            || row.submit_owner != request.executor_owner
            || row.submit_lease_epoch != request.executor_lease_epoch
            || row.execution_binding_sha256 != request.execution_binding_sha256
        {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let target_state = match request.kind {
            ProviderSubmitFailureKind::Rejected => "rejected",
            ProviderSubmitFailureKind::OutcomeUnknown => "outcome_unknown",
        };
        if row.state != "sending" {
            let intent = submit_intent_from_row(row)?;
            let compatible_state = match request.kind {
                ProviderSubmitFailureKind::Rejected => {
                    intent.state == ProviderSubmitIntentState::Rejected
                }
                ProviderSubmitFailureKind::OutcomeUnknown => matches!(
                    intent.state,
                    ProviderSubmitIntentState::OutcomeUnknown
                        | ProviderSubmitIntentState::OperationKnown
                        | ProviderSubmitIntentState::Attached
                        | ProviderSubmitIntentState::DeadlineQuarantined
                ),
            };
            let replay = compatible_state
                && intent.failure_event_identity.as_deref() == Some(&request.event_identity)
                && intent.failure_error_code.as_deref() == Some(&request.error_code);
            if replay {
                tx.commit().await.map_err(unavailable)?;
                return Ok(intent);
            }
            return Err(ProviderTaskStoreError::Conflict);
        }
        if !submit_parent_accepts_evidence(&parent, &row) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let now = database_now(&mut tx).await?;
        if request.kind == ProviderSubmitFailureKind::Rejected {
            close_submit_recovery(
                &mut tx,
                request.submission_id,
                request.executor_execution_id,
                now,
                request.recovery_fence.as_ref(),
            )
            .await?;
        }
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET state = $3, failure_event_identity = $4,
                    failure_error_code = $5, updated_at_ms = $6
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND state = 'sending'
                "#,
            )
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(target_state)
            .bind(&request.event_identity)
            .bind(&request.error_code)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        let intent = submit_intent_from_row(
            load_submit_intent_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::NotFound)?,
        )?;
        if request.kind == ProviderSubmitFailureKind::Rejected {
            resolve_submit_terminal(&mut tx, &intent, "failed", now).await?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(intent)
    }

    async fn record_submit_receipt(
        &self,
        request: &RemoteTaskSubmitReceipt,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
        validate_submit_receipt(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let parent = lock_submit_parent(
            &mut tx,
            request.executor_execution_id,
            request.submission_id,
        )
        .await?;
        let row = load_submit_intent_in(&mut tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if row.executor_execution_id != request.executor_execution_id
            || row.submit_owner != request.executor_owner
            || row.submit_lease_epoch != request.executor_lease_epoch
            || row.execution_binding_sha256 != request.execution_binding_sha256
        {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let can_append_receipt = matches!(row.state.as_str(), "sending" | "outcome_unknown")
            || (row.state == "deadline_quarantined"
                && row.remote_operation_id.is_none()
                && row.provider_request_id.is_none()
                && row.receipt_event_identity.is_none());
        if !can_append_receipt {
            let intent = submit_intent_from_row(row)?;
            let replay = matches!(
                intent.state,
                ProviderSubmitIntentState::OperationKnown
                    | ProviderSubmitIntentState::Attached
                    | ProviderSubmitIntentState::DeadlineQuarantined
            ) && intent.remote_operation_id.as_deref()
                == Some(&request.remote_operation_id)
                && intent.provider_request_id == request.provider_request_id
                && intent.receipt_event_identity.as_deref() == Some(&request.event_identity);
            if replay {
                tx.commit().await.map_err(unavailable)?;
                return Ok(intent);
            }
            return Err(ProviderTaskStoreError::Conflict);
        }
        let parent_accepts_receipt = if row.state == "deadline_quarantined" {
            submit_parent_accepts_late_receipt(&parent, &row)
        } else {
            submit_parent_accepts_evidence(&parent, &row)
        };
        if !parent_accepts_receipt {
            return Err(ProviderTaskStoreError::Conflict);
        }
        if parent.reconciliation_state.as_deref() == Some("released")
            && parent.reconciliation_evidence_kind.as_deref() == Some("remote_terminal")
            && parent.reconciliation_remote_operation_id.as_deref()
                != Some(request.remote_operation_id.as_str())
        {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let now = database_now(&mut tx).await?;
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET state = CASE
                      WHEN state = 'deadline_quarantined' THEN state
                      ELSE 'operation_known'
                    END,
                    remote_operation_id = $3,
                    provider_request_id = $4, receipt_event_identity = $5,
                    updated_at_ms = $6
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND (
                    state IN ('sending', 'outcome_unknown')
                    OR (state = 'deadline_quarantined'
                        AND remote_operation_id IS NULL
                        AND provider_request_id IS NULL
                        AND receipt_event_identity IS NULL)
                  )
                "#,
            )
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(&request.remote_operation_id)
            .bind(&request.provider_request_id)
            .bind(&request.event_identity)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        if row.state == "deadline_quarantined"
            && parent.reconciliation_state.as_deref() == Some("active")
        {
            require_one(
                sqlx::query(
                    r#"
                    UPDATE provider_capacity_reconciliations
                    SET evidence_revision = 1,
                        available_at_ms = CASE
                          WHEN reconciliation_owner IS NULL
                          THEN LEAST(available_at_ms, $3)
                          ELSE available_at_ms
                        END,
                        updated_at_ms = $3
                    WHERE submission_id = $1 AND executor_execution_id = $2
                      AND state = 'active' AND evidence_revision = 0
                    "#,
                )
                .bind(request.submission_id)
                .bind(request.executor_execution_id)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(storage_conflict)?,
                ProviderTaskStoreError::Conflict,
            )?;
        }
        let intent = submit_intent_from_row(
            load_submit_intent_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::NotFound)?,
        )?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(intent)
    }

    async fn quarantine_submit_receipt(
        &self,
        request: &RemoteTaskQuarantinedReceipt,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
        validate_quarantined_receipt(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let parent = lock_submit_parent(
            &mut tx,
            request.executor_execution_id,
            request.submission_id,
        )
        .await?;
        let row = load_submit_intent_in(&mut tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if row.executor_execution_id != request.executor_execution_id
            || row.submit_owner != request.executor_owner
            || row.submit_lease_epoch != request.executor_lease_epoch
            || row.provider_id != request.expected_provider_id
            || row.execution_binding_sha256 != request.execution_binding_sha256
            || !submit_parent_accepts_evidence(&parent, &row)
        {
            return Err(ProviderTaskStoreError::Conflict);
        }
        if row.state == "outcome_unknown" {
            if row.failure_event_identity.as_deref() != Some(&request.event_identity)
                || row.failure_error_code.as_deref() != Some(&request.reason)
                || !quarantined_receipt_matches(&mut tx, request).await?
            {
                return Err(ProviderTaskStoreError::Conflict);
            }
            let intent = submit_intent_from_row(row)?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(intent);
        }
        if row.state != "sending" {
            return Err(ProviderTaskStoreError::Conflict);
        }

        let now = database_now(&mut tx).await?;
        require_one(
            sqlx::query(
                r#"
                INSERT INTO provider_submit_quarantined_receipts
                  (event_identity, submission_id, executor_execution_id,
                   execution_binding_sha256, expected_provider_id,
                   observed_provider_id, observed_submission_id,
                   remote_operation_id, provider_request_id, reason, recorded_at_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (event_identity) DO NOTHING
                "#,
            )
            .bind(&request.event_identity)
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(&request.execution_binding_sha256)
            .bind(&request.expected_provider_id)
            .bind(&request.observed_provider_id)
            .bind(&request.observed_submission_id)
            .bind(&request.remote_operation_id)
            .bind(&request.provider_request_id)
            .bind(&request.reason)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET state = 'outcome_unknown', failure_event_identity = $3,
                    failure_error_code = $4, updated_at_ms = $5
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND state = 'sending'
                "#,
            )
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(&request.event_identity)
            .bind(&request.reason)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        let intent = submit_intent_from_row(
            load_submit_intent_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::NotFound)?,
        )?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(intent)
    }

    async fn load_submit_intent(
        &self,
        submission_id: Uuid,
    ) -> Result<Option<ProviderSubmitIntent>, ProviderTaskStoreError> {
        if submission_id.is_nil() {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        load_submit_intent(&self.pool, submission_id)
            .await?
            .map(submit_intent_from_row)
            .transpose()
    }

    async fn resolve_due_submit_deadline(
        &self,
        scope: &ProviderTaskClaimScope,
    ) -> Result<Option<ProviderSubmitIntent>, ProviderTaskStoreError> {
        validate_scope(scope)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let candidate: Option<SubmitDeadlineCandidateRow> = sqlx::query_as(
            r#"
            SELECT recovery.submission_id, recovery.executor_execution_id,
                   floor(
                     extract(epoch FROM statement_timestamp()) * 1000
                   )::BIGINT AS database_now_ms
            FROM provider_submit_recoveries recovery
            JOIN provider_remote_submit_intents intent
              ON intent.submission_id = recovery.submission_id
             AND intent.executor_execution_id = recovery.executor_execution_id
            JOIN executor_executions execution
              ON execution.executor_execution_id = recovery.executor_execution_id
             AND execution.submission_id = recovery.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = recovery.executor_execution_id
             AND submission.submission_id = recovery.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = recovery.executor_execution_id
             AND allocation.submission_id = recovery.submission_id
            WHERE recovery.provider_id = $1
              AND recovery.provider_account_id = $2
              AND recovery.state = 'active'
              AND recovery.provider_deadline_at_ms <= floor(
                    extract(epoch FROM statement_timestamp()) * 1000
                  )::BIGINT
              AND intent.state IN (
                    'sending', 'outcome_unknown', 'operation_known'
                  )
              AND execution.state = 'running'
              AND submission.state = 'running'
              AND allocation.state = 'held'
            ORDER BY recovery.provider_deadline_at_ms, recovery.submission_id
            FOR UPDATE OF execution, submission, allocation SKIP LOCKED
            LIMIT 1
            "#,
        )
        .bind(&scope.provider_id)
        .bind(scope.provider_account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some(candidate) = candidate else {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        };

        let row = load_submit_intent_in(&mut tx, candidate.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::Conflict)?;
        let recovery = load_submit_recovery_in(&mut tx, candidate.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::Conflict)?;
        if row.executor_execution_id != candidate.executor_execution_id
            || row.provider_id != scope.provider_id
            || row.provider_account_id != scope.provider_account_id
            || !matches!(
                row.state.as_str(),
                "sending" | "outcome_unknown" | "operation_known"
            )
            || recovery.state != "active"
            || recovery.provider_deadline_at_ms > candidate.database_now_ms
            || recovery
                .recovery_lease_expires_at_ms
                .is_some_and(|expiry| expiry > candidate.database_now_ms)
        {
            return Err(ProviderTaskStoreError::Conflict);
        }

        require_one(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET state = 'deadline_quarantined', updated_at_ms = $3
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND state IN ('sending', 'outcome_unknown', 'operation_known')
                "#,
            )
            .bind(candidate.submission_id)
            .bind(candidate.executor_execution_id)
            .bind(candidate.database_now_ms)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        close_submit_recovery(
            &mut tx,
            candidate.submission_id,
            candidate.executor_execution_id,
            candidate.database_now_ms,
            None,
        )
        .await?;
        resolve_submit_deadline_terminal(
            &mut tx,
            candidate.executor_execution_id,
            candidate.submission_id,
            candidate.database_now_ms,
        )
        .await?;
        insert_capacity_reconciliation(
            &mut tx,
            candidate.executor_execution_id,
            candidate.submission_id,
            candidate.database_now_ms,
        )
        .await?;
        let intent = submit_intent_from_row(
            load_submit_intent_in(&mut tx, candidate.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::Conflict)?,
        )?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(Some(intent))
    }

    async fn claim_submit_recovery(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        command_id: &str,
        lease_ms: i64,
    ) -> Result<Option<ProviderSubmitRecoveryLease>, ProviderTaskStoreError> {
        validate_scope(scope)?;
        validate_owner_and_lease(owner, lease_ms)?;
        validate_command_id(command_id)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        lock_recovery_command(&mut tx, scope, owner, command_id).await?;
        if let Some(command) = load_recovery_command_in(&mut tx, scope, owner, command_id).await? {
            if command.command_kind != "claim" || command.request_duration_ms != lease_ms {
                return Err(ProviderTaskStoreError::Conflict);
            }
            let row = load_recovery_claim_command_in(&mut tx, scope, owner, command_id).await?;
            let lease = recovery_lease_from_row(row)?;
            tx.commit().await.map_err(storage_conflict)?;
            return Ok(Some(lease));
        }

        let claimed: Option<(Uuid, Uuid, i64, i64, i64)> = sqlx::query_as(
            r#"
            WITH queue_candidates AS MATERIALIZED (
              SELECT recovery.submission_id, recovery.executor_execution_id,
                     GREATEST(
                       recovery.next_recovery_at_ms,
                       COALESCE(
                         recovery.recovery_lease_expires_at_ms,
                         recovery.next_recovery_at_ms
                       )
                     ) AS effective_due_at_ms,
                     recovery.provider_deadline_at_ms,
                     floor(extract(epoch FROM statement_timestamp()) * 1000)::BIGINT AS now_ms
              FROM provider_submit_recoveries recovery
              WHERE recovery.provider_id = $1
                AND recovery.provider_account_id = $2
                AND recovery.state = 'active'
                AND GREATEST(
                      recovery.next_recovery_at_ms,
                      COALESCE(
                        recovery.recovery_lease_expires_at_ms,
                        recovery.next_recovery_at_ms
                      )
                    ) <= floor(
                      extract(epoch FROM statement_timestamp()) * 1000
                    )::BIGINT
              ORDER BY
                GREATEST(
                  recovery.next_recovery_at_ms,
                  COALESCE(
                    recovery.recovery_lease_expires_at_ms,
                    recovery.next_recovery_at_ms
                  )
                ),
                recovery.provider_deadline_at_ms,
                recovery.submission_id
              LIMIT 64
            ), capacity_candidate AS MATERIALIZED (
              SELECT candidate.submission_id, candidate.executor_execution_id,
                     candidate.now_ms
              FROM queue_candidates candidate
              JOIN executor_capacity_allocations allocation
                ON allocation.executor_execution_id = candidate.executor_execution_id
               AND allocation.submission_id = candidate.submission_id
               AND allocation.state = 'held'
              JOIN executor_executions execution
                ON execution.executor_execution_id = candidate.executor_execution_id
               AND execution.submission_id = candidate.submission_id
               AND execution.state = 'running'
              WHERE candidate.provider_deadline_at_ms > candidate.now_ms
              ORDER BY candidate.effective_due_at_ms,
                       candidate.provider_deadline_at_ms,
                       candidate.submission_id
              FOR UPDATE OF allocation SKIP LOCKED
              LIMIT 1
            ), candidate AS MATERIALIZED (
              SELECT recovery.submission_id, capacity_candidate.now_ms
              FROM provider_submit_recoveries recovery
              JOIN capacity_candidate
                ON capacity_candidate.submission_id = recovery.submission_id
               AND capacity_candidate.executor_execution_id = recovery.executor_execution_id
              WHERE recovery.state = 'active'
                AND recovery.provider_deadline_at_ms > capacity_candidate.now_ms
                AND GREATEST(
                      recovery.next_recovery_at_ms,
                      COALESCE(
                        recovery.recovery_lease_expires_at_ms,
                        recovery.next_recovery_at_ms
                      )
                    ) <= capacity_candidate.now_ms
              FOR UPDATE OF recovery SKIP LOCKED
            ), claimed AS (
              UPDATE provider_submit_recoveries recovery
              SET recovery_owner = $3,
                  recovery_lease_epoch = recovery.recovery_lease_epoch + 1,
                  recovery_lease_expires_at_ms = LEAST(
                    recovery.provider_deadline_at_ms,
                    candidate.now_ms + $4
                  ),
                  recovery_claimed_at_ms = candidate.now_ms,
                  updated_at_ms = candidate.now_ms
              FROM candidate
              WHERE recovery.submission_id = candidate.submission_id
              RETURNING recovery.submission_id, recovery.executor_execution_id,
                        recovery.recovery_lease_epoch,
                        recovery.recovery_claimed_at_ms,
                        recovery.recovery_lease_expires_at_ms
            )
            SELECT submission_id, executor_execution_id, recovery_lease_epoch,
                   recovery_claimed_at_ms, recovery_lease_expires_at_ms
            FROM claimed
            "#,
        )
        .bind(&scope.provider_id)
        .bind(scope.provider_account_id)
        .bind(owner)
        .bind(lease_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        let Some((
            submission_id,
            executor_execution_id,
            recovery_lease_epoch,
            claimed_at_ms,
            lease_expires_at_ms,
        )) = claimed
        else {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        };
        heartbeat_capacity(&mut tx, executor_execution_id, submission_id, claimed_at_ms).await?;
        insert_recovery_claim_command(
            &mut tx,
            scope,
            owner,
            command_id,
            lease_ms,
            submission_id,
            executor_execution_id,
            recovery_lease_epoch,
            claimed_at_ms,
            lease_expires_at_ms,
        )
        .await?;
        let row = load_recovery_claim_command_in(&mut tx, scope, owner, command_id).await?;
        let lease = recovery_lease_from_row(row)?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(Some(lease))
    }

    async fn heartbeat_submit_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
        lease_ms: i64,
    ) -> Result<ProviderSubmitRecoveryLease, ProviderTaskStoreError> {
        validate_recovery_lease(lease, lease_ms)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        lock_held_capacity(
            &mut tx,
            lease.intent.executor_execution_id,
            lease.intent.submission_id,
        )
        .await?;
        let locked: Option<bool> = sqlx::query_scalar(
            r#"
            SELECT TRUE
            FROM provider_submit_recoveries recovery
            WHERE recovery.submission_id = $1
              AND recovery.executor_execution_id = $2
            FOR UPDATE
            "#,
        )
        .bind(lease.intent.submission_id)
        .bind(lease.intent.executor_execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if locked.is_none() {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let now = database_now(&mut tx).await?;
        let renewed: Option<(i64, i64)> = sqlx::query_as(
            r#"
            UPDATE provider_submit_recoveries recovery
            SET recovery_lease_expires_at_ms = LEAST(
                  recovery.provider_deadline_at_ms,
                  GREATEST(
                    recovery.recovery_lease_expires_at_ms + 1,
                    $5 + $6
                  )
                ),
                updated_at_ms = $5
            WHERE recovery.submission_id = $1
              AND recovery.executor_execution_id = $2
              AND recovery.recovery_owner = $3
              AND recovery.recovery_lease_epoch = $4
              AND recovery.state = 'active'
              AND recovery.recovery_lease_expires_at_ms > $5
              AND recovery.provider_deadline_at_ms > $5
            RETURNING recovery.recovery_lease_expires_at_ms, recovery.updated_at_ms
            "#,
        )
        .bind(lease.intent.submission_id)
        .bind(lease.intent.executor_execution_id)
        .bind(&lease.recovery_owner)
        .bind(lease.recovery_lease_epoch)
        .bind(now)
        .bind(lease_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let (expires, now) = renewed.ok_or(ProviderTaskStoreError::StaleLease)?;
        heartbeat_capacity(
            &mut tx,
            lease.intent.executor_execution_id,
            lease.intent.submission_id,
            now,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(ProviderSubmitRecoveryLease {
            recovery_lease_expires_at_ms: expires,
            ..lease.clone()
        })
    }

    async fn defer_submit_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
        command_id: &str,
        retry_after_ms: i64,
    ) -> Result<(), ProviderTaskStoreError> {
        validate_recovery_lease(lease, 1)?;
        validate_command_id(command_id)?;
        if !(1..=MAX_POLL_AFTER_MS).contains(&retry_after_ms) {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let scope = ProviderTaskClaimScope {
            provider_id: lease.intent.provider_id.clone(),
            provider_account_id: lease.intent.provider_account_id,
        };
        lock_recovery_command(&mut tx, &scope, &lease.recovery_owner, command_id).await?;
        if let Some(command) =
            load_recovery_command_in(&mut tx, &scope, &lease.recovery_owner, command_id).await?
        {
            if command.command_kind == "defer"
                && command.request_duration_ms == retry_after_ms
                && command.submission_id == lease.intent.submission_id
                && command.executor_execution_id == lease.intent.executor_execution_id
                && command.recovery_lease_epoch == lease.recovery_lease_epoch
            {
                tx.commit().await.map_err(storage_conflict)?;
                return Ok(());
            }
            return Err(ProviderTaskStoreError::Conflict);
        }
        lock_held_capacity(
            &mut tx,
            lease.intent.executor_execution_id,
            lease.intent.submission_id,
        )
        .await?;
        let now = database_now(&mut tx).await?;
        let current: Option<(String, Option<String>, i64)> = sqlx::query_as(
            r#"
            SELECT state, recovery_owner, recovery_lease_epoch
            FROM provider_submit_recoveries
            WHERE submission_id = $1 AND executor_execution_id = $2
            FOR UPDATE
            "#,
        )
        .bind(lease.intent.submission_id)
        .bind(lease.intent.executor_execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some((state, recovery_owner, recovery_lease_epoch)) = current else {
            return Err(ProviderTaskStoreError::NotFound);
        };
        if state != "active"
            || recovery_owner.as_deref() != Some(lease.recovery_owner.as_str())
            || recovery_lease_epoch != lease.recovery_lease_epoch
        {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        insert_recovery_defer_command(&mut tx, lease, command_id, retry_after_ms, now).await?;
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_submit_recoveries
                SET recovery_owner = NULL,
                    recovery_lease_expires_at_ms = NULL,
                    recovery_claimed_at_ms = NULL,
                    next_recovery_at_ms = LEAST(
                      provider_deadline_at_ms,
                      $5 + $6
                    ),
                    updated_at_ms = $5
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND recovery_owner = $3 AND recovery_lease_epoch = $4
                  AND state = 'active'
                  AND recovery_lease_expires_at_ms > $5
                  AND provider_deadline_at_ms > $5
                "#,
            )
            .bind(lease.intent.submission_id)
            .bind(lease.intent.executor_execution_id)
            .bind(&lease.recovery_owner)
            .bind(lease.recovery_lease_epoch)
            .bind(now)
            .bind(retry_after_ms)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::StaleLease,
        )?;
        heartbeat_capacity(
            &mut tx,
            lease.intent.executor_execution_id,
            lease.intent.submission_id,
            now,
        )
        .await?;
        tx.commit().await.map_err(storage_conflict)
    }

    async fn attach(
        &self,
        request: &RemoteTaskAttach,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        validate_attach(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let parent = lock_submit_parent(
            &mut tx,
            request.executor_execution_id,
            request.submission_id,
        )
        .await?;
        let intent = load_submit_intent_in(&mut tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::Conflict)?;
        if intent.executor_execution_id != request.executor_execution_id
            || intent.provider_id != parent.provider_id
            || intent.provider_account_id != parent.provider_account_id
            || intent.submit_owner != request.executor_owner
            || intent.submit_lease_epoch != request.executor_lease_epoch
            || intent.execution_binding_sha256 != request.execution_binding_sha256
            || !matches!(intent.state.as_str(), "operation_known" | "attached")
            || intent.remote_operation_id.as_deref() != Some(request.remote_operation_id.as_str())
            || intent.provider_request_id != request.provider_request_id
        {
            return Err(ProviderTaskStoreError::Conflict);
        }
        if let Some(existing) = load_task_snapshot_in(&mut tx, request.submission_id).await? {
            let requested_recovery_owner = request
                .recovery_fence
                .as_ref()
                .map(|fence| fence.recovery_owner.as_str());
            let requested_recovery_epoch = request
                .recovery_fence
                .as_ref()
                .map(|fence| fence.recovery_lease_epoch);
            if existing.attach_recovery_owner.as_deref() != requested_recovery_owner
                || existing.attach_recovery_lease_epoch != requested_recovery_epoch
            {
                return Err(ProviderTaskStoreError::StaleLease);
            }
            let task = task_from_row(existing)?;
            if task.executor_execution_id == request.executor_execution_id
                && task.remote_operation_id == request.remote_operation_id
                && task.provider_request_id == request.provider_request_id
            {
                tx.commit().await.map_err(unavailable)?;
                return Ok(task);
            }
            return Err(ProviderTaskStoreError::Conflict);
        }
        if intent.state != "operation_known" {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let now = database_now(&mut tx).await?;
        let recovery = load_submit_recovery_in(&mut tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::Conflict)?;
        if !submit_parent_accepts_evidence(&parent, &intent) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let has_attach_authority = match &request.recovery_fence {
            None => {
                parent
                    .lease_expires_at_ms
                    .is_some_and(|expiry| expiry > now)
                    && recovery.provider_deadline_at_ms > now
                    && recovery.recovery_owner.is_none()
            }
            Some(fence) => {
                recovery.provider_deadline_at_ms > now
                    && recovery.recovery_owner.as_deref() == Some(&fence.recovery_owner)
                    && recovery.recovery_lease_epoch == fence.recovery_lease_epoch
                    && recovery
                        .recovery_lease_expires_at_ms
                        .is_some_and(|expiry| expiry > now)
            }
        };
        if recovery.state != "active" || !has_attach_authority {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let next_poll_at_ms = (now + request.poll_after_ms).min(recovery.provider_deadline_at_ms);
        let observation_id = Uuid::new_v4();
        let payload_hash = observation_hash(
            "submit_attach",
            "provider_waiting",
            None,
            None,
            None,
            None,
            None,
            None,
            "not_applicable",
            Some(next_poll_at_ms),
            None,
            None,
        );
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET state = 'attached', updated_at_ms = $3
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND state = 'operation_known'
                "#,
            )
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        sqlx::query(
            r#"
            INSERT INTO provider_remote_tasks
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               remote_operation_id, provider_request_id, submit_owner, submit_lease_epoch,
               attach_recovery_owner, attach_recovery_lease_epoch,
               provider_deadline_at_ms, state, effect_certainty, next_poll_at_ms,
               state_observation_id,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, 'provider_waiting', 'not_applicable', $12, $13, $14, $14)
            "#,
        )
        .bind(request.submission_id)
        .bind(request.executor_execution_id)
        .bind(&parent.provider_id)
        .bind(parent.provider_account_id)
        .bind(&request.remote_operation_id)
        .bind(&request.provider_request_id)
        .bind(&request.executor_owner)
        .bind(request.executor_lease_epoch)
        .bind(
            request
                .recovery_fence
                .as_ref()
                .map(|fence| fence.recovery_owner.as_str()),
        )
        .bind(
            request
                .recovery_fence
                .as_ref()
                .map(|fence| fence.recovery_lease_epoch),
        )
        .bind(recovery.provider_deadline_at_ms)
        .bind(next_poll_at_ms)
        .bind(observation_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        insert_observation(
            &mut tx,
            observation_id,
            request.submission_id,
            request.executor_execution_id,
            &request.event_identity,
            "submit_attach",
            "provider_waiting",
            None,
            None,
            "not_applicable",
            Some(next_poll_at_ms),
            None,
            None,
            &payload_hash,
            now,
        )
        .await?;
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_submit_recoveries
                SET state = 'closed', next_recovery_at_ms = NULL,
                    recovery_owner = NULL, recovery_lease_expires_at_ms = NULL,
                    recovery_claimed_at_ms = NULL,
                    updated_at_ms = $5, closed_at_ms = $5
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND state = 'active'
                  AND (
                    ($3::TEXT IS NULL AND recovery_owner IS NULL)
                    OR (recovery_owner = $3 AND recovery_lease_epoch = $4)
                  )
                "#,
            )
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(
                request
                    .recovery_fence
                    .as_ref()
                    .map(|fence| fence.recovery_owner.as_str()),
            )
            .bind(
                request
                    .recovery_fence
                    .as_ref()
                    .map(|fence| fence.recovery_lease_epoch),
            )
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::StaleLease,
        )?;
        require_one(
            sqlx::query(
                r#"
                UPDATE executor_executions
                SET state = 'provider_waiting', executor_owner = NULL,
                    lease_expires_at_ms = NULL, updated_at_ms = $5
                WHERE executor_execution_id = $1 AND submission_id = $2
                  AND state = 'running' AND executor_owner = $3 AND lease_epoch = $4
                "#,
            )
            .bind(request.executor_execution_id)
            .bind(request.submission_id)
            .bind(&request.executor_owner)
            .bind(request.executor_lease_epoch)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?,
            ProviderTaskStoreError::Conflict,
        )?;
        require_one(
            sqlx::query(
                "UPDATE provider_submissions SET state = 'provider_waiting', updated_at_ms = $2 WHERE submission_id = $1 AND state = 'running'",
            )
            .bind(request.submission_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?,
            ProviderTaskStoreError::Conflict,
        )?;
        heartbeat_capacity(
            &mut tx,
            request.executor_execution_id,
            request.submission_id,
            now,
        )
        .await?;
        let task = task_from_row(load_task_in(&mut tx, request.submission_id).await?.unwrap())?;
        tx.commit().await.map_err(unavailable)?;
        Ok(task)
    }

    async fn load(
        &self,
        submission_id: Uuid,
    ) -> Result<Option<ProviderRemoteTask>, ProviderTaskStoreError> {
        if submission_id.is_nil() {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        load_task(&self.pool, submission_id)
            .await?
            .map(task_from_row)
            .transpose()
    }

    async fn claim_due(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ProviderTaskLease>, ProviderTaskStoreError> {
        validate_scope(scope)?;
        validate_owner_and_lease(owner, lease_ms)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let row: Option<ClaimRow> = sqlx::query_as(
            r#"
            WITH db_clock AS MATERIALIZED (
              SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            ), candidate_window AS MATERIALIZED (
              SELECT task.submission_id, task.executor_execution_id,
                     GREATEST(
                       task.next_poll_at_ms,
                       COALESCE(task.poll_lease_expires_at_ms, task.next_poll_at_ms)
                     ) AS due_at,
                     task.provider_deadline_at_ms
              FROM provider_remote_tasks task
              CROSS JOIN db_clock
              WHERE task.provider_id = $1 AND task.provider_account_id = $2
                AND task.state = 'provider_waiting'
                AND GREATEST(
                      task.next_poll_at_ms,
                      COALESCE(task.poll_lease_expires_at_ms, task.next_poll_at_ms)
                    ) <= db_clock.now_ms
              ORDER BY due_at, task.submission_id
              LIMIT 64
            ), candidate AS (
              SELECT task.submission_id, task.executor_execution_id, db_clock.now_ms
              FROM candidate_window candidates
              JOIN provider_remote_tasks task
                ON task.submission_id = candidates.submission_id
               AND task.executor_execution_id = candidates.executor_execution_id
              JOIN executor_capacity_allocations allocation
                ON allocation.executor_execution_id = task.executor_execution_id
               AND allocation.submission_id = task.submission_id
               AND allocation.state = 'held'
              CROSS JOIN db_clock
              WHERE task.provider_id = $1 AND task.provider_account_id = $2
                AND task.state = 'provider_waiting'
                AND task.provider_deadline_at_ms > db_clock.now_ms
                AND GREATEST(
                      task.next_poll_at_ms,
                      COALESCE(task.poll_lease_expires_at_ms, task.next_poll_at_ms)
                    ) <= db_clock.now_ms
              ORDER BY candidates.due_at, candidates.submission_id
              FOR UPDATE OF task SKIP LOCKED
              LIMIT 1
            ), claimed AS (
              UPDATE provider_remote_tasks task
              SET poll_owner = $3, poll_lease_epoch = task.poll_lease_epoch + 1,
                  poll_lease_expires_at_ms = LEAST(
                    task.provider_deadline_at_ms,
                    candidate.now_ms + $4
                  ),
                  poll_claimed_at_ms = candidate.now_ms, updated_at_ms = candidate.now_ms
              FROM candidate
              WHERE task.submission_id = candidate.submission_id
              RETURNING task.*
            )
            SELECT claimed.submission_id, claimed.executor_execution_id,
                   claimed.provider_id, claimed.provider_account_id,
                   claimed.remote_operation_id, claimed.provider_request_id,
                   claimed.state, claimed.artifact_ref, claimed.error_code,
                   claimed.next_poll_at_ms, claimed.cancel_requested,
                   claimed.poll_lease_epoch, claimed.state_observation_id,
                   claimed.attach_recovery_owner,
                   claimed.attach_recovery_lease_epoch,
                   claimed.poll_owner, claimed.poll_lease_expires_at_ms,
                   claimed.updated_at_ms AS claim_updated_at_ms,
                   submission.model, submission.command_schema,
                   submission.command_hash, submission.operation_id,
                   submission.operation_descriptor_revision,
                   submission.operation_descriptor_sha256_v1,
                   submission.completion_mode, submission.idempotency_mode,
                   submission.operation_binding_version,
                   submission.execution_profile_id,
                   submission.adapter_revision, submission.credential_pool_id,
                   submission.credential_ref, submission.credential_revision,
                   account.credential_auth_sha256,
                   submission.resource_policy_id,
                   submission.resource_policy_revision,
                   intent.idempotency_key, intent.provider_command_sha256,
                   intent.execution_binding_sha256, recovery.invocation_attempt,
                   recovery.provider_timeout_ms, claimed.provider_deadline_at_ms,
                   committed_manifest.manifest_id AS committed_manifest_id,
                   committed_manifest.artifact_authority_id AS committed_authority_id,
                   committed_authority.sha256_hex AS committed_sha256_hex,
                   committed_authority.byte_size AS committed_byte_size,
                   committed_authority.media_type AS committed_media_type
            FROM claimed
            JOIN provider_submissions submission
              ON submission.submission_id = claimed.submission_id
             AND submission.executor_execution_id = claimed.executor_execution_id
            JOIN provider_remote_submit_intents intent
              ON intent.submission_id = claimed.submission_id
             AND intent.executor_execution_id = claimed.executor_execution_id
             AND intent.state = 'attached'
            JOIN provider_submit_recoveries recovery
              ON recovery.submission_id = claimed.submission_id
             AND recovery.executor_execution_id = claimed.executor_execution_id
             AND recovery.state = 'closed'
             AND recovery.provider_deadline_at_ms = claimed.provider_deadline_at_ms
            JOIN provider_accounts account
              ON account.provider_account_id = submission.provider_account_id
             AND account.credential_pool_id = submission.credential_pool_id
             AND account.provider_id = submission.provider_id
             AND account.credential_ref = submission.credential_ref
             AND account.credential_revision = submission.credential_revision
            LEFT JOIN executor_result_manifests committed_manifest
              ON committed_manifest.submission_id = claimed.submission_id
             AND committed_manifest.executor_execution_id = claimed.executor_execution_id
            LEFT JOIN executor_artifact_authorities committed_authority
              ON committed_authority.authority_id =
                   committed_manifest.artifact_authority_id
             AND committed_authority.submission_id =
                   committed_manifest.submission_id
             AND committed_authority.executor_execution_id =
                   committed_manifest.executor_execution_id
            "#,
        )
        .bind(&scope.provider_id)
        .bind(scope.provider_account_id)
        .bind(owner)
        .bind(lease_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some(row) = row else {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        };
        heartbeat_capacity(
            &mut tx,
            row.executor_execution_id,
            row.submission_id,
            row.claim_updated_at_ms,
        )
        .await?;
        let lease = lease_from_row(row)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Some(lease))
    }

    async fn heartbeat(
        &self,
        lease: &ProviderTaskLease,
        lease_ms: i64,
    ) -> Result<ProviderTaskLease, ProviderTaskStoreError> {
        validate_lease(lease, lease_ms)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let locked: Option<bool> = sqlx::query_scalar(
            r#"
            SELECT TRUE
            FROM provider_remote_tasks task
            WHERE task.submission_id = $1
              AND task.executor_execution_id = $2
            FOR UPDATE
            "#,
        )
        .bind(lease.task.submission_id)
        .bind(lease.task.executor_execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if locked.is_none() {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let now = database_now(&mut tx).await?;
        let renewed: Option<(i64, i64)> = sqlx::query_as(
            r#"
            UPDATE provider_remote_tasks task
            SET poll_lease_expires_at_ms = LEAST(
                  task.provider_deadline_at_ms,
                  GREATEST(
                    task.poll_lease_expires_at_ms + 1,
                    $5 + $6
                  )
                ),
                updated_at_ms = $5
            WHERE task.submission_id = $1
              AND task.executor_execution_id = $2
              AND task.poll_owner = $3 AND task.poll_lease_epoch = $4
              AND task.state = 'provider_waiting'
              AND task.poll_lease_expires_at_ms > $5
              AND task.provider_deadline_at_ms > $5
            RETURNING task.poll_lease_expires_at_ms, task.updated_at_ms
            "#,
        )
        .bind(lease.task.submission_id)
        .bind(lease.task.executor_execution_id)
        .bind(&lease.poll_owner)
        .bind(lease.poll_lease_epoch)
        .bind(now)
        .bind(lease_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let (expires, now) = renewed.ok_or(ProviderTaskStoreError::StaleLease)?;
        heartbeat_capacity(
            &mut tx,
            lease.task.executor_execution_id,
            lease.task.submission_id,
            now,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(ProviderTaskLease {
            remaining_budget_ms: remaining_budget_ms(lease.context.provider_deadline_at_ms, now)?,
            poll_lease_expires_at_ms: expires,
            ..lease.clone()
        })
    }

    async fn request_cancel(
        &self,
        submission_id: Uuid,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        if submission_id.is_nil() {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let task = load_task_in(&mut tx, submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if task.state != "provider_waiting" || task.cancel_requested {
            let task = task_from_row(task)?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(task);
        }
        let now = database_now(&mut tx).await?;
        if task.provider_deadline_at_ms <= now {
            let task = resolve_remote_task_deadline_in(&mut tx, task, now).await?;
            let task = task_from_row(task)?;
            tx.commit().await.map_err(storage_conflict)?;
            return Ok(task);
        }
        let row: TaskRow = sqlx::query_as(
            r#"
            UPDATE provider_remote_tasks
            SET cancel_requested = TRUE,
                cancel_requested_at_ms = $2,
                next_poll_at_ms = LEAST(next_poll_at_ms, $2),
                updated_at_ms = $2
            WHERE submission_id = $1 AND state = 'provider_waiting'
              AND cancel_requested = FALSE
            RETURNING submission_id, executor_execution_id, provider_id,
                      provider_account_id, remote_operation_id, provider_request_id,
                      provider_deadline_at_ms, state, artifact_ref, error_code,
                      next_poll_at_ms,
                      cancel_requested, poll_owner, poll_lease_epoch,
                      poll_lease_expires_at_ms, state_observation_id,
                      deadline_quarantine_id,
                      attach_recovery_owner, attach_recovery_lease_epoch
            "#,
        )
        .bind(submission_id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        let task = task_from_row(row)?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(task)
    }

    async fn record_observation(
        &self,
        lease: &ProviderTaskLease,
        observation: &ProviderTaskObservation,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        validate_lease(lease, 1)?;
        validate_observation(observation)?;
        validate_artifact_observation_binding(lease, observation)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let task = load_task_in(&mut tx, lease.task.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::StaleLease)?;
        let now = database_now(&mut tx).await?;
        let mut values = observation_values(observation, now);
        if let Some(next_poll_at_ms) = values.next_poll_at_ms.as_mut() {
            *next_poll_at_ms = (*next_poll_at_ms).min(task.provider_deadline_at_ms);
        }
        let publication = values.publication;
        let requested_poll_after_ms = match &observation.outcome {
            ProviderTaskObservationOutcome::Waiting { poll_after_ms } => Some(*poll_after_ms),
            _ => None,
        };
        let payload_hash = observation_hash(
            values.source,
            values.state,
            values.artifact_ref,
            publication.map(|value| value.manifest.manifest_id()),
            publication.map(|value| value.sha256_hex.as_str()),
            publication.map(|value| value.byte_size),
            publication.map(|value| value.media_type.as_str()),
            values.error_code,
            values.effect_certainty,
            requested_poll_after_ms.or(values.next_poll_at_ms),
            Some(&lease.poll_owner),
            Some(lease.poll_lease_epoch),
        );
        let new_observation = NewObservation {
            submission_id: lease.task.submission_id,
            executor_execution_id: lease.task.executor_execution_id,
            event_identity: &observation.event_identity,
            source: values.source,
            state: values.state,
            artifact_ref: values.artifact_ref,
            result_manifest_id: publication.map(|value| value.manifest.manifest_id()),
            artifact_sha256_hex: publication.map(|value| value.sha256_hex.as_str()),
            artifact_byte_size: publication
                .map(|value| i64::try_from(value.byte_size))
                .transpose()
                .map_err(|_| ProviderTaskStoreError::InvalidInput)?,
            artifact_media_type: publication.map(|value| value.media_type.as_str()),
            error_code: values.error_code,
            effect_certainty: values.effect_certainty,
            next_poll_at_ms: values.next_poll_at_ms,
            poll_owner: Some(&lease.poll_owner),
            poll_lease_epoch: Some(lease.poll_lease_epoch),
            payload_hash: &payload_hash,
            requested_poll_after_ms,
            now,
        };
        if let Some(persisted) = load_matching_observation(&mut tx, &new_observation).await? {
            if task.state != values.state
                || task.artifact_ref.as_deref() != values.artifact_ref
                || task.error_code.as_deref() != values.error_code
            {
                return Err(ProviderTaskStoreError::Conflict);
            }
            values.next_poll_at_ms = persisted.next_poll_at_ms;
            resolve_observed_terminal(&mut tx, &task, values.publication, now).await?;
            let task = task_from_row(task)?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(task);
        }
        if task.executor_execution_id != lease.task.executor_execution_id
            || task.state != "provider_waiting"
            || task.poll_owner.as_deref() != Some(lease.poll_owner.as_str())
            || task.poll_lease_epoch != lease.poll_lease_epoch
            || task
                .poll_lease_expires_at_ms
                .is_none_or(|expires_at_ms| expires_at_ms <= now)
            || task.provider_deadline_at_ms <= now
        {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let persisted = insert_or_load_observation(&mut tx, &new_observation).await?;
        values.next_poll_at_ms = persisted.next_poll_at_ms;
        let observation_id = persisted.observation_id;
        let changed = sqlx::query(
            r#"
            UPDATE provider_remote_tasks
            SET state = $5, artifact_ref = $6, error_code = $7,
                effect_certainty = $8, next_poll_at_ms = $9,
                poll_owner = NULL, poll_lease_expires_at_ms = NULL,
                poll_claimed_at_ms = NULL, state_observation_id = $10,
                updated_at_ms = $11,
                terminal_at_ms = CASE WHEN $5 = 'provider_waiting' THEN NULL ELSE $11 END
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND poll_owner = $3 AND poll_lease_epoch = $4
              AND state = 'provider_waiting' AND poll_lease_expires_at_ms > $11
            "#,
        )
        .bind(lease.task.submission_id)
        .bind(lease.task.executor_execution_id)
        .bind(&lease.poll_owner)
        .bind(lease.poll_lease_epoch)
        .bind(values.state)
        .bind(values.artifact_ref)
        .bind(values.error_code)
        .bind(values.effect_certainty)
        .bind(values.next_poll_at_ms)
        .bind(observation_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?
        .rows_affected();
        if changed != 1 {
            let existing = load_task_in(&mut tx, lease.task.submission_id).await?;
            if existing.as_ref().is_some_and(|row| {
                row.state == values.state
                    && row.artifact_ref.as_deref() == values.artifact_ref
                    && row.error_code.as_deref() == values.error_code
            }) {
                let row = existing.unwrap();
                resolve_observed_terminal(&mut tx, &row, values.publication, now).await?;
                let task = task_from_row(row)?;
                tx.commit().await.map_err(unavailable)?;
                return Ok(task);
            }
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let row = load_task_in(&mut tx, lease.task.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if row.state == "provider_waiting" {
            heartbeat_capacity(
                &mut tx,
                lease.task.executor_execution_id,
                lease.task.submission_id,
                now,
            )
            .await?;
        } else {
            resolve_observed_terminal(&mut tx, &row, values.publication, now).await?;
        }
        let task = task_from_row(row)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(task)
    }

    async fn publish_artifact_authority(
        &self,
        lease: &ProviderTaskLease,
        authority: &ProviderArtifactAuthority,
    ) -> Result<ProviderArtifactPublication, ProviderTaskStoreError> {
        validate_lease(lease, 1)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let manifest =
            ExecutorResultManifest::new(lease.task.submission_id, lease.task.executor_execution_id)
                .ok_or(ProviderTaskStoreError::InvalidInput)?;
        let existing: Option<(String, String, String, i64, String)> = sqlx::query_as(
            r#"
            SELECT authority.storage_backend, authority.storage_namespace,
                   authority.sha256_hex, authority.byte_size, authority.media_type
            FROM executor_result_manifests manifest
            JOIN executor_artifact_authorities authority
              ON authority.authority_id = manifest.artifact_authority_id
             AND authority.executor_execution_id = manifest.executor_execution_id
             AND authority.submission_id = manifest.submission_id
            WHERE manifest.manifest_id = $1
              AND manifest.artifact_authority_id = $2
              AND manifest.executor_execution_id = $2
              AND manifest.submission_id = $1
              AND authority.object_key = $3
            "#,
        )
        .bind(manifest.manifest_id())
        .bind(manifest.artifact_authority_id())
        .bind(&authority.object_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if let Some((backend, namespace, sha256_hex, byte_size, media_type)) = existing {
            if backend != authority.storage_backend
                || namespace != authority.storage_namespace
                || sha256_hex != authority.sha256_hex
                || u64::try_from(byte_size).ok() != Some(authority.byte_size)
                || media_type != authority.media_type
            {
                return Err(ProviderTaskStoreError::Conflict);
            }
            tx.commit().await.map_err(unavailable)?;
            return Ok(ProviderArtifactPublication {
                manifest,
                sha256_hex: authority.sha256_hex.clone(),
                byte_size: authority.byte_size,
                media_type: authority.media_type.clone(),
            });
        }
        let locked_task: Option<(Uuid, Uuid, String, Option<String>, i64, Option<i64>, i64)> =
            sqlx::query_as(
                r#"
            SELECT submission.output_id, submission.job_id, task.state,
                   task.poll_owner, task.poll_lease_epoch, task.poll_lease_expires_at_ms,
                   task.provider_deadline_at_ms
            FROM provider_remote_tasks task
            JOIN provider_submissions submission
              ON submission.submission_id = task.submission_id
             AND submission.executor_execution_id = task.executor_execution_id
            WHERE task.submission_id = $1
              AND task.executor_execution_id = $2
            FOR UPDATE OF task
            "#,
            )
            .bind(lease.task.submission_id)
            .bind(lease.task.executor_execution_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let (
            output_id,
            job_id,
            state,
            poll_owner,
            poll_lease_epoch,
            poll_lease_expires_at_ms,
            provider_deadline_at_ms,
        ) = locked_task.ok_or(ProviderTaskStoreError::StaleLease)?;
        if state != "provider_waiting"
            || poll_owner.as_deref() != Some(lease.poll_owner.as_str())
            || poll_lease_epoch != lease.poll_lease_epoch
            || poll_lease_expires_at_ms.is_none_or(|expires_at_ms| expires_at_ms <= now)
            || provider_deadline_at_ms <= now
        {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        if authority.object_key != executor_object_key(manifest.artifact_authority_id()) {
            return Err(ProviderTaskStoreError::Conflict);
        }

        let inserted = sqlx::query(
            r#"
            INSERT INTO executor_artifact_authorities
              (authority_id, executor_execution_id, submission_id, output_id, job_id,
               storage_backend, storage_namespace, object_key, sha256_hex, byte_size,
               media_type, created_at_ms)
            VALUES ($1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (authority_id) DO NOTHING
            "#,
        )
        .bind(manifest.artifact_authority_id())
        .bind(lease.task.submission_id)
        .bind(output_id)
        .bind(job_id)
        .bind(&authority.storage_backend)
        .bind(&authority.storage_namespace)
        .bind(&authority.object_key)
        .bind(&authority.sha256_hex)
        .bind(i64::try_from(authority.byte_size).map_err(|_| ProviderTaskStoreError::InvalidInput)?)
        .bind(&authority.media_type)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?
        .rows_affected();
        if inserted == 0 {
            let matches: Option<bool> = sqlx::query_scalar(
                r#"
                SELECT storage_backend = $2
                   AND storage_namespace = $3
                   AND object_key = $4
                   AND sha256_hex = $5
                   AND byte_size = $6
                   AND media_type = $7
                   AND executor_execution_id = $1
                   AND submission_id = $8
                FROM executor_artifact_authorities
                WHERE authority_id = $1
                "#,
            )
            .bind(manifest.artifact_authority_id())
            .bind(&authority.storage_backend)
            .bind(&authority.storage_namespace)
            .bind(&authority.object_key)
            .bind(&authority.sha256_hex)
            .bind(
                i64::try_from(authority.byte_size)
                    .map_err(|_| ProviderTaskStoreError::InvalidInput)?,
            )
            .bind(&authority.media_type)
            .bind(lease.task.submission_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
            if matches != Some(true) {
                return Err(ProviderTaskStoreError::Conflict);
            }
        }
        sqlx::query(
            r#"
            INSERT INTO executor_result_manifests
              (manifest_id, artifact_authority_id, executor_execution_id,
               submission_id, created_at_ms)
            VALUES ($1, $2, $2, $1, $3)
            ON CONFLICT (manifest_id) DO NOTHING
            "#,
        )
        .bind(manifest.manifest_id())
        .bind(manifest.artifact_authority_id())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(ProviderArtifactPublication {
            manifest,
            sha256_hex: authority.sha256_hex.clone(),
            byte_size: authority.byte_size,
            media_type: authority.media_type.clone(),
        })
    }

    async fn record_verified_callback(
        &self,
        callback: &VerifiedCallbackWakeup,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        if callback.submission_id.is_nil() || !valid_public_event_identity(&callback.event_identity)
        {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let task = load_task_in(&mut tx, callback.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if task.state != "provider_waiting" {
            let task = task_from_row(task)?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(task);
        }
        let now = database_now(&mut tx).await?;
        if task.provider_deadline_at_ms <= now {
            let task = resolve_remote_task_deadline_in(&mut tx, task, now).await?;
            let task = task_from_row(task)?;
            tx.commit().await.map_err(storage_conflict)?;
            return Ok(task);
        }
        let payload_hash = observation_hash(
            "verified_callback",
            "provider_waiting",
            None,
            None,
            None,
            None,
            None,
            None,
            "not_applicable",
            Some(now),
            None,
            None,
        );
        let persisted = insert_or_load_observation(
            &mut tx,
            &NewObservation {
                submission_id: task.submission_id,
                executor_execution_id: task.executor_execution_id,
                event_identity: &callback.event_identity,
                source: "verified_callback",
                state: "provider_waiting",
                artifact_ref: None,
                result_manifest_id: None,
                artifact_sha256_hex: None,
                artifact_byte_size: None,
                artifact_media_type: None,
                error_code: None,
                effect_certainty: "not_applicable",
                next_poll_at_ms: Some(now),
                poll_owner: None,
                poll_lease_epoch: None,
                payload_hash: &payload_hash,
                requested_poll_after_ms: None,
                now,
            },
        )
        .await?;
        let observation_id = persisted.observation_id;
        let callback_poll_at: i64 = sqlx::query_scalar(
            "SELECT next_poll_at_ms FROM provider_task_observations WHERE observation_id = $1",
        )
        .bind(observation_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            r#"
            UPDATE provider_remote_tasks
            SET next_poll_at_ms = LEAST(next_poll_at_ms, $3),
                last_wakeup_observation_id = $2, updated_at_ms = $4
            WHERE submission_id = $1
              AND last_wakeup_observation_id IS DISTINCT FROM $2
            "#,
        )
        .bind(callback.submission_id)
        .bind(observation_id)
        .bind(callback_poll_at)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        let task = task_from_row(
            load_task_in(&mut tx, callback.submission_id)
                .await?
                .unwrap(),
        )?;
        tx.commit().await.map_err(unavailable)?;
        Ok(task)
    }
}

async fn load_submit_observation_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    intent: ProviderSubmitIntent,
) -> Result<ProviderSubmitInvocation, ProviderTaskStoreError> {
    let context = load_provider_context_in(tx, intent.submission_id)
        .await?
        .ok_or(ProviderTaskStoreError::Conflict)?;
    Ok(ProviderSubmitInvocation {
        intent,
        context: provider_context_from_row(context),
    })
}

#[async_trait]
impl ProviderTaskDeadlineStore for PostgresProviderTaskStore {
    async fn resolve_due_remote_task_deadline(
        &self,
        scope: &ProviderTaskClaimScope,
    ) -> Result<Option<ProviderRemoteTask>, ProviderTaskStoreError> {
        validate_scope(scope)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let candidate: Option<(Uuid, i64)> = sqlx::query_as(
            r#"
            WITH db_clock AS MATERIALIZED (
              SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            ), candidate_window AS MATERIALIZED (
              SELECT task.submission_id, task.executor_execution_id,
                     task.provider_deadline_at_ms
              FROM provider_remote_tasks task
              CROSS JOIN db_clock
              WHERE task.provider_id = $1 AND task.provider_account_id = $2
                AND task.state = 'provider_waiting'
                AND task.provider_deadline_at_ms <= db_clock.now_ms
              ORDER BY task.provider_deadline_at_ms, task.submission_id
              LIMIT 64
            ), candidate AS (
              SELECT task.submission_id, db_clock.now_ms
              FROM candidate_window candidates
              JOIN provider_remote_tasks task
                ON task.submission_id = candidates.submission_id
               AND task.executor_execution_id = candidates.executor_execution_id
              JOIN executor_capacity_allocations allocation
                ON allocation.executor_execution_id = task.executor_execution_id
               AND allocation.submission_id = task.submission_id
               AND allocation.state = 'held'
              CROSS JOIN db_clock
              WHERE task.state = 'provider_waiting'
                AND task.provider_deadline_at_ms <= db_clock.now_ms
              ORDER BY candidates.provider_deadline_at_ms, candidates.submission_id
              FOR UPDATE OF task SKIP LOCKED
              LIMIT 1
            )
            SELECT submission_id, now_ms FROM candidate
            "#,
        )
        .bind(&scope.provider_id)
        .bind(scope.provider_account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some((submission_id, now)) = candidate else {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        };
        let task = load_task_in(&mut tx, submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        let task = resolve_remote_task_deadline_in(&mut tx, task, now).await?;
        let task = task_from_row(task)?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(Some(task))
    }
}

struct ObservationValues<'a> {
    source: &'static str,
    state: &'static str,
    artifact_ref: Option<&'a str>,
    publication: Option<&'a ProviderArtifactPublication>,
    error_code: Option<&'a str>,
    effect_certainty: &'static str,
    next_poll_at_ms: Option<i64>,
}

async fn resolve_remote_task_deadline_in(
    tx: &mut Transaction<'_, Postgres>,
    task: TaskRow,
    now: i64,
) -> Result<TaskRow, ProviderTaskStoreError> {
    if task.state != "provider_waiting" {
        return Ok(task);
    }
    if task.provider_deadline_at_ms > now || task.deadline_quarantine_id.is_some() {
        return Err(ProviderTaskStoreError::StaleLease);
    }
    if let Some(publication) = load_published_artifact_in(tx, &task).await? {
        return recover_published_artifact_in(tx, task, publication, now).await;
    }

    let quarantine_id = task.executor_execution_id;
    sqlx::query(
        r#"
        INSERT INTO provider_remote_task_quarantines
          (quarantine_id, submission_id, executor_execution_id,
           provider_id, provider_account_id, remote_operation_id,
           provider_deadline_at_ms, error_code, quarantined_at_ms)
        VALUES ($1, $2, $1, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(quarantine_id)
    .bind(task.submission_id)
    .bind(&task.provider_id)
    .bind(task.provider_account_id)
    .bind(&task.remote_operation_id)
    .bind(task.provider_deadline_at_ms)
    .bind(REMOTE_TASK_DEADLINE_ERROR)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_remote_tasks
            SET state = 'uncertain', artifact_ref = NULL,
                error_code = $3, effect_certainty = 'unknown_remote_effect',
                next_poll_at_ms = NULL, poll_owner = NULL,
                poll_lease_expires_at_ms = NULL, poll_claimed_at_ms = NULL,
                deadline_quarantine_id = $1,
                updated_at_ms = $4, terminal_at_ms = $4
            WHERE submission_id = $2
              AND executor_execution_id = $1
              AND state = 'provider_waiting'
              AND provider_deadline_at_ms <= $4
            "#,
        )
        .bind(quarantine_id)
        .bind(task.submission_id)
        .bind(REMOTE_TASK_DEADLINE_ERROR)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    sqlx::query(
        r#"
        INSERT INTO executor_resolution_decisions
          (decision_id, executor_execution_id, submission_id, source,
           observation_id, provider_task_observation_id,
           provider_submit_intent_id, provider_remote_task_quarantine_id,
           resolved_state, result_manifest_id, error_code, decided_at_ms)
        VALUES ($1, $1, $2, 'remote_task_deadline',
                NULL, NULL, NULL, $1, 'uncertain', NULL, $3, $4)
        "#,
    )
    .bind(quarantine_id)
    .bind(task.submission_id)
    .bind(REMOTE_TASK_DEADLINE_ERROR)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = 'uncertain', executor_owner = NULL,
                lease_expires_at_ms = NULL, resolution_decision_id = $1,
                finished_at_ms = $3, updated_at_ms = $3, error_code = $4
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'provider_waiting'
              AND executor_owner IS NULL AND lease_expires_at_ms IS NULL
            "#,
        )
        .bind(quarantine_id)
        .bind(task.submission_id)
        .bind(now)
        .bind(REMOTE_TASK_DEADLINE_ERROR)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = 'uncertain', result_manifest_id = NULL,
                resolution_decision_id = $1, finished_at_ms = $3,
                updated_at_ms = $3, error_code = $4
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'provider_waiting'
            "#,
        )
        .bind(quarantine_id)
        .bind(task.submission_id)
        .bind(now)
        .bind(REMOTE_TASK_DEADLINE_ERROR)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    load_task_in(tx, task.submission_id)
        .await?
        .ok_or(ProviderTaskStoreError::NotFound)
}

async fn load_published_artifact_in(
    tx: &mut Transaction<'_, Postgres>,
    task: &TaskRow,
) -> Result<Option<ProviderArtifactPublication>, ProviderTaskStoreError> {
    let row: Option<(Uuid, String, i64, String)> = sqlx::query_as(
        r#"
        SELECT manifest.manifest_id, authority.sha256_hex,
               authority.byte_size, authority.media_type
        FROM executor_result_manifests manifest
        JOIN executor_artifact_authorities authority
          ON authority.authority_id = manifest.artifact_authority_id
         AND authority.executor_execution_id = manifest.executor_execution_id
         AND authority.submission_id = manifest.submission_id
        WHERE manifest.executor_execution_id = $1
          AND manifest.submission_id = $2
        "#,
    )
    .bind(task.executor_execution_id)
    .bind(task.submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some((manifest_id, sha256_hex, byte_size, media_type)) = row else {
        return Ok(None);
    };
    let manifest = ExecutorResultManifest::new(task.submission_id, task.executor_execution_id)
        .ok_or(ProviderTaskStoreError::Conflict)?;
    if manifest.manifest_id() != manifest_id {
        return Err(ProviderTaskStoreError::Conflict);
    }
    Ok(Some(ProviderArtifactPublication {
        manifest,
        sha256_hex,
        byte_size: u64::try_from(byte_size).map_err(|_| ProviderTaskStoreError::Conflict)?,
        media_type,
    }))
}

async fn recover_published_artifact_in(
    tx: &mut Transaction<'_, Postgres>,
    task: TaskRow,
    publication: ProviderArtifactPublication,
    now: i64,
) -> Result<TaskRow, ProviderTaskStoreError> {
    let artifact_ref = format!("manifest:{}", publication.manifest.manifest_id().simple());
    let payload_hash = observation_hash(
        "artifact_recovery",
        "artifact_ready",
        Some(&artifact_ref),
        Some(publication.manifest.manifest_id()),
        Some(&publication.sha256_hex),
        Some(publication.byte_size),
        Some(&publication.media_type),
        None,
        "not_applicable",
        None,
        None,
        None,
    );
    let persisted = insert_or_load_observation(
        tx,
        &NewObservation {
            submission_id: task.submission_id,
            executor_execution_id: task.executor_execution_id,
            event_identity: ARTIFACT_RECOVERY_EVENT,
            source: "artifact_recovery",
            state: "artifact_ready",
            artifact_ref: Some(&artifact_ref),
            result_manifest_id: Some(publication.manifest.manifest_id()),
            artifact_sha256_hex: Some(&publication.sha256_hex),
            artifact_byte_size: Some(
                i64::try_from(publication.byte_size)
                    .map_err(|_| ProviderTaskStoreError::Conflict)?,
            ),
            artifact_media_type: Some(&publication.media_type),
            error_code: None,
            effect_certainty: "not_applicable",
            next_poll_at_ms: None,
            poll_owner: None,
            poll_lease_epoch: None,
            payload_hash: &payload_hash,
            requested_poll_after_ms: None,
            now,
        },
    )
    .await?;
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_remote_tasks
            SET state = 'artifact_ready', artifact_ref = $3,
                error_code = NULL, effect_certainty = 'not_applicable',
                next_poll_at_ms = NULL, poll_owner = NULL,
                poll_lease_expires_at_ms = NULL, poll_claimed_at_ms = NULL,
                state_observation_id = $4, updated_at_ms = $5,
                terminal_at_ms = $5
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND state = 'provider_waiting'
              AND provider_deadline_at_ms <= $5
              AND deadline_quarantine_id IS NULL
            "#,
        )
        .bind(task.submission_id)
        .bind(task.executor_execution_id)
        .bind(&artifact_ref)
        .bind(persisted.observation_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    let row = load_task_in(tx, task.submission_id)
        .await?
        .ok_or(ProviderTaskStoreError::NotFound)?;
    resolve_observed_terminal(tx, &row, Some(&publication), now).await?;
    Ok(row)
}

async fn resolve_submit_deadline_terminal(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    sqlx::query(
        r#"
        INSERT INTO executor_resolution_decisions
          (decision_id, executor_execution_id, submission_id, source,
           observation_id, provider_task_observation_id, provider_submit_intent_id,
           resolved_state, result_manifest_id, error_code, decided_at_ms)
        VALUES ($1, $1, $2, 'remote_submit_deadline',
                NULL, NULL, $2, 'uncertain', NULL, $3, $4)
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .bind(SUBMIT_DEADLINE_ERROR)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = 'uncertain', executor_owner = NULL,
                lease_expires_at_ms = NULL, resolution_decision_id = $1,
                finished_at_ms = $3, updated_at_ms = $3, error_code = $4
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'running'
            "#,
        )
        .bind(executor_execution_id)
        .bind(submission_id)
        .bind(now)
        .bind(SUBMIT_DEADLINE_ERROR)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = 'uncertain', resolution_decision_id = $1,
                finished_at_ms = $3, updated_at_ms = $3, error_code = $4
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'running'
            "#,
        )
        .bind(executor_execution_id)
        .bind(submission_id)
        .bind(now)
        .bind(SUBMIT_DEADLINE_ERROR)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )
}

async fn resolve_submit_terminal(
    tx: &mut Transaction<'_, Postgres>,
    intent: &ProviderSubmitIntent,
    resolved_state: &str,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    if resolved_state != "failed" || intent.state != ProviderSubmitIntentState::Rejected {
        return Err(ProviderTaskStoreError::Conflict);
    }
    let error_code = intent
        .failure_error_code
        .as_deref()
        .ok_or(ProviderTaskStoreError::Conflict)?;

    sqlx::query(
        r#"
        INSERT INTO executor_resolution_decisions
          (decision_id, executor_execution_id, submission_id, source,
           observation_id, provider_task_observation_id, provider_submit_intent_id,
           resolved_state, result_manifest_id, error_code, decided_at_ms)
        VALUES ($1, $1, $2, 'remote_submit_outcome',
                NULL, NULL, $2, $3, NULL, $4, $5)
        "#,
    )
    .bind(intent.executor_execution_id)
    .bind(intent.submission_id)
    .bind(resolved_state)
    .bind(error_code)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = $3, executor_owner = NULL, lease_expires_at_ms = NULL,
                resolution_decision_id = $1, finished_at_ms = $4,
                updated_at_ms = $4, error_code = $5
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'running'
            "#,
        )
        .bind(intent.executor_execution_id)
        .bind(intent.submission_id)
        .bind(resolved_state)
        .bind(now)
        .bind(error_code)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = $3, resolution_decision_id = $1,
                finished_at_ms = $4, updated_at_ms = $4, error_code = $5
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'running'
            "#,
        )
        .bind(intent.executor_execution_id)
        .bind(intent.submission_id)
        .bind(resolved_state)
        .bind(now)
        .bind(error_code)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    release_capacity_allocation(
        tx,
        intent.executor_execution_id,
        intent.submission_id,
        resolved_state,
        "remote_submit_outcome",
        None,
        now,
    )
    .await
    .map_err(map_executor_error)
}

async fn resolve_remote_terminal(
    tx: &mut Transaction<'_, Postgres>,
    task: &TaskRow,
    manifest: Option<&ExecutorResultManifest>,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    let (resolved_state, result_manifest_id, error_code) = match task.state.as_str() {
        "artifact_ready" => {
            let manifest = manifest.ok_or(ProviderTaskStoreError::Conflict)?;
            let authority_is_bound: Option<bool> = sqlx::query_scalar(
                r#"
                SELECT TRUE
                FROM executor_result_manifests manifest
                WHERE manifest.manifest_id = $1
                  AND manifest.artifact_authority_id = $2
                  AND manifest.executor_execution_id = $3
                  AND manifest.submission_id = $4
                "#,
            )
            .bind(manifest.manifest_id())
            .bind(manifest.artifact_authority_id())
            .bind(task.executor_execution_id)
            .bind(task.submission_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unavailable)?;
            if authority_is_bound.is_none() {
                return Err(ProviderTaskStoreError::Conflict);
            }
            ("succeeded", Some(manifest.manifest_id()), None)
        }
        "failed" => (
            "failed",
            None,
            Some(
                task.error_code
                    .as_deref()
                    .ok_or(ProviderTaskStoreError::Conflict)?,
            ),
        ),
        "uncertain" => (
            "uncertain",
            None,
            Some(
                task.error_code
                    .as_deref()
                    .ok_or(ProviderTaskStoreError::Conflict)?,
            ),
        ),
        "canceled" => (
            "canceled",
            None,
            Some(
                task.error_code
                    .as_deref()
                    .ok_or(ProviderTaskStoreError::Conflict)?,
            ),
        ),
        _ => return Err(ProviderTaskStoreError::Conflict),
    };

    let existing: Option<ResolutionDecisionRow> = sqlx::query_as(
        r#"
            SELECT resolved_state, result_manifest_id, error_code,
                   provider_task_observation_id, provider_submit_intent_id
            FROM executor_resolution_decisions
            WHERE decision_id = $1
            "#,
    )
    .bind(task.executor_execution_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    if let Some(existing) = existing {
        return if existing.resolved_state == resolved_state
            && existing.result_manifest_id == result_manifest_id
            && existing.error_code.as_deref() == error_code
            && existing.provider_task_observation_id == Some(task.state_observation_id)
            && existing.provider_submit_intent_id.is_none()
        {
            Ok(())
        } else {
            Err(ProviderTaskStoreError::Conflict)
        };
    }

    sqlx::query(
        r#"
        INSERT INTO executor_resolution_decisions
          (decision_id, executor_execution_id, submission_id, source,
           observation_id, provider_task_observation_id, resolved_state,
           result_manifest_id, error_code, decided_at_ms)
        VALUES ($1, $1, $2, 'remote_provider_observation',
                NULL, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(task.executor_execution_id)
    .bind(task.submission_id)
    .bind(task.state_observation_id)
    .bind(resolved_state)
    .bind(result_manifest_id)
    .bind(error_code)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?;

    require_one(
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = $3, resolution_decision_id = $1,
                finished_at_ms = $4, updated_at_ms = $4, error_code = $5
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'provider_waiting'
              AND executor_owner IS NULL AND lease_expires_at_ms IS NULL
            "#,
        )
        .bind(task.executor_execution_id)
        .bind(task.submission_id)
        .bind(resolved_state)
        .bind(now)
        .bind(error_code)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = $3, result_manifest_id = $4,
                resolution_decision_id = $1, finished_at_ms = $5,
                updated_at_ms = $5, error_code = $6
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'provider_waiting'
            "#,
        )
        .bind(task.executor_execution_id)
        .bind(task.submission_id)
        .bind(resolved_state)
        .bind(result_manifest_id)
        .bind(now)
        .bind(error_code)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    release_capacity_allocation(
        tx,
        task.executor_execution_id,
        task.submission_id,
        resolved_state,
        "remote_provider_observation",
        None,
        now,
    )
    .await
    .map_err(map_executor_error)
}

async fn resolve_observed_terminal(
    tx: &mut Transaction<'_, Postgres>,
    task: &TaskRow,
    publication: Option<&ProviderArtifactPublication>,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    if task.state == "provider_waiting" {
        return Ok(());
    }
    if task.state == "artifact_ready" {
        let publication = publication.ok_or(ProviderTaskStoreError::Conflict)?;
        return resolve_remote_terminal(tx, task, Some(&publication.manifest), now).await;
    }
    resolve_remote_terminal(tx, task, None, now).await
}

fn observation_values<'a>(
    observation: &'a ProviderTaskObservation,
    now: i64,
) -> ObservationValues<'a> {
    let source = match observation.source {
        ProviderTaskObservationSource::Poll => "poll",
        ProviderTaskObservationSource::Cancel => "cancel",
    };
    match &observation.outcome {
        ProviderTaskObservationOutcome::Waiting { poll_after_ms } => ObservationValues {
            source,
            state: "provider_waiting",
            artifact_ref: None,
            publication: None,
            error_code: None,
            effect_certainty: "not_applicable",
            next_poll_at_ms: Some(now + poll_after_ms),
        },
        ProviderTaskObservationOutcome::ArtifactReady {
            artifact_ref,
            publication,
        } => ObservationValues {
            source,
            state: "artifact_ready",
            artifact_ref: Some(artifact_ref),
            publication: Some(publication),
            error_code: None,
            effect_certainty: "not_applicable",
            next_poll_at_ms: None,
        },
        ProviderTaskObservationOutcome::Failed { error_code } => ObservationValues {
            source,
            state: "failed",
            artifact_ref: None,
            publication: None,
            error_code: Some(error_code),
            effect_certainty: "not_applicable",
            next_poll_at_ms: None,
        },
        ProviderTaskObservationOutcome::Canceled { error_code } => ObservationValues {
            source,
            state: "canceled",
            artifact_ref: None,
            publication: None,
            error_code: Some(error_code),
            effect_certainty: "confirmed_no_effect",
            next_poll_at_ms: None,
        },
        ProviderTaskObservationOutcome::Uncertain { error_code } => ObservationValues {
            source,
            state: "uncertain",
            artifact_ref: None,
            publication: None,
            error_code: Some(error_code),
            effect_certainty: "unknown_remote_effect",
            next_poll_at_ms: None,
        },
    }
}

struct PersistedObservation {
    observation_id: Uuid,
    next_poll_at_ms: Option<i64>,
}

struct NewObservation<'a> {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    event_identity: &'a str,
    source: &'a str,
    state: &'a str,
    artifact_ref: Option<&'a str>,
    result_manifest_id: Option<Uuid>,
    artifact_sha256_hex: Option<&'a str>,
    artifact_byte_size: Option<i64>,
    artifact_media_type: Option<&'a str>,
    error_code: Option<&'a str>,
    effect_certainty: &'a str,
    next_poll_at_ms: Option<i64>,
    poll_owner: Option<&'a str>,
    poll_lease_epoch: Option<i64>,
    payload_hash: &'a str,
    requested_poll_after_ms: Option<i64>,
    now: i64,
}

async fn insert_or_load_observation(
    tx: &mut Transaction<'_, Postgres>,
    observation: &NewObservation<'_>,
) -> Result<PersistedObservation, ProviderTaskStoreError> {
    let observation_id = Uuid::new_v4();
    let inserted = sqlx::query(
        r#"
        INSERT INTO provider_task_observations
          (observation_id, submission_id, executor_execution_id, event_identity,
           source, observed_state, artifact_ref, result_manifest_id,
           artifact_sha256_hex, artifact_byte_size, artifact_media_type,
           error_code, effect_certainty, next_poll_at_ms, poll_owner,
           poll_lease_epoch, payload_hash, observed_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16, $17, $18)
        ON CONFLICT (submission_id, event_identity) DO NOTHING
        "#,
    )
    .bind(observation_id)
    .bind(observation.submission_id)
    .bind(observation.executor_execution_id)
    .bind(observation.event_identity)
    .bind(observation.source)
    .bind(observation.state)
    .bind(observation.artifact_ref)
    .bind(observation.result_manifest_id)
    .bind(observation.artifact_sha256_hex)
    .bind(observation.artifact_byte_size)
    .bind(observation.artifact_media_type)
    .bind(observation.error_code)
    .bind(observation.effect_certainty)
    .bind(observation.next_poll_at_ms)
    .bind(observation.poll_owner)
    .bind(observation.poll_lease_epoch)
    .bind(observation.payload_hash)
    .bind(observation.now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?
    .rows_affected();
    if inserted == 1 {
        return Ok(PersistedObservation {
            observation_id,
            next_poll_at_ms: observation.next_poll_at_ms,
        });
    }
    load_matching_observation(tx, observation)
        .await?
        .ok_or(ProviderTaskStoreError::Conflict)
}

async fn load_matching_observation(
    tx: &mut Transaction<'_, Postgres>,
    observation: &NewObservation<'_>,
) -> Result<Option<PersistedObservation>, ProviderTaskStoreError> {
    let existing: Option<ExistingObservation> = sqlx::query_as(
        r#"
        SELECT observation_id, source, observed_state, artifact_ref,
               result_manifest_id, artifact_sha256_hex, artifact_byte_size,
               artifact_media_type, error_code, effect_certainty, next_poll_at_ms,
               poll_owner, poll_lease_epoch, payload_hash, observed_at_ms
        FROM provider_task_observations
        WHERE submission_id = $1 AND event_identity = $2
        "#,
    )
    .bind(observation.submission_id)
    .bind(observation.event_identity)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let stable_payload_matches = existing.source == observation.source
        && existing.observed_state == observation.state
        && existing.artifact_ref.as_deref() == observation.artifact_ref
        && existing.result_manifest_id == observation.result_manifest_id
        && existing.artifact_sha256_hex.as_deref() == observation.artifact_sha256_hex
        && existing.artifact_byte_size == observation.artifact_byte_size
        && existing.artifact_media_type.as_deref() == observation.artifact_media_type
        && existing.error_code.as_deref() == observation.error_code
        && existing.effect_certainty == observation.effect_certainty
        && existing.poll_owner.as_deref() == observation.poll_owner
        && existing.poll_lease_epoch == observation.poll_lease_epoch;
    let time_dependent_replay = observation.source == "verified_callback"
        || (observation.state == "provider_waiting"
            && stable_payload_matches
            && (existing.payload_hash == observation.payload_hash
                || legacy_waiting_replay_matches(&existing, observation)));
    let exact_replay = existing.next_poll_at_ms == observation.next_poll_at_ms
        && existing.payload_hash == observation.payload_hash;
    if stable_payload_matches && (time_dependent_replay || exact_replay) {
        Ok(Some(PersistedObservation {
            observation_id: existing.observation_id,
            next_poll_at_ms: existing.next_poll_at_ms,
        }))
    } else {
        Err(ProviderTaskStoreError::Conflict)
    }
}

fn legacy_waiting_replay_matches(
    existing: &ExistingObservation,
    observation: &NewObservation<'_>,
) -> bool {
    let Some(requested_poll_after_ms) = observation.requested_poll_after_ms else {
        return false;
    };
    if existing
        .next_poll_at_ms
        .and_then(|next| next.checked_sub(existing.observed_at_ms))
        != Some(requested_poll_after_ms)
    {
        return false;
    }
    let legacy_payload_hash = observation_hash(
        &existing.source,
        &existing.observed_state,
        existing.artifact_ref.as_deref(),
        existing.result_manifest_id,
        existing.artifact_sha256_hex.as_deref(),
        existing
            .artifact_byte_size
            .and_then(|value| u64::try_from(value).ok()),
        existing.artifact_media_type.as_deref(),
        existing.error_code.as_deref(),
        &existing.effect_certainty,
        existing.next_poll_at_ms,
        existing.poll_owner.as_deref(),
        existing.poll_lease_epoch,
    );
    existing.payload_hash == legacy_payload_hash
}

#[allow(clippy::too_many_arguments)]
async fn insert_observation(
    tx: &mut Transaction<'_, Postgres>,
    observation_id: Uuid,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    event_identity: &str,
    source: &str,
    state: &str,
    artifact_ref: Option<&str>,
    error_code: Option<&str>,
    effect_certainty: &str,
    next_poll_at_ms: Option<i64>,
    poll_owner: Option<&str>,
    poll_lease_epoch: Option<i64>,
    payload_hash: &str,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    sqlx::query(
        r#"
        INSERT INTO provider_task_observations
          (observation_id, submission_id, executor_execution_id, event_identity,
           source, observed_state, artifact_ref, error_code, effect_certainty,
           next_poll_at_ms, poll_owner, poll_lease_epoch, payload_hash, observed_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        "#,
    )
    .bind(observation_id)
    .bind(submission_id)
    .bind(executor_execution_id)
    .bind(event_identity)
    .bind(source)
    .bind(state)
    .bind(artifact_ref)
    .bind(error_code)
    .bind(effect_certainty)
    .bind(next_poll_at_ms)
    .bind(poll_owner)
    .bind(poll_lease_epoch)
    .bind(payload_hash)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?;
    Ok(())
}

async fn load_task(
    pool: &PgPool,
    submission_id: Uuid,
) -> Result<Option<TaskRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission_id, executor_execution_id, provider_id, provider_account_id,
               remote_operation_id, provider_request_id, provider_deadline_at_ms,
               state, artifact_ref, error_code, next_poll_at_ms, cancel_requested,
               poll_owner, poll_lease_epoch, poll_lease_expires_at_ms,
               state_observation_id, deadline_quarantine_id,
               attach_recovery_owner, attach_recovery_lease_epoch
        FROM provider_remote_tasks
        WHERE submission_id = $1
        "#,
    )
    .bind(submission_id)
    .fetch_optional(pool)
    .await
    .map_err(unavailable)
}

async fn load_submit_intent_in(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<Option<SubmitIntentRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission_id, executor_execution_id, provider_id, provider_account_id,
               submit_owner, submit_lease_epoch, idempotency_key,
               output_index, output_total,
               provider_command_sha256, execution_binding_sha256,
               provider_timeout_ms, state,
               remote_operation_id, provider_request_id, send_started_at_ms,
               receipt_event_identity, failure_event_identity,
               failure_error_code, updated_at_ms
        FROM provider_remote_submit_intents
        WHERE submission_id = $1
        FOR UPDATE
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn load_submit_intent(
    pool: &PgPool,
    submission_id: Uuid,
) -> Result<Option<SubmitIntentRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission_id, executor_execution_id, provider_id, provider_account_id,
               submit_owner, submit_lease_epoch, idempotency_key,
               output_index, output_total,
               provider_command_sha256, execution_binding_sha256,
               provider_timeout_ms, state,
               remote_operation_id, provider_request_id, send_started_at_ms,
               receipt_event_identity, failure_event_identity,
               failure_error_code, updated_at_ms
        FROM provider_remote_submit_intents
        WHERE submission_id = $1
        "#,
    )
    .bind(submission_id)
    .fetch_optional(pool)
    .await
    .map_err(unavailable)
}

async fn quarantined_receipt_matches(
    tx: &mut Transaction<'_, Postgres>,
    request: &RemoteTaskQuarantinedReceipt,
) -> Result<bool, ProviderTaskStoreError> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM provider_submit_quarantined_receipts
          WHERE event_identity = $1 AND submission_id = $2
            AND executor_execution_id = $3 AND execution_binding_sha256 = $4
            AND expected_provider_id = $5 AND observed_provider_id = $6
            AND observed_submission_id = $7 AND remote_operation_id = $8
            AND provider_request_id IS NOT DISTINCT FROM $9 AND reason = $10
        )
        "#,
    )
    .bind(&request.event_identity)
    .bind(request.submission_id)
    .bind(request.executor_execution_id)
    .bind(&request.execution_binding_sha256)
    .bind(&request.expected_provider_id)
    .bind(&request.observed_provider_id)
    .bind(&request.observed_submission_id)
    .bind(&request.remote_operation_id)
    .bind(&request.provider_request_id)
    .bind(&request.reason)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn lock_submit_parent(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
) -> Result<SubmitParentRow, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission.provider_id, submission.provider_account_id,
               execution.state AS execution_state,
               submission.state AS submission_state,
               execution.executor_owner, execution.lease_epoch,
               execution.lease_expires_at_ms,
               execution.launch_owner, execution.launch_lease_epoch,
               allocation.state AS allocation_state,
               allocation.release_reason AS allocation_release_reason,
               allocation.release_reconciliation_id AS
                   allocation_release_reconciliation_id,
               reconciliation.state AS reconciliation_state,
               reconciliation.evidence_kind AS reconciliation_evidence_kind,
               reconciliation.remote_operation_id AS
                   reconciliation_remote_operation_id,
               execution.resolution_decision_id AS execution_resolution_decision_id,
               submission.resolution_decision_id AS submission_resolution_decision_id,
               decision.source AS resolution_source
        FROM executor_executions execution
        JOIN provider_submissions submission
          ON submission.executor_execution_id = execution.executor_execution_id
         AND submission.submission_id = execution.submission_id
        JOIN executor_capacity_allocations allocation
          ON allocation.executor_execution_id = execution.executor_execution_id
         AND allocation.submission_id = execution.submission_id
        LEFT JOIN executor_resolution_decisions decision
          ON decision.decision_id = execution.resolution_decision_id
         AND decision.executor_execution_id = execution.executor_execution_id
         AND decision.submission_id = execution.submission_id
        LEFT JOIN provider_capacity_reconciliations reconciliation
          ON reconciliation.executor_execution_id = execution.executor_execution_id
         AND reconciliation.submission_id = execution.submission_id
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

async fn load_provider_context_in(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<Option<ProviderContextRow>, ProviderTaskStoreError> {
    sqlx::query_as(
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
        FROM provider_submit_recoveries recovery
        JOIN provider_remote_submit_intents intent
          ON intent.submission_id = recovery.submission_id
         AND intent.executor_execution_id = recovery.executor_execution_id
        JOIN provider_submissions submission
          ON submission.submission_id = recovery.submission_id
         AND submission.executor_execution_id = recovery.executor_execution_id
        JOIN provider_accounts account
          ON account.provider_account_id = submission.provider_account_id
         AND account.credential_pool_id = submission.credential_pool_id
         AND account.provider_id = submission.provider_id
         AND account.credential_ref = submission.credential_ref
         AND account.credential_revision = submission.credential_revision
        WHERE recovery.submission_id = $1
        FOR UPDATE OF recovery
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn load_submit_recovery_in(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<Option<SubmitRecoveryRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT state, recovery_owner, recovery_lease_epoch,
               recovery_lease_expires_at_ms, provider_deadline_at_ms
        FROM provider_submit_recoveries
        WHERE submission_id = $1
        FOR UPDATE
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn lock_recovery_command(
    tx: &mut Transaction<'_, Postgres>,
    scope: &ProviderTaskClaimScope,
    owner: &str,
    command_id: &str,
) -> Result<(), ProviderTaskStoreError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(recovery_command_lock_key(scope, owner, command_id))
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

async fn load_recovery_command_in(
    tx: &mut Transaction<'_, Postgres>,
    scope: &ProviderTaskClaimScope,
    owner: &str,
    command_id: &str,
) -> Result<Option<RecoveryCommandRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT command_kind, request_duration_ms, submission_id,
               executor_execution_id, recovery_lease_epoch
        FROM provider_submit_recovery_commands
        WHERE provider_id = $1 AND provider_account_id = $2
          AND command_owner = $3 AND command_id = $4
        "#,
    )
    .bind(&scope.provider_id)
    .bind(scope.provider_account_id)
    .bind(owner)
    .bind(command_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn load_recovery_claim_command_in(
    tx: &mut Transaction<'_, Postgres>,
    scope: &ProviderTaskClaimScope,
    owner: &str,
    command_id: &str,
) -> Result<RecoveryClaimRow, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT command.submission_id, command.executor_execution_id,
               command.provider_id, command.provider_account_id,
               intent.submit_owner, intent.submit_lease_epoch,
               intent.idempotency_key, intent.output_index, intent.output_total,
               intent.provider_command_sha256,
               intent.execution_binding_sha256,
               intent.provider_timeout_ms AS intent_provider_timeout_ms,
               command.intent_state AS state,
               command.intent_remote_operation_id AS remote_operation_id,
               command.intent_provider_request_id AS provider_request_id,
               command.intent_send_started_at_ms AS send_started_at_ms,
               command.intent_receipt_event_identity AS receipt_event_identity,
               command.intent_failure_event_identity AS failure_event_identity,
               command.intent_failure_error_code AS failure_error_code,
               command.intent_updated_at_ms AS updated_at_ms,
               command.command_owner AS recovery_owner,
               command.recovery_lease_epoch,
               command.claim_lease_expires_at_ms AS recovery_lease_expires_at_ms,
               submission.model, submission.command_schema,
               submission.command_hash, submission.operation_id,
               submission.operation_descriptor_revision,
               submission.operation_descriptor_sha256_v1,
               submission.completion_mode, submission.idempotency_mode,
               submission.operation_binding_version,
               submission.execution_profile_id,
               submission.adapter_revision, submission.credential_pool_id,
               submission.credential_ref, submission.credential_revision,
               account.credential_auth_sha256,
               submission.resource_policy_id,
               submission.resource_policy_revision,
               recovery.invocation_attempt, recovery.provider_timeout_ms,
               recovery.provider_deadline_at_ms
        FROM provider_submit_recovery_commands command
        JOIN provider_submit_recoveries recovery
          ON recovery.submission_id = command.submission_id
         AND recovery.executor_execution_id = command.executor_execution_id
        JOIN provider_remote_submit_intents intent
          ON intent.submission_id = command.submission_id
         AND intent.executor_execution_id = command.executor_execution_id
        JOIN provider_submissions submission
          ON submission.submission_id = command.submission_id
         AND submission.executor_execution_id = command.executor_execution_id
        JOIN provider_accounts account
          ON account.provider_account_id = submission.provider_account_id
         AND account.credential_pool_id = submission.credential_pool_id
         AND account.provider_id = submission.provider_id
         AND account.credential_ref = submission.credential_ref
         AND account.credential_revision = submission.credential_revision
        WHERE command.provider_id = $1
          AND command.provider_account_id = $2
          AND command.command_owner = $3
          AND command.command_id = $4
          AND command.command_kind = 'claim'
        "#,
    )
    .bind(&scope.provider_id)
    .bind(scope.provider_account_id)
    .bind(owner)
    .bind(command_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ProviderTaskStoreError::Conflict)
}

#[allow(clippy::too_many_arguments)]
async fn insert_recovery_claim_command(
    tx: &mut Transaction<'_, Postgres>,
    scope: &ProviderTaskClaimScope,
    owner: &str,
    command_id: &str,
    lease_ms: i64,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    recovery_lease_epoch: i64,
    claimed_at_ms: i64,
    lease_expires_at_ms: i64,
) -> Result<(), ProviderTaskStoreError> {
    require_one(
        sqlx::query(
            r#"
            INSERT INTO provider_submit_recovery_commands (
                provider_id, provider_account_id, command_owner, command_id,
                command_kind, request_duration_ms, submission_id,
                executor_execution_id, recovery_lease_epoch,
                claim_claimed_at_ms, claim_lease_expires_at_ms,
                intent_state, intent_remote_operation_id,
                intent_provider_request_id, intent_send_started_at_ms,
                intent_receipt_event_identity, intent_failure_event_identity,
                intent_failure_error_code, intent_updated_at_ms, created_at_ms
            )
            SELECT $1, $2, $3, $4, 'claim', $5, intent.submission_id,
                   intent.executor_execution_id, $8, $9, $10, intent.state,
                   intent.remote_operation_id, intent.provider_request_id,
                   intent.send_started_at_ms, intent.receipt_event_identity,
                   intent.failure_event_identity, intent.failure_error_code,
                   intent.updated_at_ms, $9
            FROM provider_remote_submit_intents intent
            WHERE intent.submission_id = $6
              AND intent.executor_execution_id = $7
              AND intent.provider_id = $1
              AND intent.provider_account_id = $2
            "#,
        )
        .bind(&scope.provider_id)
        .bind(scope.provider_account_id)
        .bind(owner)
        .bind(command_id)
        .bind(lease_ms)
        .bind(submission_id)
        .bind(executor_execution_id)
        .bind(recovery_lease_epoch)
        .bind(claimed_at_ms)
        .bind(lease_expires_at_ms)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )
}

async fn insert_recovery_defer_command(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ProviderSubmitRecoveryLease,
    command_id: &str,
    retry_after_ms: i64,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    require_one(
        sqlx::query(
            r#"
            INSERT INTO provider_submit_recovery_commands (
                provider_id, provider_account_id, command_owner, command_id,
                command_kind, request_duration_ms, submission_id,
                executor_execution_id, recovery_lease_epoch, created_at_ms
            ) VALUES ($1, $2, $3, $4, 'defer', $5, $6, $7, $8, $9)
            "#,
        )
        .bind(&lease.intent.provider_id)
        .bind(lease.intent.provider_account_id)
        .bind(&lease.recovery_owner)
        .bind(command_id)
        .bind(retry_after_ms)
        .bind(lease.intent.submission_id)
        .bind(lease.intent.executor_execution_id)
        .bind(lease.recovery_lease_epoch)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )
}

async fn close_submit_recovery(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    now: i64,
    recovery_fence: Option<&super::ProviderSubmitRecoveryFence>,
) -> Result<(), ProviderTaskStoreError> {
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_submit_recoveries
            SET state = 'closed', next_recovery_at_ms = NULL,
                recovery_owner = NULL, recovery_lease_expires_at_ms = NULL,
                recovery_claimed_at_ms = NULL,
                updated_at_ms = $3, closed_at_ms = $3
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND state = 'active'
              AND (
                ($4::TEXT IS NULL AND (
                  recovery_owner IS NULL OR recovery_lease_expires_at_ms <= $3
                ))
                OR (
                  recovery_owner = $4 AND recovery_lease_epoch = $5
                  AND recovery_lease_expires_at_ms > $3
                )
              )
            "#,
        )
        .bind(submission_id)
        .bind(executor_execution_id)
        .bind(now)
        .bind(recovery_fence.map(|fence| fence.recovery_owner.as_str()))
        .bind(recovery_fence.map(|fence| fence.recovery_lease_epoch))
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::StaleLease,
    )
}

fn submit_parent_accepts_evidence(parent: &SubmitParentRow, intent: &SubmitIntentRow) -> bool {
    parent.execution_state == "running"
        && parent.submission_state == "running"
        && parent.executor_owner.as_deref() == Some(intent.submit_owner.as_str())
        && parent.lease_epoch == intent.submit_lease_epoch
        && parent.launch_owner.as_deref() == Some(intent.submit_owner.as_str())
        && parent.launch_lease_epoch == Some(intent.submit_lease_epoch)
        && parent.allocation_state == "held"
}

fn submit_parent_accepts_late_receipt(parent: &SubmitParentRow, intent: &SubmitIntentRow) -> bool {
    parent.execution_state == "uncertain"
        && parent.submission_state == "uncertain"
        && parent.executor_owner.is_none()
        && parent.lease_expires_at_ms.is_none()
        && parent.lease_epoch == intent.submit_lease_epoch
        && parent.launch_owner.as_deref() == Some(intent.submit_owner.as_str())
        && parent.launch_lease_epoch == Some(intent.submit_lease_epoch)
        && parent.execution_resolution_decision_id.is_some()
        && parent.execution_resolution_decision_id == parent.submission_resolution_decision_id
        && parent.resolution_source.as_deref() == Some("remote_submit_deadline")
        && ((parent.allocation_state == "held"
            && parent.reconciliation_state.as_deref() == Some("active"))
            || (parent.allocation_state == "released"
                && parent.allocation_release_reason.as_deref()
                    == Some("provider_capacity_reconciliation")
                && parent.allocation_release_reconciliation_id.is_some()
                && parent.allocation_release_reconciliation_id
                    == parent.execution_resolution_decision_id
                && parent.reconciliation_state.as_deref() == Some("released")))
}

async fn lock_live_submit_fence(
    tx: &mut Transaction<'_, Postgres>,
    request: &RemoteTaskSubmitReservation,
) -> Result<bool, ProviderTaskStoreError> {
    let row: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT TRUE
        FROM executor_executions execution
        JOIN provider_submissions submission
          ON submission.executor_execution_id = execution.executor_execution_id
         AND submission.submission_id = execution.submission_id
        JOIN executor_capacity_allocations allocation
          ON allocation.executor_execution_id = execution.executor_execution_id
         AND allocation.submission_id = execution.submission_id
        WHERE execution.executor_execution_id = $1
          AND execution.submission_id = $2
          AND execution.state = 'running' AND submission.state = 'running'
          AND execution.executor_owner = $3 AND execution.lease_epoch = $4
          AND execution.launch_owner = $3 AND execution.launch_lease_epoch = $4
          AND execution.lease_expires_at_ms >
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
          AND allocation.state = 'held'
        FOR UPDATE OF execution, submission, allocation
        "#,
    )
    .bind(request.executor_execution_id)
    .bind(request.submission_id)
    .bind(&request.executor_owner)
    .bind(request.executor_lease_epoch)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(row.unwrap_or(false))
}

fn submit_binding_is_live(
    binding: &SubmitBindingRow,
    request: &RemoteTaskSubmitReservation,
    now: i64,
) -> bool {
    binding.submission_state == "running"
        && binding.executor_owner.as_deref() == Some(request.executor_owner.as_str())
        && binding.executor_lease_epoch == request.executor_lease_epoch
        && binding.launch_lease_epoch == Some(request.executor_lease_epoch)
        && binding.lease_expires_at_ms.is_some_and(|value| value > now)
        && binding.allocation_state == "held"
}

async fn transition_submit_to_sending(
    tx: &mut Transaction<'_, Postgres>,
    request: &RemoteTaskSubmitReservation,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_remote_submit_intents
            SET state = 'sending', send_started_at_ms = $3, updated_at_ms = $3
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND state = 'reserved'
            "#,
        )
        .bind(request.submission_id)
        .bind(request.executor_execution_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )
}

async fn insert_submit_recovery(
    tx: &mut Transaction<'_, Postgres>,
    request: &RemoteTaskSubmitReservation,
    provider_timeout_ms: i64,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    require_one(
        sqlx::query(
            r#"
            INSERT INTO provider_submit_recoveries
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               invocation_attempt, provider_timeout_ms, provider_deadline_at_ms,
               next_recovery_at_ms, state, created_at_ms, updated_at_ms)
            SELECT intent.submission_id, intent.executor_execution_id,
                   intent.provider_id, intent.provider_account_id,
                   1, $3, $4 + $3,
                   LEAST(execution.lease_expires_at_ms, $4 + $3),
                   'active', $4, $4
            FROM provider_remote_submit_intents intent
            JOIN executor_executions execution
              ON execution.executor_execution_id = intent.executor_execution_id
             AND execution.submission_id = intent.submission_id
            WHERE intent.submission_id = $1
              AND intent.executor_execution_id = $2
              AND intent.state = 'sending'
            "#,
        )
        .bind(request.submission_id)
        .bind(request.executor_execution_id)
        .bind(provider_timeout_ms)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )
}

async fn load_submit_invocation_in(
    tx: &mut Transaction<'_, Postgres>,
    request: &RemoteTaskSubmitReservation,
    provider_timeout_ms: i64,
) -> Result<ProviderSubmitInvocation, ProviderTaskStoreError> {
    let intent = submit_intent_from_row(
        load_submit_intent_in(tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?,
    )?;
    let context = provider_context_from_row(
        load_provider_context_in(tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::Conflict)?,
    );
    if context.provider_timeout_ms != provider_timeout_ms {
        return Err(ProviderTaskStoreError::Conflict);
    }
    Ok(ProviderSubmitInvocation { intent, context })
}

async fn submit_remaining_budget(
    tx: &mut Transaction<'_, Postgres>,
    invocation: &ProviderSubmitInvocation,
) -> Result<u64, ProviderTaskStoreError> {
    let now = database_now(tx).await?;
    remaining_budget_ms(invocation.context.provider_deadline_at_ms, now)
}

fn remaining_budget_ms(deadline_at_ms: i64, now_ms: i64) -> Result<u64, ProviderTaskStoreError> {
    u64::try_from(deadline_at_ms.saturating_sub(now_ms).max(0))
        .map_err(|_| ProviderTaskStoreError::Conflict)
}

fn submit_intent_matches_reservation(
    row: &SubmitIntentRow,
    request: &RemoteTaskSubmitReservation,
) -> bool {
    row.executor_execution_id == request.executor_execution_id
        && row.submit_owner == request.executor_owner
        && row.submit_lease_epoch == request.executor_lease_epoch
        && row.idempotency_key == request.idempotency_key
        && row.output_index == i32::try_from(request.output_index()).unwrap_or(-1)
        && row.output_total == i32::try_from(request.output_total()).unwrap_or(-1)
        && provider_command_sha256_matches(&row.provider_command_sha256, request)
        && row.provider_timeout_ms == request.provider_timeout_ms
}

fn submit_intent_matches_acquire(
    row: &SubmitIntentRow,
    request: &RemoteTaskSubmitReservation,
) -> bool {
    row.executor_execution_id == request.executor_execution_id
        && row.submit_owner == request.executor_owner
        && row.submit_lease_epoch == request.executor_lease_epoch
        && row.idempotency_key == request.idempotency_key
        && row.output_index == i32::try_from(request.output_index()).unwrap_or(-1)
        && row.output_total == i32::try_from(request.output_total()).unwrap_or(-1)
        && provider_command_sha256_matches(&row.provider_command_sha256, request)
}

fn submit_output_matches_binding(
    binding: &SubmitBindingRow,
    request: &RemoteTaskSubmitReservation,
) -> bool {
    u32::try_from(binding.output_index) == Ok(request.output_index())
        && u32::try_from(binding.output_total) == Ok(request.output_total())
}

fn provider_command_sha256_matches(expected: &str, request: &RemoteTaskSubmitReservation) -> bool {
    let mut encoded = [0_u8; 64];
    hex::encode_to_slice(request.provider_command().canonical_sha256(), &mut encoded).is_ok()
        && expected.as_bytes() == encoded
}

fn provider_command_sha256(request: &RemoteTaskSubmitReservation) -> String {
    hex::encode(request.provider_command().canonical_sha256())
}

fn execution_binding_sha256(
    binding: &SubmitBindingRow,
    request: &RemoteTaskSubmitReservation,
    provider_command_sha256: &str,
    provider_timeout_ms: i64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"ai-image-factory/provider-execution-binding/v1\0");
    for (name, value) in [
        ("submission_id", request.submission_id.as_bytes().as_slice()),
        (
            "executor_execution_id",
            request.executor_execution_id.as_bytes().as_slice(),
        ),
        ("output_id", binding.output_id.as_bytes().as_slice()),
        ("provider_id", binding.provider_id.as_bytes()),
        (
            "provider_account_id",
            binding.provider_account_id.as_bytes().as_slice(),
        ),
        ("model", binding.model.as_bytes()),
        ("command_schema", binding.command_schema.as_bytes()),
        ("command_hash", binding.command_hash.as_bytes()),
        ("operation_id", binding.operation_id.as_bytes()),
        (
            "operation_descriptor_revision",
            binding.operation_descriptor_revision.as_bytes(),
        ),
        (
            "operation_descriptor_sha256_v1",
            binding.operation_descriptor_sha256_v1.as_bytes(),
        ),
        ("completion_mode", binding.completion_mode.as_bytes()),
        ("idempotency_mode", binding.idempotency_mode.as_bytes()),
        (
            "operation_binding_version",
            binding.operation_binding_version.to_be_bytes().as_slice(),
        ),
        (
            "execution_profile_id",
            binding.execution_profile_id.as_bytes().as_slice(),
        ),
        ("adapter_revision", binding.adapter_revision.as_bytes()),
        (
            "credential_pool_id",
            binding.credential_pool_id.as_bytes().as_slice(),
        ),
        ("credential_ref", binding.credential_ref.as_bytes()),
        (
            "credential_revision",
            binding.credential_revision.to_be_bytes().as_slice(),
        ),
        (
            "credential_auth_sha256",
            binding.credential_auth_sha256.as_bytes(),
        ),
        (
            "resource_policy_id",
            binding.resource_policy_id.as_bytes().as_slice(),
        ),
        (
            "resource_policy_revision",
            binding.resource_policy_revision.to_be_bytes().as_slice(),
        ),
        ("executor_owner", request.executor_owner.as_bytes()),
        (
            "executor_lease_epoch",
            request.executor_lease_epoch.to_be_bytes().as_slice(),
        ),
        (
            "submission_idempotency_key",
            request.idempotency_key.as_bytes(),
        ),
        (
            "provider_command_sha256",
            provider_command_sha256.as_bytes(),
        ),
        (
            "provider_timeout_ms",
            provider_timeout_ms.to_be_bytes().as_slice(),
        ),
    ] {
        digest.update((name.len() as u64).to_be_bytes());
        digest.update(name.as_bytes());
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    hex::encode(digest.finalize())
}

fn submit_intent_from_row(
    row: SubmitIntentRow,
) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
    let state = match row.state.as_str() {
        "reserved" => ProviderSubmitIntentState::Reserved,
        "sending" => ProviderSubmitIntentState::Sending,
        "outcome_unknown" => ProviderSubmitIntentState::OutcomeUnknown,
        "operation_known" => ProviderSubmitIntentState::OperationKnown,
        "attached" => ProviderSubmitIntentState::Attached,
        "rejected" => ProviderSubmitIntentState::Rejected,
        "deadline_quarantined" => ProviderSubmitIntentState::DeadlineQuarantined,
        _ => return Err(ProviderTaskStoreError::Conflict),
    };
    Ok(ProviderSubmitIntent {
        submission_id: row.submission_id,
        executor_execution_id: row.executor_execution_id,
        provider_id: row.provider_id,
        provider_account_id: row.provider_account_id,
        submit_owner: row.submit_owner,
        submit_lease_epoch: row.submit_lease_epoch,
        idempotency_key: row.idempotency_key,
        output_index: u32::try_from(row.output_index)
            .map_err(|_| ProviderTaskStoreError::Conflict)?,
        output_total: u32::try_from(row.output_total)
            .map_err(|_| ProviderTaskStoreError::Conflict)?,
        provider_command_sha256: row.provider_command_sha256,
        execution_binding_sha256: row.execution_binding_sha256,
        provider_timeout_ms: row.provider_timeout_ms,
        state,
        remote_operation_id: row.remote_operation_id,
        provider_request_id: row.provider_request_id,
        send_started_at_ms: row.send_started_at_ms,
        receipt_event_identity: row.receipt_event_identity,
        failure_event_identity: row.failure_event_identity,
        failure_error_code: row.failure_error_code,
        updated_at_ms: row.updated_at_ms,
    })
}

fn provider_context_from_row(row: ProviderContextRow) -> ProviderExecutionContext {
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

fn recovery_lease_from_row(
    row: RecoveryClaimRow,
) -> Result<ProviderSubmitRecoveryLease, ProviderTaskStoreError> {
    if row.intent_provider_timeout_ms != row.provider_timeout_ms {
        return Err(ProviderTaskStoreError::Conflict);
    }
    let intent = submit_intent_from_row(SubmitIntentRow {
        submission_id: row.submission_id,
        executor_execution_id: row.executor_execution_id,
        provider_id: row.provider_id,
        provider_account_id: row.provider_account_id,
        submit_owner: row.submit_owner,
        submit_lease_epoch: row.submit_lease_epoch,
        idempotency_key: row.idempotency_key.clone(),
        output_index: row.output_index,
        output_total: row.output_total,
        provider_command_sha256: row.provider_command_sha256.clone(),
        execution_binding_sha256: row.execution_binding_sha256.clone(),
        provider_timeout_ms: row.intent_provider_timeout_ms,
        state: row.state,
        remote_operation_id: row.remote_operation_id,
        provider_request_id: row.provider_request_id,
        send_started_at_ms: row.send_started_at_ms,
        receipt_event_identity: row.receipt_event_identity,
        failure_event_identity: row.failure_event_identity,
        failure_error_code: row.failure_error_code,
        updated_at_ms: row.updated_at_ms,
    })?;
    let context = ProviderExecutionContext {
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
    };
    let authority_seal = recovery_lease_authority_seal(
        &intent,
        &context,
        &row.recovery_owner,
        row.recovery_lease_epoch,
    );
    Ok(ProviderSubmitRecoveryLease {
        intent,
        context,
        recovery_owner: row.recovery_owner,
        recovery_lease_epoch: row.recovery_lease_epoch,
        recovery_lease_expires_at_ms: row.recovery_lease_expires_at_ms,
        authority_seal,
    })
}

async fn load_task_in(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<Option<TaskRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission_id, executor_execution_id, provider_id, provider_account_id,
               remote_operation_id, provider_request_id, provider_deadline_at_ms,
               state, artifact_ref, error_code, next_poll_at_ms, cancel_requested,
               poll_owner, poll_lease_epoch, poll_lease_expires_at_ms,
               state_observation_id, deadline_quarantine_id,
               attach_recovery_owner, attach_recovery_lease_epoch
        FROM provider_remote_tasks
        WHERE submission_id = $1
        FOR UPDATE
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn load_task_snapshot_in(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<Option<TaskRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission_id, executor_execution_id, provider_id, provider_account_id,
               remote_operation_id, provider_request_id, provider_deadline_at_ms,
               state, artifact_ref, error_code, next_poll_at_ms, cancel_requested,
               poll_owner, poll_lease_epoch, poll_lease_expires_at_ms,
               state_observation_id, deadline_quarantine_id,
               attach_recovery_owner, attach_recovery_lease_epoch
        FROM provider_remote_tasks
        WHERE submission_id = $1
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

fn task_from_row(row: TaskRow) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
    Ok(ProviderRemoteTask {
        submission_id: row.submission_id,
        executor_execution_id: row.executor_execution_id,
        provider_id: row.provider_id,
        provider_account_id: row.provider_account_id,
        remote_operation_id: row.remote_operation_id,
        provider_request_id: row.provider_request_id,
        state: parse_state(&row.state)?,
        artifact_ref: row.artifact_ref,
        error_code: row.error_code,
        next_poll_at_ms: row.next_poll_at_ms,
        cancel_requested: row.cancel_requested,
        poll_lease_epoch: row.poll_lease_epoch,
    })
}

fn lease_from_row(row: ClaimRow) -> Result<ProviderTaskLease, ProviderTaskStoreError> {
    let ClaimRow {
        submission_id,
        executor_execution_id,
        provider_id,
        provider_account_id,
        remote_operation_id,
        provider_request_id,
        state,
        artifact_ref,
        error_code,
        next_poll_at_ms,
        cancel_requested,
        poll_lease_epoch,
        state_observation_id,
        attach_recovery_owner,
        attach_recovery_lease_epoch,
        poll_owner,
        poll_lease_expires_at_ms,
        claim_updated_at_ms,
        model,
        command_schema,
        command_hash,
        operation_id,
        operation_descriptor_revision,
        operation_descriptor_sha256_v1,
        completion_mode,
        idempotency_mode,
        operation_binding_version,
        execution_profile_id,
        adapter_revision,
        credential_pool_id,
        credential_ref,
        credential_revision,
        credential_auth_sha256,
        resource_policy_id,
        resource_policy_revision,
        idempotency_key,
        provider_command_sha256,
        execution_binding_sha256,
        invocation_attempt,
        provider_timeout_ms,
        provider_deadline_at_ms,
        committed_manifest_id,
        committed_authority_id,
        committed_sha256_hex,
        committed_byte_size,
        committed_media_type,
    } = row;
    let task = task_from_row(TaskRow {
        submission_id,
        executor_execution_id,
        provider_id,
        provider_account_id,
        remote_operation_id,
        provider_request_id,
        provider_deadline_at_ms,
        state,
        artifact_ref,
        error_code,
        next_poll_at_ms,
        cancel_requested,
        poll_owner: Some(poll_owner.clone()),
        poll_lease_epoch,
        poll_lease_expires_at_ms: Some(poll_lease_expires_at_ms),
        state_observation_id,
        deadline_quarantine_id: None,
        attach_recovery_owner,
        attach_recovery_lease_epoch,
    })?;
    let context = ProviderExecutionContext {
        model,
        command_schema,
        command_hash,
        operation_id,
        operation_descriptor_revision,
        operation_descriptor_sha256_v1,
        completion_mode,
        idempotency_mode,
        operation_binding_version,
        execution_profile_id,
        adapter_revision,
        credential_pool_id,
        credential_ref,
        credential_revision,
        credential_auth_sha256,
        resource_policy_id,
        resource_policy_revision,
        submission_idempotency_key: idempotency_key,
        provider_command_sha256,
        execution_binding_sha256,
        invocation_attempt,
        provider_timeout_ms,
        provider_deadline_at_ms,
    };
    let committed_artifact = match (
        committed_manifest_id,
        committed_authority_id,
        committed_sha256_hex,
        committed_byte_size,
        committed_media_type,
    ) {
        (None, None, None, None, None) => None,
        (
            Some(manifest_id),
            Some(authority_id),
            Some(sha256_hex),
            Some(byte_size),
            Some(media_type),
        ) => {
            let manifest = ExecutorResultManifest::new(manifest_id, authority_id)
                .ok_or(ProviderTaskStoreError::Conflict)?;
            if manifest.manifest_id() != task.submission_id
                || manifest.artifact_authority_id() != task.executor_execution_id
            {
                return Err(ProviderTaskStoreError::Conflict);
            }
            Some(ProviderArtifactPublication {
                manifest,
                sha256_hex,
                byte_size: u64::try_from(byte_size)
                    .map_err(|_| ProviderTaskStoreError::Conflict)?,
                media_type,
            })
        }
        _ => return Err(ProviderTaskStoreError::Conflict),
    };
    let authority_seal =
        provider_task_lease_authority_seal(&task, &context, &poll_owner, poll_lease_epoch);
    let remaining_budget_ms = remaining_budget_ms(provider_deadline_at_ms, claim_updated_at_ms)?;
    Ok(ProviderTaskLease {
        task,
        context,
        committed_artifact,
        remaining_budget_ms,
        poll_owner,
        poll_lease_epoch,
        poll_lease_expires_at_ms,
        authority_seal,
    })
}

fn parse_state(value: &str) -> Result<ProviderTaskState, ProviderTaskStoreError> {
    match value {
        "provider_waiting" => Ok(ProviderTaskState::ProviderWaiting),
        "artifact_ready" => Ok(ProviderTaskState::ArtifactReady),
        "failed" => Ok(ProviderTaskState::Failed),
        "canceled" => Ok(ProviderTaskState::Canceled),
        "uncertain" => Ok(ProviderTaskState::Uncertain),
        _ => Err(ProviderTaskStoreError::Conflict),
    }
}

fn validate_attach(value: &RemoteTaskAttach) -> Result<(), ProviderTaskStoreError> {
    if value.submission_id.is_nil()
        || value.executor_execution_id.is_nil()
        || value.submission_id == value.executor_execution_id
        || value.executor_lease_epoch <= 0
        || !valid_owner(&value.executor_owner)
        || !valid_identifier(&value.remote_operation_id, 255)
        || value
            .provider_request_id
            .as_deref()
            .is_some_and(|id| !valid_identifier(id, 255))
        || !valid_public_event_identity(&value.event_identity)
        || !valid_sha256(&value.execution_binding_sha256)
        || !(0..=MAX_POLL_AFTER_MS).contains(&value.poll_after_ms)
        || value.recovery_fence.as_ref().is_some_and(|fence| {
            !valid_owner(&fence.recovery_owner) || fence.recovery_lease_epoch <= 0
        })
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_reservation(value: &RemoteTaskSubmitReservation) -> Result<(), ProviderTaskStoreError> {
    if value.submission_id.is_nil()
        || value.executor_execution_id.is_nil()
        || value.submission_id == value.executor_execution_id
        || value.executor_lease_epoch <= 0
        || !valid_owner(&value.executor_owner)
        || !valid_identifier(&value.idempotency_key, 255)
        || value.output_total() == 0
        || value.output_index() >= value.output_total()
        || i32::try_from(value.output_total()).is_err()
        || !(1..=MAX_PROVIDER_TIMEOUT_MS).contains(&value.provider_timeout_ms)
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_submit_failure(value: &RemoteTaskSubmitFailure) -> Result<(), ProviderTaskStoreError> {
    if value.submission_id.is_nil()
        || value.executor_execution_id.is_nil()
        || value.submission_id == value.executor_execution_id
        || value.executor_lease_epoch <= 0
        || !valid_owner(&value.executor_owner)
        || !valid_identifier(&value.event_identity, 255)
        || !valid_simple_identifier(&value.error_code, 128)
        || !valid_sha256(&value.execution_binding_sha256)
        || value.recovery_fence.as_ref().is_some_and(|fence| {
            value.kind != ProviderSubmitFailureKind::Rejected
                || !valid_owner(&fence.recovery_owner)
                || fence.recovery_lease_epoch <= 0
        })
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_submit_receipt(value: &RemoteTaskSubmitReceipt) -> Result<(), ProviderTaskStoreError> {
    if value.submission_id.is_nil()
        || value.executor_execution_id.is_nil()
        || value.submission_id == value.executor_execution_id
        || value.executor_lease_epoch <= 0
        || !valid_owner(&value.executor_owner)
        || !valid_identifier(&value.remote_operation_id, 255)
        || value
            .provider_request_id
            .as_deref()
            .is_some_and(|id| !valid_identifier(id, 255))
        || !valid_identifier(&value.event_identity, 255)
        || !valid_sha256(&value.execution_binding_sha256)
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_quarantined_receipt(
    value: &RemoteTaskQuarantinedReceipt,
) -> Result<(), ProviderTaskStoreError> {
    if value.submission_id.is_nil()
        || value.executor_execution_id.is_nil()
        || value.submission_id == value.executor_execution_id
        || value.executor_lease_epoch <= 0
        || !valid_owner(&value.executor_owner)
        || !valid_identifier(&value.event_identity, 255)
        || !valid_identifier(&value.expected_provider_id, 255)
        || !valid_identifier(&value.observed_provider_id, 255)
        || !valid_identifier(&value.observed_submission_id, 255)
        || !valid_identifier(&value.remote_operation_id, 255)
        || value
            .provider_request_id
            .as_deref()
            .is_some_and(|id| !valid_identifier(id, 255))
        || !valid_simple_identifier(&value.reason, 128)
        || !valid_sha256(&value.execution_binding_sha256)
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_scope(value: &ProviderTaskClaimScope) -> Result<(), ProviderTaskStoreError> {
    if value.provider_account_id.is_nil() || !valid_simple_identifier(&value.provider_id, 128) {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_owner_and_lease(owner: &str, lease_ms: i64) -> Result<(), ProviderTaskStoreError> {
    if valid_owner(owner) && (1..=MAX_LEASE_MS).contains(&lease_ms) {
        Ok(())
    } else {
        Err(ProviderTaskStoreError::InvalidInput)
    }
}

fn validate_command_id(value: &str) -> Result<(), ProviderTaskStoreError> {
    if valid_identifier(value, 255) {
        Ok(())
    } else {
        Err(ProviderTaskStoreError::InvalidInput)
    }
}

fn recovery_command_lock_key(scope: &ProviderTaskClaimScope, owner: &str, command_id: &str) -> i64 {
    let account_id = scope.provider_account_id.to_string();
    let mut digest = Sha256::new();
    for component in [
        "provider-submit-recovery-command-v1",
        scope.provider_id.as_str(),
        account_id.as_str(),
        owner,
        command_id,
    ] {
        digest.update((component.len() as u64).to_be_bytes());
        digest.update(component.as_bytes());
    }
    let digest = digest.finalize();
    let mut key = [0_u8; 8];
    key.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(key)
}

fn validate_lease(lease: &ProviderTaskLease, lease_ms: i64) -> Result<(), ProviderTaskStoreError> {
    if lease.task.submission_id.is_nil()
        || lease.task.executor_execution_id.is_nil()
        || lease.poll_lease_epoch <= 0
        || lease.task.poll_lease_epoch != lease.poll_lease_epoch
        || !valid_owner(&lease.poll_owner)
        || lease.authority_seal
            != provider_task_lease_authority_seal(
                &lease.task,
                &lease.context,
                &lease.poll_owner,
                lease.poll_lease_epoch,
            )
        || !(1..=MAX_LEASE_MS).contains(&lease_ms)
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_recovery_lease(
    lease: &ProviderSubmitRecoveryLease,
    lease_ms: i64,
) -> Result<(), ProviderTaskStoreError> {
    if lease.intent.submission_id.is_nil()
        || lease.intent.executor_execution_id.is_nil()
        || lease.intent.state == ProviderSubmitIntentState::Reserved
        || lease.recovery_lease_epoch <= 0
        || !valid_owner(&lease.recovery_owner)
        || lease.context.submission_idempotency_key != lease.intent.idempotency_key
        || lease.context.provider_command_sha256 != lease.intent.provider_command_sha256
        || lease.context.execution_binding_sha256 != lease.intent.execution_binding_sha256
        || lease.context.provider_timeout_ms != lease.intent.provider_timeout_ms
        || lease.authority_seal
            != recovery_lease_authority_seal(
                &lease.intent,
                &lease.context,
                &lease.recovery_owner,
                lease.recovery_lease_epoch,
            )
        || !(1..=MAX_PROVIDER_TIMEOUT_MS).contains(&lease.context.provider_timeout_ms)
        || !(1..=MAX_LEASE_MS).contains(&lease_ms)
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn provider_task_lease_authority_seal(
    task: &ProviderRemoteTask,
    context: &ProviderExecutionContext,
    poll_owner: &str,
    poll_lease_epoch: i64,
) -> [u8; 32] {
    let state = match task.state {
        ProviderTaskState::ProviderWaiting => b"provider_waiting".as_slice(),
        ProviderTaskState::ArtifactReady => b"artifact_ready".as_slice(),
        ProviderTaskState::Failed => b"failed".as_slice(),
        ProviderTaskState::Canceled => b"canceled".as_slice(),
        ProviderTaskState::Uncertain => b"uncertain".as_slice(),
    };
    let cancel_requested = [u8::from(task.cancel_requested)];
    authority_seal(
        b"ai-image-factory/provider-task-lease/v2\0",
        &[
            task.submission_id.as_bytes(),
            task.executor_execution_id.as_bytes(),
            task.provider_id.as_bytes(),
            task.provider_account_id.as_bytes(),
            task.remote_operation_id.as_bytes(),
            task.provider_request_id.as_deref().unwrap_or("").as_bytes(),
            state,
            &cancel_requested,
            &task.poll_lease_epoch.to_be_bytes(),
            context.execution_binding_sha256.as_bytes(),
            poll_owner.as_bytes(),
            &poll_lease_epoch.to_be_bytes(),
        ],
    )
}

fn recovery_lease_authority_seal(
    intent: &ProviderSubmitIntent,
    context: &ProviderExecutionContext,
    recovery_owner: &str,
    recovery_lease_epoch: i64,
) -> [u8; 32] {
    authority_seal(
        b"ai-image-factory/provider-submit-recovery-lease/v1\0",
        &[
            intent.submission_id.as_bytes(),
            intent.executor_execution_id.as_bytes(),
            intent.provider_id.as_bytes(),
            intent.provider_account_id.as_bytes(),
            intent.idempotency_key.as_bytes(),
            intent.provider_command_sha256.as_bytes(),
            intent.execution_binding_sha256.as_bytes(),
            context.execution_binding_sha256.as_bytes(),
            recovery_owner.as_bytes(),
            &recovery_lease_epoch.to_be_bytes(),
        ],
    )
}

fn authority_seal(domain: &[u8], values: &[&[u8]]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(domain);
    for value in values {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    digest.finalize().into()
}

fn validate_observation(value: &ProviderTaskObservation) -> Result<(), ProviderTaskStoreError> {
    if !valid_public_event_identity(&value.event_identity) {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    let valid = match &value.outcome {
        ProviderTaskObservationOutcome::Waiting { poll_after_ms } => {
            (0..=MAX_POLL_AFTER_MS).contains(poll_after_ms)
        }
        ProviderTaskObservationOutcome::ArtifactReady {
            artifact_ref,
            publication,
        } => {
            valid_identifier(artifact_ref, 512)
                && publication.sha256_hex.len() == 64
                && publication
                    .sha256_hex
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                && (1..=268_435_456).contains(&publication.byte_size)
                && matches!(
                    publication.media_type.as_str(),
                    "image/png" | "image/jpeg" | "image/webp"
                )
        }
        ProviderTaskObservationOutcome::Failed { error_code }
        | ProviderTaskObservationOutcome::Canceled { error_code }
        | ProviderTaskObservationOutcome::Uncertain { error_code } => {
            valid_simple_identifier(error_code, 128)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(ProviderTaskStoreError::InvalidInput)
    }
}

fn validate_artifact_observation_binding(
    lease: &ProviderTaskLease,
    observation: &ProviderTaskObservation,
) -> Result<(), ProviderTaskStoreError> {
    let ProviderTaskObservationOutcome::ArtifactReady { publication, .. } = &observation.outcome
    else {
        return Ok(());
    };
    if publication.manifest.manifest_id() != lease.task.submission_id
        || publication.manifest.artifact_authority_id() != lease.task.executor_execution_id
    {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    Ok(())
}

fn valid_owner(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn valid_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && !value.contains("://")
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_alphanumeric()
                || (index > 0 && matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-'))
        })
}

fn valid_public_event_identity(value: &str) -> bool {
    value != ARTIFACT_RECOVERY_EVENT && valid_identifier(value, 255)
}

fn valid_simple_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[allow(clippy::too_many_arguments)]
fn observation_hash(
    source: &str,
    state: &str,
    artifact_ref: Option<&str>,
    result_manifest_id: Option<Uuid>,
    artifact_sha256_hex: Option<&str>,
    artifact_byte_size: Option<u64>,
    artifact_media_type: Option<&str>,
    error_code: Option<&str>,
    effect_certainty: &str,
    next_poll_at_ms: Option<i64>,
    poll_owner: Option<&str>,
    poll_lease_epoch: Option<i64>,
) -> String {
    let mut hash = Sha256::new();
    let result_manifest_id = result_manifest_id
        .map(|value| value.to_string())
        .unwrap_or_default();
    for value in [
        source,
        state,
        artifact_ref.unwrap_or(""),
        &result_manifest_id,
        artifact_sha256_hex.unwrap_or(""),
        artifact_media_type.unwrap_or(""),
        error_code.unwrap_or(""),
        effect_certainty,
        poll_owner.unwrap_or(""),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(artifact_byte_size.unwrap_or(0).to_be_bytes());
    hash.update(next_poll_at_ms.unwrap_or(-1).to_be_bytes());
    hash.update(poll_lease_epoch.unwrap_or(-1).to_be_bytes());
    hex::encode(hash.finalize())
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ProviderTaskStoreError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

async fn lock_held_capacity(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
) -> Result<(), ProviderTaskStoreError> {
    let held: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT TRUE
        FROM executor_capacity_allocations
        WHERE executor_execution_id = $1 AND submission_id = $2 AND state = 'held'
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
            WHERE executor_execution_id = $1 AND submission_id = $2 AND state = 'held'
            "#,
        )
        .bind(executor_execution_id)
        .bind(submission_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
        ProviderTaskStoreError::Conflict,
    )
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
    fn elapsed_deadline_has_zero_remaining_budget() {
        assert_eq!(remaining_budget_ms(999, 1_000), Ok(0));
        assert_eq!(remaining_budget_ms(1_000, 1_000), Ok(0));
        assert_eq!(remaining_budget_ms(1_001, 1_000), Ok(1));
    }

    #[test]
    fn legacy_waiting_replay_requires_the_original_relative_delay() {
        let observed_at_ms = 1_000;
        let next_poll_at_ms = 1_250;
        let poll_owner = "legacy-poller";
        let legacy_hash = observation_hash(
            "poll",
            "provider_waiting",
            None,
            None,
            None,
            None,
            None,
            None,
            "not_applicable",
            Some(next_poll_at_ms),
            Some(poll_owner),
            Some(1),
        );
        let existing = ExistingObservation {
            observation_id: Uuid::new_v4(),
            source: "poll".to_string(),
            observed_state: "provider_waiting".to_string(),
            artifact_ref: None,
            result_manifest_id: None,
            artifact_sha256_hex: None,
            artifact_byte_size: None,
            artifact_media_type: None,
            error_code: None,
            effect_certainty: "not_applicable".to_string(),
            next_poll_at_ms: Some(next_poll_at_ms),
            poll_owner: Some(poll_owner.to_string()),
            poll_lease_epoch: Some(1),
            payload_hash: legacy_hash,
            observed_at_ms,
        };
        let semantic_hash = observation_hash(
            "poll",
            "provider_waiting",
            None,
            None,
            None,
            None,
            None,
            None,
            "not_applicable",
            Some(250),
            Some(poll_owner),
            Some(1),
        );
        let mut replay = NewObservation {
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            event_identity: "legacy-waiting",
            source: "poll",
            state: "provider_waiting",
            artifact_ref: None,
            result_manifest_id: None,
            artifact_sha256_hex: None,
            artifact_byte_size: None,
            artifact_media_type: None,
            error_code: None,
            effect_certainty: "not_applicable",
            next_poll_at_ms: Some(2_250),
            poll_owner: Some(poll_owner),
            poll_lease_epoch: Some(1),
            payload_hash: &semantic_hash,
            requested_poll_after_ms: Some(250),
            now: 2_000,
        };
        assert!(legacy_waiting_replay_matches(&existing, &replay));
        replay.requested_poll_after_ms = Some(251);
        assert!(!legacy_waiting_replay_matches(&existing, &replay));
    }
}
