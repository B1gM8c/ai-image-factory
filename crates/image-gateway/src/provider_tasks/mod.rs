use std::{fmt, sync::Arc};

use async_trait::async_trait;
use image_provider_sdk::{
    DurableArtifactManifest, OutputSlot, ProviderCommandIdentity, ProviderTaskProgress,
    RemoteVideoResult,
};
use serde_json::Value;
use uuid::Uuid;

use crate::executor::{ExecutorInputObject, ExecutorResultManifest, ExecutorSubmissionLease};

mod account_home;
mod capacity;
mod poll;
mod postgres;
mod readiness;
mod remote_submit;
mod runtime_profile;
mod submit;

pub(crate) use postgres::lock_dreamina_submit_credentials;

const MAX_PROVIDER_RUNTIME_LANES: usize = 1_024;
const SUBMIT_CREDENTIAL_BLOCKED: &str = "provider_submit_credential_blocked";

pub use account_home::{ProviderAccountHomeCapability, ProviderAccountHomeCapabilityError};

pub use capacity::{
    ProviderCapacityEvidence, ProviderCapacityEvidenceOutcome, ProviderCapacityReconciliation,
    ProviderCapacityReconciliationLease, ProviderCapacityReconciliationState,
    ProviderCapacityReconciliationStore, ProviderCapacityTerminalState,
};
pub use poll::{
    ProviderAdditionalImageArtifactSink, ProviderArtifactSinkContractError,
    ProviderArtifactStageContext, ProviderArtifactStager, ProviderArtifactStagerFactory,
    ProviderNativeBatchArtifactSink, ProviderNativeBatchPollObservation, ProviderPollDaemon,
    ProviderPollDaemonConfig, ProviderPollDaemonError, ProviderPollDaemonReport,
    ProviderPollDriver, ProviderPollDriverCall, ProviderPollIteration, ProviderPollOrchestrator,
    ProviderPollOrchestratorConfig, ProviderPollOrchestratorError, ProviderPollRun,
    ProviderPollStore, StagedProviderArtifact,
};
pub use postgres::PostgresProviderTaskStore;
pub use readiness::{
    ExecutionQueueReadinessSummary, ProviderProfileReadiness, ProviderProfileReadinessStatus,
    ProviderProfileReadinessStore, ProviderProfileReadinessSummary, ProviderRuntimeLease,
    ProviderRuntimeLeaseState, ProviderRuntimeReadinessStore, ProviderRuntimeRegistration,
    ProviderRuntimeRole, ProviderRuntimeShutdown, ProviderRuntimeSupervisor,
    ProviderRuntimeSupervisorConfig, ProviderRuntimeSupervisorError,
};
pub use remote_submit::{
    DreaminaComplianceRejectionResolution, DreaminaComplianceRejectionResolutionRequest,
    DreaminaVideoSupplementOperatorReceipt, GatedCliBinding, GatedCliCommand, GatedCliInputFile,
    GatedCliObservation, GatedCliPreparedSubmission, GatedCliProcessError, GatedCliProcessOutcome,
    GatedCliProcessTerminal, GatedCliReady, GatedCliSubmission, GatedCliSubmitCodec,
    GatedCliSubmitDriver, dispatch_dreamina_video_supplement_operator,
    poll_dreamina_video_supplement_operator, prepare_dreamina_video_supplement_operator,
    prepare_dreamina_video_supplement_successor_operator, resolve_dreamina_compliance_rejection,
    run_remote_submit_gate, run_remote_submit_runner,
};
pub use runtime_profile::{ProviderRuntimeProfile, ProviderRuntimeProfileStore};
pub use submit::{
    ProviderSubmitDaemon, ProviderSubmitDaemonConfig, ProviderSubmitDaemonError,
    ProviderSubmitDaemonReport, ProviderSubmitDriver, ProviderSubmitDriverCall,
    ProviderSubmitDriverRecovery, ProviderSubmitIteration, ProviderSubmitIterationCommand,
    ProviderSubmitIterationCommandError, ProviderSubmitOrchestrationStore,
    ProviderSubmitOrchestrator, ProviderSubmitOrchestratorError, ProviderSubmitOutcome,
    ProviderSubmitProjectionError, ProviderSubmitProjector, ProviderSubmitRecoveryWork,
    ProviderSubmitRun, ProviderSubmitSchedulingStore, ProviderSubmitService,
    ProviderSubmitServiceConfig, ProviderSubmitServiceError, ProviderSubmitWork,
};

/// Stable storage identity for a nonbillable image returned alongside source zero.
/// This is deliberately outside executor result manifests and billing output slots.
pub fn additional_image_artifact_id(execution_id: Uuid, source_index: usize) -> Option<Uuid> {
    use sha2::{Digest, Sha256};
    if execution_id.is_nil() || !(1..=9).contains(&source_index) {
        return None;
    }
    let mut digest = Sha256::new();
    digest.update(b"dreamina-additional-image-v1\0");
    digest.update(execution_id.as_bytes());
    digest.update([source_index as u8]);
    let hash = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Some(Uuid::from_bytes(bytes))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderTaskState {
    ProviderWaiting,
    ArtifactReady,
    RemoteReady,
    Failed,
    Canceled,
    Uncertain,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRemoteTask {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub provider_id: String,
    pub provider_account_id: Uuid,
    pub remote_operation_id: String,
    pub provider_request_id: Option<String>,
    pub state: ProviderTaskState,
    pub artifact_ref: Option<String>,
    pub error_code: Option<String>,
    pub next_poll_at_ms: Option<i64>,
    pub cancel_requested: bool,
    pub poll_lease_epoch: i64,
}

/// Frozen output membership for one native Dreamina image submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeBatchMember {
    pub output_index: i32,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub output_id: Uuid,
    pub job_id: Uuid,
    pub committed_artifact: Option<ProviderArtifactPublication>,
    pub committed_failure_code: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteTaskAttach {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub executor_owner: String,
    pub executor_lease_epoch: i64,
    pub remote_operation_id: String,
    pub provider_request_id: Option<String>,
    pub event_identity: String,
    pub execution_binding_sha256: String,
    pub poll_after_ms: i64,
    pub recovery_fence: Option<ProviderSubmitRecoveryFence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteTaskSubmitReservation {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub executor_owner: String,
    pub executor_lease_epoch: i64,
    pub idempotency_key: String,
    output: OutputSlot,
    provider_command: ProviderCommandIdentity,
    pub provider_timeout_ms: i64,
}

impl RemoteTaskSubmitReservation {
    pub fn new(
        executor: &ExecutorSubmissionLease,
        idempotency_key: String,
        output: OutputSlot,
        provider_command: ProviderCommandIdentity,
        provider_timeout_ms: i64,
    ) -> Self {
        Self {
            submission_id: executor.submission_id,
            executor_execution_id: executor.executor_execution_id,
            executor_owner: executor.executor_owner.clone(),
            executor_lease_epoch: executor.executor_lease_epoch,
            idempotency_key,
            output,
            provider_command,
            provider_timeout_ms,
        }
    }

    pub fn provider_command(&self) -> ProviderCommandIdentity {
        self.provider_command
    }

    pub fn output_index(&self) -> u32 {
        self.output.index()
    }

    pub fn output_total(&self) -> u32 {
        self.output.total()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSubmitIntent {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub provider_id: String,
    pub provider_account_id: Uuid,
    pub submit_owner: String,
    pub submit_lease_epoch: i64,
    pub idempotency_key: String,
    pub output_index: u32,
    pub output_total: u32,
    pub provider_command_sha256: String,
    pub execution_binding_sha256: String,
    pub provider_timeout_ms: i64,
    pub state: ProviderSubmitIntentState,
    pub remote_operation_id: Option<String>,
    pub provider_request_id: Option<String>,
    pub send_started_at_ms: Option<i64>,
    pub receipt_event_identity: Option<String>,
    pub failure_event_identity: Option<String>,
    pub failure_error_code: Option<String>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderSubmitIntentState {
    Reserved,
    Sending,
    OutcomeUnknown,
    OperationKnown,
    Attached,
    Rejected,
    DeadlineQuarantined,
}

impl ProviderSubmitIntentState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Sending => "sending",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::OperationKnown => "operation_known",
            Self::Attached => "attached",
            Self::Rejected => "rejected",
            Self::DeadlineQuarantined => "deadline_quarantined",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderSubmitFailureKind {
    Rejected,
    OutcomeUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteTaskSubmitFailure {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub executor_owner: String,
    pub executor_lease_epoch: i64,
    pub kind: ProviderSubmitFailureKind,
    pub event_identity: String,
    pub error_code: String,
    pub execution_binding_sha256: String,
    pub recovery_fence: Option<ProviderSubmitRecoveryFence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteTaskSubmitReceipt {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub executor_owner: String,
    pub executor_lease_epoch: i64,
    pub remote_operation_id: String,
    pub provider_request_id: Option<String>,
    pub event_identity: String,
    pub execution_binding_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteTaskQuarantinedReceipt {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub executor_owner: String,
    pub executor_lease_epoch: i64,
    pub event_identity: String,
    pub expected_provider_id: String,
    pub observed_provider_id: String,
    pub observed_submission_id: String,
    pub remote_operation_id: String,
    pub provider_request_id: Option<String>,
    pub reason: String,
    pub execution_binding_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTaskClaimScope {
    pub provider_id: String,
    pub provider_account_id: Uuid,
    pub execution_profile_id: Uuid,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProviderExecutionContext {
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
    submission_idempotency_key: String,
    provider_command_sha256: String,
    execution_binding_sha256: String,
    invocation_attempt: i32,
    provider_timeout_ms: i64,
    provider_deadline_at_ms: i64,
}

impl ProviderExecutionContext {
    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn command_schema(&self) -> &str {
        &self.command_schema
    }

    pub fn command_hash(&self) -> &str {
        &self.command_hash
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub fn operation_descriptor_revision(&self) -> &str {
        &self.operation_descriptor_revision
    }

    pub fn operation_descriptor_sha256_v1(&self) -> &str {
        &self.operation_descriptor_sha256_v1
    }

    pub fn completion_mode(&self) -> &str {
        &self.completion_mode
    }

    pub fn idempotency_mode(&self) -> &str {
        &self.idempotency_mode
    }

    pub fn operation_binding_version(&self) -> i16 {
        self.operation_binding_version
    }

    pub fn execution_profile_id(&self) -> Uuid {
        self.execution_profile_id
    }

    pub fn adapter_revision(&self) -> &str {
        &self.adapter_revision
    }

    pub fn credential_pool_id(&self) -> Uuid {
        self.credential_pool_id
    }

    pub fn credential_ref(&self) -> &str {
        &self.credential_ref
    }

    pub fn credential_revision(&self) -> i64 {
        self.credential_revision
    }

    pub fn credential_auth_sha256(&self) -> &str {
        &self.credential_auth_sha256
    }

    pub fn resource_policy_id(&self) -> Uuid {
        self.resource_policy_id
    }

    pub fn resource_policy_revision(&self) -> i64 {
        self.resource_policy_revision
    }

    pub fn provider_command_sha256(&self) -> &str {
        &self.provider_command_sha256
    }

    pub fn execution_binding_sha256(&self) -> &str {
        &self.execution_binding_sha256
    }

    pub fn invocation_attempt(&self) -> i32 {
        self.invocation_attempt
    }

    pub fn provider_timeout_ms(&self) -> i64 {
        self.provider_timeout_ms
    }

    pub fn provider_deadline_at_ms(&self) -> i64 {
        self.provider_deadline_at_ms
    }
}

impl fmt::Debug for ProviderExecutionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderExecutionContext")
            .field("model", &self.model)
            .field("command_schema", &self.command_schema)
            .field("command_hash", &self.command_hash)
            .field("operation_id", &self.operation_id)
            .field(
                "operation_descriptor_revision",
                &self.operation_descriptor_revision,
            )
            .field(
                "operation_descriptor_sha256_v1",
                &self.operation_descriptor_sha256_v1,
            )
            .field("completion_mode", &self.completion_mode)
            .field("idempotency_mode", &self.idempotency_mode)
            .field("operation_binding_version", &self.operation_binding_version)
            .field("execution_profile_id", &self.execution_profile_id)
            .field("adapter_revision", &self.adapter_revision)
            .field("credential_pool_id", &self.credential_pool_id)
            .field("credential_ref", &"[redacted]")
            .field("credential_revision", &self.credential_revision)
            .field("credential_auth_sha256", &"[redacted]")
            .field("resource_policy_id", &self.resource_policy_id)
            .field("resource_policy_revision", &self.resource_policy_revision)
            .field("submission_idempotency_key", &"[redacted]")
            .field("provider_command_sha256", &self.provider_command_sha256)
            .field("execution_binding_sha256", &self.execution_binding_sha256)
            .field("invocation_attempt", &self.invocation_attempt)
            .field("provider_timeout_ms", &self.provider_timeout_ms)
            .field("provider_deadline_at_ms", &self.provider_deadline_at_ms)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSubmitInvocation {
    pub intent: ProviderSubmitIntent,
    context: ProviderExecutionContext,
}

impl ProviderSubmitInvocation {
    pub fn context(&self) -> &ProviderExecutionContext {
        &self.context
    }

    pub fn submission_idempotency_key(&self) -> &str {
        &self.context.submission_idempotency_key
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSubmitStart {
    Acquired(ProviderSubmitInvocation),
    Existing(ProviderSubmitInvocation),
}

#[derive(Debug, Eq, PartialEq)]
pub struct ProviderSubmitDispatchAuthority {
    invocation: ProviderSubmitInvocation,
    remaining_budget_ms: u64,
}

impl ProviderSubmitDispatchAuthority {
    pub fn intent(&self) -> &ProviderSubmitIntent {
        &self.invocation.intent
    }

    pub fn context(&self) -> &ProviderExecutionContext {
        self.invocation.context()
    }

    pub fn remaining_budget_ms(&self) -> u64 {
        self.remaining_budget_ms
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ProviderSubmitAttachAuthority {
    invocation: ProviderSubmitInvocation,
}

impl ProviderSubmitAttachAuthority {
    pub fn intent(&self) -> &ProviderSubmitIntent {
        &self.invocation.intent
    }

    pub fn context(&self) -> &ProviderExecutionContext {
        self.invocation.context()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ProviderSubmitBusyAuthority {
    invocation: ProviderSubmitInvocation,
    remaining_budget_ms: u64,
}

impl ProviderSubmitBusyAuthority {
    pub fn intent(&self) -> &ProviderSubmitIntent {
        &self.invocation.intent
    }

    pub fn context(&self) -> &ProviderExecutionContext {
        self.invocation.context()
    }

    pub fn remaining_budget_ms(&self) -> u64 {
        self.remaining_budget_ms
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ProviderSubmitAcquire {
    Dispatch(ProviderSubmitDispatchAuthority),
    AttachOnly(ProviderSubmitAttachAuthority),
    Busy(ProviderSubmitBusyAuthority),
    ObserveOnly(ProviderSubmitInvocation),
    Terminal(ProviderSubmitIntent),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSubmitRecoveryFence {
    pub recovery_owner: String,
    pub recovery_lease_epoch: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTaskLease {
    pub task: ProviderRemoteTask,
    context: ProviderExecutionContext,
    committed_artifact: Option<ProviderArtifactPublication>,
    remaining_budget_ms: u64,
    completion_deadline_at_ms: i64,
    pub poll_owner: String,
    pub poll_lease_epoch: i64,
    pub poll_lease_expires_at_ms: i64,
    authority_seal: [u8; 32],
}

impl ProviderTaskLease {
    pub fn context(&self) -> &ProviderExecutionContext {
        &self.context
    }

    pub fn committed_artifact(&self) -> Option<&ProviderArtifactPublication> {
        self.committed_artifact.as_ref()
    }

    pub fn remaining_budget_ms(&self) -> u64 {
        self.remaining_budget_ms
    }

    /// Completion authority is distinct from the immutable submit deadline.
    pub fn completion_deadline_at_ms(&self) -> i64 {
        self.completion_deadline_at_ms
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProviderSubmitRecoveryLease {
    pub intent: ProviderSubmitIntent,
    context: ProviderExecutionContext,
    command_json: Value,
    inputs: Arc<[ExecutorInputObject]>,
    remaining_budget_ms: u64,
    pub recovery_owner: String,
    pub recovery_lease_epoch: i64,
    pub recovery_lease_expires_at_ms: i64,
    authority_seal: [u8; 32],
}

impl fmt::Debug for ProviderSubmitRecoveryLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderSubmitRecoveryLease")
            .field("intent", &self.intent)
            .field("context", &self.context)
            .field("command_json", &"[redacted]")
            .field("remaining_budget_ms", &self.remaining_budget_ms)
            .field("recovery_owner", &self.recovery_owner)
            .field("recovery_lease_epoch", &self.recovery_lease_epoch)
            .field(
                "recovery_lease_expires_at_ms",
                &self.recovery_lease_expires_at_ms,
            )
            .field("authority_seal", &"[redacted]")
            .finish()
    }
}

impl ProviderSubmitRecoveryLease {
    pub fn context(&self) -> &ProviderExecutionContext {
        &self.context
    }

    pub fn command_json(&self) -> &Value {
        &self.command_json
    }

    pub fn inputs(&self) -> &[ExecutorInputObject] {
        &self.inputs
    }

    pub fn remaining_budget_ms(&self) -> u64 {
        self.remaining_budget_ms
    }

    pub fn submission_idempotency_key(&self) -> &str {
        &self.context.submission_idempotency_key
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderArtifactAuthority {
    storage_backend: String,
    storage_namespace: String,
    object_key: String,
    sha256_hex: String,
    byte_size: u64,
    media_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderArtifactPublication {
    manifest: ExecutorResultManifest,
    sha256_hex: String,
    byte_size: u64,
    media_type: String,
}

impl ProviderArtifactPublication {
    pub fn manifest(&self) -> &ExecutorResultManifest {
        &self.manifest
    }

    pub fn matches_durable_manifest(&self, manifest: &DurableArtifactManifest) -> bool {
        let mut sha256 = [0_u8; 32];
        hex::decode_to_slice(&self.sha256_hex, &mut sha256).is_ok()
            && self.byte_size == manifest.byte_size()
            && self.media_type == manifest.media_type()
            && sha256 == *manifest.sha256()
    }
}

impl ProviderArtifactAuthority {
    pub fn new(
        storage_backend: String,
        storage_namespace: String,
        object_key: String,
        sha256_hex: String,
        byte_size: u64,
        media_type: String,
    ) -> Option<Self> {
        if !crate::executor::artifact_descriptor_is_valid(
            &storage_backend,
            &storage_namespace,
            &object_key,
            &sha256_hex,
            byte_size,
            &media_type,
        ) {
            return None;
        }
        Some(Self {
            storage_backend,
            storage_namespace,
            object_key,
            sha256_hex,
            byte_size,
            media_type,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderTaskObservationSource {
    Poll,
    Cancel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderTaskObservationOutcome {
    RemoteReady {
        result: RemoteVideoResult,
    },
    Waiting {
        poll_after_ms: i64,
    },
    ArtifactReady {
        artifact_ref: String,
        publication: ProviderArtifactPublication,
    },
    Failed {
        error_code: String,
    },
    Canceled {
        error_code: String,
    },
    Uncertain {
        error_code: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTaskObservation {
    pub event_identity: String,
    pub source: ProviderTaskObservationSource,
    pub outcome: ProviderTaskObservationOutcome,
    pub progress: Option<ProviderTaskProgress>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedCallbackWakeup {
    pub submission_id: Uuid,
    pub event_identity: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderTaskStoreError {
    #[error("provider task storage is unavailable")]
    Unavailable,
    #[error("provider task input is invalid")]
    InvalidInput,
    #[error("provider task identity or evidence conflicts")]
    Conflict,
    #[error("provider task was not found")]
    NotFound,
    #[error("provider task lease is stale")]
    StaleLease,
}

/// Keeps fresh credential state stable until dispatch permission is durable.
/// Dropping the guard releases the database locks; it does not change task state.
pub struct ProviderSubmitCredentialGuard {
    pub(crate) _transaction: Option<sqlx::Transaction<'static, sqlx::Postgres>>,
}

impl ProviderSubmitCredentialGuard {
    pub(crate) fn unrestricted() -> Self {
        Self { _transaction: None }
    }
}

#[async_trait]
pub trait ProviderTaskStore: Send + Sync + 'static {
    async fn guard_submit_credentials(
        &self,
        intent: &ProviderSubmitIntent,
        _context: &ProviderExecutionContext,
    ) -> Result<ProviderSubmitCredentialGuard, ProviderTaskStoreError> {
        if intent.provider_id == image_provider_dreamina_cli::PROVIDER_ID {
            return Err(ProviderTaskStoreError::Conflict);
        }
        Ok(ProviderSubmitCredentialGuard::unrestricted())
    }

    async fn acquire_submit(
        &self,
        request: &RemoteTaskSubmitReservation,
    ) -> Result<ProviderSubmitAcquire, ProviderTaskStoreError>;

    async fn reserve_submit(
        &self,
        request: &RemoteTaskSubmitReservation,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError>;

    async fn start_submit(
        &self,
        request: &RemoteTaskSubmitReservation,
    ) -> Result<ProviderSubmitStart, ProviderTaskStoreError>;

    async fn record_submit_failure(
        &self,
        request: &RemoteTaskSubmitFailure,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError>;

    async fn record_submit_receipt(
        &self,
        request: &RemoteTaskSubmitReceipt,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError>;

    async fn quarantine_submit_receipt(
        &self,
        request: &RemoteTaskQuarantinedReceipt,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError>;

    async fn load_submit_intent(
        &self,
        submission_id: Uuid,
    ) -> Result<Option<ProviderSubmitIntent>, ProviderTaskStoreError>;

    async fn resolve_due_submit_deadline(
        &self,
        scope: &ProviderTaskClaimScope,
    ) -> Result<Option<ProviderSubmitIntent>, ProviderTaskStoreError>;

    async fn claim_submit_recovery(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        command_id: &str,
        lease_ms: i64,
    ) -> Result<Option<ProviderSubmitRecoveryLease>, ProviderTaskStoreError>;

    async fn heartbeat_submit_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
        lease_ms: i64,
    ) -> Result<ProviderSubmitRecoveryLease, ProviderTaskStoreError>;

    async fn defer_submit_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
        command_id: &str,
        retry_after_ms: i64,
    ) -> Result<(), ProviderTaskStoreError>;

    async fn attach(
        &self,
        request: &RemoteTaskAttach,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError>;

    async fn load(
        &self,
        submission_id: Uuid,
    ) -> Result<Option<ProviderRemoteTask>, ProviderTaskStoreError>;

    async fn claim_due(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ProviderTaskLease>, ProviderTaskStoreError>;

    async fn heartbeat(
        &self,
        lease: &ProviderTaskLease,
        lease_ms: i64,
    ) -> Result<ProviderTaskLease, ProviderTaskStoreError>;

    async fn request_cancel(
        &self,
        submission_id: Uuid,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError>;

    async fn record_observation(
        &self,
        lease: &ProviderTaskLease,
        observation: &ProviderTaskObservation,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError>;

    async fn publish_artifact_authority(
        &self,
        lease: &ProviderTaskLease,
        authority: &ProviderArtifactAuthority,
    ) -> Result<ProviderArtifactPublication, ProviderTaskStoreError>;

    async fn prepare_additional_image_artifacts(
        &self,
        _lease: &ProviderTaskLease,
    ) -> Result<(), ProviderTaskStoreError> {
        Ok(())
    }

    async fn publish_additional_image_artifacts(
        &self,
        _lease: &ProviderTaskLease,
        authorities: &[(usize, ProviderArtifactAuthority)],
    ) -> Result<(), ProviderTaskStoreError> {
        if authorities.is_empty() {
            Ok(())
        } else {
            Err(ProviderTaskStoreError::InvalidInput)
        }
    }

    async fn native_batch_members(
        &self,
        _lease: &ProviderTaskLease,
    ) -> Result<Option<Vec<NativeBatchMember>>, ProviderTaskStoreError> {
        Ok(None)
    }

    async fn publish_native_batch_artifact(
        &self,
        _lease: &ProviderTaskLease,
        _output_index: i32,
        _authority: &ProviderArtifactAuthority,
    ) -> Result<ProviderArtifactPublication, ProviderTaskStoreError> {
        Err(ProviderTaskStoreError::InvalidInput)
    }

    async fn record_native_batch_failure(
        &self,
        _lease: &ProviderTaskLease,
        _output_index: i32,
        _error_code: &str,
    ) -> Result<(), ProviderTaskStoreError> {
        Err(ProviderTaskStoreError::InvalidInput)
    }

    async fn record_verified_callback(
        &self,
        callback: &VerifiedCallbackWakeup,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError>;
}

#[async_trait]
pub trait ProviderTaskDeadlineStore: Send + Sync + 'static {
    async fn resolve_due_remote_task_deadline(
        &self,
        scope: &ProviderTaskClaimScope,
    ) -> Result<Option<ProviderRemoteTask>, ProviderTaskStoreError>;
}
