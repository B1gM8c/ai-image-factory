use async_trait::async_trait;
use uuid::Uuid;

use crate::executor::ExecutorResultManifest;

mod postgres;

pub use postgres::PostgresProviderTaskStore;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderTaskState {
    ProviderWaiting,
    ArtifactReady,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteTaskAttach {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub executor_owner: String,
    pub executor_lease_epoch: i64,
    pub remote_operation_id: String,
    pub provider_request_id: Option<String>,
    pub event_identity: String,
    pub poll_after_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteTaskSubmitReservation {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub executor_owner: String,
    pub executor_lease_epoch: i64,
    pub idempotency_key: String,
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
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSubmitStart {
    Acquired(ProviderSubmitIntent),
    Existing(ProviderSubmitIntent),
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTaskClaimScope {
    pub provider_id: String,
    pub provider_account_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTaskLease {
    pub task: ProviderRemoteTask,
    pub poll_owner: String,
    pub poll_lease_epoch: i64,
    pub poll_lease_expires_at_ms: i64,
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
    Waiting { poll_after_ms: i64 },
    ArtifactReady { artifact_ref: String },
    Failed { error_code: String },
    Canceled { error_code: String },
    Uncertain { error_code: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTaskObservation {
    pub event_identity: String,
    pub source: ProviderTaskObservationSource,
    pub outcome: ProviderTaskObservationOutcome,
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
    #[error("provider task poll lease is stale")]
    StaleLease,
}

#[async_trait]
pub trait ProviderTaskStore: Send + Sync + 'static {
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

    async fn load_submit_intent(
        &self,
        submission_id: Uuid,
    ) -> Result<Option<ProviderSubmitIntent>, ProviderTaskStoreError>;

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
    ) -> Result<ExecutorResultManifest, ProviderTaskStoreError>;

    async fn resolve_artifact(
        &self,
        submission_id: Uuid,
        manifest: &ExecutorResultManifest,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError>;

    async fn record_verified_callback(
        &self,
        callback: &VerifiedCallbackWakeup,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError>;
}
