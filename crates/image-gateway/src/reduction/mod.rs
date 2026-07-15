use async_trait::async_trait;
use uuid::Uuid;

use crate::artifacts::ArtifactMetadata;

mod artifacts;
mod daemon;
mod postgres;

pub use artifacts::{CustomerArtifactPublishError, CustomerArtifactPublisher};
pub use daemon::{ReducerDaemon, ReducerDaemonError, ReducerDaemonRun, TerminalArtifactPublisher};
pub use postgres::PostgresExecutorTerminalStore;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorTerminalArtifact {
    pub authority_id: Uuid,
    pub storage_backend: String,
    pub storage_namespace: String,
    pub object_key: String,
    pub sha256_hex: String,
    pub byte_size: u64,
    pub media_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CanonicalExecutorOutcome {
    Succeeded(ExecutorTerminalArtifact),
    Failed { error_code: String },
    Uncertain { error_code: String },
    Canceled { error_code: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorTerminalLease {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub resolution_decision_id: Uuid,
    pub output_id: Uuid,
    pub output_index: i32,
    pub job_id: Uuid,
    pub tenant_id: String,
    pub work_item_id: Uuid,
    pub attempt_execution_id: Uuid,
    pub attempt_lease_epoch: i64,
    pub reducer_owner: String,
    pub reducer_lease_epoch: i64,
    pub reducer_lease_expires_at_ms: i64,
    pub outcome: CanonicalExecutorOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutorParentTerminalState {
    Pending,
    Succeeded,
    Failed,
    Uncertain,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorTerminalCompletion {
    pub receipt_id: Uuid,
    pub customer_artifact_id: Option<Uuid>,
    pub parent_state: ExecutorParentTerminalState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExecutorTerminalError {
    #[error("executor terminal reduction storage is unavailable")]
    Unavailable,
    #[error("executor terminal reduction input is invalid")]
    InvalidInput,
    #[error("executor terminal reduction conflicts with canonical evidence")]
    Conflict,
    #[error("executor terminal reduction lease is stale or invalid")]
    StaleLease,
}

#[async_trait]
pub trait ExecutorTerminalStore: Send + Sync + 'static {
    async fn claim_terminal(
        &self,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ExecutorTerminalLease>, ExecutorTerminalError>;

    async fn heartbeat_terminal(
        &self,
        lease: &ExecutorTerminalLease,
        lease_ms: i64,
    ) -> Result<ExecutorTerminalLease, ExecutorTerminalError>;

    async fn complete_terminal(
        &self,
        lease: &ExecutorTerminalLease,
        customer_artifact: Option<&ArtifactMetadata>,
    ) -> Result<ExecutorTerminalCompletion, ExecutorTerminalError>;
}
