use async_trait::async_trait;
use uuid::Uuid;

use crate::admission::WorkLease;

mod postgres;

pub use postgres::PostgresExecutorSubmissionStore;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedExecutorSubmission {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub output_id: Uuid,
    pub job_id: Uuid,
    pub tenant_id: String,
    pub provider_id: String,
    pub model: String,
    pub work_item_id: Uuid,
    pub output_index: i32,
    pub command_schema: String,
    pub command_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorSubmissionLease {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub output_id: Uuid,
    pub job_id: Uuid,
    pub tenant_id: String,
    pub provider_id: String,
    pub model: String,
    pub work_item_id: Uuid,
    pub output_index: i32,
    pub command_schema: String,
    pub command_hash: String,
    pub executor_owner: String,
    pub executor_lease_epoch: i64,
    pub executor_lease_expires_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorClaimScope {
    pub provider_id: String,
    pub command_schema: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorResultManifest {
    pub manifest_id: Uuid,
    pub storage_backend: String,
    pub object_key: String,
    pub sha256_hex: String,
    pub byte_size: u64,
    pub media_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutorSubmissionOutcome {
    Succeeded(ExecutorResultManifest),
    Failed { error_code: String },
    Uncertain { error_code: String },
}

impl ExecutorSubmissionOutcome {
    fn state(&self) -> &'static str {
        match self {
            Self::Succeeded(_) => "succeeded",
            Self::Failed { .. } => "failed",
            Self::Uncertain { .. } => "uncertain",
        }
    }

    fn error_code(&self) -> Option<&str> {
        match self {
            Self::Succeeded(_) => None,
            Self::Failed { error_code } | Self::Uncertain { error_code } => Some(error_code),
        }
    }

    fn manifest(&self) -> Option<&ExecutorResultManifest> {
        match self {
            Self::Succeeded(manifest) => Some(manifest),
            Self::Failed { .. } | Self::Uncertain { .. } => None,
        }
    }
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExecutorSubmissionError {
    #[error("executor submission storage is unavailable")]
    Unavailable,
    #[error("executor submission parameters conflict with durable identity")]
    Conflict,
    #[error("executor submission input is invalid")]
    InvalidInput,
    #[error("executor submission lease is stale or invalid")]
    StaleLease,
}

#[async_trait]
pub trait ExecutorSubmissionStore: Send + Sync + 'static {
    async fn prepare_for_lease(
        &self,
        lease: &WorkLease,
    ) -> Result<Vec<PreparedExecutorSubmission>, ExecutorSubmissionError>;

    async fn claim_prepared(
        &self,
        scope: &ExecutorClaimScope,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ExecutorSubmissionLease>, ExecutorSubmissionError>;

    async fn start(&self, lease: &ExecutorSubmissionLease) -> Result<(), ExecutorSubmissionError>;

    async fn heartbeat(
        &self,
        lease: &ExecutorSubmissionLease,
        lease_ms: i64,
    ) -> Result<ExecutorSubmissionLease, ExecutorSubmissionError>;

    async fn record_outcome(
        &self,
        lease: &ExecutorSubmissionLease,
        outcome: &ExecutorSubmissionOutcome,
    ) -> Result<(), ExecutorSubmissionError>;

    async fn reconcile_expired(&self, limit: u32) -> Result<u64, ExecutorSubmissionError>;
}
