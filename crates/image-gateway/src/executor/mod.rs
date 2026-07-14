use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::admission::WorkLease;

mod daemon;
mod postgres;
mod runner;

pub use daemon::{ExecutorDaemon, ExecutorDaemonError, ExecutorDaemonRun};
pub use postgres::PostgresExecutorSubmissionStore;
pub use runner::{DurableRunner, RunnerError, RunnerOutcome};

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

#[derive(Clone, PartialEq)]
pub struct ExecutorLaunchContext {
    request_id: String,
    api_profile: String,
    output_index: i32,
    command_schema: String,
    command_hash: String,
    command_json: Value,
}

impl ExecutorLaunchContext {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn api_profile(&self) -> &str {
        &self.api_profile
    }

    pub fn output_index(&self) -> i32 {
        self.output_index
    }

    pub fn command_schema(&self) -> &str {
        &self.command_schema
    }

    pub fn command_hash(&self) -> &str {
        &self.command_hash
    }

    pub fn command_json(&self) -> &Value {
        &self.command_json
    }
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

const MAX_RESULT_BYTES: u64 = 256 * 1024 * 1024;

pub(crate) fn result_manifest_is_valid(manifest: &ExecutorResultManifest) -> bool {
    !manifest.storage_backend.is_empty()
        && manifest.storage_backend.len() <= 128
        && !manifest.object_key.is_empty()
        && manifest.object_key.len() <= 1_024
        && !manifest
            .object_key
            .bytes()
            .any(|byte| byte.is_ascii_control())
        && is_sha256(&manifest.sha256_hex)
        && (1..=MAX_RESULT_BYTES).contains(&manifest.byte_size)
        && matches!(
            manifest.media_type.as_str(),
            "image/png" | "image/jpeg" | "image/webp"
        )
}

pub(crate) fn error_code_is_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
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

    async fn resume_running(
        &self,
        scope: &ExecutorClaimScope,
        owner: &str,
    ) -> Result<Option<ExecutorSubmissionLease>, ExecutorSubmissionError>;

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

#[async_trait]
pub trait ExecutorLaunchContextStore: Send + Sync + 'static {
    async fn load_launch_context(
        &self,
        lease: &ExecutorSubmissionLease,
    ) -> Result<ExecutorLaunchContext, ExecutorSubmissionError>;
}
