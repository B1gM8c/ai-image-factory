mod postgres;

use std::{str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::ImageGatewayError;

pub use postgres::PostgresBatchService;

pub const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_BATCH_FILE_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_BATCH_REQUESTS: usize = 1_000;
pub const MAX_BATCH_STORED_RESULT_BYTES: u64 = 128 * 1024 * 1024;
pub const MAX_BATCH_RESULT_FILE_BYTES: usize = 144 * 1024 * 1024;
pub const DEFAULT_PROJECT_FILE_STORAGE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_PROJECT_FILE_STORAGE_COUNT: u32 = 1_000;
pub const DEFAULT_BATCH_RETENTION_SECONDS: u32 = 30 * 24 * 60 * 60;
pub const MIN_FILE_RETENTION_SECONDS: u32 = 60 * 60;
pub const MAX_FILE_RETENTION_SECONDS: u32 = 30 * 24 * 60 * 60;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectScope {
    pub tenant_id: String,
    pub project_id: String,
}

impl ProjectScope {
    pub fn new(tenant_id: impl Into<String>, project_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            project_id: project_id.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectFilePurpose {
    Assistants,
    Batch,
    BatchOutput,
    #[serde(rename = "fine-tune")]
    FineTune,
    Vision,
    UserData,
    Evals,
}

impl ProjectFilePurpose {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Assistants => "assistants",
            Self::Batch => "batch",
            Self::BatchOutput => "batch_output",
            Self::FineTune => "fine-tune",
            Self::Vision => "vision",
            Self::UserData => "user_data",
            Self::Evals => "evals",
        }
    }
}

impl FromStr for ProjectFilePurpose {
    type Err = ImageGatewayError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "assistants" => Ok(Self::Assistants),
            "batch" => Ok(Self::Batch),
            "batch_output" => Ok(Self::BatchOutput),
            "fine-tune" => Ok(Self::FineTune),
            "vision" => Ok(Self::Vision),
            "user_data" => Ok(Self::UserData),
            "evals" => Ok(Self::Evals),
            _ => Err(ImageGatewayError::invalid_request(
                "purpose is not supported",
                Some("purpose".to_string()),
                "invalid_file_purpose",
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchFileBlob {
    pub storage_backend: String,
    pub object_key: String,
    pub sha256_hex: String,
    pub byte_size: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchFileBlobError {
    Unavailable,
    Integrity,
}

#[async_trait]
pub trait BatchFileBlobStore: Send + Sync + 'static {
    async fn put(&self, file_uuid: Uuid, bytes: &[u8])
    -> Result<BatchFileBlob, BatchFileBlobError>;

    async fn get(
        &self,
        file_uuid: Uuid,
        blob: &BatchFileBlob,
    ) -> Result<Vec<u8>, BatchFileBlobError>;

    async fn delete(&self, file_uuid: Uuid, blob: &BatchFileBlob)
    -> Result<(), BatchFileBlobError>;
}

pub(crate) fn batch_file_object_key(file_uuid: Uuid) -> String {
    let id = file_uuid.simple().to_string();
    format!("batch-files/{}/{}", &id[..2], id)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProjectFile {
    pub id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub purpose: ProjectFilePurpose,
    pub filename: String,
    pub bytes: u64,
    pub sha256_hex: String,
    pub created_at_ms: i64,
    pub expires_at_ms: Option<i64>,
    pub deleted_at_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct CreateProjectFile<'a> {
    pub filename: &'a str,
    pub purpose: ProjectFilePurpose,
    pub bytes: &'a [u8],
    pub expires_after: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectFilePage {
    pub data: Vec<ProjectFile>,
    pub has_more: bool,
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectFileCleanupLease {
    pub scope: ProjectScope,
    pub file_id: String,
    pub blob: BatchFileBlob,
    pub lease_owner: String,
    pub lease_epoch: i64,
    pub lease_expires_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchStatus {
    Validating,
    Failed,
    InProgress,
    Finalizing,
    Completed,
    Expired,
    Cancelling,
    Cancelled,
}

impl BatchStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Validating => "validating",
            Self::Failed => "failed",
            Self::InProgress => "in_progress",
            Self::Finalizing => "finalizing",
            Self::Completed => "completed",
            Self::Expired => "expired",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Failed | Self::Completed | Self::Expired | Self::Cancelled
        )
    }
}

impl FromStr for BatchStatus {
    type Err = ImageGatewayError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "validating" => Ok(Self::Validating),
            "failed" => Ok(Self::Failed),
            "in_progress" => Ok(Self::InProgress),
            "finalizing" => Ok(Self::Finalizing),
            "completed" => Ok(Self::Completed),
            "expired" => Ok(Self::Expired),
            "cancelling" => Ok(Self::Cancelling),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(ImageGatewayError::internal(
                "stored batch status is invalid",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchRequestState {
    Pending,
    Leased,
    Completed,
    Failed,
    Cancelled,
}

impl FromStr for BatchRequestState {
    type Err = ImageGatewayError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(ImageGatewayError::internal(
                "stored batch request state is invalid",
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BatchRequestCounts {
    pub total: u32,
    pub completed: u32,
    pub failed: u32,
    pub cancelled: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ProjectBatch {
    pub id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub input_file_id: String,
    pub endpoint: String,
    pub model: String,
    pub completion_window: String,
    pub status: BatchStatus,
    pub metadata: Value,
    pub errors: Option<Value>,
    pub request_counts: BatchRequestCounts,
    pub output_file_id: Option<String>,
    pub error_file_id: Option<String>,
    pub created_at_ms: i64,
    pub in_progress_at_ms: Option<i64>,
    pub finalizing_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub failed_at_ms: Option<i64>,
    pub expires_at_ms: i64,
    pub cancel_requested_at_ms: Option<i64>,
    pub cancelled_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedBatchLine {
    pub ordinal: u32,
    pub custom_id: String,
    pub method: String,
    pub url: String,
    pub model: String,
    pub body: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateProjectBatch {
    pub input_file_id: String,
    pub endpoint: String,
    pub completion_window: String,
    pub metadata: Value,
    pub safe_auth_snapshot: Value,
    pub route_snapshot: Value,
    pub output_retention: Duration,
    pub lines: Vec<ValidatedBatchLine>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectBatchPage {
    pub data: Vec<ProjectBatch>,
    pub has_more: bool,
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchWorkTarget {
    pub scope: ProjectScope,
    pub batch_id: String,
    pub status: BatchStatus,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchExecutionSnapshot {
    pub scope: ProjectScope,
    pub batch_id: String,
    pub safe_auth_snapshot: Value,
    pub route_snapshot: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchRequestLease {
    pub scope: ProjectScope,
    pub batch_id: String,
    pub request_id: Uuid,
    pub ordinal: u32,
    pub custom_id: String,
    pub method: String,
    pub url: String,
    pub model: String,
    pub body: Value,
    pub request_hash: String,
    pub attempt_count: u32,
    pub lease_owner: String,
    pub lease_epoch: i64,
    pub lease_expires_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchFinalizationLease {
    pub scope: ProjectScope,
    pub batch_id: String,
    pub lease_owner: String,
    pub lease_epoch: i64,
    pub lease_expires_at_ms: i64,
    pub cancelling: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchRequestSuccess {
    pub status_code: u16,
    pub request_id: Option<String>,
    pub body: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchResultRole {
    Output,
    Error,
}

impl BatchResultRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Output => "output",
            Self::Error => "error",
        }
    }
}

#[async_trait]
pub trait BatchService: Send + Sync + 'static {
    async fn create_file(
        &self,
        scope: &ProjectScope,
        request: CreateProjectFile<'_>,
    ) -> Result<ProjectFile, ImageGatewayError>;

    async fn get_file(
        &self,
        scope: &ProjectScope,
        file_id: &str,
    ) -> Result<ProjectFile, ImageGatewayError>;

    async fn read_file(
        &self,
        scope: &ProjectScope,
        file_id: &str,
    ) -> Result<Vec<u8>, ImageGatewayError>;

    async fn list_files(
        &self,
        scope: &ProjectScope,
        purpose: Option<ProjectFilePurpose>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectFilePage, ImageGatewayError>;

    async fn delete_file(
        &self,
        scope: &ProjectScope,
        file_id: &str,
    ) -> Result<ProjectFile, ImageGatewayError>;

    async fn claim_file_cleanup(
        &self,
        worker_id: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<ProjectFileCleanupLease>, ImageGatewayError>;

    async fn delete_file_blob(
        &self,
        lease: &ProjectFileCleanupLease,
    ) -> Result<(), ImageGatewayError>;

    async fn complete_file_cleanup(
        &self,
        lease: &ProjectFileCleanupLease,
    ) -> Result<(), ImageGatewayError>;

    async fn release_file_cleanup(
        &self,
        lease: &ProjectFileCleanupLease,
    ) -> Result<(), ImageGatewayError>;

    async fn create_batch(
        &self,
        scope: &ProjectScope,
        request: CreateProjectBatch,
    ) -> Result<ProjectBatch, ImageGatewayError>;

    async fn mark_batch_validated(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<ProjectBatch, ImageGatewayError>;

    async fn fail_batch_validation(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
        errors: Value,
    ) -> Result<ProjectBatch, ImageGatewayError>;

    async fn get_batch(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<ProjectBatch, ImageGatewayError>;

    async fn list_batches(
        &self,
        scope: &ProjectScope,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectBatchPage, ImageGatewayError>;

    async fn list_runnable_batches(
        &self,
        limit: usize,
    ) -> Result<Vec<BatchWorkTarget>, ImageGatewayError>;

    async fn load_execution_snapshot(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<BatchExecutionSnapshot, ImageGatewayError>;

    async fn claim_requests(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
        worker_id: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<BatchRequestLease>, ImageGatewayError>;

    async fn complete_request(
        &self,
        lease: &BatchRequestLease,
        result: BatchRequestSuccess,
    ) -> Result<(), ImageGatewayError>;

    async fn fail_request(
        &self,
        lease: &BatchRequestLease,
        error: Value,
    ) -> Result<(), ImageGatewayError>;

    async fn retry_request(
        &self,
        lease: &BatchRequestLease,
        error: Value,
        delay: Duration,
    ) -> Result<(), ImageGatewayError>;

    async fn cancel_batch(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<ProjectBatch, ImageGatewayError>;

    async fn expire_batch(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<ProjectBatch, ImageGatewayError>;

    async fn claim_finalization(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<Option<BatchFinalizationLease>, ImageGatewayError>;

    async fn generate_result_jsonl(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
        role: BatchResultRole,
    ) -> Result<Vec<u8>, ImageGatewayError>;

    async fn materialize_result_files(
        &self,
        lease: &BatchFinalizationLease,
    ) -> Result<(Option<ProjectFile>, Option<ProjectFile>), ImageGatewayError>;

    async fn finalize_batch(
        &self,
        lease: &BatchFinalizationLease,
    ) -> Result<ProjectBatch, ImageGatewayError>;
}

pub fn postgres_batch_service(
    pool: sqlx::PgPool,
    blobs: Arc<dyn BatchFileBlobStore>,
) -> PostgresBatchService {
    PostgresBatchService::new(pool, blobs)
}
