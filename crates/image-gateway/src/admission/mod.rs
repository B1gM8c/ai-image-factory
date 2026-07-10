use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

pub mod command;
mod memory;
mod postgres;

pub use command::{
    GENERATION_COMMAND_SCHEMA_VERSION, GENERATION_OPERATION, GenerationCommandV1,
    IdempotencyKeyError, idempotency_key_digest, validate_idempotency_key,
};
pub use memory::InMemoryAdmissionStore;
pub use postgres::PostgresAdmissionStore;

#[derive(Clone, Debug)]
pub struct ClaimAdmission {
    pub tenant_id: String,
    pub project_id: String,
    pub api_profile: String,
    pub operation: String,
    pub request_id: String,
    pub idempotency_key_digest: Option<String>,
    pub request_hash: String,
    pub deadline_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionTicket {
    pub session_id: Uuid,
    pub owner_token: Uuid,
    pub request_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionClaim {
    Owner(AdmissionTicket),
    InProgress { session_id: Uuid },
    Existing { job_id: Uuid, state: String },
    Conflict { job_id: Option<Uuid> },
}

#[derive(Clone, Debug)]
pub struct AttachJob {
    pub ticket: AdmissionTicket,
    pub job_id: Uuid,
    pub command_schema: String,
    pub command_json: Value,
    pub work_kind: String,
    pub schedule_scope: String,
    pub schedule_weight: u32,
    pub schedule_priority: u8,
    pub schedule_cost: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachedWork {
    pub work_item_id: Uuid,
    pub job_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkLease {
    pub work_item_id: Uuid,
    pub job_id: Uuid,
    pub execution_id: Uuid,
    pub lease_epoch: i64,
    pub worker_id: String,
    pub command_schema: String,
    pub command_json: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkOutcome {
    Succeeded,
    Failed,
    Uncertain,
}

impl WorkOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Uncertain => "uncertain",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("durable admission storage is unavailable")]
    Unavailable,
    #[error("admission deadline has expired")]
    Expired,
    #[error("admission owner or state is invalid")]
    InvalidOwner,
    #[error("work lease is stale or invalid")]
    StaleLease,
    #[error("durable command payload must be a JSON object")]
    InvalidCommand,
}

#[async_trait]
pub trait AdmissionStore: Send + Sync + 'static {
    async fn claim(&self, request: ClaimAdmission) -> Result<AdmissionClaim, AdmissionError>;

    async fn attach(&self, request: AttachJob) -> Result<AttachedWork, AdmissionError>;

    async fn attach_and_start(
        &self,
        request: AttachJob,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<WorkLease, AdmissionError> {
        let attached = self.attach(request).await?;
        let lease = self
            .claim_job(attached.job_id, worker_id, lease_duration_ms)
            .await?
            .ok_or(AdmissionError::Unavailable)?;
        self.start(&lease).await?;
        Ok(lease)
    }

    async fn abort(&self, ticket: &AdmissionTicket) -> Result<(), AdmissionError>;

    async fn claim_ready(
        &self,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<Option<WorkLease>, AdmissionError>;

    async fn claim_job(
        &self,
        job_id: Uuid,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<Option<WorkLease>, AdmissionError>;

    async fn start(&self, lease: &WorkLease) -> Result<(), AdmissionError>;

    async fn heartbeat(
        &self,
        lease: &WorkLease,
        lease_duration_ms: i64,
    ) -> Result<(), AdmissionError>;

    async fn settle(
        &self,
        lease: &WorkLease,
        outcome: WorkOutcome,
        error_code: Option<&str>,
    ) -> Result<(), AdmissionError>;
}
