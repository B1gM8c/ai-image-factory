use async_trait::async_trait;

use crate::{
    admission::{EditCommandV1, EditInputRoleV1, WorkLease},
    generator::GenerationJob,
    input_blobs::InputBlobRef,
    usage::UsageReservation,
};

mod postgres;

pub use postgres::PostgresExecutionContextStore;

#[derive(Debug, thiserror::Error)]
pub enum ExecutionContextError {
    #[error("execution context storage is unavailable")]
    Unavailable,
    #[error("execution context failed integrity validation")]
    Invalid { reservation: UsageReservation },
}

#[derive(Clone, Debug)]
pub struct GenerationExecutionContext {
    pub job: GenerationJob,
    pub reservation: UsageReservation,
    pub api_profile: String,
    pub response_schema: String,
    pub economics_contract_version: i16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedEditInput {
    pub blob: InputBlobRef,
    pub role: EditInputRoleV1,
    pub index: u16,
    pub media_type: String,
}

#[derive(Clone, Debug)]
pub struct EditExecutionContext {
    pub command: EditCommandV1,
    pub inputs: Vec<PersistedEditInput>,
    pub reservation: UsageReservation,
    pub response_schema: String,
    pub economics_contract_version: i16,
}

#[async_trait]
pub trait ExecutionContextStore: Send + Sync + 'static {
    async fn load_generation(
        &self,
        lease: &WorkLease,
    ) -> Result<GenerationExecutionContext, ExecutionContextError>;

    async fn load_edit(
        &self,
        lease: &WorkLease,
    ) -> Result<EditExecutionContext, ExecutionContextError>;
}
