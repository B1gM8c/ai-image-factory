use async_trait::async_trait;

use crate::{admission::WorkLease, generator::GenerationJob, usage::UsageReservation};

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
}

#[async_trait]
pub trait ExecutionContextStore: Send + Sync + 'static {
    async fn load_generation(
        &self,
        lease: &WorkLease,
    ) -> Result<GenerationExecutionContext, ExecutionContextError>;
}
