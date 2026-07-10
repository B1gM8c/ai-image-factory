use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    ImageGatewayError,
    admission::{AdmissionError, AdmissionStore, WorkLease, WorkOutcome},
    usage::{UsageReservation, UsageSnapshot, UsageStore},
};

mod postgres;

pub use postgres::PostgresExecutionSettlementStore;

#[async_trait]
pub trait ExecutionSettlementStore: Send + Sync + 'static {
    async fn succeed(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
    ) -> Result<UsageSnapshot, ImageGatewayError>;
}

pub(crate) struct SequentialExecutionSettlementStore {
    admission_store: Arc<dyn AdmissionStore>,
    usage_store: Arc<dyn UsageStore>,
}

impl SequentialExecutionSettlementStore {
    pub(crate) fn new(
        admission_store: Arc<dyn AdmissionStore>,
        usage_store: Arc<dyn UsageStore>,
    ) -> Self {
        Self {
            admission_store,
            usage_store,
        }
    }
}

#[async_trait]
impl ExecutionSettlementStore for SequentialExecutionSettlementStore {
    async fn succeed(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
    ) -> Result<UsageSnapshot, ImageGatewayError> {
        if lease.job_id != reservation.job_id {
            return Err(ImageGatewayError::internal(
                "work lease and quota reservation belong to different jobs",
            ));
        }
        self.admission_store
            .settle(lease, WorkOutcome::Succeeded, None)
            .await
            .map_err(map_admission_error)?;
        self.usage_store.commit(reservation).await
    }
}

fn map_admission_error(error: AdmissionError) -> ImageGatewayError {
    match error {
        AdmissionError::Expired => ImageGatewayError::timeout(),
        AdmissionError::Unavailable => {
            ImageGatewayError::service_unavailable("admission settlement unavailable")
        }
        AdmissionError::InvalidOwner
        | AdmissionError::StaleLease
        | AdmissionError::InvalidCommand => {
            ImageGatewayError::internal("execution lease is stale or invalid")
        }
    }
}
