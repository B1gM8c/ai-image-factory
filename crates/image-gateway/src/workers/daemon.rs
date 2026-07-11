use std::{sync::Arc, time::Duration};

use uuid::Uuid;

use super::{GenerationWorker, INLINE_LEASE_GRACE, duration_ms};
use crate::{
    ImageGatewayError,
    admission::{AdmissionError, AdmissionStore},
    artifacts::ArtifactBlobStore,
    execution::{ExecutionContextError, ExecutionContextStore},
    generator::ImageGenerator,
    settlement::ExecutionSettlementStore,
};

pub struct Workerd {
    worker_id: String,
    admission: Arc<dyn AdmissionStore>,
    contexts: Arc<dyn ExecutionContextStore>,
    generation: GenerationWorker,
    lease_duration: Duration,
}

impl Workerd {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        worker_id: String,
        generator: Arc<dyn ImageGenerator>,
        admission: Arc<dyn AdmissionStore>,
        contexts: Arc<dyn ExecutionContextStore>,
        settlement: Arc<dyn ExecutionSettlementStore>,
        artifacts: Arc<dyn ArtifactBlobStore>,
        request_timeout: Duration,
    ) -> Self {
        let generation = GenerationWorker::new(
            generator,
            admission.clone(),
            settlement,
            artifacts,
            request_timeout,
        );
        Self {
            worker_id,
            admission,
            contexts,
            generation,
            lease_duration: request_timeout.saturating_add(INLINE_LEASE_GRACE),
        }
    }

    pub async fn run_once(&self) -> Result<Option<Uuid>, ImageGatewayError> {
        let Some(lease) = self
            .admission
            .claim_ready(&self.worker_id, duration_ms(self.lease_duration))
            .await
            .map_err(map_admission_error)?
        else {
            return Ok(None);
        };
        let context = match self.contexts.load_generation(&lease).await {
            Ok(context) => context,
            Err(ExecutionContextError::Invalid { reservation }) => {
                self.generation
                    .reject_invalid_context(&lease, &reservation)
                    .await?;
                return Err(ImageGatewayError::internal(
                    "durable execution context failed integrity validation",
                ));
            }
            Err(ExecutionContextError::Unavailable) => {
                return Err(ImageGatewayError::service_unavailable(
                    "execution context unavailable",
                ));
            }
        };
        self.admission
            .start(&lease)
            .await
            .map_err(map_admission_error)?;
        self.generation
            .execute(
                &lease,
                &context.reservation,
                context.job,
                &context.api_profile,
                &context.response_schema,
            )
            .await?;
        Ok(Some(lease.job_id))
    }
}

fn map_admission_error(error: AdmissionError) -> ImageGatewayError {
    match error {
        AdmissionError::Expired => ImageGatewayError::timeout(),
        AdmissionError::Unavailable => {
            ImageGatewayError::service_unavailable("durable work claim unavailable")
        }
        AdmissionError::InvalidOwner
        | AdmissionError::StaleLease
        | AdmissionError::InvalidCommand => {
            ImageGatewayError::internal("durable work lease is stale or invalid")
        }
    }
}
