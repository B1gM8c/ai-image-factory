use std::{sync::Arc, time::Duration};

use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tracing::{Instrument, info_span};

use crate::{
    ImageGatewayError,
    admission::{AdmissionError, AdmissionStore, WorkLease, WorkOutcome},
    generator::{GeneratedImage, GenerationJob, ImageGenerator, normalize_generated_images},
    settlement::ExecutionSettlementStore,
    usage::{UsageReservation, UsageSnapshot, UsageStore},
};

const INLINE_LEASE_GRACE: Duration = Duration::from_secs(60);

pub(crate) struct GenerationWorker {
    generator: Arc<dyn ImageGenerator>,
    admission: Arc<dyn AdmissionStore>,
    settlement: Arc<dyn ExecutionSettlementStore>,
    usage: Arc<dyn UsageStore>,
    request_timeout: Duration,
    lease_duration: Duration,
}

pub(crate) struct GenerationExecution {
    pub(crate) images: Vec<GeneratedImage>,
    pub(crate) usage: UsageSnapshot,
}

impl GenerationWorker {
    pub(crate) fn new(
        generator: Arc<dyn ImageGenerator>,
        admission: Arc<dyn AdmissionStore>,
        settlement: Arc<dyn ExecutionSettlementStore>,
        usage: Arc<dyn UsageStore>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            generator,
            admission,
            settlement,
            usage,
            request_timeout,
            lease_duration: request_timeout.saturating_add(INLINE_LEASE_GRACE),
        }
    }

    pub(crate) async fn execute(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        job: GenerationJob,
        size: &str,
        output_format: &str,
        output_compression: Option<u8>,
    ) -> Result<GenerationExecution, ImageGatewayError> {
        let (stop_heartbeat, heartbeat) = self.start_heartbeat(lease.clone());
        let result = timeout(self.request_timeout, self.generator.generate(job))
            .instrument(info_span!(
                "worker.generate",
                image.units = reservation.charge.units
            ))
            .await
            .map_err(|_| ImageGatewayError::timeout())
            .and_then(|result| result)
            .and_then(|images| {
                normalize_generated_images(images, size, output_format, output_compression)
            });
        let _ = stop_heartbeat.send(());
        let _ = heartbeat.await;

        let images = match result {
            Ok(images) => images,
            Err(error) => return self.fail(lease, reservation, error).await,
        };
        let usage = self
            .settlement
            .succeed(lease, reservation)
            .await
            .map_err(|_| {
                ImageGatewayError::service_unavailable("generation settlement unavailable")
            })?;
        Ok(GenerationExecution { images, usage })
    }

    fn start_heartbeat(&self, lease: WorkLease) -> (oneshot::Sender<()>, JoinHandle<()>) {
        let (stop, mut stop_rx) = oneshot::channel();
        let admission = self.admission.clone();
        let interval = (self.lease_duration / 3).max(Duration::from_millis(1));
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sleep(interval) => {
                        if admission.heartbeat(&lease, duration_ms(interval * 3)).await.is_err() {
                            break;
                        }
                    }
                    _ = &mut stop_rx => break,
                }
            }
        });
        (stop, handle)
    }

    async fn fail(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        error: ImageGatewayError,
    ) -> Result<GenerationExecution, ImageGatewayError> {
        self.admission
            .settle(lease, WorkOutcome::Failed, Some("generation_failed"))
            .await
            .map_err(map_admission_error)?;
        self.usage.release(reservation, "generation_failed").await?;
        Err(error)
    }
}

fn duration_ms(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}

fn map_admission_error(error: AdmissionError) -> ImageGatewayError {
    match error {
        AdmissionError::Expired => ImageGatewayError::timeout(),
        AdmissionError::Unavailable
        | AdmissionError::InvalidOwner
        | AdmissionError::StaleLease
        | AdmissionError::InvalidCommand => {
            ImageGatewayError::service_unavailable("durable admission is unavailable")
        }
    }
}
