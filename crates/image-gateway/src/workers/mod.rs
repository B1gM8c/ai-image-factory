use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tracing::{Instrument, info_span};

use crate::{
    ImageGatewayError,
    admission::{AdmissionError, AdmissionStore, WorkLease, WorkOutcome},
    artifacts::{
        ArtifactBlobStore, ArtifactIdentity, ArtifactWriteError, GenerationResponseProjection,
        GenerationResultManifest, media_type_for_output_format,
    },
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
    artifact_store: Arc<dyn ArtifactBlobStore>,
    request_timeout: Duration,
    lease_duration: Duration,
}

pub(crate) struct GenerationExecution {
    pub(crate) images: Vec<GeneratedImage>,
    pub(crate) projection: GenerationResponseProjection,
    pub(crate) usage: UsageSnapshot,
}

impl GenerationWorker {
    pub(crate) fn new(
        generator: Arc<dyn ImageGenerator>,
        admission: Arc<dyn AdmissionStore>,
        settlement: Arc<dyn ExecutionSettlementStore>,
        usage: Arc<dyn UsageStore>,
        artifact_store: Arc<dyn ArtifactBlobStore>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            generator,
            admission,
            settlement,
            usage,
            artifact_store,
            request_timeout,
            lease_duration: request_timeout.saturating_add(INLINE_LEASE_GRACE),
        }
    }

    pub(crate) async fn execute(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        job: GenerationJob,
        api_profile: &str,
        response_schema: &str,
    ) -> Result<GenerationExecution, ImageGatewayError> {
        let (stop_heartbeat, heartbeat) = self.start_heartbeat(lease.clone());
        let result = timeout(self.request_timeout, self.generator.generate(job.clone()))
            .instrument(info_span!(
                "worker.generate",
                image.units = reservation.charge.units
            ))
            .await
            .map_err(|_| ImageGatewayError::timeout())
            .and_then(|result| result)
            .and_then(|images| {
                normalize_generated_images(
                    images,
                    &job.size,
                    &job.output_format,
                    job.output_compression,
                )
            });

        let images = match result {
            Ok(images) => images,
            Err(error) => {
                stop_worker_heartbeat(stop_heartbeat, heartbeat).await;
                return self.fail(lease, reservation, error).await;
            }
        };
        let manifest = match self
            .stage_result(
                lease,
                reservation,
                &job,
                api_profile,
                response_schema,
                &images,
            )
            .await
        {
            Ok(manifest) => manifest,
            Err(error) => {
                stop_worker_heartbeat(stop_heartbeat, heartbeat).await;
                return self.mark_uncertain(lease, error).await;
            }
        };
        let usage = self
            .settlement
            .succeed(lease, reservation, &manifest)
            .await
            .map_err(|_| {
                ImageGatewayError::service_unavailable("generation settlement unavailable")
            });
        stop_worker_heartbeat(stop_heartbeat, heartbeat).await;
        let usage = usage?;
        Ok(GenerationExecution {
            images,
            projection: manifest.projection,
            usage,
        })
    }

    async fn stage_result(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        job: &GenerationJob,
        api_profile: &str,
        response_schema: &str,
        images: &[GeneratedImage],
    ) -> Result<GenerationResultManifest, ImageGatewayError> {
        let media_type = media_type_for_output_format(&job.output_format)
            .ok_or_else(|| ImageGatewayError::internal("unsupported artifact output format"))?;
        let size = response_size(images)?;
        let mut artifacts = Vec::with_capacity(images.len());
        for (output_index, image) in images.iter().enumerate() {
            let identity = ArtifactIdentity {
                artifact_id: uuid::Uuid::new_v4(),
                tenant_id: reservation.charge.tenant_id.clone(),
                job_id: lease.job_id,
                work_item_id: lease.work_item_id,
                execution_id: lease.execution_id,
                lease_epoch: lease.lease_epoch,
                output_index: output_index as u32,
                media_type: media_type.to_string(),
            };
            artifacts.push(
                self.artifact_store
                    .put(identity, &image.bytes)
                    .await
                    .map_err(map_artifact_write_error)?,
            );
        }
        Ok(GenerationResultManifest {
            job_id: lease.job_id,
            tenant_id: reservation.charge.tenant_id.clone(),
            projection: GenerationResponseProjection {
                api_profile: api_profile.to_string(),
                response_schema: response_schema.to_string(),
                created_at_seconds: unix_seconds(),
                output_format: job.output_format.clone(),
                quality: job.quality.clone(),
                size,
                background: job.background.clone(),
                stream: job.stream,
                usage: reservation.snapshot.clone(),
            },
            artifacts,
        })
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

    async fn mark_uncertain(
        &self,
        lease: &WorkLease,
        error: ImageGatewayError,
    ) -> Result<GenerationExecution, ImageGatewayError> {
        self.admission
            .settle(
                lease,
                WorkOutcome::Uncertain,
                Some("artifact_persist_failed"),
            )
            .await
            .map_err(map_admission_error)?;
        Err(error)
    }
}

async fn stop_worker_heartbeat(stop: oneshot::Sender<()>, heartbeat: JoinHandle<()>) {
    let _ = stop.send(());
    let _ = heartbeat.await;
}

fn response_size(images: &[GeneratedImage]) -> Result<String, ImageGatewayError> {
    let image = images
        .first()
        .ok_or_else(|| ImageGatewayError::backend("Codex CLI returned no images"))?;
    let decoded = image::load_from_memory(&image.bytes)
        .map_err(|_| ImageGatewayError::backend("Codex CLI produced an unreadable image"))?;
    Ok(format!("{}x{}", decoded.width(), decoded.height()))
}

fn unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn map_artifact_write_error(_: ArtifactWriteError) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("artifact storage unavailable")
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
