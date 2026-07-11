use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tracing::{Instrument, info_span};

mod daemon;
#[cfg(test)]
mod tests;

pub use daemon::Workerd;

use crate::{
    ImageGatewayError,
    admission::{AdmissionError, AdmissionStore, WorkLease, WorkOutcome},
    artifacts::{
        ArtifactBlobStore, ArtifactIdentity, ArtifactWriteError, GenerationResponseProjection,
        GenerationResultManifest, media_type_for_output_format,
    },
    generator::{GeneratedImage, GenerationJob, ImageGenerator, normalize_generated_images},
    settlement::ExecutionSettlementStore,
    usage::{UsageReservation, UsageSnapshot},
};

const INLINE_LEASE_GRACE: Duration = Duration::from_secs(60);

pub(crate) struct GenerationWorker {
    generator: Arc<dyn ImageGenerator>,
    admission: Arc<dyn AdmissionStore>,
    settlement: Arc<dyn ExecutionSettlementStore>,
    artifact_store: Arc<dyn ArtifactBlobStore>,
    request_timeout: Duration,
    lease_duration: Duration,
}

pub(crate) struct GenerationExecution {
    pub(crate) images: Vec<GeneratedImage>,
    pub(crate) projection: GenerationResponseProjection,
    pub(crate) usage: UsageSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LeaseLost;

struct WorkerHeartbeat {
    stop: oneshot::Sender<()>,
    lost: oneshot::Receiver<()>,
    task: JoinHandle<()>,
}

enum StageResultError {
    LeaseLost,
    Persist(ImageGatewayError),
}

struct GenerationResultStage<'a> {
    lease: &'a WorkLease,
    reservation: &'a UsageReservation,
    job: &'a GenerationJob,
    api_profile: &'a str,
    response_schema: &'a str,
    images: &'a [GeneratedImage],
}

impl GenerationWorker {
    pub(crate) fn new(
        generator: Arc<dyn ImageGenerator>,
        admission: Arc<dyn AdmissionStore>,
        settlement: Arc<dyn ExecutionSettlementStore>,
        artifact_store: Arc<dyn ArtifactBlobStore>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            generator,
            admission,
            settlement,
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
        let mut heartbeat = self.start_heartbeat(lease.clone());
        let generation =
            timeout(self.request_timeout, self.generator.generate(job.clone())).instrument(
                info_span!("worker.generate", image.units = reservation.charge.units),
            );
        let result = match run_until_lease_lost(&mut heartbeat.lost, generation).await {
            Ok(result) => result
                .map_err(|_| ImageGatewayError::timeout())
                .and_then(|result| result)
                .and_then(|images| {
                    normalize_generated_images(
                        images,
                        &job.size,
                        &job.output_format,
                        job.output_compression,
                    )
                }),
            Err(LeaseLost) => {
                heartbeat.stop().await;
                return Err(lease_lost_error());
            }
        };

        let images = match result {
            Ok(images) => images,
            Err(error) => {
                heartbeat.stop().await;
                return self.fail(lease, reservation, error).await;
            }
        };
        let manifest = match self
            .stage_result(
                GenerationResultStage {
                    lease,
                    reservation,
                    job: &job,
                    api_profile,
                    response_schema,
                    images: &images,
                },
                &mut heartbeat,
            )
            .await
        {
            Ok(manifest) => manifest,
            Err(StageResultError::LeaseLost) => {
                heartbeat.stop().await;
                return Err(lease_lost_error());
            }
            Err(StageResultError::Persist(error)) => {
                heartbeat.stop().await;
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
        heartbeat.stop().await;
        let usage = usage?;
        Ok(GenerationExecution {
            images,
            projection: manifest.projection,
            usage,
        })
    }

    pub(crate) async fn reject_invalid_context(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
    ) -> Result<(), ImageGatewayError> {
        self.settlement
            .fail(lease, reservation, "invalid_execution_context")
            .await
    }

    async fn stage_result(
        &self,
        stage: GenerationResultStage<'_>,
        heartbeat: &mut WorkerHeartbeat,
    ) -> Result<GenerationResultManifest, StageResultError> {
        let media_type =
            media_type_for_output_format(&stage.job.output_format).ok_or_else(|| {
                StageResultError::Persist(ImageGatewayError::internal(
                    "unsupported artifact output format",
                ))
            })?;
        let size = response_size(stage.images).map_err(StageResultError::Persist)?;
        let mut artifacts = Vec::with_capacity(stage.images.len());
        for (output_index, image) in stage.images.iter().enumerate() {
            let identity = ArtifactIdentity {
                artifact_id: uuid::Uuid::new_v4(),
                tenant_id: stage.reservation.charge.tenant_id.clone(),
                job_id: stage.lease.job_id,
                work_item_id: stage.lease.work_item_id,
                execution_id: stage.lease.execution_id,
                lease_epoch: stage.lease.lease_epoch,
                output_index: output_index as u32,
                media_type: media_type.to_string(),
            };
            let put = self.artifact_store.put(identity, &image.bytes);
            tokio::pin!(put);
            let stored = tokio::select! {
                biased;
                _ = &mut heartbeat.lost => {
                    if let Ok(stored) = (&mut put).await {
                        let _ = self.artifact_store.delete(&stored).await;
                    }
                    self.delete_staged_artifacts(&artifacts).await;
                    return Err(StageResultError::LeaseLost);
                }
                result = &mut put => result,
            };
            let stored = match stored {
                Ok(stored) => stored,
                Err(error) => {
                    self.delete_staged_artifacts(&artifacts).await;
                    return Err(StageResultError::Persist(map_artifact_write_error(error)));
                }
            };
            artifacts.push(stored);
        }
        if !matches!(
            heartbeat.lost.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ) {
            self.delete_staged_artifacts(&artifacts).await;
            return Err(StageResultError::LeaseLost);
        }
        Ok(GenerationResultManifest {
            job_id: stage.lease.job_id,
            tenant_id: stage.reservation.charge.tenant_id.clone(),
            projection: GenerationResponseProjection {
                api_profile: stage.api_profile.to_string(),
                response_schema: stage.response_schema.to_string(),
                created_at_seconds: unix_seconds(),
                output_format: stage.job.output_format.clone(),
                quality: stage.job.quality.clone(),
                size,
                background: stage.job.background.clone(),
                stream: stage.job.stream,
                usage: stage.reservation.snapshot.clone(),
            },
            artifacts,
        })
    }

    async fn delete_staged_artifacts(&self, artifacts: &[crate::artifacts::ArtifactMetadata]) {
        for artifact in artifacts {
            let _ = self.artifact_store.delete(artifact).await;
        }
    }

    fn start_heartbeat(&self, lease: WorkLease) -> WorkerHeartbeat {
        let (stop, mut stop_rx) = oneshot::channel();
        let (lost_tx, lost) = oneshot::channel();
        let admission = self.admission.clone();
        let interval = (self.lease_duration / 3).max(Duration::from_millis(1));
        let task = tokio::spawn(async move {
            let mut lost_tx = Some(lost_tx);
            loop {
                tokio::select! {
                    _ = sleep(interval) => {
                        if admission.heartbeat(&lease, duration_ms(interval * 3)).await.is_err() {
                            if let Some(lost_tx) = lost_tx.take() {
                                let _ = lost_tx.send(());
                            }
                            break;
                        }
                    }
                    _ = &mut stop_rx => break,
                }
            }
        });
        WorkerHeartbeat { stop, lost, task }
    }

    async fn fail(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        error: ImageGatewayError,
    ) -> Result<GenerationExecution, ImageGatewayError> {
        self.settlement
            .fail(
                lease,
                reservation,
                error.error_code().unwrap_or("image_generation_failed"),
            )
            .await?;
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

impl WorkerHeartbeat {
    async fn stop(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}

async fn run_until_lease_lost<T>(
    lost: &mut oneshot::Receiver<()>,
    future: impl Future<Output = T>,
) -> Result<T, LeaseLost> {
    tokio::pin!(future);
    tokio::select! {
        biased;
        _ = lost => Err(LeaseLost),
        value = &mut future => Ok(value),
    }
}

fn lease_lost_error() -> ImageGatewayError {
    ImageGatewayError::service_unavailable("execution lease was lost")
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
