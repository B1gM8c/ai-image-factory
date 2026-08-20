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
    generator::{
        EditJob, GeneratedImage, GenerationJob, ImageGenerator, normalize_generated_images,
    },
    settlement::{ExecutionSettlementStore, GenerationResultStatus},
    usage::{UsageReservation, UsageSnapshot},
};

const INLINE_LEASE_GRACE: Duration = Duration::from_secs(60);
const SETTLEMENT_ATTEMPTS: usize = 3;
const SETTLEMENT_RETRY_DELAY: Duration = Duration::from_millis(50);

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

#[derive(Clone, Copy)]
struct ImageExecutionSpec<'a> {
    operation: &'a str,
    output_format: &'a str,
    output_compression: Option<u8>,
    quality: &'a str,
    background: &'a str,
    stream: bool,
    failure_code: &'static str,
}

struct ImageResultStage<'a> {
    lease: &'a WorkLease,
    reservation: &'a UsageReservation,
    spec: ImageExecutionSpec<'a>,
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
        let provider = self.generator.generate(job.clone()).instrument(info_span!(
            "worker.generate",
            image.units = reservation.charge.output_count
        ));
        self.execute_provider(
            lease,
            reservation,
            ImageExecutionSpec {
                operation: crate::admission::GENERATION_OPERATION,
                output_format: &job.output_format,
                output_compression: job.output_compression,
                quality: &job.quality,
                background: &job.background,
                stream: job.stream,
                failure_code: "image_generation_failed",
            },
            api_profile,
            response_schema,
            provider,
        )
        .await
    }

    pub(crate) async fn execute_edit(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        job: EditJob,
        api_profile: &str,
        response_schema: &str,
    ) -> Result<GenerationExecution, ImageGatewayError> {
        let provider = self.generator.edit(job.clone()).instrument(info_span!(
            "worker.edit",
            image.units = reservation.charge.output_count
        ));
        self.execute_provider(
            lease,
            reservation,
            ImageExecutionSpec {
                operation: crate::admission::EDIT_OPERATION,
                output_format: &job.output_format,
                output_compression: job.output_compression,
                quality: &job.quality,
                background: &job.background,
                stream: job.stream,
                failure_code: "image_edit_failed",
            },
            api_profile,
            response_schema,
            provider,
        )
        .await
    }

    async fn execute_provider(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        spec: ImageExecutionSpec<'_>,
        api_profile: &str,
        response_schema: &str,
        provider: impl Future<Output = Result<Vec<GeneratedImage>, ImageGatewayError>>,
    ) -> Result<GenerationExecution, ImageGatewayError> {
        let mut heartbeat = self.start_heartbeat(lease.clone());
        let result = match run_until_lease_lost(
            &mut heartbeat.lost,
            timeout(self.request_timeout, provider),
        )
        .await
        {
            Ok(result) => result
                .map_err(|_| ImageGatewayError::timeout())
                .and_then(|result| result)
                .and_then(|images| {
                    normalize_generated_images(images, spec.output_format, spec.output_compression)
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
                return self
                    .fail(lease, reservation, error, spec.failure_code)
                    .await;
            }
        };
        let manifest = match self
            .stage_result(
                ImageResultStage {
                    lease,
                    reservation,
                    spec,
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
        let usage = self.settle_success(lease, reservation, &manifest).await;
        heartbeat.stop().await;
        let usage = usage?;
        Ok(GenerationExecution {
            images,
            projection: manifest.projection,
            usage,
        })
    }

    async fn settle_success(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        manifest: &GenerationResultManifest,
    ) -> Result<UsageSnapshot, ImageGatewayError> {
        for attempt in 0..SETTLEMENT_ATTEMPTS {
            match self.settlement.succeed(lease, reservation, manifest).await {
                Ok(usage) => return Ok(usage),
                Err(_) if attempt + 1 < SETTLEMENT_ATTEMPTS => {
                    sleep(SETTLEMENT_RETRY_DELAY).await;
                }
                Err(_) => break,
            }
        }
        match self.settlement.generation_status(lease.job_id).await {
            Ok(GenerationResultStatus::Succeeded(result))
                if result.projection == manifest.projection =>
            {
                Ok(reservation.snapshot.clone())
            }
            _ => Err(ImageGatewayError::service_unavailable(
                "image settlement unavailable",
            )),
        }
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

    pub(crate) async fn reject_before_provider(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        error_code: &'static str,
    ) -> Result<(), ImageGatewayError> {
        self.settlement.fail(lease, reservation, error_code).await
    }

    async fn stage_result(
        &self,
        stage: ImageResultStage<'_>,
        heartbeat: &mut WorkerHeartbeat,
    ) -> Result<GenerationResultManifest, StageResultError> {
        let media_type =
            media_type_for_output_format(stage.spec.output_format).ok_or_else(|| {
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
                operation: stage.spec.operation.to_string(),
                response_schema: stage.response_schema.to_string(),
                created_at_seconds: unix_seconds(),
                output_format: stage.spec.output_format.to_string(),
                quality: stage.spec.quality.to_string(),
                size,
                background: stage.spec.background.to_string(),
                stream: stage.spec.stream,
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
        fallback_code: &'static str,
    ) -> Result<GenerationExecution, ImageGatewayError> {
        self.settlement
            .fail(
                lease,
                reservation,
                error.error_code().unwrap_or(fallback_code),
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
        AdmissionError::BillingLimitExceeded => ImageGatewayError::billing_limit_exceeded(),
        AdmissionError::ProjectBudgetExceeded => ImageGatewayError::project_budget_exceeded(),
        AdmissionError::Unavailable
        | AdmissionError::PricingUnavailable
        | AdmissionError::InvalidOwner
        | AdmissionError::StaleLease
        | AdmissionError::InvalidCommand => {
            ImageGatewayError::service_unavailable("durable admission is unavailable")
        }
    }
}
