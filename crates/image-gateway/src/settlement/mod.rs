use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;

use crate::{
    ImageGatewayError,
    admission::{AdmissionError, AdmissionStore, WorkLease, WorkOutcome},
    artifacts::{
        ArtifactBlobStore, GenerationResultManifest, StoredGenerationResult,
        hydrate_generation_result,
    },
    usage::{UsageReservation, UsageSnapshot, UsageStore},
};

mod postgres;

pub use postgres::PostgresExecutionSettlementStore;
pub use postgres::image_success_reconciliation::{
    DreaminaImageSuccessResolution, DreaminaImageSuccessResolutionRequest,
    resolve_dreamina_image_success,
};
pub use postgres::manual_video_cancellation::{
    DreaminaManualVideoCancellationProof, DreaminaManualVideoCancellationRequest,
};
pub use postgres::supplement_video_cancellation::DreaminaSupplementVideoCancellationRequest;
pub use postgres::video_success_reconciliation::{
    DreaminaVideoArchiveResolution, DreaminaVideoArchiveResolutionRequest,
    DreaminaVideoSuccessResolution, DreaminaVideoSuccessResolutionRequest,
    resolve_dreamina_video_archive, resolve_dreamina_video_success,
};

pub use postgres::video_supplementation::{
    DreaminaVideoSupplementAcceptedRequest, DreaminaVideoSupplementAuthorizationRequest,
    DreaminaVideoSupplementPlan, DreaminaVideoSupplementPublishRequest,
    DreaminaVideoSupplementResolution, DreaminaVideoSupplementUncertainRequest,
};

#[async_trait]
pub trait ExecutionSettlementStore: Send + Sync + 'static {
    fn artifact_storage_identity(&self) -> String;

    async fn succeed(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        result: &GenerationResultManifest,
    ) -> Result<UsageSnapshot, ImageGatewayError>;

    async fn fail(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        error_code: &'static str,
    ) -> Result<(), ImageGatewayError>;

    async fn load_generation_result(
        &self,
        job_id: uuid::Uuid,
    ) -> Result<GenerationResultLookup, ImageGatewayError>;

    async fn generation_status(
        &self,
        job_id: uuid::Uuid,
    ) -> Result<GenerationResultStatus, ImageGatewayError>;

    async fn project_image_status(
        &self,
        _tenant_id: &str,
        _project_id: &str,
        _actor_user_id: Option<uuid::Uuid>,
        _job_id: uuid::Uuid,
    ) -> Result<Option<ImageTaskProjection>, ImageGatewayError> {
        Ok(None)
    }

    async fn validate_complete_image_result_budget(
        &self,
        _tenant_id: &str,
        _project_id: &str,
        _actor_user_id: Option<uuid::Uuid>,
        _job_id: uuid::Uuid,
    ) -> Result<(), ImageGatewayError> {
        Ok(())
    }

    async fn project_additional_image_results(
        &self,
        _tenant_id: &str,
        _project_id: &str,
        _actor_user_id: Option<uuid::Uuid>,
        _job_id: uuid::Uuid,
    ) -> Result<Vec<AdditionalImageResult>, ImageGatewayError> {
        Ok(Vec::new())
    }

    async fn project_image_job_by_idempotency_key(
        &self,
        _tenant_id: &str,
        _project_id: &str,
        _actor_user_id: Option<uuid::Uuid>,
        _operation: &str,
        _key_digest: &str,
    ) -> Result<Option<uuid::Uuid>, ImageGatewayError> {
        Ok(None)
    }

    async fn project_video_job_by_idempotency_key(
        &self,
        _tenant_id: &str,
        _project_id: &str,
        _actor_user_id: Option<uuid::Uuid>,
        _key_digest: &str,
    ) -> Result<Option<VideoIdempotencyProjection>, ImageGatewayError> {
        Ok(None)
    }

    async fn video_status(
        &self,
        _tenant_id: &str,
        _job_id: uuid::Uuid,
    ) -> Result<Option<VideoResultStatus>, ImageGatewayError> {
        Ok(None)
    }

    async fn project_video_status(
        &self,
        _tenant_id: &str,
        _project_id: &str,
        _actor_user_id: Option<uuid::Uuid>,
        _job_id: uuid::Uuid,
    ) -> Result<Option<VideoResultStatus>, ImageGatewayError> {
        Ok(None)
    }

    async fn project_video_status_with_metadata(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<uuid::Uuid>,
        job_id: uuid::Uuid,
    ) -> Result<Option<(VideoResultStatus, VideoRequestMetadata)>, ImageGatewayError> {
        Ok(self
            .project_video_status(tenant_id, project_id, actor_user_id, job_id)
            .await?
            .map(|status| (status, VideoRequestMetadata::default())))
    }

    async fn project_video_released_settlement(
        &self,
        _tenant_id: &str,
        _project_id: &str,
        _actor_user_id: Option<uuid::Uuid>,
        _job_id: uuid::Uuid,
    ) -> Result<Option<image_api_contracts::dreamina::DreaminaReleasedSettlement>, ImageGatewayError>
    {
        Ok(None)
    }

    async fn project_dreamina_manual_video_cancellation(
        &self,
        _tenant_id: &str,
        _project_id: &str,
        _actor_user_id: Option<uuid::Uuid>,
        _job_id: uuid::Uuid,
    ) -> Result<Option<DreaminaManualVideoCancellationProof>, ImageGatewayError> {
        Ok(None)
    }

    async fn load_video_artifact(
        &self,
        _tenant_id: &str,
        _artifact_id: uuid::Uuid,
    ) -> Result<Option<StoredVideoArtifact>, ImageGatewayError> {
        Ok(None)
    }

    async fn load_project_video_artifact(
        &self,
        _tenant_id: &str,
        _project_id: &str,
        _actor_user_id: Option<uuid::Uuid>,
        _artifact_id: uuid::Uuid,
    ) -> Result<Option<StoredVideoArtifact>, ImageGatewayError> {
        Ok(None)
    }
}

#[derive(Debug)]
pub enum GenerationResultStatus {
    Pending,
    Succeeded(StoredGenerationResult),
    Expired,
    Failed { error_code: Option<String> },
    Uncertain,
}

#[derive(Debug)]
pub enum GenerationResultLookup {
    Available(StoredGenerationResult),
    Expired,
    Missing,
}

#[derive(Debug)]
pub struct AdditionalImageResult {
    pub source_index: usize,
    pub bytes: Vec<u8>,
    pub sha256: String,
    pub byte_size: u64,
    pub mime_type: String,
}

#[derive(Debug)]
pub struct ImageTaskProjection {
    pub model: String,
    pub request_id: String,
    pub request: image_api_contracts::dreamina::DreaminaImageRequestMetadata,
    pub status: ImageResultStatus,
}

#[derive(Debug)]
pub enum ImageResultStatus {
    Pending {
        stage: VideoPendingStage,
        provider_progress: Option<VideoProviderProgress>,
    },
    Succeeded {
        result: StoredGenerationResult,
        output_counts: image_api_contracts::dreamina::DreaminaImageOutputCounts,
    },
    Failed {
        error_code: Option<String>,
    },
    Uncertain,
    Expired,
}

#[derive(Debug)]
pub struct VideoIdempotencyProjection {
    pub job_id: uuid::Uuid,
    pub request_id: String,
    pub request: image_api_contracts::dreamina::DreaminaVideoRequestMetadata,
}

#[derive(Debug, Eq, PartialEq)]
pub enum VideoResultStatus {
    Pending {
        model: String,
        duration_ms: u32,
        stage: VideoPendingStage,
        provider_progress: Option<VideoProviderProgress>,
    },
    Succeeded {
        model: String,
        duration_ms: u32,
        artifact_id: uuid::Uuid,
    },
    SucceededRemote {
        model: String,
        duration_ms: u32,
        result: image_provider_sdk::RemoteVideoResult,
    },
    Failed {
        model: String,
        duration_ms: u32,
        error_code: Option<String>,
    },
    Uncertain {
        model: String,
        duration_ms: u32,
        provider_progress: Option<VideoProviderProgress>,
        /// Recorded on the bound submission before a deadline may replace the job error.
        submission_error_code: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VideoProviderProgress {
    pub progress: image_provider_sdk::ProviderTaskProgress,
    pub observed_at_ms: i64,
}

/// Specifications frozen at admission, not measurements of the generated media.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct VideoRequestMetadata {
    pub provider_model: Option<String>,
    pub resolution: Option<String>,
    pub ratio: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoPendingStage {
    Queued,
    Dispatching,
    Processing,
}

#[derive(Debug, Eq, PartialEq)]
pub struct StoredVideoArtifact {
    pub media_type: String,
    pub bytes: Vec<u8>,
}

pub(crate) struct SequentialExecutionSettlementStore {
    admission_store: Arc<dyn AdmissionStore>,
    usage_store: Arc<dyn UsageStore>,
    artifact_store: Arc<dyn ArtifactBlobStore>,
    results: Mutex<HashMap<uuid::Uuid, GenerationResultManifest>>,
}

impl SequentialExecutionSettlementStore {
    pub(crate) fn new(
        admission_store: Arc<dyn AdmissionStore>,
        usage_store: Arc<dyn UsageStore>,
        artifact_store: Arc<dyn ArtifactBlobStore>,
    ) -> Self {
        Self {
            admission_store,
            usage_store,
            artifact_store,
            results: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ExecutionSettlementStore for SequentialExecutionSettlementStore {
    fn artifact_storage_identity(&self) -> String {
        self.artifact_store.storage_identity()
    }

    async fn succeed(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        result: &GenerationResultManifest,
    ) -> Result<UsageSnapshot, ImageGatewayError> {
        validate_generation_result(lease, reservation, result)?;
        {
            let mut results = self
                .results
                .lock()
                .map_err(|_| ImageGatewayError::internal("result projection lock poisoned"))?;
            if let Some(existing) = results.get(&lease.job_id) {
                if existing != result {
                    return Err(ImageGatewayError::internal(
                        "generation result differs from committed projection",
                    ));
                }
            } else {
                results.insert(lease.job_id, result.clone());
            }
        }
        self.admission_store
            .settle(lease, WorkOutcome::Succeeded, None)
            .await
            .map_err(map_admission_error)?;
        self.usage_store.commit(reservation).await
    }

    async fn fail(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        error_code: &'static str,
    ) -> Result<(), ImageGatewayError> {
        self.admission_store
            .settle(lease, WorkOutcome::Failed, Some(error_code))
            .await
            .map_err(map_admission_error)?;
        self.usage_store.release(reservation, error_code).await
    }

    async fn load_generation_result(
        &self,
        job_id: uuid::Uuid,
    ) -> Result<GenerationResultLookup, ImageGatewayError> {
        let manifest = self
            .results
            .lock()
            .map_err(|_| ImageGatewayError::internal("result projection lock poisoned"))?
            .get(&job_id)
            .cloned();
        match manifest {
            Some(manifest) => hydrate_generation_result(self.artifact_store.as_ref(), manifest)
                .await
                .map(GenerationResultLookup::Available),
            None => Ok(GenerationResultLookup::Missing),
        }
    }

    async fn generation_status(
        &self,
        job_id: uuid::Uuid,
    ) -> Result<GenerationResultStatus, ImageGatewayError> {
        match self.load_generation_result(job_id).await? {
            GenerationResultLookup::Available(result) => {
                Ok(GenerationResultStatus::Succeeded(result))
            }
            GenerationResultLookup::Expired => Ok(GenerationResultStatus::Expired),
            GenerationResultLookup::Missing => Ok(GenerationResultStatus::Pending),
        }
    }
}

pub(super) fn validate_generation_result(
    lease: &WorkLease,
    reservation: &UsageReservation,
    result: &GenerationResultManifest,
) -> Result<(), ImageGatewayError> {
    let artifacts_match = result.artifacts.len() == reservation.charge.output_count as usize
        && result
            .artifacts
            .iter()
            .enumerate()
            .all(|(index, artifact)| {
                artifact.identity.job_id == lease.job_id
                    && artifact.identity.tenant_id == reservation.charge.tenant_id
                    && artifact.identity.work_item_id == lease.work_item_id
                    && artifact.identity.execution_id == lease.execution_id
                    && artifact.identity.lease_epoch == lease.lease_epoch
                    && artifact.identity.output_index == index as u32
                    && artifact.identity.media_type.starts_with("image/")
                    && artifact.byte_size > 0
                    && artifact.sha256_hex.len() == 64
                    && artifact
                        .sha256_hex
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit())
                    && !artifact.storage_backend.is_empty()
                    && !artifact.object_key.is_empty()
            });
    if lease.job_id != reservation.job_id
        || result.job_id != lease.job_id
        || result.tenant_id != reservation.charge.tenant_id
        || result.projection.api_profile.is_empty()
        || !matches!(result.projection.operation.as_str(), "generation" | "edit")
        || result.projection.response_schema.is_empty()
        || result.projection.created_at_seconds <= 0
        || result.projection.usage != reservation.snapshot
        || !artifacts_match
    {
        return Err(ImageGatewayError::internal(
            "generation result does not match execution settlement state",
        ));
    }
    Ok(())
}

fn map_admission_error(error: AdmissionError) -> ImageGatewayError {
    match error {
        AdmissionError::Expired => ImageGatewayError::timeout(),
        AdmissionError::Unavailable
        | AdmissionError::PricingUnavailable
        | AdmissionError::BillingLimitExceeded
        | AdmissionError::ProjectBudgetExceeded => {
            ImageGatewayError::service_unavailable("admission settlement unavailable")
        }
        AdmissionError::InvalidOwner
        | AdmissionError::StaleLease
        | AdmissionError::InvalidCommand => {
            ImageGatewayError::internal("execution lease is stale or invalid")
        }
    }
}
