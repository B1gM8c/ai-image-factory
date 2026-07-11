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
    ) -> Result<Option<StoredGenerationResult>, ImageGatewayError>;

    async fn generation_status(
        &self,
        job_id: uuid::Uuid,
    ) -> Result<GenerationResultStatus, ImageGatewayError>;
}

#[derive(Debug)]
pub enum GenerationResultStatus {
    Pending,
    Succeeded(StoredGenerationResult),
    Failed { error_code: Option<String> },
    Uncertain,
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
    ) -> Result<Option<StoredGenerationResult>, ImageGatewayError> {
        let manifest = self
            .results
            .lock()
            .map_err(|_| ImageGatewayError::internal("result projection lock poisoned"))?
            .get(&job_id)
            .cloned();
        match manifest {
            Some(manifest) => hydrate_generation_result(self.artifact_store.as_ref(), manifest)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    async fn generation_status(
        &self,
        job_id: uuid::Uuid,
    ) -> Result<GenerationResultStatus, ImageGatewayError> {
        match self.load_generation_result(job_id).await? {
            Some(result) => Ok(GenerationResultStatus::Succeeded(result)),
            None => Ok(GenerationResultStatus::Pending),
        }
    }
}

pub(super) fn validate_generation_result(
    lease: &WorkLease,
    reservation: &UsageReservation,
    result: &GenerationResultManifest,
) -> Result<(), ImageGatewayError> {
    let artifacts_match = result.artifacts.len() == reservation.charge.units as usize
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
