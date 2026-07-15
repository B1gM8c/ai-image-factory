use std::sync::Arc;

use crate::artifacts::{
    ArtifactBlobStore, ArtifactIdentity, ArtifactMetadata, ArtifactReadError, ArtifactWriteError,
    ExecutorArtifactPublishError, ExecutorArtifactReference, FilesystemArtifactBlobStore,
    customer_object_key, media_type_from_bytes,
};

use super::{CanonicalExecutorOutcome, ExecutorTerminalLease};

pub struct CustomerArtifactPublisher {
    blobs: Arc<FilesystemArtifactBlobStore>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CustomerArtifactPublishError {
    #[error("customer artifact publication requires a canonical success")]
    InvalidInput,
    #[error("customer artifact storage is unavailable")]
    Unavailable,
    #[error("customer artifact authority or bytes failed integrity verification")]
    Integrity,
}

impl CustomerArtifactPublisher {
    pub fn new(blobs: Arc<FilesystemArtifactBlobStore>) -> Self {
        Self { blobs }
    }

    pub async fn publish(
        &self,
        lease: &ExecutorTerminalLease,
    ) -> Result<ArtifactMetadata, CustomerArtifactPublishError> {
        let CanonicalExecutorOutcome::Succeeded(authority) = &lease.outcome else {
            return Err(CustomerArtifactPublishError::InvalidInput);
        };
        let output_index = u32::try_from(lease.output_index)
            .map_err(|_| CustomerArtifactPublishError::InvalidInput)?;
        let source = ExecutorArtifactReference {
            authority_id: authority.authority_id,
            storage_backend: &authority.storage_backend,
            storage_namespace: &authority.storage_namespace,
            object_key: &authority.object_key,
            sha256_hex: &authority.sha256_hex,
            byte_size: authority.byte_size,
        };
        let bytes = self
            .blobs
            .read_executor_reference(&source)
            .await
            .map_err(map_read_error)?;
        let media_type = media_type_from_bytes(&bytes).map_err(map_media_error)?;
        if media_type != authority.media_type {
            return Err(CustomerArtifactPublishError::Integrity);
        }
        let identity = ArtifactIdentity {
            artifact_id: lease.output_id,
            tenant_id: lease.tenant_id.clone(),
            job_id: lease.job_id,
            work_item_id: lease.work_item_id,
            execution_id: lease.attempt_execution_id,
            lease_epoch: lease.attempt_lease_epoch,
            output_index,
            media_type: media_type.to_string(),
        };
        let stored = self
            .blobs
            .put(identity.clone(), &bytes)
            .await
            .map_err(map_write_error)?;
        if stored.identity != identity
            || stored.storage_backend != authority.storage_backend
            || stored.object_key != customer_object_key(lease.output_id)
            || stored.sha256_hex != authority.sha256_hex
            || stored.byte_size != authority.byte_size
        {
            return Err(CustomerArtifactPublishError::Integrity);
        }
        let replay = self.blobs.get(&stored).await.map_err(map_read_error)?;
        if replay != bytes {
            return Err(CustomerArtifactPublishError::Integrity);
        }
        Ok(stored)
    }
}

fn map_read_error(error: ArtifactReadError) -> CustomerArtifactPublishError {
    match error {
        ArtifactReadError::Unavailable => CustomerArtifactPublishError::Unavailable,
        ArtifactReadError::Integrity => CustomerArtifactPublishError::Integrity,
    }
}

fn map_write_error(_: ArtifactWriteError) -> CustomerArtifactPublishError {
    CustomerArtifactPublishError::Unavailable
}

fn map_media_error(error: ExecutorArtifactPublishError) -> CustomerArtifactPublishError {
    match error {
        ExecutorArtifactPublishError::ArtifactUnavailable
        | ExecutorArtifactPublishError::Authority(_) => CustomerArtifactPublishError::Unavailable,
        ExecutorArtifactPublishError::InvalidInput
        | ExecutorArtifactPublishError::ArtifactIntegrity => {
            CustomerArtifactPublishError::Integrity
        }
    }
}
