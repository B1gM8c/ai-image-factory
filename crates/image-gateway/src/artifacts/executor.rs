use std::{io::Cursor, sync::Arc};

#[cfg(test)]
use super::{ArtifactBlobStore, MEMORY_BACKEND, memory::InMemoryArtifactBlobStore};
use super::{
    ArtifactIdentity, ArtifactMetadata, ArtifactReadError, ArtifactWriteError, FILESYSTEM_BACKEND,
    executor_object_key, filesystem::FilesystemArtifactBlobStore, sha256_hex,
};
use crate::executor::{
    ExecutorArtifactAuthority, ExecutorArtifactAuthorityStore, ExecutorArtifactSink,
    ExecutorResultManifest, ExecutorSubmissionError, ExecutorSubmissionLease,
    PostgresExecutorSubmissionStore, RunnerError,
};
use async_trait::async_trait;

const MAX_DECODED_IMAGE_PIXELS: u64 = 16 * 1024 * 1024;
const MAX_DECODED_IMAGE_DIMENSION: u32 = 8 * 1024;
const MAX_ARTIFACT_BYTES: usize = 256 * 1024 * 1024;

#[async_trait]
trait ExecutorArtifactBlobStore: Send + Sync + 'static {
    fn storage_backend(&self) -> &'static str;
    fn storage_namespace(&self) -> Result<String, ArtifactReadError>;

    async fn put(
        &self,
        identity: ArtifactIdentity,
        bytes: &[u8],
    ) -> Result<ArtifactMetadata, ArtifactWriteError>;

    async fn get(&self, artifact: &ArtifactMetadata) -> Result<Vec<u8>, ArtifactReadError>;

    async fn delete_unpublished(
        &self,
        artifact: &ArtifactMetadata,
    ) -> Result<(), ArtifactWriteError>;
}

#[async_trait]
impl ExecutorArtifactBlobStore for FilesystemArtifactBlobStore {
    fn storage_backend(&self) -> &'static str {
        FILESYSTEM_BACKEND
    }

    fn storage_namespace(&self) -> Result<String, ArtifactReadError> {
        self.executor_storage_namespace()
    }

    async fn put(
        &self,
        identity: ArtifactIdentity,
        bytes: &[u8],
    ) -> Result<ArtifactMetadata, ArtifactWriteError> {
        self.put_executor_artifact(identity, bytes).await
    }

    async fn get(&self, artifact: &ArtifactMetadata) -> Result<Vec<u8>, ArtifactReadError> {
        self.get_executor_artifact(artifact).await
    }

    async fn delete_unpublished(
        &self,
        artifact: &ArtifactMetadata,
    ) -> Result<(), ArtifactWriteError> {
        self.delete_unpublished_executor_artifact(artifact).await
    }
}

#[cfg(test)]
#[async_trait]
impl ExecutorArtifactBlobStore for InMemoryArtifactBlobStore {
    fn storage_backend(&self) -> &'static str {
        MEMORY_BACKEND
    }

    fn storage_namespace(&self) -> Result<String, ArtifactReadError> {
        Ok(ArtifactBlobStore::storage_identity(self))
    }

    async fn put(
        &self,
        identity: ArtifactIdentity,
        bytes: &[u8],
    ) -> Result<ArtifactMetadata, ArtifactWriteError> {
        self.put_executor_artifact(identity, bytes).await
    }

    async fn get(&self, artifact: &ArtifactMetadata) -> Result<Vec<u8>, ArtifactReadError> {
        self.get_executor_artifact(artifact).await
    }

    async fn delete_unpublished(
        &self,
        artifact: &ArtifactMetadata,
    ) -> Result<(), ArtifactWriteError> {
        self.delete_unpublished_executor_artifact(artifact).await
    }
}

pub struct ExecutorArtifactPublisher {
    blobs: Arc<dyn ExecutorArtifactBlobStore>,
    authorities: Arc<dyn ExecutorArtifactAuthorityStore>,
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExecutorArtifactPublishError {
    #[error("executor artifact publication input is invalid")]
    InvalidInput,
    #[error("executor artifact storage is unavailable")]
    ArtifactUnavailable,
    #[error("executor artifact failed integrity verification")]
    ArtifactIntegrity,
    #[error(transparent)]
    Authority(ExecutorSubmissionError),
}

impl ExecutorArtifactPublisher {
    pub fn with_filesystem_store(
        blobs: Arc<FilesystemArtifactBlobStore>,
        authorities: PostgresExecutorSubmissionStore,
    ) -> Self {
        Self::new(blobs, Arc::new(authorities))
    }

    fn new<B>(blobs: Arc<B>, authorities: Arc<dyn ExecutorArtifactAuthorityStore>) -> Self
    where
        B: ExecutorArtifactBlobStore,
    {
        Self { blobs, authorities }
    }

    pub async fn publish(
        &self,
        lease: &ExecutorSubmissionLease,
        bytes: &[u8],
    ) -> Result<ExecutorResultManifest, ExecutorArtifactPublishError> {
        if bytes.is_empty() || bytes.len() > MAX_ARTIFACT_BYTES {
            return Err(ExecutorArtifactPublishError::InvalidInput);
        }
        let output_index = u32::try_from(lease.output_index)
            .map_err(|_| ExecutorArtifactPublishError::InvalidInput)?;
        let manifest =
            ExecutorResultManifest::new(lease.submission_id, lease.executor_execution_id)
                .ok_or(ExecutorArtifactPublishError::InvalidInput)?;
        let authority_id = manifest.artifact_authority_id();
        let media_type = media_type_from_bytes(bytes)?;
        let expected_object_key = executor_object_key(authority_id);
        let expected_sha256_hex = sha256_hex(bytes);
        let expected_byte_size = bytes.len() as u64;
        let identity = ArtifactIdentity {
            artifact_id: authority_id,
            tenant_id: lease.tenant_id.clone(),
            job_id: lease.job_id,
            work_item_id: lease.work_item_id,
            execution_id: lease.executor_execution_id,
            lease_epoch: lease.executor_lease_epoch,
            output_index,
            media_type: media_type.to_string(),
        };
        let stored = self
            .blobs
            .put(identity.clone(), bytes)
            .await
            .map_err(map_write_error)?;
        if stored.identity != identity
            || stored.storage_backend != self.blobs.storage_backend()
            || stored.object_key != expected_object_key
            || stored.sha256_hex != expected_sha256_hex
            || stored.byte_size != expected_byte_size
            || stored.identity.media_type != media_type
        {
            self.delete_unpublished(&stored).await?;
            return Err(ExecutorArtifactPublishError::ArtifactIntegrity);
        }
        let verified = self.blobs.get(&stored).await.map_err(map_read_error)?;
        if verified != bytes || media_type_from_bytes(&verified)? != media_type {
            self.delete_unpublished(&stored).await?;
            return Err(ExecutorArtifactPublishError::ArtifactIntegrity);
        }
        let authority = ExecutorArtifactAuthority {
            authority_id,
            storage_backend: stored.storage_backend.clone(),
            storage_namespace: self.blobs.storage_namespace().map_err(map_read_error)?,
            object_key: stored.object_key.clone(),
            sha256_hex: stored.sha256_hex.clone(),
            byte_size: stored.byte_size,
            media_type: stored.identity.media_type.clone(),
        };
        match self
            .authorities
            .publish_artifact_authority(lease, &authority)
            .await
        {
            Ok(()) => Ok(manifest),
            Err(
                error
                @ (ExecutorSubmissionError::Unavailable | ExecutorSubmissionError::StaleLease),
            ) => Err(ExecutorArtifactPublishError::Authority(error)),
            Err(error) => {
                self.delete_unpublished(&stored).await?;
                Err(ExecutorArtifactPublishError::Authority(error))
            }
        }
    }

    async fn delete_unpublished(
        &self,
        artifact: &super::ArtifactMetadata,
    ) -> Result<(), ExecutorArtifactPublishError> {
        self.blobs
            .delete_unpublished(artifact)
            .await
            .map_err(map_write_error)
    }
}

#[async_trait]
impl ExecutorArtifactSink for ExecutorArtifactPublisher {
    async fn publish(
        &self,
        lease: &ExecutorSubmissionLease,
        bytes: &[u8],
    ) -> Result<ExecutorResultManifest, RunnerError> {
        ExecutorArtifactPublisher::publish(self, lease, bytes)
            .await
            .map_err(|error| match error {
                ExecutorArtifactPublishError::InvalidInput
                | ExecutorArtifactPublishError::ArtifactIntegrity => RunnerError::Internal,
                ExecutorArtifactPublishError::ArtifactUnavailable
                | ExecutorArtifactPublishError::Authority(ExecutorSubmissionError::Unavailable) => {
                    RunnerError::Unavailable
                }
                ExecutorArtifactPublishError::Authority(
                    ExecutorSubmissionError::Conflict
                    | ExecutorSubmissionError::InvalidInput
                    | ExecutorSubmissionError::StaleLease,
                ) => RunnerError::Unknown {
                    error_code: "artifact_authority_rejected".to_string(),
                },
            })
    }
}

fn media_type_from_bytes(bytes: &[u8]) -> Result<&'static str, ExecutorArtifactPublishError> {
    let format =
        image::guess_format(bytes).map_err(|_| ExecutorArtifactPublishError::ArtifactIntegrity)?;
    let media_type = match format {
        image::ImageFormat::Png => "image/png",
        image::ImageFormat::Jpeg => "image/jpeg",
        image::ImageFormat::WebP => "image/webp",
        _ => return Err(ExecutorArtifactPublishError::ArtifactIntegrity),
    };
    let reader = image::ImageReader::with_format(Cursor::new(bytes), format);
    let (width, height) = reader
        .into_dimensions()
        .map_err(|_| ExecutorArtifactPublishError::ArtifactIntegrity)?;
    if width > MAX_DECODED_IMAGE_DIMENSION
        || height > MAX_DECODED_IMAGE_DIMENSION
        || u64::from(width).saturating_mul(u64::from(height)) > MAX_DECODED_IMAGE_PIXELS
    {
        return Err(ExecutorArtifactPublishError::ArtifactIntegrity);
    }
    image::load_from_memory_with_format(bytes, format)
        .map_err(|_| ExecutorArtifactPublishError::ArtifactIntegrity)?;
    Ok(media_type)
}

fn map_write_error(_: ArtifactWriteError) -> ExecutorArtifactPublishError {
    ExecutorArtifactPublishError::ArtifactUnavailable
}

fn map_read_error(error: ArtifactReadError) -> ExecutorArtifactPublishError {
    match error {
        ArtifactReadError::Unavailable => ExecutorArtifactPublishError::ArtifactUnavailable,
        ArtifactReadError::Integrity => ExecutorArtifactPublishError::ArtifactIntegrity,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;
    use crate::artifacts::{ArtifactMetadata, InMemoryArtifactBlobStore, sha256_hex};

    #[derive(Default)]
    struct FakeAuthorityStore {
        published: Mutex<Option<ExecutorArtifactAuthority>>,
        failure: Mutex<Option<ExecutorSubmissionError>>,
    }

    struct LyingBlobStore;

    #[async_trait]
    impl ExecutorArtifactBlobStore for LyingBlobStore {
        fn storage_backend(&self) -> &'static str {
            MEMORY_BACKEND
        }

        fn storage_namespace(&self) -> Result<String, ArtifactReadError> {
            Ok("memory-v1:lying".to_string())
        }

        async fn put(
            &self,
            identity: ArtifactIdentity,
            _bytes: &[u8],
        ) -> Result<ArtifactMetadata, ArtifactWriteError> {
            Ok(ArtifactMetadata {
                identity,
                storage_backend: MEMORY_BACKEND.to_string(),
                object_key: "../forged".to_string(),
                sha256_hex: "f".repeat(64),
                byte_size: 1,
            })
        }

        async fn get(&self, _artifact: &ArtifactMetadata) -> Result<Vec<u8>, ArtifactReadError> {
            unreachable!("forged metadata must be rejected before readback")
        }

        async fn delete_unpublished(
            &self,
            _artifact: &ArtifactMetadata,
        ) -> Result<(), ArtifactWriteError> {
            Ok(())
        }
    }

    #[async_trait]
    impl ExecutorArtifactAuthorityStore for FakeAuthorityStore {
        async fn publish_artifact_authority(
            &self,
            _lease: &ExecutorSubmissionLease,
            authority: &ExecutorArtifactAuthority,
        ) -> Result<(), ExecutorSubmissionError> {
            if let Some(error) = self.failure.lock().unwrap().take() {
                return Err(error);
            }
            let mut published = self.published.lock().unwrap();
            match published.as_ref() {
                Some(existing) if existing == authority => Ok(()),
                Some(_) => Err(ExecutorSubmissionError::Conflict),
                None => {
                    *published = Some(authority.clone());
                    Ok(())
                }
            }
        }
    }

    #[tokio::test]
    async fn publisher_derives_metadata_from_verified_bytes_and_replays() {
        let blobs = Arc::new(InMemoryArtifactBlobStore::default());
        let authorities = Arc::new(FakeAuthorityStore::default());
        let publisher = ExecutorArtifactPublisher::new(blobs.clone(), authorities.clone());
        let lease = lease();
        let bytes = png([10, 20, 30, 255]);

        let first = publisher.publish(&lease, &bytes).await.unwrap();
        let replay = publisher.publish(&lease, &bytes).await.unwrap();

        assert_eq!(first, replay);
        assert_eq!(first.manifest_id(), lease.submission_id);
        assert_eq!(first.artifact_authority_id(), lease.executor_execution_id);
        let authority = authorities.published.lock().unwrap().clone().unwrap();
        assert_eq!(authority.authority_id, lease.executor_execution_id);
        assert_eq!(authority.sha256_hex, sha256_hex(&bytes));
        assert_eq!(authority.byte_size, bytes.len() as u64);
        assert_eq!(authority.media_type, "image/png");
        assert!(
            authority
                .object_key
                .contains(&lease.executor_execution_id.simple().to_string())
        );
        let metadata = ArtifactMetadata {
            identity: ArtifactIdentity {
                artifact_id: authority.authority_id,
                tenant_id: lease.tenant_id.clone(),
                job_id: lease.job_id,
                work_item_id: lease.work_item_id,
                execution_id: lease.executor_execution_id,
                lease_epoch: lease.executor_lease_epoch,
                output_index: lease.output_index as u32,
                media_type: authority.media_type.clone(),
            },
            storage_backend: authority.storage_backend.clone(),
            object_key: authority.object_key.clone(),
            sha256_hex: authority.sha256_hex.clone(),
            byte_size: authority.byte_size,
        };
        assert_eq!(
            ArtifactBlobStore::delete(&*blobs, &metadata).await,
            Err(ArtifactWriteError::Unavailable)
        );
        let generic =
            ArtifactBlobStore::put(&*blobs, metadata.identity.clone(), &png([90, 80, 70, 255]))
                .await
                .unwrap();
        assert_ne!(generic.object_key, metadata.object_key);
        assert_eq!(blobs.get_executor_artifact(&metadata).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn conflicting_bytes_cannot_replace_a_published_object() {
        let blobs = Arc::new(InMemoryArtifactBlobStore::default());
        let authorities = Arc::new(FakeAuthorityStore::default());
        let publisher = ExecutorArtifactPublisher::new(blobs, authorities);
        let lease = lease();
        let first = png([1, 2, 3, 255]);
        let second = png([4, 5, 6, 255]);

        publisher.publish(&lease, &first).await.unwrap();
        assert_eq!(
            publisher.publish(&lease, &second).await,
            Err(ExecutorArtifactPublishError::ArtifactUnavailable)
        );
    }

    #[tokio::test]
    async fn ambiguous_authority_commit_retains_the_verified_object() {
        let blobs = Arc::new(InMemoryArtifactBlobStore::default());
        let authorities = Arc::new(FakeAuthorityStore::default());
        *authorities.failure.lock().unwrap() = Some(ExecutorSubmissionError::Unavailable);
        let publisher = ExecutorArtifactPublisher::new(blobs.clone(), authorities);
        let lease = lease();
        let authority_id = lease.executor_execution_id;
        let bytes = png([40, 50, 60, 255]);

        assert_eq!(
            publisher.publish(&lease, &bytes).await,
            Err(ExecutorArtifactPublishError::Authority(
                ExecutorSubmissionError::Unavailable
            ))
        );
        let metadata = ArtifactMetadata {
            identity: ArtifactIdentity {
                artifact_id: authority_id,
                tenant_id: lease.tenant_id.clone(),
                job_id: lease.job_id,
                work_item_id: lease.work_item_id,
                execution_id: lease.executor_execution_id,
                lease_epoch: lease.executor_lease_epoch,
                output_index: lease.output_index as u32,
                media_type: "image/png".to_string(),
            },
            storage_backend: super::super::MEMORY_BACKEND.to_string(),
            object_key: executor_object_key(authority_id),
            sha256_hex: sha256_hex(&bytes),
            byte_size: bytes.len() as u64,
        };
        assert_eq!(blobs.get_executor_artifact(&metadata).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn stale_lease_retains_the_verified_object_for_late_observation() {
        let blobs = Arc::new(InMemoryArtifactBlobStore::default());
        let authorities = Arc::new(FakeAuthorityStore::default());
        *authorities.failure.lock().unwrap() = Some(ExecutorSubmissionError::StaleLease);
        let publisher = ExecutorArtifactPublisher::new(blobs.clone(), authorities);
        let lease = lease();
        let bytes = png([15, 25, 35, 255]);

        assert_eq!(
            publisher.publish(&lease, &bytes).await,
            Err(ExecutorArtifactPublishError::Authority(
                ExecutorSubmissionError::StaleLease
            ))
        );
        let metadata = ArtifactMetadata {
            identity: ArtifactIdentity {
                artifact_id: lease.executor_execution_id,
                tenant_id: lease.tenant_id.clone(),
                job_id: lease.job_id,
                work_item_id: lease.work_item_id,
                execution_id: lease.executor_execution_id,
                lease_epoch: lease.executor_lease_epoch,
                output_index: lease.output_index as u32,
                media_type: "image/png".to_string(),
            },
            storage_backend: MEMORY_BACKEND.to_string(),
            object_key: executor_object_key(lease.executor_execution_id),
            sha256_hex: sha256_hex(&bytes),
            byte_size: bytes.len() as u64,
        };
        assert_eq!(blobs.get_executor_artifact(&metadata).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn publisher_rejects_backend_supplied_authority_metadata() {
        let authorities = Arc::new(FakeAuthorityStore::default());
        let publisher =
            ExecutorArtifactPublisher::new(Arc::new(LyingBlobStore), authorities.clone());

        assert_eq!(
            publisher.publish(&lease(), &png([70, 80, 90, 255])).await,
            Err(ExecutorArtifactPublishError::ArtifactIntegrity)
        );
        assert!(authorities.published.lock().unwrap().is_none());
    }

    fn lease() -> ExecutorSubmissionLease {
        ExecutorSubmissionLease {
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            output_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            tenant_id: "tenant-test".to_string(),
            provider_id: "provider-test".to_string(),
            model: "model-test".to_string(),
            work_item_id: Uuid::new_v4(),
            output_index: 0,
            command_schema: "command-v1".to_string(),
            command_hash: "a".repeat(64),
            executor_owner: "executor-test".to_string(),
            executor_lease_epoch: 1,
            executor_lease_expires_at_ms: i64::MAX,
        }
    }

    fn png(pixel: [u8; 4]) -> Vec<u8> {
        use std::io::Cursor;

        let image = image::RgbaImage::from_pixel(1, 1, image::Rgba(pixel));
        let mut cursor = Cursor::new(Vec::new());
        image::DynamicImage::ImageRgba8(image)
            .write_to(&mut cursor, image::ImageFormat::Png)
            .unwrap();
        cursor.into_inner()
    }
}
