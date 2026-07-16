use std::sync::Arc;

use image_provider_sdk::{
    ArtifactMetadata, ArtifactSinkError, ArtifactSinkErrorKind, DurableArtifactManifest,
    DurableArtifactRef,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    FILESYSTEM_BACKEND, FilesystemArtifactBlobStore,
    executor::media_type_from_file,
    executor_object_key,
    filesystem::{ExecutorArtifactStageError, FilesystemExecutorArtifactStage, MAX_ARTIFACT_BYTES},
};
use crate::{
    executor::ExecutorResultManifest,
    provider_tasks::{
        ProviderArtifactAuthority, ProviderArtifactStageContext, ProviderArtifactStager,
        ProviderArtifactStagerFactory, StagedProviderArtifact,
    },
};

const PROVIDER_ARTIFACT_NAMESPACE: &str = "executor-artifacts";

pub struct FilesystemProviderArtifactStagerFactory {
    store: Arc<FilesystemArtifactBlobStore>,
    max_artifact_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderArtifactStagerConfigurationError {
    #[error("provider artifact size limit must be within the filesystem artifact limit")]
    InvalidSizeLimit,
}

impl FilesystemProviderArtifactStagerFactory {
    pub fn new(
        store: Arc<FilesystemArtifactBlobStore>,
        max_artifact_bytes: u64,
    ) -> Result<Self, ProviderArtifactStagerConfigurationError> {
        if max_artifact_bytes == 0 || max_artifact_bytes > MAX_ARTIFACT_BYTES {
            return Err(ProviderArtifactStagerConfigurationError::InvalidSizeLimit);
        }
        Ok(Self {
            store,
            max_artifact_bytes,
        })
    }
}

impl ProviderArtifactStagerFactory for FilesystemProviderArtifactStagerFactory {
    type Stager = FilesystemProviderArtifactStager;

    async fn begin(
        &self,
        context: &ProviderArtifactStageContext,
    ) -> Result<Self::Stager, ArtifactSinkError> {
        let manifest =
            ExecutorResultManifest::new(context.submission_id(), context.executor_execution_id())
                .ok_or_else(|| invalid_artifact("provider_artifact_stage_context_invalid"))?;
        let stage = self
            .store
            .begin_executor_artifact_stage(
                manifest.artifact_authority_id(),
                context.poll_lease_epoch(),
            )
            .await
            .map_err(map_stage_error)?;
        let storage_namespace = self
            .store
            .executor_storage_namespace()
            .map_err(|_| storage_error("provider_artifact_namespace_unavailable"))?;
        Ok(FilesystemProviderArtifactStager {
            stage,
            submission_id: context.submission_id(),
            artifact_id: manifest.artifact_authority_id(),
            storage_namespace,
            max_artifact_bytes: self.max_artifact_bytes,
            byte_size: 0,
            hasher: Sha256::new(),
            finalized: false,
        })
    }
}

pub struct FilesystemProviderArtifactStager {
    stage: FilesystemExecutorArtifactStage,
    submission_id: Uuid,
    artifact_id: Uuid,
    storage_namespace: String,
    max_artifact_bytes: u64,
    byte_size: u64,
    hasher: Sha256,
    finalized: bool,
}

impl ProviderArtifactStager for FilesystemProviderArtifactStager {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactSinkError> {
        if self.finalized || chunk.is_empty() {
            return Err(invalid_artifact("provider_artifact_chunk_invalid"));
        }
        let byte_size = self
            .byte_size
            .checked_add(chunk.len() as u64)
            .filter(|byte_size| *byte_size <= self.max_artifact_bytes)
            .ok_or_else(|| invalid_artifact("provider_artifact_size_limit_exceeded"))?;
        self.stage
            .write_chunk(chunk)
            .await
            .map_err(map_stage_error)?;
        self.hasher.update(chunk);
        self.byte_size = byte_size;
        Ok(())
    }

    async fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> Result<StagedProviderArtifact, ArtifactSinkError> {
        if self.finalized || self.byte_size == 0 {
            return Err(invalid_artifact("provider_artifact_finalize_invalid"));
        }
        self.finalized = true;
        self.stage.finish_writes().await.map_err(map_stage_error)?;
        let reader = self
            .stage
            .open_reader(self.byte_size)
            .await
            .map_err(map_stage_error)?;
        let actual_media_type = tokio::task::spawn_blocking(move || media_type_from_file(reader))
            .await
            .map_err(|_| storage_error("provider_artifact_validation_unavailable"))?
            .map_err(|_| invalid_artifact("provider_artifact_media_invalid"))?;
        if metadata.media_type != actual_media_type {
            return Err(invalid_artifact("provider_artifact_media_mismatch"));
        }

        let sha256: [u8; 32] = self.hasher.clone().finalize().into();
        let artifact_ref = DurableArtifactRef::new(
            PROVIDER_ARTIFACT_NAMESPACE,
            format!(
                "{}:{}",
                self.submission_id.simple(),
                self.artifact_id.simple()
            ),
        )
        .map_err(|_| invalid_artifact("provider_artifact_ref_invalid"))?;
        let manifest =
            DurableArtifactManifest::new(artifact_ref, actual_media_type, self.byte_size, sha256)
                .map_err(|_| invalid_artifact("provider_artifact_manifest_invalid"))?;
        let authority = ProviderArtifactAuthority::new(
            FILESYSTEM_BACKEND.to_owned(),
            self.storage_namespace.clone(),
            executor_object_key(self.artifact_id),
            hex::encode(sha256),
            self.byte_size,
            actual_media_type.to_owned(),
        )
        .ok_or_else(|| invalid_artifact("provider_artifact_authority_invalid"))?;

        self.stage
            .commit(sha256, self.byte_size)
            .await
            .map_err(map_stage_error)?;
        StagedProviderArtifact::new(manifest, authority)
            .map_err(|_| invalid_artifact("provider_artifact_contract_invalid"))
    }
}

fn map_stage_error(error: ExecutorArtifactStageError) -> ArtifactSinkError {
    match error {
        ExecutorArtifactStageError::Unavailable => {
            storage_error("provider_artifact_storage_unavailable")
        }
        ExecutorArtifactStageError::Integrity => {
            invalid_artifact("provider_artifact_storage_conflict")
        }
    }
}

fn invalid_artifact(code: &'static str) -> ArtifactSinkError {
    ArtifactSinkError::new(ArtifactSinkErrorKind::InvalidArtifact, code)
}

fn storage_error(code: &'static str) -> ArtifactSinkError {
    ArtifactSinkError::new(ArtifactSinkErrorKind::Storage, code)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn factory_rejects_zero_and_oversized_limits() {
        let fixture = fixture();
        for limit in [0, MAX_ARTIFACT_BYTES + 1] {
            assert!(matches!(
                FilesystemProviderArtifactStagerFactory::new(Arc::clone(&fixture.store), limit),
                Err(ProviderArtifactStagerConfigurationError::InvalidSizeLimit)
            ));
        }
    }

    #[tokio::test]
    async fn streams_valid_image_into_immutable_executor_object() {
        let fixture = fixture();
        let bytes = png_bytes([10, 20, 30, 255]);
        let mut stager = fixture.stager(1, MAX_ARTIFACT_BYTES).await;

        for chunk in bytes.chunks(7) {
            stager.write_chunk(chunk).await.unwrap();
        }
        let staged = stager
            .finalize(ArtifactMetadata {
                media_type: "image/png",
            })
            .await
            .unwrap();

        assert_eq!(staged.manifest().byte_size(), bytes.len() as u64);
        assert_eq!(staged.manifest().media_type(), "image/png");
        assert_eq!(
            staged.manifest().sha256(),
            &<[u8; 32]>::from(Sha256::digest(&bytes))
        );
        assert_eq!(fs::read(fixture.object_path()).unwrap(), bytes);
        assert!(staging_entries(fixture.staging_path()).is_empty());
    }

    #[tokio::test]
    async fn byte_stable_replay_reuses_object_but_conflicting_replay_is_rejected() {
        let fixture = fixture();
        let bytes = png_bytes([1, 2, 3, 255]);
        fixture.finalize(1, &bytes, "image/png").await.unwrap();
        fixture.finalize(2, &bytes, "image/png").await.unwrap();

        let different = png_bytes([9, 8, 7, 255]);
        let error = expect_sink_error(fixture.finalize(3, &different, "image/png").await);

        assert_eq!(error.kind(), ArtifactSinkErrorKind::InvalidArtifact);
        assert_eq!(error.code(), "provider_artifact_storage_conflict");
        assert_eq!(fs::read(fixture.object_path()).unwrap(), bytes);
        assert!(staging_entries(fixture.staging_path()).is_empty());
    }

    #[tokio::test]
    async fn newer_epoch_unlinks_abandoned_stage_and_fences_older_finalization() {
        let fixture = fixture();
        let bytes = png_bytes([10, 20, 30, 255]);
        let mut stale = fixture.stager(1, MAX_ARTIFACT_BYTES).await;
        stale.write_chunk(&bytes).await.unwrap();

        let mut current = fixture.stager(2, MAX_ARTIFACT_BYTES).await;
        current.write_chunk(&bytes).await.unwrap();
        let stale_error = expect_sink_error(
            stale
                .finalize(ArtifactMetadata {
                    media_type: "image/png",
                })
                .await,
        );

        assert_eq!(stale_error.kind(), ArtifactSinkErrorKind::InvalidArtifact);
        current
            .finalize(ArtifactMetadata {
                media_type: "image/png",
            })
            .await
            .unwrap();
        assert_eq!(fs::read(fixture.object_path()).unwrap(), bytes);
        assert!(staging_entries(fixture.staging_path()).is_empty());
    }

    #[tokio::test]
    async fn metadata_spoof_and_size_overflow_never_publish() {
        let fixture = fixture();
        let bytes = png_bytes([10, 20, 30, 255]);
        let media_error = expect_sink_error(fixture.finalize(1, &bytes, "image/jpeg").await);
        assert_eq!(media_error.kind(), ArtifactSinkErrorKind::InvalidArtifact);
        assert!(!fixture.object_path().exists());

        let mut limited = fixture.stager(2, bytes.len() as u64 - 1).await;
        let size_error = limited.write_chunk(&bytes).await.unwrap_err();
        assert_eq!(size_error.kind(), ArtifactSinkErrorKind::InvalidArtifact);
        drop(limited);
        assert!(!fixture.object_path().exists());
        assert!(staging_entries(fixture.staging_path()).is_empty());
    }

    #[tokio::test]
    async fn dropped_and_crash_left_stages_are_cleaned_before_reuse() {
        let fixture = fixture();
        let bytes = png_bytes([10, 20, 30, 255]);
        let mut dropped = fixture.stager(1, MAX_ARTIFACT_BYTES).await;
        dropped.write_chunk(&bytes).await.unwrap();
        drop(dropped);
        assert!(staging_entries(fixture.staging_path()).is_empty());

        fs::write(
            fixture
                .staging_path()
                .join(".epoch-1-00000000000000000000000000000000"),
            b"orphan",
        )
        .unwrap();
        let mut current = fixture.stager(2, MAX_ARTIFACT_BYTES).await;
        assert!(
            staging_entries(fixture.staging_path())
                .iter()
                .all(|name| name.starts_with(".epoch-2-"))
        );
        current.write_chunk(&bytes).await.unwrap();
        current
            .finalize(ArtifactMetadata {
                media_type: "image/png",
            })
            .await
            .unwrap();
        assert!(staging_entries(fixture.staging_path()).is_empty());
    }

    #[tokio::test]
    async fn older_epoch_cannot_delete_a_future_stage_name() {
        let fixture = fixture();
        let initialized = fixture.stager(1, MAX_ARTIFACT_BYTES).await;
        drop(initialized);
        let future_name = ".epoch-99-00000000000000000000000000000000";
        fs::write(fixture.staging_path().join(future_name), b"future").unwrap();

        assert!(matches!(
            fixture
                .store
                .begin_executor_artifact_stage(fixture.artifact_id, 2)
                .await,
            Err(ExecutorArtifactStageError::Integrity)
        ));
        assert!(fixture.staging_path().join(future_name).exists());
    }

    struct Fixture {
        _root: TempDir,
        store: Arc<FilesystemArtifactBlobStore>,
        submission_id: Uuid,
        artifact_id: Uuid,
    }

    impl Fixture {
        async fn stager(
            &self,
            lease_epoch: i64,
            max_artifact_bytes: u64,
        ) -> FilesystemProviderArtifactStager {
            let stage = self
                .store
                .begin_executor_artifact_stage(self.artifact_id, lease_epoch)
                .await
                .unwrap();
            FilesystemProviderArtifactStager {
                stage,
                submission_id: self.submission_id,
                artifact_id: self.artifact_id,
                storage_namespace: self.store.executor_storage_namespace().unwrap(),
                max_artifact_bytes,
                byte_size: 0,
                hasher: Sha256::new(),
                finalized: false,
            }
        }

        async fn finalize(
            &self,
            lease_epoch: i64,
            bytes: &[u8],
            media_type: &str,
        ) -> Result<StagedProviderArtifact, ArtifactSinkError> {
            let mut stager = self.stager(lease_epoch, MAX_ARTIFACT_BYTES).await;
            stager.write_chunk(bytes).await?;
            stager.finalize(ArtifactMetadata { media_type }).await
        }

        fn object_path(&self) -> std::path::PathBuf {
            let name = self.artifact_id.simple().to_string();
            self._root
                .path()
                .join("executor-objects")
                .join(&name[..2])
                .join(name)
        }

        fn staging_path(&self) -> std::path::PathBuf {
            let name = self.artifact_id.simple().to_string();
            self._root
                .path()
                .join("executor-staging")
                .join(&name[..2])
                .join(name)
        }
    }

    fn fixture() -> Fixture {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(FilesystemArtifactBlobStore::new(root.path()).unwrap());
        Fixture {
            _root: root,
            store,
            submission_id: Uuid::from_u128(1),
            artifact_id: Uuid::from_u128(2),
        }
    }

    fn staging_entries(path: impl AsRef<Path>) -> Vec<String> {
        match fs::read_dir(path) {
            Ok(entries) => entries
                .map(|entry| {
                    entry
                        .unwrap()
                        .file_name()
                        .into_string()
                        .expect("ASCII stage name")
                })
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("staging directory unavailable: {error}"),
        }
    }

    fn png_bytes(pixel: [u8; 4]) -> Vec<u8> {
        let image = RgbaImage::from_pixel(1, 1, Rgba(pixel));
        let mut cursor = std::io::Cursor::new(Vec::new());
        DynamicImage::ImageRgba8(image)
            .write_to(&mut cursor, ImageFormat::Png)
            .unwrap();
        cursor.into_inner()
    }

    fn expect_sink_error<T>(result: Result<T, ArtifactSinkError>) -> ArtifactSinkError {
        match result {
            Ok(_) => panic!("expected artifact sink error"),
            Err(error) => error,
        }
    }
}
