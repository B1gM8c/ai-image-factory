use std::{env, path::PathBuf};

use async_trait::async_trait;
use uuid::Uuid;

use crate::{ImageGatewayError, generator::GeneratedImage, usage::UsageSnapshot};

mod filesystem;
mod memory;

pub use filesystem::FilesystemArtifactBlobStore;
pub use memory::InMemoryArtifactBlobStore;

pub const FILESYSTEM_BACKEND: &str = "filesystem-v1";
pub const MEMORY_BACKEND: &str = "memory-v1";
pub const GENERATION_RESPONSE_SCHEMA: &str = "openai.images.response.v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactIdentity {
    pub artifact_id: Uuid,
    pub tenant_id: String,
    pub job_id: Uuid,
    pub work_item_id: Uuid,
    pub execution_id: Uuid,
    pub lease_epoch: i64,
    pub output_index: u32,
    pub media_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactMetadata {
    pub identity: ArtifactIdentity,
    pub storage_backend: String,
    pub object_key: String,
    pub sha256_hex: String,
    pub byte_size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationResponseProjection {
    pub api_profile: String,
    pub response_schema: String,
    pub created_at_seconds: i64,
    pub output_format: String,
    pub quality: String,
    pub size: String,
    pub background: String,
    pub stream: bool,
    pub usage: UsageSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationResultManifest {
    pub job_id: Uuid,
    pub tenant_id: String,
    pub projection: GenerationResponseProjection,
    pub artifacts: Vec<ArtifactMetadata>,
}

#[derive(Clone, Debug)]
pub struct StoredGenerationResult {
    pub projection: GenerationResponseProjection,
    pub images: Vec<GeneratedImage>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactReadError {
    Unavailable,
    Integrity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactWriteError {
    Unavailable,
}

#[async_trait]
pub trait ArtifactBlobStore: Send + Sync + 'static {
    async fn put(
        &self,
        identity: ArtifactIdentity,
        bytes: &[u8],
    ) -> Result<ArtifactMetadata, ArtifactWriteError>;

    async fn get(&self, artifact: &ArtifactMetadata) -> Result<Vec<u8>, ArtifactReadError>;

    async fn delete(&self, artifact: &ArtifactMetadata) -> Result<(), ArtifactWriteError>;
}

pub fn artifact_root_from_env() -> Result<PathBuf, ImageGatewayError> {
    let value = env::var("GATEWAY_ARTIFACT_ROOT")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ImageGatewayError::config("GATEWAY_ARTIFACT_ROOT is required"))?;
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(ImageGatewayError::config(
            "GATEWAY_ARTIFACT_ROOT must be an absolute path",
        ));
    }
    Ok(path)
}

pub fn validate_artifact_root_isolated(
    artifact_root: &std::path::Path,
    codex_home: &std::path::Path,
) -> Result<(), ImageGatewayError> {
    let artifact_root = std::fs::canonicalize(artifact_root).map_err(|_| {
        ImageGatewayError::config("GATEWAY_ARTIFACT_ROOT could not be canonicalized")
    })?;
    let codex_home = std::fs::canonicalize(codex_home)
        .map_err(|_| ImageGatewayError::config("GATEWAY_CODEX_HOME could not be canonicalized"))?;
    if artifact_root.starts_with(&codex_home) || codex_home.starts_with(&artifact_root) {
        return Err(ImageGatewayError::config(
            "GATEWAY_ARTIFACT_ROOT and GATEWAY_CODEX_HOME must be separate directory trees",
        ));
    }
    Ok(())
}

pub(crate) async fn hydrate_generation_result(
    blobs: &dyn ArtifactBlobStore,
    manifest: GenerationResultManifest,
) -> Result<StoredGenerationResult, ImageGatewayError> {
    let mut images = Vec::with_capacity(manifest.artifacts.len());
    for artifact in &manifest.artifacts {
        let bytes = blobs.get(artifact).await.map_err(|error| match error {
            ArtifactReadError::Integrity => ImageGatewayError::artifact_integrity(),
            ArtifactReadError::Unavailable => {
                ImageGatewayError::service_unavailable("artifact storage unavailable")
            }
        })?;
        images.push(GeneratedImage { bytes });
    }
    Ok(StoredGenerationResult {
        projection: manifest.projection,
        images,
    })
}

pub(crate) fn media_type_for_output_format(output_format: &str) -> Option<&'static str> {
    match output_format {
        "png" => Some("image/png"),
        "jpeg" => Some("image/jpeg"),
        "webp" => Some("image/webp"),
        _ => None,
    }
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    hex::encode(Sha256::digest(bytes))
}
