use std::{collections::HashMap, sync::Mutex};

use async_trait::async_trait;

use super::{
    ArtifactBlobStore, ArtifactIdentity, ArtifactMetadata, ArtifactReadError, ArtifactWriteError,
    MEMORY_BACKEND, sha256_hex,
};

#[derive(Default)]
pub struct InMemoryArtifactBlobStore {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

#[async_trait]
impl ArtifactBlobStore for InMemoryArtifactBlobStore {
    async fn put(
        &self,
        identity: ArtifactIdentity,
        bytes: &[u8],
    ) -> Result<ArtifactMetadata, ArtifactWriteError> {
        if bytes.is_empty() {
            return Err(ArtifactWriteError::Unavailable);
        }
        let object_key = format!("objects/{}", identity.artifact_id.simple());
        self.objects
            .lock()
            .map_err(|_| ArtifactWriteError::Unavailable)?
            .insert(object_key.clone(), bytes.to_vec());
        Ok(ArtifactMetadata {
            identity,
            storage_backend: MEMORY_BACKEND.to_string(),
            object_key,
            sha256_hex: sha256_hex(bytes),
            byte_size: bytes.len() as u64,
        })
    }

    async fn get(&self, artifact: &ArtifactMetadata) -> Result<Vec<u8>, ArtifactReadError> {
        if artifact.storage_backend != MEMORY_BACKEND
            || artifact.object_key != format!("objects/{}", artifact.identity.artifact_id.simple())
        {
            return Err(ArtifactReadError::Integrity);
        }
        let bytes = self
            .objects
            .lock()
            .map_err(|_| ArtifactReadError::Unavailable)?
            .get(&artifact.object_key)
            .cloned()
            .ok_or(ArtifactReadError::Integrity)?;
        if bytes.len() as u64 != artifact.byte_size || sha256_hex(&bytes) != artifact.sha256_hex {
            return Err(ArtifactReadError::Integrity);
        }
        Ok(bytes)
    }
}
