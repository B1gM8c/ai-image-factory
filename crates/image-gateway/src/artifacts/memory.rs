use std::{collections::HashMap, sync::Mutex};

use async_trait::async_trait;

use super::{
    ArtifactBlobStore, ArtifactIdentity, ArtifactMetadata, ArtifactReadError, ArtifactWriteError,
    MEMORY_BACKEND, sha256_hex,
};
use crate::input_blobs::{
    InputBlobDeleteError, InputBlobKey, InputBlobReadError, InputBlobRef, InputBlobStore,
    InputBlobWriteError,
};

pub struct InMemoryArtifactBlobStore {
    storage_identity: String,
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl Default for InMemoryArtifactBlobStore {
    fn default() -> Self {
        Self {
            storage_identity: format!("{MEMORY_BACKEND}:{}", uuid::Uuid::new_v4().simple()),
            objects: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ArtifactBlobStore for InMemoryArtifactBlobStore {
    fn storage_identity(&self) -> String {
        self.storage_identity.clone()
    }

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

    async fn delete(&self, artifact: &ArtifactMetadata) -> Result<(), ArtifactWriteError> {
        if artifact.storage_backend != MEMORY_BACKEND
            || artifact.object_key != format!("objects/{}", artifact.identity.artifact_id.simple())
        {
            return Err(ArtifactWriteError::Unavailable);
        }
        self.objects
            .lock()
            .map_err(|_| ArtifactWriteError::Unavailable)?
            .remove(&artifact.object_key);
        Ok(())
    }
}

fn input_blob_key(key: &InputBlobKey) -> String {
    format!(
        "inputs/{}/{}",
        key.admission_session_id.simple(),
        key.input_id.simple()
    )
}

#[async_trait]
impl InputBlobStore for InMemoryArtifactBlobStore {
    fn storage_identity(&self) -> String {
        self.storage_identity.clone()
    }

    async fn put(
        &self,
        key: InputBlobKey,
        bytes: &[u8],
    ) -> Result<InputBlobRef, InputBlobWriteError> {
        if bytes.is_empty() {
            return Err(InputBlobWriteError::Unavailable);
        }
        let object_key = input_blob_key(&key);
        self.objects
            .lock()
            .map_err(|_| InputBlobWriteError::Unavailable)?
            .insert(object_key.clone(), bytes.to_vec());
        Ok(InputBlobRef {
            key,
            storage_backend: MEMORY_BACKEND.to_string(),
            object_key,
            sha256_hex: sha256_hex(bytes),
            byte_size: bytes.len() as u64,
        })
    }

    async fn get(&self, blob: &InputBlobRef) -> Result<Vec<u8>, InputBlobReadError> {
        if blob.storage_backend != MEMORY_BACKEND || blob.object_key != input_blob_key(&blob.key) {
            return Err(InputBlobReadError::Integrity);
        }
        let bytes = self
            .objects
            .lock()
            .map_err(|_| InputBlobReadError::Unavailable)?
            .get(&blob.object_key)
            .cloned()
            .ok_or(InputBlobReadError::Integrity)?;
        if bytes.len() as u64 != blob.byte_size || sha256_hex(&bytes) != blob.sha256_hex {
            return Err(InputBlobReadError::Integrity);
        }
        Ok(bytes)
    }

    async fn delete(&self, blob: &InputBlobRef) -> Result<(), InputBlobDeleteError> {
        if blob.storage_backend != MEMORY_BACKEND || blob.object_key != input_blob_key(&blob.key) {
            return Err(InputBlobDeleteError::Unavailable);
        }
        self.objects
            .lock()
            .map_err(|_| InputBlobDeleteError::Unavailable)?
            .remove(&blob.object_key);
        Ok(())
    }

    async fn delete_session(
        &self,
        admission_session_id: uuid::Uuid,
    ) -> Result<(), InputBlobDeleteError> {
        let prefix = format!("inputs/{}/", admission_session_id.simple());
        self.objects
            .lock()
            .map_err(|_| InputBlobDeleteError::Unavailable)?
            .retain(|key, _| !key.starts_with(&prefix));
        Ok(())
    }
}
