use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InputBlobKey {
    pub admission_session_id: Uuid,
    pub input_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InputBlobRef {
    pub key: InputBlobKey,
    pub storage_backend: String,
    pub object_key: String,
    pub sha256_hex: String,
    pub byte_size: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputBlobReadError {
    Unavailable,
    Integrity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputBlobWriteError {
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputBlobDeleteError {
    Unavailable,
}

#[async_trait]
pub trait InputBlobStore: Send + Sync + 'static {
    fn storage_identity(&self) -> String {
        "custom-input-v1".to_string()
    }

    async fn put(
        &self,
        key: InputBlobKey,
        bytes: &[u8],
    ) -> Result<InputBlobRef, InputBlobWriteError>;

    async fn get(&self, blob: &InputBlobRef) -> Result<Vec<u8>, InputBlobReadError>;

    async fn delete(&self, blob: &InputBlobRef) -> Result<(), InputBlobDeleteError>;

    async fn delete_session(&self, admission_session_id: Uuid) -> Result<(), InputBlobDeleteError>;
}
