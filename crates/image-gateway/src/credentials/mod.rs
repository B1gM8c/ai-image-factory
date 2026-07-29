use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use uuid::Uuid;

mod postgres;

pub use postgres::{CredentialRefreshLease, PostgresCredentialStore};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalCredential {
    pub provider_account_id: Uuid,
    pub provider_id: String,
    pub revision: i64,
    pub material_kind: String,
    pub material_fingerprint_sha256: String,
    pub environment_ref: Arc<std::path::PathBuf>,
    pub access_expires_at_ms: Option<i64>,
}

impl OperationalCredential {
    pub fn home(&self) -> &Path {
        self.environment_ref.as_path()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialResolveError {
    Invalid,
    Unavailable,
    ReauthorizationRequired,
    Unsupported,
}

#[async_trait]
pub trait OperationalCredentialResolver: Send + Sync + 'static {
    async fn resolve(
        &self,
        provider_account_id: Uuid,
    ) -> Result<OperationalCredential, CredentialResolveError>;
}
