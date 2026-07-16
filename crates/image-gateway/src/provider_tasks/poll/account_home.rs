use std::{fmt, path::Path};

use image_cli_runtime::{CommandSpecError, WorkingDirectory};
use uuid::Uuid;

use super::ProviderPollRuntimeProfile;

pub struct ProviderAccountHomeCapability {
    provider_id: String,
    credential_pool_id: Uuid,
    provider_account_id: Uuid,
    credential_ref: String,
    credential_revision: i64,
    credential_auth_sha256: String,
    directory: WorkingDirectory,
}

impl ProviderAccountHomeCapability {
    pub fn new(
        provider_id: impl Into<String>,
        credential_pool_id: Uuid,
        provider_account_id: Uuid,
        credential_ref: impl Into<String>,
        credential_revision: i64,
        credential_auth_sha256: impl Into<String>,
        directory: impl AsRef<Path>,
    ) -> Result<Self, ProviderAccountHomeCapabilityError> {
        let provider_id = provider_id.into();
        let credential_ref = credential_ref.into();
        let credential_auth_sha256 = credential_auth_sha256.into();
        if !valid_identifier(&provider_id)
            || credential_pool_id.is_nil()
            || provider_account_id.is_nil()
            || !valid_text(&credential_ref, 1_024)
            || credential_revision <= 0
            || !valid_sha256(&credential_auth_sha256)
        {
            return Err(ProviderAccountHomeCapabilityError::InvalidIdentity);
        }
        let directory = WorkingDirectory::new_private(directory)
            .map_err(ProviderAccountHomeCapabilityError::Directory)?;
        Ok(Self {
            provider_id,
            credential_pool_id,
            provider_account_id,
            credential_ref,
            credential_revision,
            credential_auth_sha256,
            directory,
        })
    }

    pub fn bind(
        &self,
        profile: &ProviderPollRuntimeProfile,
    ) -> Result<WorkingDirectory, ProviderAccountHomeCapabilityError> {
        if profile.provider_id() != self.provider_id
            || profile.credential_pool_id() != self.credential_pool_id
            || profile.provider_account_id() != self.provider_account_id
            || profile.credential_ref() != self.credential_ref
            || profile.credential_revision() != self.credential_revision
            || profile.credential_auth_sha256() != self.credential_auth_sha256
        {
            return Err(ProviderAccountHomeCapabilityError::ProfileMismatch);
        }
        Ok(self.directory.clone())
    }
}

impl fmt::Debug for ProviderAccountHomeCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderAccountHomeCapability")
            .field("provider_id", &self.provider_id)
            .field("credential_pool_id", &self.credential_pool_id)
            .field("provider_account_id", &self.provider_account_id)
            .field("credential_ref", &"[redacted]")
            .field("credential_revision", &self.credential_revision)
            .field("credential_auth_sha256", &"[redacted]")
            .field("directory", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderAccountHomeCapabilityError {
    #[error("provider account-home identity is invalid")]
    InvalidIdentity,
    #[error("provider account-home directory is invalid")]
    Directory(#[source] CommandSpecError),
    #[error("provider account-home capability does not match the runtime profile")]
    ProfileMismatch,
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[cfg(test)]
mod tests;
