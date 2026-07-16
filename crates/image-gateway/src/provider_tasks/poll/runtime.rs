use std::{fmt, future::Future};

use uuid::Uuid;

use super::MAX_PROVIDER_POLL_LANES;
use crate::{
    executor::ExecutorExecutionProfile,
    provider_tasks::{ProviderTaskClaimScope, ProviderTaskStoreError},
};

pub trait ProviderPollRuntimeProfileStore: Send + Sync + 'static {
    fn load_active_poll_runtime_profile(
        &self,
        profile_key: &str,
    ) -> impl Future<Output = Result<ProviderPollRuntimeProfile, ProviderTaskStoreError>> + Send;
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProviderPollRuntimeProfile {
    profile: ExecutorExecutionProfile,
    max_in_flight: usize,
}

impl ProviderPollRuntimeProfile {
    pub(crate) fn new(
        profile: ExecutorExecutionProfile,
    ) -> Result<Self, ProviderPollRuntimeProfileError> {
        let max_in_flight = usize::try_from(profile.max_concurrency)
            .map_err(|_| ProviderPollRuntimeProfileError)?;
        if profile.execution_profile_id.is_nil()
            || profile.provider_account_id.is_nil()
            || profile.credential_pool_id.is_nil()
            || profile.resource_policy_id.is_nil()
            || profile.completion_mode != "remote_task"
            || profile.credential_revision <= 0
            || profile.resource_policy_revision <= 0
            || !(1..=MAX_PROVIDER_POLL_LANES).contains(&max_in_flight)
            || !valid_simple_identifier(&profile.profile_key)
            || !valid_simple_identifier(&profile.provider_id)
            || !valid_simple_identifier(&profile.command_schema)
            || !valid_text(&profile.operation_id, 128)
            || !valid_text(&profile.operation_descriptor_revision, 255)
            || !valid_sha256(&profile.operation_descriptor_sha256_v1)
            || !valid_simple_identifier(&profile.idempotency_mode)
            || !valid_simple_identifier(&profile.adapter_revision)
            || !valid_text(&profile.credential_ref, 1_024)
            || !valid_sha256(&profile.credential_auth_sha256)
        {
            return Err(ProviderPollRuntimeProfileError);
        }
        Ok(Self {
            profile,
            max_in_flight,
        })
    }

    pub fn execution_profile_id(&self) -> Uuid {
        self.profile.execution_profile_id
    }

    pub fn profile_key(&self) -> &str {
        &self.profile.profile_key
    }

    pub fn provider_id(&self) -> &str {
        &self.profile.provider_id
    }

    pub fn command_schema(&self) -> &str {
        &self.profile.command_schema
    }

    pub fn operation_id(&self) -> &str {
        &self.profile.operation_id
    }

    pub fn operation_descriptor_revision(&self) -> &str {
        &self.profile.operation_descriptor_revision
    }

    pub fn operation_descriptor_sha256_v1(&self) -> &str {
        &self.profile.operation_descriptor_sha256_v1
    }

    pub fn idempotency_mode(&self) -> &str {
        &self.profile.idempotency_mode
    }

    pub fn adapter_revision(&self) -> &str {
        &self.profile.adapter_revision
    }

    pub fn credential_pool_id(&self) -> Uuid {
        self.profile.credential_pool_id
    }

    pub fn provider_account_id(&self) -> Uuid {
        self.profile.provider_account_id
    }

    pub fn credential_ref(&self) -> &str {
        &self.profile.credential_ref
    }

    pub fn credential_revision(&self) -> i64 {
        self.profile.credential_revision
    }

    pub fn credential_auth_sha256(&self) -> &str {
        &self.profile.credential_auth_sha256
    }

    pub fn resource_policy_id(&self) -> Uuid {
        self.profile.resource_policy_id
    }

    pub fn resource_policy_revision(&self) -> i64 {
        self.profile.resource_policy_revision
    }

    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    pub fn claim_scope(&self) -> ProviderTaskClaimScope {
        ProviderTaskClaimScope {
            provider_id: self.profile.provider_id.clone(),
            provider_account_id: self.profile.provider_account_id,
        }
    }
}

impl fmt::Debug for ProviderPollRuntimeProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderPollRuntimeProfile")
            .field("execution_profile_id", &self.profile.execution_profile_id)
            .field("profile_key", &self.profile.profile_key)
            .field("provider_id", &self.profile.provider_id)
            .field("command_schema", &self.profile.command_schema)
            .field("operation_id", &self.profile.operation_id)
            .field(
                "operation_descriptor_revision",
                &self.profile.operation_descriptor_revision,
            )
            .field(
                "operation_descriptor_sha256_v1",
                &self.profile.operation_descriptor_sha256_v1,
            )
            .field("idempotency_mode", &self.profile.idempotency_mode)
            .field("adapter_revision", &self.profile.adapter_revision)
            .field("credential_pool_id", &self.profile.credential_pool_id)
            .field("provider_account_id", &self.profile.provider_account_id)
            .field("credential_ref", &"[redacted]")
            .field("credential_revision", &self.profile.credential_revision)
            .field("credential_auth_sha256", &"[redacted]")
            .field("resource_policy_id", &self.profile.resource_policy_id)
            .field(
                "resource_policy_revision",
                &self.profile.resource_policy_revision,
            )
            .field("max_in_flight", &self.max_in_flight)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("provider poll runtime profile is invalid")]
pub(crate) struct ProviderPollRuntimeProfileError;

fn valid_simple_identifier(value: &str) -> bool {
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
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests;
