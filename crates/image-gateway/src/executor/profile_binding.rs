use image_provider_contracts::openai_codex;
use image_provider_grok_cli::{
    ADAPTER_REVISION as GROK_ADAPTER_REVISION, GROK_IMAGE_EDIT_COMMAND_SCHEMA,
    GROK_IMAGE_EDIT_OPERATION_V1, GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
    GROK_IMAGE_GENERATION_OPERATION_V1, GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
    GROK_VIDEO_GENERATION_OPERATION_V1, PROVIDER_ID as GROK_PROVIDER_ID,
};
use thiserror::Error;

use super::{CODEX_GENERATION_ADAPTER_REVISION, ExecutorExecutionProfile};
use crate::admission::GENERATION_COMMAND_SCHEMA;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutorProfileBinding {
    CodexImageGeneration,
    GrokImageGeneration,
    GrokImageEdit,
    GrokVideoGeneration,
}

impl ExecutorProfileBinding {
    pub fn provider_executable_env(self) -> &'static str {
        match self {
            Self::CodexImageGeneration => "EXECUTOR_CODEX_EXECUTABLE",
            Self::GrokImageGeneration | Self::GrokImageEdit | Self::GrokVideoGeneration => {
                "EXECUTOR_GROK_EXECUTABLE"
            }
        }
    }

    pub fn credential_home_env(self) -> &'static str {
        match self {
            Self::CodexImageGeneration => "EXECUTOR_CODEX_CREDENTIAL_HOME",
            Self::GrokImageGeneration | Self::GrokImageEdit | Self::GrokVideoGeneration => {
                "EXECUTOR_GROK_CREDENTIAL_HOME"
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutorProfileBindingError {
    #[error("executor profile provider is unsupported")]
    UnsupportedProvider,
    #[error("executor profile immutable binding does not match the runtime adapter")]
    BindingMismatch,
}

pub fn identify_executor_profile_binding(
    profile: &ExecutorExecutionProfile,
) -> Result<ExecutorProfileBinding, ExecutorProfileBindingError> {
    match profile.provider_id.as_str() {
        openai_codex::PROVIDER_ID => {
            let operation = openai_codex::operation("images.generations")
                .ok_or(ExecutorProfileBindingError::BindingMismatch)?;
            validate(
                profile,
                GENERATION_COMMAND_SCHEMA,
                operation.id,
                operation.descriptor_revision,
                &operation.canonical_sha256_v1_hex(),
                operation.completion.as_str(),
                operation.idempotency.as_str(),
                CODEX_GENERATION_ADAPTER_REVISION,
            )?;
            Ok(ExecutorProfileBinding::CodexImageGeneration)
        }
        GROK_PROVIDER_ID => {
            let (command_schema, operation, binding) = match profile.command_schema.as_str() {
                GROK_IMAGE_GENERATION_COMMAND_SCHEMA => (
                    GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
                    GROK_IMAGE_GENERATION_OPERATION_V1,
                    ExecutorProfileBinding::GrokImageGeneration,
                ),
                GROK_IMAGE_EDIT_COMMAND_SCHEMA => (
                    GROK_IMAGE_EDIT_COMMAND_SCHEMA,
                    GROK_IMAGE_EDIT_OPERATION_V1,
                    ExecutorProfileBinding::GrokImageEdit,
                ),
                GROK_VIDEO_GENERATION_COMMAND_SCHEMA => (
                    GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
                    GROK_VIDEO_GENERATION_OPERATION_V1,
                    ExecutorProfileBinding::GrokVideoGeneration,
                ),
                _ => return Err(ExecutorProfileBindingError::BindingMismatch),
            };
            validate(
                profile,
                command_schema,
                operation.id,
                operation.descriptor_revision,
                &operation.canonical_sha256_v1_hex(),
                operation.completion.as_str(),
                operation.idempotency.as_str(),
                GROK_ADAPTER_REVISION,
            )?;
            Ok(binding)
        }
        _ => Err(ExecutorProfileBindingError::UnsupportedProvider),
    }
}

#[allow(clippy::too_many_arguments)]
fn validate(
    profile: &ExecutorExecutionProfile,
    command_schema: &str,
    operation_id: &str,
    descriptor_revision: &str,
    descriptor_sha256: &str,
    completion_mode: &str,
    idempotency_mode: &str,
    adapter_revision: &str,
) -> Result<(), ExecutorProfileBindingError> {
    if profile.command_schema == command_schema
        && profile.operation_id == operation_id
        && profile.operation_descriptor_revision == descriptor_revision
        && profile.operation_descriptor_sha256_v1 == descriptor_sha256
        && profile.completion_mode == completion_mode
        && profile.idempotency_mode == idempotency_mode
        && profile.adapter_revision == adapter_revision
    {
        Ok(())
    } else {
        Err(ExecutorProfileBindingError::BindingMismatch)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn grok_profile() -> ExecutorExecutionProfile {
        let operation = GROK_IMAGE_GENERATION_OPERATION_V1;
        ExecutorExecutionProfile {
            execution_profile_id: Uuid::new_v4(),
            profile_key: "grok-image".to_owned(),
            provider_id: GROK_PROVIDER_ID.to_owned(),
            command_schema: GROK_IMAGE_GENERATION_COMMAND_SCHEMA.to_owned(),
            operation_id: operation.id.to_owned(),
            operation_descriptor_revision: operation.descriptor_revision.to_owned(),
            operation_descriptor_sha256_v1: operation.canonical_sha256_v1_hex(),
            completion_mode: operation.completion.as_str().to_owned(),
            idempotency_mode: operation.idempotency.as_str().to_owned(),
            adapter_revision: GROK_ADAPTER_REVISION.to_owned(),
            credential_pool_id: Uuid::new_v4(),
            provider_account_id: Uuid::new_v4(),
            credential_ref: "grok-oauth".to_owned(),
            credential_revision: 1,
            credential_auth_sha256: "a".repeat(64),
            resource_policy_id: Uuid::new_v4(),
            resource_policy_revision: 1,
            max_concurrency: 1,
        }
    }

    #[test]
    fn grok_profile_requires_every_immutable_descriptor_field() {
        let profile = grok_profile();
        assert_eq!(
            identify_executor_profile_binding(&profile),
            Ok(ExecutorProfileBinding::GrokImageGeneration)
        );

        let mut drifted = profile;
        drifted.operation_descriptor_sha256_v1 = "f".repeat(64);
        assert_eq!(
            identify_executor_profile_binding(&drifted),
            Err(ExecutorProfileBindingError::BindingMismatch)
        );
    }

    #[test]
    fn grok_video_profile_is_distinct_from_image_generation() {
        let mut profile = grok_profile();
        let operation = GROK_VIDEO_GENERATION_OPERATION_V1;
        profile.profile_key = "grok-video".to_owned();
        profile.command_schema = GROK_VIDEO_GENERATION_COMMAND_SCHEMA.to_owned();
        profile.operation_id = operation.id.to_owned();
        profile.operation_descriptor_revision = operation.descriptor_revision.to_owned();
        profile.operation_descriptor_sha256_v1 = operation.canonical_sha256_v1_hex();
        profile.completion_mode = operation.completion.as_str().to_owned();
        profile.idempotency_mode = operation.idempotency.as_str().to_owned();

        assert_eq!(
            identify_executor_profile_binding(&profile),
            Ok(ExecutorProfileBinding::GrokVideoGeneration)
        );
    }

    #[test]
    fn grok_edit_profile_is_distinct_from_image_generation() {
        let mut profile = grok_profile();
        let operation = GROK_IMAGE_EDIT_OPERATION_V1;
        profile.profile_key = "grok-edit".to_owned();
        profile.command_schema = GROK_IMAGE_EDIT_COMMAND_SCHEMA.to_owned();
        profile.operation_id = operation.id.to_owned();
        profile.operation_descriptor_revision = operation.descriptor_revision.to_owned();
        profile.operation_descriptor_sha256_v1 = operation.canonical_sha256_v1_hex();
        profile.completion_mode = operation.completion.as_str().to_owned();
        profile.idempotency_mode = operation.idempotency.as_str().to_owned();

        assert_eq!(
            identify_executor_profile_binding(&profile),
            Ok(ExecutorProfileBinding::GrokImageEdit)
        );
    }
}
