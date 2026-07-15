use image_provider_contracts::openai_codex;
use serde::{Deserialize, Serialize};

use super::{ExecutorLaunchContext, ExecutorSubmissionLease};
use crate::admission::{
    GENERATION_COMMAND_SCHEMA, GENERATION_COMMAND_SCHEMA_VERSION, GENERATION_OPERATION,
    GenerationCommandV1,
};
use crate::size::parse_size_constraint;

const OPENAI_IMAGES_API_PROFILE: &str = "openai-images-v1";

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CodexOutputRequest {
    pub request_id: String,
    pub model: String,
    pub prompt: String,
    pub moderation: String,
    pub original_n: u32,
    pub candidate_index: u32,
    pub size: String,
    pub quality: String,
    pub output_format: String,
    pub output_compression: Option<u8>,
    pub background: String,
    pub stream: bool,
    pub partial_images: u32,
}

impl CodexOutputRequest {
    pub(crate) fn validate(&self) -> Result<(), CodexRequestProjectionError> {
        if self.request_id.is_empty()
            || self.request_id.len() > 1_024
            || self.request_id.chars().any(char::is_control)
            || !openai_codex::is_supported_model(&self.model)
            || self.prompt.trim().is_empty()
            || self.prompt.chars().count() > 32_000
            || self.moderation != "auto"
            || !(1..=10).contains(&self.original_n)
            || !(1..=self.original_n).contains(&self.candidate_index)
            || parse_size_constraint(&self.size).is_none()
            || !matches!(self.quality.as_str(), "auto" | "low" | "medium" | "high")
            || !matches!(self.output_format.as_str(), "png" | "jpeg" | "webp")
            || (self.output_format == "png" && self.output_compression.is_some())
            || !matches!(self.background.as_str(), "auto" | "opaque")
            || self.partial_images != 0
        {
            return Err(CodexRequestProjectionError::UnsupportedCommand);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CodexRequestProjectionError {
    #[error("executor launch context does not match its lease")]
    ContextMismatch,
    #[error("generation command failed integrity validation")]
    InvalidCommand,
    #[error("command is not supported by the Codex generation adapter")]
    UnsupportedCommand,
    #[error("executor output index is outside the command output range")]
    OutputOutOfRange,
}

pub fn project_codex_output_request(
    lease: &ExecutorSubmissionLease,
    context: &ExecutorLaunchContext,
) -> Result<CodexOutputRequest, CodexRequestProjectionError> {
    validate_codex_generation_contract()?;
    if lease.output_index != context.output_index()
        || lease.command_schema != context.command_schema()
        || lease.command_hash != context.command_hash()
    {
        return Err(CodexRequestProjectionError::ContextMismatch);
    }
    if lease.command_schema != GENERATION_COMMAND_SCHEMA {
        return Err(CodexRequestProjectionError::UnsupportedCommand);
    }

    let command: GenerationCommandV1 = serde_json::from_value(context.command_json().clone())
        .map_err(|_| CodexRequestProjectionError::InvalidCommand)?;
    if command.request_hash_hex() != lease.command_hash {
        return Err(CodexRequestProjectionError::InvalidCommand);
    }
    if command.schema_version != GENERATION_COMMAND_SCHEMA_VERSION
        || command.operation != GENERATION_OPERATION
        || command.provider_id != openai_codex::PROVIDER_ID
        || lease.provider_id != command.provider_id
        || !openai_codex::is_supported_model(&command.model)
        || lease.model != command.model
        || command.source_api_profile != OPENAI_IMAGES_API_PROFILE
        || context.api_profile() != command.source_api_profile
    {
        return Err(CodexRequestProjectionError::UnsupportedCommand);
    }

    let output_index = u32::try_from(lease.output_index)
        .map_err(|_| CodexRequestProjectionError::OutputOutOfRange)?;
    if output_index >= command.n {
        return Err(CodexRequestProjectionError::OutputOutOfRange);
    }

    let request = CodexOutputRequest {
        request_id: context.request_id().to_string(),
        model: command.model,
        prompt: command.prompt,
        moderation: command.moderation.unwrap_or_else(|| "auto".to_string()),
        original_n: command.n,
        candidate_index: output_index + 1,
        size: command.size,
        quality: command.quality,
        output_format: command.output_format,
        output_compression: command.output_compression,
        background: command.background,
        stream: command.stream,
        partial_images: command.partial_images,
    };
    request.validate()?;
    Ok(request)
}

fn validate_codex_generation_contract() -> Result<(), CodexRequestProjectionError> {
    use image_provider_contracts::{CompletionMode, OutputCardinality};

    openai_codex::DEFINITION
        .validate()
        .map_err(|_| CodexRequestProjectionError::UnsupportedCommand)?;
    let descriptor = openai_codex::operation("images.generations")
        .ok_or(CodexRequestProjectionError::UnsupportedCommand)?;
    if descriptor.command_schema != GENERATION_COMMAND_SCHEMA
        || descriptor.completion != CompletionMode::Inline
        || descriptor.output_cardinality != OutputCardinality::ExactlyOne
    {
        return Err(CodexRequestProjectionError::UnsupportedCommand);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::*;

    fn command(n: u32) -> GenerationCommandV1 {
        serde_json::from_value(json!({
            "background": "auto",
            "model": openai_codex::MODEL_GPT_IMAGE_2,
            "n": n,
            "operation": GENERATION_OPERATION,
            "output_compression": null,
            "output_format": "png",
            "partial_images": 0,
            "prompt": "a red fox",
            "provider_id": openai_codex::PROVIDER_ID,
            "quality": "high",
            "schema_version": GENERATION_COMMAND_SCHEMA_VERSION,
            "size": "1024x1024",
            "source_api_profile": OPENAI_IMAGES_API_PROFILE,
            "stream": false
        }))
        .unwrap()
    }

    #[test]
    fn codex_generation_descriptor_matches_the_durable_gateway_contract() {
        let descriptor = openai_codex::operation("images.generations").unwrap();

        assert_eq!(descriptor.command_schema, GENERATION_COMMAND_SCHEMA);
        assert_eq!(
            descriptor.output_schema,
            "factory.openai-compatible.images.response.v1"
        );
        assert_eq!(
            descriptor.completion,
            image_provider_contracts::CompletionMode::Inline
        );
        assert_eq!(
            descriptor.output_cardinality,
            image_provider_contracts::OutputCardinality::ExactlyOne
        );
    }

    fn lease(output_index: i32, command: &GenerationCommandV1) -> ExecutorSubmissionLease {
        ExecutorSubmissionLease {
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            output_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            tenant_id: "tenant-1".to_string(),
            provider_id: command.provider_id.clone(),
            model: command.model.clone(),
            work_item_id: Uuid::new_v4(),
            output_index,
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            command_hash: command.request_hash_hex(),
            execution_profile_id: Uuid::new_v4(),
            adapter_revision: crate::executor::CODEX_GENERATION_ADAPTER_REVISION.to_string(),
            executor_owner: "executor-owner-1".to_string(),
            executor_lease_epoch: 1,
            executor_lease_expires_at_ms: i64::MAX,
        }
    }

    fn context(output_index: i32, command: &GenerationCommandV1) -> ExecutorLaunchContext {
        ExecutorLaunchContext {
            request_id: "request-1".to_string(),
            api_profile: command.source_api_profile.clone(),
            output_index,
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            command_hash: command.request_hash_hex(),
            command_json: serde_json::to_value(command).unwrap(),
        }
    }

    fn set_command(
        lease: &mut ExecutorSubmissionLease,
        context: &mut ExecutorLaunchContext,
        command: &GenerationCommandV1,
    ) {
        let hash = command.request_hash_hex();
        lease.command_hash.clone_from(&hash);
        context.command_hash = hash;
        context.command_json = serde_json::to_value(command).unwrap();
    }

    #[test]
    fn two_output_command_projects_one_cli_request_per_output() {
        let command = command(2);

        let first =
            project_codex_output_request(&lease(0, &command), &context(0, &command)).unwrap();
        let second =
            project_codex_output_request(&lease(1, &command), &context(1, &command)).unwrap();

        assert_eq!(first.original_n, 2);
        assert_eq!(first.candidate_index, 1);
        assert_eq!(first.request_id, "request-1");
        assert_eq!(first.model, openai_codex::MODEL_GPT_IMAGE_2);
        assert_eq!(first.prompt, "a red fox");
        assert_eq!(first.output_format, "png");
        assert_eq!(second.original_n, 2);
        assert_eq!(second.candidate_index, 2);
        assert_eq!(second.prompt, "a red fox");
    }

    #[test]
    fn negative_and_past_end_output_indexes_are_rejected() {
        let command = command(2);

        for output_index in [-1, 2] {
            assert_eq!(
                project_codex_output_request(
                    &lease(output_index, &command),
                    &context(output_index, &command),
                )
                .err(),
                Some(CodexRequestProjectionError::OutputOutOfRange)
            );
        }
    }

    #[test]
    fn lease_and_context_output_indexes_must_match() {
        let command = command(2);

        assert_eq!(
            project_codex_output_request(&lease(0, &command), &context(1, &command)).err(),
            Some(CodexRequestProjectionError::ContextMismatch)
        );
    }

    #[test]
    fn tampered_command_json_is_rejected() {
        let command = command(1);
        let lease = lease(0, &command);
        let mut context = context(0, &command);
        context.command_json["prompt"] = Value::String("tampered".to_string());

        assert_eq!(
            project_codex_output_request(&lease, &context).err(),
            Some(CodexRequestProjectionError::InvalidCommand)
        );
    }

    #[test]
    fn unknown_command_field_is_rejected_even_when_raw_json_hash_agrees() {
        let command = command(1);
        let mut lease = lease(0, &command);
        let mut context = context(0, &command);
        context.command_json["unknown"] = Value::Bool(true);
        let raw_hash = hex::encode(Sha256::digest(
            serde_json::to_vec(&context.command_json).unwrap(),
        ));
        lease.command_hash.clone_from(&raw_hash);
        context.command_hash = raw_hash;

        assert_eq!(
            project_codex_output_request(&lease, &context).err(),
            Some(CodexRequestProjectionError::InvalidCommand)
        );
    }

    #[test]
    fn mismatched_hash_is_rejected() {
        let command = command(1);
        let lease = lease(0, &command);
        let mut context = context(0, &command);
        context.command_hash = "f".repeat(64);

        assert_eq!(
            project_codex_output_request(&lease, &context).err(),
            Some(CodexRequestProjectionError::ContextMismatch)
        );
    }

    #[test]
    fn wrong_command_schema_is_rejected() {
        let command = command(1);
        let mut lease = lease(0, &command);
        let mut context = context(0, &command);
        lease.command_schema = "openai.images.edit.v1".to_string();
        context.command_schema.clone_from(&lease.command_schema);

        assert_eq!(
            project_codex_output_request(&lease, &context).err(),
            Some(CodexRequestProjectionError::UnsupportedCommand)
        );
    }

    #[test]
    fn command_version_and_operation_are_rejected_after_rehash() {
        let base = command(1);
        for changed in [
            GenerationCommandV1 {
                schema_version: GENERATION_COMMAND_SCHEMA_VERSION + 1,
                ..base.clone()
            },
            GenerationCommandV1 {
                operation: "edit".to_string(),
                ..base.clone()
            },
        ] {
            let mut lease = lease(0, &base);
            let mut context = context(0, &base);
            set_command(&mut lease, &mut context, &changed);

            assert_eq!(
                project_codex_output_request(&lease, &context).err(),
                Some(CodexRequestProjectionError::UnsupportedCommand)
            );
        }
    }

    #[test]
    fn non_codex_provider_is_rejected_even_when_lease_and_hash_agree() {
        let mut command = command(1);
        command.provider_id = "other-provider".to_string();
        let lease = lease(0, &command);
        let context = context(0, &command);

        assert_eq!(
            project_codex_output_request(&lease, &context).err(),
            Some(CodexRequestProjectionError::UnsupportedCommand)
        );
    }

    #[test]
    fn unsupported_model_is_rejected_even_when_lease_and_hash_agree() {
        let mut command = command(1);
        command.model = "other-model".to_string();
        let lease = lease(0, &command);
        let context = context(0, &command);

        assert_eq!(
            project_codex_output_request(&lease, &context).err(),
            Some(CodexRequestProjectionError::UnsupportedCommand)
        );
    }

    #[test]
    fn supported_command_model_must_still_match_lease() {
        let base = command(1);
        let mut changed = base.clone();
        changed.model = openai_codex::MODEL_GPT_IMAGE_2_SNAPSHOT.to_string();
        let mut lease = lease(0, &base);
        let mut context = context(0, &base);
        set_command(&mut lease, &mut context, &changed);

        assert_eq!(
            project_codex_output_request(&lease, &context).err(),
            Some(CodexRequestProjectionError::UnsupportedCommand)
        );
    }

    #[test]
    fn non_images_source_profile_is_rejected_after_rehash() {
        let mut command = command(1);
        command.source_api_profile = "other-profile".to_string();
        let lease = lease(0, &command);
        let context = context(0, &command);

        assert_eq!(
            project_codex_output_request(&lease, &context).err(),
            Some(CodexRequestProjectionError::UnsupportedCommand)
        );
    }

    #[test]
    fn command_source_profile_must_match_launch_context() {
        let command = command(1);
        let lease = lease(0, &command);
        let mut context = context(0, &command);
        context.api_profile = "other-profile".to_string();

        assert_eq!(
            project_codex_output_request(&lease, &context).err(),
            Some(CodexRequestProjectionError::UnsupportedCommand)
        );
    }
}
