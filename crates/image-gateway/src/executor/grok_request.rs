use image_provider_grok_cli::{
    ADAPTER_REVISION, GROK_IMAGE_EDIT_COMMAND_SCHEMA, GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
    GROK_VIDEO_GENERATION_COMMAND_SCHEMA, GrokCliRequestV1, GrokImageEditRequestV1,
    GrokImageGenerationRequestV1, GrokVideoGenerationRequestV1, PROVIDER_ID,
    VIDEO_ADAPTER_REVISION, parse_image_edit_command, parse_image_generation_command,
    parse_video_generation_command,
};
use sha2::{Digest, Sha256};

pub use image_api_contracts::xai::XAI_IMAGES_API_PROFILE;

use super::{ExecutorLaunchContext, ExecutorSubmissionLease};

pub const XAI_VIDEOS_API_PROFILE: &str = "xai-videos-v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrokExecutionRequest {
    ImageGeneration(GrokImageGenerationRequestV1),
    ImageEdit(GrokImageEditRequestV1),
    VideoGeneration(GrokVideoGenerationRequestV1),
}

impl GrokExecutionRequest {
    pub fn into_cli_request(self) -> GrokCliRequestV1 {
        match self {
            Self::ImageGeneration(request) => request.into(),
            Self::ImageEdit(request) => request.into(),
            Self::VideoGeneration(request) => request.into(),
        }
    }

    pub(super) fn model(&self) -> &'static str {
        match self {
            Self::ImageGeneration(request) => request.model().as_str(),
            Self::ImageEdit(_) => "grok-imagine-image-quality",
            Self::VideoGeneration(GrokVideoGenerationRequestV1::TextToVideo(_)) => {
                "grok-imagine-video-1.5-preview"
            }
            Self::VideoGeneration(GrokVideoGenerationRequestV1::ImageToVideo(_)) => {
                "grok-imagine-video-1.5-preview"
            }
            Self::VideoGeneration(GrokVideoGenerationRequestV1::ReferenceToVideo(_)) => {
                "grok-imagine-video"
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GrokRequestProjectionError {
    #[error("executor launch context does not match its Grok lease")]
    ContextMismatch,
    #[error("Grok command failed integrity validation")]
    InvalidCommand,
    #[error("Grok command is unsupported by the selected CLI binding")]
    UnsupportedCommand,
    #[error("Grok CLI media commands produce exactly one output")]
    OutputOutOfRange,
}

pub fn project_grok_execution_request(
    lease: &ExecutorSubmissionLease,
    context: &ExecutorLaunchContext,
) -> Result<GrokExecutionRequest, GrokRequestProjectionError> {
    if lease.provider_id != PROVIDER_ID
        || Some(lease.adapter_revision.as_str())
            != expected_grok_adapter_revision(lease.command_schema.as_str())
        || lease.output_index != context.output_index()
        || lease.command_schema != context.command_schema()
        || lease.command_hash != context.command_hash()
    {
        return Err(GrokRequestProjectionError::ContextMismatch);
    }
    if lease.output_index != 0 {
        return Err(GrokRequestProjectionError::OutputOutOfRange);
    }

    let command_bytes = serde_json::to_vec(context.command_json())
        .map_err(|_| GrokRequestProjectionError::InvalidCommand)?;
    if hex::encode(Sha256::digest(&command_bytes)) != lease.command_hash {
        return Err(GrokRequestProjectionError::InvalidCommand);
    }

    let request = match lease.command_schema.as_str() {
        GROK_IMAGE_GENERATION_COMMAND_SCHEMA => {
            require_api_profile(context, XAI_IMAGES_API_PROFILE)?;
            GrokExecutionRequest::ImageGeneration(
                parse_image_generation_command(&command_bytes)
                    .map_err(|_| GrokRequestProjectionError::InvalidCommand)?,
            )
        }
        GROK_IMAGE_EDIT_COMMAND_SCHEMA => {
            require_api_profile(context, XAI_IMAGES_API_PROFILE)?;
            GrokExecutionRequest::ImageEdit(
                parse_image_edit_command(&command_bytes)
                    .map_err(|_| GrokRequestProjectionError::InvalidCommand)?,
            )
        }
        GROK_VIDEO_GENERATION_COMMAND_SCHEMA => {
            require_api_profile(context, XAI_VIDEOS_API_PROFILE)?;
            GrokExecutionRequest::VideoGeneration(
                parse_video_generation_command(&command_bytes)
                    .map_err(|_| GrokRequestProjectionError::InvalidCommand)?,
            )
        }
        _ => return Err(GrokRequestProjectionError::UnsupportedCommand),
    };
    if lease.model != request.model() {
        return Err(GrokRequestProjectionError::UnsupportedCommand);
    }
    Ok(request)
}

pub(super) fn expected_grok_adapter_revision(command_schema: &str) -> Option<&'static str> {
    match command_schema {
        GROK_IMAGE_GENERATION_COMMAND_SCHEMA | GROK_IMAGE_EDIT_COMMAND_SCHEMA => {
            Some(ADAPTER_REVISION)
        }
        GROK_VIDEO_GENERATION_COMMAND_SCHEMA => Some(VIDEO_ADAPTER_REVISION),
        _ => None,
    }
}

fn require_api_profile(
    context: &ExecutorLaunchContext,
    expected: &str,
) -> Result<(), GrokRequestProjectionError> {
    if context.api_profile() == expected {
        Ok(())
    } else {
        Err(GrokRequestProjectionError::UnsupportedCommand)
    }
}

#[cfg(test)]
mod tests {
    use image_api_contracts::xai::{
        XaiImageAspectRatio, XaiImageGenerationCommandV1, XaiImageGenerationRequest,
        XaiImageResolution, XaiImageResponseFormat,
    };
    use image_provider_grok_cli::{GrokImageGenerationPayloadV1, ImageModel};
    use image_provider_sdk::{CanonicalCommandPayload, OutputSlot};
    use serde_json::Value;
    use uuid::Uuid;

    use super::*;

    fn source_command_for_model(model: &str) -> Value {
        let source = XaiImageGenerationCommandV1::from_request(XaiImageGenerationRequest {
            aspect_ratio: Some(XaiImageAspectRatio::R1x1),
            model: Some(model.to_owned()),
            n: Some(1),
            prompt: "draw a lighthouse".to_owned(),
            resolution: Some(XaiImageResolution::R1k),
            response_format: Some(XaiImageResponseFormat::B64Json),
            storage_options: None,
            user: None,
        })
        .unwrap();
        let payload = GrokImageGenerationPayloadV1::from_xai_command(source).unwrap();
        let bytes = payload.into_canonical_bytes(OutputSlot::new(0, 1).unwrap());
        serde_json::from_slice(&bytes).unwrap()
    }

    fn lease_and_context_for_model(
        model: &str,
    ) -> (ExecutorSubmissionLease, ExecutorLaunchContext) {
        let command_json = source_command_for_model(model);
        let command_hash = hex::encode(Sha256::digest(serde_json::to_vec(&command_json).unwrap()));
        let lease = ExecutorSubmissionLease {
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            output_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            tenant_id: "tenant-1".to_owned(),
            provider_id: PROVIDER_ID.to_owned(),
            model: model.to_owned(),
            work_item_id: Uuid::new_v4(),
            output_index: 0,
            command_schema: GROK_IMAGE_GENERATION_COMMAND_SCHEMA.to_owned(),
            command_hash: command_hash.clone(),
            execution_profile_id: Uuid::new_v4(),
            adapter_revision: ADAPTER_REVISION.to_owned(),
            executor_owner: "executor-1".to_owned(),
            executor_lease_epoch: 1,
            executor_lease_expires_at_ms: i64::MAX,
        };
        let context = ExecutorLaunchContext {
            request_id: "request-1".to_owned(),
            api_profile: XAI_IMAGES_API_PROFILE.to_owned(),
            output_index: 0,
            command_schema: GROK_IMAGE_GENERATION_COMMAND_SCHEMA.to_owned(),
            command_hash,
            command_json,
            inputs: Vec::new(),
        };
        (lease, context)
    }

    fn lease_and_context() -> (ExecutorSubmissionLease, ExecutorLaunchContext) {
        lease_and_context_for_model("grok-imagine-image-quality")
    }

    #[test]
    fn projects_digest_bound_xai_image_generation() {
        let (lease, context) = lease_and_context();
        let request = project_grok_execution_request(&lease, &context).unwrap();

        let GrokExecutionRequest::ImageGeneration(request) = request else {
            panic!("expected image generation request");
        };
        assert_eq!(request.prompt(), "draw a lighthouse");
        assert_eq!(request.model(), ImageModel::Quality);
    }

    #[test]
    fn projects_the_base_image_model_without_remapping_it() {
        let (lease, context) = lease_and_context_for_model("grok-imagine-image");
        let request = project_grok_execution_request(&lease, &context).unwrap();

        let GrokExecutionRequest::ImageGeneration(request) = request else {
            panic!("expected image generation request");
        };
        assert_eq!(request.model(), ImageModel::Base);
    }

    #[test]
    fn rejects_tampered_payload_even_when_context_and_lease_hash_match_each_other() {
        let (mut lease, mut context) = lease_and_context();
        context.command_json["prompt"] = Value::String("tampered".to_owned());
        lease.command_hash = context.command_hash.clone();

        assert_eq!(
            project_grok_execution_request(&lease, &context),
            Err(GrokRequestProjectionError::InvalidCommand)
        );
    }

    #[test]
    fn rejects_wrong_facade_model_and_output_slot() {
        let (mut lease, context) = lease_and_context();
        lease.model = "grok-imagine-image".to_owned();
        assert_eq!(
            project_grok_execution_request(&lease, &context),
            Err(GrokRequestProjectionError::UnsupportedCommand)
        );

        let (mut lease, mut context) = lease_and_context();
        lease.output_index = 1;
        context.output_index = 1;
        assert_eq!(
            project_grok_execution_request(&lease, &context),
            Err(GrokRequestProjectionError::OutputOutOfRange)
        );
    }
}
