#![forbid(unsafe_code)]

mod capabilities;
mod command;
mod policy;
mod receipt;
mod request;
mod xai;
mod xai_video;

pub use capabilities::{
    GROK_IMAGE_EDIT_OPERATION_V1, GROK_IMAGE_GENERATION_OPERATION_V1,
    GROK_VIDEO_GENERATION_OPERATION_V1,
};
pub use command::{
    GROK_IMAGE_EDIT_COMMAND_SCHEMA, GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
    GROK_VIDEO_GENERATION_COMMAND_SCHEMA, GrokCommandError, GrokImageEditPayloadV1,
    GrokImageGenerationPayloadV1, GrokVideoGenerationPayloadV1, MAX_CANONICAL_COMMAND_BYTES,
    parse_image_edit_command, parse_image_edit_payload, parse_image_generation_command,
    parse_image_generation_payload, parse_video_generation_command, parse_video_generation_payload,
};
pub use policy::{
    GrokCliPolicyError, GrokCliPolicyV1, GrokCliRequestV1, GrokExpectedToolCallV1,
    GrokInvocationV1, GrokTool,
};
pub use receipt::{
    GrokCliReceiptV1, GrokReceiptError, MAX_HISTORY_BYTES, MAX_STDOUT_BYTES,
    parse_invocation_receipt,
};
pub use request::{
    GrokImageEditRequestV1, GrokImageGenerationRequestV1, GrokVideoGenerationRequestV1,
    ImageAspectRatio, ImageModel, ImageToVideoRequestV1, MAX_IMAGE_EDIT_REFERENCES,
    ReferenceToVideoRequestV1, RequestValidationError, StagedImageV1, TextToVideoRequestV1,
    VideoAspectRatio, VideoDuration, VideoResolution,
};
pub use xai::{
    GROK_CLI_IMAGE_MAX_OUTPUTS, GROK_CLI_IMAGE_RESOLUTION, GROK_CLI_IMAGE_RESPONSE_FORMAT,
    GrokImageGenerationProjectionV1, XaiGrokProjectionError, project_xai_image_generation,
};
pub use xai_video::{
    GrokVideoGenerationProjectionV1, XaiGrokVideoProjectionError, project_xai_video_generation,
};

pub const PROVIDER_ID: &str = "grok-cli";
pub const GROK_CLI_COMPATIBILITY_VERSION: &str = "1.0.5";
pub const ADAPTER_REVISION: &str = "grok-cli-1.0.5.agentic-media.v2";
pub const VIDEO_ADAPTER_REVISION: &str = "grok-api-1.0.5.direct-image-video.v5";
pub const REQUEST_SCHEMA_VERSION: u16 = 1;

#[cfg(test)]
mod tests;
