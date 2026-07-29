use image_api_contracts::xai::{
    XAI_IMAGE_GENERATION_COMMAND_SCHEMA, XAI_VIDEO_GENERATION_COMMAND_SCHEMA,
};
use image_provider_contracts::{
    ArtifactDelivery, BillingMetric, CompletionMode, IdempotencyMode, MediaKind, MediaOperation,
    OfficialParamsContract, OfficialParamsKind, OperationDescriptor, OutputCardinality,
    StreamingMode,
};

pub const GROK_IMAGE_GENERATION_OPERATION_V1: OperationDescriptor = OperationDescriptor {
    id: "images.generations",
    descriptor_revision: "grok-cli/images.generations/v1",
    command_schema: crate::GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
    output_schema: "factory.provider-artifact.image.v1",
    media: MediaKind::Image,
    operation: MediaOperation::Generation,
    completion: CompletionMode::Inline,
    artifact_delivery: ArtifactDelivery::InlineBounded {
        max_bytes: 32 * 1024 * 1024,
    },
    client_streaming: StreamingMode::None,
    idempotency: IdempotencyMode::SubmissionBound,
    billing_metric: BillingMetric::Output,
    output_cardinality: OutputCardinality::ExactlyOne,
    official_params: OfficialParamsContract {
        kind: OfficialParamsKind::XaiImage,
        schema_id: XAI_IMAGE_GENERATION_COMMAND_SCHEMA,
        passthrough_allowed: false,
    },
};

pub const GROK_IMAGE_EDIT_OPERATION_V1: OperationDescriptor = OperationDescriptor {
    id: "images.edits",
    descriptor_revision: "grok-cli/images.edits/v1",
    command_schema: crate::GROK_IMAGE_EDIT_COMMAND_SCHEMA,
    output_schema: "factory.provider-artifact.image.v1",
    media: MediaKind::Image,
    operation: MediaOperation::Edit,
    completion: CompletionMode::Inline,
    artifact_delivery: ArtifactDelivery::InlineBounded {
        max_bytes: 32 * 1024 * 1024,
    },
    client_streaming: StreamingMode::None,
    idempotency: IdempotencyMode::SubmissionBound,
    billing_metric: BillingMetric::Output,
    output_cardinality: OutputCardinality::ExactlyOne,
    official_params: OfficialParamsContract {
        kind: OfficialParamsKind::XaiImage,
        schema_id: "xai.images.edits/grok-cli-subset-v1",
        passthrough_allowed: false,
    },
};

pub const GROK_VIDEO_GENERATION_OPERATION_V1: OperationDescriptor = OperationDescriptor {
    id: "videos.generations",
    descriptor_revision: "grok-cli/videos.generations/v1",
    command_schema: crate::GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
    output_schema: "factory.provider-artifact.video.v1",
    media: MediaKind::Video,
    operation: MediaOperation::Generation,
    // The CLI polls xAI internally and returns only after the MP4 is local.
    completion: CompletionMode::Inline,
    artifact_delivery: ArtifactDelivery::InlineBounded {
        max_bytes: 256 * 1024 * 1024,
    },
    client_streaming: StreamingMode::None,
    idempotency: IdempotencyMode::SubmissionBound,
    billing_metric: BillingMetric::VideoSecond,
    output_cardinality: OutputCardinality::ExactlyOne,
    official_params: OfficialParamsContract {
        kind: OfficialParamsKind::XaiVideo,
        schema_id: XAI_VIDEO_GENERATION_COMMAND_SCHEMA,
        passthrough_allowed: false,
    },
};
