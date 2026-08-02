use image_provider_contracts::{
    ArtifactDelivery, BillingMetric, CallbackMode, CancellationMode, CompletionMode,
    IdempotencyMode, MediaKind, MediaOperation, OfficialParamsContract, OfficialParamsKind,
    OperationDescriptor, OutputCardinality, RemoteTaskControls, SpatialEditMode, StreamingMode,
};

pub const DREAMINA_CLI_REMOTE_TASK_CONTROLS_V1: RemoteTaskControls = RemoteTaskControls {
    callback: CallbackMode::Unsupported,
    cancellation: CancellationMode::Unsupported,
};

pub const DREAMINA_IMAGE_GENERATION_OPERATION_V1: OperationDescriptor = OperationDescriptor {
    id: "images.generations",
    descriptor_revision: "dreamina-cli/images.generations/v1",
    command_schema: crate::DREAMINA_SUBMIT_COMMAND_SCHEMA,
    output_schema: "factory.provider-artifact.image.v1",
    media: MediaKind::Image,
    operation: MediaOperation::Generation,
    completion: CompletionMode::RemoteTask(DREAMINA_CLI_REMOTE_TASK_CONTROLS_V1),
    artifact_delivery: ArtifactDelivery::Streamed,
    client_streaming: StreamingMode::None,
    idempotency: IdempotencyMode::SubmissionBound,
    billing_metric: BillingMetric::Output,
    output_cardinality: OutputCardinality::ExactlyOne,
    spatial_edit_mode: SpatialEditMode::Unsupported,
    official_params: OfficialParamsContract {
        kind: OfficialParamsKind::DreaminaCli,
        schema_id: "dreamina-cli/text2image-v1",
        passthrough_allowed: false,
    },
};

pub const DREAMINA_VIDEO_GENERATION_OPERATION_V1: OperationDescriptor = OperationDescriptor {
    id: "videos.generations",
    descriptor_revision: "dreamina-cli/videos.generations/v1",
    command_schema: crate::DREAMINA_SUBMIT_COMMAND_SCHEMA,
    output_schema: "factory.provider-artifact.video.v1",
    media: MediaKind::Video,
    operation: MediaOperation::Generation,
    completion: CompletionMode::RemoteTask(DREAMINA_CLI_REMOTE_TASK_CONTROLS_V1),
    artifact_delivery: ArtifactDelivery::Streamed,
    client_streaming: StreamingMode::None,
    idempotency: IdempotencyMode::SubmissionBound,
    billing_metric: BillingMetric::VideoSecond,
    output_cardinality: OutputCardinality::ExactlyOne,
    spatial_edit_mode: SpatialEditMode::Unsupported,
    official_params: OfficialParamsContract {
        kind: OfficialParamsKind::DreaminaCli,
        schema_id: "dreamina-cli/text2video-v1",
        passthrough_allowed: false,
    },
};
