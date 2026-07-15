use crate::{MediaKind, MediaOperation, OfficialParamsContract, StreamingMode};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallbackMode {
    Unsupported,
    WakeupHint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationMode {
    Unsupported,
    ProviderConfirmed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteTaskControls {
    pub callback: CallbackMode,
    pub cancellation: CancellationMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionMode {
    Inline,
    RemoteTask(RemoteTaskControls),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactDelivery {
    InlineBounded { max_bytes: u64 },
    Streamed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdempotencyMode {
    /// The platform binds one immutable command to one provider submission.
    SubmissionBound,
    /// The provider accepts and enforces a client supplied idempotency token.
    ProviderToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BillingMetric {
    Output,
    Request,
    VideoSecond,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputCardinality {
    ExactlyOne,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationDescriptor {
    pub id: &'static str,
    pub descriptor_revision: &'static str,
    pub command_schema: &'static str,
    pub output_schema: &'static str,
    pub media: MediaKind,
    pub operation: MediaOperation,
    pub completion: CompletionMode,
    pub artifact_delivery: ArtifactDelivery,
    pub client_streaming: StreamingMode,
    pub idempotency: IdempotencyMode,
    pub billing_metric: BillingMetric,
    pub output_cardinality: OutputCardinality,
    pub official_params: OfficialParamsContract,
}

pub type ProviderCapabilities = &'static [OperationDescriptor];
