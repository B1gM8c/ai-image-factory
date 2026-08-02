use crate::{MediaKind, MediaOperation, OfficialParamsContract, StreamingMode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpatialEditMode {
    NativeMask,
    SemanticMask,
    VisualRegion,
    Unsupported,
}

impl SpatialEditMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NativeMask => "native_mask",
            Self::SemanticMask => "semantic_mask",
            Self::VisualRegion => "visual_region",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallbackMode {
    Unsupported,
    WakeupHint,
}

impl CallbackMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::WakeupHint => "wakeup_hint",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationMode {
    Unsupported,
    ProviderConfirmed,
}

impl CancellationMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::ProviderConfirmed => "provider_confirmed",
        }
    }
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

impl CompletionMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::RemoteTask(_) => "remote_task",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactDelivery {
    InlineBounded { max_bytes: u64 },
    Streamed,
}

impl ArtifactDelivery {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InlineBounded { .. } => "inline_bounded",
            Self::Streamed => "streamed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdempotencyMode {
    /// The platform binds one immutable command to one provider submission.
    SubmissionBound,
    /// The provider accepts and enforces a client supplied idempotency token.
    ProviderToken,
}

impl IdempotencyMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SubmissionBound => "submission_bound",
            Self::ProviderToken => "provider_token",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BillingMetric {
    Output,
    Request,
    VideoSecond,
}

impl BillingMetric {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Output => "output",
            Self::Request => "request",
            Self::VideoSecond => "video_second",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputCardinality {
    ExactlyOne,
}

impl OutputCardinality {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExactlyOne => "exactly_one",
        }
    }
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
    pub spatial_edit_mode: SpatialEditMode,
    pub official_params: OfficialParamsContract,
}

impl OperationDescriptor {
    pub fn canonical_sha256_v1(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"ai-image-factory/provider-operation-descriptor/v1\0");
        digest_field(&mut digest, b"id", self.id.as_bytes());
        digest_field(
            &mut digest,
            b"descriptor_revision",
            self.descriptor_revision.as_bytes(),
        );
        digest_field(
            &mut digest,
            b"command_schema",
            self.command_schema.as_bytes(),
        );
        digest_field(&mut digest, b"output_schema", self.output_schema.as_bytes());
        digest_field(&mut digest, b"media", self.media.as_str().as_bytes());
        digest_field(
            &mut digest,
            b"operation",
            self.operation.as_str().as_bytes(),
        );
        digest_field(
            &mut digest,
            b"completion",
            self.completion.as_str().as_bytes(),
        );
        let (callback, cancellation) = match self.completion {
            CompletionMode::Inline => ("none", "none"),
            CompletionMode::RemoteTask(controls) => {
                (controls.callback.as_str(), controls.cancellation.as_str())
            }
        };
        digest_field(&mut digest, b"callback", callback.as_bytes());
        digest_field(&mut digest, b"cancellation", cancellation.as_bytes());
        digest_field(
            &mut digest,
            b"artifact_delivery",
            self.artifact_delivery.as_str().as_bytes(),
        );
        let artifact_limit = match self.artifact_delivery {
            ArtifactDelivery::InlineBounded { max_bytes } => max_bytes,
            ArtifactDelivery::Streamed => 0,
        };
        digest_field(
            &mut digest,
            b"artifact_max_bytes",
            &artifact_limit.to_be_bytes(),
        );
        digest_field(
            &mut digest,
            b"client_streaming",
            self.client_streaming.as_str().as_bytes(),
        );
        digest_field(
            &mut digest,
            b"idempotency",
            self.idempotency.as_str().as_bytes(),
        );
        digest_field(
            &mut digest,
            b"billing_metric",
            self.billing_metric.as_str().as_bytes(),
        );
        digest_field(
            &mut digest,
            b"output_cardinality",
            self.output_cardinality.as_str().as_bytes(),
        );
        digest_field(
            &mut digest,
            b"official_params_kind",
            self.official_params.kind.as_str().as_bytes(),
        );
        digest_field(
            &mut digest,
            b"official_params_schema",
            self.official_params.schema_id.as_bytes(),
        );
        digest_field(
            &mut digest,
            b"official_params_passthrough",
            &[u8::from(self.official_params.passthrough_allowed)],
        );
        digest.finalize().into()
    }

    pub fn canonical_sha256_v1_hex(&self) -> String {
        self.canonical_sha256_v1()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

fn digest_field(digest: &mut Sha256, name: &[u8], value: &[u8]) {
    digest.update((name.len() as u64).to_be_bytes());
    digest.update(name);
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

pub type ProviderCapabilities = &'static [OperationDescriptor];
