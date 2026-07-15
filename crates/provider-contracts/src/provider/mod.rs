use crate::{OperationDescriptor, ProviderCapabilities};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderStatus {
    Active,
    Planned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderExecutionMode {
    NativeCli,
    ManagedApi,
    CliBridge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderDefinition {
    pub id: &'static str,
    pub display_name: &'static str,
    pub owner: &'static str,
    pub status: ProviderStatus,
    pub execution_mode: ProviderExecutionMode,
    pub models: &'static [&'static str],
    pub capabilities: ProviderCapabilities,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderDescriptorError {
    InvalidProviderIdentity,
    MissingModel,
    DuplicateModel,
    MissingOperation,
    InvalidOperation,
    DuplicateOperation,
}

impl ProviderDefinition {
    pub fn validate(&self) -> Result<(), ProviderDescriptorError> {
        if !valid_identifier(self.id) || self.display_name.is_empty() || self.owner.is_empty() {
            return Err(ProviderDescriptorError::InvalidProviderIdentity);
        }
        if self.models.is_empty() || self.models.iter().any(|model| !valid_text(model)) {
            return Err(ProviderDescriptorError::MissingModel);
        }
        if has_duplicate(self.models) {
            return Err(ProviderDescriptorError::DuplicateModel);
        }
        if self.capabilities.is_empty() {
            return Err(ProviderDescriptorError::MissingOperation);
        }
        for (index, operation) in self.capabilities.iter().enumerate() {
            if !valid_identifier(operation.id)
                || !valid_text(operation.descriptor_revision)
                || !valid_text(operation.command_schema)
                || !valid_text(operation.output_schema)
                || !valid_text(operation.official_params.schema_id)
                || matches!(
                    operation.artifact_delivery,
                    crate::ArtifactDelivery::InlineBounded { max_bytes: 0 }
                )
            {
                return Err(ProviderDescriptorError::InvalidOperation);
            }
            if self.capabilities[..index]
                .iter()
                .any(|candidate| candidate.id == operation.id)
            {
                return Err(ProviderDescriptorError::DuplicateOperation);
            }
        }
        Ok(())
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_text(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn has_duplicate(values: &[&str]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].contains(value))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderRoadmapMetadata {
    pub id: &'static str,
    pub display_name: &'static str,
    pub owner: &'static str,
    pub execution_mode: ProviderExecutionMode,
    pub candidate_models: &'static [&'static str],
    pub intended_scope: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderRoadmapEntry {
    Active(ProviderDefinition),
    Planned(ProviderRoadmapMetadata),
}

impl ProviderRoadmapEntry {
    pub const fn id(self) -> &'static str {
        match self {
            Self::Active(provider) => provider.id,
            Self::Planned(provider) => provider.id,
        }
    }

    pub const fn status(self) -> ProviderStatus {
        match self {
            Self::Active(_) => ProviderStatus::Active,
            Self::Planned(_) => ProviderStatus::Planned,
        }
    }
}

pub mod openai_codex {
    use super::{
        OperationDescriptor, ProviderCapabilities, ProviderDefinition, ProviderExecutionMode,
        ProviderStatus,
    };
    use crate::{
        ArtifactDelivery, BillingMetric, CompletionMode, IdempotencyMode, MediaKind,
        MediaOperation, OfficialParamsContract, OfficialParamsKind, OutputCardinality,
        StreamingMode,
    };

    pub const PROVIDER_ID: &str = "openai-codex";
    pub const PROVIDER_DISPLAY_NAME: &str = "OpenAI GPT Image via Codex CLI";
    pub const MODEL_GPT_IMAGE_2: &str = "gpt-image-2";
    pub const MODEL_GPT_IMAGE_2_SNAPSHOT: &str = "gpt-image-2-2026-04-21";
    pub const OWNER: &str = "openai";
    pub const MODELS: &[&str] = &[MODEL_GPT_IMAGE_2, MODEL_GPT_IMAGE_2_SNAPSHOT];

    const OFFICIAL_PARAMS: OfficialParamsContract = OfficialParamsContract {
        kind: OfficialParamsKind::OpenAiCodexCli,
        schema_id: "openai-codex-cli/v1",
        passthrough_allowed: false,
    };

    pub const OPERATIONS: &[OperationDescriptor] = &[
        OperationDescriptor {
            id: "images.generations",
            descriptor_revision: "openai-codex/images.generations/v1",
            command_schema: "openai.images.generation.v1",
            output_schema: "factory.openai-compatible.images.response.v1",
            media: MediaKind::Image,
            operation: MediaOperation::Generation,
            completion: CompletionMode::Inline,
            artifact_delivery: ArtifactDelivery::InlineBounded {
                max_bytes: 256 * 1024 * 1024,
            },
            client_streaming: StreamingMode::FinalEvent,
            idempotency: IdempotencyMode::SubmissionBound,
            billing_metric: BillingMetric::Output,
            output_cardinality: OutputCardinality::ExactlyOne,
            official_params: OFFICIAL_PARAMS,
        },
        OperationDescriptor {
            id: "images.edits",
            descriptor_revision: "openai-codex/images.edits/v1",
            command_schema: "openai.images.edit.v1",
            output_schema: "factory.openai-compatible.images.response.v1",
            media: MediaKind::Image,
            operation: MediaOperation::Edit,
            completion: CompletionMode::Inline,
            artifact_delivery: ArtifactDelivery::InlineBounded {
                max_bytes: 256 * 1024 * 1024,
            },
            client_streaming: StreamingMode::FinalEvent,
            idempotency: IdempotencyMode::SubmissionBound,
            billing_metric: BillingMetric::Output,
            output_cardinality: OutputCardinality::ExactlyOne,
            official_params: OFFICIAL_PARAMS,
        },
    ];

    pub const CAPABILITIES: ProviderCapabilities = OPERATIONS;

    pub const DEFINITION: ProviderDefinition = ProviderDefinition {
        id: PROVIDER_ID,
        display_name: PROVIDER_DISPLAY_NAME,
        owner: OWNER,
        status: ProviderStatus::Active,
        execution_mode: ProviderExecutionMode::NativeCli,
        models: MODELS,
        capabilities: CAPABILITIES,
    };

    pub fn operation(id: &str) -> Option<&'static OperationDescriptor> {
        OPERATIONS.iter().find(|operation| operation.id == id)
    }

    pub fn is_supported_model(model: &str) -> bool {
        MODELS.contains(&model)
    }
}

pub mod planned {
    use super::{ProviderExecutionMode, ProviderRoadmapMetadata};

    pub const PROVIDERS: &[ProviderRoadmapMetadata] = &[
        ProviderRoadmapMetadata {
            id: "midjourney",
            display_name: "Midjourney",
            owner: "midjourney",
            execution_mode: ProviderExecutionMode::ManagedApi,
            candidate_models: &["midjourney-v7"],
            intended_scope: "image generation",
        },
        ProviderRoadmapMetadata {
            id: "dreamina-cli",
            display_name: "Dreamina CLI",
            owner: "bytedance",
            execution_mode: ProviderExecutionMode::CliBridge,
            candidate_models: &["dreamina-image-5.0", "seedance2.0", "seedance2.0fast"],
            intended_scope: "official Dreamina CLI image and video generation",
        },
        ProviderRoadmapMetadata {
            id: "volcengine-ark-media",
            display_name: "Volcengine Ark Media API",
            owner: "volcengine",
            execution_mode: ProviderExecutionMode::ManagedApi,
            candidate_models: &["seedream", "seedance"],
            intended_scope: "Ark bearer-authenticated image and video APIs",
        },
        ProviderRoadmapMetadata {
            id: "volcengine-jimeng-visual",
            display_name: "Volcengine JiMeng Visual OpenAPI",
            owner: "volcengine",
            execution_mode: ProviderExecutionMode::ManagedApi,
            candidate_models: &["jimeng-image", "jimeng-video"],
            intended_scope: "AK/SK-signed JiMeng Visual image and video APIs",
        },
        ProviderRoadmapMetadata {
            id: "grok-cli",
            display_name: "Grok CLI",
            owner: "xai",
            execution_mode: ProviderExecutionMode::CliBridge,
            candidate_models: &["grok-image"],
            intended_scope: "image generation",
        },
    ];
}

pub fn active_providers() -> &'static [ProviderDefinition] {
    &[openai_codex::DEFINITION]
}

pub fn all_provider_roadmap() -> Vec<ProviderRoadmapEntry> {
    active_providers()
        .iter()
        .copied()
        .map(ProviderRoadmapEntry::Active)
        .chain(
            planned::PROVIDERS
                .iter()
                .copied()
                .map(ProviderRoadmapEntry::Planned),
        )
        .collect()
}
