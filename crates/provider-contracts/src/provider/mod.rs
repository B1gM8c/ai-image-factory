use crate::{
    JobMode, MediaKind, MediaOperation, OfficialParamsContract, OfficialParamsKind, StreamingMode,
};

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
pub struct ProviderCapabilities {
    pub generations: bool,
    pub edits: bool,
    pub variations: bool,
    pub final_event_stream: bool,
    pub partial_image_stream: bool,
    pub async_jobs: bool,
    pub media: &'static [MediaKind],
    pub operations: &'static [MediaOperation],
    pub streaming: StreamingMode,
    pub job_mode: JobMode,
    pub official_params: OfficialParamsContract,
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

pub mod openai_codex {
    use super::{
        JobMode, MediaKind, MediaOperation, OfficialParamsContract, OfficialParamsKind,
        ProviderCapabilities, ProviderDefinition, ProviderExecutionMode, ProviderStatus,
        StreamingMode,
    };

    pub const PROVIDER_ID: &str = "openai-codex";
    pub const PROVIDER_DISPLAY_NAME: &str = "OpenAI GPT Image via Codex CLI";
    pub const MODEL_GPT_IMAGE_2: &str = "gpt-image-2";
    pub const MODEL_GPT_IMAGE_2_SNAPSHOT: &str = "gpt-image-2-2026-04-21";
    pub const OWNER: &str = "openai";
    pub const MODELS: &[&str] = &[MODEL_GPT_IMAGE_2, MODEL_GPT_IMAGE_2_SNAPSHOT];

    pub const CAPABILITIES: ProviderCapabilities = ProviderCapabilities {
        generations: true,
        edits: true,
        variations: false,
        final_event_stream: true,
        partial_image_stream: false,
        async_jobs: false,
        media: &[MediaKind::Image],
        operations: &[MediaOperation::Generation, MediaOperation::Edit],
        streaming: StreamingMode::FinalEvent,
        job_mode: JobMode::Sync,
        official_params: OfficialParamsContract {
            kind: OfficialParamsKind::OpenAiCodexCli,
            schema_id: "openai-codex-cli/v1",
            passthrough_allowed: false,
        },
    };

    pub const DEFINITION: ProviderDefinition = ProviderDefinition {
        id: PROVIDER_ID,
        display_name: PROVIDER_DISPLAY_NAME,
        owner: OWNER,
        status: ProviderStatus::Active,
        execution_mode: ProviderExecutionMode::NativeCli,
        models: MODELS,
        capabilities: CAPABILITIES,
    };

    pub fn is_supported_model(model: &str) -> bool {
        MODELS.contains(&model)
    }
}

pub mod planned {
    use super::{
        JobMode, MediaKind, MediaOperation, OfficialParamsContract, OfficialParamsKind,
        ProviderCapabilities, ProviderDefinition, ProviderExecutionMode, ProviderStatus,
        StreamingMode,
    };

    const IMAGE_ASYNC: ProviderCapabilities = ProviderCapabilities {
        generations: true,
        edits: false,
        variations: false,
        final_event_stream: false,
        partial_image_stream: false,
        async_jobs: true,
        media: &[MediaKind::Image],
        operations: &[MediaOperation::Generation],
        streaming: StreamingMode::None,
        job_mode: JobMode::Async,
        official_params: OfficialParamsContract {
            kind: OfficialParamsKind::XaiImage,
            schema_id: "image-generation/v1",
            passthrough_allowed: false,
        },
    };

    const JIMENG_IMAGE_VIDEO: ProviderCapabilities = ProviderCapabilities {
        generations: true,
        edits: false,
        variations: false,
        final_event_stream: false,
        partial_image_stream: false,
        async_jobs: true,
        media: &[MediaKind::Image, MediaKind::Video],
        operations: &[MediaOperation::Generation],
        streaming: StreamingMode::None,
        job_mode: JobMode::Async,
        official_params: OfficialParamsContract {
            kind: OfficialParamsKind::VolcengineJimengImage,
            schema_id: "volcengine-jimeng/v1",
            passthrough_allowed: false,
        },
    };

    const GROK_IMAGE: ProviderCapabilities = ProviderCapabilities {
        official_params: OfficialParamsContract {
            kind: OfficialParamsKind::XaiImage,
            schema_id: "xai-image/v1",
            passthrough_allowed: false,
        },
        ..IMAGE_ASYNC
    };

    const SEEDANCE_VIDEO: ProviderCapabilities = ProviderCapabilities {
        generations: true,
        edits: false,
        variations: false,
        final_event_stream: false,
        partial_image_stream: false,
        async_jobs: true,
        media: &[MediaKind::Video],
        operations: &[MediaOperation::Generation],
        streaming: StreamingMode::None,
        job_mode: JobMode::Async,
        official_params: OfficialParamsContract {
            kind: OfficialParamsKind::BytePlusSeedanceVideo,
            schema_id: "byteplus-seedance-video/v1",
            passthrough_allowed: false,
        },
    };

    pub const PROVIDERS: &[ProviderDefinition] = &[
        ProviderDefinition {
            id: "midjourney",
            display_name: "Midjourney",
            owner: "midjourney",
            status: ProviderStatus::Planned,
            execution_mode: ProviderExecutionMode::ManagedApi,
            models: &["midjourney-v7"],
            capabilities: IMAGE_ASYNC,
        },
        ProviderDefinition {
            id: "jimeng-cli",
            display_name: "JiMeng CLI",
            owner: "volcengine",
            status: ProviderStatus::Planned,
            execution_mode: ProviderExecutionMode::CliBridge,
            models: &["jimeng-image", "jimeng-video"],
            capabilities: JIMENG_IMAGE_VIDEO,
        },
        ProviderDefinition {
            id: "grok-cli",
            display_name: "Grok CLI",
            owner: "xai",
            status: ProviderStatus::Planned,
            execution_mode: ProviderExecutionMode::CliBridge,
            models: &["grok-image"],
            capabilities: GROK_IMAGE,
        },
        ProviderDefinition {
            id: "seedance-cli",
            display_name: "Seedance CLI",
            owner: "byteplus",
            status: ProviderStatus::Planned,
            execution_mode: ProviderExecutionMode::CliBridge,
            models: &["seedance-video"],
            capabilities: SEEDANCE_VIDEO,
        },
    ];
}

pub fn active_providers() -> &'static [ProviderDefinition] {
    &[openai_codex::DEFINITION]
}

pub fn all_provider_roadmap() -> Vec<ProviderDefinition> {
    active_providers()
        .iter()
        .copied()
        .chain(planned::PROVIDERS.iter().copied())
        .collect()
}
