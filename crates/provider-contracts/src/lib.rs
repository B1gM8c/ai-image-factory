pub mod jobs;
pub mod media;
pub mod official_params;
pub mod provider;

pub use jobs::{JobKind, JobLifecycleState};
pub use media::{JobMode, MediaKind, MediaOperation, StreamingMode};
pub use official_params::{OfficialParamsContract, OfficialParamsKind};
pub use provider::{
    ProviderCapabilities, ProviderDefinition, ProviderExecutionMode, ProviderStatus,
    active_providers, all_provider_roadmap, openai_codex, planned,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_codex_is_the_first_active_provider() {
        let providers = active_providers();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, openai_codex::PROVIDER_ID);
        assert!(providers[0].capabilities.generations);
        assert!(providers[0].capabilities.edits);
        assert!(!providers[0].capabilities.partial_image_stream);
        assert_eq!(providers[0].capabilities.media, &[MediaKind::Image]);
        assert_eq!(providers[0].capabilities.job_mode, JobMode::Sync);
    }

    #[test]
    fn roadmap_keeps_future_cli_providers_out_of_active_set() {
        assert!(openai_codex::is_supported_model("gpt-image-2"));
        assert!(openai_codex::is_supported_model("gpt-image-2-2026-04-21"));
        assert!(!openai_codex::is_supported_model("midjourney-v7"));
        assert!(
            all_provider_roadmap()
                .iter()
                .any(|provider| provider.id == "jimeng-cli")
        );
        assert!(
            all_provider_roadmap()
                .iter()
                .any(|provider| provider.id == "seedance-cli")
        );
    }
}
