pub mod capability;
pub mod jobs;
pub mod media;
pub mod official_params;
pub mod provider;

pub use capability::{
    ArtifactDelivery, BillingMetric, CallbackMode, CancellationMode, CompletionMode,
    IdempotencyMode, OperationDescriptor, OutputCardinality, ProviderCapabilities,
    RemoteTaskControls,
};
pub use jobs::{JobKind, JobLifecycleState};
pub use media::{JobMode, MediaKind, MediaOperation, StreamingMode};
pub use official_params::{OfficialParamsContract, OfficialParamsKind};
pub use provider::{
    ProviderDefinition, ProviderDescriptorError, ProviderExecutionMode, ProviderRoadmapEntry,
    ProviderRoadmapMetadata, ProviderStatus, active_providers, all_provider_roadmap, openai_codex,
    planned,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_codex_is_the_first_active_provider() {
        let providers = active_providers();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, openai_codex::PROVIDER_ID);
        assert_eq!(providers[0].capabilities.len(), 2);
        assert_eq!(providers[0].capabilities[0].id, "images.generations");
        assert_eq!(
            providers[0].capabilities[0].command_schema,
            "openai.images.generation.v1"
        );
        assert_eq!(
            providers[0].capabilities[0].output_schema,
            "factory.openai-compatible.images.response.v1"
        );
        assert_eq!(providers[0].capabilities[0].media, MediaKind::Image);
        assert_eq!(
            providers[0].capabilities[0].completion,
            CompletionMode::Inline
        );
        assert_eq!(
            providers[0].capabilities[0].output_cardinality,
            OutputCardinality::ExactlyOne
        );
    }

    #[test]
    fn roadmap_keeps_future_cli_providers_out_of_active_set() {
        assert!(openai_codex::is_supported_model("gpt-image-2"));
        assert!(openai_codex::is_supported_model("gpt-image-2-2026-04-21"));
        assert!(!openai_codex::is_supported_model("midjourney-v7"));
        assert!(
            all_provider_roadmap()
                .iter()
                .any(|provider| provider.id() == "jimeng-cli")
        );
        assert!(
            all_provider_roadmap()
                .iter()
                .any(|provider| provider.id() == "seedance-cli")
        );
        assert!(
            all_provider_roadmap()
                .iter()
                .filter(|provider| provider.status() == ProviderStatus::Active)
                .all(|provider| provider.id() == openai_codex::PROVIDER_ID)
        );
    }

    #[test]
    fn active_operation_descriptors_have_unique_ids() {
        for provider in active_providers() {
            assert_eq!(provider.validate(), Ok(()));
            for (index, operation) in provider.capabilities.iter().enumerate() {
                assert!(
                    provider.capabilities[..index]
                        .iter()
                        .all(|candidate| candidate.id != operation.id)
                );
            }
        }
    }
}
