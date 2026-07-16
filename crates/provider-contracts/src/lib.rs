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
                .any(|provider| provider.id() == "dreamina-cli")
        );
        assert!(
            all_provider_roadmap()
                .iter()
                .any(|provider| provider.id() == "volcengine-ark-media")
        );
        assert!(
            all_provider_roadmap()
                .iter()
                .any(|provider| provider.id() == "volcengine-jimeng-visual")
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

    #[test]
    fn active_operation_descriptor_digests_are_stable_and_distinct() {
        let generation = openai_codex::operation("images.generations").unwrap();
        let edit = openai_codex::operation("images.edits").unwrap();

        assert_ne!(generation.canonical_sha256_v1(), edit.canonical_sha256_v1());
        assert_eq!(
            generation.canonical_sha256_v1_hex(),
            "f7f3e84594bfda2312d9420aa22108e76b10b3b22c52535ccf768f944d9b7aaa"
        );
        assert_eq!(
            edit.canonical_sha256_v1_hex(),
            "c9a714ae667cab60f8130b841aa8887077232a29a1c3bb59ba7ecb77b8ddb471"
        );
    }

    #[test]
    fn operation_descriptor_sha256_v1_covers_each_variable_field() {
        let base = *openai_codex::operation("images.generations").unwrap();
        let base_digest = base.canonical_sha256_v1();
        let variants = [
            OperationDescriptor {
                id: "images.other",
                ..base
            },
            OperationDescriptor {
                descriptor_revision: "revision-v2",
                ..base
            },
            OperationDescriptor {
                command_schema: "command-v2",
                ..base
            },
            OperationDescriptor {
                output_schema: "output-v2",
                ..base
            },
            OperationDescriptor {
                media: MediaKind::Video,
                ..base
            },
            OperationDescriptor {
                operation: MediaOperation::Variation,
                ..base
            },
            OperationDescriptor {
                completion: CompletionMode::RemoteTask(RemoteTaskControls {
                    callback: CallbackMode::WakeupHint,
                    cancellation: CancellationMode::ProviderConfirmed,
                }),
                ..base
            },
            OperationDescriptor {
                artifact_delivery: ArtifactDelivery::InlineBounded {
                    max_bytes: 256 * 1024 * 1024 + 1,
                },
                ..base
            },
            OperationDescriptor {
                artifact_delivery: ArtifactDelivery::Streamed,
                ..base
            },
            OperationDescriptor {
                client_streaming: StreamingMode::PartialEvents,
                ..base
            },
            OperationDescriptor {
                idempotency: IdempotencyMode::ProviderToken,
                ..base
            },
            OperationDescriptor {
                billing_metric: BillingMetric::Request,
                ..base
            },
            OperationDescriptor {
                official_params: OfficialParamsContract {
                    kind: OfficialParamsKind::XaiImage,
                    ..base.official_params
                },
                ..base
            },
            OperationDescriptor {
                official_params: OfficialParamsContract {
                    schema_id: "official-v2",
                    ..base.official_params
                },
                ..base
            },
            OperationDescriptor {
                official_params: OfficialParamsContract {
                    passthrough_allowed: !base.official_params.passthrough_allowed,
                    ..base.official_params
                },
                ..base
            },
        ];

        for variant in variants {
            assert_ne!(variant.canonical_sha256_v1(), base_digest, "{variant:?}");
        }

        let remote_base = OperationDescriptor {
            completion: CompletionMode::RemoteTask(RemoteTaskControls {
                callback: CallbackMode::Unsupported,
                cancellation: CancellationMode::Unsupported,
            }),
            ..base
        };
        let remote_digest = remote_base.canonical_sha256_v1();
        assert_ne!(remote_digest, base_digest);
        assert_ne!(
            OperationDescriptor {
                completion: CompletionMode::RemoteTask(RemoteTaskControls {
                    callback: CallbackMode::WakeupHint,
                    cancellation: CancellationMode::Unsupported,
                }),
                ..remote_base
            }
            .canonical_sha256_v1(),
            remote_digest
        );
        assert_ne!(
            OperationDescriptor {
                completion: CompletionMode::RemoteTask(RemoteTaskControls {
                    callback: CallbackMode::Unsupported,
                    cancellation: CancellationMode::ProviderConfirmed,
                }),
                ..remote_base
            }
            .canonical_sha256_v1(),
            remote_digest
        );
    }
}
