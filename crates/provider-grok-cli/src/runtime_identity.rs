use std::{collections::BTreeMap, str::FromStr};

use serde::{Deserialize, Deserializer};
use thiserror::Error;

const V1_LOCK: &str = include_str!("../../../providers/grok-cli-v1.lock.json");
const V2_LOCK: &str = include_str!("../../../providers/grok-cli.lock.json");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrokRuntimeGeneration {
    V1,
    V2,
}

impl FromStr for GrokRuntimeGeneration {
    type Err = GrokRuntimeIdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "v1" => Ok(Self::V1),
            "v2" => Ok(Self::V2),
            other => Err(GrokRuntimeIdentityError::UnknownGeneration(
                other.to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrokRuntimeIdentity {
    pub generation: GrokRuntimeGeneration,
    pub target_triple: String,
    pub version: String,
    pub version_output: String,
    pub compatibility_revision: String,
    pub image_adapter_revision: Option<String>,
    pub video_adapter_revision: String,
    pub url: String,
    pub sha256: String,
    pub bytes: u64,
    pub elf_machine: u16,
}

#[derive(Debug, Error)]
pub enum GrokRuntimeIdentityError {
    #[error("unknown Grok runtime generation: {0}")]
    UnknownGeneration(String),
    #[error("unsupported Grok runtime target for {generation:?}: {target}")]
    UnsupportedTarget {
        generation: GrokRuntimeGeneration,
        target: String,
    },
    #[error("invalid Grok {generation:?} provider lock: {source}")]
    InvalidLock {
        generation: GrokRuntimeGeneration,
        #[source]
        source: serde_json::Error,
    },
    #[error("Grok {generation:?} provider lock failed validation: {reason}")]
    InvalidLockShape {
        generation: GrokRuntimeGeneration,
        reason: &'static str,
    },
}

#[derive(Debug, Deserialize)]
struct ProviderLock {
    schema_version: u64,
    provider: String,
    source_repository: String,
    version: String,
    version_output: String,
    compatibility_revision: String,
    #[serde(deserialize_with = "deserialize_image_adapter_revision")]
    image_adapter_revision: ImageAdapterRevision,
    video_adapter_revision: String,
    artifacts: BTreeMap<String, ProviderArtifact>,
}

#[derive(Debug, Deserialize)]
struct ProviderArtifact {
    url: String,
    sha256: String,
    bytes: u64,
    elf_machine: u16,
}

#[derive(Debug)]
enum ImageAdapterRevision {
    Null,
    Value(String),
}

fn deserialize_image_adapter_revision<'de, D>(
    deserializer: D,
) -> Result<ImageAdapterRevision, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match Option::<String>::deserialize(deserializer)? {
        Some(value) => ImageAdapterRevision::Value(value),
        None => ImageAdapterRevision::Null,
    })
}

pub fn lookup_runtime_identity(
    generation: GrokRuntimeGeneration,
    target: &str,
) -> Result<GrokRuntimeIdentity, GrokRuntimeIdentityError> {
    let lock_text = match generation {
        GrokRuntimeGeneration::V1 => V1_LOCK,
        GrokRuntimeGeneration::V2 => V2_LOCK,
    };
    lookup_runtime_identity_from_lock(generation, target, lock_text)
}

pub(crate) fn lookup_runtime_identity_from_lock(
    generation: GrokRuntimeGeneration,
    target: &str,
    lock_text: &str,
) -> Result<GrokRuntimeIdentity, GrokRuntimeIdentityError> {
    let expected_machine = match target {
        "x86_64-unknown-linux-gnu" => 62,
        "aarch64-unknown-linux-gnu" => 183,
        _ => {
            return Err(GrokRuntimeIdentityError::UnsupportedTarget {
                generation,
                target: target.to_owned(),
            });
        }
    };
    let lock: ProviderLock = serde_json::from_str(lock_text)
        .map_err(|source| GrokRuntimeIdentityError::InvalidLock { generation, source })?;
    validate_lock(generation, &lock)?;
    let artifact =
        lock.artifacts
            .get(target)
            .ok_or_else(|| GrokRuntimeIdentityError::UnsupportedTarget {
                generation,
                target: target.to_owned(),
            })?;
    if artifact.elf_machine != expected_machine {
        return Err(GrokRuntimeIdentityError::InvalidLockShape {
            generation,
            reason: "artifact ELF machine does not match target",
        });
    }
    Ok(GrokRuntimeIdentity {
        generation,
        target_triple: target.to_owned(),
        version: lock.version,
        version_output: lock.version_output,
        compatibility_revision: lock.compatibility_revision,
        image_adapter_revision: match lock.image_adapter_revision {
            ImageAdapterRevision::Null => None,
            ImageAdapterRevision::Value(value) => Some(value),
        },
        video_adapter_revision: lock.video_adapter_revision,
        url: artifact.url.clone(),
        sha256: artifact.sha256.clone(),
        bytes: artifact.bytes,
        elf_machine: artifact.elf_machine,
    })
}

fn validate_lock(
    generation: GrokRuntimeGeneration,
    lock: &ProviderLock,
) -> Result<(), GrokRuntimeIdentityError> {
    if lock.schema_version != 1 {
        return Err(GrokRuntimeIdentityError::InvalidLockShape {
            generation,
            reason: "unsupported schema version",
        });
    }
    if lock.provider != "xai-grok-cli" {
        return Err(GrokRuntimeIdentityError::InvalidLockShape {
            generation,
            reason: "unexpected provider",
        });
    }
    if lock.source_repository != "https://github.com/xai-org/grok-build" {
        return Err(GrokRuntimeIdentityError::InvalidLockShape {
            generation,
            reason: "unexpected source repository",
        });
    }
    match generation {
        GrokRuntimeGeneration::V1 => {
            if lock.version != "1.0.5"
                || lock.version_output != "grok 1.0.5 (5115b46bc9)"
                || lock.compatibility_revision != "grok-cli-1.0.5"
                || !matches!(
                    &lock.image_adapter_revision,
                    ImageAdapterRevision::Value(value)
                        if value == "grok-cli-1.0.5.agentic-media.v2"
                )
                || lock.video_adapter_revision != "grok-api-1.0.5.direct-image-video.v5"
            {
                return Err(GrokRuntimeIdentityError::InvalidLockShape {
                    generation,
                    reason: "V1 lock identity mismatch",
                });
            }
        }
        GrokRuntimeGeneration::V2 => {
            if lock.version != "1.0.34"
                || lock.version_output != "grok 1.0.34 (3736acbc8658)"
                || lock.compatibility_revision != "grok-cli-1.0.34"
                || !matches!(lock.image_adapter_revision, ImageAdapterRevision::Null)
                || lock.video_adapter_revision != "grok-cli-1.0.34.agentic-video.v1"
            {
                return Err(GrokRuntimeIdentityError::InvalidLockShape {
                    generation,
                    reason: "V2 lock identity mismatch",
                });
            }
        }
    }
    for artifact in lock.artifacts.values() {
        if !artifact.url.starts_with("https://x.ai/cli/")
            || artifact.bytes == 0
            || artifact.sha256.len() != 64
            || !artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(GrokRuntimeIdentityError::InvalidLockShape {
                generation,
                reason: "invalid artifact metadata",
            });
        }
    }
    Ok(())
}
