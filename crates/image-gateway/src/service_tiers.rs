use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectServiceTier {
    #[default]
    Default,
    Priority,
}

impl ProjectServiceTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Priority => "priority",
        }
    }

    pub fn from_database(value: &str) -> Option<Self> {
        match value {
            "default" => Some(Self::Default),
            "priority" => Some(Self::Priority),
            _ => None,
        }
    }

    fn requested_effective(self) -> EffectiveServiceTier {
        match self {
            Self::Default => EffectiveServiceTier::Default,
            Self::Priority => EffectiveServiceTier::Priority,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RequestedServiceTier {
    #[default]
    Auto,
    Default,
    Flex,
    Priority,
}

impl RequestedServiceTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Default => "default",
            Self::Flex => "flex",
            Self::Priority => "priority",
        }
    }

    fn desired(self, project: ProjectServiceTier) -> EffectiveServiceTier {
        match self {
            Self::Auto => project.requested_effective(),
            Self::Default => EffectiveServiceTier::Default,
            Self::Flex => EffectiveServiceTier::Flex,
            Self::Priority => EffectiveServiceTier::Priority,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EffectiveServiceTier {
    #[default]
    Default,
    Flex,
    Priority,
}

impl EffectiveServiceTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Flex => "flex",
            Self::Priority => "priority",
        }
    }

    pub fn pricing_key(self) -> &'static str {
        match self {
            Self::Default => "standard",
            Self::Flex => "flex",
            Self::Priority => "priority",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceTierSupport {
    pub flex: bool,
    pub priority: bool,
}

impl ServiceTierSupport {
    pub const fn default_only() -> Self {
        Self {
            flex: false,
            priority: false,
        }
    }

    fn supports(self, tier: EffectiveServiceTier) -> bool {
        match tier {
            EffectiveServiceTier::Default => true,
            EffectiveServiceTier::Flex => self.flex,
            EffectiveServiceTier::Priority => self.priority,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceTierDecision {
    pub requested: RequestedServiceTier,
    pub project_default: ProjectServiceTier,
    pub effective: EffectiveServiceTier,
    pub fallback_reason: Option<&'static str>,
}

impl ServiceTierDecision {
    pub fn for_default_only_project(project_default: ProjectServiceTier) -> Self {
        resolve_service_tier(
            RequestedServiceTier::Auto,
            project_default,
            ServiceTierSupport::default_only(),
        )
        .expect("auto service tier resolution always permits default fallback")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnsupportedServiceTier {
    pub requested: RequestedServiceTier,
}

pub fn resolve_service_tier(
    requested: RequestedServiceTier,
    project_default: ProjectServiceTier,
    support: ServiceTierSupport,
) -> Result<ServiceTierDecision, UnsupportedServiceTier> {
    let desired = requested.desired(project_default);
    let (effective, fallback_reason) = match (requested, support.supports(desired)) {
        (_, true) => (desired, None),
        (RequestedServiceTier::Auto, false) => (
            EffectiveServiceTier::Default,
            Some("model_service_tier_unsupported"),
        ),
        (_, false) => return Err(UnsupportedServiceTier { requested }),
    };
    Ok(ServiceTierDecision {
        requested,
        project_default,
        effective,
        fallback_reason,
    })
}

pub fn request_hash_with_project_service_tier(
    base_request_hash: &str,
    project_default: ProjectServiceTier,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"aif-service-tier-request-v1\0");
    digest.update(base_request_hash.as_bytes());
    digest.update(b"\0");
    digest.update(project_default.as_str().as_bytes());
    hex::encode(digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_inherits_project_priority_when_supported() {
        let decision = resolve_service_tier(
            RequestedServiceTier::Auto,
            ProjectServiceTier::Priority,
            ServiceTierSupport {
                flex: false,
                priority: true,
            },
        )
        .expect("priority should be supported");
        assert_eq!(decision.effective, EffectiveServiceTier::Priority);
        assert_eq!(decision.fallback_reason, None);
    }

    #[test]
    fn unsupported_project_priority_falls_back_to_default() {
        let decision = resolve_service_tier(
            RequestedServiceTier::Auto,
            ProjectServiceTier::Priority,
            ServiceTierSupport::default_only(),
        )
        .expect("auto may fall back to default");
        assert_eq!(decision.effective, EffectiveServiceTier::Default);
        assert_eq!(
            decision.fallback_reason,
            Some("model_service_tier_unsupported")
        );
    }

    #[test]
    fn explicit_default_overrides_project_priority() {
        let decision = resolve_service_tier(
            RequestedServiceTier::Default,
            ProjectServiceTier::Priority,
            ServiceTierSupport::default_only(),
        )
        .expect("default is always supported");
        assert_eq!(decision.effective, EffectiveServiceTier::Default);
        assert_eq!(decision.fallback_reason, None);
    }

    #[test]
    fn explicit_priority_is_rejected_when_unsupported() {
        let error = resolve_service_tier(
            RequestedServiceTier::Priority,
            ProjectServiceTier::Default,
            ServiceTierSupport::default_only(),
        )
        .expect_err("explicit priority must not silently fall back");
        assert_eq!(error.requested, RequestedServiceTier::Priority);
    }

    #[test]
    fn explicit_flex_is_rejected_when_unsupported() {
        let error = resolve_service_tier(
            RequestedServiceTier::Flex,
            ProjectServiceTier::Default,
            ServiceTierSupport::default_only(),
        )
        .expect_err("explicit flex must fail closed");
        assert_eq!(error.requested, RequestedServiceTier::Flex);
    }

    #[test]
    fn project_default_is_part_of_request_identity() {
        let default_hash =
            request_hash_with_project_service_tier("base", ProjectServiceTier::Default);
        let priority_hash =
            request_hash_with_project_service_tier("base", ProjectServiceTier::Priority);
        assert_ne!(default_hash, priority_hash);
        assert_eq!(
            default_hash,
            request_hash_with_project_service_tier("base", ProjectServiceTier::Default)
        );
    }
}
