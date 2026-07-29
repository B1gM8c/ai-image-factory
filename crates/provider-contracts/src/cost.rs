use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCostAuthority {
    ProviderReported,
}

impl ProviderCostAuthority {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderReported => "provider_reported",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCostConfidence {
    Exact,
}

impl ProviderCostConfidence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCostNativeUnit {
    UsdTick,
}

impl ProviderCostNativeUnit {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UsdTick => "usd_tick",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCostEvidenceScope {
    ApiResponse,
    CliInvocation,
}

impl ProviderCostEvidenceScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApiResponse => "api_response",
            Self::CliInvocation => "cli_invocation",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCostObservationV1 {
    pub provider_id: String,
    pub execution_surface: String,
    pub provider_operation_id: String,
    pub currency: String,
    pub native_unit: ProviderCostNativeUnit,
    #[serde(with = "u128_decimal_string")]
    pub native_quantity: u128,
    pub authority: ProviderCostAuthority,
    pub confidence: ProviderCostConfidence,
    pub evidence_hash: [u8; 32],
    pub evidence_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderReportedCostEvidenceV1 {
    scope: ProviderCostEvidenceScope,
    observation: ProviderCostObservationV1,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderReportedCostEvidenceWire {
    scope: ProviderCostEvidenceScope,
    observation: ProviderCostObservationV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderCostObservationError {
    #[error("provider cost observation is invalid")]
    Invalid,
}

impl ProviderReportedCostEvidenceV1 {
    pub fn usd_ticks(
        scope: ProviderCostEvidenceScope,
        provider_id: impl Into<String>,
        execution_surface: impl Into<String>,
        provider_operation_id: impl Into<String>,
        native_quantity: u128,
        evidence: &[u8],
        evidence_path: impl Into<String>,
    ) -> Result<Self, ProviderCostObservationError> {
        let evidence = Self {
            scope,
            observation: ProviderCostObservationV1::provider_reported_usd_ticks(
                provider_id,
                execution_surface,
                provider_operation_id,
                native_quantity,
                evidence,
                evidence_path,
            )?,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    pub fn from_observation(
        scope: ProviderCostEvidenceScope,
        observation: ProviderCostObservationV1,
    ) -> Result<Self, ProviderCostObservationError> {
        let evidence = Self { scope, observation };
        evidence.validate()?;
        Ok(evidence)
    }

    pub const fn scope(&self) -> ProviderCostEvidenceScope {
        self.scope
    }

    pub fn observation(&self) -> &ProviderCostObservationV1 {
        &self.observation
    }

    pub fn into_observation(self) -> ProviderCostObservationV1 {
        self.observation
    }

    pub fn validate(&self) -> Result<(), ProviderCostObservationError> {
        self.observation.validate()?;
        let expected_surface = match self.scope {
            ProviderCostEvidenceScope::ApiResponse => "provider_api",
            ProviderCostEvidenceScope::CliInvocation => "provider_cli",
        };
        if self.observation.execution_surface != expected_surface {
            return Err(ProviderCostObservationError::Invalid);
        }
        Ok(())
    }

    pub fn canonical_sha256_v1(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"provider-reported-cost-evidence-v1\0");
        update_field(&mut digest, self.scope.as_str().as_bytes());
        update_field(&mut digest, &self.observation.canonical_sha256_v1());
        digest.finalize().into()
    }
}

impl<'de> serde::Deserialize<'de> for ProviderReportedCostEvidenceV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ProviderReportedCostEvidenceWire::deserialize(deserializer)?;
        Self::from_observation(wire.scope, wire.observation).map_err(serde::de::Error::custom)
    }
}

impl ProviderCostObservationV1 {
    pub fn provider_reported_usd_ticks(
        provider_id: impl Into<String>,
        execution_surface: impl Into<String>,
        provider_operation_id: impl Into<String>,
        native_quantity: u128,
        evidence: &[u8],
        evidence_path: impl Into<String>,
    ) -> Result<Self, ProviderCostObservationError> {
        let observation = Self {
            provider_id: provider_id.into(),
            execution_surface: execution_surface.into(),
            provider_operation_id: provider_operation_id.into(),
            currency: "USD".to_string(),
            native_unit: ProviderCostNativeUnit::UsdTick,
            native_quantity,
            authority: ProviderCostAuthority::ProviderReported,
            confidence: ProviderCostConfidence::Exact,
            evidence_hash: Sha256::digest(evidence).into(),
            evidence_path: evidence_path.into(),
        };
        observation.validate()?;
        Ok(observation)
    }

    pub fn provider_reported_usd_ticks_from_evidence_hash(
        provider_id: impl Into<String>,
        execution_surface: impl Into<String>,
        provider_operation_id: impl Into<String>,
        native_quantity: u128,
        evidence_hash: [u8; 32],
        evidence_path: impl Into<String>,
    ) -> Result<Self, ProviderCostObservationError> {
        let observation = Self {
            provider_id: provider_id.into(),
            execution_surface: execution_surface.into(),
            provider_operation_id: provider_operation_id.into(),
            currency: "USD".to_string(),
            native_unit: ProviderCostNativeUnit::UsdTick,
            native_quantity,
            authority: ProviderCostAuthority::ProviderReported,
            confidence: ProviderCostConfidence::Exact,
            evidence_hash,
            evidence_path: evidence_path.into(),
        };
        observation.validate()?;
        Ok(observation)
    }

    pub fn validate(&self) -> Result<(), ProviderCostObservationError> {
        if !valid_identifier(&self.provider_id, 128)
            || !matches!(
                self.execution_surface.as_str(),
                "provider_api" | "provider_cli" | "manual_import"
            )
            || !valid_text(&self.provider_operation_id, 512)
            || self.currency != "USD"
            || self.native_unit != ProviderCostNativeUnit::UsdTick
            || self.authority != ProviderCostAuthority::ProviderReported
            || self.confidence != ProviderCostConfidence::Exact
            || !valid_text(&self.evidence_path, 512)
        {
            return Err(ProviderCostObservationError::Invalid);
        }
        Ok(())
    }

    pub fn observation_key(&self) -> String {
        hex::encode(self.canonical_sha256_v1())
    }

    pub fn canonical_sha256_v1(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"provider-cost-observation-v1\0");
        update_field(&mut digest, self.provider_id.as_bytes());
        update_field(&mut digest, self.execution_surface.as_bytes());
        update_field(&mut digest, self.provider_operation_id.as_bytes());
        update_field(&mut digest, self.currency.as_bytes());
        update_field(&mut digest, self.native_unit.as_str().as_bytes());
        update_field(&mut digest, self.native_quantity.to_string().as_bytes());
        update_field(&mut digest, self.authority.as_str().as_bytes());
        update_field(&mut digest, self.confidence.as_str().as_bytes());
        update_field(&mut digest, &self.evidence_hash);
        update_field(&mut digest, self.evidence_path.as_bytes());
        digest.finalize().into()
    }
}

fn update_field(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn valid_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn valid_text(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

mod u128_decimal_string {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse::<u128>()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_reported_ticks_are_exact_and_digest_stable() {
        let observation = ProviderCostObservationV1::provider_reported_usd_ticks(
            "xai",
            "provider_api",
            "request-123",
            10_000_000_000,
            br#"{"usage":{"cost_in_usd_ticks":10000000000}}"#,
            "response.usage.cost_in_usd_ticks",
        )
        .unwrap();

        assert_eq!(observation.currency, "USD");
        assert_eq!(observation.native_unit.as_str(), "usd_tick");
        assert_eq!(observation.authority.as_str(), "provider_reported");
        assert_eq!(observation.confidence.as_str(), "exact");
        assert_eq!(observation.observation_key().len(), 64);
        assert_eq!(observation.observation_key(), observation.observation_key());
    }

    #[test]
    fn observation_identity_covers_upstream_operation_and_evidence() {
        let first = ProviderCostObservationV1::provider_reported_usd_ticks(
            "xai",
            "provider_api",
            "request-123",
            10,
            b"evidence-a",
            "response.usage.cost_in_usd_ticks",
        )
        .unwrap();
        let second = ProviderCostObservationV1::provider_reported_usd_ticks(
            "xai",
            "provider_api",
            "request-124",
            10,
            b"evidence-a",
            "response.usage.cost_in_usd_ticks",
        )
        .unwrap();
        let third = ProviderCostObservationV1::provider_reported_usd_ticks(
            "xai",
            "provider_api",
            "request-123",
            10,
            b"evidence-b",
            "response.usage.cost_in_usd_ticks",
        )
        .unwrap();

        assert_ne!(first.observation_key(), second.observation_key());
        assert_ne!(first.observation_key(), third.observation_key());
    }

    #[test]
    fn agent_token_cost_cannot_use_the_media_cost_contract() {
        assert!(
            ProviderCostObservationV1::provider_reported_usd_ticks(
                "grok",
                "agent_model",
                "session-123",
                10,
                b"headless_usage.costUSD",
                "headless_usage.costUSD",
            )
            .is_err()
        );
    }

    #[test]
    fn reported_cost_evidence_keeps_api_and_cli_scopes_distinct() {
        let api = ProviderReportedCostEvidenceV1::usd_ticks(
            ProviderCostEvidenceScope::ApiResponse,
            "xai",
            "provider_api",
            "request-123",
            200_000_000,
            br#"{"usage":{"cost_in_usd_ticks":200000000}}"#,
            "response.usage.cost_in_usd_ticks",
        )
        .unwrap();
        let cli = ProviderReportedCostEvidenceV1::usd_ticks(
            ProviderCostEvidenceScope::CliInvocation,
            "grok-cli",
            "provider_cli",
            "request-123",
            200_000_000,
            br#"{"total_cost_usd_ticks":200000000}"#,
            "end.total_cost_usd_ticks",
        )
        .unwrap();

        assert_eq!(api.scope().as_str(), "api_response");
        assert_eq!(cli.scope().as_str(), "cli_invocation");
        assert_ne!(api.canonical_sha256_v1(), cli.canonical_sha256_v1());
        assert_eq!(cli.clone().into_observation(), cli.observation().clone());
    }

    #[test]
    fn reported_cost_evidence_roundtrips_without_digest_drift() {
        let evidence = ProviderReportedCostEvidenceV1::usd_ticks(
            ProviderCostEvidenceScope::CliInvocation,
            "grok-cli",
            "provider_cli",
            "request-123",
            200_000_000,
            br#"{"total_cost_usd_ticks":200000000}"#,
            "end.total_cost_usd_ticks",
        )
        .unwrap();
        let encoded = serde_json::to_vec(&evidence).unwrap();
        let decoded: ProviderReportedCostEvidenceV1 = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(decoded, evidence);
        assert_eq!(
            decoded.canonical_sha256_v1(),
            evidence.canonical_sha256_v1()
        );
    }

    #[test]
    fn reported_cost_evidence_deserialization_rejects_scope_surface_mismatch() {
        let invalid = serde_json::json!({
            "scope": "api_response",
            "observation": {
                "provider_id": "grok-cli",
                "execution_surface": "provider_cli",
                "provider_operation_id": "request-123",
                "currency": "USD",
                "native_unit": "usd_tick",
                "native_quantity": "200000000",
                "authority": "provider_reported",
                "confidence": "exact",
                "evidence_hash": vec![0; 32],
                "evidence_path": "end.total_cost_usd_ticks"
            }
        });

        assert!(serde_json::from_value::<ProviderReportedCostEvidenceV1>(invalid).is_err());
    }

    #[test]
    fn reported_cost_scope_must_match_the_execution_surface() {
        assert!(
            ProviderReportedCostEvidenceV1::usd_ticks(
                ProviderCostEvidenceScope::ApiResponse,
                "grok-cli",
                "provider_cli",
                "request-123",
                1,
                b"evidence",
                "end.total_cost_usd_ticks",
            )
            .is_err()
        );
        assert!(
            ProviderReportedCostEvidenceV1::usd_ticks(
                ProviderCostEvidenceScope::CliInvocation,
                "xai",
                "provider_api",
                "request-123",
                1,
                b"evidence",
                "response.usage.cost_in_usd_ticks",
            )
            .is_err()
        );
    }
}
