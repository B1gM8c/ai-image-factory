use image_api_contracts::xai::{
    XAI_IMAGES_API_PROFILE, XaiImageGenerationCommandV1, XaiImageGenerationRequest,
    XaiImageResponseFormat, XaiImageStorageOptions, XaiRequestError,
};
use image_provider_grok_cli::{
    ADAPTER_REVISION, GROK_IMAGE_GENERATION_COMMAND_SCHEMA, GrokCommandError,
    GrokImageGenerationPayloadV1, PROVIDER_ID, XaiGrokProjectionError,
};
use image_provider_sdk::{OutputSlot, SingleOutputCommand};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use super::{AdmissionContract, AdmissionTicket, AttachJob, ClaimAdmission, GENERATION_OPERATION};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XaiImageAdmissionPlan {
    source_command: XaiImageGenerationCommandV1,
    source_request_hash: String,
    provider_model: String,
    provider_command: Value,
}

impl XaiImageAdmissionPlan {
    pub fn for_grok_cli(
        request: XaiImageGenerationRequest,
    ) -> Result<Self, XaiImageAdmissionError> {
        let source_command = XaiImageGenerationCommandV1::from_request(request)?;
        let source_request_hash = source_command.canonical_sha256_hex();
        let payload = GrokImageGenerationPayloadV1::from_xai_command(source_command.clone())
            .map_err(|error| match error {
                GrokCommandError::SourceProjection(error) => {
                    XaiImageAdmissionError::UnsupportedBinding(error)
                }
                _ => XaiImageAdmissionError::InvalidProviderCommand,
            })?;
        let provider_model = payload.request().model().as_str().to_owned();
        let command = SingleOutputCommand::new(OutputSlot::new(0, 1).unwrap(), payload)
            .map_err(|_| XaiImageAdmissionError::InvalidProviderCommand)?;
        let provider_command = serde_json::from_slice(command.canonical_payload())
            .map_err(|_| XaiImageAdmissionError::InvalidProviderCommand)?;
        Ok(Self {
            source_command,
            source_request_hash,
            provider_model,
            provider_command,
        })
    }

    pub fn source_command(&self) -> &XaiImageGenerationCommandV1 {
        &self.source_command
    }

    pub fn source_request_hash(&self) -> &str {
        &self.source_request_hash
    }

    pub fn provider_id(&self) -> &'static str {
        PROVIDER_ID
    }

    pub fn provider_model(&self) -> &str {
        &self.provider_model
    }

    pub fn command_schema(&self) -> &'static str {
        GROK_IMAGE_GENERATION_COMMAND_SCHEMA
    }

    pub fn command_json(&self) -> &Value {
        &self.provider_command
    }

    pub fn adapter_revision(&self) -> &'static str {
        ADAPTER_REVISION
    }

    pub fn response_format(&self) -> XaiImageResponseFormat {
        self.source_command.response_format
    }

    pub fn storage_options(&self) -> Option<&XaiImageStorageOptions> {
        self.source_command.storage_options.as_ref()
    }

    pub fn user(&self) -> Option<&str> {
        self.source_command.user.as_deref()
    }

    pub fn claim(
        &self,
        owner_token: Uuid,
        tenant_id: impl Into<String>,
        project_id: impl Into<String>,
        request_id: impl Into<String>,
        idempotency_key_digest: Option<String>,
        deadline_at_ms: i64,
    ) -> ClaimAdmission {
        ClaimAdmission {
            owner_token,
            tenant_id: tenant_id.into(),
            project_id: project_id.into(),
            api_profile: XAI_IMAGES_API_PROFILE.to_owned(),
            operation: GENERATION_OPERATION.to_owned(),
            request_id: request_id.into(),
            idempotency_key_digest,
            request_hash: self.source_request_hash.clone(),
            deadline_at_ms,
        }
    }

    pub fn attach(
        &self,
        ticket: AdmissionTicket,
        job_id: Uuid,
        schedule_scope: impl Into<String>,
        contract: AdmissionContract,
    ) -> AttachJob {
        AttachJob {
            ticket,
            job_id,
            command_schema: GROK_IMAGE_GENERATION_COMMAND_SCHEMA.to_owned(),
            command_json: self.provider_command.clone(),
            input_manifest: None,
            work_kind: "image_batch".to_owned(),
            schedule_scope: schedule_scope.into(),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: 1,
            contract,
            customer_pricing: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum XaiImageAdmissionError {
    #[error(transparent)]
    InvalidRequest(#[from] XaiRequestError),
    #[error(transparent)]
    UnsupportedBinding(#[from] XaiGrokProjectionError),
    #[error("xAI request could not be encoded as an immutable provider command")]
    InvalidProviderCommand,
}

#[cfg(test)]
mod tests {
    use image_api_contracts::xai::{XaiImageAspectRatio, XaiImageResolution};
    use image_provider_grok_cli::parse_image_generation_payload;

    use super::*;
    use crate::admission::{
        AdmissionClaim, AdmissionError, AdmissionStore, GENERATION_COMMAND_SCHEMA,
        InMemoryAdmissionStore, validate_attach_request,
    };

    fn request() -> XaiImageGenerationRequest {
        XaiImageGenerationRequest {
            aspect_ratio: Some(XaiImageAspectRatio::R16x9),
            model: Some("grok-imagine-image-quality".to_owned()),
            n: Some(1),
            prompt: "a lighthouse".to_owned(),
            resolution: Some(XaiImageResolution::R1k),
            response_format: Some(XaiImageResponseFormat::B64Json),
            storage_options: None,
            user: Some("customer-1".to_owned()),
        }
    }

    #[test]
    fn plan_binds_the_official_source_hash_into_the_provider_command() {
        let plan = XaiImageAdmissionPlan::for_grok_cli(request()).unwrap();
        let payload =
            parse_image_generation_payload(&serde_json::to_vec(&plan.provider_command).unwrap())
                .unwrap();

        assert_eq!(payload.source_command_sha256(), plan.source_request_hash());
        assert_eq!(payload.source_command(), plan.source_command());
        assert_eq!(plan.provider_id(), "grok-cli");
        assert_eq!(plan.provider_model(), "grok-imagine-image-quality");
        assert_eq!(plan.response_format(), XaiImageResponseFormat::B64Json);
        assert_eq!(plan.user(), Some("customer-1"));
    }

    #[test]
    fn plan_preserves_the_base_model_through_admission() {
        let mut request = request();
        request.model = Some("grok-imagine-image".to_owned());

        let plan = XaiImageAdmissionPlan::for_grok_cli(request).unwrap();
        let payload =
            parse_image_generation_payload(&serde_json::to_vec(&plan.provider_command).unwrap())
                .unwrap();

        assert_eq!(plan.provider_model(), "grok-imagine-image");
        assert_eq!(payload.request().model().as_str(), "grok-imagine-image");
    }

    #[tokio::test]
    async fn durable_admission_accepts_only_the_matching_xai_source_hash() {
        let plan = XaiImageAdmissionPlan::for_grok_cli(request()).unwrap();
        let store = InMemoryAdmissionStore::default();
        let claim = plan.claim(
            Uuid::new_v4(),
            "tenant-1",
            "project-1",
            "request-1",
            None,
            i64::MAX,
        );
        let AdmissionClaim::Owner(ticket) = store.claim(claim).await.unwrap() else {
            panic!("expected admission ownership");
        };
        let attach = plan.attach(
            ticket.clone(),
            Uuid::new_v4(),
            "tenant:tenant-1",
            AdmissionContract::LegacyV1,
        );
        validate_attach_request(&attach).unwrap();

        let mut forged_projection = attach.clone();
        forged_projection.command_json["prompt"] = "a different image".into();
        assert!(validate_attach_request(&forged_projection).is_err());

        let mut forged_source_hash = attach.clone();
        forged_source_hash.command_json["source_command_sha256"] = "f".repeat(64).into();
        assert!(validate_attach_request(&forged_source_hash).is_err());

        let mut forged_ticket = attach.clone();
        forged_ticket.ticket.request_hash = "f".repeat(64);
        validate_attach_request(&forged_ticket).unwrap();
        assert!(matches!(
            store.attach(forged_ticket).await,
            Err(AdmissionError::InvalidOwner)
        ));

        store.attach(attach).await.unwrap();
        assert!(
            store
                .claim_ready_for_schema(
                    "codex-worker",
                    30_000,
                    AdmissionContract::LegacyV1,
                    GENERATION_COMMAND_SCHEMA,
                )
                .await
                .unwrap()
                .is_none()
        );
        let lease = store
            .claim_ready_for_schema(
                "grok-worker",
                30_000,
                AdmissionContract::LegacyV1,
                GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
            )
            .await
            .unwrap()
            .expect("Grok worker claims only its command schema");
        assert_eq!(lease.command_schema, GROK_IMAGE_GENERATION_COMMAND_SCHEMA);
    }

    #[test]
    fn unsupported_cli_options_fail_before_admission_side_effects() {
        let mut request = request();
        request.n = Some(2);
        assert_eq!(
            XaiImageAdmissionPlan::for_grok_cli(request),
            Err(XaiImageAdmissionError::UnsupportedBinding(
                XaiGrokProjectionError::UnsupportedOutputCount
            ))
        );
    }

    #[test]
    fn each_unavailable_official_field_is_rejected_with_its_xai_parameter() {
        let mut resolution = request();
        resolution.resolution = Some(XaiImageResolution::R2k);
        let error = XaiImageAdmissionPlan::for_grok_cli(resolution).unwrap_err();
        assert_eq!(
            error,
            XaiImageAdmissionError::UnsupportedBinding(
                XaiGrokProjectionError::UnsupportedResolution
            )
        );
        assert_eq!(
            XaiGrokProjectionError::UnsupportedResolution.parameter(),
            Some("resolution")
        );

        let mut response_format = request();
        response_format.response_format = None;
        let error = XaiImageAdmissionPlan::for_grok_cli(response_format).unwrap_err();
        assert_eq!(
            error,
            XaiImageAdmissionError::UnsupportedBinding(
                XaiGrokProjectionError::UnsupportedResponseFormat
            )
        );
        assert_eq!(
            XaiGrokProjectionError::UnsupportedResponseFormat.parameter(),
            Some("response_format")
        );

        let mut storage = request();
        storage.storage_options = Some(XaiImageStorageOptions {
            expires_after: Some(3_600),
            filename: "image.jpg".to_owned(),
            public_url: None,
        });
        let error = XaiImageAdmissionPlan::for_grok_cli(storage).unwrap_err();
        assert_eq!(
            error,
            XaiImageAdmissionError::UnsupportedBinding(
                XaiGrokProjectionError::UnsupportedStorageOptions
            )
        );
        assert_eq!(
            XaiGrokProjectionError::UnsupportedStorageOptions.parameter(),
            Some("storage_options")
        );

        let mut long_prompt = request();
        long_prompt.prompt = "x".repeat(1_025);
        assert!(XaiImageAdmissionPlan::for_grok_cli(long_prompt).is_ok());
    }
}
