use std::collections::{BTreeMap, HashSet};

use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::input_blobs::InputBlobRef;
use image_provider_dreamina_cli::{DREAMINA_SUBMIT_COMMAND_SCHEMA, parse_submit_command};
use image_provider_grok_cli::{
    GROK_IMAGE_EDIT_COMMAND_SCHEMA, GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
    GROK_VIDEO_GENERATION_COMMAND_SCHEMA, GrokVideoGenerationRequestV1, StagedImageV1,
    parse_image_edit_payload, parse_image_generation_payload, parse_video_generation_payload,
};

mod ark;
pub mod command;
mod dreamina;
mod memory;
mod postgres;
mod xai_image_edits;
mod xai_images;
mod xai_videos;

pub use ark::{ArkAdmissionError, ArkImageAdmissionPlan, ArkVideoAdmissionPlan};
pub use command::{
    EDIT_COMMAND_SCHEMA, EDIT_COMMAND_SCHEMA_VERSION, EDIT_INPUT_MANIFEST_SCHEMA, EDIT_OPERATION,
    EditCommandV1, EditInputDescriptorV1, EditInputRoleV1, GENERATION_COMMAND_SCHEMA,
    GENERATION_COMMAND_SCHEMA_VERSION, GENERATION_OPERATION, GenerationCommandV1,
    IdempotencyKeyError, idempotency_key_digest, validate_idempotency_key,
};
pub use dreamina::{
    DreaminaAdmissionError, DreaminaImageAdmissionPlan, DreaminaVideoAdmissionPlan,
};
pub use memory::InMemoryAdmissionStore;
pub use postgres::PostgresAdmissionStore;
pub use xai_image_edits::{
    XaiImageEditAdmissionError, XaiImageEditAdmissionPlan, XaiImageEditFallbackMode,
};
pub use xai_images::{XaiImageAdmissionError, XaiImageAdmissionPlan};
pub use xai_videos::{
    VIDEO_GENERATION_OPERATION, XAI_VIDEO_INPUT_MANIFEST_SCHEMA, XaiVideoAdmissionError,
    XaiVideoAdmissionInput, XaiVideoAdmissionIntent, XaiVideoAdmissionPlan,
};

#[derive(Clone, Debug)]
pub struct ClaimAdmission {
    pub owner_token: Uuid,
    pub tenant_id: String,
    pub project_id: String,
    pub api_profile: String,
    pub operation: String,
    pub request_id: String,
    pub idempotency_key_digest: Option<String>,
    pub request_hash: String,
    pub deadline_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionTicket {
    pub session_id: Uuid,
    pub owner_token: Uuid,
    pub request_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionClaim {
    Owner(AdmissionTicket),
    InProgress { session_id: Uuid },
    Existing { job_id: Uuid, state: String },
    Conflict { job_id: Option<Uuid> },
}

#[derive(Clone, Debug)]
pub struct AttachJob {
    pub ticket: AdmissionTicket,
    pub job_id: Uuid,
    pub command_schema: String,
    pub command_json: Value,
    pub input_manifest: Option<AttachInputManifest>,
    pub work_kind: String,
    pub schedule_scope: String,
    pub schedule_weight: u32,
    pub schedule_priority: u8,
    pub schedule_cost: u64,
    pub contract: AdmissionContract,
    pub customer_pricing: Option<CustomerPricingIntent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionContract {
    LegacyV1,
    OutputEconomicsV2,
    MediaEconomicsV3,
    CustomerPricingV4,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomerPricingIntent {
    pub public_model_id: String,
    pub provider_model_id: String,
    pub execution_model_id: String,
    pub provider_command_hash: Option<String>,
    pub media_kind: String,
    pub service_tier: String,
    pub service_tier_decision: crate::service_tiers::ServiceTierDecision,
    pub execution_surface: String,
    pub currency: String,
    pub pricing_dimensions: BTreeMap<String, String>,
    pub processing_mode: PricingProcessingMode,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PricingProcessingMode {
    #[default]
    Synchronous,
    Batch,
}

impl PricingProcessingMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Synchronous => "synchronous",
            Self::Batch => "batch",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachInputManifest {
    pub manifest_schema: String,
    pub manifest_hash: String,
    pub inputs: Vec<AttachInputObject>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachInputObject {
    pub blob: InputBlobRef,
    pub role: EditInputRoleV1,
    pub index: u16,
    pub media_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachedWork {
    pub work_item_id: Uuid,
    pub job_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkLease {
    pub work_item_id: Uuid,
    pub job_id: Uuid,
    pub execution_id: Uuid,
    pub lease_epoch: i64,
    pub worker_id: String,
    pub command_schema: String,
    pub command_json: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkOutcome {
    Succeeded,
    Failed,
    Uncertain,
}

impl WorkOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Uncertain => "uncertain",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("durable admission storage is unavailable")]
    Unavailable,
    #[error("admission deadline has expired")]
    Expired,
    #[error("admission owner or state is invalid")]
    InvalidOwner,
    #[error("work lease is stale or invalid")]
    StaleLease,
    #[error("durable command payload must be a JSON object")]
    InvalidCommand,
    #[error("no published price is available for this image route")]
    PricingUnavailable,
    #[error("billing credit is insufficient for this image request")]
    BillingLimitExceeded,
    #[error("project monthly hard spend limit exceeded")]
    ProjectBudgetExceeded,
}

pub(crate) fn validate_attach_request(request: &AttachJob) -> Result<(), AdmissionError> {
    match (request.contract, request.customer_pricing.as_ref()) {
        (AdmissionContract::CustomerPricingV4, None) => {
            return Err(AdmissionError::PricingUnavailable);
        }
        (AdmissionContract::CustomerPricingV4, Some(_)) | (_, None) => {}
        (_, Some(_)) => return Err(AdmissionError::InvalidCommand),
    }
    if !request.command_json.is_object() {
        return Err(AdmissionError::InvalidCommand);
    }
    let provider_command_hash = provider_command_hash(request)?;
    let Some(manifest) = request.input_manifest.as_ref() else {
        return match request.command_schema.as_str() {
            GENERATION_COMMAND_SCHEMA => {
                let command: GenerationCommandV1 =
                    serde_json::from_value(request.command_json.clone())
                        .map_err(|_| AdmissionError::InvalidCommand)?;
                if command.schema_version == GENERATION_COMMAND_SCHEMA_VERSION
                    && command.operation == GENERATION_OPERATION
                    && command.request_hash_hex() == provider_command_hash
                {
                    Ok(())
                } else {
                    Err(AdmissionError::InvalidCommand)
                }
            }
            GROK_IMAGE_GENERATION_COMMAND_SCHEMA => {
                let bytes = serde_json::to_vec(&request.command_json)
                    .map_err(|_| AdmissionError::InvalidCommand)?;
                let payload = parse_image_generation_payload(&bytes)
                    .map_err(|_| AdmissionError::InvalidCommand)?;
                if payload.source_command_sha256() == provider_command_hash {
                    Ok(())
                } else {
                    Err(AdmissionError::InvalidCommand)
                }
            }
            DREAMINA_SUBMIT_COMMAND_SCHEMA => {
                let bytes = serde_json::to_vec(&request.command_json)
                    .map_err(|_| AdmissionError::InvalidCommand)?;
                parse_submit_command(&bytes).map_err(|_| AdmissionError::InvalidCommand)?;
                if hex::encode(Sha256::digest(&bytes)) == provider_command_hash {
                    Ok(())
                } else {
                    Err(AdmissionError::InvalidCommand)
                }
            }
            _ => Err(AdmissionError::InvalidCommand),
        };
    };
    if request.command_schema == GROK_VIDEO_GENERATION_COMMAND_SCHEMA {
        return validate_video_attach_request(request, manifest);
    }
    if request.command_schema == GROK_IMAGE_EDIT_COMMAND_SCHEMA {
        return validate_grok_image_edit_attach_request(request, manifest);
    }
    if request.command_schema != EDIT_COMMAND_SCHEMA
        || manifest.manifest_schema != EDIT_INPUT_MANIFEST_SCHEMA
        || manifest.inputs.is_empty()
        || manifest.inputs.len() > 17
        || !is_sha256(&manifest.manifest_hash)
    {
        return Err(AdmissionError::InvalidCommand);
    }
    let command: EditCommandV1 = serde_json::from_value(request.command_json.clone())
        .map_err(|_| AdmissionError::InvalidCommand)?;
    if command.schema_version != EDIT_COMMAND_SCHEMA_VERSION
        || command.operation != EDIT_OPERATION
        || command.provider_id.is_empty()
        || command.model.is_empty()
        || command.source_api_profile.is_empty()
        || command
            .moderation
            .as_deref()
            .is_some_and(|value| !matches!(value, "auto" | "low"))
        || command.request_hash_hex() != provider_command_hash
        || command.input_manifest_hash_hex() != manifest.manifest_hash
        || command.inputs.len() != manifest.inputs.len()
    {
        return Err(AdmissionError::InvalidCommand);
    }
    let mut positions = HashSet::new();
    let mut input_ids = HashSet::new();
    let mut object_keys = HashSet::new();
    let mut image_count = 0_usize;
    let mut mask_count = 0_usize;
    for (descriptor, input) in command.inputs.iter().zip(&manifest.inputs) {
        let expected = EditInputDescriptorV1 {
            byte_size: input.blob.byte_size,
            index: input.index,
            media_type: input.media_type.clone(),
            role: input.role,
            sha256_hex: input.blob.sha256_hex.clone(),
        };
        if descriptor != &expected
            || input.blob.key.admission_session_id != request.ticket.session_id
            || input.blob.byte_size == 0
            || input.blob.storage_backend.is_empty()
            || input.blob.object_key.is_empty()
            || !is_sha256(&input.blob.sha256_hex)
            || !positions.insert((input.role.as_str(), input.index))
            || !input_ids.insert(input.blob.key.input_id)
            || !object_keys.insert((
                input.blob.storage_backend.as_str(),
                input.blob.object_key.as_str(),
            ))
        {
            return Err(AdmissionError::InvalidCommand);
        }
        match input.role {
            EditInputRoleV1::Image => {
                image_count += 1;
                if input.index >= 16
                    || !matches!(
                        input.media_type.as_str(),
                        "image/png" | "image/jpeg" | "image/webp"
                    )
                {
                    return Err(AdmissionError::InvalidCommand);
                }
            }
            EditInputRoleV1::Mask => {
                mask_count += 1;
                if input.index != 0 || input.media_type != "image/png" {
                    return Err(AdmissionError::InvalidCommand);
                }
            }
        }
    }
    if !(1..=16).contains(&image_count) || mask_count > 1 {
        return Err(AdmissionError::InvalidCommand);
    }
    Ok(())
}

pub(crate) fn provider_command_hash(request: &AttachJob) -> Result<String, AdmissionError> {
    let derived_hash = match request.command_schema.as_str() {
        GENERATION_COMMAND_SCHEMA => {
            let command: GenerationCommandV1 = serde_json::from_value(request.command_json.clone())
                .map_err(|_| AdmissionError::InvalidCommand)?;
            command.request_hash_hex()
        }
        EDIT_COMMAND_SCHEMA => {
            let command: EditCommandV1 = serde_json::from_value(request.command_json.clone())
                .map_err(|_| AdmissionError::InvalidCommand)?;
            command.request_hash_hex()
        }
        GROK_IMAGE_GENERATION_COMMAND_SCHEMA => {
            let bytes = serde_json::to_vec(&request.command_json)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            parse_image_generation_payload(&bytes)
                .map_err(|_| AdmissionError::InvalidCommand)?
                .source_command_sha256()
                .to_string()
        }
        GROK_IMAGE_EDIT_COMMAND_SCHEMA => {
            let bytes = serde_json::to_vec(&request.command_json)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            parse_image_edit_payload(&bytes)
                .map_err(|_| AdmissionError::InvalidCommand)?
                .source_command_sha256()
                .to_string()
        }
        GROK_VIDEO_GENERATION_COMMAND_SCHEMA => {
            let bytes = serde_json::to_vec(&request.command_json)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            parse_video_generation_payload(&bytes)
                .map_err(|_| AdmissionError::InvalidCommand)?
                .source_command_sha256()
                .to_string()
        }
        DREAMINA_SUBMIT_COMMAND_SCHEMA => {
            let bytes = serde_json::to_vec(&request.command_json)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            parse_submit_command(&bytes).map_err(|_| AdmissionError::InvalidCommand)?;
            hex::encode(Sha256::digest(&bytes))
        }
        _ => return Err(AdmissionError::InvalidCommand),
    };
    let explicit_hash = request
        .customer_pricing
        .as_ref()
        .and_then(|intent| intent.provider_command_hash.as_deref());
    match explicit_hash {
        Some(hash) if is_sha256(hash) && hash == derived_hash => Ok(derived_hash),
        Some(_) => Err(AdmissionError::InvalidCommand),
        None => Ok(derived_hash),
    }
}

fn validate_grok_image_edit_attach_request(
    request: &AttachJob,
    manifest: &AttachInputManifest,
) -> Result<(), AdmissionError> {
    if manifest.manifest_schema != EDIT_INPUT_MANIFEST_SCHEMA
        || manifest.inputs.is_empty()
        || manifest.inputs.len() > image_provider_grok_cli::MAX_IMAGE_EDIT_REFERENCES
        || !is_sha256(&manifest.manifest_hash)
    {
        return Err(AdmissionError::InvalidCommand);
    }
    let bytes =
        serde_json::to_vec(&request.command_json).map_err(|_| AdmissionError::InvalidCommand)?;
    let payload = parse_image_edit_payload(&bytes).map_err(|_| AdmissionError::InvalidCommand)?;
    if payload.source_command_sha256() != provider_command_hash(request)?
        || payload.request().images().len() != manifest.inputs.len()
    {
        return Err(AdmissionError::InvalidCommand);
    }

    let mut descriptors = Vec::with_capacity(manifest.inputs.len());
    let mut input_ids = HashSet::new();
    let mut object_keys = HashSet::new();
    for (index, (expected, input)) in payload
        .request()
        .images()
        .iter()
        .zip(&manifest.inputs)
        .enumerate()
    {
        if input.role != EditInputRoleV1::Image
            || usize::from(input.index) != index
            || input.blob.key.admission_session_id != request.ticket.session_id
            || input.blob.sha256_hex != expected.sha256()
            || input.blob.byte_size == 0
            || input.blob.storage_backend.is_empty()
            || input.blob.object_key.is_empty()
            || !is_sha256(&input.blob.sha256_hex)
            || !matches!(
                input.media_type.as_str(),
                "image/png" | "image/jpeg" | "image/webp"
            )
            || !input_ids.insert(input.blob.key.input_id)
            || !object_keys.insert((
                input.blob.storage_backend.as_str(),
                input.blob.object_key.as_str(),
            ))
        {
            return Err(AdmissionError::InvalidCommand);
        }
        descriptors.push(EditInputDescriptorV1 {
            byte_size: input.blob.byte_size,
            index: input.index,
            media_type: input.media_type.clone(),
            role: input.role,
            sha256_hex: input.blob.sha256_hex.clone(),
        });
    }
    let descriptor_bytes =
        serde_json::to_vec(&descriptors).map_err(|_| AdmissionError::InvalidCommand)?;
    if hex::encode(Sha256::digest(descriptor_bytes)) != manifest.manifest_hash {
        return Err(AdmissionError::InvalidCommand);
    }
    Ok(())
}

fn validate_video_attach_request(
    request: &AttachJob,
    manifest: &AttachInputManifest,
) -> Result<(), AdmissionError> {
    if !matches!(
        request.contract,
        AdmissionContract::MediaEconomicsV3 | AdmissionContract::CustomerPricingV4
    ) || manifest.manifest_schema != XAI_VIDEO_INPUT_MANIFEST_SCHEMA
        || manifest.inputs.is_empty()
        || !is_sha256(&manifest.manifest_hash)
    {
        return Err(AdmissionError::InvalidCommand);
    }
    let bytes =
        serde_json::to_vec(&request.command_json).map_err(|_| AdmissionError::InvalidCommand)?;
    let payload =
        parse_video_generation_payload(&bytes).map_err(|_| AdmissionError::InvalidCommand)?;
    if payload.source_command_sha256() != provider_command_hash(request)? {
        return Err(AdmissionError::InvalidCommand);
    }
    let expected_images: &[StagedImageV1] = match payload.request() {
        GrokVideoGenerationRequestV1::ImageToVideo(video) => std::slice::from_ref(video.image()),
        GrokVideoGenerationRequestV1::ReferenceToVideo(video) => video.images(),
    };
    if expected_images.len() != manifest.inputs.len()
        || !xai_videos::video_input_manifest_hash_matches(expected_images, manifest)
    {
        return Err(AdmissionError::InvalidCommand);
    }

    let mut input_ids = HashSet::new();
    let mut object_keys = HashSet::new();
    for (index, (expected, input)) in expected_images.iter().zip(&manifest.inputs).enumerate() {
        if input.role != EditInputRoleV1::Image
            || usize::from(input.index) != index
            || input.blob.key.admission_session_id != request.ticket.session_id
            || input.blob.sha256_hex != expected.sha256()
            || input.blob.byte_size == 0
            || input.blob.storage_backend.is_empty()
            || input.blob.object_key.is_empty()
            || !matches!(
                input.media_type.as_str(),
                "image/png" | "image/jpeg" | "image/webp"
            )
            || !input_ids.insert(input.blob.key.input_id)
            || !object_keys.insert((
                input.blob.storage_backend.as_str(),
                input.blob.object_key.as_str(),
            ))
        {
            return Err(AdmissionError::InvalidCommand);
        }
    }
    Ok(())
}

pub(crate) fn attach_operation(request: &AttachJob) -> Result<&'static str, AdmissionError> {
    match request.command_schema.as_str() {
        GENERATION_COMMAND_SCHEMA => Ok(GENERATION_OPERATION),
        GROK_IMAGE_GENERATION_COMMAND_SCHEMA => Ok(GENERATION_OPERATION),
        GROK_IMAGE_EDIT_COMMAND_SCHEMA => Ok(EDIT_OPERATION),
        GROK_VIDEO_GENERATION_COMMAND_SCHEMA => Ok(VIDEO_GENERATION_OPERATION),
        DREAMINA_SUBMIT_COMMAND_SCHEMA => {
            let bytes = serde_json::to_vec(&request.command_json)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            match parse_submit_command(&bytes).map_err(|_| AdmissionError::InvalidCommand)? {
                image_provider_dreamina_cli::DreaminaSubmitRequestV1::TextToImage(_) => {
                    Ok(GENERATION_OPERATION)
                }
                image_provider_dreamina_cli::DreaminaSubmitRequestV1::TextToVideo(_) => {
                    Ok(VIDEO_GENERATION_OPERATION)
                }
            }
        }
        EDIT_COMMAND_SCHEMA => Ok(EDIT_OPERATION),
        _ => Err(AdmissionError::InvalidCommand),
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[async_trait]
pub trait AdmissionStore: Send + Sync + 'static {
    async fn claim(&self, request: ClaimAdmission) -> Result<AdmissionClaim, AdmissionError>;

    async fn attach(&self, request: AttachJob) -> Result<AttachedWork, AdmissionError>;

    async fn attach_and_start(
        &self,
        request: AttachJob,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<WorkLease, AdmissionError> {
        let attached = self.attach(request).await?;
        let lease = self
            .claim_job(attached.job_id, worker_id, lease_duration_ms)
            .await?
            .ok_or(AdmissionError::Unavailable)?;
        self.start(&lease).await?;
        Ok(lease)
    }

    async fn abort(&self, ticket: &AdmissionTicket) -> Result<(), AdmissionError>;

    async fn claim_ready(
        &self,
        worker_id: &str,
        lease_duration_ms: i64,
        contract: AdmissionContract,
    ) -> Result<Option<WorkLease>, AdmissionError>;

    async fn claim_ready_for_schema(
        &self,
        worker_id: &str,
        lease_duration_ms: i64,
        contract: AdmissionContract,
        command_schema: &str,
    ) -> Result<Option<WorkLease>, AdmissionError>;

    async fn claim_ready_for_profile(
        &self,
        worker_id: &str,
        lease_duration_ms: i64,
        contract: AdmissionContract,
        command_schema: &str,
        _execution_profile_id: Uuid,
    ) -> Result<Option<WorkLease>, AdmissionError> {
        self.claim_ready_for_schema(worker_id, lease_duration_ms, contract, command_schema)
            .await
    }

    async fn claim_job(
        &self,
        job_id: Uuid,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<Option<WorkLease>, AdmissionError>;

    async fn start(&self, lease: &WorkLease) -> Result<(), AdmissionError>;

    async fn heartbeat(
        &self,
        lease: &WorkLease,
        lease_duration_ms: i64,
    ) -> Result<(), AdmissionError>;

    async fn settle(
        &self,
        lease: &WorkLease,
        outcome: WorkOutcome,
        error_code: Option<&str>,
    ) -> Result<(), AdmissionError>;
}
