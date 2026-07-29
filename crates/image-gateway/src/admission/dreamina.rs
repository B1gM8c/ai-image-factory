use std::collections::BTreeMap;

use image_api_contracts::dreamina::{
    DREAMINA_IMAGES_API_PROFILE, DREAMINA_VIDEOS_API_PROFILE, DreaminaImageGenerationRequest,
    DreaminaVideoGenerationRequest,
};
use image_provider_dreamina_cli::{
    DREAMINA_SUBMIT_COMMAND_SCHEMA, DreaminaSubmitRequestV1, ImageModelVersion, ImageRatio,
    ImageResolution, TextToImageRequestV1, TextToVideoRequestV1, VideoModelVersion, VideoRatio,
    VideoResolution, encode_submit_command,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::{
    AdmissionContract, AdmissionTicket, AttachJob, ClaimAdmission, GENERATION_OPERATION,
    VIDEO_GENERATION_OPERATION,
};

const PROVIDER_ID: &str = image_provider_dreamina_cli::PROVIDER_ID;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaImageAdmissionPlan {
    request_hash: String,
    provider_model_id: String,
    provider_model: String,
    provider_command: Value,
    output_count: u32,
    pricing_dimensions: BTreeMap<String, String>,
}

impl DreaminaImageAdmissionPlan {
    pub fn new(request: DreaminaImageGenerationRequest) -> Result<Self, DreaminaAdmissionError> {
        let model = parse_image_model(request.model_version.as_deref().unwrap_or("5.0"))?;
        let resolution = parse_image_resolution(&request.resolution_type)?;
        let output_count = request.generate_num.unwrap_or(1);
        let typed = match (request.ratio.as_deref(), request.width, request.height) {
            (ratio, None, None) => TextToImageRequestV1::new(
                request.prompt,
                model,
                parse_image_ratio(ratio.unwrap_or("16:9"))?,
                resolution,
                output_count,
            ),
            (None, Some(width), Some(height)) => TextToImageRequestV1::new_custom(
                request.prompt,
                model,
                width,
                height,
                resolution,
                output_count,
            ),
            _ => return Err(DreaminaAdmissionError::InvalidImageGeometry),
        }
        .map_err(DreaminaAdmissionError::InvalidProviderRequest)?;
        let mut pricing_dimensions = BTreeMap::from([(
            "resolution_type".to_string(),
            typed.resolution().as_str().to_string(),
        )]);
        if let Some(ratio) = typed.ratio() {
            pricing_dimensions.insert("ratio".to_string(), ratio.as_str().to_string());
        } else {
            pricing_dimensions.insert(
                "width".to_string(),
                typed
                    .width()
                    .expect("custom image geometry has a width")
                    .to_string(),
            );
            pricing_dimensions.insert(
                "height".to_string(),
                typed
                    .height()
                    .expect("custom image geometry has a height")
                    .to_string(),
            );
        }
        let bytes = encode_submit_command(DreaminaSubmitRequestV1::from(typed));
        let provider_command = serde_json::from_slice(&bytes)
            .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?;
        let durable_bytes = serde_json::to_vec(&provider_command)
            .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?;
        Ok(Self {
            request_hash: hex::encode(Sha256::digest(&durable_bytes)),
            provider_model_id: model.as_str().to_string(),
            provider_model: format!("dreamina-image-{}", model.as_str()),
            provider_command,
            output_count: u32::from(output_count),
            pricing_dimensions,
        })
    }

    pub fn provider_id(&self) -> &'static str {
        PROVIDER_ID
    }

    pub fn provider_model(&self) -> &str {
        &self.provider_model
    }

    pub fn provider_model_id(&self) -> &str {
        &self.provider_model_id
    }

    pub fn provider_command_hash(&self) -> &str {
        &self.request_hash
    }

    pub fn output_count(&self) -> u32 {
        self.output_count
    }

    pub fn resolution(&self) -> &str {
        self.pricing_dimensions
            .get("resolution_type")
            .expect("Dreamina image resolution is always frozen")
    }

    pub fn pricing_dimensions(&self) -> &BTreeMap<String, String> {
        &self.pricing_dimensions
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
        self.claim_for_profile(
            DREAMINA_IMAGES_API_PROFILE,
            owner_token,
            tenant_id,
            project_id,
            request_id,
            idempotency_key_digest,
            deadline_at_ms,
        )
    }

    pub fn claim_for_profile(
        &self,
        api_profile: &str,
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
            api_profile: api_profile.to_owned(),
            operation: GENERATION_OPERATION.to_owned(),
            request_id: request_id.into(),
            idempotency_key_digest,
            request_hash: self.request_hash.clone(),
            deadline_at_ms,
        }
    }

    pub fn attach(
        &self,
        ticket: AdmissionTicket,
        job_id: Uuid,
        schedule_scope: impl Into<String>,
    ) -> AttachJob {
        AttachJob {
            ticket,
            job_id,
            command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.to_owned(),
            command_json: self.provider_command.clone(),
            input_manifest: None,
            work_kind: "image_batch".to_owned(),
            schedule_scope: schedule_scope.into(),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: u64::from(self.output_count),
            contract: AdmissionContract::OutputEconomicsV2,
            customer_pricing: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaVideoAdmissionPlan {
    request_hash: String,
    provider_model_id: String,
    provider_command: Value,
    duration: u8,
    pricing_dimensions: BTreeMap<String, String>,
}

impl DreaminaVideoAdmissionPlan {
    pub fn new(request: DreaminaVideoGenerationRequest) -> Result<Self, DreaminaAdmissionError> {
        let model = parse_video_model(
            request
                .model_version
                .as_deref()
                .unwrap_or("seedance2.0fast"),
        )?;
        let ratio = parse_video_ratio(request.ratio.as_deref().unwrap_or("16:9"))?;
        let duration = request.duration.unwrap_or(5);
        let resolution = parse_video_resolution(&request.video_resolution)?;
        let typed = TextToVideoRequestV1::new(request.prompt, model, ratio, duration, resolution)
            .map_err(DreaminaAdmissionError::InvalidProviderRequest)?;
        let bytes = encode_submit_command(DreaminaSubmitRequestV1::from(typed));
        let provider_command = serde_json::from_slice(&bytes)
            .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?;
        let durable_bytes = serde_json::to_vec(&provider_command)
            .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?;
        Ok(Self {
            request_hash: hex::encode(Sha256::digest(&durable_bytes)),
            provider_model_id: model.as_str().to_owned(),
            provider_command,
            duration,
            pricing_dimensions: BTreeMap::from([
                ("duration".to_string(), duration.to_string()),
                ("ratio".to_string(), ratio.as_str().to_string()),
                ("resolution".to_string(), resolution.as_str().to_string()),
            ]),
        })
    }

    pub fn provider_id(&self) -> &'static str {
        PROVIDER_ID
    }

    pub fn provider_model(&self) -> &str {
        &self.provider_model_id
    }

    pub fn provider_model_id(&self) -> &str {
        &self.provider_model_id
    }

    pub fn provider_command_hash(&self) -> &str {
        &self.request_hash
    }

    pub fn duration(&self) -> u8 {
        self.duration
    }

    pub fn resolution(&self) -> &str {
        self.pricing_dimensions
            .get("resolution")
            .expect("Dreamina video resolution is always frozen")
    }

    pub fn pricing_dimensions(&self) -> &BTreeMap<String, String> {
        &self.pricing_dimensions
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
        self.claim_for_profile(
            DREAMINA_VIDEOS_API_PROFILE,
            owner_token,
            tenant_id,
            project_id,
            request_id,
            idempotency_key_digest,
            deadline_at_ms,
        )
    }

    pub fn claim_for_profile(
        &self,
        api_profile: &str,
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
            api_profile: api_profile.to_owned(),
            operation: VIDEO_GENERATION_OPERATION.to_owned(),
            request_id: request_id.into(),
            idempotency_key_digest,
            request_hash: self.request_hash.clone(),
            deadline_at_ms,
        }
    }

    pub fn attach(
        &self,
        ticket: AdmissionTicket,
        job_id: Uuid,
        schedule_scope: impl Into<String>,
    ) -> AttachJob {
        AttachJob {
            ticket,
            job_id,
            command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.to_owned(),
            command_json: self.provider_command.clone(),
            input_manifest: None,
            work_kind: "video_single".to_owned(),
            schedule_scope: schedule_scope.into(),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: u64::from(self.duration),
            contract: AdmissionContract::MediaEconomicsV3,
            customer_pricing: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DreaminaAdmissionError {
    #[error("Dreamina image geometry must use either ratio or a complete width/height pair")]
    InvalidImageGeometry,
    #[error("Dreamina model_version is unsupported")]
    InvalidModel,
    #[error("Dreamina ratio is unsupported")]
    InvalidRatio,
    #[error("Dreamina resolution is unsupported")]
    InvalidResolution,
    #[error(transparent)]
    InvalidProviderRequest(image_provider_dreamina_cli::RequestValidationError),
    #[error("Dreamina request could not be encoded as an immutable provider command")]
    InvalidProviderCommand,
}

impl DreaminaAdmissionError {
    pub const fn parameter(self) -> &'static str {
        match self {
            Self::InvalidImageGeometry => "ratio",
            Self::InvalidModel => "model_version",
            Self::InvalidRatio => "ratio",
            Self::InvalidResolution => "resolution_type",
            Self::InvalidProviderRequest(_) => "request",
            Self::InvalidProviderCommand => "request",
        }
    }
}

fn parse_image_model(value: &str) -> Result<ImageModelVersion, DreaminaAdmissionError> {
    match value {
        "3.0" => Ok(ImageModelVersion::V3_0),
        "3.1" => Ok(ImageModelVersion::V3_1),
        "4.0" => Ok(ImageModelVersion::V4_0),
        "4.1" => Ok(ImageModelVersion::V4_1),
        "4.5" => Ok(ImageModelVersion::V4_5),
        "4.6" => Ok(ImageModelVersion::V4_6),
        "4.7" => Ok(ImageModelVersion::V4_7),
        "5.0" => Ok(ImageModelVersion::V5_0),
        "5.0Pro" => Ok(ImageModelVersion::V5_0Pro),
        _ => Err(DreaminaAdmissionError::InvalidModel),
    }
}

fn parse_image_ratio(value: &str) -> Result<ImageRatio, DreaminaAdmissionError> {
    match value {
        "21:9" => Ok(ImageRatio::R21x9),
        "16:9" => Ok(ImageRatio::R16x9),
        "3:2" => Ok(ImageRatio::R3x2),
        "4:3" => Ok(ImageRatio::R4x3),
        "1:1" => Ok(ImageRatio::R1x1),
        "3:4" => Ok(ImageRatio::R3x4),
        "2:3" => Ok(ImageRatio::R2x3),
        "9:16" => Ok(ImageRatio::R9x16),
        _ => Err(DreaminaAdmissionError::InvalidRatio),
    }
}

fn parse_image_resolution(value: &str) -> Result<ImageResolution, DreaminaAdmissionError> {
    match value {
        "1k" => Ok(ImageResolution::K1),
        "2k" => Ok(ImageResolution::K2),
        "4k" => Ok(ImageResolution::K4),
        _ => Err(DreaminaAdmissionError::InvalidResolution),
    }
}

fn parse_video_model(value: &str) -> Result<VideoModelVersion, DreaminaAdmissionError> {
    match value {
        "seedance2.0" => Ok(VideoModelVersion::Seedance2_0),
        "seedance2.0fast" => Ok(VideoModelVersion::Seedance2_0Fast),
        "seedance2.0_vip" => Ok(VideoModelVersion::Seedance2_0Vip),
        "seedance2.0fast_vip" => Ok(VideoModelVersion::Seedance2_0FastVip),
        "seedance2.0mini" => Ok(VideoModelVersion::Seedance2_0Mini),
        _ => Err(DreaminaAdmissionError::InvalidModel),
    }
}

fn parse_video_ratio(value: &str) -> Result<VideoRatio, DreaminaAdmissionError> {
    match value {
        "1:1" => Ok(VideoRatio::R1x1),
        "3:4" => Ok(VideoRatio::R3x4),
        "16:9" => Ok(VideoRatio::R16x9),
        "4:3" => Ok(VideoRatio::R4x3),
        "9:16" => Ok(VideoRatio::R9x16),
        "21:9" => Ok(VideoRatio::R21x9),
        _ => Err(DreaminaAdmissionError::InvalidRatio),
    }
}

fn parse_video_resolution(value: &str) -> Result<VideoResolution, DreaminaAdmissionError> {
    match value {
        "720p" => Ok(VideoResolution::P720),
        "1080p" => Ok(VideoResolution::P1080),
        "4k" => Ok(VideoResolution::K4),
        _ => Err(DreaminaAdmissionError::InvalidResolution),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use image_provider_dreamina_cli::parse_submit_command;

    #[test]
    fn image_batch_is_frozen_once_and_split_by_output_slot_later() {
        let plan = DreaminaImageAdmissionPlan::new(DreaminaImageGenerationRequest {
            prompt: "a city".to_owned(),
            model_version: Some("5.0Pro".to_owned()),
            ratio: None,
            resolution_type: "2k".to_owned(),
            width: Some(1536),
            height: Some(1024),
            generate_num: Some(3),
        })
        .unwrap();
        assert_eq!(plan.output_count(), 3);
        assert_eq!(plan.provider_model(), "dreamina-image-5.0Pro");
        let parsed =
            parse_submit_command(&serde_json::to_vec(&plan.provider_command).unwrap()).unwrap();
        assert_eq!(parsed.output_count(), 3);
        let attach = plan.attach(
            AdmissionTicket {
                session_id: Uuid::new_v4(),
                owner_token: Uuid::new_v4(),
                request_hash: plan.request_hash.clone(),
            },
            Uuid::new_v4(),
            "tenant:test",
        );
        assert_eq!(
            crate::admission::attach_operation(&attach).unwrap(),
            GENERATION_OPERATION
        );
    }

    #[test]
    fn video_defaults_match_the_current_cli_guide() {
        let plan = DreaminaVideoAdmissionPlan::new(DreaminaVideoGenerationRequest {
            prompt: "camera push in".to_owned(),
            model_version: None,
            ratio: None,
            duration: None,
            video_resolution: "720p".to_owned(),
        })
        .unwrap();
        assert_eq!(plan.provider_model(), "seedance2.0fast");
        assert_eq!(plan.provider_model_id(), "seedance2.0fast");
        assert_eq!(plan.duration(), 5);
        assert_eq!(
            plan.pricing_dimensions(),
            &BTreeMap::from([
                ("duration".to_string(), "5".to_string()),
                ("ratio".to_string(), "16:9".to_string()),
                ("resolution".to_string(), "720p".to_string()),
            ])
        );
        let attach = plan.attach(
            AdmissionTicket {
                session_id: Uuid::new_v4(),
                owner_token: Uuid::new_v4(),
                request_hash: plan.request_hash.clone(),
            },
            Uuid::new_v4(),
            "tenant:test",
        );
        assert_eq!(
            crate::admission::attach_operation(&attach).unwrap(),
            VIDEO_GENERATION_OPERATION
        );
    }
}
