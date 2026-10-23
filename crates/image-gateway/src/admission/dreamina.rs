use std::collections::BTreeMap;

use image_api_contracts::dreamina::{
    DREAMINA_IMAGES_API_PROFILE, DREAMINA_VIDEOS_API_PROFILE, DreaminaImageGenerationRequest,
    DreaminaImageWorkflowRequest, DreaminaMultiframeToVideoRequest, DreaminaVideoGenerationRequest,
    DreaminaVideoWorkflowRequest,
};
use image_provider_dreamina_cli::{
    DREAMINA_SUBMIT_COMMAND_SCHEMA, DreaminaSubmitRequestV1, FramesToVideoRequestV1,
    ImageModelVersion, ImageRatio, ImageResolution, ImageToImageRequestV1, ImageToVideoRequestV1,
    ImageUpscaleRequestV1, MultiframeToVideoRequestV1, MultimodalToVideoRequestV1,
    ReferenceVideoOptionsV1, StagedImageV1, StagedInputKindV1, StagedTimedMediaV1,
    TextToImageRequestV1, TextToVideoRequestV1, UpscaleResolution, VideoModelVersion, VideoRatio,
    VideoResolution, VideoTransitionV1, encode_submit_command, parse_submit_command,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::{
    AdmissionContract, AdmissionTicket, AttachInputManifest, AttachInputObject, AttachJob,
    ClaimAdmission, EditInputRoleV1, GENERATION_OPERATION, VIDEO_GENERATION_OPERATION,
};

const PROVIDER_ID: &str = image_provider_dreamina_cli::PROVIDER_ID;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaImageAdmissionPlan {
    request_hash: String,
    provider_command_hash: String,
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
        if resolution == ImageResolution::K1_5 {
            pricing_dimensions.insert("workflow".to_owned(), "text2image".to_owned());
        }
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
        let provider_command: Value = serde_json::from_slice(&bytes)
            .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?;
        let durable_bytes = serde_json::to_vec(&provider_command)
            .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?;
        // Execution format upgrades must not change an existing public key's
        // request identity. Version 1 is the historical image request identity.
        let mut request_identity = provider_command.clone();
        request_identity["schema_version"] = Value::from(1);
        Ok(Self {
            request_hash: source_hash(&request_identity)?,
            provider_command_hash: hex::encode(Sha256::digest(&durable_bytes)),
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
        &self.provider_command_hash
    }

    pub fn request_identity_hash(&self) -> &str {
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

pub const DREAMINA_INPUT_MANIFEST_SCHEMA: &str = "dreamina-cli.inputs.v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaInputSource {
    pub kind: StagedInputKindV1,
    pub parameter: &'static str,
    pub url: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WorkflowRequest {
    Image(DreaminaImageWorkflowRequest),
    Video(DreaminaVideoWorkflowRequest),
}

/// Source identity is frozen before downloading; content identity is frozen after probing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaWorkflowIntent {
    request: WorkflowRequest,
    request_hash: String,
    provider_model_id: String,
    sources: Vec<DreaminaInputSource>,
}

#[derive(Clone, Debug, Default)]
pub struct DreaminaStagedInputs {
    pub images: Vec<StagedImageV1>,
    pub videos: Vec<StagedTimedMediaV1>,
    pub audios: Vec<StagedTimedMediaV1>,
    pub objects: Vec<AttachInputObject>,
}

impl DreaminaWorkflowIntent {
    pub fn image(request: DreaminaImageWorkflowRequest) -> Result<Self, DreaminaAdmissionError> {
        use DreaminaImageWorkflowRequest::*;
        let (model, urls, parameter) = match &request {
            TextToImage(parameters) => {
                DreaminaImageAdmissionPlan::new(parameters.clone())?;
                (
                    parameters.model_version.as_deref().unwrap_or("5.0"),
                    Vec::new(),
                    "images",
                )
            }
            ImageToImage(parameters) => {
                if !(1..=10).contains(&parameters.images.len()) {
                    return Err(DreaminaAdmissionError::InvalidInputs);
                }
                let model = parameters.model_version.as_deref().unwrap_or("5.0");
                if matches!(
                    parse_image_model(model)?,
                    ImageModelVersion::V3_0 | ImageModelVersion::V3_1
                ) {
                    return Err(DreaminaAdmissionError::InvalidModel);
                }
                DreaminaImageAdmissionPlan::new(image_parameters(parameters.clone()))?;
                (model, parameters.images.clone(), "images")
            }
            ImageUpscale(parameters) => {
                parse_upscale_resolution(&parameters.resolution_type)?;
                ("image_upscale", vec![parameters.image.clone()], "image")
            }
        };
        let provider_model_id = model.to_owned();
        let sources = urls
            .into_iter()
            .map(|url| DreaminaInputSource {
                kind: StagedInputKindV1::Image,
                parameter,
                url,
            })
            .collect();
        let request_hash = source_hash(&request)?;
        Ok(Self {
            request: WorkflowRequest::Image(request),
            request_hash,
            provider_model_id,
            sources,
        })
    }

    pub fn video(request: DreaminaVideoWorkflowRequest) -> Result<Self, DreaminaAdmissionError> {
        use DreaminaVideoWorkflowRequest::*;
        let mut sources = Vec::new();
        let mut add = |kind, parameter, urls: Vec<String>| {
            sources.extend(urls.into_iter().map(|url| DreaminaInputSource {
                kind,
                parameter,
                url,
            }));
        };
        let model = match &request {
            TextToVideo(parameters) => {
                DreaminaVideoAdmissionPlan::new(parameters.clone())?;
                parameters
                    .model_version
                    .as_deref()
                    .unwrap_or("seedance2.0fast")
            }
            ImageToVideo(parameters) => {
                reference_options(
                    &parameters.prompt,
                    parameters.model_version.as_deref(),
                    parameters.ratio.as_deref(),
                    parameters.duration,
                    &parameters.video_resolution,
                )?;
                add(
                    StagedInputKindV1::Image,
                    "image",
                    vec![parameters.image.clone()],
                );
                parameters
                    .model_version
                    .as_deref()
                    .unwrap_or("seedance2.0_vip")
            }
            FramesToVideo(parameters) => {
                let options = reference_options(
                    &parameters.prompt,
                    parameters.model_version.as_deref(),
                    parameters.ratio.as_deref(),
                    parameters.duration,
                    &parameters.video_resolution,
                )?;
                if options.model() == VideoModelVersion::Seedance1_0Fast {
                    return Err(DreaminaAdmissionError::InvalidModel);
                }
                add(
                    StagedInputKindV1::Image,
                    "first",
                    vec![parameters.first.clone()],
                );
                add(
                    StagedInputKindV1::Image,
                    "last",
                    vec![parameters.last.clone()],
                );
                parameters
                    .model_version
                    .as_deref()
                    .unwrap_or("seedance2.0_vip")
            }
            MultiframeToVideo(parameters) => {
                if !(2..=20).contains(&parameters.images.len()) {
                    return Err(DreaminaAdmissionError::InvalidInputs);
                }
                multiframe_transitions(parameters)?;
                if !matches!(
                    parse_video_resolution(&parameters.video_resolution)?,
                    VideoResolution::P720 | VideoResolution::P1080
                ) {
                    return Err(DreaminaAdmissionError::InvalidResolution);
                }
                add(
                    StagedInputKindV1::Image,
                    "images",
                    parameters.images.clone(),
                );
                "multiframe2video"
            }
            MultimodalToVideo(parameters) => {
                let model_name = parameters
                    .model_version
                    .as_deref()
                    .unwrap_or("seedance2.0_vip");
                let model = parse_video_model(model_name)?;
                if matches!(
                    model,
                    VideoModelVersion::Seedance1_0Fast | VideoModelVersion::Seedance1_5Pro
                ) {
                    return Err(DreaminaAdmissionError::InvalidModel);
                }
                // This checks model, resolution and duration without inventing media metadata.
                TextToVideoRequestV1::new(
                    "validate options",
                    model,
                    parse_video_ratio(parameters.ratio.as_deref().unwrap_or("16:9"))?,
                    parameters.duration.unwrap_or(5),
                    parse_video_resolution(&parameters.video_resolution)?,
                )
                .map_err(DreaminaAdmissionError::InvalidProviderRequest)?;
                let (max_images, max_timed, max_total) = if model == VideoModelVersion::Seedance2_5
                {
                    (30, 10, 50)
                } else {
                    (9, 3, 12)
                };
                let count =
                    parameters.image.len() + parameters.video.len() + parameters.audio.len();
                if count == 0
                    || count > max_total
                    || parameters.image.len() > max_images
                    || parameters.video.len() > max_timed
                    || parameters.audio.len() > max_timed
                    || (model != VideoModelVersion::Seedance2_5
                        && parameters.image.is_empty()
                        && parameters.video.is_empty())
                {
                    return Err(DreaminaAdmissionError::InvalidInputs);
                }
                if parameters.prompt.contains('\0') {
                    return Err(DreaminaAdmissionError::InvalidInputs);
                }
                add(StagedInputKindV1::Image, "image", parameters.image.clone());
                add(StagedInputKindV1::Video, "video", parameters.video.clone());
                add(StagedInputKindV1::Audio, "audio", parameters.audio.clone());
                model_name
            }
        };
        let provider_model_id = model.to_owned();
        let request_hash = source_hash(&request)?;
        Ok(Self {
            request: WorkflowRequest::Video(request),
            request_hash,
            provider_model_id,
            sources,
        })
    }

    pub fn sources(&self) -> &[DreaminaInputSource] {
        &self.sources
    }
    pub fn request_hash(&self) -> &str {
        &self.request_hash
    }
    pub fn provider_model_id(&self) -> &str {
        &self.provider_model_id
    }
    pub fn is_video(&self) -> bool {
        matches!(self.request, WorkflowRequest::Video(_))
    }
    pub fn output_count(&self) -> u32 {
        match &self.request {
            WorkflowRequest::Image(DreaminaImageWorkflowRequest::TextToImage(r)) => {
                u32::from(r.generate_num.unwrap_or(1))
            }
            WorkflowRequest::Image(DreaminaImageWorkflowRequest::ImageToImage(r)) => {
                u32::from(r.generate_num.unwrap_or(1))
            }
            _ => 1,
        }
    }

    pub fn bind(
        self,
        inputs: DreaminaStagedInputs,
    ) -> Result<DreaminaWorkflowAdmissionPlan, DreaminaAdmissionError> {
        let expected = [
            StagedInputKindV1::Image,
            StagedInputKindV1::Video,
            StagedInputKindV1::Audio,
        ]
        .map(|kind| {
            self.sources
                .iter()
                .filter(|source| source.kind == kind)
                .count()
        });
        if expected
            != [
                inputs.images.len(),
                inputs.videos.len(),
                inputs.audios.len(),
            ]
            || inputs.objects.len() != self.sources.len()
        {
            return Err(DreaminaAdmissionError::InvalidInputs);
        }
        let DreaminaStagedInputs {
            images,
            videos,
            audios,
            objects,
        } = inputs;
        let request = match self.request {
            WorkflowRequest::Image(DreaminaImageWorkflowRequest::TextToImage(request)) => {
                typed_image(request)?.into()
            }
            WorkflowRequest::Image(DreaminaImageWorkflowRequest::ImageToImage(request)) => {
                ImageToImageRequestV1::new(images, typed_image(image_parameters(request))?)
                    .map_err(DreaminaAdmissionError::InvalidProviderRequest)?
                    .into()
            }
            WorkflowRequest::Image(DreaminaImageWorkflowRequest::ImageUpscale(request)) => {
                ImageUpscaleRequestV1::new(
                    images
                        .into_iter()
                        .next()
                        .ok_or(DreaminaAdmissionError::InvalidInputs)?,
                    parse_upscale_resolution(&request.resolution_type)?,
                )
                .into()
            }
            WorkflowRequest::Video(DreaminaVideoWorkflowRequest::TextToVideo(request)) => {
                let plan = DreaminaVideoAdmissionPlan::new(request)?;
                parse_submit_command(
                    &serde_json::to_vec(&plan.provider_command)
                        .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?,
                )
                .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?
            }
            WorkflowRequest::Video(DreaminaVideoWorkflowRequest::ImageToVideo(request)) => {
                ImageToVideoRequestV1::new(
                    images
                        .into_iter()
                        .next()
                        .ok_or(DreaminaAdmissionError::InvalidInputs)?,
                    reference_options(
                        &request.prompt,
                        request.model_version.as_deref(),
                        request.ratio.as_deref(),
                        request.duration,
                        &request.video_resolution,
                    )?,
                )
                .map_err(DreaminaAdmissionError::InvalidProviderRequest)?
                .into()
            }
            WorkflowRequest::Video(DreaminaVideoWorkflowRequest::FramesToVideo(request)) => {
                let mut images = images.into_iter();
                FramesToVideoRequestV1::new(
                    images.next().ok_or(DreaminaAdmissionError::InvalidInputs)?,
                    images.next().ok_or(DreaminaAdmissionError::InvalidInputs)?,
                    reference_options(
                        &request.prompt,
                        request.model_version.as_deref(),
                        request.ratio.as_deref(),
                        request.duration,
                        &request.video_resolution,
                    )?,
                )
                .map_err(DreaminaAdmissionError::InvalidProviderRequest)?
                .into()
            }
            WorkflowRequest::Video(DreaminaVideoWorkflowRequest::MultiframeToVideo(request)) => {
                MultiframeToVideoRequestV1::new(
                    images,
                    multiframe_transitions(&request)?,
                    parse_video_resolution(&request.video_resolution)?,
                )
                .map_err(DreaminaAdmissionError::InvalidProviderRequest)?
                .into()
            }
            WorkflowRequest::Video(DreaminaVideoWorkflowRequest::MultimodalToVideo(request)) => {
                MultimodalToVideoRequestV1::new(
                    images,
                    videos,
                    audios,
                    request.prompt,
                    parse_video_model(
                        request
                            .model_version
                            .as_deref()
                            .unwrap_or("seedance2.0_vip"),
                    )?,
                    parse_video_ratio(request.ratio.as_deref().unwrap_or("16:9"))?,
                    request.duration.unwrap_or(5),
                    parse_video_resolution(&request.video_resolution)?,
                )
                .map_err(DreaminaAdmissionError::InvalidProviderRequest)?
                .into()
            }
        };
        DreaminaWorkflowAdmissionPlan::new(
            self.request_hash,
            self.provider_model_id,
            request,
            objects,
        )
    }
}

#[derive(Clone, Debug)]
pub struct DreaminaWorkflowAdmissionPlan {
    source_request_hash: String,
    provider_model_id: String,
    request: DreaminaSubmitRequestV1,
    provider_command: Value,
    provider_command_hash: String,
    input_manifest: Option<AttachInputManifest>,
}

impl DreaminaWorkflowAdmissionPlan {
    fn new(
        source_request_hash: String,
        provider_model_id: String,
        request: DreaminaSubmitRequestV1,
        objects: Vec<AttachInputObject>,
    ) -> Result<Self, DreaminaAdmissionError> {
        let provider_command: Value =
            serde_json::from_slice(&encode_submit_command(request.clone()))
                .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?;
        let provider_command_hash = source_hash(&provider_command)?;
        let input_manifest = if objects.is_empty() {
            None
        } else {
            Some(AttachInputManifest {
                manifest_schema: DREAMINA_INPUT_MANIFEST_SCHEMA.to_owned(),
                manifest_hash: dreamina_input_manifest_hash(&request, &objects)?,
                inputs: objects,
            })
        };
        Ok(Self {
            source_request_hash,
            provider_model_id,
            request,
            provider_command,
            provider_command_hash,
            input_manifest,
        })
    }
    pub fn source_request_hash(&self) -> &str {
        &self.source_request_hash
    }
    pub fn provider_id(&self) -> &'static str {
        PROVIDER_ID
    }
    pub fn provider_model_id(&self) -> &str {
        &self.provider_model_id
    }
    pub fn provider_command(&self) -> &Value {
        &self.provider_command
    }
    pub fn provider_command_hash(&self) -> &str {
        &self.provider_command_hash
    }
    pub fn typed_request(&self) -> &DreaminaSubmitRequestV1 {
        &self.request
    }
    pub fn output_count(&self) -> u32 {
        u32::from(self.request.output_count())
    }
    pub fn workflow(&self) -> &'static str {
        use DreaminaSubmitRequestV1::*;
        match &self.request {
            TextToImage(_) => "text2image",
            ImageToImage(_) => "image2image",
            ImageUpscale(_) => "image_upscale",
            TextToVideo(_) => "text2video",
            ImageToVideo(_) => "image2video",
            FramesToVideo(_) => "frames2video",
            MultiframeToVideo(_) => "multiframe2video",
            MultimodalToVideo(_) => "multimodal2video",
        }
    }
    pub fn duration_ms(&self) -> Option<u32> {
        use DreaminaSubmitRequestV1::*;
        match &self.request {
            TextToVideo(r) => Some(u32::from(r.duration_seconds()) * 1000),
            ImageToVideo(r) => Some(u32::from(r.options().duration_seconds()) * 1000),
            FramesToVideo(r) => Some(u32::from(r.options().duration_seconds()) * 1000),
            MultimodalToVideo(r) => Some(u32::from(r.duration_seconds()) * 1000),
            MultiframeToVideo(r) => Some(
                r.transitions()
                    .iter()
                    .map(VideoTransitionV1::duration_ms)
                    .sum(),
            ),
            _ => None,
        }
    }
    pub fn input_counts(&self) -> (usize, usize, usize) {
        let inputs = self.request.staged_inputs();
        let count = |kind| inputs.iter().filter(|input| input.kind == kind).count();
        (
            count(StagedInputKindV1::Image),
            count(StagedInputKindV1::Video),
            count(StagedInputKindV1::Audio),
        )
    }
    pub fn resolution(&self) -> &'static str {
        use DreaminaSubmitRequestV1::*;
        match &self.request {
            TextToImage(r) => r.resolution().as_str(),
            ImageToImage(r) => r.parameters().resolution().as_str(),
            ImageUpscale(r) => r.resolution().as_str(),
            TextToVideo(r) => r.resolution().as_str(),
            ImageToVideo(r) => r.options().resolution().as_str(),
            FramesToVideo(r) => r.options().resolution().as_str(),
            MultiframeToVideo(r) => r.resolution().as_str(),
            MultimodalToVideo(r) => r.resolution().as_str(),
        }
    }
    pub fn ratio(&self) -> Option<&'static str> {
        use DreaminaSubmitRequestV1::*;
        match &self.request {
            TextToImage(r) => r.ratio().map(|r| r.as_str()),
            ImageToImage(r) => r.parameters().ratio().map(|r| r.as_str()),
            TextToVideo(r) => Some(r.ratio().as_str()),
            ImageToVideo(r) => r.options().ratio().map(|r| r.as_str()),
            FramesToVideo(r) => r.options().ratio().map(|r| r.as_str()),
            MultimodalToVideo(r) => Some(r.ratio().as_str()),
            _ => None,
        }
    }
    pub fn image_dimensions(&self) -> Option<(u32, u32)> {
        let r = match &self.request {
            DreaminaSubmitRequestV1::TextToImage(r) => r,
            DreaminaSubmitRequestV1::ImageToImage(r) => r.parameters(),
            _ => return None,
        };
        r.width().zip(r.height())
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
            input_manifest: self.input_manifest.clone(),
            work_kind: if self.duration_ms().is_some() {
                "video_single"
            } else {
                "image_batch"
            }
            .to_owned(),
            schedule_scope: schedule_scope.into(),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: self
                .duration_ms()
                .map_or(u64::from(self.output_count()), |ms| {
                    u64::from(ms).div_ceil(1000)
                }),
            // New workflows must have an explicit quote; missing intent fails closed.
            contract: AdmissionContract::CustomerPricingV4,
            customer_pricing: None,
        }
    }
}

pub(crate) fn dreamina_input_manifest_hash(
    request: &DreaminaSubmitRequestV1,
    objects: &[AttachInputObject],
) -> Result<String, DreaminaAdmissionError> {
    let staged = request.staged_inputs();
    if staged.len() != objects.len() {
        return Err(DreaminaAdmissionError::InvalidInputs);
    }
    let mut descriptors = Vec::with_capacity(staged.len());
    for (global_index, (input, object)) in staged.iter().zip(objects).enumerate() {
        let role = match input.kind {
            StagedInputKindV1::Image => EditInputRoleV1::Image,
            StagedInputKindV1::Video => EditInputRoleV1::Video,
            StagedInputKindV1::Audio => EditInputRoleV1::Audio,
        };
        if object.role != role
            || usize::from(object.index) != global_index
            || object.blob.sha256_hex != input.sha256
        {
            return Err(DreaminaAdmissionError::InvalidInputs);
        }
        descriptors.push(serde_json::json!({ "filename":input.filename, "index":global_index, "role":role.as_str(),
            "role_index":input.index, "media_type":object.media_type, "byte_size":object.blob.byte_size,
            "sha256":input.sha256, "dimensions":input.dimensions, "duration_ms":input.duration_ms }));
    }
    source_hash(
        &serde_json::json!({ "schema":DREAMINA_INPUT_MANIFEST_SCHEMA, "inputs":descriptors }),
    )
}

fn source_hash(value: &impl serde::Serialize) -> Result<String, DreaminaAdmissionError> {
    let value =
        serde_json::to_value(value).map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?;
    let bytes =
        serde_json::to_vec(&value).map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn image_parameters(
    r: image_api_contracts::dreamina::DreaminaImageToImageRequest,
) -> DreaminaImageGenerationRequest {
    DreaminaImageGenerationRequest {
        prompt: r.prompt,
        model_version: r.model_version,
        ratio: r.ratio,
        resolution_type: r.resolution_type,
        width: r.width,
        height: r.height,
        generate_num: r.generate_num,
    }
}

fn typed_image(
    request: DreaminaImageGenerationRequest,
) -> Result<TextToImageRequestV1, DreaminaAdmissionError> {
    let plan = DreaminaImageAdmissionPlan::new(request)?;
    let bytes = serde_json::to_vec(&plan.provider_command)
        .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?;
    match parse_submit_command(&bytes)
        .map_err(|_| DreaminaAdmissionError::InvalidProviderCommand)?
    {
        DreaminaSubmitRequestV1::TextToImage(request) => Ok(request),
        _ => Err(DreaminaAdmissionError::InvalidProviderCommand),
    }
}

fn parse_upscale_resolution(value: &str) -> Result<UpscaleResolution, DreaminaAdmissionError> {
    match value {
        "2k" => Ok(UpscaleResolution::K2),
        "4k" => Ok(UpscaleResolution::K4),
        "8k" => Ok(UpscaleResolution::K8),
        _ => Err(DreaminaAdmissionError::InvalidResolution),
    }
}

fn reference_options(
    prompt: &str,
    model: Option<&str>,
    ratio: Option<&str>,
    duration: Option<u8>,
    resolution: &str,
) -> Result<ReferenceVideoOptionsV1, DreaminaAdmissionError> {
    ReferenceVideoOptionsV1::new(
        prompt,
        parse_video_model(model.unwrap_or("seedance2.0_vip"))?,
        ratio.map(parse_video_ratio).transpose()?,
        duration.unwrap_or(5),
        parse_video_resolution(resolution)?,
    )
    .map_err(DreaminaAdmissionError::InvalidProviderRequest)
}

fn multiframe_transitions(
    request: &DreaminaMultiframeToVideoRequest,
) -> Result<Vec<VideoTransitionV1>, DreaminaAdmissionError> {
    let values = match (
        &request.transitions,
        &request.transition_prompt,
        &request.transition_duration,
        &request.prompt,
        &request.duration,
    ) {
        (Some(transitions), None, None, None, None) => transitions
            .iter()
            .map(|t| (t.prompt.as_str(), t.duration.as_ref()))
            .collect::<Vec<_>>(),
        (None, Some(prompts), durations, None, None)
            if durations.as_ref().is_none_or(|d| d.len() == prompts.len()) =>
        {
            prompts
                .iter()
                .enumerate()
                .map(|(index, prompt)| (prompt.as_str(), durations.as_ref().map(|d| &d[index])))
                .collect()
        }
        (None, None, None, Some(prompt), duration) if request.images.len() == 2 => {
            vec![(prompt.as_str(), duration.as_ref())]
        }
        _ => return Err(DreaminaAdmissionError::InvalidTransitions),
    };
    if values.len() + 1 != request.images.len() {
        return Err(DreaminaAdmissionError::InvalidTransitions);
    }
    let transitions = values
        .into_iter()
        .map(|(prompt, duration)| {
            let ms = duration
                .map(decimal_duration_ms)
                .transpose()?
                .unwrap_or(3000);
            VideoTransitionV1::new(prompt, ms)
                .map_err(DreaminaAdmissionError::InvalidProviderRequest)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if transitions
        .iter()
        .map(VideoTransitionV1::duration_ms)
        .sum::<u32>()
        < 2000
    {
        return Err(DreaminaAdmissionError::InvalidTransitions);
    }
    Ok(transitions)
}

/// Convert decimal seconds to milliseconds without a floating-point round trip.
fn decimal_duration_ms(number: &serde_json::Number) -> Result<u32, DreaminaAdmissionError> {
    let text = number.to_string();
    if text.len() > 64 || text.starts_with('-') {
        return Err(DreaminaAdmissionError::InvalidTransitions);
    }
    let (significand, exponent) = match text.split_once(['e', 'E']) {
        Some((base, exp)) => (
            base,
            exp.parse::<i32>()
                .map_err(|_| DreaminaAdmissionError::InvalidTransitions)?,
        ),
        None => (text.as_str(), 0),
    };
    let (whole, fraction) = significand.split_once('.').unwrap_or((significand, ""));
    let digits = format!("{whole}{fraction}");
    let mut integer = digits
        .parse::<u128>()
        .map_err(|_| DreaminaAdmissionError::InvalidTransitions)?;
    let power = exponent
        .checked_add(3)
        .and_then(|e| e.checked_sub(fraction.len() as i32))
        .ok_or(DreaminaAdmissionError::InvalidTransitions)?;
    if !(-38..=38).contains(&power) {
        return Err(DreaminaAdmissionError::InvalidTransitions);
    }
    let scale = 10_u128.pow(power.unsigned_abs());
    if power < 0 {
        if integer % scale != 0 {
            return Err(DreaminaAdmissionError::InvalidTransitions);
        }
        integer /= scale;
    } else {
        integer = integer
            .checked_mul(scale)
            .ok_or(DreaminaAdmissionError::InvalidTransitions)?;
    }
    u32::try_from(integer).map_err(|_| DreaminaAdmissionError::InvalidTransitions)
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
    #[error("Dreamina inputs do not match the workflow, order or frozen content")]
    InvalidInputs,
    #[error("Dreamina transitions require exactly one prompt per segment and whole milliseconds")]
    InvalidTransitions,
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
            Self::InvalidInputs => "inputs",
            Self::InvalidTransitions => "transitions",
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
        "1.5k" => Ok(ImageResolution::K1_5),
        "2k" => Ok(ImageResolution::K2),
        "4k" => Ok(ImageResolution::K4),
        _ => Err(DreaminaAdmissionError::InvalidResolution),
    }
}

fn parse_video_model(value: &str) -> Result<VideoModelVersion, DreaminaAdmissionError> {
    match value {
        "seedance1.0fast" => Ok(VideoModelVersion::Seedance1_0Fast),
        "seedance1.5pro" => Ok(VideoModelVersion::Seedance1_5Pro),
        "seedance2.0" => Ok(VideoModelVersion::Seedance2_0),
        "seedance2.0fast" => Ok(VideoModelVersion::Seedance2_0Fast),
        "seedance2.0_vip" => Ok(VideoModelVersion::Seedance2_0Vip),
        "seedance2.0fast_vip" => Ok(VideoModelVersion::Seedance2_0FastVip),
        "seedance2.0mini" => Ok(VideoModelVersion::Seedance2_0Mini),
        "seedance2.5" => Ok(VideoModelVersion::Seedance2_5),
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
        "480p" => Ok(VideoResolution::P480),
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

    fn stage_fixture(
        intent: DreaminaWorkflowIntent,
    ) -> (DreaminaWorkflowAdmissionPlan, AdmissionTicket) {
        let session_id = Uuid::new_v4();
        let ticket = AdmissionTicket {
            session_id,
            owner_token: Uuid::new_v4(),
            request_hash: intent.request_hash().to_owned(),
        };
        let mut staged = DreaminaStagedInputs::default();
        for (index, source) in intent.sources().iter().enumerate() {
            let sha = format!("{index:064x}");
            let (role, media_type, extension) = match source.kind {
                StagedInputKindV1::Image => (EditInputRoleV1::Image, "image/png", "png"),
                StagedInputKindV1::Video => (EditInputRoleV1::Video, "video/mp4", "mp4"),
                StagedInputKindV1::Audio => (EditInputRoleV1::Audio, "audio/wav", "wav"),
            };
            let filename = format!("input-{index}.{extension}");
            match source.kind {
                StagedInputKindV1::Image => staged
                    .images
                    .push(StagedImageV1::new(filename, &sha, 16, 9).unwrap()),
                StagedInputKindV1::Video => staged
                    .videos
                    .push(StagedTimedMediaV1::new(filename, &sha, 2000).unwrap()),
                StagedInputKindV1::Audio => staged
                    .audios
                    .push(StagedTimedMediaV1::new(filename, &sha, 2000).unwrap()),
            }
            staged.objects.push(AttachInputObject {
                role,
                index: index as u16,
                media_type: media_type.to_owned(),
                blob: crate::input_blobs::InputBlobRef {
                    key: crate::input_blobs::InputBlobKey {
                        admission_session_id: session_id,
                        input_id: Uuid::new_v4(),
                    },
                    storage_backend: "test".to_owned(),
                    object_key: format!("{session_id}/{index}"),
                    sha256_hex: sha,
                    byte_size: 100,
                },
            });
        }
        (intent.bind(staged).unwrap(), ticket)
    }

    #[test]
    fn native_workflow_binding_freezes_every_operation_and_exact_duration() {
        for (json, workflow) in [
            (
                r#"{"workflow":"text2image","prompt":"city","resolution_type":"2k"}"#,
                "text2image",
            ),
            (
                r#"{"workflow":"image2image","images":["a"],"prompt":"city","resolution_type":"2k"}"#,
                "image2image",
            ),
            (
                r#"{"workflow":"image_upscale","image":"a","resolution_type":"8k"}"#,
                "image_upscale",
            ),
        ] {
            let request = serde_json::from_str(json).unwrap();
            let (plan, ticket) = stage_fixture(DreaminaWorkflowIntent::image(request).unwrap());
            assert_eq!(
                super::super::attach_operation(&plan.attach(ticket, Uuid::new_v4(), "test"))
                    .unwrap(),
                GENERATION_OPERATION
            );
            assert_eq!(plan.workflow(), workflow);
            assert_eq!(
                parse_submit_command(&serde_json::to_vec(plan.provider_command()).unwrap())
                    .unwrap(),
                *plan.typed_request()
            );
        }
        for (json, workflow, duration_ms) in [
            (
                r#"{"workflow":"text2video","prompt":"move","video_resolution":"720p"}"#,
                "text2video",
                5000,
            ),
            (
                r#"{"workflow":"image2video","image":"a","prompt":"move","model_version":"seedance2.5","video_resolution":"480p"}"#,
                "image2video",
                5000,
            ),
            (
                r#"{"workflow":"frames2video","first":"a","last":"b","prompt":"move","video_resolution":"1080p"}"#,
                "frames2video",
                5000,
            ),
            (
                r#"{"workflow":"multiframe2video","images":["a","b"],"prompt":"move","duration":2.501,"video_resolution":"720p"}"#,
                "multiframe2video",
                2501,
            ),
            (
                r#"{"workflow":"multimodal2video","image":["a"],"video":["b"],"audio":["c"],"video_resolution":"720p"}"#,
                "multimodal2video",
                5000,
            ),
        ] {
            let request = serde_json::from_str(json).unwrap();
            let (plan, ticket) = stage_fixture(DreaminaWorkflowIntent::video(request).unwrap());
            assert_eq!(
                super::super::attach_operation(&plan.attach(ticket, Uuid::new_v4(), "test"))
                    .unwrap(),
                VIDEO_GENERATION_OPERATION
            );
            assert_eq!(plan.workflow(), workflow);
            assert_eq!(plan.duration_ms(), Some(duration_ms));
            assert_eq!(
                parse_submit_command(&serde_json::to_vec(plan.provider_command()).unwrap())
                    .unwrap(),
                *plan.typed_request()
            );
            if workflow == "image2video" {
                assert_eq!(plan.ratio(), None);
            }
        }
    }

    #[test]
    fn legacy_explicit_workflow_keeps_original_canonical_bytes_and_defaults() {
        let request: DreaminaVideoGenerationRequest =
            serde_json::from_str(r#"{"prompt":"move","video_resolution":"720p"}"#).unwrap();
        let legacy = DreaminaVideoAdmissionPlan::new(request.clone()).unwrap();
        let (workflow, _) = stage_fixture(
            DreaminaWorkflowIntent::video(DreaminaVideoWorkflowRequest::TextToVideo(request))
                .unwrap(),
        );
        assert_eq!(workflow.provider_command(), &legacy.provider_command);
        assert_eq!(
            workflow.provider_command_hash(),
            legacy.provider_command_hash()
        );
    }

    #[test]
    fn reference_image_native_batch_preserves_count_and_per_output_billing() {
        let request = serde_json::from_str(r#"{"workflow":"image2image","images":["a"],"prompt":"city","resolution_type":"2k","generate_num":10}"#).unwrap();
        let (plan, _) = stage_fixture(DreaminaWorkflowIntent::image(request).unwrap());
        assert_eq!(plan.output_count(), 10);
        assert_eq!(plan.provider_command()["schema_version"], 2);
        let facts = crate::pricing::admission::dreamina_pricing_facts(
            plan.typed_request(),
            DREAMINA_IMAGES_API_PROFILE,
        )
        .unwrap();
        assert_eq!(facts.output_count, 10);
        assert_eq!(facts.output_billable_units, 1);
        assert_eq!(
            facts.output_count.checked_mul(facts.output_billable_units),
            Some(10)
        );
        assert_eq!(
            plan.typed_request()
                .clone()
                .into_single_output()
                .output_count(),
            1
        );
    }

    #[test]
    fn decimal_milliseconds_are_exact_bounded_and_support_scientific_notation() {
        for (number, expected) in [
            ("2", 2000),
            ("2.501", 2501),
            ("2.5000", 2500),
            ("25e-1", 2500),
            ("2500e-3", 2500),
            ("2e+0", 2000),
        ] {
            assert_eq!(
                decimal_duration_ms(&number.parse().unwrap()).unwrap(),
                expected
            );
        }
        for number in [
            "-2",
            "2.0001",
            "2.0000000000000001",
            "1e-8",
            "1e1000000",
            "1e-1000000",
            "4294967296",
            "1e1000000000000000",
        ] {
            assert!(
                decimal_duration_ms(&number.parse().unwrap()).is_err(),
                "{number}"
            );
        }
        let request = r#"{"workflow":"multiframe2video","images":["a","b","c"],"transition_prompt":["one","two"],"video_resolution":"720p"}"#;
        let (plan, _) = stage_fixture(
            DreaminaWorkflowIntent::video(serde_json::from_str(request).unwrap()).unwrap(),
        );
        assert_eq!(plan.duration_ms(), Some(6000));
        for json in [
            r#"{"workflow":"multiframe2video","images":["a","b"],"prompt":"move","duration":1,"video_resolution":"720p"}"#,
            r#"{"workflow":"multiframe2video","images":["a","b"],"prompt":"move","transitions":[{"prompt":"other"}],"video_resolution":"720p"}"#,
            r#"{"workflow":"multiframe2video","images":["a","b","c"],"transition_prompt":["one","two"],"transition_duration":[3],"video_resolution":"720p"}"#,
        ] {
            assert!(DreaminaWorkflowIntent::video(serde_json::from_str(json).unwrap()).is_err());
        }
    }

    #[test]
    fn immutable_manifest_rejects_tampering_missing_input_and_cross_session_blob() {
        let request = r#"{"workflow":"multimodal2video","image":["a"],"video":["b"],"audio":["c"],"video_resolution":"720p"}"#;
        let (plan, ticket) = stage_fixture(
            DreaminaWorkflowIntent::video(serde_json::from_str(request).unwrap()).unwrap(),
        );
        assert_eq!(plan.input_counts(), (1, 1, 1));
        let attach = plan.attach(ticket, Uuid::new_v4(), "test");
        let manifest = attach.input_manifest.as_ref().unwrap();
        assert!(super::super::validate_dreamina_attach_request(&attach, manifest).is_ok());
        let mut wrong = manifest.clone();
        wrong.inputs.swap(0, 1);
        assert!(super::super::validate_dreamina_attach_request(&attach, &wrong).is_err());
        let mut wrong = manifest.clone();
        wrong.inputs[0].blob.key.admission_session_id = Uuid::new_v4();
        assert!(super::super::validate_dreamina_attach_request(&attach, &wrong).is_err());
        let mut wrong = manifest.clone();
        wrong.inputs[0].media_type = "image/jpeg".to_owned();
        assert!(super::super::validate_dreamina_attach_request(&attach, &wrong).is_err());
        let mut wrong = manifest.clone();
        wrong.inputs.pop();
        assert!(super::super::validate_dreamina_attach_request(&attach, &wrong).is_err());
        let mut wrong = attach.clone();
        wrong.contract = AdmissionContract::MediaEconomicsV3;
        assert!(matches!(
            super::super::validate_dreamina_attach_request(&wrong, manifest),
            Err(super::super::AdmissionError::PricingUnavailable)
        ));
    }

    #[test]
    fn image_batch_is_frozen_once_for_a_native_submission() {
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
        assert_eq!(plan.provider_command["schema_version"], 2);
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
    fn native_batch_preserves_historical_request_identity_without_reusing_execution_hash() {
        for count in [1, 2, 10] {
            let plan = DreaminaImageAdmissionPlan::new(DreaminaImageGenerationRequest {
                prompt: "idempotent image".to_owned(),
                model_version: Some("5.0".to_owned()),
                ratio: Some("1:1".to_owned()),
                resolution_type: "2k".to_owned(),
                width: None,
                height: None,
                generate_num: Some(count),
            })
            .unwrap();
            let mut historical = plan.provider_command.clone();
            historical["schema_version"] = Value::from(1);
            assert_eq!(
                plan.request_identity_hash(),
                source_hash(&historical).unwrap()
            );
            assert_eq!(
                plan.provider_command_hash(),
                source_hash(&plan.provider_command).unwrap()
            );
            assert_eq!(
                plan.request_identity_hash() == plan.provider_command_hash(),
                count == 1
            );
        }
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

    #[test]
    fn seedance_25_admission_freezes_the_requested_video_specification() {
        let request = DreaminaVideoGenerationRequest {
            prompt: "a paper airplane".to_owned(),
            model_version: Some("seedance2.5".to_owned()),
            ratio: Some("16:9".to_owned()),
            duration: Some(5),
            video_resolution: "480p".to_owned(),
        };
        let plan = DreaminaVideoAdmissionPlan::new(request).unwrap();
        assert_eq!(plan.provider_model_id(), "seedance2.5");
        assert_eq!(plan.duration(), 5);
        assert_eq!(plan.resolution(), "480p");
        assert_eq!(plan.pricing_dimensions()["ratio"], "16:9");
        let command =
            parse_submit_command(&serde_json::to_vec(&plan.provider_command).unwrap()).unwrap();
        let DreaminaSubmitRequestV1::TextToVideo(request) = command else {
            panic!("video admission must preserve the video command");
        };
        assert_eq!(request.model().as_str(), "seedance2.5");
    }
}
