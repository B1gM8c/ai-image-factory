use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use image_provider_contracts::{active_providers, openai_codex};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::{
    ImageGatewayError,
    core::provider::validate_edit_job,
    generator::{EditJob, GeneratedImage, GenerationJob, InputImage},
    size::is_valid_gpt_image_2_size,
};

const PROMPT_MAX_CHARS: usize = 32_000;
const DEFAULT_IMAGE_MODEL: &str = openai_codex::MODEL_GPT_IMAGE_2;

#[derive(Debug, Deserialize)]
pub struct ImageGenerationRequest {
    pub model: Option<String>,
    pub prompt: Option<String>,
    pub n: Option<u32>,
    pub size: Option<String>,
    pub quality: Option<String>,
    pub output_format: Option<String>,
    pub output_compression: Option<u16>,
    pub background: Option<String>,
    pub response_format: Option<String>,
    pub user: Option<String>,
    pub moderation: Option<String>,
    pub stream: Option<bool>,
    pub partial_images: Option<u32>,
    pub style: Option<String>,
}

#[derive(Debug, Default)]
pub struct EditForm {
    pub model: Option<String>,
    pub prompt: Option<String>,
    pub n: Option<u32>,
    pub size: Option<String>,
    pub quality: Option<String>,
    pub output_format: Option<String>,
    pub output_compression: Option<u16>,
    pub background: Option<String>,
    pub response_format: Option<String>,
    pub user: Option<String>,
    pub moderation: Option<String>,
    pub stream: Option<bool>,
    pub partial_images: Option<u32>,
    pub style: Option<String>,
    pub input_fidelity: Option<String>,
    pub images: Vec<InputImage>,
    pub mask: Option<InputImage>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ImagesResponse {
    pub created: i64,
    pub data: Vec<ImageData>,
    pub output_format: String,
    pub quality: String,
    pub size: String,
    pub background: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ImageData {
    pub b64_json: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ImageStreamEvent {
    #[serde(rename = "type")]
    #[schema(value_type = String)]
    pub event_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_image_index: Option<usize>,
    pub b64_json: String,
    pub created_at: i64,
    pub background: String,
    pub output_format: String,
    pub quality: String,
    pub size: String,
}

#[derive(Clone, Copy, Debug)]
pub enum ImageStreamKind {
    Generation,
    Edit,
}

impl ImageStreamKind {
    fn completed_event(self) -> &'static str {
        match self {
            Self::Generation => "image_generation.completed",
            Self::Edit => "image_edit.completed",
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
    #[schema(value_type = String)]
    pub status: &'static str,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderProfileReadinessCounts {
    pub configured: i64,
    pub active: i64,
    pub draining: i64,
    pub blocked: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ExecutionQueueReadinessCounts {
    pub ready_work_items: i64,
    pub active_work_leases: i64,
    pub oldest_ready_work_age_ms: i64,
    pub stalled_work_profiles: i64,
    pub prepared_executions: i64,
    pub active_executor_leases: i64,
    pub oldest_prepared_execution_age_ms: i64,
    pub stalled_executor_profiles: i64,
    pub ready_reductions: i64,
    pub active_reducer_leases: i64,
    pub oldest_ready_reduction_age_ms: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ReadinessResponse {
    #[schema(value_type = String)]
    pub status: &'static str,
    pub provider_profiles: Option<ProviderProfileReadinessCounts>,
    pub execution_queue: Option<ExecutionQueueReadinessCounts>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ModelsResponse {
    pub object: String,
    pub data: Vec<ModelData>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ModelData {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub owned_by: String,
}

pub fn parse_generation(
    value: Value,
    request_id: String,
) -> Result<GenerationJob, ImageGatewayError> {
    reject_unknown_parameters(
        &value,
        &[
            "model",
            "prompt",
            "n",
            "size",
            "quality",
            "output_format",
            "output_compression",
            "background",
            "response_format",
            "user",
            "moderation",
            "stream",
            "partial_images",
            "style",
        ],
    )?;

    let request: ImageGenerationRequest = serde_json::from_value(value).map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })?;
    request.into_job(request_id)
}

impl ImageGenerationRequest {
    fn into_job(self, request_id: String) -> Result<GenerationJob, ImageGatewayError> {
        let _accepted_user_passthrough = self.user;
        let CommonFields {
            model,
            prompt,
            moderation,
            n,
            size,
            quality,
            output_format,
            output_compression,
            background,
            stream,
            partial_images,
        } = validate_common(
            self.model,
            self.prompt,
            self.n,
            self.size,
            self.quality,
            self.output_format,
            self.output_compression,
            self.background,
            self.response_format,
            self.moderation,
            self.stream,
            self.partial_images,
            self.style,
        )?;

        Ok(GenerationJob {
            request_id,
            model,
            prompt,
            moderation,
            n,
            size,
            quality,
            output_format,
            output_compression,
            background,
            stream,
            partial_images,
        })
    }
}

impl EditForm {
    pub fn into_job(self, request_id: String) -> Result<EditJob, ImageGatewayError> {
        if self.images.is_empty() {
            return Err(ImageGatewayError::invalid_request(
                "Missing required image file",
                Some("image".to_string()),
                "missing_required_parameter",
            ));
        }
        if self.images.len() > 16 {
            return Err(ImageGatewayError::invalid_request(
                "A maximum of 16 images are supported",
                Some("image".to_string()),
                "invalid_value",
            ));
        }

        let CommonFields {
            model,
            prompt,
            moderation,
            n,
            size,
            quality,
            output_format,
            output_compression,
            background,
            stream,
            partial_images,
        } = validate_common(
            self.model,
            self.prompt,
            self.n,
            self.size,
            self.quality,
            self.output_format,
            self.output_compression,
            self.background,
            self.response_format,
            self.moderation,
            self.stream,
            self.partial_images,
            self.style,
        )?;
        if self.input_fidelity.is_some() {
            return Err(ImageGatewayError::unsupported(
                "input_fidelity",
                "gpt-image-2 always uses high fidelity for input images; omit input_fidelity.",
            ));
        }

        let job = EditJob {
            request_id,
            model,
            prompt,
            moderation,
            images: self.images,
            mask: self.mask,
            n,
            size,
            quality,
            output_format,
            output_compression,
            background,
            stream,
            partial_images,
        };
        validate_edit_job(&job)?;
        Ok(job)
    }
}

struct CommonFields {
    model: String,
    prompt: String,
    moderation: String,
    n: u32,
    size: String,
    quality: String,
    output_format: String,
    output_compression: Option<u8>,
    background: String,
    stream: bool,
    partial_images: u32,
}

#[allow(clippy::too_many_arguments)]
fn validate_common(
    model: Option<String>,
    prompt: Option<String>,
    n: Option<u32>,
    size: Option<String>,
    quality: Option<String>,
    output_format: Option<String>,
    output_compression: Option<u16>,
    background: Option<String>,
    response_format: Option<String>,
    moderation: Option<String>,
    stream: Option<bool>,
    partial_images: Option<u32>,
    style: Option<String>,
) -> Result<CommonFields, ImageGatewayError> {
    let model = model.unwrap_or_else(|| DEFAULT_IMAGE_MODEL.to_string());
    if !openai_codex::is_supported_model(&model) {
        return Err(ImageGatewayError::model_not_found(&model));
    }

    if response_format.as_deref() == Some("url") {
        return Err(ImageGatewayError::unsupported(
            "response_format",
            "Unsupported parameter: 'response_format=url'. GPT image models always return b64_json.",
        ));
    }
    if let Some(value) = response_format.as_deref()
        && value != "b64_json"
    {
        return Err(ImageGatewayError::invalid_request(
            "response_format must be b64_json for GPT image models",
            Some("response_format".to_string()),
            "invalid_value",
        ));
    }
    let stream = stream.unwrap_or(false);
    let partial_images = partial_images.unwrap_or(0);
    if stream && partial_images > 0 {
        return Err(ImageGatewayError::unsupported(
            "partial_images",
            "Codex CLI does not expose native partial image events; only stream=true with partial_images=0 is supported as a final-image SSE compatibility mode.",
        ));
    }
    if !stream && partial_images > 0 {
        return Err(ImageGatewayError::unsupported(
            "partial_images",
            "partial_images requires stream=true",
        ));
    }
    if style.is_some() {
        return Err(ImageGatewayError::unsupported(
            "style",
            "style is only supported for DALL-E models",
        ));
    }
    let moderation = moderation.unwrap_or_else(|| "auto".to_string());
    if !matches!(moderation.as_str(), "auto" | "low") {
        return Err(ImageGatewayError::invalid_request(
            "moderation must be auto or low",
            Some("moderation".to_string()),
            "invalid_value",
        ));
    }
    if moderation == "low" {
        return Err(ImageGatewayError::unsupported(
            "moderation",
            "The active Codex CLI provider cannot enforce moderation=low; use moderation=auto.",
        ));
    }

    let prompt = prompt.ok_or_else(|| {
        ImageGatewayError::invalid_request(
            "Missing required parameter: prompt",
            Some("prompt".to_string()),
            "missing_required_parameter",
        )
    })?;
    if prompt.trim().is_empty() {
        return Err(ImageGatewayError::invalid_request(
            "prompt must not be empty",
            Some("prompt".to_string()),
            "invalid_value",
        ));
    }
    if prompt.chars().count() > PROMPT_MAX_CHARS {
        return Err(ImageGatewayError::invalid_request(
            "prompt is too long",
            Some("prompt".to_string()),
            "invalid_value",
        ));
    }

    let n = n.unwrap_or(1);
    if !(1..=10).contains(&n) {
        return Err(ImageGatewayError::invalid_request(
            "n must be between 1 and 10",
            Some("n".to_string()),
            "invalid_value",
        ));
    }

    let size = size.unwrap_or_else(|| "auto".to_string());
    validate_size(&size)?;

    let quality = quality.unwrap_or_else(|| "auto".to_string());
    if !matches!(quality.as_str(), "auto" | "low" | "medium" | "high") {
        return Err(ImageGatewayError::invalid_request(
            "quality must be auto, low, medium, or high",
            Some("quality".to_string()),
            "invalid_value",
        ));
    }

    let output_format = output_format.unwrap_or_else(|| "png".to_string());
    if !matches!(output_format.as_str(), "png" | "jpeg" | "webp") {
        return Err(ImageGatewayError::invalid_request(
            "output_format must be png, jpeg, or webp",
            Some("output_format".to_string()),
            "invalid_value",
        ));
    }
    if output_compression.is_some() && output_format == "png" {
        return Err(ImageGatewayError::invalid_request(
            "output_compression is only supported for jpeg and webp",
            Some("output_compression".to_string()),
            "invalid_value",
        ));
    }
    let output_compression = match output_compression {
        Some(value) if value > 100 => {
            return Err(ImageGatewayError::invalid_request(
                "output_compression must be between 0 and 100",
                Some("output_compression".to_string()),
                "invalid_value",
            ));
        }
        Some(value) => Some(value as u8),
        None => None,
    };

    let background = background.unwrap_or_else(|| "auto".to_string());
    if background == "transparent" {
        return Err(ImageGatewayError::unsupported(
            "background",
            "gpt-image-2 does not support transparent backgrounds",
        ));
    }
    if !matches!(background.as_str(), "auto" | "opaque") {
        return Err(ImageGatewayError::invalid_request(
            "background must be auto or opaque",
            Some("background".to_string()),
            "invalid_value",
        ));
    }
    let background = if background == "auto" {
        "opaque".to_string()
    } else {
        background
    };

    Ok(CommonFields {
        model,
        prompt,
        moderation,
        n,
        size,
        quality,
        output_format,
        output_compression,
        background,
        stream,
        partial_images,
    })
}

fn validate_size(size: &str) -> Result<(), ImageGatewayError> {
    if is_valid_gpt_image_2_size(size) {
        Ok(())
    } else {
        invalid_size()
    }
}

fn invalid_size() -> Result<(), ImageGatewayError> {
    Err(ImageGatewayError::invalid_request(
        "Invalid image size for gpt-image-2",
        Some("size".to_string()),
        "invalid_image_size",
    ))
}

fn reject_unknown_parameters(value: &Value, allowed: &[&str]) -> Result<(), ImageGatewayError> {
    let Some(object) = value.as_object() else {
        return Err(ImageGatewayError::invalid_request(
            "Request body must be a JSON object",
            None,
            "invalid_json",
        ));
    };

    let allowed: BTreeSet<_> = allowed.iter().copied().collect();
    let keys: BTreeMap<_, _> = object.iter().collect();
    for key in keys.keys() {
        if !allowed.contains(key.as_str()) {
            return Err(ImageGatewayError::unknown_parameter(key));
        }
    }
    Ok(())
}

pub fn images_response_at(
    created: i64,
    images: Vec<GeneratedImage>,
    output_format: String,
    quality: String,
    size: String,
    background: String,
) -> ImagesResponse {
    ImagesResponse {
        created,
        data: images
            .into_iter()
            .map(|image| ImageData {
                b64_json: STANDARD.encode(image.bytes),
            })
            .collect(),
        output_format,
        quality,
        size,
        background,
    }
}

pub fn image_stream_events(
    response: &ImagesResponse,
    kind: ImageStreamKind,
) -> Vec<ImageStreamEvent> {
    response
        .data
        .iter()
        .map(|image| ImageStreamEvent {
            event_type: kind.completed_event(),
            partial_image_index: None,
            b64_json: image.b64_json.clone(),
            created_at: response.created,
            background: response.background.clone(),
            output_format: response.output_format.clone(),
            quality: response.quality.clone(),
            size: response.size.clone(),
        })
        .collect()
}

pub fn models_response() -> ModelsResponse {
    ModelsResponse {
        object: "list".to_owned(),
        data: active_providers()
            .iter()
            .flat_map(|provider| {
                provider.models.iter().map(|model| ModelData {
                    id: (*model).to_owned(),
                    object: "model".to_owned(),
                    created: 0,
                    owned_by: provider.owner.to_owned(),
                })
            })
            .collect(),
    }
}
