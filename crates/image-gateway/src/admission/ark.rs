use std::collections::BTreeMap;

use image_api_contracts::{
    ark::{ArkContentGenerationTaskRequest, ArkContentItem, ArkImageGenerationRequest},
    dreamina::{DreaminaImageGenerationRequest, DreaminaVideoGenerationRequest},
};
use thiserror::Error;
use uuid::Uuid;

use super::{
    AdmissionTicket, AttachJob, ClaimAdmission, DreaminaAdmissionError, DreaminaImageAdmissionPlan,
    DreaminaVideoAdmissionPlan,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArkImageAdmissionPlan {
    public_model: String,
    inner: DreaminaImageAdmissionPlan,
}

impl ArkImageAdmissionPlan {
    pub fn new(request: ArkImageGenerationRequest) -> Result<Self, ArkAdmissionError> {
        reject_unsupported_image_fields(&request)?;
        let model_version = ark_image_model(&request.model)?;
        let (resolution_type, ratio, width, height) =
            ark_image_size(request.size.as_deref().unwrap_or("2K"))?;
        let generate_num = ark_image_count(&request)?;
        let public_model = request.model;
        let inner = DreaminaImageAdmissionPlan::new(DreaminaImageGenerationRequest {
            prompt: request.prompt,
            model_version: Some(model_version.to_owned()),
            ratio,
            resolution_type,
            width,
            height,
            generate_num: Some(generate_num),
        })?;
        Ok(Self {
            public_model,
            inner,
        })
    }

    pub fn public_model(&self) -> &str {
        &self.public_model
    }

    pub fn provider_id(&self) -> &'static str {
        self.inner.provider_id()
    }

    pub fn provider_model(&self) -> &str {
        self.inner.provider_model()
    }

    pub fn provider_model_id(&self) -> &str {
        self.inner.provider_model_id()
    }

    pub fn provider_command_hash(&self) -> &str {
        self.inner.provider_command_hash()
    }

    pub fn pricing_dimensions(&self) -> &BTreeMap<String, String> {
        self.inner.pricing_dimensions()
    }

    pub fn output_count(&self) -> u32 {
        self.inner.output_count()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn claim(
        &self,
        api_profile: &str,
        owner_token: Uuid,
        tenant_id: impl Into<String>,
        project_id: impl Into<String>,
        request_id: impl Into<String>,
        idempotency_key_digest: Option<String>,
        deadline_at_ms: i64,
    ) -> ClaimAdmission {
        self.inner.claim_for_profile(
            api_profile,
            owner_token,
            tenant_id,
            project_id,
            request_id,
            idempotency_key_digest,
            deadline_at_ms,
        )
    }

    pub fn attach(
        &self,
        ticket: AdmissionTicket,
        job_id: Uuid,
        schedule_scope: impl Into<String>,
    ) -> AttachJob {
        self.inner.attach(ticket, job_id, schedule_scope)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArkVideoAdmissionPlan {
    public_model: String,
    inner: DreaminaVideoAdmissionPlan,
}

impl ArkVideoAdmissionPlan {
    pub fn new(request: ArkContentGenerationTaskRequest) -> Result<Self, ArkAdmissionError> {
        reject_unsupported_video_fields(&request)?;
        let model_version = ark_video_model(&request.model)?;
        let prompt = match request.content.as_slice() {
            [ArkContentItem::Text { text }] => text.clone(),
            _ => return Err(ArkAdmissionError::Unsupported("content")),
        };
        let public_model = request.model;
        let inner = DreaminaVideoAdmissionPlan::new(DreaminaVideoGenerationRequest {
            prompt,
            model_version: Some(model_version.to_owned()),
            ratio: request.ratio,
            duration: request.duration,
            video_resolution: request.resolution.unwrap_or_else(|| "720p".to_owned()),
        })?;
        Ok(Self {
            public_model,
            inner,
        })
    }

    pub fn public_model(&self) -> &str {
        &self.public_model
    }

    pub fn provider_id(&self) -> &'static str {
        self.inner.provider_id()
    }

    pub fn provider_model(&self) -> &str {
        self.inner.provider_model()
    }

    pub fn provider_model_id(&self) -> &str {
        self.inner.provider_model_id()
    }

    pub fn provider_command_hash(&self) -> &str {
        self.inner.provider_command_hash()
    }

    pub fn duration(&self) -> u8 {
        self.inner.duration()
    }

    pub fn pricing_dimensions(&self) -> &BTreeMap<String, String> {
        self.inner.pricing_dimensions()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn claim(
        &self,
        api_profile: &str,
        owner_token: Uuid,
        tenant_id: impl Into<String>,
        project_id: impl Into<String>,
        request_id: impl Into<String>,
        idempotency_key_digest: Option<String>,
        deadline_at_ms: i64,
    ) -> ClaimAdmission {
        self.inner.claim_for_profile(
            api_profile,
            owner_token,
            tenant_id,
            project_id,
            request_id,
            idempotency_key_digest,
            deadline_at_ms,
        )
    }

    pub fn attach(
        &self,
        ticket: AdmissionTicket,
        job_id: Uuid,
        schedule_scope: impl Into<String>,
    ) -> AttachJob {
        self.inner.attach(ticket, job_id, schedule_scope)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ArkAdmissionError {
    #[error(
        "Ark parameter `{0}` is represented by the official API but cannot be honored by the current Dreamina CLI binding"
    )]
    Unsupported(&'static str),
    #[error("Ark parameter `{0}` has an unsupported value")]
    InvalidValue(&'static str),
    #[error(transparent)]
    Dreamina(#[from] DreaminaAdmissionError),
}

impl ArkAdmissionError {
    pub const fn parameter(self) -> &'static str {
        match self {
            Self::Unsupported(parameter) | Self::InvalidValue(parameter) => parameter,
            Self::Dreamina(error) => error.parameter(),
        }
    }
}

fn reject_unsupported_image_fields(
    request: &ArkImageGenerationRequest,
) -> Result<(), ArkAdmissionError> {
    let unsupported = [
        (request.image.is_some(), "image"),
        (request.seed.is_some(), "seed"),
        (request.guidance_scale.is_some(), "guidance_scale"),
        (request.watermark.is_some(), "watermark"),
        (request.optimize_prompt.is_some(), "optimize_prompt"),
        (
            request.optimize_prompt_options.is_some(),
            "optimize_prompt_options",
        ),
        (request.tools.is_some(), "tools"),
        (request.output_format.is_some(), "output_format"),
        (request.stream == Some(true), "stream"),
    ];
    if let Some((_, parameter)) = unsupported.into_iter().find(|(present, _)| *present) {
        return Err(ArkAdmissionError::Unsupported(parameter));
    }
    match request.response_format.as_deref() {
        None | Some("b64_json") => Ok(()),
        Some("url") => Err(ArkAdmissionError::Unsupported("response_format")),
        Some(_) => Err(ArkAdmissionError::InvalidValue("response_format")),
    }
}

fn ark_image_count(request: &ArkImageGenerationRequest) -> Result<u8, ArkAdmissionError> {
    match request.sequential_image_generation.as_deref() {
        None | Some("disabled") => {
            if request.sequential_image_generation_options.is_some() {
                return Err(ArkAdmissionError::InvalidValue(
                    "sequential_image_generation_options",
                ));
            }
            Ok(1)
        }
        Some("auto") => request
            .sequential_image_generation_options
            .as_ref()
            .and_then(|options| options.max_images)
            .filter(|count| (1..=10).contains(count))
            .ok_or(ArkAdmissionError::InvalidValue(
                "sequential_image_generation_options.max_images",
            )),
        Some(_) => Err(ArkAdmissionError::InvalidValue(
            "sequential_image_generation",
        )),
    }
}

fn ark_image_model(model: &str) -> Result<&'static str, ArkAdmissionError> {
    if model_matches(model, "doubao-seedream-5-0-lite") {
        Ok("5.0")
    } else if model_matches(model, "doubao-seedream-5-0")
        || model_matches(model, "doubao-seedream-5-0-pro")
    {
        Ok("5.0Pro")
    } else {
        Err(ArkAdmissionError::InvalidValue("model"))
    }
}

fn ark_video_model(model: &str) -> Result<&'static str, ArkAdmissionError> {
    if model_matches(model, "doubao-seedance-2-0-fast") {
        Ok("seedance2.0fast")
    } else if model_matches(model, "doubao-seedance-2-0-mini") {
        Ok("seedance2.0mini")
    } else if model_matches(model, "doubao-seedance-2-0") {
        Ok("seedance2.0")
    } else {
        Err(ArkAdmissionError::InvalidValue("model"))
    }
}

fn model_matches(value: &str, stable_name: &str) -> bool {
    value == stable_name
        || value.strip_prefix(stable_name).is_some_and(|suffix| {
            suffix.len() == 7
                && suffix.starts_with('-')
                && suffix[1..].bytes().all(|byte| byte.is_ascii_digit())
        })
}

type ArkImageGeometry = (String, Option<String>, Option<u32>, Option<u32>);

fn ark_image_size(size: &str) -> Result<ArkImageGeometry, ArkAdmissionError> {
    match size.to_ascii_lowercase().as_str() {
        "1k" => return Ok(("1k".to_owned(), Some("1:1".to_owned()), None, None)),
        "2k" => return Ok(("2k".to_owned(), Some("1:1".to_owned()), None, None)),
        "4k" => return Ok(("4k".to_owned(), Some("1:1".to_owned()), None, None)),
        _ => {}
    }
    let Some((width, height)) = size.split_once(['x', 'X']) else {
        return Err(ArkAdmissionError::InvalidValue("size"));
    };
    let width = width
        .parse::<u32>()
        .map_err(|_| ArkAdmissionError::InvalidValue("size"))?;
    let height = height
        .parse::<u32>()
        .map_err(|_| ArkAdmissionError::InvalidValue("size"))?;
    let resolution =
        custom_dimension_bucket(width, height).ok_or(ArkAdmissionError::InvalidValue("size"))?;
    Ok((resolution.to_owned(), None, Some(width), Some(height)))
}

fn custom_dimension_bucket(width: u32, height: u32) -> Option<&'static str> {
    let pixels = u64::from(width) * u64::from(height);
    if (512..=2_016).contains(&width) && (512..=2_016).contains(&height) && pixels <= 1_763_584 {
        Some("1k")
    } else if (768..=3_072).contains(&width)
        && (768..=3_072).contains(&height)
        && pixels <= 4_194_304
    {
        Some("2k")
    } else if (1_536..=6_240).contains(&width)
        && (1_536..=6_240).contains(&height)
        && pixels <= 16_777_216
    {
        Some("4k")
    } else {
        None
    }
}

fn reject_unsupported_video_fields(
    request: &ArkContentGenerationTaskRequest,
) -> Result<(), ArkAdmissionError> {
    let unsupported = [
        (request.safety_identifier.is_some(), "safety_identifier"),
        (request.callback_url.is_some(), "callback_url"),
        (request.return_last_frame.is_some(), "return_last_frame"),
        (request.service_tier.is_some(), "service_tier"),
        (
            request.execution_expires_after.is_some(),
            "execution_expires_after",
        ),
        (request.priority.is_some(), "priority"),
        (request.generate_audio.is_some(), "generate_audio"),
        (request.draft.is_some(), "draft"),
        (request.camera_fixed.is_some(), "camera_fixed"),
        (request.watermark.is_some(), "watermark"),
        (request.seed.is_some(), "seed"),
        (request.frames.is_some(), "frames"),
        (request.tools.is_some(), "tools"),
    ];
    if let Some((_, parameter)) = unsupported.into_iter().find(|(present, _)| *present) {
        Err(ArkAdmissionError::Unsupported(parameter))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use image_api_contracts::ark::ArkSequentialImageGenerationOptions;

    #[test]
    fn official_seedream_request_maps_to_an_immutable_cli_command() {
        let plan = ArkImageAdmissionPlan::new(ArkImageGenerationRequest {
            model: "doubao-seedream-5-0-260128".to_owned(),
            prompt: "a city".to_owned(),
            image: None,
            response_format: Some("b64_json".to_owned()),
            size: Some("2048x2048".to_owned()),
            seed: None,
            guidance_scale: None,
            watermark: None,
            optimize_prompt: None,
            optimize_prompt_options: None,
            sequential_image_generation: Some("auto".to_owned()),
            sequential_image_generation_options: Some(ArkSequentialImageGenerationOptions {
                max_images: Some(3),
            }),
            tools: None,
            output_format: None,
            stream: Some(false),
        })
        .unwrap();
        assert_eq!(plan.public_model(), "doubao-seedream-5-0-260128");
        assert_eq!(plan.provider_model(), "dreamina-image-5.0Pro");
        assert_eq!(plan.output_count(), 3);
    }

    #[test]
    fn official_seedance_request_maps_to_the_current_text_only_cli() {
        let plan = ArkVideoAdmissionPlan::new(ArkContentGenerationTaskRequest {
            model: "doubao-seedance-2-0-fast-260128".to_owned(),
            content: vec![ArkContentItem::Text {
                text: "camera push in".to_owned(),
            }],
            safety_identifier: None,
            callback_url: None,
            return_last_frame: None,
            service_tier: None,
            execution_expires_after: None,
            priority: None,
            generate_audio: None,
            draft: None,
            camera_fixed: None,
            watermark: None,
            seed: None,
            resolution: Some("720p".to_owned()),
            ratio: Some("16:9".to_owned()),
            duration: Some(5),
            frames: None,
            tools: None,
        })
        .unwrap();
        assert_eq!(plan.provider_model(), "seedance2.0fast");
        assert_eq!(plan.duration(), 5);
    }

    #[test]
    fn represented_but_unbound_official_fields_fail_closed() {
        let mut request: ArkContentGenerationTaskRequest = serde_json::from_str(
            r#"{"model":"doubao-seedance-2-0-260128","content":[{"type":"text","text":"city"}],"callback_url":"https://example.com/callback"}"#,
        )
        .unwrap();
        assert_eq!(
            ArkVideoAdmissionPlan::new(request.clone()).unwrap_err(),
            ArkAdmissionError::Unsupported("callback_url")
        );
        request.callback_url = None;
        request.content.push(ArkContentItem::ImageUrl {
            image_url: image_api_contracts::ark::ArkMediaUrl {
                url: "https://example.com/image.png".to_owned(),
            },
            role: "first_frame".to_owned(),
        });
        assert_eq!(
            ArkVideoAdmissionPlan::new(request).unwrap_err(),
            ArkAdmissionError::Unsupported("content")
        );
    }
}
