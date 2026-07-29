use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{XaiPublicUrlConfig, XaiPublicUrlOptions};

pub const XAI_VIDEOS_API_PROFILE: &str = "xai-videos-v1";
pub const XAI_VIDEO_GENERATION_COMMAND_SCHEMA: &str = "xai.videos.generations.v1";
const MIN_DURATION_SECONDS: u8 = 1;
const MAX_DURATION_SECONDS: u8 = 15;
const DEFAULT_DURATION_SECONDS: u8 = 8;
const MIN_STORAGE_TTL_SECONDS: i64 = 3_600;
const MAX_STORAGE_TTL_SECONDS: i64 = 2_592_000;

/// Versioned xAI wire DTO. Unsupported binding fields remain represented and fail closed later.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiVideoGenerationRequest {
    #[serde(default)]
    pub aspect_ratio: Option<XaiVideoAspectRatio>,
    /// xAI accepts both `duration` and the OpenAI-compatible alias `seconds`.
    #[serde(
        default,
        alias = "seconds",
        deserialize_with = "deserialize_optional_duration"
    )]
    pub duration: Option<u8>,
    #[serde(default, alias = "input_reference")]
    pub image: Option<XaiVideoImageUrl>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub output: Option<XaiVideoOutput>,
    /// Optional only for image-to-video; xAI requires it for text/reference workflows.
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub reference_images: Vec<XaiVideoImageUrl>,
    #[serde(default)]
    pub resolution: Option<XaiVideoResolution>,
    #[serde(default)]
    pub storage_options: Option<XaiVideoStorageOptions>,
    #[serde(default)]
    pub user: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum XaiVideoAspectRatio {
    #[serde(rename = "1:1")]
    R1x1,
    #[serde(rename = "16:9")]
    R16x9,
    #[serde(rename = "9:16")]
    R9x16,
    #[serde(rename = "4:3")]
    R4x3,
    #[serde(rename = "3:4")]
    R3x4,
    #[serde(rename = "3:2")]
    R3x2,
    #[serde(rename = "2:3")]
    R2x3,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum XaiVideoResolution {
    #[serde(rename = "480p")]
    P480,
    #[serde(rename = "720p")]
    P720,
    #[serde(rename = "1080p")]
    P1080,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiVideoImageUrl {
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default, alias = "image_url")]
    pub url: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiVideoOutput {
    pub upload_url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiVideoStorageOptions {
    #[serde(default)]
    pub expires_after: Option<i64>,
    pub filename: String,
    #[serde(default)]
    pub public_url: Option<XaiPublicUrlOptions>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XaiVideoWorkflow {
    TextToVideo,
    ImageToVideo,
    ReferenceToVideo,
}

/// Canonical official request semantics before any provider-specific projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiVideoGenerationCommandV1 {
    pub schema_version: u16,
    pub operation: String,
    pub aspect_ratio: Option<XaiVideoAspectRatio>,
    pub duration: u8,
    pub image: Option<XaiVideoImageUrl>,
    pub model: Option<String>,
    pub output: Option<XaiVideoOutput>,
    pub prompt: Option<String>,
    pub reference_images: Vec<XaiVideoImageUrl>,
    pub resolution: XaiVideoResolution,
    pub storage_options: Option<XaiVideoStorageOptions>,
    pub user: Option<String>,
}

impl XaiVideoGenerationCommandV1 {
    pub fn from_request(request: XaiVideoGenerationRequest) -> Result<Self, XaiVideoRequestError> {
        validate_model(request.model.as_deref())?;
        validate_user(request.user.as_deref())?;
        validate_prompt(request.prompt.as_deref())?;
        if let Some(image) = request.image.as_ref() {
            validate_image(image)?;
        }
        for image in &request.reference_images {
            validate_image(image)?;
        }
        if request.image.is_some() && !request.reference_images.is_empty() {
            return Err(XaiVideoRequestError::ConflictingInputs);
        }
        let workflow = if request.image.is_some() {
            XaiVideoWorkflow::ImageToVideo
        } else if request.reference_images.is_empty() {
            XaiVideoWorkflow::TextToVideo
        } else {
            XaiVideoWorkflow::ReferenceToVideo
        };
        if workflow != XaiVideoWorkflow::ImageToVideo && request.prompt.is_none() {
            return Err(XaiVideoRequestError::PromptRequired);
        }
        let duration = request.duration.unwrap_or(DEFAULT_DURATION_SECONDS);
        if !(MIN_DURATION_SECONDS..=MAX_DURATION_SECONDS).contains(&duration) {
            return Err(XaiVideoRequestError::InvalidDuration);
        }
        if request
            .output
            .as_ref()
            .is_some_and(|output| !valid_text(&output.upload_url))
        {
            return Err(XaiVideoRequestError::InvalidOutput);
        }
        if let Some(storage) = request.storage_options.as_ref() {
            validate_storage(storage)?;
        }
        Ok(Self {
            schema_version: 1,
            operation: "videos.generations".to_owned(),
            aspect_ratio: request.aspect_ratio,
            duration,
            image: request.image,
            model: request.model,
            output: request.output,
            prompt: normalize_prompt(request.prompt),
            reference_images: request.reference_images,
            resolution: request.resolution.unwrap_or(XaiVideoResolution::P480),
            storage_options: request.storage_options,
            user: request.user,
        })
    }

    pub fn workflow(&self) -> XaiVideoWorkflow {
        if self.image.is_some() {
            XaiVideoWorkflow::ImageToVideo
        } else if self.reference_images.is_empty() {
            XaiVideoWorkflow::TextToVideo
        } else {
            XaiVideoWorkflow::ReferenceToVideo
        }
    }

    pub fn canonical_sha256_hex(&self) -> String {
        let bytes = serde_json::to_vec(self)
            .expect("xAI video generation command serialization cannot fail");
        hex::encode(Sha256::digest(bytes))
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum XaiVideoRequestError {
    #[error("xAI video prompt is invalid")]
    InvalidPrompt,
    #[error("xAI video prompt is required for this workflow")]
    PromptRequired,
    #[error("xAI video duration must be between 1 and 15 seconds")]
    InvalidDuration,
    #[error("xAI video model is invalid")]
    InvalidModel,
    #[error("xAI video user is invalid")]
    InvalidUser,
    #[error("xAI video image input is invalid")]
    InvalidImage,
    #[error("xAI video image and reference_images are mutually exclusive")]
    ConflictingInputs,
    #[error("xAI video output is invalid")]
    InvalidOutput,
    #[error("xAI video storage options are invalid")]
    InvalidStorageOptions,
}

impl XaiVideoRequestError {
    pub const fn parameter(self) -> &'static str {
        match self {
            Self::InvalidPrompt | Self::PromptRequired => "prompt",
            Self::InvalidDuration => "duration",
            Self::InvalidModel => "model",
            Self::InvalidUser => "user",
            Self::InvalidImage => "image",
            Self::ConflictingInputs => "reference_images",
            Self::InvalidOutput => "output",
            Self::InvalidStorageOptions => "storage_options",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaiStartDeferredResponse {
    pub request_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaiVideoResponse {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<XaiVideoError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<XaiVideoUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video: Option<XaiGeneratedVideo>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaiVideoError {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaiVideoUsage {
    pub cost_in_usd_ticks: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaiGeneratedVideo {
    pub duration: u8,
    pub respect_moderation: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_output: Option<XaiVideoFileOutput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaiVideoFileOutput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    pub file_id: String,
    pub filename: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url_expires_at: Option<i64>,
}

fn validate_model(model: Option<&str>) -> Result<(), XaiVideoRequestError> {
    if model.is_some_and(|model| !valid_text(model)) {
        Err(XaiVideoRequestError::InvalidModel)
    } else {
        Ok(())
    }
}

fn validate_user(user: Option<&str>) -> Result<(), XaiVideoRequestError> {
    if user.is_some_and(|user| !valid_text(user)) {
        Err(XaiVideoRequestError::InvalidUser)
    } else {
        Ok(())
    }
}

fn validate_prompt(prompt: Option<&str>) -> Result<(), XaiVideoRequestError> {
    if prompt.is_some_and(|prompt| prompt.contains('\0')) {
        Err(XaiVideoRequestError::InvalidPrompt)
    } else {
        Ok(())
    }
}

fn normalize_prompt(prompt: Option<String>) -> Option<String> {
    prompt.and_then(|prompt| (!prompt.trim().is_empty()).then_some(prompt))
}

fn validate_image(image: &XaiVideoImageUrl) -> Result<(), XaiVideoRequestError> {
    let url_valid = image.url.as_deref().is_some_and(valid_text);
    let file_valid = image.file_id.as_deref().is_some_and(valid_text);
    if url_valid ^ file_valid {
        Ok(())
    } else {
        Err(XaiVideoRequestError::InvalidImage)
    }
}

fn validate_storage(storage: &XaiVideoStorageOptions) -> Result<(), XaiVideoRequestError> {
    let public_expiry = match storage.public_url.as_ref() {
        Some(XaiPublicUrlOptions::Options(XaiPublicUrlConfig { expires_after })) => {
            expires_after.map(i64::from)
        }
        Some(XaiPublicUrlOptions::Enabled(_)) | None => None,
    };
    if storage.filename.is_empty()
        || storage.filename.len() > 255
        || storage.filename.chars().any(char::is_control)
        || storage.expires_after.is_some_and(|seconds| {
            !(MIN_STORAGE_TTL_SECONDS..=MAX_STORAGE_TTL_SECONDS).contains(&seconds)
        })
        || public_expiry.is_some_and(|seconds| {
            !(MIN_STORAGE_TTL_SECONDS..=MAX_STORAGE_TTL_SECONDS).contains(&seconds)
                || storage
                    .expires_after
                    .is_some_and(|file_expiry| seconds > file_expiry)
        })
    {
        Err(XaiVideoRequestError::InvalidStorageOptions)
    } else {
        Ok(())
    }
}

fn valid_text(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(char::is_control)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum DurationWire {
    Number(u8),
    Text(String),
}

fn deserialize_optional_duration<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let wire = Option::<DurationWire>::deserialize(deserializer)?;
    wire.map(|wire| match wire {
        DurationWire::Number(value) => Ok(value),
        DurationWire::Text(value) => value.parse::<u8>().map_err(serde::de::Error::custom),
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn official_aliases_and_defaults_normalize_stably() {
        let request: XaiVideoGenerationRequest = serde_json::from_str(
            r#"{
                "model":"grok-imagine-video",
                "prompt":"subtle camera motion",
                "input_reference":{"image_url":"data:image/png;base64,AA=="},
                "seconds":"6"
            }"#,
        )
        .unwrap();
        let command = XaiVideoGenerationCommandV1::from_request(request).unwrap();
        assert_eq!(command.workflow(), XaiVideoWorkflow::ImageToVideo);
        assert_eq!(command.duration, 6);
        assert_eq!(command.resolution, XaiVideoResolution::P480);
        assert_eq!(command.aspect_ratio, None);
        assert_eq!(command.canonical_sha256_hex().len(), 64);

        let defaulted = XaiVideoGenerationCommandV1::from_request(XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: None,
            image: command.image.clone(),
            model: command.model.clone(),
            output: None,
            prompt: command.prompt.clone(),
            reference_images: Vec::new(),
            resolution: None,
            storage_options: None,
            user: None,
        })
        .unwrap();
        assert_eq!(defaulted.duration, 8);
    }

    #[test]
    fn workflow_and_input_conflicts_fail_before_projection() {
        let image = XaiVideoImageUrl {
            file_id: None,
            url: Some("https://example.com/source.png".to_owned()),
        };
        let request = XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(6),
            image: Some(image.clone()),
            model: None,
            output: None,
            prompt: None,
            reference_images: vec![image],
            resolution: None,
            storage_options: None,
            user: None,
        };
        assert_eq!(
            XaiVideoGenerationCommandV1::from_request(request),
            Err(XaiVideoRequestError::ConflictingInputs)
        );
    }

    #[test]
    fn text_and_reference_workflows_require_a_prompt() {
        let request = XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(6),
            image: None,
            model: None,
            output: None,
            prompt: None,
            reference_images: Vec::new(),
            resolution: None,
            storage_options: None,
            user: None,
        };
        assert_eq!(
            XaiVideoGenerationCommandV1::from_request(request),
            Err(XaiVideoRequestError::PromptRequired)
        );
    }
}
