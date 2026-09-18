use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{XaiPublicUrlConfig, XaiPublicUrlOptions};

pub const XAI_VIDEOS_API_PROFILE: &str = "xai-videos-v1";
pub const XAI_VIDEO_GENERATION_COMMAND_SCHEMA: &str = "xai.videos.generations.v1";
pub const XAI_VIDEO_GENERATION_COMMAND_SCHEMA_V2: &str = "xai.videos.generations.v2";
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
    #[serde(default)]
    pub generate_audio: Option<bool>,
    #[serde(default, alias = "input_reference")]
    pub image: Option<XaiVideoImageUrl>,
    #[serde(default)]
    pub last_frame: Option<XaiVideoImageUrl>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub output: Option<XaiVideoOutput>,
    /// Optional when an image, frame, or reference is supplied; required for text-to-video.
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub reference_audios: Vec<XaiVideoAudioReference>,
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
pub struct XaiVideoAudioReference {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub voice_id: Option<String>,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiVideoGenerationCommandV2 {
    pub schema_version: u16,
    pub operation: String,
    pub aspect_ratio: Option<XaiVideoAspectRatio>,
    pub duration: u8,
    pub generate_audio: bool,
    pub image: Option<XaiVideoImageUrl>,
    pub last_frame: Option<XaiVideoImageUrl>,
    pub model: Option<String>,
    pub output: Option<XaiVideoOutput>,
    pub prompt: Option<String>,
    pub reference_audios: Vec<XaiVideoAudioReference>,
    pub reference_images: Vec<XaiVideoImageUrl>,
    pub resolution: XaiVideoResolution,
    pub storage_options: Option<XaiVideoStorageOptions>,
    pub user: Option<String>,
}

impl XaiVideoGenerationCommandV1 {
    pub fn from_request(request: XaiVideoGenerationRequest) -> Result<Self, XaiVideoRequestError> {
        if request.generate_audio.is_some() {
            return Err(XaiVideoRequestError::UnsupportedGenerateAudio);
        }
        if request.last_frame.is_some() {
            return Err(XaiVideoRequestError::UnsupportedLastFrame);
        }
        if !request.reference_audios.is_empty() {
            return Err(XaiVideoRequestError::UnsupportedReferenceAudios);
        }
        validate_model(request.model.as_deref())?;
        validate_user(request.user.as_deref())?;
        let prompt = normalize_prompt(request.prompt);
        validate_prompt(prompt.as_deref())?;
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
        if workflow != XaiVideoWorkflow::ImageToVideo && prompt.is_none() {
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
            prompt,
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

impl XaiVideoGenerationCommandV2 {
    pub fn from_request(request: XaiVideoGenerationRequest) -> Result<Self, XaiVideoRequestError> {
        validate_model(request.model.as_deref())?;
        validate_user(request.user.as_deref())?;
        let prompt = normalize_prompt(request.prompt);
        validate_prompt(prompt.as_deref())?;
        if let Some(image) = request.image.as_ref() {
            validate_image(image).map_err(|_| XaiVideoRequestError::InvalidImage)?;
        }
        if let Some(last_frame) = request.last_frame.as_ref() {
            validate_image(last_frame).map_err(|_| XaiVideoRequestError::InvalidLastFrame)?;
        }
        for image in &request.reference_images {
            validate_image(image).map_err(|_| XaiVideoRequestError::InvalidReferenceImage)?;
        }
        if request.reference_images.len() > 7 {
            return Err(XaiVideoRequestError::TooManyReferenceImages);
        }
        for audio in &request.reference_audios {
            validate_audio(audio)?;
        }
        if request.reference_audios.len() > 3 {
            return Err(XaiVideoRequestError::TooManyReferenceAudios);
        }
        let has_frame_or_reference = request.image.is_some()
            || request.last_frame.is_some()
            || !request.reference_images.is_empty()
            || !request.reference_audios.is_empty();
        if !has_frame_or_reference && prompt.is_none() {
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
            schema_version: 2,
            operation: "videos.generations".to_owned(),
            aspect_ratio: request.aspect_ratio,
            duration,
            generate_audio: request.generate_audio.unwrap_or(true),
            image: request.image,
            last_frame: request.last_frame,
            model: request.model,
            output: request.output,
            prompt,
            reference_audios: request.reference_audios,
            reference_images: request.reference_images,
            resolution: request.resolution.unwrap_or(XaiVideoResolution::P480),
            storage_options: request.storage_options,
            user: request.user,
        })
    }

    pub fn workflow(&self) -> XaiVideoWorkflow {
        if self.last_frame.is_some()
            || !self.reference_images.is_empty()
            || !self.reference_audios.is_empty()
        {
            XaiVideoWorkflow::ReferenceToVideo
        } else if self.image.is_some() {
            XaiVideoWorkflow::ImageToVideo
        } else {
            XaiVideoWorkflow::TextToVideo
        }
    }

    pub fn canonical_sha256_hex(&self) -> String {
        let bytes = serde_json::to_vec(self)
            .expect("xAI video generation v2 command serialization cannot fail");
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
    #[error("xAI video generate_audio is not supported by the v1 command")]
    UnsupportedGenerateAudio,
    #[error("xAI video last_frame is not supported by the v1 command")]
    UnsupportedLastFrame,
    #[error("xAI video reference_audios are not supported by the v1 command")]
    UnsupportedReferenceAudios,
    #[error("xAI video last_frame input is invalid")]
    InvalidLastFrame,
    #[error("xAI video reference image input is invalid")]
    InvalidReferenceImage,
    #[error("xAI video reference image count exceeds seven")]
    TooManyReferenceImages,
    #[error("xAI video reference audio is invalid")]
    InvalidReferenceAudio,
    #[error("xAI video reference audio count exceeds three")]
    TooManyReferenceAudios,
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
            Self::UnsupportedGenerateAudio => "generate_audio",
            Self::UnsupportedLastFrame | Self::InvalidLastFrame => "last_frame",
            Self::UnsupportedReferenceAudios => "reference_audios",
            Self::InvalidReferenceImage => "reference_images",
            Self::TooManyReferenceImages => "reference_images",
            Self::InvalidReferenceAudio | Self::TooManyReferenceAudios => "reference_audios",
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

fn validate_audio(audio: &XaiVideoAudioReference) -> Result<(), XaiVideoRequestError> {
    let url_valid = audio.url.as_deref().is_some_and(valid_text);
    let voice_valid = audio.voice_id.as_deref().is_some_and(valid_text);
    if url_valid ^ voice_valid {
        Ok(())
    } else {
        Err(XaiVideoRequestError::InvalidReferenceAudio)
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
    fn v2_classifies_last_frame_as_reference_video() {
        let request: XaiVideoGenerationRequest = serde_json::from_value(serde_json::json!({
            "model": "grok-imagine-video-1.5",
            "last_frame": {"url": "data:image/png;base64,AA=="}
        }))
        .unwrap();
        let command = XaiVideoGenerationCommandV2::from_request(request).unwrap();
        assert_eq!(command.workflow(), XaiVideoWorkflow::ReferenceToVideo);
    }

    #[test]
    fn v2_preserves_official_defaults_and_seconds_alias() {
        let request: XaiVideoGenerationRequest = serde_json::from_value(serde_json::json!({
            "model": "grok-imagine-video-1.5",
            "prompt": "moonlit lake",
            "seconds": "8"
        }))
        .unwrap();
        let command = XaiVideoGenerationCommandV2::from_request(request).unwrap();
        assert_eq!(command.duration, 8);
        assert_eq!(command.resolution, XaiVideoResolution::P480);
    }

    #[test]
    fn v2_rejects_more_than_seven_images() {
        let mut request = request_with_reference_images(8);
        assert_eq!(
            XaiVideoGenerationCommandV2::from_request(request.clone()),
            Err(XaiVideoRequestError::TooManyReferenceImages)
        );
        request.reference_images.truncate(7);
        assert!(XaiVideoGenerationCommandV2::from_request(request).is_ok());
    }

    #[test]
    fn v2_rejects_ambiguous_audio_sources() {
        let request: XaiVideoGenerationRequest = serde_json::from_value(serde_json::json!({
            "model": "grok-imagine-video-1.5",
            "reference_audios": [{"url": "https://example.invalid/a.wav", "voice_id": "eve"}]
        }))
        .unwrap();
        assert_eq!(
            XaiVideoGenerationCommandV2::from_request(request),
            Err(XaiVideoRequestError::InvalidReferenceAudio)
        );
        assert_eq!(
            XaiVideoRequestError::InvalidReferenceAudio.parameter(),
            "reference_audios"
        );
    }

    #[test]
    fn v2_requires_nonblank_prompt_only_for_text_video() {
        let mut text = v1_text_request();
        text.prompt = Some("   ".to_owned());
        assert_eq!(
            XaiVideoGenerationCommandV2::from_request(text),
            Err(XaiVideoRequestError::PromptRequired)
        );

        let mut image = v1_text_request();
        image.prompt = Some("   ".to_owned());
        image.image = Some(XaiVideoImageUrl {
            file_id: None,
            url: Some("https://example.com/first.png".to_owned()),
        });
        assert!(XaiVideoGenerationCommandV2::from_request(image).is_ok());

        let mut reference = v1_text_request();
        reference.prompt = Some("   ".to_owned());
        reference.reference_images = vec![XaiVideoImageUrl {
            file_id: None,
            url: Some("https://example.com/reference.png".to_owned()),
        }];
        assert!(XaiVideoGenerationCommandV2::from_request(reference).is_ok());
    }

    #[test]
    fn v1_rejects_each_v2_only_field_with_its_parameter() {
        let mut generate_audio = v1_text_request();
        generate_audio.generate_audio = Some(true);
        assert_eq!(
            XaiVideoGenerationCommandV1::from_request(generate_audio),
            Err(XaiVideoRequestError::UnsupportedGenerateAudio)
        );
        assert_eq!(
            XaiVideoRequestError::UnsupportedGenerateAudio.parameter(),
            "generate_audio"
        );

        let mut last_frame = v1_text_request();
        last_frame.last_frame = Some(XaiVideoImageUrl {
            file_id: None,
            url: Some("https://example.com/last.png".to_owned()),
        });
        assert_eq!(
            XaiVideoGenerationCommandV1::from_request(last_frame),
            Err(XaiVideoRequestError::UnsupportedLastFrame)
        );
        assert_eq!(
            XaiVideoRequestError::UnsupportedLastFrame.parameter(),
            "last_frame"
        );

        let mut audios = v1_text_request();
        audios.reference_audios = vec![XaiVideoAudioReference {
            url: None,
            voice_id: Some("eve".to_owned()),
        }];
        assert_eq!(
            XaiVideoGenerationCommandV1::from_request(audios),
            Err(XaiVideoRequestError::UnsupportedReferenceAudios)
        );
        assert_eq!(
            XaiVideoRequestError::UnsupportedReferenceAudios.parameter(),
            "reference_audios"
        );
    }

    #[test]
    fn v2_enforces_audio_limit_and_source_parameter() {
        let mut request = v1_text_request();
        request.reference_audios = (0..3)
            .map(|index| XaiVideoAudioReference {
                url: None,
                voice_id: Some(format!("voice-{index}")),
            })
            .collect();
        assert!(XaiVideoGenerationCommandV2::from_request(request.clone()).is_ok());
        request.reference_audios.push(XaiVideoAudioReference {
            url: None,
            voice_id: Some("voice-3".to_owned()),
        });
        assert_eq!(
            XaiVideoGenerationCommandV2::from_request(request),
            Err(XaiVideoRequestError::TooManyReferenceAudios)
        );
        assert_eq!(
            XaiVideoRequestError::TooManyReferenceAudios.parameter(),
            "reference_audios"
        );

        let mut missing = v1_text_request();
        missing.reference_audios = vec![XaiVideoAudioReference {
            url: None,
            voice_id: None,
        }];
        assert_eq!(
            XaiVideoGenerationCommandV2::from_request(missing),
            Err(XaiVideoRequestError::InvalidReferenceAudio)
        );
    }

    #[test]
    fn v2_preserves_field_parameters_and_combined_frame_precedence() {
        let mut invalid_image = v1_text_request();
        invalid_image.prompt = None;
        invalid_image.image = Some(XaiVideoImageUrl {
            file_id: None,
            url: None,
        });
        assert_eq!(
            XaiVideoGenerationCommandV2::from_request(invalid_image),
            Err(XaiVideoRequestError::InvalidImage)
        );
        assert_eq!(XaiVideoRequestError::InvalidImage.parameter(), "image");

        let mut invalid_last = v1_text_request();
        invalid_last.last_frame = Some(XaiVideoImageUrl {
            file_id: None,
            url: None,
        });
        assert_eq!(
            XaiVideoGenerationCommandV2::from_request(invalid_last),
            Err(XaiVideoRequestError::InvalidLastFrame)
        );
        assert_eq!(
            XaiVideoRequestError::InvalidLastFrame.parameter(),
            "last_frame"
        );

        let mut invalid_reference = v1_text_request();
        invalid_reference.reference_images = vec![XaiVideoImageUrl {
            file_id: None,
            url: None,
        }];
        assert_eq!(
            XaiVideoGenerationCommandV2::from_request(invalid_reference),
            Err(XaiVideoRequestError::InvalidReferenceImage)
        );
        assert_eq!(
            XaiVideoRequestError::InvalidReferenceImage.parameter(),
            "reference_images"
        );

        let mut combined = v1_text_request();
        combined.prompt = None;
        combined.image = Some(XaiVideoImageUrl {
            file_id: None,
            url: Some("https://example.com/first.png".to_owned()),
        });
        combined.last_frame = Some(XaiVideoImageUrl {
            file_id: None,
            url: Some("https://example.com/last.png".to_owned()),
        });
        assert_eq!(
            XaiVideoGenerationCommandV2::from_request(combined)
                .unwrap()
                .workflow(),
            XaiVideoWorkflow::ReferenceToVideo
        );
    }

    #[test]
    fn v1_golden_command_bytes_do_not_change() {
        let command = v1_text_command();
        assert_eq!(
            serde_json::to_vec(&command).unwrap(),
            br#"{"schema_version":1,"operation":"videos.generations","aspect_ratio":"16:9","duration":6,"image":null,"model":"grok-imagine-video-1.5-preview","output":null,"prompt":"moonlit lake","reference_images":[],"resolution":"480p","storage_options":null,"user":null}"#
        );
    }

    fn request_with_reference_images(count: usize) -> XaiVideoGenerationRequest {
        XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(8),
            generate_audio: None,
            image: None,
            last_frame: None,
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: None,
            reference_audios: Vec::new(),
            reference_images: (0..count)
                .map(|index| XaiVideoImageUrl {
                    file_id: None,
                    url: Some(format!("https://example.com/{index}.png")),
                })
                .collect(),
            resolution: None,
            storage_options: None,
            user: None,
        }
    }

    fn v1_text_command() -> XaiVideoGenerationCommandV1 {
        XaiVideoGenerationCommandV1::from_request(v1_text_request()).unwrap()
    }

    fn v1_text_request() -> XaiVideoGenerationRequest {
        XaiVideoGenerationRequest {
            aspect_ratio: Some(XaiVideoAspectRatio::R16x9),
            duration: Some(6),
            generate_audio: None,
            image: None,
            last_frame: None,
            model: Some("grok-imagine-video-1.5-preview".to_owned()),
            output: None,
            prompt: Some("moonlit lake".to_owned()),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: None,
            storage_options: None,
            user: None,
        }
    }

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
            generate_audio: None,
            image: command.image.clone(),
            last_frame: None,
            model: command.model.clone(),
            output: None,
            prompt: command.prompt.clone(),
            reference_audios: Vec::new(),
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
            generate_audio: None,
            image: Some(image.clone()),
            last_frame: None,
            model: None,
            output: None,
            prompt: None,
            reference_audios: Vec::new(),
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
            generate_audio: None,
            image: None,
            last_frame: None,
            model: None,
            output: None,
            prompt: None,
            reference_audios: Vec::new(),
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
