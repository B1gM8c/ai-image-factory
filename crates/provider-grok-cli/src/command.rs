use image_api_contracts::xai::{XaiImageGenerationCommandV1, XaiVideoGenerationCommandV1};
use image_provider_sdk::{CanonicalCommandPayload, OutputSlot};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ADAPTER_REVISION, GrokImageEditRequestV1, GrokImageGenerationRequestV1,
    GrokVideoGenerationRequestV1, ImageAspectRatio, ImageModel, ImageToVideoRequestV1,
    REQUEST_SCHEMA_VERSION, ReferenceToVideoRequestV1, RequestValidationError, StagedImageV1,
    VideoAspectRatio, VideoDuration, VideoResolution, XaiGrokProjectionError,
    XaiGrokVideoProjectionError, project_xai_image_generation, project_xai_video_generation,
};

pub const GROK_IMAGE_GENERATION_COMMAND_SCHEMA: &str = "grok-cli.images.generate.v1";
pub const GROK_IMAGE_EDIT_COMMAND_SCHEMA: &str = "grok-cli.images.edit.v1";
pub const GROK_VIDEO_GENERATION_COMMAND_SCHEMA: &str = "grok-cli.videos.generate.v1";
pub const MAX_CANONICAL_COMMAND_BYTES: usize = 64 * 1024;
const STAGED_INPUT_URL_PREFIX: &str = "factory-staged-sha256:";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrokImageGenerationPayloadV1 {
    source_command: XaiImageGenerationCommandV1,
    source_command_sha256: String,
    request: GrokImageGenerationRequestV1,
}

impl GrokImageGenerationPayloadV1 {
    pub fn from_xai_command(
        source_command: XaiImageGenerationCommandV1,
    ) -> Result<Self, GrokCommandError> {
        let request = project_xai_image_generation(source_command.clone())?.into_provider_request();
        let source_command_sha256 = source_command.canonical_sha256_hex();
        Ok(Self {
            source_command,
            source_command_sha256,
            request,
        })
    }

    pub fn source_command(&self) -> &XaiImageGenerationCommandV1 {
        &self.source_command
    }

    pub fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }

    pub fn request(&self) -> &GrokImageGenerationRequestV1 {
        &self.request
    }

    pub fn into_request(self) -> GrokImageGenerationRequestV1 {
        self.request
    }
}

impl CanonicalCommandPayload for GrokImageGenerationPayloadV1 {
    const SCHEMA_ID: &'static str = GROK_IMAGE_GENERATION_COMMAND_SCHEMA;
    const ADAPTER_REVISION: &'static str = ADAPTER_REVISION;

    fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }

    fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
        serde_json::to_vec(&CanonicalImageGenerationV1::from(self))
            .expect("Grok image generation command serialization cannot fail")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrokImageEditPayloadV1 {
    source_command_sha256: String,
    request: GrokImageEditRequestV1,
}

impl GrokImageEditPayloadV1 {
    pub fn new(
        source_command_sha256: impl Into<String>,
        request: GrokImageEditRequestV1,
    ) -> Result<Self, GrokCommandError> {
        Ok(Self {
            source_command_sha256: validate_source_sha256(source_command_sha256.into())?,
            request,
        })
    }

    pub fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }

    pub fn request(&self) -> &GrokImageEditRequestV1 {
        &self.request
    }

    pub fn into_request(self) -> GrokImageEditRequestV1 {
        self.request
    }
}

impl CanonicalCommandPayload for GrokImageEditPayloadV1 {
    const SCHEMA_ID: &'static str = GROK_IMAGE_EDIT_COMMAND_SCHEMA;
    const ADAPTER_REVISION: &'static str = ADAPTER_REVISION;

    fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }

    fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
        serde_json::to_vec(&CanonicalImageEditV1::from(self))
            .expect("Grok image edit command serialization cannot fail")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrokVideoGenerationPayloadV1 {
    source_command: XaiVideoGenerationCommandV1,
    source_command_sha256: String,
    request: GrokVideoGenerationRequestV1,
}

impl GrokVideoGenerationPayloadV1 {
    pub fn from_xai_command(
        source_command: XaiVideoGenerationCommandV1,
        staged_images: Vec<StagedImageV1>,
    ) -> Result<Self, GrokCommandError> {
        let request = project_xai_video_generation(source_command.clone(), staged_images.clone())?
            .into_provider_request();
        let source_command_sha256 = source_command.canonical_sha256_hex();
        let source_command = redact_video_input_urls(source_command, &staged_images)?;
        Ok(Self {
            source_command,
            source_command_sha256,
            request,
        })
    }

    pub fn source_command(&self) -> &XaiVideoGenerationCommandV1 {
        &self.source_command
    }

    pub fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }

    pub fn request(&self) -> &GrokVideoGenerationRequestV1 {
        &self.request
    }

    pub fn into_request(self) -> GrokVideoGenerationRequestV1 {
        self.request
    }
}

impl CanonicalCommandPayload for GrokVideoGenerationPayloadV1 {
    const SCHEMA_ID: &'static str = GROK_VIDEO_GENERATION_COMMAND_SCHEMA;
    const ADAPTER_REVISION: &'static str = ADAPTER_REVISION;

    fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }

    fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
        serde_json::to_vec(&CanonicalVideoGenerationV1::from(self))
            .expect("Grok video generation command serialization cannot fail")
    }
}

pub fn parse_image_generation_payload(
    input: &[u8],
) -> Result<GrokImageGenerationPayloadV1, GrokCommandError> {
    validate_command_size(input)?;
    let canonical: CanonicalImageGenerationV1 =
        serde_json::from_slice(input).map_err(|_| GrokCommandError::InvalidCanonicalCommand)?;
    let source_command_sha256 = validate_source_sha256(canonical.source_command_sha256.clone())?;
    let source_command = canonical.source_command.clone();
    let request = canonical.try_into()?;
    let payload = GrokImageGenerationPayloadV1::from_xai_command(source_command)?;
    if payload.source_command_sha256 != source_command_sha256 || payload.request != request {
        return Err(GrokCommandError::InvalidCanonicalCommand);
    }
    Ok(payload)
}

pub fn parse_image_generation_command(
    input: &[u8],
) -> Result<GrokImageGenerationRequestV1, GrokCommandError> {
    parse_image_generation_payload(input).map(GrokImageGenerationPayloadV1::into_request)
}

pub fn parse_image_edit_payload(input: &[u8]) -> Result<GrokImageEditPayloadV1, GrokCommandError> {
    validate_command_size(input)?;
    let canonical: CanonicalImageEditV1 =
        serde_json::from_slice(input).map_err(|_| GrokCommandError::InvalidCanonicalCommand)?;
    let source_command_sha256 = validate_source_sha256(canonical.source_command_sha256.clone())?;
    GrokImageEditPayloadV1::new(source_command_sha256, canonical.try_into()?)
}

pub fn parse_image_edit_command(input: &[u8]) -> Result<GrokImageEditRequestV1, GrokCommandError> {
    parse_image_edit_payload(input).map(GrokImageEditPayloadV1::into_request)
}

pub fn parse_video_generation_payload(
    input: &[u8],
) -> Result<GrokVideoGenerationPayloadV1, GrokCommandError> {
    validate_command_size(input)?;
    let canonical: CanonicalVideoGenerationV1 =
        serde_json::from_slice(input).map_err(|_| GrokCommandError::InvalidCanonicalCommand)?;
    let source_command_sha256 =
        validate_source_sha256(canonical.source_command_sha256().to_owned())?;
    let source_command = canonical.source_command().clone();
    let staged_images = canonical.staged_images()?;
    let request = canonical.try_into()?;
    validate_redacted_video_inputs(&source_command, &staged_images)?;
    let projected_request = project_xai_video_generation(source_command.clone(), staged_images)?
        .into_provider_request();
    if projected_request != request {
        return Err(GrokCommandError::InvalidCanonicalCommand);
    }
    Ok(GrokVideoGenerationPayloadV1 {
        source_command,
        source_command_sha256,
        request,
    })
}

pub fn parse_video_generation_command(
    input: &[u8],
) -> Result<GrokVideoGenerationRequestV1, GrokCommandError> {
    parse_video_generation_payload(input).map(GrokVideoGenerationPayloadV1::into_request)
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum GrokCommandError {
    #[error("source command binding is invalid")]
    InvalidSourceCommand,
    #[error("Grok canonical command is invalid")]
    InvalidCanonicalCommand,
    #[error("Grok canonical command has an unsupported schema version")]
    UnsupportedSchemaVersion,
    #[error("Grok canonical command contains an option unsupported by the CLI binding")]
    UnsupportedCliOption,
    #[error(transparent)]
    SourceProjection(#[from] XaiGrokProjectionError),
    #[error(transparent)]
    VideoSourceProjection(#[from] XaiGrokVideoProjectionError),
    #[error(transparent)]
    InvalidRequest(#[from] RequestValidationError),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalImageGenerationV1 {
    schema_version: u16,
    source_command: XaiImageGenerationCommandV1,
    source_command_sha256: String,
    model: String,
    prompt: String,
    n: u8,
    aspect_ratio: String,
    resolution: String,
}

impl From<GrokImageGenerationPayloadV1> for CanonicalImageGenerationV1 {
    fn from(payload: GrokImageGenerationPayloadV1) -> Self {
        let GrokImageGenerationPayloadV1 {
            source_command,
            source_command_sha256,
            request,
        } = payload;
        Self {
            schema_version: REQUEST_SCHEMA_VERSION,
            source_command,
            source_command_sha256,
            model: request.model().as_str().to_owned(),
            prompt: request.prompt().to_owned(),
            n: 1,
            aspect_ratio: request.aspect_ratio().as_str().to_owned(),
            resolution: "1k".to_owned(),
        }
    }
}

impl TryFrom<CanonicalImageGenerationV1> for GrokImageGenerationRequestV1 {
    type Error = GrokCommandError;

    fn try_from(value: CanonicalImageGenerationV1) -> Result<Self, Self::Error> {
        validate_envelope(value.schema_version, value.n, &value.resolution)?;
        Ok(Self::new(
            value.prompt,
            parse_image_model(&value.model)?,
            parse_image_ratio(&value.aspect_ratio)?,
        )?)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalImageEditV1 {
    schema_version: u16,
    source_command_sha256: String,
    model: String,
    prompt: String,
    images: Vec<CanonicalStagedImageV1>,
    n: u8,
    aspect_ratio: String,
    resolution: String,
}

impl From<GrokImageEditPayloadV1> for CanonicalImageEditV1 {
    fn from(payload: GrokImageEditPayloadV1) -> Self {
        let GrokImageEditPayloadV1 {
            source_command_sha256,
            request,
        } = payload;
        Self {
            schema_version: REQUEST_SCHEMA_VERSION,
            source_command_sha256,
            model: ImageModel::Quality.as_str().to_owned(),
            prompt: request.prompt().to_owned(),
            images: request.images().iter().map(Into::into).collect(),
            n: 1,
            aspect_ratio: request.aspect_ratio().as_str().to_owned(),
            resolution: "1k".to_owned(),
        }
    }
}

impl TryFrom<CanonicalImageEditV1> for GrokImageEditRequestV1 {
    type Error = GrokCommandError;

    fn try_from(value: CanonicalImageEditV1) -> Result<Self, Self::Error> {
        validate_envelope(value.schema_version, value.n, &value.resolution)?;
        if value.model != ImageModel::Quality.as_str() {
            return Err(GrokCommandError::UnsupportedCliOption);
        }
        Ok(Self::new(
            value.prompt,
            parse_staged_images(value.images)?,
            parse_image_ratio(&value.aspect_ratio)?,
        )?)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "workflow", deny_unknown_fields)]
enum CanonicalVideoGenerationV1 {
    #[serde(rename = "image_to_video")]
    ImageToVideo {
        schema_version: u16,
        source_command: XaiVideoGenerationCommandV1,
        source_command_sha256: String,
        model: String,
        prompt: Option<String>,
        image: CanonicalStagedImageV1,
        duration: u8,
        resolution: String,
    },
    #[serde(rename = "reference_to_video")]
    ReferenceToVideo {
        schema_version: u16,
        source_command: XaiVideoGenerationCommandV1,
        source_command_sha256: String,
        model: String,
        prompt: String,
        images: Vec<CanonicalStagedImageV1>,
        aspect_ratio: String,
        duration: u8,
        resolution: String,
    },
}

impl From<GrokVideoGenerationPayloadV1> for CanonicalVideoGenerationV1 {
    fn from(payload: GrokVideoGenerationPayloadV1) -> Self {
        let GrokVideoGenerationPayloadV1 {
            source_command,
            source_command_sha256,
            request,
        } = payload;
        match request {
            GrokVideoGenerationRequestV1::ImageToVideo(request) => Self::ImageToVideo {
                schema_version: REQUEST_SCHEMA_VERSION,
                source_command,
                source_command_sha256,
                model: "grok-imagine-video-1.5-preview".to_owned(),
                prompt: request.prompt().map(str::to_owned),
                image: request.image().into(),
                duration: request.duration().seconds(),
                resolution: request.resolution().as_str().to_owned(),
            },
            GrokVideoGenerationRequestV1::ReferenceToVideo(request) => Self::ReferenceToVideo {
                schema_version: REQUEST_SCHEMA_VERSION,
                source_command,
                source_command_sha256,
                model: "grok-imagine-video".to_owned(),
                prompt: request.prompt().to_owned(),
                images: request.images().iter().map(Into::into).collect(),
                aspect_ratio: request.aspect_ratio().as_str().to_owned(),
                duration: request.duration().seconds(),
                resolution: request.resolution().as_str().to_owned(),
            },
        }
    }
}

impl TryFrom<CanonicalVideoGenerationV1> for GrokVideoGenerationRequestV1 {
    type Error = GrokCommandError;

    fn try_from(value: CanonicalVideoGenerationV1) -> Result<Self, Self::Error> {
        match value {
            CanonicalVideoGenerationV1::ImageToVideo {
                schema_version,
                source_command: _,
                source_command_sha256: _,
                model,
                prompt,
                image,
                duration,
                resolution,
            } => {
                validate_schema(schema_version)?;
                if model != "grok-imagine-video-1.5-preview" {
                    return Err(GrokCommandError::UnsupportedCliOption);
                }
                Ok(ImageToVideoRequestV1::new(
                    prompt,
                    image.try_into()?,
                    parse_video_duration(duration)?,
                    parse_video_resolution(&resolution)?,
                )?
                .into())
            }
            CanonicalVideoGenerationV1::ReferenceToVideo {
                schema_version,
                source_command: _,
                source_command_sha256: _,
                model,
                prompt,
                images,
                aspect_ratio,
                duration,
                resolution,
            } => {
                validate_schema(schema_version)?;
                if model != "grok-imagine-video" {
                    return Err(GrokCommandError::UnsupportedCliOption);
                }
                Ok(ReferenceToVideoRequestV1::new(
                    prompt,
                    parse_staged_images(images)?,
                    parse_video_ratio(&aspect_ratio)?,
                    parse_video_duration(duration)?,
                    parse_video_resolution(&resolution)?,
                )?
                .into())
            }
        }
    }
}

impl CanonicalVideoGenerationV1 {
    fn source_command(&self) -> &XaiVideoGenerationCommandV1 {
        match self {
            Self::ImageToVideo { source_command, .. }
            | Self::ReferenceToVideo { source_command, .. } => source_command,
        }
    }

    fn source_command_sha256(&self) -> &str {
        match self {
            Self::ImageToVideo {
                source_command_sha256,
                ..
            }
            | Self::ReferenceToVideo {
                source_command_sha256,
                ..
            } => source_command_sha256,
        }
    }

    fn staged_images(&self) -> Result<Vec<StagedImageV1>, GrokCommandError> {
        match self {
            Self::ImageToVideo { image, .. } => Ok(vec![StagedImageV1::try_from(image.clone())?]),
            Self::ReferenceToVideo { images, .. } => images
                .iter()
                .cloned()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(Into::into),
        }
    }
}

fn redact_video_input_urls(
    mut command: XaiVideoGenerationCommandV1,
    staged_images: &[StagedImageV1],
) -> Result<XaiVideoGenerationCommandV1, GrokCommandError> {
    let references: Vec<_> = command
        .image
        .iter_mut()
        .chain(command.reference_images.iter_mut())
        .collect();
    if references.len() != staged_images.len() {
        return Err(GrokCommandError::InvalidCanonicalCommand);
    }
    for (reference, staged) in references.into_iter().zip(staged_images) {
        reference.file_id = None;
        reference.url = Some(format!("{STAGED_INPUT_URL_PREFIX}{}", staged.sha256()));
    }
    Ok(command)
}

fn validate_redacted_video_inputs(
    command: &XaiVideoGenerationCommandV1,
    staged_images: &[StagedImageV1],
) -> Result<(), GrokCommandError> {
    let references: Vec<_> = command
        .image
        .iter()
        .chain(command.reference_images.iter())
        .collect();
    let valid = references.len() == staged_images.len()
        && references
            .iter()
            .zip(staged_images)
            .all(|(reference, staged)| {
                let expected = format!("{STAGED_INPUT_URL_PREFIX}{}", staged.sha256());
                reference.file_id.is_none() && reference.url.as_deref() == Some(expected.as_str())
            });
    if valid {
        Ok(())
    } else {
        Err(GrokCommandError::InvalidCanonicalCommand)
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalStagedImageV1 {
    filename: String,
    sha256: String,
}

impl From<&StagedImageV1> for CanonicalStagedImageV1 {
    fn from(image: &StagedImageV1) -> Self {
        Self {
            filename: image.filename().to_owned(),
            sha256: image.sha256().to_owned(),
        }
    }
}

impl TryFrom<CanonicalStagedImageV1> for StagedImageV1 {
    type Error = RequestValidationError;

    fn try_from(image: CanonicalStagedImageV1) -> Result<Self, Self::Error> {
        Self::new(image.filename, image.sha256)
    }
}

fn parse_staged_images(
    images: Vec<CanonicalStagedImageV1>,
) -> Result<Vec<StagedImageV1>, RequestValidationError> {
    images.into_iter().map(TryInto::try_into).collect()
}

fn validate_source_sha256(value: String) -> Result<String, GrokCommandError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(GrokCommandError::InvalidSourceCommand);
    }
    Ok(value)
}

fn validate_command_size(input: &[u8]) -> Result<(), GrokCommandError> {
    if input.is_empty() || input.len() > MAX_CANONICAL_COMMAND_BYTES {
        return Err(GrokCommandError::InvalidCanonicalCommand);
    }
    Ok(())
}

fn validate_schema(schema_version: u16) -> Result<(), GrokCommandError> {
    if schema_version != REQUEST_SCHEMA_VERSION {
        return Err(GrokCommandError::UnsupportedSchemaVersion);
    }
    Ok(())
}

fn validate_envelope(schema_version: u16, n: u8, resolution: &str) -> Result<(), GrokCommandError> {
    validate_schema(schema_version)?;
    if n != 1 || resolution != "1k" {
        return Err(GrokCommandError::UnsupportedCliOption);
    }
    Ok(())
}

fn parse_image_model(value: &str) -> Result<ImageModel, GrokCommandError> {
    match value {
        "grok-imagine-image" => Ok(ImageModel::Base),
        "grok-imagine-image-quality" => Ok(ImageModel::Quality),
        _ => Err(GrokCommandError::UnsupportedCliOption),
    }
}

fn parse_image_ratio(value: &str) -> Result<ImageAspectRatio, GrokCommandError> {
    match value {
        "auto" => Ok(ImageAspectRatio::Auto),
        "1:1" => Ok(ImageAspectRatio::R1x1),
        "16:9" => Ok(ImageAspectRatio::R16x9),
        "9:16" => Ok(ImageAspectRatio::R9x16),
        "4:3" => Ok(ImageAspectRatio::R4x3),
        "3:4" => Ok(ImageAspectRatio::R3x4),
        "3:2" => Ok(ImageAspectRatio::R3x2),
        "2:3" => Ok(ImageAspectRatio::R2x3),
        "2:1" => Ok(ImageAspectRatio::R2x1),
        "1:2" => Ok(ImageAspectRatio::R1x2),
        "19.5:9" => Ok(ImageAspectRatio::R19_5x9),
        "9:19.5" => Ok(ImageAspectRatio::R9x19_5),
        "20:9" => Ok(ImageAspectRatio::R20x9),
        "9:20" => Ok(ImageAspectRatio::R9x20),
        _ => Err(GrokCommandError::UnsupportedCliOption),
    }
}

fn parse_video_ratio(value: &str) -> Result<VideoAspectRatio, GrokCommandError> {
    match value {
        "1:1" => Ok(VideoAspectRatio::R1x1),
        "16:9" => Ok(VideoAspectRatio::R16x9),
        "9:16" => Ok(VideoAspectRatio::R9x16),
        "3:2" => Ok(VideoAspectRatio::R3x2),
        "2:3" => Ok(VideoAspectRatio::R2x3),
        _ => Err(GrokCommandError::UnsupportedCliOption),
    }
}

fn parse_video_duration(value: u8) -> Result<VideoDuration, GrokCommandError> {
    match value {
        6 => Ok(VideoDuration::Seconds6),
        10 => Ok(VideoDuration::Seconds10),
        _ => Err(GrokCommandError::UnsupportedCliOption),
    }
}

fn parse_video_resolution(value: &str) -> Result<VideoResolution, GrokCommandError> {
    match value {
        "480p" => Ok(VideoResolution::P480),
        "720p" => Ok(VideoResolution::P720),
        _ => Err(GrokCommandError::UnsupportedCliOption),
    }
}
