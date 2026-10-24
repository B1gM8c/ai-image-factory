use image_provider_sdk::{CanonicalCommandPayload, OutputSlot};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ADAPTER_REVISION, DreaminaSubmitRequestV1, FramesToVideoRequestV1, ImageModelVersion,
    ImageRatio, ImageResolution, ImageToImageRequestV1, ImageToVideoRequestV1,
    ImageUpscaleRequestV1, MultiframeToVideoRequestV1, MultimodalToVideoRequestV1,
    ReferenceVideoOptionsV1, RequestValidationError, StagedImageV1, StagedTimedMediaV1,
    TextToImageRequestV1, TextToVideoRequestV1, UpscaleResolution, VideoModelVersion, VideoRatio,
    VideoResolution, VideoTransitionV1,
};

pub const DREAMINA_SUBMIT_COMMAND_SCHEMA: &str = "dreamina-cli.submit.v1";
pub const MAX_SUBMIT_COMMAND_BYTES: usize = 1024 * 1024;
pub const NATIVE_IMAGE_BATCH_SCHEMA_VERSION: u16 = 2;

/// An explicit native batch parsed from a version 2 canonical command.
/// Version 1 image commands retain their historical per-output execution path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaNativeImageBatchV2 {
    request: DreaminaSubmitRequestV1,
}

impl DreaminaNativeImageBatchV2 {
    pub fn request(&self) -> &DreaminaSubmitRequestV1 {
        &self.request
    }

    pub(crate) fn argv(&self) -> Vec<std::ffi::OsString> {
        match &self.request {
            DreaminaSubmitRequestV1::TextToImage(request) => request.to_argv(),
            DreaminaSubmitRequestV1::ImageToImage(request) => request.to_argv(),
            _ => unreachable!("native image batch parsing only permits image generation"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaSubmitPayloadV1 {
    source_command_sha256: String,
    request: DreaminaSubmitRequestV1,
}

impl DreaminaSubmitPayloadV1 {
    pub fn new(
        source_command_sha256: impl Into<String>,
        request: DreaminaSubmitRequestV1,
    ) -> Result<Self, DreaminaSubmitCommandError> {
        let source_command_sha256 = source_command_sha256.into();
        if !valid_sha256(&source_command_sha256) {
            return Err(DreaminaSubmitCommandError::InvalidSourceCommand);
        }
        ensure_single_output(&request)?;
        Ok(Self {
            source_command_sha256,
            request,
        })
    }

    pub fn request(&self) -> &DreaminaSubmitRequestV1 {
        &self.request
    }

    pub fn new_native_image_batch(
        source_command_sha256: impl Into<String>,
        batch: DreaminaNativeImageBatchV2,
    ) -> Result<Self, DreaminaSubmitCommandError> {
        let source_command_sha256 = source_command_sha256.into();
        if !valid_sha256(&source_command_sha256) {
            return Err(DreaminaSubmitCommandError::InvalidSourceCommand);
        }
        Ok(Self {
            source_command_sha256,
            request: batch.request,
        })
    }
}

impl CanonicalCommandPayload for DreaminaSubmitPayloadV1 {
    const SCHEMA_ID: &'static str = DREAMINA_SUBMIT_COMMAND_SCHEMA;
    const ADAPTER_REVISION: &'static str = ADAPTER_REVISION;

    fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }

    fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
        serde_json::to_vec(&CanonicalSubmitV1::from(self.request))
            .expect("Dreamina canonical submit serialization cannot fail")
    }
}

pub fn parse_submit_command(
    input: &[u8],
) -> Result<DreaminaSubmitRequestV1, DreaminaSubmitCommandError> {
    parse_canonical_command(input)?.try_into()
}

pub fn is_native_image_batch_command(input: &[u8]) -> Result<bool, DreaminaSubmitCommandError> {
    let canonical = parse_canonical_command(input)?;
    let native_batch = canonical.schema_version() == NATIVE_IMAGE_BATCH_SCHEMA_VERSION;
    DreaminaSubmitRequestV1::try_from(canonical)?;
    Ok(native_batch)
}

pub fn parse_native_image_batch_command(
    input: &[u8],
) -> Result<DreaminaNativeImageBatchV2, DreaminaSubmitCommandError> {
    let canonical = parse_canonical_command(input)?;
    if canonical.schema_version() != NATIVE_IMAGE_BATCH_SCHEMA_VERSION {
        return Err(DreaminaSubmitCommandError::NativeImageBatchRequired);
    }
    Ok(DreaminaNativeImageBatchV2 {
        request: canonical.try_into()?,
    })
}

fn parse_canonical_command(input: &[u8]) -> Result<CanonicalSubmitV1, DreaminaSubmitCommandError> {
    if input.is_empty() || input.len() > MAX_SUBMIT_COMMAND_BYTES {
        return Err(DreaminaSubmitCommandError::InvalidCanonicalCommand);
    }
    serde_json::from_slice(input).map_err(|_| DreaminaSubmitCommandError::InvalidCanonicalCommand)
}

pub fn encode_submit_command(request: DreaminaSubmitRequestV1) -> Vec<u8> {
    serde_json::to_vec(&CanonicalSubmitV1::from(request))
        .expect("Dreamina canonical submit serialization cannot fail")
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum DreaminaSubmitCommandError {
    #[error("source command SHA-256 is invalid")]
    InvalidSourceCommand,
    #[error("Dreamina canonical submit command is invalid")]
    InvalidCanonicalCommand,
    #[error("Dreamina canonical submit command has an unsupported schema version")]
    UnsupportedSchemaVersion,
    #[error("Dreamina canonical submit command must disable CLI polling")]
    PollingMustBeDisabled,
    #[error("Dreamina canonical submit command contains an unknown official option")]
    UnknownOfficialOption,
    #[error(transparent)]
    InvalidRequest(#[from] RequestValidationError),
    #[error("Dreamina execution supports exactly one output per provider submission")]
    BatchSubmissionUnsupported,
    #[error("Dreamina native batch requires a version 2 image command with 2-10 outputs")]
    NativeImageBatchRequired,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "operation", deny_unknown_fields)]
enum CanonicalSubmitV1 {
    #[serde(rename = "text2image")]
    TextToImage {
        schema_version: u16,
        prompt: String,
        model_version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ratio: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        width: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        height: Option<u32>,
        resolution_type: String,
        generate_num: u8,
        poll: u8,
    },
    #[serde(rename = "text2video")]
    TextToVideo {
        schema_version: u16,
        prompt: String,
        model_version: String,
        ratio: String,
        duration: u8,
        video_resolution: String,
        poll: u8,
    },
    #[serde(rename = "image2image")]
    ImageToImage {
        schema_version: u16,
        images: Vec<CanonicalImageV1>,
        prompt: String,
        model_version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ratio: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        width: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        height: Option<u32>,
        resolution_type: String,
        generate_num: u8,
        poll: u8,
    },
    #[serde(rename = "image_upscale")]
    ImageUpscale {
        schema_version: u16,
        image: CanonicalImageV1,
        resolution_type: String,
        poll: u8,
    },
    #[serde(rename = "image2video")]
    ImageToVideo {
        schema_version: u16,
        image: CanonicalImageV1,
        prompt: String,
        model_version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ratio: Option<String>,
        duration: u8,
        video_resolution: String,
        poll: u8,
    },
    #[serde(rename = "frames2video")]
    FramesToVideo {
        schema_version: u16,
        first: CanonicalImageV1,
        last: CanonicalImageV1,
        prompt: String,
        model_version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ratio: Option<String>,
        duration: u8,
        video_resolution: String,
        poll: u8,
    },
    #[serde(rename = "multiframe2video")]
    MultiframeToVideo {
        schema_version: u16,
        images: Vec<CanonicalImageV1>,
        transitions: Vec<CanonicalTransitionV1>,
        video_resolution: String,
        poll: u8,
    },
    #[serde(rename = "multimodal2video")]
    MultimodalToVideo {
        schema_version: u16,
        images: Vec<CanonicalImageV1>,
        videos: Vec<CanonicalTimedMediaV1>,
        audios: Vec<CanonicalTimedMediaV1>,
        prompt: String,
        model_version: String,
        ratio: String,
        duration: u8,
        video_resolution: String,
        poll: u8,
    },
}

impl CanonicalSubmitV1 {
    fn schema_version(&self) -> u16 {
        match self {
            Self::TextToImage { schema_version, .. }
            | Self::TextToVideo { schema_version, .. }
            | Self::ImageToImage { schema_version, .. }
            | Self::ImageUpscale { schema_version, .. }
            | Self::ImageToVideo { schema_version, .. }
            | Self::FramesToVideo { schema_version, .. }
            | Self::MultiframeToVideo { schema_version, .. }
            | Self::MultimodalToVideo { schema_version, .. } => *schema_version,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalImageV1 {
    filename: String,
    sha256: String,
    width: u32,
    height: u32,
}
impl From<&StagedImageV1> for CanonicalImageV1 {
    fn from(image: &StagedImageV1) -> Self {
        Self {
            filename: image.filename().to_owned(),
            sha256: image.sha256().to_owned(),
            width: image.width(),
            height: image.height(),
        }
    }
}
impl TryFrom<CanonicalImageV1> for StagedImageV1 {
    type Error = RequestValidationError;
    fn try_from(value: CanonicalImageV1) -> Result<Self, Self::Error> {
        Self::new(value.filename, value.sha256, value.width, value.height)
    }
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalTimedMediaV1 {
    filename: String,
    sha256: String,
    duration_ms: u64,
}
impl From<&StagedTimedMediaV1> for CanonicalTimedMediaV1 {
    fn from(media: &StagedTimedMediaV1) -> Self {
        Self {
            filename: media.filename().to_owned(),
            sha256: media.sha256().to_owned(),
            duration_ms: media.duration_ms(),
        }
    }
}
impl TryFrom<CanonicalTimedMediaV1> for StagedTimedMediaV1 {
    type Error = RequestValidationError;
    fn try_from(value: CanonicalTimedMediaV1) -> Result<Self, Self::Error> {
        Self::new(value.filename, value.sha256, value.duration_ms)
    }
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalTransitionV1 {
    prompt: String,
    duration_ms: u32,
}

impl From<DreaminaSubmitRequestV1> for CanonicalSubmitV1 {
    fn from(request: DreaminaSubmitRequestV1) -> Self {
        match request {
            DreaminaSubmitRequestV1::TextToImage(request) => Self::TextToImage {
                schema_version: image_schema_version(request.generate_num()),
                prompt: request.prompt().to_owned(),
                model_version: request.model().as_str().to_owned(),
                ratio: request.ratio().map(|ratio| ratio.as_str().to_owned()),
                width: request.width(),
                height: request.height(),
                resolution_type: request.resolution().as_str().to_owned(),
                generate_num: request.generate_num(),
                poll: 0,
            },
            DreaminaSubmitRequestV1::TextToVideo(request) => Self::TextToVideo {
                schema_version: crate::REQUEST_SCHEMA_VERSION,
                prompt: request.prompt().to_owned(),
                model_version: request.model().as_str().to_owned(),
                ratio: request.ratio().as_str().to_owned(),
                duration: request.duration_seconds(),
                video_resolution: request.resolution().as_str().to_owned(),
                poll: 0,
            },
            DreaminaSubmitRequestV1::ImageToImage(request) => Self::ImageToImage {
                schema_version: image_schema_version(request.generate_num()),
                images: request.images().iter().map(Into::into).collect(),
                prompt: request.parameters().prompt().to_owned(),
                model_version: request.parameters().model().as_str().to_owned(),
                ratio: request.parameters().ratio().map(|r| r.as_str().to_owned()),
                width: request.parameters().width(),
                height: request.parameters().height(),
                resolution_type: request.parameters().resolution().as_str().to_owned(),
                generate_num: request.generate_num(),
                poll: 0,
            },
            DreaminaSubmitRequestV1::ImageUpscale(request) => Self::ImageUpscale {
                schema_version: crate::REQUEST_SCHEMA_VERSION,
                image: request.image().into(),
                resolution_type: request.resolution().as_str().to_owned(),
                poll: 0,
            },
            DreaminaSubmitRequestV1::ImageToVideo(request) => Self::ImageToVideo {
                schema_version: crate::REQUEST_SCHEMA_VERSION,
                image: request.image().into(),
                prompt: request.options().prompt().to_owned(),
                model_version: request.options().model().as_str().to_owned(),
                ratio: request.options().ratio().map(|r| r.as_str().to_owned()),
                duration: request.options().duration_seconds(),
                video_resolution: request.options().resolution().as_str().to_owned(),
                poll: 0,
            },
            DreaminaSubmitRequestV1::FramesToVideo(request) => Self::FramesToVideo {
                schema_version: crate::REQUEST_SCHEMA_VERSION,
                first: request.first().into(),
                last: request.last().into(),
                prompt: request.options().prompt().to_owned(),
                model_version: request.options().model().as_str().to_owned(),
                ratio: request.options().ratio().map(|r| r.as_str().to_owned()),
                duration: request.options().duration_seconds(),
                video_resolution: request.options().resolution().as_str().to_owned(),
                poll: 0,
            },
            DreaminaSubmitRequestV1::MultiframeToVideo(request) => Self::MultiframeToVideo {
                schema_version: crate::REQUEST_SCHEMA_VERSION,
                images: request.images().iter().map(Into::into).collect(),
                transitions: request
                    .transitions()
                    .iter()
                    .map(|t| CanonicalTransitionV1 {
                        prompt: t.prompt().to_owned(),
                        duration_ms: t.duration_ms(),
                    })
                    .collect(),
                video_resolution: request.resolution().as_str().to_owned(),
                poll: 0,
            },
            DreaminaSubmitRequestV1::MultimodalToVideo(request) => Self::MultimodalToVideo {
                schema_version: crate::REQUEST_SCHEMA_VERSION,
                images: request.images().iter().map(Into::into).collect(),
                videos: request.videos().iter().map(Into::into).collect(),
                audios: request.audios().iter().map(Into::into).collect(),
                prompt: request.prompt().to_owned(),
                model_version: request.model().as_str().to_owned(),
                ratio: request.ratio().as_str().to_owned(),
                duration: request.duration_seconds(),
                video_resolution: request.resolution().as_str().to_owned(),
                poll: 0,
            },
        }
    }
}

impl TryFrom<CanonicalSubmitV1> for DreaminaSubmitRequestV1 {
    type Error = DreaminaSubmitCommandError;

    fn try_from(value: CanonicalSubmitV1) -> Result<Self, Self::Error> {
        match value {
            CanonicalSubmitV1::TextToImage {
                schema_version,
                prompt,
                model_version,
                ratio,
                width,
                height,
                resolution_type,
                generate_num,
                poll,
            } => {
                validate_image_envelope(schema_version, poll, generate_num)?;
                let model = parse_image_model(&model_version)?;
                let resolution = parse_image_resolution(&resolution_type)?;
                match (ratio, width, height) {
                    (Some(ratio), None, None) => TextToImageRequestV1::new(
                        prompt,
                        model,
                        parse_image_ratio(&ratio)?,
                        resolution,
                        generate_num,
                    ),
                    (None, Some(width), Some(height)) => TextToImageRequestV1::new_custom(
                        prompt,
                        model,
                        width,
                        height,
                        resolution,
                        generate_num,
                    ),
                    _ => return Err(DreaminaSubmitCommandError::UnknownOfficialOption),
                }
                .map(Into::into)
                .map_err(Into::into)
            }
            CanonicalSubmitV1::TextToVideo {
                schema_version,
                prompt,
                model_version,
                ratio,
                duration,
                video_resolution,
                poll,
            } => {
                validate_envelope(schema_version, poll)?;
                TextToVideoRequestV1::new(
                    prompt,
                    parse_video_model(&model_version)?,
                    parse_video_ratio(&ratio)?,
                    duration,
                    parse_video_resolution(&video_resolution)?,
                )
                .map(Into::into)
                .map_err(Into::into)
            }
            CanonicalSubmitV1::ImageToImage {
                schema_version,
                images,
                prompt,
                model_version,
                ratio,
                width,
                height,
                resolution_type,
                generate_num,
                poll,
            } => {
                validate_image_envelope(schema_version, poll, generate_num)?;
                let model = parse_image_model(&model_version)?;
                let resolution = parse_image_resolution(&resolution_type)?;
                let parameters = match (ratio, width, height) {
                    (Some(ratio), None, None) => TextToImageRequestV1::new(
                        prompt,
                        model,
                        parse_image_ratio(&ratio)?,
                        resolution,
                        generate_num,
                    ),
                    (None, Some(width), Some(height)) => TextToImageRequestV1::new_custom(
                        prompt,
                        model,
                        width,
                        height,
                        resolution,
                        generate_num,
                    ),
                    _ => return Err(DreaminaSubmitCommandError::UnknownOfficialOption),
                }?;
                Ok(ImageToImageRequestV1::new(parse_images(images)?, parameters)?.into())
            }
            CanonicalSubmitV1::ImageUpscale {
                schema_version,
                image,
                resolution_type,
                poll,
            } => {
                validate_envelope(schema_version, poll)?;
                let resolution = match resolution_type.as_str() {
                    "2k" => UpscaleResolution::K2,
                    "4k" => UpscaleResolution::K4,
                    "8k" => UpscaleResolution::K8,
                    _ => return Err(RequestValidationError::InvalidUpscaleResolution.into()),
                };
                Ok(ImageUpscaleRequestV1::new(image.try_into()?, resolution).into())
            }
            CanonicalSubmitV1::ImageToVideo {
                schema_version,
                image,
                prompt,
                model_version,
                ratio,
                duration,
                video_resolution,
                poll,
            } => {
                validate_envelope(schema_version, poll)?;
                let options = parse_reference_options(
                    prompt,
                    model_version,
                    ratio,
                    duration,
                    video_resolution,
                )?;
                Ok(ImageToVideoRequestV1::new(image.try_into()?, options)?.into())
            }
            CanonicalSubmitV1::FramesToVideo {
                schema_version,
                first,
                last,
                prompt,
                model_version,
                ratio,
                duration,
                video_resolution,
                poll,
            } => {
                validate_envelope(schema_version, poll)?;
                let options = parse_reference_options(
                    prompt,
                    model_version,
                    ratio,
                    duration,
                    video_resolution,
                )?;
                Ok(
                    FramesToVideoRequestV1::new(first.try_into()?, last.try_into()?, options)?
                        .into(),
                )
            }
            CanonicalSubmitV1::MultiframeToVideo {
                schema_version,
                images,
                transitions,
                video_resolution,
                poll,
            } => {
                validate_envelope(schema_version, poll)?;
                let transitions = transitions
                    .into_iter()
                    .map(|t| VideoTransitionV1::new(t.prompt, t.duration_ms))
                    .collect::<Result<_, _>>()?;
                Ok(MultiframeToVideoRequestV1::new(
                    parse_images(images)?,
                    transitions,
                    parse_video_resolution(&video_resolution)?,
                )?
                .into())
            }
            CanonicalSubmitV1::MultimodalToVideo {
                schema_version,
                images,
                videos,
                audios,
                prompt,
                model_version,
                ratio,
                duration,
                video_resolution,
                poll,
            } => {
                validate_envelope(schema_version, poll)?;
                Ok(MultimodalToVideoRequestV1::new(
                    parse_images(images)?,
                    videos
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<_, _>>()?,
                    audios
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<_, _>>()?,
                    prompt,
                    parse_video_model(&model_version)?,
                    parse_video_ratio(&ratio)?,
                    duration,
                    parse_video_resolution(&video_resolution)?,
                )?
                .into())
            }
        }
    }
}

fn parse_images(
    images: Vec<CanonicalImageV1>,
) -> Result<Vec<StagedImageV1>, RequestValidationError> {
    images.into_iter().map(TryInto::try_into).collect()
}
fn parse_reference_options(
    prompt: String,
    model: String,
    ratio: Option<String>,
    duration: u8,
    resolution: String,
) -> Result<ReferenceVideoOptionsV1, DreaminaSubmitCommandError> {
    Ok(ReferenceVideoOptionsV1::new(
        prompt,
        parse_video_model(&model)?,
        ratio.map(|r| parse_video_ratio(&r)).transpose()?,
        duration,
        parse_video_resolution(&resolution)?,
    )?)
}

fn validate_envelope(schema_version: u16, poll: u8) -> Result<(), DreaminaSubmitCommandError> {
    if schema_version != crate::REQUEST_SCHEMA_VERSION {
        return Err(DreaminaSubmitCommandError::UnsupportedSchemaVersion);
    }
    if poll != 0 {
        return Err(DreaminaSubmitCommandError::PollingMustBeDisabled);
    }
    Ok(())
}

fn image_schema_version(generate_num: u8) -> u16 {
    if generate_num > 1 {
        NATIVE_IMAGE_BATCH_SCHEMA_VERSION
    } else {
        crate::REQUEST_SCHEMA_VERSION
    }
}

fn validate_image_envelope(
    schema_version: u16,
    poll: u8,
    generate_num: u8,
) -> Result<(), DreaminaSubmitCommandError> {
    if schema_version == NATIVE_IMAGE_BATCH_SCHEMA_VERSION {
        if !(2..=10).contains(&generate_num) {
            return Err(DreaminaSubmitCommandError::NativeImageBatchRequired);
        }
        if poll != 0 {
            return Err(DreaminaSubmitCommandError::PollingMustBeDisabled);
        }
        Ok(())
    } else {
        validate_envelope(schema_version, poll)
    }
}

fn parse_image_model(value: &str) -> Result<ImageModelVersion, DreaminaSubmitCommandError> {
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
        _ => Err(DreaminaSubmitCommandError::UnknownOfficialOption),
    }
}

fn parse_image_ratio(value: &str) -> Result<ImageRatio, DreaminaSubmitCommandError> {
    match value {
        "21:9" => Ok(ImageRatio::R21x9),
        "16:9" => Ok(ImageRatio::R16x9),
        "3:2" => Ok(ImageRatio::R3x2),
        "4:3" => Ok(ImageRatio::R4x3),
        "1:1" => Ok(ImageRatio::R1x1),
        "3:4" => Ok(ImageRatio::R3x4),
        "2:3" => Ok(ImageRatio::R2x3),
        "9:16" => Ok(ImageRatio::R9x16),
        _ => Err(DreaminaSubmitCommandError::UnknownOfficialOption),
    }
}

fn parse_image_resolution(value: &str) -> Result<ImageResolution, DreaminaSubmitCommandError> {
    match value {
        "1k" => Ok(ImageResolution::K1),
        "1.5k" => Ok(ImageResolution::K1_5),
        "2k" => Ok(ImageResolution::K2),
        "4k" => Ok(ImageResolution::K4),
        _ => Err(DreaminaSubmitCommandError::UnknownOfficialOption),
    }
}

fn parse_video_model(value: &str) -> Result<VideoModelVersion, DreaminaSubmitCommandError> {
    match value {
        "seedance1.0fast" => Ok(VideoModelVersion::Seedance1_0Fast),
        "seedance1.5pro" => Ok(VideoModelVersion::Seedance1_5Pro),
        "seedance2.0" => Ok(VideoModelVersion::Seedance2_0),
        "seedance2.0fast" => Ok(VideoModelVersion::Seedance2_0Fast),
        "seedance2.0_vip" => Ok(VideoModelVersion::Seedance2_0Vip),
        "seedance2.0fast_vip" => Ok(VideoModelVersion::Seedance2_0FastVip),
        "seedance2.0mini" => Ok(VideoModelVersion::Seedance2_0Mini),
        "seedance2.5" => Ok(VideoModelVersion::Seedance2_5),
        _ => Err(DreaminaSubmitCommandError::UnknownOfficialOption),
    }
}

fn parse_video_ratio(value: &str) -> Result<VideoRatio, DreaminaSubmitCommandError> {
    match value {
        "1:1" => Ok(VideoRatio::R1x1),
        "3:4" => Ok(VideoRatio::R3x4),
        "16:9" => Ok(VideoRatio::R16x9),
        "4:3" => Ok(VideoRatio::R4x3),
        "9:16" => Ok(VideoRatio::R9x16),
        "21:9" => Ok(VideoRatio::R21x9),
        _ => Err(DreaminaSubmitCommandError::UnknownOfficialOption),
    }
}

fn parse_video_resolution(value: &str) -> Result<VideoResolution, DreaminaSubmitCommandError> {
    match value {
        "480p" => Ok(VideoResolution::P480),
        "720p" => Ok(VideoResolution::P720),
        "1080p" => Ok(VideoResolution::P1080),
        "4k" => Ok(VideoResolution::K4),
        _ => Err(DreaminaSubmitCommandError::UnknownOfficialOption),
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn ensure_single_output(
    request: &DreaminaSubmitRequestV1,
) -> Result<(), DreaminaSubmitCommandError> {
    if request.output_count() != 1 {
        Err(DreaminaSubmitCommandError::BatchSubmissionUnsupported)
    } else {
        Ok(())
    }
}
