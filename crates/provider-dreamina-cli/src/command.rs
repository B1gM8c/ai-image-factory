use image_provider_sdk::{CanonicalCommandPayload, OutputSlot};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ADAPTER_REVISION, DreaminaSubmitRequestV1, ImageModelVersion, ImageRatio, ImageResolution,
    RequestValidationError, TextToImageRequestV1, TextToVideoRequestV1, VideoModelVersion,
    VideoRatio, VideoResolution,
};

pub const DREAMINA_SUBMIT_COMMAND_SCHEMA: &str = "dreamina-cli.submit.v1";
pub const MAX_SUBMIT_COMMAND_BYTES: usize = 1024 * 1024;

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
    if input.is_empty() || input.len() > MAX_SUBMIT_COMMAND_BYTES {
        return Err(DreaminaSubmitCommandError::InvalidCanonicalCommand);
    }
    let canonical: CanonicalSubmitV1 = serde_json::from_slice(input)
        .map_err(|_| DreaminaSubmitCommandError::InvalidCanonicalCommand)?;
    canonical.try_into()
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
}

impl From<DreaminaSubmitRequestV1> for CanonicalSubmitV1 {
    fn from(request: DreaminaSubmitRequestV1) -> Self {
        match request {
            DreaminaSubmitRequestV1::TextToImage(request) => Self::TextToImage {
                schema_version: crate::REQUEST_SCHEMA_VERSION,
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
                validate_envelope(schema_version, poll)?;
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
        }
    }
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
        "2k" => Ok(ImageResolution::K2),
        "4k" => Ok(ImageResolution::K4),
        _ => Err(DreaminaSubmitCommandError::UnknownOfficialOption),
    }
}

fn parse_video_model(value: &str) -> Result<VideoModelVersion, DreaminaSubmitCommandError> {
    match value {
        "seedance2.0" => Ok(VideoModelVersion::Seedance2_0),
        "seedance2.0fast" => Ok(VideoModelVersion::Seedance2_0Fast),
        "seedance2.0_vip" => Ok(VideoModelVersion::Seedance2_0Vip),
        "seedance2.0fast_vip" => Ok(VideoModelVersion::Seedance2_0FastVip),
        "seedance2.0mini" => Ok(VideoModelVersion::Seedance2_0Mini),
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
    match request {
        DreaminaSubmitRequestV1::TextToImage(request) if request.generate_num() != 1 => {
            Err(DreaminaSubmitCommandError::BatchSubmissionUnsupported)
        }
        DreaminaSubmitRequestV1::TextToImage(_) | DreaminaSubmitRequestV1::TextToVideo(_) => Ok(()),
    }
}
