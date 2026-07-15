use std::{
    ffi::OsString,
    os::unix::ffi::OsStrExt,
    path::{Component, PathBuf},
};

use image_provider_sdk::OpaqueProviderId;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageModelVersion {
    V3_0,
    V3_1,
    V4_0,
    V4_1,
    V4_5,
    V4_6,
    V4_7,
    V5_0,
}

impl ImageModelVersion {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V3_0 => "3.0",
            Self::V3_1 => "3.1",
            Self::V4_0 => "4.0",
            Self::V4_1 => "4.1",
            Self::V4_5 => "4.5",
            Self::V4_6 => "4.6",
            Self::V4_7 => "4.7",
            Self::V5_0 => "5.0",
        }
    }

    const fn supports(self, resolution: ImageResolution) -> bool {
        match self {
            Self::V3_0 | Self::V3_1 => {
                matches!(resolution, ImageResolution::K1 | ImageResolution::K2)
            }
            Self::V4_0 | Self::V4_1 | Self::V4_5 | Self::V4_6 | Self::V4_7 | Self::V5_0 => {
                matches!(resolution, ImageResolution::K2 | ImageResolution::K4)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageRatio {
    R21x9,
    R16x9,
    R3x2,
    R4x3,
    R1x1,
    R3x4,
    R2x3,
    R9x16,
}

impl ImageRatio {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::R21x9 => "21:9",
            Self::R16x9 => "16:9",
            Self::R3x2 => "3:2",
            Self::R4x3 => "4:3",
            Self::R1x1 => "1:1",
            Self::R3x4 => "3:4",
            Self::R2x3 => "2:3",
            Self::R9x16 => "9:16",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageResolution {
    K1,
    K2,
    K4,
}

impl ImageResolution {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::K1 => "1k",
            Self::K2 => "2k",
            Self::K4 => "4k",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoModelVersion {
    Seedance2_0,
    Seedance2_0Fast,
    Seedance2_0Vip,
    Seedance2_0FastVip,
    Seedance2_0Mini,
}

impl VideoModelVersion {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Seedance2_0 => "seedance2.0",
            Self::Seedance2_0Fast => "seedance2.0fast",
            Self::Seedance2_0Vip => "seedance2.0_vip",
            Self::Seedance2_0FastVip => "seedance2.0fast_vip",
            Self::Seedance2_0Mini => "seedance2.0mini",
        }
    }

    const fn supports(self, resolution: VideoResolution) -> bool {
        match self {
            Self::Seedance2_0Vip => true,
            Self::Seedance2_0
            | Self::Seedance2_0Fast
            | Self::Seedance2_0FastVip
            | Self::Seedance2_0Mini => {
                matches!(resolution, VideoResolution::P720)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoRatio {
    R1x1,
    R3x4,
    R16x9,
    R4x3,
    R9x16,
    R21x9,
}

impl VideoRatio {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::R1x1 => "1:1",
            Self::R3x4 => "3:4",
            Self::R16x9 => "16:9",
            Self::R4x3 => "4:3",
            Self::R9x16 => "9:16",
            Self::R21x9 => "21:9",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoResolution {
    P720,
    P1080,
    K4,
}

impl VideoResolution {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::P720 => "720p",
            Self::P1080 => "1080p",
            Self::K4 => "4k",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextToImageRequestV1 {
    prompt: String,
    model: ImageModelVersion,
    ratio: ImageRatio,
    resolution: ImageResolution,
    generate_num: u8,
}

impl TextToImageRequestV1 {
    pub fn new(
        prompt: impl Into<String>,
        model: ImageModelVersion,
        ratio: ImageRatio,
        resolution: ImageResolution,
        generate_num: u8,
    ) -> Result<Self, RequestValidationError> {
        let prompt = validate_prompt(prompt.into())?;
        if !(1..=10).contains(&generate_num) {
            return Err(RequestValidationError::InvalidGenerateNum(generate_num));
        }
        if !model.supports(resolution) {
            return Err(RequestValidationError::UnsupportedImageResolution { model, resolution });
        }
        Ok(Self {
            prompt,
            model,
            ratio,
            resolution,
            generate_num,
        })
    }

    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    pub fn model(&self) -> ImageModelVersion {
        self.model
    }

    pub fn ratio(&self) -> ImageRatio {
        self.ratio
    }

    pub fn resolution(&self) -> ImageResolution {
        self.resolution
    }

    pub fn generate_num(&self) -> u8 {
        self.generate_num
    }

    pub fn to_argv(&self) -> Vec<OsString> {
        vec![
            "text2image".into(),
            "--prompt".into(),
            self.prompt.clone().into(),
            "--model_version".into(),
            self.model.as_str().into(),
            "--ratio".into(),
            self.ratio.as_str().into(),
            "--resolution_type".into(),
            self.resolution.as_str().into(),
            "--generate_num".into(),
            self.generate_num.to_string().into(),
            "--poll=0".into(),
        ]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextToVideoRequestV1 {
    prompt: String,
    model: VideoModelVersion,
    ratio: VideoRatio,
    duration_seconds: u8,
    resolution: VideoResolution,
}

impl TextToVideoRequestV1 {
    pub fn new(
        prompt: impl Into<String>,
        model: VideoModelVersion,
        ratio: VideoRatio,
        duration_seconds: u8,
        resolution: VideoResolution,
    ) -> Result<Self, RequestValidationError> {
        let prompt = validate_prompt(prompt.into())?;
        if !(4..=15).contains(&duration_seconds) {
            return Err(RequestValidationError::InvalidVideoDuration(
                duration_seconds,
            ));
        }
        if !model.supports(resolution) {
            return Err(RequestValidationError::UnsupportedVideoResolution { model, resolution });
        }
        Ok(Self {
            prompt,
            model,
            ratio,
            duration_seconds,
            resolution,
        })
    }

    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    pub fn model(&self) -> VideoModelVersion {
        self.model
    }

    pub fn ratio(&self) -> VideoRatio {
        self.ratio
    }

    pub fn duration_seconds(&self) -> u8 {
        self.duration_seconds
    }

    pub fn resolution(&self) -> VideoResolution {
        self.resolution
    }

    pub fn to_argv(&self) -> Vec<OsString> {
        vec![
            "text2video".into(),
            "--prompt".into(),
            self.prompt.clone().into(),
            "--model_version".into(),
            self.model.as_str().into(),
            "--ratio".into(),
            self.ratio.as_str().into(),
            "--duration".into(),
            self.duration_seconds.to_string().into(),
            "--video_resolution".into(),
            self.resolution.as_str().into(),
            "--poll=0".into(),
        ]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryResultRequestV1 {
    submit_id: OpaqueProviderId,
    download_dir: PathBuf,
}

impl QueryResultRequestV1 {
    pub fn new(
        submit_id: impl Into<String>,
        download_dir: impl Into<PathBuf>,
    ) -> Result<Self, RequestValidationError> {
        let submit_id = submit_id.into();
        if submit_id.trim().is_empty() {
            return Err(RequestValidationError::EmptySubmitId);
        }
        let submit_id = OpaqueProviderId::new(submit_id)
            .map_err(|_| RequestValidationError::InvalidSubmitId)?;

        let download_dir = download_dir.into();
        if download_dir.as_os_str().is_empty() {
            return Err(RequestValidationError::EmptyDownloadDirectory);
        }
        if !download_dir.is_absolute() {
            return Err(RequestValidationError::DownloadDirectoryNotAbsolute);
        }
        if download_dir
            .components()
            .any(|component| component == Component::ParentDir)
            || download_dir.as_os_str().as_bytes().contains(&0)
        {
            return Err(RequestValidationError::InvalidDownloadDirectory);
        }

        Ok(Self {
            submit_id,
            download_dir,
        })
    }

    pub fn submit_id(&self) -> &str {
        self.submit_id.as_str()
    }

    pub fn download_dir(&self) -> &std::path::Path {
        &self.download_dir
    }

    pub fn to_argv(&self) -> Vec<OsString> {
        vec![
            "query_result".into(),
            "--submit_id".into(),
            self.submit_id.as_str().into(),
            "--download_dir".into(),
            self.download_dir.as_os_str().to_owned(),
        ]
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RequestValidationError {
    #[error("prompt must contain non-whitespace text")]
    EmptyPrompt,
    #[error("prompt contains a NUL byte")]
    InvalidPrompt,
    #[error("generate_num must be between 1 and 10, got {0}")]
    InvalidGenerateNum(u8),
    #[error("image model {model:?} does not support resolution {resolution:?}")]
    UnsupportedImageResolution {
        model: ImageModelVersion,
        resolution: ImageResolution,
    },
    #[error("video duration must be between 4 and 15 seconds, got {0}")]
    InvalidVideoDuration(u8),
    #[error("video model {model:?} does not support resolution {resolution:?}")]
    UnsupportedVideoResolution {
        model: VideoModelVersion,
        resolution: VideoResolution,
    },
    #[error("submit_id must contain non-whitespace text")]
    EmptySubmitId,
    #[error("submit_id contains control characters")]
    InvalidSubmitId,
    #[error("download directory must not be empty")]
    EmptyDownloadDirectory,
    #[error("download directory must be absolute")]
    DownloadDirectoryNotAbsolute,
    #[error("download directory contains a parent traversal or NUL byte")]
    InvalidDownloadDirectory,
}

fn validate_prompt(prompt: String) -> Result<String, RequestValidationError> {
    if prompt.trim().is_empty() {
        return Err(RequestValidationError::EmptyPrompt);
    }
    if prompt.contains('\0') {
        return Err(RequestValidationError::InvalidPrompt);
    }
    Ok(prompt)
}
