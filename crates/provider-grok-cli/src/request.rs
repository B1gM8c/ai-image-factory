use std::path::{Component, Path};

use thiserror::Error;

pub const MAX_PROMPT_CHARS: usize = 1_024;
pub const MAX_IMAGE_EDIT_REFERENCES: usize = 3;
pub const MAX_REFERENCE_VIDEO_IMAGES: usize = 7;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageModel {
    Base,
    Quality,
}

impl ImageModel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Base => "grok-imagine-image",
            Self::Quality => "grok-imagine-image-quality",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageAspectRatio {
    Auto,
    R1x1,
    R16x9,
    R9x16,
    R4x3,
    R3x4,
    R3x2,
    R2x3,
    R2x1,
    R1x2,
    R19_5x9,
    R9x19_5,
    R20x9,
    R9x20,
}

impl ImageAspectRatio {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::R1x1 => "1:1",
            Self::R16x9 => "16:9",
            Self::R9x16 => "9:16",
            Self::R4x3 => "4:3",
            Self::R3x4 => "3:4",
            Self::R3x2 => "3:2",
            Self::R2x3 => "2:3",
            Self::R2x1 => "2:1",
            Self::R1x2 => "1:2",
            Self::R19_5x9 => "19.5:9",
            Self::R9x19_5 => "9:19.5",
            Self::R20x9 => "20:9",
            Self::R9x20 => "9:20",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoAspectRatio {
    R1x1,
    R16x9,
    R9x16,
    R3x2,
    R2x3,
}

impl VideoAspectRatio {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::R1x1 => "1:1",
            Self::R16x9 => "16:9",
            Self::R9x16 => "9:16",
            Self::R3x2 => "3:2",
            Self::R2x3 => "2:3",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoDuration {
    Seconds6,
    Seconds10,
}

impl VideoDuration {
    pub const fn seconds(self) -> u8 {
        match self {
            Self::Seconds6 => 6,
            Self::Seconds10 => 10,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoResolution {
    P480,
    P720,
}

impl VideoResolution {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::P480 => "480p",
            Self::P720 => "720p",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedImageV1 {
    filename: String,
    sha256: String,
}

impl StagedImageV1 {
    pub fn new(
        filename: impl Into<String>,
        sha256: impl Into<String>,
    ) -> Result<Self, RequestValidationError> {
        let filename = filename.into();
        let mut components = Path::new(&filename).components();
        let valid_filename = matches!(components.next(), Some(Component::Normal(_)))
            && components.next().is_none()
            && filename.len() <= 255
            && !filename.contains('\0');
        if !valid_filename {
            return Err(RequestValidationError::InvalidStagedFilename);
        }

        let sha256 = sha256.into();
        if !valid_sha256(&sha256) {
            return Err(RequestValidationError::InvalidStagedSha256);
        }
        Ok(Self { filename, sha256 })
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrokImageGenerationRequestV1 {
    prompt: String,
    model: ImageModel,
    aspect_ratio: ImageAspectRatio,
}

impl GrokImageGenerationRequestV1 {
    pub fn new(
        prompt: impl Into<String>,
        model: ImageModel,
        aspect_ratio: ImageAspectRatio,
    ) -> Result<Self, RequestValidationError> {
        Ok(Self {
            prompt: validate_required_prompt(prompt.into())?,
            model,
            aspect_ratio,
        })
    }

    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    pub fn model(&self) -> ImageModel {
        self.model
    }

    pub fn aspect_ratio(&self) -> ImageAspectRatio {
        self.aspect_ratio
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrokImageEditRequestV1 {
    prompt: String,
    images: Vec<StagedImageV1>,
    aspect_ratio: ImageAspectRatio,
}

impl GrokImageEditRequestV1 {
    pub fn new(
        prompt: impl Into<String>,
        images: Vec<StagedImageV1>,
        aspect_ratio: ImageAspectRatio,
    ) -> Result<Self, RequestValidationError> {
        validate_reference_count(images.len(), 1, MAX_IMAGE_EDIT_REFERENCES)?;
        validate_unique_filenames(&images)?;
        if images.len() == 1 && aspect_ratio != ImageAspectRatio::Auto {
            return Err(RequestValidationError::SingleImageAspectRatioUnsupported);
        }
        Ok(Self {
            prompt: validate_required_prompt(prompt.into())?,
            images,
            aspect_ratio,
        })
    }

    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    pub fn images(&self) -> &[StagedImageV1] {
        &self.images
    }

    pub fn aspect_ratio(&self) -> ImageAspectRatio {
        self.aspect_ratio
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageToVideoRequestV1 {
    prompt: Option<String>,
    image: StagedImageV1,
    duration: VideoDuration,
    resolution: VideoResolution,
}

impl ImageToVideoRequestV1 {
    pub fn new(
        prompt: Option<String>,
        image: StagedImageV1,
        duration: VideoDuration,
        resolution: VideoResolution,
    ) -> Result<Self, RequestValidationError> {
        Ok(Self {
            prompt: validate_optional_prompt(prompt)?,
            image,
            duration,
            resolution,
        })
    }

    pub fn prompt(&self) -> Option<&str> {
        self.prompt.as_deref()
    }

    pub fn image(&self) -> &StagedImageV1 {
        &self.image
    }

    pub fn duration(&self) -> VideoDuration {
        self.duration
    }

    pub fn resolution(&self) -> VideoResolution {
        self.resolution
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextToVideoRequestV1 {
    prompt: String,
    aspect_ratio: VideoAspectRatio,
    duration: VideoDuration,
    resolution: VideoResolution,
}

impl TextToVideoRequestV1 {
    pub fn new(
        prompt: impl Into<String>,
        aspect_ratio: VideoAspectRatio,
        duration: VideoDuration,
        resolution: VideoResolution,
    ) -> Result<Self, RequestValidationError> {
        Ok(Self {
            prompt: validate_required_prompt(prompt.into())?,
            aspect_ratio,
            duration,
            resolution,
        })
    }

    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    pub fn aspect_ratio(&self) -> VideoAspectRatio {
        self.aspect_ratio
    }

    pub fn duration(&self) -> VideoDuration {
        self.duration
    }

    pub fn resolution(&self) -> VideoResolution {
        self.resolution
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceToVideoRequestV1 {
    prompt: String,
    images: Vec<StagedImageV1>,
    aspect_ratio: VideoAspectRatio,
    duration: VideoDuration,
    resolution: VideoResolution,
}

impl ReferenceToVideoRequestV1 {
    pub fn new(
        prompt: impl Into<String>,
        images: Vec<StagedImageV1>,
        aspect_ratio: VideoAspectRatio,
        duration: VideoDuration,
        resolution: VideoResolution,
    ) -> Result<Self, RequestValidationError> {
        validate_reference_count(images.len(), 2, MAX_REFERENCE_VIDEO_IMAGES)?;
        validate_unique_filenames(&images)?;
        Ok(Self {
            prompt: validate_required_prompt(prompt.into())?,
            images,
            aspect_ratio,
            duration,
            resolution,
        })
    }

    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    pub fn images(&self) -> &[StagedImageV1] {
        &self.images
    }

    pub fn aspect_ratio(&self) -> VideoAspectRatio {
        self.aspect_ratio
    }

    pub fn duration(&self) -> VideoDuration {
        self.duration
    }

    pub fn resolution(&self) -> VideoResolution {
        self.resolution
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrokVideoGenerationRequestV1 {
    TextToVideo(TextToVideoRequestV1),
    ImageToVideo(ImageToVideoRequestV1),
    ReferenceToVideo(ReferenceToVideoRequestV1),
}

impl From<TextToVideoRequestV1> for GrokVideoGenerationRequestV1 {
    fn from(request: TextToVideoRequestV1) -> Self {
        Self::TextToVideo(request)
    }
}

impl From<ImageToVideoRequestV1> for GrokVideoGenerationRequestV1 {
    fn from(request: ImageToVideoRequestV1) -> Self {
        Self::ImageToVideo(request)
    }
}

impl From<ReferenceToVideoRequestV1> for GrokVideoGenerationRequestV1 {
    fn from(request: ReferenceToVideoRequestV1) -> Self {
        Self::ReferenceToVideo(request)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RequestValidationError {
    #[error("prompt must contain non-whitespace text")]
    EmptyPrompt,
    #[error("prompt exceeds the 1024 character Grok media limit")]
    PromptTooLong,
    #[error("prompt contains a NUL byte")]
    InvalidPrompt,
    #[error("staged image filename must be one safe path component")]
    InvalidStagedFilename,
    #[error("staged image SHA-256 must be 64 lowercase hexadecimal characters")]
    InvalidStagedSha256,
    #[error("reference image count is outside the supported Grok CLI range")]
    InvalidReferenceCount,
    #[error("reference image filenames must be unique")]
    DuplicateReferenceFilename,
    #[error("Grok CLI ignores aspect_ratio for single-image edits")]
    SingleImageAspectRatioUnsupported,
}

fn validate_required_prompt(prompt: String) -> Result<String, RequestValidationError> {
    if prompt.trim().is_empty() {
        return Err(RequestValidationError::EmptyPrompt);
    }
    validate_prompt_bytes(&prompt)?;
    Ok(prompt)
}

fn validate_optional_prompt(
    prompt: Option<String>,
) -> Result<Option<String>, RequestValidationError> {
    let Some(prompt) = prompt else {
        return Ok(None);
    };
    if prompt.trim().is_empty() {
        return Ok(None);
    }
    validate_prompt_bytes(&prompt)?;
    Ok(Some(prompt))
}

fn validate_prompt_bytes(prompt: &str) -> Result<(), RequestValidationError> {
    if prompt.contains('\0') {
        return Err(RequestValidationError::InvalidPrompt);
    }
    if prompt.chars().count() > MAX_PROMPT_CHARS {
        return Err(RequestValidationError::PromptTooLong);
    }
    Ok(())
}

fn validate_reference_count(
    actual: usize,
    minimum: usize,
    maximum: usize,
) -> Result<(), RequestValidationError> {
    if !(minimum..=maximum).contains(&actual) {
        return Err(RequestValidationError::InvalidReferenceCount);
    }
    Ok(())
}

fn validate_unique_filenames(images: &[StagedImageV1]) -> Result<(), RequestValidationError> {
    for (index, image) in images.iter().enumerate() {
        if images[..index]
            .iter()
            .any(|other| other.filename == image.filename)
        {
            return Err(RequestValidationError::DuplicateReferenceFilename);
        }
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}
