use std::ffi::OsString;

use crate::{
    ImageModelVersion, RequestValidationError, StagedImageV1, StagedTimedMediaV1,
    TextToImageRequestV1, VideoModelVersion, VideoRatio, VideoResolution,
};
use crate::{request::validate_prompt, staged::validate_unique_names};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageToImageRequestV1 {
    images: Vec<StagedImageV1>,
    parameters: TextToImageRequestV1,
}

impl ImageToImageRequestV1 {
    pub fn new(
        images: Vec<StagedImageV1>,
        parameters: TextToImageRequestV1,
    ) -> Result<Self, RequestValidationError> {
        validate_images(&images, 1, 10)?;
        if matches!(
            parameters.model(),
            ImageModelVersion::V3_0 | ImageModelVersion::V3_1
        ) {
            return Err(RequestValidationError::UnsupportedWorkflowModel);
        }
        Ok(Self { images, parameters })
    }
    pub fn images(&self) -> &[StagedImageV1] {
        &self.images
    }
    pub fn parameters(&self) -> &TextToImageRequestV1 {
        &self.parameters
    }
    pub fn generate_num(&self) -> u8 {
        self.parameters.generate_num()
    }
    pub(crate) fn into_single_output(mut self) -> Self {
        self.parameters = self.parameters.into_single_output();
        self
    }
    pub fn to_argv(&self) -> Vec<OsString> {
        let mut args = self.parameters.to_argv();
        args[0] = "image2image".into();
        args.splice(
            1..1,
            [OsString::from("--images"), image_list(&self.images).into()],
        );
        args
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpscaleResolution {
    K2,
    K4,
    K8,
}
impl UpscaleResolution {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::K2 => "2k",
            Self::K4 => "4k",
            Self::K8 => "8k",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageUpscaleRequestV1 {
    image: StagedImageV1,
    resolution: UpscaleResolution,
}
impl ImageUpscaleRequestV1 {
    pub fn new(image: StagedImageV1, resolution: UpscaleResolution) -> Self {
        Self { image, resolution }
    }
    pub fn image(&self) -> &StagedImageV1 {
        &self.image
    }
    pub fn resolution(&self) -> UpscaleResolution {
        self.resolution
    }
    pub fn to_argv(&self) -> Vec<OsString> {
        vec![
            "image_upscale".into(),
            "--image".into(),
            self.image.filename().into(),
            "--resolution_type".into(),
            self.resolution.as_str().into(),
            "--poll=0".into(),
        ]
    }
}

/// Options shared by first-frame and first/last-frame generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceVideoOptionsV1 {
    prompt: String,
    model: VideoModelVersion,
    ratio: Option<VideoRatio>,
    duration_seconds: u8,
    resolution: VideoResolution,
}
impl ReferenceVideoOptionsV1 {
    pub fn new(
        prompt: impl Into<String>,
        model: VideoModelVersion,
        ratio: Option<VideoRatio>,
        duration_seconds: u8,
        resolution: VideoResolution,
    ) -> Result<Self, RequestValidationError> {
        let prompt = validate_prompt(prompt.into())?;
        validate_video(model, duration_seconds, resolution)?;
        if model == VideoModelVersion::Seedance2_5 && ratio.is_some() {
            return Err(RequestValidationError::ReferenceRatioUnsupported);
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
    pub fn ratio(&self) -> Option<VideoRatio> {
        self.ratio
    }
    pub fn duration_seconds(&self) -> u8 {
        self.duration_seconds
    }
    pub fn resolution(&self) -> VideoResolution {
        self.resolution
    }
    fn validate_image(&self, image: &StagedImageV1) -> Result<(), RequestValidationError> {
        if matches!(
            self.model,
            VideoModelVersion::Seedance1_0Fast | VideoModelVersion::Seedance1_5Pro
        ) && let Some(ratio) = self.ratio
        {
            let (width, height) = ratio_dimensions(ratio);
            if u64::from(image.width()) * height != u64::from(image.height()) * width {
                return Err(RequestValidationError::ReferenceRatioMismatch);
            }
        }
        Ok(())
    }
    fn append_argv(&self, args: &mut Vec<OsString>) {
        args.extend([
            "--prompt".into(),
            self.prompt.clone().into(),
            "--model_version".into(),
            self.model.as_str().into(),
            "--duration".into(),
            self.duration_seconds.to_string().into(),
            "--video_resolution".into(),
            self.resolution.as_str().into(),
        ]);
        if let Some(ratio) = self.ratio {
            args.extend(["--ratio".into(), ratio.as_str().into()]);
        }
        args.push("--poll=0".into());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageToVideoRequestV1 {
    image: StagedImageV1,
    options: ReferenceVideoOptionsV1,
}
impl ImageToVideoRequestV1 {
    pub fn new(
        image: StagedImageV1,
        options: ReferenceVideoOptionsV1,
    ) -> Result<Self, RequestValidationError> {
        options.validate_image(&image)?;
        Ok(Self { image, options })
    }
    pub fn image(&self) -> &StagedImageV1 {
        &self.image
    }
    pub fn options(&self) -> &ReferenceVideoOptionsV1 {
        &self.options
    }
    pub fn to_argv(&self) -> Vec<OsString> {
        let mut args = vec![
            "image2video".into(),
            "--image".into(),
            self.image.filename().into(),
        ];
        self.options.append_argv(&mut args);
        args
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FramesToVideoRequestV1 {
    first: StagedImageV1,
    last: StagedImageV1,
    options: ReferenceVideoOptionsV1,
}
impl FramesToVideoRequestV1 {
    pub fn new(
        first: StagedImageV1,
        last: StagedImageV1,
        options: ReferenceVideoOptionsV1,
    ) -> Result<Self, RequestValidationError> {
        if options.model() == VideoModelVersion::Seedance1_0Fast {
            return Err(RequestValidationError::UnsupportedWorkflowModel);
        }
        validate_unique_names([first.filename(), last.filename()])?;
        options.validate_image(&first)?;
        Ok(Self {
            first,
            last,
            options,
        })
    }
    pub fn first(&self) -> &StagedImageV1 {
        &self.first
    }
    pub fn last(&self) -> &StagedImageV1 {
        &self.last
    }
    pub fn options(&self) -> &ReferenceVideoOptionsV1 {
        &self.options
    }
    pub fn to_argv(&self) -> Vec<OsString> {
        let mut args = vec![
            "frames2video".into(),
            "--first".into(),
            self.first.filename().into(),
            "--last".into(),
            self.last.filename().into(),
        ];
        self.options.append_argv(&mut args);
        args
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VideoTransitionV1 {
    prompt: String,
    duration_ms: u32,
}
impl VideoTransitionV1 {
    pub fn new(
        prompt: impl Into<String>,
        duration_ms: u32,
    ) -> Result<Self, RequestValidationError> {
        if !(1000..=8000).contains(&duration_ms) {
            return Err(RequestValidationError::InvalidTransitionDuration);
        }
        Ok(Self {
            prompt: validate_prompt(prompt.into())?,
            duration_ms,
        })
    }
    pub fn prompt(&self) -> &str {
        &self.prompt
    }
    pub fn duration_ms(&self) -> u32 {
        self.duration_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultiframeToVideoRequestV1 {
    images: Vec<StagedImageV1>,
    transitions: Vec<VideoTransitionV1>,
    resolution: VideoResolution,
}
impl MultiframeToVideoRequestV1 {
    pub fn new(
        images: Vec<StagedImageV1>,
        transitions: Vec<VideoTransitionV1>,
        resolution: VideoResolution,
    ) -> Result<Self, RequestValidationError> {
        validate_images(&images, 2, 20)?;
        if transitions.len() != images.len() - 1 {
            return Err(RequestValidationError::InvalidTransitionCount);
        }
        if transitions.iter().map(|t| t.duration_ms).sum::<u32>() < 2000 {
            return Err(RequestValidationError::InvalidTransitionDuration);
        }
        if !matches!(resolution, VideoResolution::P720 | VideoResolution::P1080) {
            return Err(RequestValidationError::InvalidMultiframeResolution);
        }
        Ok(Self {
            images,
            transitions,
            resolution,
        })
    }
    pub fn images(&self) -> &[StagedImageV1] {
        &self.images
    }
    pub fn transitions(&self) -> &[VideoTransitionV1] {
        &self.transitions
    }
    pub fn resolution(&self) -> VideoResolution {
        self.resolution
    }
    pub fn to_argv(&self) -> Vec<OsString> {
        let mut args = vec![
            "multiframe2video".into(),
            "--images".into(),
            image_list(&self.images).into(),
            "--video_resolution".into(),
            self.resolution.as_str().into(),
        ];
        let shorthand = self.images.len() == 2;
        for transition in &self.transitions {
            args.extend([
                if shorthand {
                    "--prompt"
                } else {
                    "--transition-prompt"
                }
                .into(),
                transition.prompt.clone().into(),
                if shorthand {
                    "--duration"
                } else {
                    "--transition-duration"
                }
                .into(),
                seconds_from_millis(transition.duration_ms).into(),
            ]);
        }
        args.push("--poll=0".into());
        args
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultimodalToVideoRequestV1 {
    images: Vec<StagedImageV1>,
    videos: Vec<StagedTimedMediaV1>,
    audios: Vec<StagedTimedMediaV1>,
    prompt: String,
    model: VideoModelVersion,
    ratio: VideoRatio,
    duration_seconds: u8,
    resolution: VideoResolution,
}
impl MultimodalToVideoRequestV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        images: Vec<StagedImageV1>,
        videos: Vec<StagedTimedMediaV1>,
        audios: Vec<StagedTimedMediaV1>,
        prompt: impl Into<String>,
        model: VideoModelVersion,
        ratio: VideoRatio,
        duration_seconds: u8,
        resolution: VideoResolution,
    ) -> Result<Self, RequestValidationError> {
        if matches!(
            model,
            VideoModelVersion::Seedance1_0Fast | VideoModelVersion::Seedance1_5Pro
        ) {
            return Err(RequestValidationError::UnsupportedWorkflowModel);
        }
        validate_video(model, duration_seconds, resolution)?;
        let prompt = prompt.into();
        if prompt.contains('\0') {
            return Err(RequestValidationError::InvalidPrompt);
        }
        let (max_images, max_timed, max_total, max_duration) =
            if model == VideoModelVersion::Seedance2_5 {
                (30, 10, 50, 30_000)
            } else {
                (9, 3, 12, 15_000)
            };
        let count = images.len() + videos.len() + audios.len();
        if count == 0
            || count > max_total
            || images.len() > max_images
            || videos.len() > max_timed
            || audios.len() > max_timed
            || (model != VideoModelVersion::Seedance2_5 && images.is_empty() && videos.is_empty())
        {
            return Err(RequestValidationError::InvalidInputCount);
        }
        for media in [&videos, &audios] {
            if media
                .iter()
                .any(|m| !(2000..=max_duration).contains(&m.duration_ms()))
                || media
                    .iter()
                    .try_fold(0_u64, |total, m| total.checked_add(m.duration_ms()))
                    .is_none_or(|total| total > max_duration)
            {
                return Err(RequestValidationError::UnsupportedReferenceDuration);
            }
        }
        validate_unique_names(
            images.iter().map(StagedImageV1::filename).chain(
                videos
                    .iter()
                    .chain(&audios)
                    .map(StagedTimedMediaV1::filename),
            ),
        )?;
        Ok(Self {
            images,
            videos,
            audios,
            prompt,
            model,
            ratio,
            duration_seconds,
            resolution,
        })
    }
    pub fn images(&self) -> &[StagedImageV1] {
        &self.images
    }
    pub fn videos(&self) -> &[StagedTimedMediaV1] {
        &self.videos
    }
    pub fn audios(&self) -> &[StagedTimedMediaV1] {
        &self.audios
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
        let mut args = vec!["multimodal2video".into()];
        for image in &self.images {
            args.extend(["--image".into(), image.filename().into()]);
        }
        for video in &self.videos {
            args.extend(["--video".into(), video.filename().into()]);
        }
        for audio in &self.audios {
            args.extend(["--audio".into(), audio.filename().into()]);
        }
        if !self.prompt.is_empty() {
            args.extend(["--prompt".into(), self.prompt.clone().into()]);
        }
        args.extend([
            "--model_version".into(),
            self.model.as_str().into(),
            "--ratio".into(),
            self.ratio.as_str().into(),
            "--duration".into(),
            self.duration_seconds.to_string().into(),
            "--video_resolution".into(),
            self.resolution.as_str().into(),
            "--poll=0".into(),
        ]);
        args
    }
}

fn validate_video(
    model: VideoModelVersion,
    duration: u8,
    resolution: VideoResolution,
) -> Result<(), RequestValidationError> {
    let (min, max) = match model {
        VideoModelVersion::Seedance1_0Fast => (5, 10),
        VideoModelVersion::Seedance1_5Pro => (5, 12),
        VideoModelVersion::Seedance2_5 => (4, 30),
        _ => (4, 15),
    };
    if !(min..=max).contains(&duration) {
        return Err(RequestValidationError::InvalidVideoDuration(duration));
    }
    if !model.supports(resolution) {
        return Err(RequestValidationError::UnsupportedVideoResolution { model, resolution });
    }
    Ok(())
}
fn validate_images(
    images: &[StagedImageV1],
    min: usize,
    max: usize,
) -> Result<(), RequestValidationError> {
    if !(min..=max).contains(&images.len()) {
        return Err(RequestValidationError::InvalidInputCount);
    }
    validate_unique_names(images.iter().map(StagedImageV1::filename))
}
fn image_list(images: &[StagedImageV1]) -> String {
    images
        .iter()
        .map(StagedImageV1::filename)
        .collect::<Vec<_>>()
        .join(",")
}
fn ratio_dimensions(ratio: VideoRatio) -> (u64, u64) {
    match ratio {
        VideoRatio::R1x1 => (1, 1),
        VideoRatio::R3x4 => (3, 4),
        VideoRatio::R16x9 => (16, 9),
        VideoRatio::R4x3 => (4, 3),
        VideoRatio::R9x16 => (9, 16),
        VideoRatio::R21x9 => (21, 9),
    }
}
fn seconds_from_millis(ms: u32) -> String {
    if ms.is_multiple_of(1000) {
        (ms / 1000).to_string()
    } else {
        format!("{}.{:03}", ms / 1000, ms % 1000)
            .trim_end_matches('0')
            .to_owned()
    }
}
