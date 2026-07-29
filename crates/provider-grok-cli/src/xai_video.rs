use image_api_contracts::xai::{
    XaiVideoAspectRatio, XaiVideoGenerationCommandV1, XaiVideoResolution, XaiVideoWorkflow,
};
use thiserror::Error;

use crate::{
    GrokVideoGenerationRequestV1, ImageToVideoRequestV1, ReferenceToVideoRequestV1,
    RequestValidationError, StagedImageV1, VideoAspectRatio, VideoDuration, VideoResolution,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrokVideoGenerationProjectionV1 {
    provider_request: GrokVideoGenerationRequestV1,
    user: Option<String>,
}

impl GrokVideoGenerationProjectionV1 {
    pub fn provider_request(&self) -> &GrokVideoGenerationRequestV1 {
        &self.provider_request
    }

    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    pub fn into_provider_request(self) -> GrokVideoGenerationRequestV1 {
        self.provider_request
    }
}

pub fn project_xai_video_generation(
    command: XaiVideoGenerationCommandV1,
    staged_images: Vec<StagedImageV1>,
) -> Result<GrokVideoGenerationProjectionV1, XaiGrokVideoProjectionError> {
    if command.schema_version != 1 || command.operation != "videos.generations" {
        return Err(XaiGrokVideoProjectionError::InvalidSourceCommand);
    }
    if command.output.is_some() {
        return Err(XaiGrokVideoProjectionError::UnsupportedOutput);
    }
    if command.storage_options.is_some() {
        return Err(XaiGrokVideoProjectionError::UnsupportedStorageOptions);
    }
    if command
        .image
        .iter()
        .chain(command.reference_images.iter())
        .any(|image| image.file_id.is_some())
    {
        return Err(XaiGrokVideoProjectionError::UnsupportedFileId);
    }
    let duration = map_duration(command.duration)?;
    let resolution = map_resolution(command.resolution)?;
    let provider_request = match command.workflow() {
        XaiVideoWorkflow::TextToVideo => {
            return Err(XaiGrokVideoProjectionError::UnsupportedWorkflow);
        }
        XaiVideoWorkflow::ImageToVideo => {
            require_model(
                command.model.as_deref(),
                &[
                    "grok-imagine-video-1.5",
                    "grok-imagine-video-1.5-preview",
                    "grok-imagine-video-1.5-2026-05-30",
                ],
            )?;
            if command.aspect_ratio.is_some() {
                return Err(XaiGrokVideoProjectionError::UnsupportedAspectRatio);
            }
            let [image] = staged_images.as_slice() else {
                return Err(XaiGrokVideoProjectionError::InputManifestMismatch);
            };
            ImageToVideoRequestV1::new(command.prompt, image.clone(), duration, resolution)?.into()
        }
        XaiVideoWorkflow::ReferenceToVideo => {
            require_model(command.model.as_deref(), &["grok-imagine-video"])?;
            if staged_images.len() != command.reference_images.len() {
                return Err(XaiGrokVideoProjectionError::InputManifestMismatch);
            }
            let aspect_ratio =
                map_aspect_ratio(command.aspect_ratio.unwrap_or(XaiVideoAspectRatio::R16x9))?;
            ReferenceToVideoRequestV1::new(
                command.prompt.unwrap_or_default(),
                staged_images,
                aspect_ratio,
                duration,
                resolution,
            )?
            .into()
        }
    };
    Ok(GrokVideoGenerationProjectionV1 {
        provider_request,
        user: command.user,
    })
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum XaiGrokVideoProjectionError {
    #[error("xAI video source command is invalid")]
    InvalidSourceCommand,
    #[error("xAI field `model` is required by the Grok CLI video binding")]
    ModelRequired,
    #[error("xAI video model is not bound by this Grok CLI workflow")]
    UnsupportedModel,
    #[error("xAI video workflow is not exposed by Grok CLI")]
    UnsupportedWorkflow,
    #[error("xAI field `duration` is not supported by Grok CLI")]
    UnsupportedDuration,
    #[error("xAI field `resolution` is not supported by Grok CLI")]
    UnsupportedResolution,
    #[error("xAI field `aspect_ratio` is not supported by this Grok CLI workflow")]
    UnsupportedAspectRatio,
    #[error("xAI field `output` is not supported by the local-artifact binding")]
    UnsupportedOutput,
    #[error("xAI field `storage_options` is not supported by the Grok CLI binding")]
    UnsupportedStorageOptions,
    #[error("xAI Files API `file_id` input is not supported by the Grok CLI binding")]
    UnsupportedFileId,
    #[error("sealed input manifest does not match the xAI video request")]
    InputManifestMismatch,
    #[error(transparent)]
    InvalidProviderRequest(#[from] RequestValidationError),
}

impl XaiGrokVideoProjectionError {
    pub const fn parameter(self) -> Option<&'static str> {
        match self {
            Self::InvalidSourceCommand | Self::InputManifestMismatch => None,
            Self::ModelRequired | Self::UnsupportedModel => Some("model"),
            Self::UnsupportedWorkflow => Some("image"),
            Self::UnsupportedDuration => Some("duration"),
            Self::UnsupportedResolution => Some("resolution"),
            Self::UnsupportedAspectRatio => Some("aspect_ratio"),
            Self::UnsupportedOutput => Some("output"),
            Self::UnsupportedStorageOptions => Some("storage_options"),
            Self::UnsupportedFileId => Some("image"),
            Self::InvalidProviderRequest(_) => Some("prompt"),
        }
    }
}

fn require_model(
    model: Option<&str>,
    supported: &[&str],
) -> Result<(), XaiGrokVideoProjectionError> {
    let model = model.ok_or(XaiGrokVideoProjectionError::ModelRequired)?;
    if supported.contains(&model) {
        Ok(())
    } else {
        Err(XaiGrokVideoProjectionError::UnsupportedModel)
    }
}

fn map_duration(value: u8) -> Result<VideoDuration, XaiGrokVideoProjectionError> {
    match value {
        6 => Ok(VideoDuration::Seconds6),
        10 => Ok(VideoDuration::Seconds10),
        _ => Err(XaiGrokVideoProjectionError::UnsupportedDuration),
    }
}

fn map_resolution(
    value: XaiVideoResolution,
) -> Result<VideoResolution, XaiGrokVideoProjectionError> {
    match value {
        XaiVideoResolution::P480 => Ok(VideoResolution::P480),
        XaiVideoResolution::P720 => Ok(VideoResolution::P720),
        XaiVideoResolution::P1080 => Err(XaiGrokVideoProjectionError::UnsupportedResolution),
    }
}

fn map_aspect_ratio(
    value: XaiVideoAspectRatio,
) -> Result<VideoAspectRatio, XaiGrokVideoProjectionError> {
    match value {
        XaiVideoAspectRatio::R1x1 => Ok(VideoAspectRatio::R1x1),
        XaiVideoAspectRatio::R16x9 => Ok(VideoAspectRatio::R16x9),
        XaiVideoAspectRatio::R9x16 => Ok(VideoAspectRatio::R9x16),
        XaiVideoAspectRatio::R3x2 => Ok(VideoAspectRatio::R3x2),
        XaiVideoAspectRatio::R2x3 => Ok(VideoAspectRatio::R2x3),
        XaiVideoAspectRatio::R4x3 | XaiVideoAspectRatio::R3x4 => {
            Err(XaiGrokVideoProjectionError::UnsupportedAspectRatio)
        }
    }
}

#[cfg(test)]
mod tests {
    use image_api_contracts::xai::{XaiVideoGenerationRequest, XaiVideoImageUrl};

    use super::*;

    fn image() -> XaiVideoImageUrl {
        XaiVideoImageUrl {
            file_id: None,
            url: Some("data:image/png;base64,AA==".to_owned()),
        }
    }

    fn staged() -> StagedImageV1 {
        StagedImageV1::new("input.png", "a".repeat(64)).unwrap()
    }

    #[test]
    fn image_to_video_maps_the_official_15_model_alias() {
        let source = XaiVideoGenerationCommandV1::from_request(XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(6),
            image: Some(image()),
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: Some("slow push in".to_owned()),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: Some("customer-1".to_owned()),
        })
        .unwrap();
        let projection = project_xai_video_generation(source, vec![staged()]).unwrap();
        let GrokVideoGenerationRequestV1::ImageToVideo(request) = projection.provider_request()
        else {
            panic!("expected image-to-video")
        };
        assert_eq!(request.duration(), VideoDuration::Seconds6);
        assert_eq!(request.resolution(), VideoResolution::P480);
        assert_eq!(projection.user(), Some("customer-1"));
    }

    #[test]
    fn unsupported_official_options_fail_by_field() {
        let request = |duration, resolution, aspect_ratio| XaiVideoGenerationRequest {
            aspect_ratio,
            duration: Some(duration),
            image: Some(image()),
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: None,
            reference_images: Vec::new(),
            resolution: Some(resolution),
            storage_options: None,
            user: None,
        };
        assert_eq!(
            project_xai_video_generation(
                XaiVideoGenerationCommandV1::from_request(request(
                    8,
                    XaiVideoResolution::P480,
                    None,
                ))
                .unwrap(),
                vec![staged()],
            ),
            Err(XaiGrokVideoProjectionError::UnsupportedDuration)
        );
        assert_eq!(
            project_xai_video_generation(
                XaiVideoGenerationCommandV1::from_request(request(
                    6,
                    XaiVideoResolution::P1080,
                    None,
                ))
                .unwrap(),
                vec![staged()],
            ),
            Err(XaiGrokVideoProjectionError::UnsupportedResolution)
        );
        assert_eq!(
            project_xai_video_generation(
                XaiVideoGenerationCommandV1::from_request(request(
                    6,
                    XaiVideoResolution::P480,
                    Some(XaiVideoAspectRatio::R16x9),
                ))
                .unwrap(),
                vec![staged()],
            ),
            Err(XaiGrokVideoProjectionError::UnsupportedAspectRatio)
        );
    }
}
