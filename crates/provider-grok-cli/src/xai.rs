use image_api_contracts::xai::{
    XaiImageAspectRatio, XaiImageGenerationCommandV1, XaiImageResolution, XaiImageResponseFormat,
    XaiImageStorageOptions,
};
use thiserror::Error;

use crate::{GrokImageGenerationRequestV1, ImageAspectRatio, ImageModel, RequestValidationError};

pub const GROK_CLI_IMAGE_MAX_OUTPUTS: u32 = 1;
/// Grok CLI 0.2.102 does not expose image resolution; observed output is fixed at 1K.
pub const GROK_CLI_IMAGE_RESOLUTION: XaiImageResolution = XaiImageResolution::R1k;
/// Grok CLI yields a local artifact, so the factory can faithfully project only Base64.
pub const GROK_CLI_IMAGE_RESPONSE_FORMAT: XaiImageResponseFormat = XaiImageResponseFormat::B64Json;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrokImageGenerationProjectionV1 {
    provider_request: GrokImageGenerationRequestV1,
    response_format: XaiImageResponseFormat,
    storage_options: Option<XaiImageStorageOptions>,
    user: Option<String>,
}

impl GrokImageGenerationProjectionV1 {
    pub fn provider_request(&self) -> &GrokImageGenerationRequestV1 {
        &self.provider_request
    }

    pub fn response_format(&self) -> XaiImageResponseFormat {
        self.response_format
    }

    pub fn storage_options(&self) -> Option<&XaiImageStorageOptions> {
        self.storage_options.as_ref()
    }

    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    pub fn into_provider_request(self) -> GrokImageGenerationRequestV1 {
        self.provider_request
    }
}

pub fn project_xai_image_generation(
    command: XaiImageGenerationCommandV1,
) -> Result<GrokImageGenerationProjectionV1, XaiGrokProjectionError> {
    if command.schema_version != 1 || command.operation != "images.generations" {
        return Err(XaiGrokProjectionError::InvalidSourceCommand);
    }
    let model = match command.model.as_deref() {
        Some("grok-imagine-image-quality") => ImageModel::Quality,
        Some("grok-imagine-image") => ImageModel::Base,
        Some(_) => return Err(XaiGrokProjectionError::UnsupportedModel),
        None => return Err(XaiGrokProjectionError::ModelRequired),
    };
    if command.n != GROK_CLI_IMAGE_MAX_OUTPUTS {
        return Err(XaiGrokProjectionError::UnsupportedOutputCount);
    }
    if command.resolution != GROK_CLI_IMAGE_RESOLUTION {
        return Err(XaiGrokProjectionError::UnsupportedResolution);
    }
    if command.response_format != GROK_CLI_IMAGE_RESPONSE_FORMAT {
        return Err(XaiGrokProjectionError::UnsupportedResponseFormat);
    }
    if command.storage_options.is_some() {
        return Err(XaiGrokProjectionError::UnsupportedStorageOptions);
    }
    let provider_request = GrokImageGenerationRequestV1::new(
        command.prompt,
        model,
        map_aspect_ratio(command.aspect_ratio),
    )?;
    Ok(GrokImageGenerationProjectionV1 {
        provider_request,
        response_format: command.response_format,
        storage_options: command.storage_options,
        user: command.user,
    })
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum XaiGrokProjectionError {
    #[error("xAI source command is invalid")]
    InvalidSourceCommand,
    #[error("xAI field `model` is required by the Grok CLI binding")]
    ModelRequired,
    #[error("xAI model is not bound by Grok CLI")]
    UnsupportedModel,
    #[error("xAI field `n` is not supported by the Grok CLI binding")]
    UnsupportedOutputCount,
    #[error("xAI field `resolution` is not supported by the Grok CLI binding")]
    UnsupportedResolution,
    #[error("xAI field `response_format` is not supported by the Grok CLI binding")]
    UnsupportedResponseFormat,
    #[error("xAI field `storage_options` is not supported by the Grok CLI binding")]
    UnsupportedStorageOptions,
    #[error(transparent)]
    InvalidProviderRequest(#[from] RequestValidationError),
}

impl XaiGrokProjectionError {
    pub const fn parameter(self) -> Option<&'static str> {
        match self {
            Self::InvalidSourceCommand => None,
            Self::ModelRequired | Self::UnsupportedModel => Some("model"),
            Self::UnsupportedOutputCount => Some("n"),
            Self::UnsupportedResolution => Some("resolution"),
            Self::UnsupportedResponseFormat => Some("response_format"),
            Self::UnsupportedStorageOptions => Some("storage_options"),
            Self::InvalidProviderRequest(_) => Some("prompt"),
        }
    }
}

fn map_aspect_ratio(value: XaiImageAspectRatio) -> ImageAspectRatio {
    match value {
        XaiImageAspectRatio::Auto => ImageAspectRatio::Auto,
        XaiImageAspectRatio::R1x1 => ImageAspectRatio::R1x1,
        XaiImageAspectRatio::R3x4 => ImageAspectRatio::R3x4,
        XaiImageAspectRatio::R4x3 => ImageAspectRatio::R4x3,
        XaiImageAspectRatio::R9x16 => ImageAspectRatio::R9x16,
        XaiImageAspectRatio::R16x9 => ImageAspectRatio::R16x9,
        XaiImageAspectRatio::R2x3 => ImageAspectRatio::R2x3,
        XaiImageAspectRatio::R3x2 => ImageAspectRatio::R3x2,
        XaiImageAspectRatio::R9x19_5 => ImageAspectRatio::R9x19_5,
        XaiImageAspectRatio::R19_5x9 => ImageAspectRatio::R19_5x9,
        XaiImageAspectRatio::R9x20 => ImageAspectRatio::R9x20,
        XaiImageAspectRatio::R20x9 => ImageAspectRatio::R20x9,
        XaiImageAspectRatio::R1x2 => ImageAspectRatio::R1x2,
        XaiImageAspectRatio::R2x1 => ImageAspectRatio::R2x1,
    }
}

#[cfg(test)]
mod tests {
    use image_api_contracts::xai::{XaiImageGenerationRequest, XaiPublicUrlOptions};

    use super::*;

    #[test]
    fn projector_preserves_supported_facade_metadata() {
        let source = XaiImageGenerationCommandV1::from_request(XaiImageGenerationRequest {
            aspect_ratio: Some(XaiImageAspectRatio::R16x9),
            model: Some("grok-imagine-image-quality".to_owned()),
            n: Some(1),
            prompt: "draw a lighthouse".to_owned(),
            resolution: Some(XaiImageResolution::R1k),
            response_format: Some(XaiImageResponseFormat::B64Json),
            storage_options: None,
            user: Some("customer-1".to_owned()),
        })
        .unwrap();

        let projection = project_xai_image_generation(source).unwrap();

        assert_eq!(projection.provider_request().model(), ImageModel::Quality);
        assert_eq!(
            projection.provider_request().aspect_ratio(),
            ImageAspectRatio::R16x9
        );
        assert_eq!(
            projection.response_format(),
            XaiImageResponseFormat::B64Json
        );
        assert!(projection.storage_options().is_none());
        assert_eq!(projection.user(), Some("customer-1"));
    }

    #[test]
    fn projector_rejects_each_official_option_the_cli_cannot_execute() {
        let request = |model: Option<&str>, n, resolution, response_format, storage_options| {
            XaiImageGenerationRequest {
                aspect_ratio: None,
                model: model.map(str::to_owned),
                n: Some(n),
                prompt: "image".to_owned(),
                resolution: Some(resolution),
                response_format: Some(response_format),
                storage_options,
                user: None,
            }
        };

        assert_eq!(
            project_xai_image_generation(
                XaiImageGenerationCommandV1::from_request(request(
                    Some("grok-imagine-image-quality"),
                    2,
                    XaiImageResolution::R1k,
                    XaiImageResponseFormat::B64Json,
                    None,
                ))
                .unwrap()
            ),
            Err(XaiGrokProjectionError::UnsupportedOutputCount)
        );
        assert_eq!(
            project_xai_image_generation(
                XaiImageGenerationCommandV1::from_request(request(
                    Some("grok-imagine-image-quality"),
                    1,
                    XaiImageResolution::R2k,
                    XaiImageResponseFormat::B64Json,
                    None,
                ))
                .unwrap()
            ),
            Err(XaiGrokProjectionError::UnsupportedResolution)
        );
        assert_eq!(
            project_xai_image_generation(
                XaiImageGenerationCommandV1::from_request(request(
                    None,
                    1,
                    XaiImageResolution::R1k,
                    XaiImageResponseFormat::B64Json,
                    None,
                ))
                .unwrap()
            ),
            Err(XaiGrokProjectionError::ModelRequired)
        );
        assert_eq!(
            project_xai_image_generation(
                XaiImageGenerationCommandV1::from_request(request(
                    Some("grok-imagine-image-unknown"),
                    1,
                    XaiImageResolution::R1k,
                    XaiImageResponseFormat::B64Json,
                    None,
                ))
                .unwrap()
            ),
            Err(XaiGrokProjectionError::UnsupportedModel)
        );
        assert_eq!(
            project_xai_image_generation(
                XaiImageGenerationCommandV1::from_request(request(
                    Some("grok-imagine-image-quality"),
                    1,
                    XaiImageResolution::R1k,
                    XaiImageResponseFormat::Url,
                    None,
                ))
                .unwrap()
            ),
            Err(XaiGrokProjectionError::UnsupportedResponseFormat)
        );
        let storage = XaiImageStorageOptions {
            expires_after: Some(3_600),
            filename: "image.jpg".to_owned(),
            public_url: Some(XaiPublicUrlOptions::Enabled(false)),
        };
        assert_eq!(
            project_xai_image_generation(
                XaiImageGenerationCommandV1::from_request(request(
                    Some("grok-imagine-image-quality"),
                    1,
                    XaiImageResolution::R1k,
                    XaiImageResponseFormat::B64Json,
                    Some(storage),
                ))
                .unwrap()
            ),
            Err(XaiGrokProjectionError::UnsupportedStorageOptions)
        );
        for (error, parameter) in [
            (XaiGrokProjectionError::ModelRequired, "model"),
            (XaiGrokProjectionError::UnsupportedModel, "model"),
            (XaiGrokProjectionError::UnsupportedOutputCount, "n"),
            (XaiGrokProjectionError::UnsupportedResolution, "resolution"),
            (
                XaiGrokProjectionError::UnsupportedResponseFormat,
                "response_format",
            ),
            (
                XaiGrokProjectionError::UnsupportedStorageOptions,
                "storage_options",
            ),
        ] {
            assert_eq!(error.parameter(), Some(parameter));
        }
    }

    #[test]
    fn projector_binds_the_generation_only_base_model() {
        let source = XaiImageGenerationCommandV1::from_request(XaiImageGenerationRequest {
            aspect_ratio: Some(XaiImageAspectRatio::R1x1),
            model: Some("grok-imagine-image".to_owned()),
            n: Some(1),
            prompt: "draw a portrait".to_owned(),
            resolution: Some(XaiImageResolution::R1k),
            response_format: Some(XaiImageResponseFormat::B64Json),
            storage_options: None,
            user: None,
        })
        .unwrap();

        let projection = project_xai_image_generation(source).unwrap();

        assert_eq!(projection.provider_request().model(), ImageModel::Base);
    }

    #[test]
    fn projector_accepts_official_media_defaults_and_rejects_the_official_url_default() {
        let request = XaiImageGenerationRequest {
            aspect_ratio: None,
            model: Some("grok-imagine-image-quality".to_owned()),
            n: None,
            prompt: "image".to_owned(),
            resolution: None,
            response_format: Some(XaiImageResponseFormat::B64Json),
            storage_options: None,
            user: None,
        };
        let projection = project_xai_image_generation(
            XaiImageGenerationCommandV1::from_request(request.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            projection.provider_request().aspect_ratio(),
            ImageAspectRatio::Auto
        );

        let mut official_default = request;
        official_default.response_format = None;
        assert_eq!(
            project_xai_image_generation(
                XaiImageGenerationCommandV1::from_request(official_default).unwrap()
            ),
            Err(XaiGrokProjectionError::UnsupportedResponseFormat)
        );
    }
}
