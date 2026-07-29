use image_api_contracts::xai::XAI_IMAGES_API_PROFILE;
use image_provider_grok_cli::{
    GROK_IMAGE_EDIT_COMMAND_SCHEMA, GrokImageEditPayloadV1, GrokImageEditRequestV1,
    ImageAspectRatio, PROVIDER_ID, RequestValidationError, StagedImageV1,
};
use image_provider_sdk::{OutputSlot, SingleOutputCommand};
use serde_json::Value;
use thiserror::Error;

use crate::generator::EditJob;

use super::{EDIT_INPUT_MANIFEST_SCHEMA, EditCommandV1, EditInputDescriptorV1, EditInputRoleV1};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XaiImageEditAdmissionPlan {
    source_command: EditCommandV1,
    provider_command: Value,
}

impl XaiImageEditAdmissionPlan {
    pub fn for_grok_cli(
        job: &EditJob,
        inputs: Vec<EditInputDescriptorV1>,
    ) -> Result<Self, XaiImageEditAdmissionError> {
        validate_supported_fields(job)?;
        let source_command =
            EditCommandV1::from_edit_job(job, inputs, XAI_IMAGES_API_PROFILE, PROVIDER_ID);
        let staged_images = source_command
            .inputs
            .iter()
            .map(staged_image)
            .collect::<Result<Vec<_>, _>>()?;
        let aspect_ratio = if staged_images.len() == 1 {
            ImageAspectRatio::Auto
        } else {
            parse_aspect_ratio(&job.size)?
        };
        let request = GrokImageEditRequestV1::new(job.prompt.clone(), staged_images, aspect_ratio)?;
        let payload = GrokImageEditPayloadV1::new(source_command.request_hash_hex(), request)
            .map_err(|_| XaiImageEditAdmissionError::InvalidProviderCommand)?;
        let command = SingleOutputCommand::new(
            OutputSlot::new(0, 1)
                .map_err(|_| XaiImageEditAdmissionError::InvalidProviderCommand)?,
            payload,
        )
        .map_err(|_| XaiImageEditAdmissionError::InvalidProviderCommand)?;
        let provider_command = serde_json::from_slice(command.canonical_payload())
            .map_err(|_| XaiImageEditAdmissionError::InvalidProviderCommand)?;
        Ok(Self {
            source_command,
            provider_command,
        })
    }

    pub fn source_request_hash(&self) -> String {
        self.source_command.request_hash_hex()
    }

    pub fn input_manifest_hash(&self) -> String {
        self.source_command.input_manifest_hash_hex()
    }

    pub fn provider_command(&self) -> &Value {
        &self.provider_command
    }

    pub fn provider_model(&self) -> &'static str {
        "grok-imagine-image-quality"
    }

    pub fn command_schema(&self) -> &'static str {
        GROK_IMAGE_EDIT_COMMAND_SCHEMA
    }

    pub fn input_manifest_schema(&self) -> &'static str {
        EDIT_INPUT_MANIFEST_SCHEMA
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum XaiImageEditAdmissionError {
    #[error("Grok image edits do not support masks")]
    UnsupportedMask,
    #[error("Grok image edits return exactly one image")]
    UnsupportedOutputCount,
    #[error("Grok image edits do not support streaming or partial images")]
    UnsupportedStreaming,
    #[error("Grok image edits do not support output compression")]
    UnsupportedOutputCompression,
    #[error("the requested image edit aspect ratio is unavailable in Grok CLI")]
    UnsupportedAspectRatio,
    #[error(transparent)]
    InvalidProviderRequest(#[from] RequestValidationError),
    #[error("the Grok image edit command could not be encoded")]
    InvalidProviderCommand,
}

fn validate_supported_fields(job: &EditJob) -> Result<(), XaiImageEditAdmissionError> {
    if job.mask.is_some() {
        return Err(XaiImageEditAdmissionError::UnsupportedMask);
    }
    if job.n != 1 {
        return Err(XaiImageEditAdmissionError::UnsupportedOutputCount);
    }
    if job.stream || job.partial_images != 0 {
        return Err(XaiImageEditAdmissionError::UnsupportedStreaming);
    }
    if job.output_compression.is_some() {
        return Err(XaiImageEditAdmissionError::UnsupportedOutputCompression);
    }
    Ok(())
}

fn staged_image(
    input: &EditInputDescriptorV1,
) -> Result<StagedImageV1, XaiImageEditAdmissionError> {
    if input.role != EditInputRoleV1::Image {
        return Err(XaiImageEditAdmissionError::UnsupportedMask);
    }
    let extension = match input.media_type.as_str() {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/webp" => "webp",
        _ => return Err(XaiImageEditAdmissionError::InvalidProviderCommand),
    };
    Ok(StagedImageV1::new(
        format!("image-{}.{}", input.index, extension),
        input.sha256_hex.clone(),
    )?)
}

fn parse_aspect_ratio(value: &str) -> Result<ImageAspectRatio, XaiImageEditAdmissionError> {
    match value {
        "auto" => Ok(ImageAspectRatio::Auto),
        "1:1" | "1024x1024" => Ok(ImageAspectRatio::R1x1),
        "16:9" | "1536x1024" => Ok(ImageAspectRatio::R16x9),
        "9:16" | "1024x1536" => Ok(ImageAspectRatio::R9x16),
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
        _ => Err(XaiImageEditAdmissionError::UnsupportedAspectRatio),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job() -> EditJob {
        EditJob {
            request_id: "request-1".to_owned(),
            model: "grok-imagine-image-quality".to_owned(),
            prompt: "preserve the subject and change the background".to_owned(),
            moderation: "auto".to_owned(),
            images: Vec::new(),
            mask: None,
            n: 1,
            size: "16:9".to_owned(),
            quality: "auto".to_owned(),
            output_format: "png".to_owned(),
            output_compression: None,
            background: "opaque".to_owned(),
            stream: false,
            partial_images: 0,
        }
    }

    fn descriptors(count: u16) -> Vec<EditInputDescriptorV1> {
        (0..count)
            .map(|index| EditInputDescriptorV1 {
                byte_size: 16,
                index,
                media_type: "image/png".to_owned(),
                role: EditInputRoleV1::Image,
                sha256_hex: format!("{:064x}", u64::from(index) + 1),
            })
            .collect()
    }

    #[test]
    fn plan_binds_every_reference_into_the_grok_command() {
        let plan = XaiImageEditAdmissionPlan::for_grok_cli(&job(), descriptors(2)).unwrap();
        let bytes = serde_json::to_vec(plan.provider_command()).unwrap();
        let payload = image_provider_grok_cli::parse_image_edit_payload(&bytes).unwrap();

        assert_eq!(payload.source_command_sha256(), plan.source_request_hash());
        assert_eq!(payload.request().images().len(), 2);
        assert_eq!(
            payload.request().aspect_ratio(),
            image_provider_grok_cli::ImageAspectRatio::R16x9
        );
    }

    #[test]
    fn one_reference_forces_the_cli_auto_ratio_contract() {
        let plan = XaiImageEditAdmissionPlan::for_grok_cli(&job(), descriptors(1)).unwrap();
        let bytes = serde_json::to_vec(plan.provider_command()).unwrap();
        let payload = image_provider_grok_cli::parse_image_edit_payload(&bytes).unwrap();

        assert_eq!(
            payload.request().aspect_ratio(),
            image_provider_grok_cli::ImageAspectRatio::Auto
        );
    }

    #[test]
    fn unsupported_output_count_fails_before_admission() {
        let mut request = job();
        request.n = 2;
        assert_eq!(
            XaiImageEditAdmissionPlan::for_grok_cli(&request, descriptors(1)),
            Err(XaiImageEditAdmissionError::UnsupportedOutputCount)
        );
    }
}
