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

const SEMANTIC_MASK_PROMPT_MARKER: &str = "[factory-spatial-edit:semantic-mask-v1]";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XaiImageEditFallbackMode {
    SemanticMask,
}

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
        Self::for_grok_cli_inner(job, inputs, None)
    }

    pub fn for_grok_cli_with_fallback(
        job: &EditJob,
        inputs: Vec<EditInputDescriptorV1>,
        fallback: XaiImageEditFallbackMode,
    ) -> Result<Self, XaiImageEditAdmissionError> {
        Self::for_grok_cli_inner(job, inputs, Some(fallback))
    }

    fn for_grok_cli_inner(
        job: &EditJob,
        inputs: Vec<EditInputDescriptorV1>,
        fallback: Option<XaiImageEditFallbackMode>,
    ) -> Result<Self, XaiImageEditAdmissionError> {
        validate_supported_fields(job, fallback)?;
        let source_command =
            EditCommandV1::from_edit_job(job, inputs, XAI_IMAGES_API_PROFILE, PROVIDER_ID);
        let source_image_count = source_command
            .inputs
            .iter()
            .filter(|input| input.role == EditInputRoleV1::Image)
            .count();
        let staged_images = source_command
            .inputs
            .iter()
            .map(|input| staged_image(input, fallback))
            .collect::<Result<Vec<_>, _>>()?;
        let aspect_ratio = if source_image_count == 1 {
            ImageAspectRatio::Auto
        } else {
            parse_aspect_ratio(&job.size)?
        };
        let prompt = semantic_mask_prompt(&job.prompt, job.mask.is_some(), fallback);
        let request = GrokImageEditRequestV1::new(prompt, staged_images, aspect_ratio)?;
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

fn validate_supported_fields(
    job: &EditJob,
    fallback: Option<XaiImageEditFallbackMode>,
) -> Result<(), XaiImageEditAdmissionError> {
    if job.mask.is_some() && fallback.is_none() {
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
    fallback: Option<XaiImageEditFallbackMode>,
) -> Result<StagedImageV1, XaiImageEditAdmissionError> {
    let filename_prefix = match (input.role, fallback) {
        (EditInputRoleV1::Image, _) => format!("image-{}", input.index),
        (EditInputRoleV1::Mask, Some(XaiImageEditFallbackMode::SemanticMask)) => "mask".to_owned(),
        (EditInputRoleV1::Mask, None) => return Err(XaiImageEditAdmissionError::UnsupportedMask),
    };
    let extension = match input.media_type.as_str() {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/webp" => "webp",
        _ => return Err(XaiImageEditAdmissionError::InvalidProviderCommand),
    };
    Ok(StagedImageV1::new(
        format!("{filename_prefix}.{extension}"),
        input.sha256_hex.clone(),
    )?)
}

fn semantic_mask_prompt(
    user_prompt: &str,
    has_mask: bool,
    fallback: Option<XaiImageEditFallbackMode>,
) -> String {
    if !has_mask
        || fallback != Some(XaiImageEditFallbackMode::SemanticMask)
        || user_prompt.contains(SEMANTIC_MASK_PROMPT_MARKER)
    {
        return user_prompt.to_owned();
    }
    format!(
        "{user_prompt}\n\n{SEMANTIC_MASK_PROMPT_MARKER}\nThe final reference image is mask.png. Its transparent pixels identify the requested edit region. Apply the requested change only inside that region and preserve pixels outside it as closely as possible. This is a semantic region hint, not pixel-exact inpainting."
    )
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

    fn mask_descriptor() -> EditInputDescriptorV1 {
        EditInputDescriptorV1 {
            byte_size: 16,
            index: 0,
            media_type: "image/png".to_owned(),
            role: EditInputRoleV1::Mask,
            sha256_hex: format!("{:064x}", 99),
        }
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

    #[test]
    fn strict_xai_projection_still_rejects_masks() {
        let mut request = job();
        request.mask = Some(crate::generator::InputImage {
            bytes: Vec::new(),
            content_type: Some("image/png".to_owned()),
            filename: Some("mask.png".to_owned()),
        });
        let mut inputs = descriptors(1);
        inputs.push(mask_descriptor());

        assert_eq!(
            XaiImageEditAdmissionPlan::for_grok_cli(&request, inputs),
            Err(XaiImageEditAdmissionError::UnsupportedMask)
        );
    }

    #[test]
    fn opted_in_semantic_mask_becomes_a_final_reference_and_prompt_hint() {
        let mut request = job();
        request.mask = Some(crate::generator::InputImage {
            bytes: Vec::new(),
            content_type: Some("image/png".to_owned()),
            filename: Some("mask.png".to_owned()),
        });
        let mut inputs = descriptors(1);
        inputs.push(mask_descriptor());

        let plan = XaiImageEditAdmissionPlan::for_grok_cli_with_fallback(
            &request,
            inputs,
            XaiImageEditFallbackMode::SemanticMask,
        )
        .unwrap();
        let bytes = serde_json::to_vec(plan.provider_command()).unwrap();
        let payload = image_provider_grok_cli::parse_image_edit_payload(&bytes).unwrap();

        assert_eq!(payload.request().images().len(), 2);
        assert_eq!(payload.request().images()[1].filename(), "mask.png");
        assert_eq!(
            payload.request().aspect_ratio(),
            image_provider_grok_cli::ImageAspectRatio::Auto
        );
        assert_eq!(
            payload
                .request()
                .prompt()
                .matches(SEMANTIC_MASK_PROMPT_MARKER)
                .count(),
            1
        );
    }

    #[test]
    fn semantic_mask_marker_is_not_appended_twice() {
        let prompt = format!("edit the marked region\n{SEMANTIC_MASK_PROMPT_MARKER}");
        let projected =
            semantic_mask_prompt(&prompt, true, Some(XaiImageEditFallbackMode::SemanticMask));

        assert_eq!(projected.matches(SEMANTIC_MASK_PROMPT_MARKER).count(), 1);
    }
}
