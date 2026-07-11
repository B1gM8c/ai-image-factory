use async_trait::async_trait;

use crate::ImageGatewayError;

use super::image_bytes::{
    dimensions_within_input_budget, image_dimensions, is_png, png_has_alpha_channel,
};

#[derive(Clone, Debug)]
pub struct GenerationJob {
    pub request_id: String,
    pub model: String,
    pub prompt: String,
    pub moderation: String,
    pub n: u32,
    pub size: String,
    pub quality: String,
    pub output_format: String,
    pub output_compression: Option<u8>,
    pub background: String,
    pub stream: bool,
    pub partial_images: u32,
}

#[derive(Clone, Debug)]
pub struct EditJob {
    pub request_id: String,
    pub model: String,
    pub prompt: String,
    pub moderation: String,
    pub images: Vec<InputImage>,
    pub mask: Option<InputImage>,
    pub n: u32,
    pub size: String,
    pub quality: String,
    pub output_format: String,
    pub output_compression: Option<u8>,
    pub background: String,
    pub stream: bool,
    pub partial_images: u32,
}

#[derive(Clone, Debug)]
pub struct InputImage {
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct GeneratedImage {
    pub bytes: Vec<u8>,
}

#[async_trait]
pub trait ImageGenerator: Send + Sync + 'static {
    async fn generate(&self, job: GenerationJob) -> Result<Vec<GeneratedImage>, ImageGatewayError>;

    async fn edit(&self, job: EditJob) -> Result<Vec<GeneratedImage>, ImageGatewayError>;
}

pub(crate) fn validate_edit_job(job: &EditJob) -> Result<(), ImageGatewayError> {
    if let Some(mask) = &job.mask {
        validate_edit_mask(job.images.first(), mask)?;
    }
    Ok(())
}

pub(crate) fn validate_edit_mask(
    image: Option<&InputImage>,
    mask: &InputImage,
) -> Result<(), ImageGatewayError> {
    if !is_png(&mask.bytes) {
        return Err(ImageGatewayError::invalid_request(
            "mask must be a PNG image",
            Some("mask".to_string()),
            "invalid_image_format",
        ));
    }
    if !png_has_alpha_channel(&mask.bytes) {
        return Err(ImageGatewayError::invalid_request(
            "mask must contain an alpha channel",
            Some("mask".to_string()),
            "invalid_image_format",
        ));
    }
    if let Some(image) = image {
        let image_dims = image_dimensions(&image.bytes).ok_or_else(|| {
            ImageGatewayError::invalid_request(
                "image dimensions could not be read",
                Some("image".to_string()),
                "invalid_image_format",
            )
        })?;
        let mask_dims = image_dimensions(&mask.bytes).ok_or_else(|| {
            ImageGatewayError::invalid_request(
                "mask dimensions could not be read",
                Some("mask".to_string()),
                "invalid_image_format",
            )
        })?;
        if !dimensions_within_input_budget(image_dims) || !dimensions_within_input_budget(mask_dims)
        {
            return Err(ImageGatewayError::invalid_request(
                "image dimensions exceed the decode budget",
                Some("image".to_string()),
                "image_too_large",
            ));
        }
        if image_dims != mask_dims {
            return Err(ImageGatewayError::invalid_request(
                "mask dimensions must match the first image",
                Some("mask".to_string()),
                "image_dimensions_mismatch",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oversized_png_header() -> Vec<u8> {
        let mut bytes = vec![0_u8; 26];
        bytes[..8].copy_from_slice(b"\x89PNG\r\n\x1a\n");
        bytes[12..16].copy_from_slice(b"IHDR");
        bytes[16..20].copy_from_slice(&8192_u32.to_be_bytes());
        bytes[20..24].copy_from_slice(&8192_u32.to_be_bytes());
        bytes[25] = 6;
        bytes
    }

    #[test]
    fn oversized_mask_dimensions_are_rejected_without_decoding_pixels() {
        let image = InputImage {
            filename: None,
            content_type: Some("image/png".to_string()),
            bytes: oversized_png_header(),
        };
        let mask = InputImage {
            filename: None,
            content_type: Some("image/png".to_string()),
            bytes: oversized_png_header(),
        };

        let error = validate_edit_mask(Some(&image), &mask).unwrap_err();
        assert_eq!(error.error_code(), Some("image_too_large"));
    }
}
