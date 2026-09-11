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
    for image in &job.images {
        validate_edit_input_dimensions(image, "image")?;
    }
    if let Some(mask) = &job.mask {
        validate_edit_mask(job.images.first(), mask)?;
    }
    Ok(())
}

fn validate_edit_input_dimensions(
    input: &InputImage,
    param: &'static str,
) -> Result<(u32, u32), ImageGatewayError> {
    let dimensions = image_dimensions(&input.bytes).ok_or_else(|| {
        ImageGatewayError::invalid_request(
            "input image dimensions could not be read",
            Some(param.to_string()),
            "invalid_image_format",
        )
    })?;
    if !dimensions_within_input_budget(dimensions) {
        return Err(ImageGatewayError::invalid_request(
            "input image dimensions exceed the decode budget",
            Some(param.to_string()),
            "invalid_image_size",
        ));
    }
    Ok(dimensions)
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
    let mask_dims = validate_edit_input_dimensions(mask, "mask")?;
    if let Some(image) = image {
        let image_dims = validate_edit_input_dimensions(image, "image")?;
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

    fn png_header(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = vec![0_u8; 26];
        bytes[..8].copy_from_slice(b"\x89PNG\r\n\x1a\n");
        bytes[12..16].copy_from_slice(b"IHDR");
        bytes[16..20].copy_from_slice(&width.to_be_bytes());
        bytes[20..24].copy_from_slice(&height.to_be_bytes());
        bytes[25] = 6;
        bytes
    }

    fn edit_job(image: InputImage, mask: Option<InputImage>) -> EditJob {
        EditJob {
            request_id: "request".to_string(),
            model: "gpt-image-2".to_string(),
            prompt: "edit".to_string(),
            moderation: "auto".to_string(),
            images: vec![image],
            mask,
            n: 1,
            size: "auto".to_string(),
            quality: "auto".to_string(),
            output_format: "png".to_string(),
            output_compression: None,
            background: "opaque".to_string(),
            stream: false,
            partial_images: 0,
        }
    }

    #[test]
    fn oversized_image_dimensions_are_rejected_without_a_mask() {
        let image = InputImage {
            filename: None,
            content_type: Some("image/png".to_string()),
            bytes: png_header(6929, 6929),
        };

        let error = validate_edit_job(&edit_job(image, None)).unwrap_err();
        assert_eq!(error.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(error.error_code(), Some("invalid_image_size"));
    }

    #[test]
    fn image_side_above_limit_is_rejected_without_a_mask() {
        let image = InputImage {
            filename: None,
            content_type: Some("image/png".to_string()),
            bytes: png_header(8193, 1),
        };

        let error = validate_edit_job(&edit_job(image, None)).unwrap_err();
        assert_eq!(error.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(error.error_code(), Some("invalid_image_size"));
    }

    #[test]
    fn normal_image_dimensions_are_accepted_without_a_mask() {
        let image = InputImage {
            filename: None,
            content_type: Some("image/png".to_string()),
            bytes: png_header(1024, 1024),
        };

        validate_edit_job(&edit_job(image, None)).unwrap();
    }

    #[test]
    fn unreadable_image_dimensions_keep_the_invalid_format_error() {
        let image = InputImage {
            filename: None,
            content_type: Some("image/png".to_string()),
            bytes: b"not-an-image".to_vec(),
        };

        let error = validate_edit_job(&edit_job(image, None)).unwrap_err();
        assert_eq!(error.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(error.error_code(), Some("invalid_image_format"));
    }

    #[test]
    fn oversized_mask_dimensions_are_rejected_without_decoding_pixels() {
        let image = InputImage {
            filename: None,
            content_type: Some("image/png".to_string()),
            bytes: png_header(8192, 8192),
        };
        let mask = InputImage {
            filename: None,
            content_type: Some("image/png".to_string()),
            bytes: png_header(8192, 8192),
        };

        let error = validate_edit_mask(Some(&image), &mask).unwrap_err();
        assert_eq!(error.error_code(), Some("invalid_image_size"));
    }
}
