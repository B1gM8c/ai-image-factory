use std::io::Cursor;

use image::{DynamicImage, ImageFormat, ImageReader, Rgb, RgbImage};

use crate::{
    ImageGatewayError,
    core::provider::GeneratedImage,
    size::{
        SizeConstraint, aspect_ratio_matches, aspect_ratio_tolerance_percent, parse_size_constraint,
    },
};

const MAX_DECODED_IMAGE_PIXELS: u64 = 16 * 1024 * 1024;
const MAX_DECODED_IMAGE_DIMENSION: u32 = 8 * 1024;
const MAX_ASPECT_RATIO_CENTER_CROP_FRACTION: f64 = 0.02;

pub fn normalize_generated_images(
    images: Vec<GeneratedImage>,
    size: &str,
    output_format: &str,
    output_compression: Option<u8>,
) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
    let requested_size = requested_size_constraint(size)?;
    images
        .into_iter()
        .map(|image| {
            normalize_generated_image(image, requested_size, output_format, output_compression)
        })
        .collect()
}

fn normalize_generated_image(
    image: GeneratedImage,
    requested_size: SizeConstraint,
    output_format: &str,
    output_compression: Option<u8>,
) -> Result<GeneratedImage, ImageGatewayError> {
    let decoded = decode_generated_image(&image.bytes)?;
    let decoded = match requested_size {
        SizeConstraint::Auto => decoded,
        SizeConstraint::Dimensions { width, height } => {
            if !aspect_ratio_matches(decoded.width(), decoded.height(), width, height) {
                return Err(ImageGatewayError::backend(format!(
                    "Codex CLI produced an image with dimensions {}x{}, not the requested {}x{} aspect ratio within {:.2}% tolerance",
                    decoded.width(),
                    decoded.height(),
                    width,
                    height,
                    aspect_ratio_tolerance_percent()
                )));
            }
            decoded
        }
        SizeConstraint::AspectRatio { width, height } => {
            center_crop_to_aspect_ratio(decoded, width, height)?
        }
    };
    encode_image(
        flatten_to_opaque(decoded),
        output_format,
        output_compression,
    )
    .map(|bytes| GeneratedImage { bytes })
}

fn center_crop_to_aspect_ratio(
    image: DynamicImage,
    target_width: u32,
    target_height: u32,
) -> Result<DynamicImage, ImageGatewayError> {
    let width = image.width();
    let height = image.height();
    let actual_ratio = f64::from(width) / f64::from(height);
    let target_ratio = f64::from(target_width) / f64::from(target_height);

    let (crop_width, crop_height) = if actual_ratio > target_ratio {
        ((f64::from(height) * target_ratio).round() as u32, height)
    } else {
        (width, (f64::from(width) / target_ratio).round() as u32)
    };
    let cropped_fraction = if crop_width < width {
        f64::from(width - crop_width) / f64::from(width)
    } else {
        f64::from(height - crop_height) / f64::from(height)
    };
    if cropped_fraction > MAX_ASPECT_RATIO_CENTER_CROP_FRACTION {
        return Err(ImageGatewayError::backend(format!(
            "Codex CLI produced an image with dimensions {}x{}, too far from the requested {}:{} aspect ratio for a safe center crop",
            width, height, target_width, target_height
        )));
    }

    let x = (width - crop_width) / 2;
    let y = (height - crop_height) / 2;
    Ok(image.crop_imm(x, y, crop_width, crop_height))
}

fn requested_size_constraint(size: &str) -> Result<SizeConstraint, ImageGatewayError> {
    parse_size_constraint(size)
        .ok_or_else(|| ImageGatewayError::backend("Invalid requested image size"))
}

fn encode_image(
    image: DynamicImage,
    output_format: &str,
    output_compression: Option<u8>,
) -> Result<Vec<u8>, ImageGatewayError> {
    let mut cursor = Cursor::new(Vec::new());
    match output_format {
        "png" => image
            .write_to(&mut cursor, ImageFormat::Png)
            .map_err(|_| ImageGatewayError::backend("Failed to encode PNG image"))?,
        "jpeg" => {
            let quality = output_compression.unwrap_or(100);
            let rgb = image.to_rgb8();
            let mut encoder =
                image::codecs::jpeg::JpegEncoder::new_with_quality(&mut cursor, quality);
            encoder
                .encode(
                    &rgb,
                    rgb.width(),
                    rgb.height(),
                    image::ExtendedColorType::Rgb8,
                )
                .map_err(|_| ImageGatewayError::backend("Failed to encode JPEG image"))?;
        }
        "webp" => image
            .write_to(&mut cursor, ImageFormat::WebP)
            .map_err(|_| ImageGatewayError::backend("Failed to encode WebP image"))?,
        _ => {
            return Err(ImageGatewayError::backend(
                "Unsupported normalized image output format",
            ));
        }
    }
    Ok(cursor.into_inner())
}

fn decode_generated_image(bytes: &[u8]) -> Result<DynamicImage, ImageGatewayError> {
    let reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|_| ImageGatewayError::backend("Codex CLI produced an unreadable image"))?;
    let (width, height) = reader
        .into_dimensions()
        .map_err(|_| ImageGatewayError::backend("Codex CLI produced an unreadable image"))?;
    if width > MAX_DECODED_IMAGE_DIMENSION
        || height > MAX_DECODED_IMAGE_DIMENSION
        || u64::from(width).saturating_mul(u64::from(height)) > MAX_DECODED_IMAGE_PIXELS
    {
        return Err(ImageGatewayError::backend(
            "Codex CLI produced an image that exceeds the decode budget",
        ));
    }
    image::load_from_memory(bytes)
        .map_err(|_| ImageGatewayError::backend("Codex CLI produced an unreadable image"))
}

fn flatten_to_opaque(image: DynamicImage) -> DynamicImage {
    if !image.color().has_alpha() {
        return image;
    }

    let rgba = image.to_rgba8();
    let mut flattened = RgbImage::from_pixel(rgba.width(), rgba.height(), Rgb([255, 255, 255]));
    for (x, y, pixel) in rgba.enumerate_pixels() {
        let [red, green, blue, alpha] = pixel.0;
        let alpha = f32::from(alpha) / 255.0;
        let blend = |channel: u8| -> u8 {
            ((f32::from(channel) * alpha) + (255.0 * (1.0 - alpha))).round() as u8
        };
        flattened.put_pixel(x, y, Rgb([blend(red), blend(green), blend(blue)]));
    }
    DynamicImage::ImageRgb8(flattened)
}

#[cfg(test)]
mod tests {
    use image::ImageFormat;

    use super::*;
    use crate::core::image_bytes::is_png;

    fn valid_png_with_dimensions(width: u32, height: u32) -> Vec<u8> {
        let image =
            image::ImageBuffer::from_pixel(width, height, image::Rgba([255u8, 255, 255, 255]));
        let mut cursor = Cursor::new(Vec::new());
        image.write_to(&mut cursor, ImageFormat::Png).unwrap();
        cursor.into_inner()
    }

    fn transparent_png_with_dimensions(width: u32, height: u32) -> Vec<u8> {
        let image = image::ImageBuffer::from_pixel(width, height, image::Rgba([255u8, 0, 0, 128]));
        let mut cursor = Cursor::new(Vec::new());
        image.write_to(&mut cursor, ImageFormat::Png).unwrap();
        cursor.into_inner()
    }

    fn jpeg_with_comment(comment: &[u8]) -> Vec<u8> {
        let image = image::ImageBuffer::from_pixel(1, 1, image::Rgb([255u8, 255, 255]));
        let mut cursor = Cursor::new(Vec::new());
        image.write_to(&mut cursor, ImageFormat::Jpeg).unwrap();
        let encoded = cursor.into_inner();
        let segment_len = u16::try_from(comment.len() + 2).unwrap();
        let mut with_comment = vec![0xff, 0xd8, 0xff, 0xfe];
        with_comment.extend_from_slice(&segment_len.to_be_bytes());
        with_comment.extend_from_slice(comment);
        with_comment.extend_from_slice(&encoded[2..]);
        with_comment
    }

    #[test]
    fn accepts_png_with_requested_dimension_ratio() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1254, 1254),
        };

        assert!(normalize_generated_images(vec![image], "1024x1024", "png", None).is_ok());
    }

    #[test]
    fn rejects_png_with_unexpected_dimension_ratio() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1254, 1254),
        };

        assert!(normalize_generated_images(vec![image], "1536x1024", "png", None).is_err());
    }

    #[test]
    fn accepts_png_with_requested_aspect_ratio() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1672, 941),
        };

        assert!(normalize_generated_images(vec![image], "16:9", "png", None).is_ok());
    }

    #[test]
    fn center_crops_small_codex_aspect_ratio_drift() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1659, 948),
        };

        let normalized = normalize_generated_images(vec![image], "16:9", "png", None).unwrap();
        let decoded = image::load_from_memory(&normalized[0].bytes).unwrap();

        assert_eq!((decoded.width(), decoded.height()), (1659, 933));
        assert!(aspect_ratio_matches(
            decoded.width(),
            decoded.height(),
            16,
            9
        ));
    }

    #[test]
    fn matching_output_is_reencoded_without_untrusted_metadata() {
        let secret = b"credential-must-not-survive";
        let output = normalize_generated_images(
            vec![GeneratedImage {
                bytes: jpeg_with_comment(secret),
            }],
            "auto",
            "jpeg",
            None,
        )
        .unwrap();

        assert!(
            !output[0]
                .bytes
                .windows(secret.len())
                .any(|window| window == secret)
        );
    }

    #[test]
    fn rejects_png_with_unexpected_aspect_ratio() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1254, 1254),
        };

        assert!(normalize_generated_images(vec![image], "16:9", "png", None).is_err());
    }

    #[test]
    fn normalizes_png_to_requested_jpeg_format() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1024, 1024),
        };

        let normalized =
            normalize_generated_images(vec![image], "1024x1024", "jpeg", Some(80)).unwrap();

        assert!(normalized[0].bytes.starts_with(&[0xff, 0xd8, 0xff]));
    }

    #[test]
    fn flattens_transparent_png_to_opaque_png() {
        let image = GeneratedImage {
            bytes: transparent_png_with_dimensions(1024, 1024),
        };

        let normalized = normalize_generated_images(vec![image], "1024x1024", "png", None).unwrap();
        let decoded = image::load_from_memory(&normalized[0].bytes).unwrap();

        assert!(is_png(&normalized[0].bytes));
        assert!(!decoded.color().has_alpha());
    }
}
