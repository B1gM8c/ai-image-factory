use std::io::Cursor;

use image::{DynamicImage, ImageFormat, Rgb, RgbImage};

use crate::{
    ImageGatewayError,
    core::{
        image_bytes::{is_jpeg, is_png, is_webp},
        provider::GeneratedImage,
    },
    size::{
        SizeConstraint, aspect_ratio_matches, aspect_ratio_tolerance_percent, parse_size_constraint,
    },
};

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
    let format_matches = bytes_match_output_format(&image.bytes, output_format);

    match requested_size {
        SizeConstraint::Auto => {
            if format_matches && !generated_image_has_alpha(&image.bytes) {
                return Ok(image);
            }
            let decoded = decode_generated_image(&image.bytes)?;
            let decoded = flatten_to_opaque(decoded);
            encode_image(decoded, output_format, output_compression)
                .map(|bytes| GeneratedImage { bytes })
        }
        SizeConstraint::Dimensions { width, height } => {
            let decoded = decode_generated_image(&image.bytes)?;
            if decoded.width() != width || decoded.height() != height {
                return Err(ImageGatewayError::backend(format!(
                    "Codex CLI produced an image with dimensions {}x{}, not the requested {}x{}",
                    decoded.width(),
                    decoded.height(),
                    width,
                    height
                )));
            }
            let decoded = flatten_to_opaque(decoded);
            if format_matches && !generated_image_has_alpha(&image.bytes) {
                return Ok(image);
            }
            encode_image(decoded, output_format, output_compression)
                .map(|bytes| GeneratedImage { bytes })
        }
        SizeConstraint::AspectRatio { width, height } => {
            let decoded = decode_generated_image(&image.bytes)?;
            if !aspect_ratio_matches(decoded.width(), decoded.height(), width, height) {
                return Err(ImageGatewayError::backend(format!(
                    "Codex CLI produced an image with dimensions {}x{}, not the requested {}:{} aspect ratio within {:.2}% tolerance",
                    decoded.width(),
                    decoded.height(),
                    width,
                    height,
                    aspect_ratio_tolerance_percent()
                )));
            }
            let decoded = flatten_to_opaque(decoded);
            if format_matches && !generated_image_has_alpha(&image.bytes) {
                return Ok(image);
            }
            encode_image(decoded, output_format, output_compression)
                .map(|bytes| GeneratedImage { bytes })
        }
    }
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
    image::load_from_memory(bytes)
        .map_err(|_| ImageGatewayError::backend("Codex CLI produced an unreadable image"))
}

fn generated_image_has_alpha(bytes: &[u8]) -> bool {
    image::load_from_memory(bytes)
        .map(|image| image.color().has_alpha())
        .unwrap_or(false)
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

fn bytes_match_output_format(bytes: &[u8], output_format: &str) -> bool {
    match output_format {
        "png" => is_png(bytes),
        "jpeg" => is_jpeg(bytes),
        "webp" => is_webp(bytes),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use image::ImageFormat;

    use super::*;

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

    #[test]
    fn rejects_png_with_unexpected_dimensions() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1254, 1254),
        };

        assert!(normalize_generated_images(vec![image], "1024x1024", "png", None).is_err());
    }

    #[test]
    fn accepts_png_with_requested_aspect_ratio() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1672, 941),
        };

        assert!(normalize_generated_images(vec![image], "16:9", "png", None).is_ok());
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

        assert!(is_jpeg(&normalized[0].bytes));
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
