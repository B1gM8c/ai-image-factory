use std::io::Cursor;

use image::{DynamicImage, ImageFormat, ImageReader, Rgb, RgbImage};

use crate::{ImageGatewayError, core::provider::GeneratedImage};

const MAX_DECODED_IMAGE_PIXELS: u64 = 16 * 1024 * 1024;
const MAX_DECODED_IMAGE_DIMENSION: u32 = 8 * 1024;

pub fn normalize_generated_images(
    images: Vec<GeneratedImage>,
    output_format: &str,
    output_compression: Option<u8>,
) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
    if images.is_empty() {
        return Err(ImageGatewayError::backend(
            "Provider returned no generated images",
        ));
    }
    images
        .into_iter()
        .map(|image| normalize_generated_image(image, output_format, output_compression))
        .collect()
}

fn normalize_generated_image(
    image: GeneratedImage,
    output_format: &str,
    output_compression: Option<u8>,
) -> Result<GeneratedImage, ImageGatewayError> {
    let decoded = decode_generated_image(&image.bytes)?;
    encode_image(
        flatten_to_opaque(decoded),
        output_format,
        output_compression,
    )
    .map(|bytes| GeneratedImage { bytes })
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
    fn rejects_empty_provider_output() {
        assert!(normalize_generated_images(Vec::new(), "png", None).is_err());
    }

    #[test]
    fn accepts_valid_provider_png() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1254, 1254),
        };

        assert!(normalize_generated_images(vec![image], "png", None).is_ok());
    }

    #[test]
    fn preserves_square_provider_geometry() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1254, 1254),
        };

        let normalized = normalize_generated_images(vec![image], "png", None).unwrap();
        let decoded = image::load_from_memory(&normalized[0].bytes).unwrap();

        assert_eq!((decoded.width(), decoded.height()), (1254, 1254));
    }

    #[test]
    fn accepts_widescreen_provider_png() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1672, 941),
        };

        assert!(normalize_generated_images(vec![image], "png", None).is_ok());
    }

    #[test]
    fn preserves_near_widescreen_provider_geometry() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1659, 948),
        };

        let normalized = normalize_generated_images(vec![image], "png", None).unwrap();
        let decoded = image::load_from_memory(&normalized[0].bytes).unwrap();

        assert_eq!((decoded.width(), decoded.height()), (1659, 948));
    }

    #[test]
    fn preserves_portrait_provider_geometry_and_content_alignment() {
        let mut source = image::RgbaImage::new(948, 1659);
        for (x, _y, pixel) in source.enumerate_pixels_mut() {
            *pixel = image::Rgba([(x % 256) as u8, 0, 0, 255]);
        }
        let mut cursor = Cursor::new(Vec::new());
        DynamicImage::ImageRgba8(source)
            .write_to(&mut cursor, ImageFormat::Png)
            .unwrap();

        let normalized = normalize_generated_images(
            vec![GeneratedImage {
                bytes: cursor.into_inner(),
            }],
            "png",
            None,
        )
        .unwrap();
        let decoded = image::load_from_memory(&normalized[0].bytes).unwrap();

        assert_eq!((decoded.width(), decoded.height()), (948, 1659));
        assert_eq!(decoded.to_rgb8().get_pixel(0, 0).0[0], 0);
        assert_eq!(decoded.to_rgb8().get_pixel(947, 0).0[0], 179);
    }

    #[test]
    fn geometry_preservation_keeps_requested_output_format() {
        for (format, compression, prefix) in [
            ("jpeg", Some(80), &[0xff, 0xd8, 0xff][..]),
            ("webp", None, &b"RIFF"[..]),
        ] {
            let normalized = normalize_generated_images(
                vec![GeneratedImage {
                    bytes: valid_png_with_dimensions(1659, 948),
                }],
                format,
                compression,
            )
            .unwrap();
            let decoded = image::load_from_memory(&normalized[0].bytes).unwrap();

            assert!(normalized[0].bytes.starts_with(prefix));
            assert_eq!((decoded.width(), decoded.height()), (1659, 948));
        }
    }

    #[test]
    fn matching_output_is_reencoded_without_untrusted_metadata() {
        let secret = b"credential-must-not-survive";
        let output = normalize_generated_images(
            vec![GeneratedImage {
                bytes: jpeg_with_comment(secret),
            }],
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
    fn preserves_provider_geometry_without_ratio_enforcement() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1254, 1254),
        };

        let normalized = normalize_generated_images(vec![image], "png", None).unwrap();
        let decoded = image::load_from_memory(&normalized[0].bytes).unwrap();

        assert_eq!((decoded.width(), decoded.height()), (1254, 1254));
    }

    #[test]
    fn normalizes_png_to_requested_jpeg_format() {
        let image = GeneratedImage {
            bytes: valid_png_with_dimensions(1024, 1024),
        };

        let normalized = normalize_generated_images(vec![image], "jpeg", Some(80)).unwrap();

        assert!(normalized[0].bytes.starts_with(&[0xff, 0xd8, 0xff]));
    }

    #[test]
    fn flattens_transparent_png_to_opaque_png() {
        let image = GeneratedImage {
            bytes: transparent_png_with_dimensions(1024, 1024),
        };

        let normalized = normalize_generated_images(vec![image], "png", None).unwrap();
        let decoded = image::load_from_memory(&normalized[0].bytes).unwrap();

        assert!(is_png(&normalized[0].bytes));
        assert!(!decoded.color().has_alpha());
    }
}
