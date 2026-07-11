pub(crate) fn is_png(bytes: &[u8]) -> bool {
    bytes.starts_with(b"\x89PNG\r\n\x1a\n")
}

pub(crate) fn png_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 24 || !is_png(bytes) || &bytes[12..16] != b"IHDR" {
        return None;
    }
    Some((
        u32::from_be_bytes(bytes[16..20].try_into().ok()?),
        u32::from_be_bytes(bytes[20..24].try_into().ok()?),
    ))
}

pub(crate) fn image_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    png_dimensions(bytes).or_else(|| {
        ImageReader::new(Cursor::new(bytes))
            .with_guessed_format()
            .ok()
            .and_then(|reader| reader.into_dimensions().ok())
    })
}

pub(crate) fn dimensions_within_input_budget((width, height): (u32, u32)) -> bool {
    width <= MAX_INPUT_IMAGE_DIMENSION
        && height <= MAX_INPUT_IMAGE_DIMENSION
        && u64::from(width).saturating_mul(u64::from(height)) <= MAX_INPUT_IMAGE_PIXELS
}

pub(crate) fn png_has_alpha_channel(bytes: &[u8]) -> bool {
    matches!(png_color_type(bytes), Some(4 | 6))
}

fn png_color_type(bytes: &[u8]) -> Option<u8> {
    if bytes.len() < 26 || !is_png(bytes) || &bytes[12..16] != b"IHDR" {
        return None;
    }
    Some(bytes[25])
}
use std::io::Cursor;

use image::ImageReader;

const MAX_INPUT_IMAGE_PIXELS: u64 = 16 * 1024 * 1024;
const MAX_INPUT_IMAGE_DIMENSION: u32 = 8 * 1024;
