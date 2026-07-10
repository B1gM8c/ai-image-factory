pub(crate) fn is_png(bytes: &[u8]) -> bool {
    bytes.starts_with(b"\x89PNG\r\n\x1a\n")
}

pub(crate) fn is_jpeg(bytes: &[u8]) -> bool {
    bytes.len() >= 3 && bytes[0] == 0xff && bytes[1] == 0xd8 && bytes[2] == 0xff
}

pub(crate) fn is_webp(bytes: &[u8]) -> bool {
    bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP"
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
        image::load_from_memory(bytes)
            .ok()
            .map(|image| (image.width(), image.height()))
    })
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
