//! Bounded, server-side measurements of immutable input bytes. No external processes or I/O.
//!
//! Audio is decoded packet by packet and discarded. Video validation checks the MP4 video
//! track, sample tables and data extents; it does not decode compressed video pixels.
//! MP4 inputs currently require one AVC/H.264 video track; HEVC, VP9, fragmented MP4
//! and open-ended containers are unsupported. These are gateway acceptance limits,
//! not a statement of all media formats accepted by the upstream CLI.

use std::io::Cursor;

mod audio;
mod video;

pub const MAX_TIMED_INPUT_DURATION_MS: u64 = 30_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaInputKind {
    Image,
    Video,
    Audio,
}

impl MediaInputKind {
    /// Gateway resource limits, not claims about the upstream CLI's file-size limits.
    pub const fn max_bytes(self) -> u64 {
        match self {
            Self::Image | Self::Audio => 32 * 1024 * 1024,
            Self::Video => 256 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaInputMetadata {
    media_type: &'static str,
    dimensions: Option<(u32, u32)>,
    duration_ms: Option<u64>,
}

impl MediaInputMetadata {
    pub fn media_type(&self) -> &'static str {
        self.media_type
    }
    pub fn dimensions(&self) -> Option<(u32, u32)> {
        self.dimensions
    }
    pub fn duration_ms(&self) -> Option<u64> {
        self.duration_ms
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum MediaInputError {
    #[error("invalid media input")]
    Invalid,
    #[error("unsupported media input")]
    Unsupported,
    #[error("media input exceeds resource limits")]
    LimitExceeded,
}

/// Call from a blocking worker after a bounded download. Measurements must be frozen with
/// the blob's SHA-256, not accepted from client-provided dimensions or duration fields.
pub fn probe_input(
    bytes: &[u8],
    kind: MediaInputKind,
) -> Result<MediaInputMetadata, MediaInputError> {
    if bytes.is_empty() {
        return Err(MediaInputError::Invalid);
    }
    if bytes.len() as u64 > kind.max_bytes() {
        return Err(MediaInputError::LimitExceeded);
    }
    match kind {
        MediaInputKind::Image => probe_image(bytes),
        MediaInputKind::Video => video::probe(bytes),
        MediaInputKind::Audio => audio::probe(bytes),
    }
}

fn probe_image(bytes: &[u8]) -> Result<MediaInputMetadata, MediaInputError> {
    let format = image::guess_format(bytes).map_err(|_| MediaInputError::Unsupported)?;
    let media_type = match format {
        image::ImageFormat::Png => "image/png",
        image::ImageFormat::Jpeg => "image/jpeg",
        image::ImageFormat::WebP => "image/webp",
        _ => return Err(MediaInputError::Unsupported),
    };
    let mut reader = image::ImageReader::with_format(Cursor::new(bytes), format);
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(16_384);
    limits.max_image_height = Some(16_384);
    limits.max_alloc = Some(128 * 1024 * 1024);
    reader.limits(limits);
    let image = reader.decode().map_err(|error| match error {
        image::ImageError::Limits(_) => MediaInputError::LimitExceeded,
        _ => MediaInputError::Invalid,
    })?;
    if image.width() == 0 || image.height() == 0 {
        return Err(MediaInputError::Invalid);
    }
    Ok(MediaInputMetadata {
        media_type,
        dimensions: Some((image.width(), image.height())),
        duration_ms: None,
    })
}

fn duration_ms(units: u64, timescale: u32) -> Result<u64, MediaInputError> {
    if units == 0 || timescale == 0 {
        return Err(MediaInputError::Invalid);
    }
    let ms = (u128::from(units) * 1_000).div_ceil(u128::from(timescale));
    if ms > u128::from(MAX_TIMED_INPUT_DURATION_MS) {
        return Err(MediaInputError::LimitExceeded);
    }
    Ok(ms as u64)
}

#[cfg(test)]
mod tests;
