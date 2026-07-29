use serde_json::Value;

pub const OPENAI_GPT_IMAGE_2_CALCULATOR_SOURCE: &str =
    "https://developers.openai.com/api/docs/guides/image-generation#gpt-image-2-output-tokens";

const PIXEL_ALIGNMENT: u64 = 16;
const MIN_PIXELS: u64 = 655_360;
const MAX_PIXELS: u64 = 8_294_400;
const MAX_EDGE: u64 = 3_840;
const MAX_ASPECT_RATIO: u64 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GptImage2Quality {
    Low,
    Medium,
    High,
}

impl GptImage2Quality {
    fn long_edge_grid(self) -> u64 {
        match self {
            Self::Low => 16,
            Self::Medium => 48,
            Self::High => 96,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OfficialMeteringError {
    #[error("the official lookup dimensions are unavailable")]
    MissingDimensions,
    #[error("the official lookup quality is unsupported")]
    UnsupportedQuality,
    #[error("the official lookup size is invalid")]
    InvalidSize,
    #[error("the official lookup calculation overflowed")]
    Overflow,
}

pub fn gpt_image_2_output_tokens_from_dimensions(
    dimensions: &Value,
) -> Result<u64, OfficialMeteringError> {
    let object = dimensions
        .as_object()
        .ok_or(OfficialMeteringError::MissingDimensions)?;
    let quality = match object.get("quality").and_then(Value::as_str) {
        Some("low") => GptImage2Quality::Low,
        Some("medium") => GptImage2Quality::Medium,
        Some("high") => GptImage2Quality::High,
        Some(_) => return Err(OfficialMeteringError::UnsupportedQuality),
        None => return Err(OfficialMeteringError::MissingDimensions),
    };
    let size = object
        .get("size")
        .and_then(Value::as_str)
        .ok_or(OfficialMeteringError::MissingDimensions)?;
    let (width, height) = size
        .split_once('x')
        .ok_or(OfficialMeteringError::InvalidSize)?;
    let width = width
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OfficialMeteringError::InvalidSize)?;
    let height = height
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OfficialMeteringError::InvalidSize)?;
    gpt_image_2_output_tokens(width, height, quality)
}

pub fn gpt_image_2_output_tokens(
    width: u64,
    height: u64,
    quality: GptImage2Quality,
) -> Result<u64, OfficialMeteringError> {
    if width == 0 || height == 0 || width % PIXEL_ALIGNMENT != 0 || height % PIXEL_ALIGNMENT != 0 {
        return Err(OfficialMeteringError::InvalidSize);
    }
    let pixels = width
        .checked_mul(height)
        .ok_or(OfficialMeteringError::Overflow)?;
    let long_edge = width.max(height);
    let short_edge = width.min(height);
    if !(MIN_PIXELS..=MAX_PIXELS).contains(&pixels)
        || long_edge > MAX_EDGE
        || long_edge > short_edge.saturating_mul(MAX_ASPECT_RATIO)
    {
        return Err(OfficialMeteringError::InvalidSize);
    }

    let long_grid = quality.long_edge_grid();
    let scaled_short_numerator = long_grid
        .checked_mul(short_edge)
        .ok_or(OfficialMeteringError::Overflow)?;
    let short_grid = scaled_short_numerator
        .checked_add(long_edge / 2)
        .ok_or(OfficialMeteringError::Overflow)?
        / long_edge;
    let grid_area = long_grid
        .checked_mul(short_grid)
        .ok_or(OfficialMeteringError::Overflow)?;
    let pixel_factor = 2_000_000_u64
        .checked_add(pixels)
        .ok_or(OfficialMeteringError::Overflow)?;
    let numerator = grid_area
        .checked_mul(pixel_factor)
        .ok_or(OfficialMeteringError::Overflow)?;
    Ok(numerator
        .checked_add(4_000_000 - 1)
        .ok_or(OfficialMeteringError::Overflow)?
        / 4_000_000)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        GptImage2Quality, OfficialMeteringError, gpt_image_2_output_tokens,
        gpt_image_2_output_tokens_from_dimensions,
    };

    #[test]
    fn matches_the_official_calculator_for_documented_sizes() {
        let cases = [
            (1024, 1024, GptImage2Quality::Low, 196),
            (1024, 1024, GptImage2Quality::Medium, 1_756),
            (1024, 1024, GptImage2Quality::High, 7_024),
            (1024, 1536, GptImage2Quality::Low, 158),
            (1024, 1536, GptImage2Quality::Medium, 1_372),
            (1024, 1536, GptImage2Quality::High, 5_488),
            (1536, 1024, GptImage2Quality::High, 5_488),
        ];
        for (width, height, quality, expected) in cases {
            assert_eq!(
                gpt_image_2_output_tokens(width, height, quality),
                Ok(expected)
            );
        }
    }

    #[test]
    fn parses_frozen_request_dimensions() {
        assert_eq!(
            gpt_image_2_output_tokens_from_dimensions(
                &json!({"quality": "high", "size": "1024x1024"})
            ),
            Ok(7_024)
        );
    }

    #[test]
    fn rejects_auto_and_sizes_outside_the_official_contract() {
        assert_eq!(
            gpt_image_2_output_tokens_from_dimensions(
                &json!({"quality": "auto", "size": "1024x1024"})
            ),
            Err(OfficialMeteringError::UnsupportedQuality)
        );
        for (width, height) in [(1000, 1000), (512, 512), (3840, 1024), (3856, 1024)] {
            assert_eq!(
                gpt_image_2_output_tokens(width, height, GptImage2Quality::Low),
                Err(OfficialMeteringError::InvalidSize)
            );
        }
    }
}
