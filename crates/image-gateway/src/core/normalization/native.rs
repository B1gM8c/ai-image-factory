use std::{collections::BTreeSet, io::Read, num::NonZeroU64};

use quick_xml::{Reader, XmlVersion, events::Event};

use crate::ImageGatewayError;

const MAX_GIF_BYTES: usize = 32 * 1024 * 1024;
const MAX_SVG_BYTES: usize = 4 * 1024 * 1024;
const MAX_DIMENSION: u32 = 8192;
const MAX_PIXELS: u64 = 16 * 1024 * 1024;
const MAX_SVG_ELEMENTS: usize = 10_000;
const MAX_SVG_DEPTH: usize = 64;
const MAX_SVG_ATTRIBUTE_BYTES: usize = 64 * 1024;

pub(super) fn validate(bytes: &[u8], format: &str) -> Result<(), ImageGatewayError> {
    let valid = match format {
        "gif" => validate_gif(bytes),
        "svg" => validate_svg(bytes),
        _ => false,
    };
    valid.then_some(()).ok_or_else(|| {
        ImageGatewayError::backend("Provider returned invalid or unsafe native image output")
    })
}

fn validate_gif(bytes: &[u8]) -> bool {
    if bytes.len() > MAX_GIF_BYTES
        || !(bytes.starts_with(b"GIF89a") || bytes.starts_with(b"GIF87a"))
    {
        return false;
    }
    let mut options = gif::DecodeOptions::new();
    options.set_color_output(gif::ColorOutput::Indexed);
    options.check_frame_consistency(true);
    options.check_lzw_end_code(true);
    options.set_memory_limit(gif::MemoryLimit::Bytes(
        NonZeroU64::new(MAX_PIXELS).unwrap(),
    ));
    let Ok(mut decoder) = options.read_info(bytes) else {
        return false;
    };
    let (width, height) = (u32::from(decoder.width()), u32::from(decoder.height()));
    if !valid_dimensions(width, height) {
        return false;
    }
    let mut count = 0_u64;
    let mut duration_ms = 0_u64;
    loop {
        match decoder.next_frame_info() {
            Ok(Some(frame)) => {
                count += 1;
                let delay_ms = u64::from(frame.delay) * 10;
                duration_ms += delay_ms;
                if count > 16
                    || u64::from(width) * u64::from(height) * count > MAX_PIXELS
                    || !(20..=2000).contains(&delay_ms)
                    || duration_ms > 32_000
                    || !valid_dimensions(u32::from(frame.width), u32::from(frame.height))
                {
                    return false;
                }
                let mut pixels = vec![0; usize::from(frame.width) * usize::from(frame.height)];
                if decoder.read_into_buffer(&mut pixels).is_err() {
                    return false;
                }
            }
            Ok(None) => break,
            Err(_) => return false,
        }
    }
    let mut remaining = decoder.into_inner();
    count >= 2 && matches!(remaining.read(&mut [0_u8; 1]), Ok(0))
}

fn valid_dimensions(width: u32, height: u32) -> bool {
    width > 0
        && height > 0
        && width <= MAX_DIMENSION
        && height <= MAX_DIMENSION
        && u64::from(width) * u64::from(height) <= MAX_PIXELS
}

fn validate_svg(bytes: &[u8]) -> bool {
    if bytes.is_empty() || bytes.len() > MAX_SVG_BYTES {
        return false;
    }
    let Ok(xml) = std::str::from_utf8(bytes) else {
        return false;
    };
    let mut reader = Reader::from_str(xml);
    reader.config_mut().check_comments = true;
    let mut depth = 0_usize;
    let mut elements = 0_usize;
    let mut attributes = 0_usize;
    let mut root_seen = false;
    let mut declared = false;
    let mut ids = BTreeSet::new();
    let mut references = BTreeSet::new();
    loop {
        let Ok(event) = reader.read_event() else {
            return false;
        };
        match event {
            Event::Start(ref e) | Event::Empty(ref e) => {
                let root = !root_seen;
                if (depth == 0 && root_seen) || (root && e.name().as_ref() != "svg") {
                    return false;
                }
                root_seen = true;
                elements += 1;
                if elements > MAX_SVG_ELEMENTS
                    || depth >= MAX_SVG_DEPTH
                    || !safe_element(e.name().as_ref())
                {
                    return false;
                }
                let mut namespace = false;
                let mut root_width = None;
                let mut root_height = None;
                let mut viewbox = None;
                let mut local_attributes = 0_usize;
                for attribute in e.attributes() {
                    let Ok(attribute) = attribute else {
                        return false;
                    };
                    attributes += 1;
                    local_attributes += 1;
                    if attributes > 100_000
                        || local_attributes > 64
                        || attribute.value.len() > MAX_SVG_ATTRIBUTE_BYTES
                    {
                        return false;
                    }
                    let name = attribute.key.as_ref();
                    let Ok(value) = attribute.normalized_value(XmlVersion::Implicit1_0) else {
                        return false;
                    };
                    if name == "xmlns" {
                        if !root || value != "http://www.w3.org/2000/svg" {
                            return false;
                        }
                        namespace = true;
                    } else if name == "xmlns:xlink" {
                        if !root || value != "http://www.w3.org/1999/xlink" {
                            return false;
                        }
                    } else if !safe_attribute(name, &value, &mut ids, &mut references) {
                        return false;
                    }
                    if root {
                        match name {
                            "width" => match dimension(&value) {
                                Some(width) => root_width = Some(width),
                                None => return false,
                            },
                            "height" => match dimension(&value) {
                                Some(height) => root_height = Some(height),
                                None => return false,
                            },
                            "viewBox" => match viewbox_dimensions(&value) {
                                Some(dimensions) => viewbox = Some(dimensions),
                                None => return false,
                            },
                            _ => {}
                        }
                    }
                }
                if root {
                    if !namespace {
                        return false;
                    }
                    let dimensions = match (root_width, root_height) {
                        (Some(width), Some(height)) => Some((width, height)),
                        (None, None) => viewbox,
                        _ => None,
                    };
                    if dimensions.is_none_or(|(w, h)| !valid_dimensions(w, h)) {
                        return false;
                    }
                }
                if matches!(event, Event::Start(_)) {
                    depth += 1;
                }
            }
            Event::End(_) => {
                if depth == 0 {
                    return false;
                }
                depth -= 1;
            }
            Event::Text(ref text) => {
                if text.len() > MAX_SVG_ATTRIBUTE_BYTES
                    || (depth == 0 && !text.as_ref().trim().is_empty())
                {
                    return false;
                }
            }
            Event::Decl(declaration) => {
                if root_seen || declared || !declaration.version().is_ok_and(|v| v == "1.0") {
                    return false;
                }
                declared = true;
                if declaration.encoding().is_some_and(|encoding| {
                    !encoding.is_ok_and(|value| value.eq_ignore_ascii_case("utf-8"))
                }) {
                    return false;
                }
            }
            Event::GeneralRef(reference) => {
                if depth == 0 || !safe_entity(reference.as_ref()) {
                    return false;
                }
            }
            Event::Comment(_) => {}
            Event::Eof => return root_seen && depth == 0 && references.is_subset(&ids),
            _ => return false,
        }
    }
}

fn safe_element(name: &str) -> bool {
    matches!(
        name,
        "svg"
            | "g"
            | "defs"
            | "path"
            | "rect"
            | "circle"
            | "ellipse"
            | "line"
            | "polyline"
            | "polygon"
            | "text"
            | "tspan"
            | "title"
            | "desc"
            | "linearGradient"
            | "radialGradient"
            | "stop"
            | "clipPath"
    )
}

fn safe_attribute(
    name: &str,
    value: &str,
    ids: &mut BTreeSet<String>,
    references: &mut BTreeSet<String>,
) -> bool {
    if name == "id" {
        return valid_id(value) && ids.insert(value.to_owned());
    }
    if matches!(name, "href" | "xlink:href") {
        return fragment(value, references);
    }
    if !matches!(
        name,
        "width"
            | "height"
            | "viewBox"
            | "preserveAspectRatio"
            | "version"
            | "x"
            | "y"
            | "x1"
            | "y1"
            | "x2"
            | "y2"
            | "cx"
            | "cy"
            | "r"
            | "rx"
            | "ry"
            | "fx"
            | "fy"
            | "d"
            | "points"
            | "transform"
            | "fill"
            | "fill-rule"
            | "fill-opacity"
            | "stroke"
            | "stroke-width"
            | "stroke-linecap"
            | "stroke-linejoin"
            | "stroke-miterlimit"
            | "stroke-dasharray"
            | "stroke-dashoffset"
            | "stroke-opacity"
            | "opacity"
            | "clip-path"
            | "clip-rule"
            | "clipPathUnits"
            | "gradientUnits"
            | "gradientTransform"
            | "spreadMethod"
            | "offset"
            | "stop-color"
            | "stop-opacity"
            | "font-family"
            | "font-size"
            | "font-weight"
            | "font-style"
            | "text-anchor"
            | "dominant-baseline"
            | "alignment-baseline"
            | "letter-spacing"
            | "word-spacing"
            | "dx"
            | "dy"
            | "rotate"
            | "textLength"
            | "lengthAdjust"
    ) {
        return false;
    }
    if value.contains(['\\', ';', '{', '}', '@']) {
        return false;
    }
    let lowercase = value.to_ascii_lowercase();
    if lowercase.contains("url") {
        return matches!(name, "fill" | "stroke" | "clip-path")
            && value
                .strip_prefix("url(")
                .and_then(|v| v.strip_suffix(')'))
                .is_some_and(|v| fragment(v, references));
    }
    !lowercase.contains("var(") && !lowercase.contains("attr(")
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

fn fragment(value: &str, references: &mut BTreeSet<String>) -> bool {
    value
        .strip_prefix('#')
        .filter(|v| valid_id(v))
        .is_some_and(|v| {
            references.insert(v.to_owned());
            true
        })
}

fn dimension(value: &str) -> Option<u32> {
    let value = value
        .strip_suffix("px")
        .unwrap_or(value)
        .parse::<f64>()
        .ok()?;
    (value.is_finite() && value > 0.0 && value <= f64::from(MAX_DIMENSION))
        .then_some(value.ceil() as u32)
}

fn viewbox_dimensions(value: &str) -> Option<(u32, u32)> {
    let values = value
        .split(|c: char| c.is_ascii_whitespace() || c == ',')
        .filter(|v| !v.is_empty())
        .map(str::parse::<f64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    if values.len() != 4 || values.iter().any(|v| !v.is_finite()) {
        return None;
    }
    dimension(&values[2].to_string()).zip(dimension(&values[3].to_string()))
}

fn safe_entity(value: &str) -> bool {
    if matches!(value, "amp" | "lt" | "gt" | "apos" | "quot") {
        return true;
    }
    let codepoint = value
        .strip_prefix("#x")
        .and_then(|v| u32::from_str_radix(v, 16).ok())
        .or_else(|| value.strip_prefix('#').and_then(|v| v.parse::<u32>().ok()));
    codepoint
        .and_then(char::from_u32)
        .is_some_and(|c| !c.is_control() || matches!(c, '\t' | '\n' | '\r'))
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::{core::provider::GeneratedImage, generator::normalize_generated_images};

    fn gif_bytes(count: usize, delay: u16) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut encoder =
                gif::Encoder::new(&mut bytes, 2, 2, &[0, 0, 0, 255, 255, 255]).unwrap();
            for _ in 0..count {
                encoder
                    .write_frame(&gif::Frame {
                        width: 2,
                        height: 2,
                        delay,
                        buffer: Cow::Borrowed(&[0, 1, 1, 0]),
                        ..Default::default()
                    })
                    .unwrap();
            }
        }
        bytes
    }

    #[test]
    fn native_gif_preserves_all_original_bytes() {
        for count in [2, 16] {
            let bytes = gif_bytes(count, 20);
            let normalized = normalize_generated_images(
                vec![GeneratedImage {
                    bytes: bytes.clone(),
                }],
                "gif",
                None,
            )
            .unwrap();
            assert_eq!(normalized[0].bytes, bytes);
        }
    }

    #[test]
    fn rejects_gif_frame_and_time_budget_violations() {
        for bytes in [
            gif_bytes(1, 20),
            gif_bytes(17, 20),
            gif_bytes(2, 0),
            gif_bytes(2, 201),
        ] {
            assert!(!validate_gif(&bytes));
        }
    }

    #[test]
    fn rejects_truncated_gif_and_trailing_bytes() {
        let bytes = gif_bytes(2, 20);
        for end in [bytes.len() - 1, bytes.len() - 3, bytes.len() / 2] {
            assert!(!validate_gif(&bytes[..end]));
        }
        let mut extra = bytes;
        extra.extend_from_slice(b"<script>alert(1)</script>");
        assert!(!validate_gif(&extra));
    }

    #[test]
    fn rejects_gif_frame_outside_the_logical_canvas() {
        let mut bytes = gif_bytes(2, 20);
        let descriptor = bytes.iter().position(|b| *b == 0x2c).unwrap();
        bytes[descriptor + 1] = 2;
        assert!(!validate_gif(&bytes));
    }

    #[test]
    fn static_svg_preserves_original_bytes() {
        let bytes = br##"<?xml version="1.0" encoding="UTF-8"?><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><defs><linearGradient id="gold"><stop offset="0" stop-color="#fff"/></linearGradient><clipPath id="c"><rect width="32" height="32"/></clipPath></defs><path d="M0 0 L32 32" fill="url(#gold)" clip-path="url(#c)"/><text x="1" y="20">A &amp; B</text></svg>"##.to_vec();
        let normalized = normalize_generated_images(
            vec![GeneratedImage {
                bytes: bytes.clone(),
            }],
            "svg",
            None,
        )
        .unwrap();
        assert_eq!(normalized[0].bytes, bytes);
    }

    #[test]
    fn rejects_active_svg_and_external_references() {
        for contents in [
            r#"<script>alert(1)</script>"#,
            r#"<image href="https://example.com/a.png"/>"#,
            r#"<foreignObject/>"#,
            r##"<use href="#a"/>"##,
            r#"<path onload="alert(1)"/>"#,
            r#"<path style="fill:red"/>"#,
            r#"<path fill="url(https://example.com/a)"/>"#,
            r#"<path fill="url(&#104;ttps://example.com/a)"/>"#,
            r#"<path xml:base="https://example.com"/>"#,
            r#"<path xmlns="http://www.w3.org/1999/xhtml"/>"#,
            r#"<x:path xmlns:x="http://www.w3.org/2000/svg"/>"#,
            r#"<animate attributeName="x"/>"#,
        ] {
            let svg = format!(
                r#"<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32">{contents}</svg>"#
            );
            assert!(!validate_svg(svg.as_bytes()), "accepted {contents}");
        }
    }

    #[test]
    fn rejects_invalid_xml_or_unbounded_svg() {
        for svg in [
            r#"<!DOCTYPE svg [<!ENTITY x SYSTEM "file:///etc/passwd">]><svg xmlns="http://www.w3.org/2000/svg" width="32" height="32">&x;</svg>"#,
            r#"<?xml-stylesheet href="https://example.com/a.css"?><svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"/>"#,
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="99999" height="32"/>"#,
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"><path></svg>"#,
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"/><svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"/>"#,
        ] {
            assert!(!validate_svg(svg.as_bytes()));
        }
        let svg = format!(
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32">{}{} </svg>"#,
            "<g>".repeat(65),
            "</g>".repeat(65)
        );
        assert!(!validate_svg(svg.as_bytes()));
    }

    #[test]
    fn viewbox_does_not_override_invalid_or_unbounded_intrinsic_dimensions() {
        for attributes in [
            r#"width="1000000000" height="1000000000" viewBox="0 0 32 32""#,
            r#"width="NaN" height="32" viewBox="0 0 32 32""#,
            r#"width="auto" height="32" viewBox="0 0 32 32""#,
            r#"width="8192" viewBox="0 0 32 32""#,
            r#"width="8192" height="8192" viewBox="0 0 32 32""#,
            r#"width="32" height="32" viewBox="0 0 NaN 32""#,
        ] {
            let svg = format!(r#"<svg xmlns="http://www.w3.org/2000/svg" {attributes}/>"#);
            assert!(!validate_svg(svg.as_bytes()), "accepted {attributes}");
        }
    }

    #[test]
    fn rejects_invalid_xml_comment() {
        assert!(!validate_svg(
            br#"<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"><!-- invalid -- comment --></svg>"#,
        ));
    }

    #[test]
    fn native_formats_do_not_accept_compression_overrides() {
        assert!(
            normalize_generated_images(
                vec![GeneratedImage {
                    bytes: gif_bytes(2, 20)
                }],
                "gif",
                Some(50)
            )
            .is_err()
        );
    }
}
