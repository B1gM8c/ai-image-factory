use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use image_api_contracts::xai::{
    XaiVideoGenerationCommandV1, XaiVideoGenerationCommandV2, XaiVideoWorkflow,
};
use image_provider_sdk::{ArtifactSink, ArtifactSinkError};
use reqwest::{Client, Url, redirect::Policy};
use sha2::{Digest, Sha256};

use crate::{ImageGatewayError, generator::InputImage};

use super::{super::admission::XaiVideoInputRoleV2, edit_input::decode_data_url_image};

pub(super) const MAX_VIDEO_INPUT_BYTES: usize = 32 * 1024 * 1024;
const HTTPS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HTTPS_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const GENERATED_VIDEO_TIMEOUT: Duration = Duration::from_secs(10 * 60);
#[cfg(debug_assertions)]
const TEST_GENERATED_VIDEO_URL_ENV: &str = "GATEWAY_TEST_GENERATED_VIDEO_URL";

#[derive(Debug)]
pub(crate) enum GeneratedVideoDownloadError {
    Unavailable,
    Invalid,
    Sink(ArtifactSinkError),
}

#[derive(Debug)]
pub(crate) struct GeneratedVideoDownload {
    pub(crate) byte_size: u64,
    pub(crate) sha256: [u8; 32],
}

/// Query results become deliverable only after bounded bytes reach the existing
/// private artifact stager. Input fetching keeps its separate, shorter budget.
pub(crate) async fn stream_generated_video<S: ArtifactSink>(
    source: &str,
    max_bytes: u64,
    sink: &mut S,
) -> Result<GeneratedVideoDownload, GeneratedVideoDownloadError> {
    use GeneratedVideoDownloadError::{Invalid, Unavailable};
    if max_bytes == 0 || max_bytes > crate::artifacts::MAX_ARTIFACT_BYTES {
        return Err(Invalid);
    }
    // Like the existing direct-edit fixture endpoint, this seam does not exist
    // in release builds. Real bytes still cross the normal bounded stream/sink.
    #[cfg(debug_assertions)]
    if let Some(endpoint) = std::env::var_os(TEST_GENERATED_VIDEO_URL_ENV) {
        let endpoint = test_generated_video_url(endpoint.to_str().ok_or(Invalid)?)?;
        let response = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .connect_timeout(HTTPS_CONNECT_TIMEOUT)
            .timeout(GENERATED_VIDEO_TIMEOUT)
            .build()
            .map_err(|_| Unavailable)?
            .get(endpoint)
            .send()
            .await
            .map_err(|_| Unavailable)?;
        return stream_generated_video_response(response, max_bytes, sink).await;
    }
    let url = validate_public_https_url("video", source)
        .await
        .map_err(|_| Invalid)?;
    let host = url.host_str().ok_or(Invalid)?.to_ascii_lowercase();
    let mut addresses = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, 443)]
    } else {
        tokio::time::timeout(
            HTTPS_CONNECT_TIMEOUT,
            tokio::net::lookup_host((host.as_str(), 443)),
        )
        .await
        .map_err(|_| Unavailable)?
        .map_err(|_| Unavailable)?
        .collect::<Vec<_>>()
    };
    addresses.sort();
    addresses.dedup();
    if addresses.is_empty() {
        return Err(Unavailable);
    }
    if addresses.iter().any(|address| !is_public_ip(address.ip())) {
        return Err(Invalid);
    }
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .connect_timeout(HTTPS_CONNECT_TIMEOUT)
        .timeout(GENERATED_VIDEO_TIMEOUT)
        .resolve_to_addrs(&host, &addresses)
        .build()
        .map_err(|_| Unavailable)?;
    let response = client.get(url).send().await.map_err(|_| Unavailable)?;
    stream_generated_video_response(response, max_bytes, sink).await
}

#[cfg(debug_assertions)]
fn test_generated_video_url(source: &str) -> Result<Url, GeneratedVideoDownloadError> {
    let url = Url::parse(source).map_err(|_| GeneratedVideoDownloadError::Invalid)?;
    let loopback = url.host_str().is_some_and(|host| {
        host.trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    });
    if url.scheme() != "http"
        || !loopback
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(GeneratedVideoDownloadError::Invalid);
    }
    Ok(url)
}

async fn stream_generated_video_response<S: ArtifactSink>(
    response: reqwest::Response,
    max_bytes: u64,
    sink: &mut S,
) -> Result<GeneratedVideoDownload, GeneratedVideoDownloadError> {
    use GeneratedVideoDownloadError::{Invalid, Sink, Unavailable};
    if response.status().is_redirection() {
        return Err(Invalid);
    }
    if !response.status().is_success() {
        // An expired CDN URL is refreshed by querying the same accepted task.
        return Err(Unavailable);
    }
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .unwrap_or("");
    if !matches!(content_type, "video/mp4" | "application/octet-stream")
        || response
            .content_length()
            .is_some_and(|length| length > max_bytes)
    {
        return Err(Invalid);
    }
    let mut total = 0_u64;
    let mut digest = Sha256::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| Unavailable)?;
        if chunk.is_empty() {
            continue;
        }
        total = total
            .checked_add(chunk.len() as u64)
            .filter(|size| *size <= max_bytes)
            .ok_or(Invalid)?;
        sink.write_chunk(&chunk).await.map_err(Sink)?;
        digest.update(&chunk);
    }
    if total == 0 {
        return Err(Unavailable);
    }
    Ok(GeneratedVideoDownload {
        byte_size: total,
        sha256: digest.finalize().into(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedVideoInput {
    pub(crate) filename: String,
    pub(crate) media_type: String,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Default)]
struct SourceCache {
    decoded: HashMap<String, DecodedVideoInput>,
}

pub(super) async fn decode_video_inputs_v2(
    command: &XaiVideoGenerationCommandV2,
    max_upload_bytes: usize,
) -> Result<Vec<DecodedVideoInput>, ImageGatewayError> {
    let limit = max_upload_bytes.min(MAX_VIDEO_INPUT_BYTES);
    let mut cache = SourceCache::default();
    let mut total_bytes = 0usize;
    let mut inputs = Vec::new();
    let sources = command
        .image
        .iter()
        .map(|image| (XaiVideoInputRoleV2::FirstFrame, 0_u8, image))
        .chain(
            command
                .last_frame
                .iter()
                .map(|image| (XaiVideoInputRoleV2::LastFrame, 0_u8, image)),
        )
        .chain(
            command
                .reference_images
                .iter()
                .enumerate()
                .map(|(index, image)| {
                    (
                        XaiVideoInputRoleV2::ReferenceImage,
                        u8::try_from(index).unwrap_or(u8::MAX),
                        image,
                    )
                }),
        )
        .chain(
            command
                .keyframes
                .iter()
                .enumerate()
                .map(|(index, keyframe)| {
                    (
                        XaiVideoInputRoleV2::Keyframe,
                        u8::try_from(index).unwrap_or(u8::MAX),
                        &keyframe.image,
                    )
                }),
        );

    for (role, role_index, image) in sources {
        let source = image
            .url
            .as_deref()
            .ok_or_else(|| unsupported_source(role.parameter()))?;
        let key = source.to_owned();
        let decoded = if let Some(decoded) = cache.decoded.get(&key) {
            decoded.clone()
        } else {
            let decoded = decode_source(role.parameter(), source, limit, &mut total_bytes).await?;
            cache.decoded.insert(key, decoded.clone());
            decoded
        };
        let extension = match decoded.media_type.as_str() {
            "image/png" => "png",
            "image/jpeg" => "jpg",
            "image/webp" => "webp",
            _ => return Err(invalid_image(role.parameter(), "unsupported image format")),
        };
        inputs.push(DecodedVideoInput {
            filename: format!("{}-{}.{}", role.as_str(), role_index, extension),
            ..decoded
        });
    }
    Ok(inputs)
}

/// Decode the original V1 input contract.  V1 intentionally remains limited
/// to inline data URLs; public HTTPS fetching is a V2-only capability.
pub(super) fn decode_video_inputs_v1(
    command: &XaiVideoGenerationCommandV1,
    max_upload_bytes: usize,
) -> Result<Vec<DecodedVideoInput>, ImageGatewayError> {
    let sources = match command.workflow() {
        XaiVideoWorkflow::TextToVideo => Vec::new(),
        XaiVideoWorkflow::ImageToVideo => vec![(
            "image".to_owned(),
            command
                .image
                .as_ref()
                .and_then(|image| image.url.as_deref())
                .ok_or_else(|| unsupported_source("image"))?,
        )],
        XaiVideoWorkflow::ReferenceToVideo => command
            .reference_images
            .iter()
            .enumerate()
            .map(|(index, image)| {
                image
                    .url
                    .as_deref()
                    .map(|url| (format!("reference_images[{index}]"), url))
                    .ok_or_else(|| unsupported_source("reference_images"))
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    let mut total_bytes = 0usize;
    sources
        .into_iter()
        .enumerate()
        .map(|(index, (param, source))| {
            let InputImage {
                content_type,
                bytes,
                ..
            } = decode_data_url_image(&param, source, false, &mut total_bytes, max_upload_bytes)?;
            let media_type = content_type.ok_or_else(|| {
                ImageGatewayError::internal("decoded video input has no media type")
            })?;
            let extension = match media_type.as_str() {
                "image/png" => "png",
                "image/jpeg" => "jpg",
                "image/webp" => "webp",
                _ => return Err(invalid_image(&param, "unsupported image format")),
            };
            Ok(DecodedVideoInput {
                filename: format!("input-{index}.{extension}"),
                media_type,
                bytes,
            })
        })
        .collect()
}

async fn decode_source(
    param: &str,
    source: &str,
    max_bytes: usize,
    total_bytes: &mut usize,
) -> Result<DecodedVideoInput, ImageGatewayError> {
    if source.starts_with("data:") {
        return decode_data_url(param, source, max_bytes, total_bytes);
    }
    if source.starts_with("https://") {
        return decode_https_url(param, source, max_bytes, total_bytes).await;
    }
    Err(unsupported_source(param))
}

fn decode_data_url(
    param: &str,
    source: &str,
    max_bytes: usize,
    total_bytes: &mut usize,
) -> Result<DecodedVideoInput, ImageGatewayError> {
    let decoded = decode_media_data_url(
        param,
        source,
        max_bytes,
        max_bytes,
        &["image/png", "image/jpeg", "image/webp"],
        total_bytes,
    )?;
    validate_signature(&decoded.media_type, &decoded.bytes, param)?;
    Ok(decoded)
}

/// Fetch bounded source bytes; callers must probe the body before accepting its MIME.
pub(crate) async fn decode_media_source(
    param: &str,
    source: &str,
    max_bytes: usize,
    max_total_bytes: usize,
    media_types: &[&str],
    total_bytes: &mut usize,
) -> Result<DecodedVideoInput, ImageGatewayError> {
    if source.starts_with("data:") {
        decode_media_data_url(
            param,
            source,
            max_bytes,
            max_total_bytes,
            media_types,
            total_bytes,
        )
    } else if source.starts_with("https://") {
        fetch_https_input(
            param,
            source,
            max_bytes,
            max_total_bytes,
            media_types,
            total_bytes,
        )
        .await
    } else {
        Err(unsupported_source(param))
    }
}

fn decode_media_data_url(
    param: &str,
    source: &str,
    max_bytes: usize,
    max_total_bytes: usize,
    media_types: &[&str],
    total_bytes: &mut usize,
) -> Result<DecodedVideoInput, ImageGatewayError> {
    let Some((metadata, encoded)) = source
        .strip_prefix("data:")
        .and_then(|value| value.split_once(','))
    else {
        return Err(invalid_image(param, "invalid image data URL"));
    };
    let mut parts = metadata.split(';');
    let media_type = parts.next().unwrap_or("");
    if !parts.any(|part| part.eq_ignore_ascii_case("base64")) {
        return Err(invalid_image(
            param,
            "image data URL must be base64 encoded",
        ));
    }
    if !media_types.contains(&media_type) {
        return Err(invalid_image(param, "unsupported image media type"));
    }
    let estimated = encoded.len().saturating_mul(3).saturating_div(4);
    if estimated > max_bytes || total_bytes.saturating_add(estimated) > max_total_bytes {
        return Err(ImageGatewayError::payload_too_large(
            "video input payload is too large",
        ));
    }
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| invalid_image(param, "invalid base64 image data"))?;
    add_total(total_bytes, bytes.len(), max_total_bytes)?;
    Ok(DecodedVideoInput {
        filename: String::new(),
        media_type: media_type.to_owned(),
        bytes,
    })
}

async fn decode_https_url(
    param: &str,
    source: &str,
    max_bytes: usize,
    total_bytes: &mut usize,
) -> Result<DecodedVideoInput, ImageGatewayError> {
    let decoded = fetch_https_input(
        param,
        source,
        max_bytes,
        max_bytes,
        &["image/png", "image/jpeg", "image/webp"],
        total_bytes,
    )
    .await?;
    validate_signature(&decoded.media_type, &decoded.bytes, param)?;
    Ok(decoded)
}

async fn fetch_https_input(
    param: &str,
    source: &str,
    max_bytes: usize,
    max_total_bytes: usize,
    media_types: &[&str],
    total_bytes: &mut usize,
) -> Result<DecodedVideoInput, ImageGatewayError> {
    let url = validate_public_https_url(param, source).await?;
    let host = url
        .host_str()
        .expect("validated URL host")
        .to_ascii_lowercase();
    let port = 443;
    let addresses = resolve_public_addresses(&host, port, param).await?;
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .connect_timeout(HTTPS_CONNECT_TIMEOUT)
        .timeout(HTTPS_REQUEST_TIMEOUT)
        .resolve_to_addrs(&host, &addresses)
        .build()
        .map_err(|_| ImageGatewayError::service_unavailable("video input client unavailable"))?;
    let response = client.get(url).send().await.map_err(|_| {
        ImageGatewayError::invalid_request(
            "video input URL could not be fetched",
            Some(param.to_owned()),
            "invalid_image_url",
        )
    })?;
    if response.status().is_redirection() {
        return Err(invalid_image(
            param,
            "video input redirects are not allowed",
        ));
    }
    if !response.status().is_success() {
        return Err(invalid_image(param, "video input URL returned an error"));
    }
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .unwrap_or("")
        .to_owned();
    if !media_types.contains(&content_type.as_str()) {
        return Err(invalid_image(
            param,
            "video input URL has unsupported media type",
        ));
    }
    if response.content_length().is_some_and(|length| {
        length > max_bytes as u64 || length > max_total_bytes.saturating_sub(*total_bytes) as u64
    }) {
        return Err(ImageGatewayError::payload_too_large(
            "video input payload is too large",
        ));
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| invalid_image(param, "video input URL could not be read"))?;
        if bytes.len().saturating_add(chunk.len()) > max_bytes
            || total_bytes
                .saturating_add(bytes.len())
                .saturating_add(chunk.len())
                > max_total_bytes
        {
            return Err(ImageGatewayError::payload_too_large(
                "video input payload is too large",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    add_total(total_bytes, bytes.len(), max_total_bytes)?;
    Ok(DecodedVideoInput {
        filename: String::new(),
        media_type: content_type,
        bytes,
    })
}

async fn validate_public_https_url(param: &str, source: &str) -> Result<Url, ImageGatewayError> {
    let url = Url::parse(source).map_err(|_| invalid_image(param, "video input URL is invalid"))?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.port().is_some_and(|port| port != 443)
        || url.host_str().is_none()
    {
        return Err(invalid_image(
            param,
            "video input URL must be public HTTPS without credentials or fragments",
        ));
    }
    Ok(url)
}

async fn resolve_public_addresses(
    host: &str,
    port: u16,
    param: &str,
) -> Result<Vec<SocketAddr>, ImageGatewayError> {
    let mut addresses = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        tokio::net::lookup_host((host, port))
            .await
            .map_err(|_| invalid_image(param, "video input host could not be resolved"))?
            .collect::<Vec<_>>()
    };
    addresses.sort();
    addresses.dedup();
    if addresses.is_empty() || addresses.iter().any(|address| !is_public_ip(address.ip())) {
        return Err(invalid_image(
            param,
            "video input host must resolve only to public addresses",
        ));
    }
    Ok(addresses)
}

fn add_total(total: &mut usize, bytes: usize, max_bytes: usize) -> Result<(), ImageGatewayError> {
    *total = total
        .checked_add(bytes)
        .ok_or_else(|| ImageGatewayError::payload_too_large("video input payload is too large"))?;
    if *total > max_bytes {
        return Err(ImageGatewayError::payload_too_large(
            "video input payload is too large",
        ));
    }
    Ok(())
}

fn validate_signature(
    media_type: &str,
    bytes: &[u8],
    param: &str,
) -> Result<(), ImageGatewayError> {
    let valid = match media_type {
        "image/png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg" => bytes.len() >= 3 && bytes[..3] == [0xff, 0xd8, 0xff],
        "image/webp" => bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP",
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid_image(
            param,
            "image bytes do not match declared media type",
        ))
    }
}

fn unsupported_source(param: &str) -> ImageGatewayError {
    ImageGatewayError::unsupported(
        param,
        "video input must use a base64 data URL or public HTTPS URL",
    )
}

fn invalid_image(param: &str, message: &str) -> ImageGatewayError {
    ImageGatewayError::invalid_request(message, Some(param.to_owned()), "invalid_image_url")
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, d] = ip.octets();
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_unspecified()
                || ip == Ipv4Addr::BROADCAST
                || matches!(
                    (a, b, c, d),
                    (0, _, _, _)
                        | (100, 64..=127, _, _)
                        | (192, 0, 0, _)
                        | (192, 0, 2, _)
                        | (198, 18..=19, _, _)
                        | (198, 51, 100, _)
                        | (203, 0, 113, _)
                        | (240..=255, _, _, _)
                ))
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_public_ip(IpAddr::V4(mapped));
            }
            let segments = ip.segments();
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || (segments[0] & 0xfe00) == 0xfc00
                || (segments[0] & 0xffc0) == 0xfe80
                || (segments[0] == 0x2001 && segments[1] == 0x0db8))
        }
    }
}

impl XaiVideoInputRoleV2 {
    fn parameter(self) -> &'static str {
        match self {
            Self::FirstFrame => "image",
            Self::LastFrame => "last_frame",
            Self::ReferenceImage => "reference_images",
            Self::Keyframe => "keyframes",
        }
    }
}

#[cfg(test)]
mod tests {
    use image_api_contracts::xai::{XaiVideoImageUrl, XaiVideoKeyframe};

    use super::*;

    #[test]
    fn data_url_requires_signature_and_media_type() {
        let mut total = 0;
        assert!(decode_data_url("image", "data:image/png;base64,AA==", 1024, &mut total).is_err());
        let png = "data:image/png;base64,iVBORw0KGgo=";
        assert!(decode_data_url("image", png, 1024, &mut total).is_ok());
    }

    #[tokio::test]
    async fn url_policy_rejects_credentials_fragments_ports_and_private_hosts() {
        assert!(
            validate_public_https_url("image", "http://example.com/a")
                .await
                .is_err()
        );
        assert!(
            validate_public_https_url("image", "https://user:pass@example.com/a")
                .await
                .is_err()
        );
        assert!(
            validate_public_https_url("image", "https://example.com:8443/a")
                .await
                .is_err()
        );
        assert!(
            resolve_public_addresses("127.0.0.1", 443, "image")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn duplicate_source_is_cached_by_exact_string() {
        let command = XaiVideoGenerationCommandV2 {
            schema_version: 2,
            operation: "videos.generations".to_owned(),
            aspect_ratio: None,
            duration: 8,
            generate_audio: true,
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,iVBORw0KGgo=".to_owned()),
            }),
            keyframes: vec![XaiVideoKeyframe {
                image: XaiVideoImageUrl {
                    file_id: None,
                    url: Some("data:image/png;base64,iVBORw0KGgo=".to_owned()),
                },
                timestamp_s: 2.0,
            }],
            last_frame: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,iVBORw0KGgo=".to_owned()),
            }),
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: None,
            reference_audios: Vec::new(),
            reference_images: vec![XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,iVBORw0KGgo=".to_owned()),
            }],
            resolution: image_api_contracts::xai::XaiVideoResolution::P480,
            storage_options: None,
            user: None,
        };
        let inputs = decode_video_inputs_v2(&command, 12).await.unwrap();
        assert_eq!(inputs.len(), 4);
        assert_eq!(inputs[0].filename, "first_frame-0.png");
        assert_eq!(inputs[1].filename, "last_frame-0.png");
        assert_eq!(inputs[2].filename, "reference_image-0.png");
        assert_eq!(inputs[3].filename, "keyframe-0.png");
        assert!(inputs.windows(2).all(|pair| pair[0].bytes == pair[1].bytes));
    }

    #[test]
    fn v1_decoder_preserves_inline_only_input_contract() {
        let command = XaiVideoGenerationCommandV1::from_request(
            image_api_contracts::xai::XaiVideoGenerationRequest {
                aspect_ratio: None,
                duration: Some(6),
                generate_audio: None,
                image: Some(XaiVideoImageUrl {
                    file_id: None,
                    url: Some("data:image/png;base64,iVBORw0KGgo=".to_owned()),
                }),
                keyframes: Vec::new(),
                last_frame: None,
                model: Some("grok-imagine-video-1.5-preview".to_owned()),
                output: None,
                prompt: None,
                reference_audios: Vec::new(),
                reference_images: Vec::new(),
                resolution: Some(image_api_contracts::xai::XaiVideoResolution::P480),
                storage_options: None,
                user: None,
            },
        )
        .unwrap();
        let inputs = decode_video_inputs_v1(&command, 1024).unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].filename, "input-0.png");
        assert!(
            decode_video_inputs_v1(
                &XaiVideoGenerationCommandV1 {
                    image: Some(XaiVideoImageUrl {
                        file_id: None,
                        url: Some("https://example.com/input.png".to_owned()),
                    }),
                    ..command
                },
                1024,
            )
            .is_err()
        );
    }

    #[test]
    fn v1_decoder_uses_configured_limit_and_handles_padded_base64() {
        let mut bytes = vec![0x89, b'P', b'N', b'G', b'\r', b'\n', b'\x1a', b'\n'];
        bytes.extend(std::iter::repeat_n(0_u8, 32 * 1024 * 1024 + 1));
        let command = XaiVideoGenerationCommandV1 {
            schema_version: 1,
            operation: "videos.generations".to_owned(),
            aspect_ratio: None,
            duration: 6,
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some(format!("data:image/png;base64,{}", STANDARD.encode(&bytes))),
            }),
            model: Some("grok-imagine-video-1.5-preview".to_owned()),
            output: None,
            prompt: None,
            reference_images: Vec::new(),
            resolution: image_api_contracts::xai::XaiVideoResolution::P480,
            storage_options: None,
            user: None,
        };
        let inputs = decode_video_inputs_v1(&command, 33 * 1024 * 1024).unwrap();
        assert_eq!(inputs[0].bytes.len(), 32 * 1024 * 1024 + 9);
        assert!(decode_video_inputs_v1(&command, 32 * 1024 * 1024).is_err());
    }
}

#[cfg(test)]
mod generated_video_tests {
    use super::*;
    use image_provider_sdk::DurableArtifactRef;
    use image_provider_test_support::RecordingArtifactSink;

    fn sink() -> RecordingArtifactSink {
        RecordingArtifactSink::new(
            DurableArtifactRef::new("test", "video-download").unwrap(),
            [0; 32],
        )
    }

    #[cfg(debug_assertions)]
    #[test]
    fn explicit_test_download_endpoint_accepts_only_uncredentialed_loopback_http() {
        for source in [
            "http://127.0.0.1:12345/fixture.mp4",
            "http://[::1]:12345/fixture.mp4",
        ] {
            assert!(test_generated_video_url(source).is_ok());
        }
        for source in [
            "http://example.com/fixture.mp4",
            "https://127.0.0.1/fixture.mp4",
            "http://user:password@127.0.0.1/fixture.mp4",
            "http://127.0.0.1/fixture.mp4#fragment",
            "http://169.254.169.254/fixture.mp4",
            "http://localhost/fixture.mp4",
        ] {
            assert!(test_generated_video_url(source).is_err());
        }
    }

    fn response(
        status: u16,
        media_type: &str,
        chunks: Vec<Result<Vec<u8>, std::io::Error>>,
    ) -> reqwest::Response {
        axum::http::Response::builder()
            .status(status)
            .header(reqwest::header::CONTENT_TYPE, media_type)
            .body(reqwest::Body::wrap_stream(futures_util::stream::iter(
                chunks,
            )))
            .unwrap()
            .into()
    }

    #[tokio::test]
    async fn generated_video_stream_hashes_bounded_chunks_without_finalizing_early() {
        let bytes = include_bytes!("../media_inputs/fixtures/blue.mp4");
        let chunks = bytes.chunks(37).map(|chunk| Ok(chunk.to_vec())).collect();
        let mut sink = sink();
        let downloaded = stream_generated_video_response(
            response(200, "video/mp4", chunks),
            bytes.len() as u64,
            &mut sink,
        )
        .await
        .unwrap();
        assert_eq!(sink.bytes(), bytes);
        assert!(sink.chunk_sizes().iter().all(|size| *size <= 37));
        assert_eq!(downloaded.byte_size, bytes.len() as u64);
        assert_eq!(downloaded.sha256, <[u8; 32]>::from(Sha256::digest(bytes)));
        assert_eq!(
            sink.finalize_count(),
            0,
            "only the verified file stager may finalize"
        );
    }

    #[tokio::test]
    async fn interrupted_or_expired_cdn_download_remains_retryable_not_deliverable() {
        let mut sink = sink();
        let error = stream_generated_video_response(
            response(
                200,
                "video/mp4",
                vec![
                    Ok(b"partial".to_vec()),
                    Err(std::io::Error::other("connection reset")),
                ],
            ),
            1024,
            &mut sink,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, GeneratedVideoDownloadError::Unavailable));
        assert_eq!(sink.bytes(), b"partial");
        assert_eq!(sink.finalize_count(), 0);
        for status in [403, 404, 429, 503] {
            let mut sink = self::sink();
            assert!(matches!(
                stream_generated_video_response(
                    response(status, "video/mp4", vec![]),
                    1024,
                    &mut sink
                )
                .await,
                Err(GeneratedVideoDownloadError::Unavailable)
            ));
            assert!(sink.bytes().is_empty());
        }
        let mut sink = self::sink();
        assert!(matches!(
            stream_generated_video_response(response(200, "video/mp4", vec![]), 1024, &mut sink)
                .await,
            Err(GeneratedVideoDownloadError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn generated_video_rejects_redirect_mime_and_streamed_size_overflow() {
        for (status, mime) in [(302, "video/mp4"), (200, "text/html")] {
            let mut sink = sink();
            assert!(matches!(
                stream_generated_video_response(
                    response(status, mime, vec![Ok(vec![1])]),
                    1024,
                    &mut sink
                )
                .await,
                Err(GeneratedVideoDownloadError::Invalid)
            ));
            assert!(sink.bytes().is_empty());
        }
        let mut sink = sink();
        assert!(matches!(
            stream_generated_video_response(
                response(200, "video/mp4", vec![Ok(vec![1; 3]), Ok(vec![2; 3])]),
                5,
                &mut sink
            )
            .await,
            Err(GeneratedVideoDownloadError::Invalid)
        ));
        assert_eq!(sink.bytes(), [1; 3]);
        assert_eq!(sink.finalize_count(), 0);
    }

    #[tokio::test]
    async fn generated_video_public_wrapper_rejects_private_or_credentialed_sources() {
        for source in [
            "http://example.com/v.mp4",
            "https://127.0.0.1/v.mp4",
            "https://user:password@example.com/v.mp4",
            "https://example.com:8443/v.mp4",
            "https://example.com/v.mp4#fragment",
        ] {
            let mut sink = sink();
            assert!(matches!(
                stream_generated_video(source, 1024, &mut sink).await,
                Err(GeneratedVideoDownloadError::Invalid)
            ));
            assert!(sink.bytes().is_empty());
        }
    }
}
