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
use reqwest::{Client, Url, redirect::Policy};

use crate::ImageGatewayError;

use super::super::admission::XaiVideoInputRoleV2;

pub(super) const MAX_VIDEO_INPUT_BYTES: usize = 32 * 1024 * 1024;
const HTTPS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HTTPS_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DecodedVideoInput {
    pub(super) filename: String,
    pub(super) media_type: String,
    pub(super) bytes: Vec<u8>,
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
    let limit = max_upload_bytes.min(MAX_VIDEO_INPUT_BYTES);
    let mut total_bytes = 0usize;
    sources
        .into_iter()
        .enumerate()
        .map(|(index, (param, source))| {
            let decoded = decode_data_url(&param, source, limit, &mut total_bytes)?;
            let extension = match decoded.media_type.as_str() {
                "image/png" => "png",
                "image/jpeg" => "jpg",
                "image/webp" => "webp",
                _ => return Err(invalid_image(&param, "unsupported image format")),
            };
            Ok(DecodedVideoInput {
                filename: format!("input-{index}.{extension}"),
                ..decoded
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
    if !matches!(media_type, "image/png" | "image/jpeg" | "image/webp") {
        return Err(invalid_image(param, "unsupported image media type"));
    }
    let estimated = encoded.len().saturating_mul(3).saturating_div(4);
    if estimated > max_bytes || total_bytes.saturating_add(estimated) > max_bytes {
        return Err(ImageGatewayError::payload_too_large(
            "video input payload is too large",
        ));
    }
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| invalid_image(param, "invalid base64 image data"))?;
    add_total(total_bytes, bytes.len(), max_bytes)?;
    validate_signature(media_type, &bytes, param)?;
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
    if !matches!(
        content_type.as_str(),
        "image/png" | "image/jpeg" | "image/webp"
    ) {
        return Err(invalid_image(
            param,
            "video input URL has unsupported media type",
        ));
    }
    if response.content_length().is_some_and(|length| {
        length > max_bytes as u64 || length > max_bytes.saturating_sub(*total_bytes) as u64
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
                > max_bytes
        {
            return Err(ImageGatewayError::payload_too_large(
                "video input payload is too large",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    add_total(total_bytes, bytes.len(), max_bytes)?;
    validate_signature(&content_type, &bytes, param)?;
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
        }
    }
}

#[cfg(test)]
mod tests {
    use image_api_contracts::xai::XaiVideoImageUrl;

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
            last_frame: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,iVBORw0KGgo=".to_owned()),
            }),
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: None,
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: image_api_contracts::xai::XaiVideoResolution::P480,
            storage_options: None,
            user: None,
        };
        let inputs = decode_video_inputs_v2(&command, 12).await.unwrap();
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].bytes, inputs[1].bytes);
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
}
