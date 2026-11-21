use std::{borrow::Cow, path::Path, time::Duration};

#[cfg(any(debug_assertions, test))]
use std::net::IpAddr;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode, header::HeaderMap, redirect::Policy};
use serde::{Deserialize, Serialize, de::IgnoredAny};
use sha2::{Digest, Sha256};

use crate::{
    ImageGatewayError,
    codex_app_server::matches_codex_ip_denial,
    core::provider::{GeneratedImage, InputImage},
};

const CODEX_IMAGE_EDITS_URL: &str = "https://chatgpt.com/backend-api/codex/images/edits";
#[cfg(debug_assertions)]
const TEST_CODEX_IMAGE_EDITS_URL_ENV: &str = "GATEWAY_TEST_CODEX_IMAGE_EDITS_URL";
const CODEX_ORIGINATOR: &str = "codex_cli_rs";
const CODEX_USER_AGENT: &str = "codex_cli_rs/0.145.0";
const MAX_AUTH_BYTES: u64 = 64 * 1024;
const MAX_RESPONSE_BYTES: usize = 96 * 1024 * 1024;
const MAX_ERROR_INSPECTION_BYTES: usize = 64 * 1024;
const MAX_OUTPUT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Deserialize)]
struct AuthFile {
    tokens: Option<AuthTokens>,
}

#[derive(Deserialize)]
struct AuthTokens {
    access_token: String,
    account_id: Option<String>,
}

struct DirectAuth {
    access_token: String,
    account_id: Option<String>,
}

pub(crate) struct DirectEditFailure {
    error: ImageGatewayError,
    error_code: &'static str,
    http_status: Option<u16>,
    upstream_error_code: Option<&'static str>,
    upstream_request_id_sha256: Option<String>,
    outcome_uncertain: bool,
}

struct HttpRejection {
    failure: DirectEditFailure,
    body_shape: &'static str,
    error_type: Option<&'static str>,
}

pub(crate) struct DirectEditParameters<'a> {
    pub(crate) model: &'a str,
    pub(crate) prompt: &'a str,
    pub(crate) background: &'a str,
    pub(crate) quality: &'a str,
    pub(crate) size: &'a str,
    pub(crate) output_index: u32,
}

pub(super) struct DirectEditBatchParameters<'a> {
    pub(super) model: &'a str,
    pub(super) prompt: &'a str,
    pub(super) n: u32,
}

impl DirectEditFailure {
    pub(crate) fn error_code(&self) -> &'static str {
        self.error_code
    }

    pub(crate) fn http_status(&self) -> Option<u16> {
        self.http_status
    }

    pub(crate) fn upstream_error_code(&self) -> Option<&'static str> {
        self.upstream_error_code
    }

    pub(crate) fn upstream_request_id_sha256(&self) -> Option<&str> {
        self.upstream_request_id_sha256.as_deref()
    }

    pub(crate) fn is_authentication_rejection(&self) -> bool {
        self.error_code == "codex_authentication_rejected"
            && self.http_status == Some(StatusCode::UNAUTHORIZED.as_u16())
    }

    pub(crate) fn outcome_uncertain(&self) -> bool {
        self.outcome_uncertain
    }

    fn into_gateway_error(self) -> ImageGatewayError {
        self.error
    }

    fn with_upstream_response(mut self, status: StatusCode, request_id: Option<String>) -> Self {
        self.http_status = Some(status.as_u16());
        self.upstream_request_id_sha256 = request_id;
        self
    }
}

#[derive(Serialize)]
struct EditRequest<'a> {
    images: Vec<ImageUrl>,
    prompt: &'a str,
    background: &'a str,
    model: &'a str,
    n: u32,
    quality: &'a str,
    size: &'a str,
}

#[derive(Serialize)]
struct ImageUrl {
    image_url: String,
}

#[derive(Deserialize)]
struct EditResponse {
    data: Vec<ImageData>,
}

#[derive(Deserialize)]
struct ImageData {
    b64_json: String,
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    error: Option<ErrorBody>,
}

#[derive(Deserialize)]
struct ErrorBody {
    code: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RejectionEnvelope {
    Object { error: Option<RejectionBody> },
    Other(IgnoredAny),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RejectionBody {
    Object {
        #[serde(rename = "code")]
        _code: Option<String>,
        #[serde(rename = "type")]
        error_type: Option<String>,
        message: Option<String>,
    },
}

pub(super) async fn edit(
    auth_home: &Path,
    images: &[InputImage],
    mask: Option<&InputImage>,
    parameters: DirectEditBatchParameters<'_>,
    timeout: Duration,
) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
    let endpoint = image_edits_endpoint()?;
    edit_at(&endpoint, auth_home, images, mask, parameters, timeout).await
}

pub(crate) async fn edit_one(
    auth_home: &Path,
    images: &[InputImage],
    mask: Option<&InputImage>,
    parameters: DirectEditParameters<'_>,
    timeout: Duration,
) -> Result<GeneratedImage, DirectEditFailure> {
    let endpoint = image_edits_endpoint().map_err(local_unavailable)?;
    edit_one_at(&endpoint, auth_home, images, mask, parameters, timeout).await
}

fn image_edits_endpoint() -> Result<Cow<'static, str>, ImageGatewayError> {
    #[cfg(debug_assertions)]
    if let Ok(endpoint) = std::env::var(TEST_CODEX_IMAGE_EDITS_URL_ENV) {
        validate_test_endpoint(&endpoint)?;
        return Ok(Cow::Owned(endpoint));
    }
    Ok(Cow::Borrowed(CODEX_IMAGE_EDITS_URL))
}

#[cfg(any(debug_assertions, test))]
fn validate_test_endpoint(endpoint: &str) -> Result<(), ImageGatewayError> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| unavailable())?;
    if url.scheme() != "http" || url.username() != "" || url.password().is_some() {
        return Err(unavailable());
    }
    let host = url.host_str().ok_or_else(unavailable)?;
    let ip_host = host.trim_start_matches('[').trim_end_matches(']');
    let loopback = host == "localhost"
        || ip_host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if !loopback {
        return Err(unavailable());
    }
    Ok(())
}

pub(super) async fn edit_at(
    endpoint: &str,
    auth_home: &Path,
    images: &[InputImage],
    mask: Option<&InputImage>,
    parameters: DirectEditBatchParameters<'_>,
    timeout: Duration,
) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
    if parameters.n == 0 {
        return Err(invalid_response());
    }
    let mut total = 0usize;
    let mut outputs = Vec::with_capacity(parameters.n as usize);
    for output_index in 0..parameters.n {
        let image = edit_one_at(
            endpoint,
            auth_home,
            images,
            mask,
            DirectEditParameters {
                model: parameters.model,
                prompt: parameters.prompt,
                background: "auto",
                quality: "auto",
                size: "auto",
                output_index,
            },
            timeout,
        )
        .await
        .map_err(DirectEditFailure::into_gateway_error)?;
        total = total
            .checked_add(image.bytes.len())
            .ok_or_else(invalid_response)?;
        if total > MAX_OUTPUT_BYTES {
            return Err(invalid_response());
        }
        outputs.push(image);
    }
    Ok(outputs)
}

pub(crate) async fn edit_one_at(
    endpoint: &str,
    auth_home: &Path,
    images: &[InputImage],
    mask: Option<&InputImage>,
    parameters: DirectEditParameters<'_>,
    timeout: Duration,
) -> Result<GeneratedImage, DirectEditFailure> {
    let auth = read_auth(auth_home).map_err(local_credentials_unavailable)?;
    let payload = edit_request(images, mask, &parameters).map_err(local_invalid_request)?;
    let client = Client::builder()
        .redirect(Policy::none())
        .build()
        .map_err(|_| local_unavailable(unavailable()))?;
    let mut outputs = post_one(
        &client,
        endpoint,
        &auth,
        &payload,
        timeout,
        parameters.output_index,
    )
    .await?;
    outputs.pop().ok_or_else(direct_invalid_response)
}

async fn post_one(
    client: &Client,
    endpoint: &str,
    auth: &DirectAuth,
    payload: &EditRequest<'_>,
    timeout: Duration,
    output_index: u32,
) -> Result<Vec<GeneratedImage>, DirectEditFailure> {
    let mut request = client
        .post(endpoint)
        .bearer_auth(&auth.access_token)
        .header("originator", CODEX_ORIGINATOR)
        .header(reqwest::header::USER_AGENT, CODEX_USER_AGENT)
        .timeout(timeout)
        .json(payload);
    if let Some(account_id) = &auth.account_id {
        request = request.header("ChatGPT-Account-ID", account_id);
    }
    let response = request.send().await.map_err(|error| {
        if error.is_connect() {
            local_unavailable(unavailable())
        } else {
            uncertain_transport()
        }
    })?;
    let status = response.status();
    let request_id_sha256 = upstream_request_id_sha256(response.headers());
    let request_id_state = match request_id_sha256.as_ref() {
        Some(_) => "recognized",
        None if response.headers().contains_key("x-request-id") => "present_unusable",
        None => "absent",
    };
    let body = read_bounded_body(response).await.map_err(|_| {
        uncertain_transport().with_upstream_response(status, request_id_sha256.clone())
    })?;
    if !status.is_success() {
        let rejection = map_http_error(status, &body);
        let failure = rejection
            .failure
            .with_upstream_response(status, request_id_sha256);
        tracing::warn!(
            codex.edit.output_index = output_index,
            http.status = status.as_u16(),
            upstream.response_body_shape = rejection.body_shape,
            upstream.error_type = rejection.error_type.unwrap_or("none"),
            upstream.code = failure.upstream_error_code().unwrap_or("none"),
            upstream.request_id_state = request_id_state,
            upstream.request_id_sha256 = failure.upstream_request_id_sha256().unwrap_or("none"),
            "Codex image edit upstream rejected request"
        );
        return Err(failure);
    }
    decode_outputs(&body, 1)
        .map_err(|_| direct_invalid_response().with_upstream_response(status, request_id_sha256))
}

fn edit_request<'a>(
    images: &[InputImage],
    mask: Option<&InputImage>,
    parameters: &'a DirectEditParameters<'a>,
) -> Result<EditRequest<'a>, ImageGatewayError> {
    let mut encoded = Vec::with_capacity(images.len() + usize::from(mask.is_some()));
    for image in images.iter().chain(mask) {
        encoded.push(ImageUrl {
            image_url: data_url(image)?,
        });
    }
    Ok(EditRequest {
        images: encoded,
        prompt: parameters.prompt,
        background: parameters.background,
        model: parameters.model,
        n: 1,
        quality: parameters.quality,
        size: parameters.size,
    })
}

fn data_url(image: &InputImage) -> Result<String, ImageGatewayError> {
    let mime = match ::image::guess_format(&image.bytes) {
        Ok(::image::ImageFormat::Png) => "image/png",
        Ok(::image::ImageFormat::Jpeg) => "image/jpeg",
        Ok(::image::ImageFormat::WebP) => "image/webp",
        _ => {
            return Err(ImageGatewayError::invalid_request(
                "Unsupported input image format",
                Some("image".to_string()),
                "invalid_image_format",
            ));
        }
    };
    Ok(format!(
        "data:{mime};base64,{}",
        STANDARD.encode(&image.bytes)
    ))
}

fn read_auth(home: &Path) -> Result<DirectAuth, ImageGatewayError> {
    let path = home.join("auth.json");
    let metadata = std::fs::metadata(&path).map_err(|_| credentials_unavailable())?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_AUTH_BYTES {
        return Err(credentials_unavailable());
    }
    let auth: AuthFile =
        serde_json::from_slice(&std::fs::read(path).map_err(|_| credentials_unavailable())?)
            .map_err(|_| credentials_unavailable())?;
    let tokens = auth.tokens.ok_or_else(credentials_unavailable)?;
    if tokens.access_token.trim().is_empty() || tokens.access_token.len() > 16 * 1024 {
        return Err(credentials_unavailable());
    }
    if tokens
        .account_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty() || value.len() > 512)
    {
        return Err(credentials_unavailable());
    }
    Ok(DirectAuth {
        access_token: tokens.access_token,
        account_id: tokens.account_id,
    })
}

async fn read_bounded_body(response: reqwest::Response) -> Result<Vec<u8>, ImageGatewayError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(invalid_response());
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| unavailable())?;
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(invalid_response());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn decode_outputs(body: &[u8], expected: u32) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
    let response: EditResponse = serde_json::from_slice(body).map_err(|_| invalid_response())?;
    if response.data.len() != expected as usize {
        return Err(invalid_response());
    }
    let mut total = 0usize;
    let mut outputs = Vec::with_capacity(response.data.len());
    for item in response.data {
        let bytes = STANDARD
            .decode(item.b64_json.as_bytes())
            .map_err(|_| invalid_response())?;
        if bytes.is_empty() {
            return Err(invalid_response());
        }
        total = total
            .checked_add(bytes.len())
            .ok_or_else(invalid_response)?;
        if total > MAX_OUTPUT_BYTES {
            return Err(invalid_response());
        }
        outputs.push(GeneratedImage { bytes });
    }
    Ok(outputs)
}

fn map_http_error(status: StatusCode, body: &[u8]) -> HttpRejection {
    let raw_code = structured_error_code(body).map(|code| code.to_ascii_lowercase());
    let upstream_error_code = safe_upstream_error_code(raw_code.as_deref());
    let oversized = body.len() > MAX_ERROR_INSPECTION_BYTES;
    let json = (!oversized)
        .then(|| serde_json::from_slice::<RejectionEnvelope>(body).ok())
        .flatten();
    // Derived object fields reject duplicates before any IP-denial signal is used.
    let (body_shape, error_type, ip_denial) = match json.as_ref() {
        Some(RejectionEnvelope::Object {
            error:
                Some(RejectionBody::Object {
                    error_type,
                    message,
                    ..
                }),
        }) => (
            "json_error",
            safe_upstream_error_code(error_type.as_deref()),
            matches!(raw_code.as_deref(), Some("ip_not_allowed" | "ip_rejected"))
                || matches_codex_ip_denial(error_type.as_deref(), message.as_deref()),
        ),
        Some(_) => ("json_other", None, false),
        None if oversized => ("oversized", None, false),
        None => {
            let shape = match body.iter().find(|byte| !byte.is_ascii_whitespace()) {
                None => "empty",
                Some(b'<') => "markup_like",
                Some(_) => "other",
            };
            (shape, None, false)
        }
    };
    let (error, error_code) = match (status, raw_code.as_deref()) {
        (
            _,
            Some("content_policy" | "content_policy_violation" | "moderation_blocked" | "safety"),
        ) => (
            ImageGatewayError::content_policy_rejected(),
            "content_policy_rejected",
        ),
        (StatusCode::UNAUTHORIZED, _) if ip_denial => {
            let error_code = "codex_image_tool_ip_not_authorized";
            (
                ImageGatewayError::codex_app_server_failure(error_code),
                error_code,
            )
        }
        (StatusCode::UNAUTHORIZED, _) => {
            (credentials_unavailable(), "codex_authentication_rejected")
        }
        (StatusCode::TOO_MANY_REQUESTS, _) => (
            ImageGatewayError::queue_overloaded(),
            "codex_image_edit_rate_limited",
        ),
        (status, _) if status.is_server_error() => {
            (unavailable(), "codex_image_edit_upstream_unavailable")
        }
        _ => (
            ImageGatewayError::backend("Codex image edit request was rejected"),
            "codex_image_edit_rejected",
        ),
    };
    HttpRejection {
        failure: DirectEditFailure {
            error,
            error_code,
            http_status: Some(status.as_u16()),
            upstream_error_code,
            upstream_request_id_sha256: None,
            outcome_uncertain: false,
        },
        body_shape,
        error_type,
    }
}

fn local_credentials_unavailable(error: ImageGatewayError) -> DirectEditFailure {
    DirectEditFailure {
        error,
        error_code: "codex_credentials_unavailable",
        http_status: None,
        upstream_error_code: None,
        upstream_request_id_sha256: None,
        outcome_uncertain: false,
    }
}

fn local_invalid_request(error: ImageGatewayError) -> DirectEditFailure {
    DirectEditFailure {
        error,
        error_code: "codex_image_edit_request_invalid",
        http_status: None,
        upstream_error_code: None,
        upstream_request_id_sha256: None,
        outcome_uncertain: false,
    }
}

fn local_unavailable(error: ImageGatewayError) -> DirectEditFailure {
    DirectEditFailure {
        error,
        error_code: "codex_image_edit_upstream_unavailable",
        http_status: None,
        upstream_error_code: None,
        upstream_request_id_sha256: None,
        outcome_uncertain: false,
    }
}

fn uncertain_transport() -> DirectEditFailure {
    DirectEditFailure {
        error: unavailable(),
        error_code: "codex_image_edit_outcome_unknown",
        http_status: None,
        upstream_error_code: None,
        upstream_request_id_sha256: None,
        outcome_uncertain: true,
    }
}

fn direct_invalid_response() -> DirectEditFailure {
    DirectEditFailure {
        error: invalid_response(),
        error_code: "codex_image_edit_invalid_response",
        http_status: Some(StatusCode::OK.as_u16()),
        upstream_error_code: None,
        upstream_request_id_sha256: None,
        outcome_uncertain: true,
    }
}

fn structured_error_code(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<ErrorEnvelope>(body)
        .ok()
        .and_then(|value| value.error)
        .and_then(|error| error.code)
}

fn safe_upstream_error_code(value: Option<&str>) -> Option<&'static str> {
    // A token-shaped string can still contain a credential or customer data.
    const ALLOWED: &[&str] = &[
        "access_token_expired",
        "account_deactivated",
        "account_not_found",
        "authentication_error",
        "content_policy",
        "content_policy_violation",
        "expired_token",
        "insufficient_quota",
        "internal_server_error",
        "invalid_api_key",
        "invalid_authentication",
        "invalid_request_error",
        "invalid_token",
        "ip_not_allowed",
        "ip_rejected",
        "moderation_blocked",
        "rate_limit_exceeded",
        "safety",
        "server_error",
        "service_unavailable",
        "token_expired",
        "token_invalidated",
        "unauthorized",
        "unsupported_country_region_territory",
    ];
    let value = value?;
    Some(
        ALLOWED
            .iter()
            .copied()
            .find(|code| *code == value)
            .unwrap_or("redacted"),
    )
}

fn upstream_request_id_sha256(headers: &HeaderMap) -> Option<String> {
    let mut values = headers.get_all("x-request-id").iter();
    let request_id = values.next()?.to_str().ok()?;
    let suffix = request_id.strip_prefix("req_")?;
    if values.next().is_some()
        || suffix.len() != 32
        || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(hex::encode(Sha256::digest(request_id.as_bytes())))
}

fn credentials_unavailable() -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Codex credentials are unavailable")
}

fn unavailable() -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Codex image edit service is unavailable")
}

fn invalid_response() -> ImageGatewayError {
    ImageGatewayError::backend("Codex image edit response is invalid")
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
    use serde_json::Value;
    use tempfile::TempDir;

    use super::*;

    #[derive(Default)]
    struct ObservedRequest {
        authorized: bool,
        account_bound: bool,
        originator_bound: bool,
        request_count: usize,
        active_requests: usize,
        max_active_requests: usize,
        upstream_counts: Vec<u64>,
        body: Option<Value>,
    }

    fn png() -> InputImage {
        InputImage {
            filename: Some("input.png".to_string()),
            content_type: Some("image/png".to_string()),
            bytes: vec![
                137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0,
                1, 8, 6, 0, 0, 0, 31, 21, 196, 137,
            ],
        }
    }

    #[test]
    fn direct_edit_payload_uses_the_official_single_output_contract() {
        let parameters = DirectEditParameters {
            model: "gpt-image-2.5-sunburst",
            prompt: "edit",
            background: "auto",
            quality: "auto",
            size: "auto",
            output_index: 0,
        };
        let request = edit_request(&[png()], None, &parameters).unwrap();
        let value = serde_json::to_value(request).unwrap();
        assert_eq!(value["model"], "gpt-image-2.5-sunburst");
        assert_eq!(value["n"], 1);
        assert_eq!(value["size"], "auto");
        assert_eq!(value["quality"], "auto");
        assert_eq!(value["images"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn direct_edit_requires_exact_output_count() {
        let one = STANDARD.encode(b"one");
        let body = serde_json::to_vec(&serde_json::json!({
            "data": [{"b64_json": one}]
        }))
        .unwrap();
        assert!(decode_outputs(&body, 2).is_err());
        let outputs = decode_outputs(&body, 1).unwrap();
        assert_eq!(outputs[0].bytes, b"one");
    }

    #[test]
    fn direct_edit_maps_only_structured_policy_codes() {
        let policy = map_http_error(
            StatusCode::BAD_REQUEST,
            br#"{"error":{"code":"content_policy"}}"#,
        )
        .failure;
        assert_eq!(policy.error_code(), "content_policy_rejected");
        let moderation = map_http_error(
            StatusCode::BAD_REQUEST,
            br#"{"error":{"code":"moderation_blocked"}}"#,
        )
        .failure;
        assert_eq!(moderation.error_code(), "content_policy_rejected");
        let generic = map_http_error(
            StatusCode::BAD_REQUEST,
            br#"{"error":{"message":"content policy"}}"#,
        )
        .failure;
        assert_eq!(generic.error_code(), "codex_image_edit_rejected");
        assert!(!generic.is_authentication_rejection());

        let unauthorized = map_http_error(StatusCode::UNAUTHORIZED, br#"{}"#).failure;
        assert!(unauthorized.is_authentication_rejection());
        let unauthorized_policy = map_http_error(
            StatusCode::UNAUTHORIZED,
            br#"{"error":{"code":"content_policy"}}"#,
        )
        .failure;
        assert!(!unauthorized_policy.is_authentication_rejection());
        let forbidden = map_http_error(StatusCode::FORBIDDEN, br#"{}"#).failure;
        assert!(!forbidden.is_authentication_rejection());
    }

    #[test]
    fn direct_edit_requires_structured_ip_denial_in_a_401() {
        let exact_signature = br#"{"error":{"type":"invalid_request_error","message":"Your IP is not authorized to make this request."}}"#;
        for (body, upstream_code) in [
            (&exact_signature[..], None),
            (
                br#"{"error":{"code":"ip_not_allowed"}}"#,
                Some("ip_not_allowed"),
            ),
            (br#"{"error":{"code":"ip_rejected"}}"#, Some("ip_rejected")),
        ] {
            let failure = map_http_error(StatusCode::UNAUTHORIZED, body).failure;
            assert_eq!(failure.error_code(), "codex_image_tool_ip_not_authorized");
            assert_eq!(failure.upstream_error_code(), upstream_code);
            assert_eq!(failure.http_status(), Some(401));
            assert!(!failure.is_authentication_rejection());
            assert!(!failure.outcome_uncertain());
            let failure = map_http_error(StatusCode::INTERNAL_SERVER_ERROR, body).failure;
            assert_eq!(
                failure.error_code(),
                "codex_image_edit_upstream_unavailable"
            );
            assert!(!failure.is_authentication_rejection());
        }
        for body in [
            &br#"{"error":{"type":"authentication_error","message":"Your IP is not authorized to make this request."}}"#[..],
            br#"{"error":{"type":"invalid_request_error","message":"quoted: Your IP is not authorized to make this request."}}"#,
            br#"{"error":{"message":"Your IP is not authorized to make this request."}}"#,
            br#"{"type":"invalid_request_error","message":"Your IP is not authorized to make this request."}"#,
            br#"{"error":{"code":"unsupported_country_region_territory"}}"#,
            br#"{"code":"ip_not_allowed"}"#,
            b"<html>Your IP is not authorized to make this request.</html>",
        ] {
            let failure = map_http_error(StatusCode::UNAUTHORIZED, body).failure;
            assert_eq!(failure.error_code(), "codex_authentication_rejected");
            assert!(failure.is_authentication_rejection());
        }
    }

    #[test]
    fn direct_edit_does_not_classify_ambiguous_json_as_ip_denial() {
        for body in [
            &br#"{"error":null,"error":{"type":"invalid_request_error","message":"Your IP is not authorized to make this request."}}"#[..],
            br#"{"error":{"code":null,"code":"ip_rejected"}}"#,
            br#"{"error":{"type":null,"type":"invalid_request_error","message":"Your IP is not authorized to make this request."}}"#,
            br#"{"error":{"type":"invalid_request_error","message":null,"message":"Your IP is not authorized to make this request."}}"#,
            br#"{"error":{"code":"ip_rejected","type":null,"type":"invalid_request_error"}}"#,
            br#"{"error":["ip_rejected","invalid_request_error","Your IP is not authorized to make this request."]}"#,
            br#"[["ip_rejected","invalid_request_error","Your IP is not authorized to make this request."]]"#,
        ] {
            let rejection = map_http_error(StatusCode::UNAUTHORIZED, body);
            assert!(rejection.failure.is_authentication_rejection());
            assert_eq!(rejection.error_type, None);
        }
        let rejection = map_http_error(
            StatusCode::UNAUTHORIZED,
            br#"{"error":{"code":"not_policy","code":"content_policy"}}"#,
        );
        assert!(rejection.failure.is_authentication_rejection());
        assert_eq!(rejection.failure.upstream_error_code(), None);
    }

    #[test]
    fn direct_edit_records_only_coarse_http_rejection_metadata() {
        for (body, body_shape, error_type) in [
            (
                &br#"{"error":{"type":"invalid_request_error"}}"#[..],
                "json_error",
                Some("invalid_request_error"),
            ),
            (
                br#"{"error":{"type":"sk-fixture-private-token"}}"#,
                "json_error",
                Some("redacted"),
            ),
            (br#"{"type":"invalid_request_error"}"#, "json_other", None),
            (
                b" \n<html>fixture-private-content</html>",
                "markup_like",
                None,
            ),
            (b" \t\n", "empty", None),
            (b"fixture-private-content", "other", None),
        ] {
            let rejection = map_http_error(StatusCode::UNAUTHORIZED, body);
            assert_eq!(rejection.body_shape, body_shape);
            assert_eq!(rejection.error_type, error_type);
            assert!(rejection.failure.is_authentication_rejection());
        }
    }

    #[test]
    fn direct_edit_upstream_code_is_allowlisted_without_affecting_401_classification() {
        for (body, expected) in [
            (
                &br#"{"error":{"code":"TOKEN_EXPIRED"}}"#[..],
                Some("token_expired"),
            ),
            (
                br#"{"error":{"code":"sk-fixture-private-token"}}"#,
                Some("redacted"),
            ),
            (
                br#"{"error":{"code":"https://fixture.invalid/private"}}"#,
                Some("redacted"),
            ),
            (br#"{"error":{"message":"token_expired"}}"#, None),
            (b"<html>token_expired</html>", None),
        ] {
            let failure = map_http_error(StatusCode::UNAUTHORIZED, body).failure;
            assert_eq!(failure.upstream_error_code(), expected);
            assert!(failure.is_authentication_rejection());
        }
    }

    #[test]
    fn direct_edit_hashes_only_one_canonical_request_id() {
        let mut headers = HeaderMap::new();
        assert_eq!(upstream_request_id_sha256(&headers), None);
        headers.insert("x-request-id", "fixture-private-data".parse().unwrap());
        assert_eq!(upstream_request_id_sha256(&headers), None);
        let request_id = "req_0123456789abcdef0123456789abcdef";
        headers.insert("x-request-id", request_id.parse().unwrap());
        assert_eq!(
            upstream_request_id_sha256(&headers),
            Some(hex::encode(Sha256::digest(request_id.as_bytes()))),
        );
        headers.append("x-request-id", request_id.parse().unwrap());
        assert_eq!(upstream_request_id_sha256(&headers), None);
    }

    #[test]
    fn direct_edit_test_endpoint_is_restricted_to_plain_loopback_http() {
        assert!(validate_test_endpoint("http://127.0.0.1:1234/images/edits").is_ok());
        assert!(validate_test_endpoint("http://[::1]:1234/images/edits").is_ok());
        assert!(validate_test_endpoint("http://localhost:1234/images/edits").is_ok());
        assert!(validate_test_endpoint("https://127.0.0.1:1234/images/edits").is_err());
        assert!(validate_test_endpoint("http://example.com/images/edits").is_err());
        assert!(validate_test_endpoint("http://user:secret@127.0.0.1/images/edits").is_err());
    }

    #[tokio::test]
    async fn direct_edit_serializes_authenticated_single_output_requests_without_exposing_auth() {
        let observed = Arc::new(Mutex::new(ObservedRequest::default()));
        let app = Router::new()
            .route("/images/edits", post(capture_edit))
            .with_state(Arc::clone(&observed));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/images/edits", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let auth_home = TempDir::new().unwrap();
        std::fs::write(
            auth_home.path().join("auth.json"),
            br#"{"tokens":{"access_token":"access-test","refresh_token":"refresh-test","account_id":"account-test"}}"#,
        )
        .unwrap();

        for n in [1, 2, 3, 4] {
            let outputs = edit_at(
                &endpoint,
                auth_home.path(),
                &[png()],
                None,
                DirectEditBatchParameters {
                    model: "gpt-image-2",
                    prompt: "edit",
                    n,
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(outputs.len(), n as usize);
        }
        server.abort();

        let observed = observed.lock().unwrap();
        assert!(observed.authorized);
        assert!(observed.account_bound);
        assert!(observed.originator_bound);
        assert_eq!(observed.request_count, 10);
        assert_eq!(observed.max_active_requests, 1);
        assert!(observed.upstream_counts.iter().all(|count| *count == 1));
        assert_eq!(observed.body.as_ref().unwrap()["n"], 1);
        assert_eq!(observed.body.as_ref().unwrap()["model"], "gpt-image-2");
    }

    async fn capture_edit(
        State(observed): State<Arc<Mutex<ObservedRequest>>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let n = body["n"].as_u64().unwrap();
        {
            let mut observed = observed.lock().unwrap();
            observed.authorized = headers
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                == Some("Bearer access-test");
            observed.account_bound = headers
                .get("ChatGPT-Account-ID")
                .and_then(|value| value.to_str().ok())
                == Some("account-test");
            observed.originator_bound = headers
                .get("originator")
                .and_then(|value| value.to_str().ok())
                == Some("codex_cli_rs");
            observed.request_count += 1;
            observed.active_requests += 1;
            observed.max_active_requests =
                observed.max_active_requests.max(observed.active_requests);
            observed.upstream_counts.push(n);
            observed.body = Some(body);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        observed.lock().unwrap().active_requests -= 1;
        Json(serde_json::json!({
            "created": 1,
            "data": (0..n)
                .map(|index| serde_json::json!({
                    "b64_json": STANDARD.encode(format!("image-{index}"))
                }))
                .collect::<Vec<_>>()
        }))
    }
}
