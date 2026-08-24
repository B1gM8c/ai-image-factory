use std::{borrow::Cow, path::Path, time::Duration};

#[cfg(debug_assertions)]
use std::net::IpAddr;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode, redirect::Policy};
use serde::{Deserialize, Serialize};

use crate::{
    ImageGatewayError,
    core::provider::{GeneratedImage, InputImage},
};

const CODEX_IMAGE_EDITS_URL: &str = "https://chatgpt.com/backend-api/codex/images/edits";
#[cfg(debug_assertions)]
const TEST_CODEX_IMAGE_EDITS_URL_ENV: &str = "GATEWAY_TEST_CODEX_IMAGE_EDITS_URL";
const CODEX_IMAGE_MODEL: &str = "gpt-image-2";
const CODEX_ORIGINATOR: &str = "codex_cli_rs";
const CODEX_USER_AGENT: &str = "codex_cli_rs/0.145.0";
const MAX_AUTH_BYTES: u64 = 64 * 1024;
const MAX_RESPONSE_BYTES: usize = 96 * 1024 * 1024;
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
    outcome_uncertain: bool,
}

pub(crate) struct DirectEditParameters<'a> {
    pub(crate) prompt: &'a str,
    pub(crate) background: &'a str,
    pub(crate) quality: &'a str,
    pub(crate) size: &'a str,
    pub(crate) output_index: u32,
}

impl DirectEditFailure {
    pub(crate) fn error_code(&self) -> &'static str {
        self.error_code
    }

    pub(crate) fn http_status(&self) -> Option<u16> {
        self.http_status
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
}

#[derive(Serialize)]
struct EditRequest<'a> {
    images: Vec<ImageUrl>,
    prompt: &'a str,
    background: &'a str,
    model: &'static str,
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

pub(super) async fn edit(
    auth_home: &Path,
    images: &[InputImage],
    mask: Option<&InputImage>,
    prompt: &str,
    n: u32,
    timeout: Duration,
) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
    let endpoint = image_edits_endpoint()?;
    edit_at(&endpoint, auth_home, images, mask, prompt, n, timeout).await
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

#[cfg(debug_assertions)]
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
    prompt: &str,
    n: u32,
    timeout: Duration,
) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
    if n == 0 {
        return Err(invalid_response());
    }
    let mut total = 0usize;
    let mut outputs = Vec::with_capacity(n as usize);
    for output_index in 0..n {
        let image = edit_one_at(
            endpoint,
            auth_home,
            images,
            mask,
            DirectEditParameters {
                prompt,
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
    let payload = edit_request(
        images,
        mask,
        parameters.prompt,
        parameters.background,
        parameters.quality,
        parameters.size,
        1,
    )
    .map_err(local_invalid_request)?;
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
    let body = read_bounded_body(response)
        .await
        .map_err(|_| uncertain_transport())?;
    if !status.is_success() {
        let upstream_code = structured_error_code(&body);
        tracing::warn!(
            codex.edit.output_index = output_index,
            http.status = status.as_u16(),
            upstream.code = safe_upstream_token(upstream_code.as_deref()),
            "Codex image edit upstream rejected request"
        );
        return Err(map_http_error(status, &body));
    }
    decode_outputs(&body, 1).map_err(|_| direct_invalid_response())
}

fn edit_request<'a>(
    images: &[InputImage],
    mask: Option<&InputImage>,
    prompt: &'a str,
    background: &'a str,
    quality: &'a str,
    size: &'a str,
    n: u32,
) -> Result<EditRequest<'a>, ImageGatewayError> {
    let mut encoded = Vec::with_capacity(images.len() + usize::from(mask.is_some()));
    for image in images.iter().chain(mask) {
        encoded.push(ImageUrl {
            image_url: data_url(image)?,
        });
    }
    Ok(EditRequest {
        images: encoded,
        prompt,
        background,
        model: CODEX_IMAGE_MODEL,
        n,
        quality,
        size,
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

fn map_http_error(status: StatusCode, body: &[u8]) -> DirectEditFailure {
    let code = structured_error_code(body)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if matches!(
        code.as_str(),
        "content_policy" | "content_policy_violation" | "moderation_blocked" | "safety"
    ) {
        return DirectEditFailure {
            error: ImageGatewayError::content_policy_rejected(),
            error_code: "content_policy_rejected",
            http_status: Some(status.as_u16()),
            outcome_uncertain: false,
        };
    }
    let (error, error_code) = match status {
        StatusCode::UNAUTHORIZED => (credentials_unavailable(), "codex_authentication_rejected"),
        StatusCode::TOO_MANY_REQUESTS => (
            ImageGatewayError::queue_overloaded(),
            "codex_image_edit_rate_limited",
        ),
        status if status.is_server_error() => {
            (unavailable(), "codex_image_edit_upstream_unavailable")
        }
        _ => (
            ImageGatewayError::backend("Codex image edit request was rejected"),
            "codex_image_edit_rejected",
        ),
    };
    DirectEditFailure {
        error,
        error_code,
        http_status: Some(status.as_u16()),
        outcome_uncertain: false,
    }
}

fn local_credentials_unavailable(error: ImageGatewayError) -> DirectEditFailure {
    DirectEditFailure {
        error,
        error_code: "codex_credentials_unavailable",
        http_status: None,
        outcome_uncertain: false,
    }
}

fn local_invalid_request(error: ImageGatewayError) -> DirectEditFailure {
    DirectEditFailure {
        error,
        error_code: "codex_image_edit_request_invalid",
        http_status: None,
        outcome_uncertain: false,
    }
}

fn local_unavailable(error: ImageGatewayError) -> DirectEditFailure {
    DirectEditFailure {
        error,
        error_code: "codex_image_edit_upstream_unavailable",
        http_status: None,
        outcome_uncertain: false,
    }
}

fn uncertain_transport() -> DirectEditFailure {
    DirectEditFailure {
        error: unavailable(),
        error_code: "codex_image_edit_outcome_unknown",
        http_status: None,
        outcome_uncertain: true,
    }
}

fn direct_invalid_response() -> DirectEditFailure {
    DirectEditFailure {
        error: invalid_response(),
        error_code: "codex_image_edit_invalid_response",
        http_status: Some(StatusCode::OK.as_u16()),
        outcome_uncertain: true,
    }
}

fn structured_error_code(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<ErrorEnvelope>(body)
        .ok()
        .and_then(|value| value.error)
        .and_then(|error| error.code)
}

fn safe_upstream_token(value: Option<&str>) -> &str {
    match value {
        Some(value)
            if !value.is_empty()
                && value.len() <= 64
                && value.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                }) =>
        {
            value
        }
        Some(_) => "redacted",
        None => "none",
    }
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
        let request = edit_request(&[png()], None, "edit", "auto", "auto", "auto", 1).unwrap();
        let value = serde_json::to_value(request).unwrap();
        assert_eq!(value["model"], "gpt-image-2");
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
        );
        assert_eq!(policy.error_code(), "content_policy_rejected");
        let moderation = map_http_error(
            StatusCode::BAD_REQUEST,
            br#"{"error":{"code":"moderation_blocked"}}"#,
        );
        assert_eq!(moderation.error_code(), "content_policy_rejected");
        let generic = map_http_error(
            StatusCode::BAD_REQUEST,
            br#"{"error":{"message":"content policy"}}"#,
        );
        assert_eq!(generic.error_code(), "codex_image_edit_rejected");
        assert!(!generic.is_authentication_rejection());

        let unauthorized = map_http_error(StatusCode::UNAUTHORIZED, br#"{}"#);
        assert!(unauthorized.is_authentication_rejection());
        let unauthorized_policy = map_http_error(
            StatusCode::UNAUTHORIZED,
            br#"{"error":{"code":"content_policy"}}"#,
        );
        assert!(!unauthorized_policy.is_authentication_rejection());
        let forbidden = map_http_error(StatusCode::FORBIDDEN, br#"{}"#);
        assert!(!forbidden.is_authentication_rejection());
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
                "edit",
                n,
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
