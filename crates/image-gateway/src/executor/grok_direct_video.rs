use std::{io::Read, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use image_provider_grok_cli::ImageToVideoRequestV1;
use reqwest::{Client, StatusCode, Url, header::AUTHORIZATION};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[cfg(test)]
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use super::private_auth;
use crate::{
    artifacts::media_type_from_bytes,
    runner::process::{ExecutionSpool, sha256},
};

const XAI_API_BASE: &str = "https://api.x.ai/v1";
const XAI_VIDEO_MODEL: &str = "grok-imagine-video-1.5";
const START_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_VIDEO_BYTES: usize = 256 * 1024 * 1024;
const MAX_INPUT_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GrokDiagnosticV1 {
    schema_version: u16,
    stage: String,
    class: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    http_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    upstream_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    upstream_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    remote_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mismatch_field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    actual_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    actual_sha256: Option<String>,
}

impl GrokDiagnosticV1 {
    pub(super) fn io(stream: &str, bytes: &[u8], truncated: bool) -> Self {
        Self {
            schema_version: 1,
            stage: format!("provider_{stream}"),
            class: if truncated { "truncated" } else { "captured" }.to_owned(),
            http_status: None,
            upstream_code: None,
            upstream_type: None,
            message_sha256: Some(sha256(bytes)),
            message_bytes: Some(bytes.len() as u64),
            remote_task_id: None,
            mismatch_field: None,
            expected_type: None,
            expected_sha256: None,
            actual_type: None,
            actual_sha256: None,
        }
    }

    #[cfg(test)]
    pub(super) fn receipt_mismatch(
        field: &str,
        expected: Option<&Value>,
        actual: Option<&Value>,
    ) -> Self {
        Self {
            schema_version: 1,
            stage: "receipt".to_owned(),
            class: "tool_arguments_mismatch".to_owned(),
            http_status: None,
            upstream_code: None,
            upstream_type: None,
            message_sha256: None,
            message_bytes: None,
            remote_task_id: None,
            mismatch_field: safe_token(field),
            expected_type: expected.map(json_type).map(str::to_owned),
            expected_sha256: expected.and_then(json_sha256),
            actual_type: actual.map(json_type).map(str::to_owned),
            actual_sha256: actual.and_then(json_sha256),
        }
    }

    pub(super) fn receipt_mismatch_summary(
        field: &str,
        expected_type: &str,
        expected_sha256: Option<&str>,
        actual_type: &str,
        actual_sha256: Option<&str>,
    ) -> Self {
        Self {
            schema_version: 1,
            stage: "receipt".to_owned(),
            class: "tool_arguments_mismatch".to_owned(),
            http_status: None,
            upstream_code: None,
            upstream_type: None,
            message_sha256: None,
            message_bytes: None,
            remote_task_id: None,
            mismatch_field: safe_token(field),
            expected_type: safe_token(expected_type),
            expected_sha256: expected_sha256.and_then(safe_sha256),
            actual_type: safe_token(actual_type),
            actual_sha256: actual_sha256.and_then(safe_sha256),
        }
    }

    pub(super) fn receipt_tool_failure(
        upstream_code: Option<&str>,
        upstream_type: Option<&str>,
        message_sha256: &str,
        message_bytes: u64,
    ) -> Self {
        Self {
            schema_version: 1,
            stage: "receipt".to_owned(),
            class: "tool_execution_failed".to_owned(),
            http_status: None,
            upstream_code: upstream_code.and_then(safe_token),
            upstream_type: upstream_type.and_then(safe_token),
            message_sha256: safe_sha256(message_sha256),
            message_bytes: Some(message_bytes),
            remote_task_id: None,
            mismatch_field: None,
            expected_type: None,
            expected_sha256: None,
            actual_type: None,
            actual_sha256: None,
        }
    }

    pub(super) fn direct_success(remote_task_id: &str) -> Self {
        Self {
            schema_version: 1,
            stage: "download".to_owned(),
            class: "succeeded".to_owned(),
            http_status: Some(200),
            upstream_code: None,
            upstream_type: None,
            message_sha256: None,
            message_bytes: None,
            remote_task_id: safe_remote_id(remote_task_id),
            mismatch_field: None,
            expected_type: None,
            expected_sha256: None,
            actual_type: None,
            actual_sha256: None,
        }
    }

    pub(super) fn receipt_failure(class: &str, message: Option<&str>) -> Self {
        let (message_sha256, message_bytes) = message_digest(message);
        Self {
            schema_version: 1,
            stage: "receipt".to_owned(),
            class: safe_token(class).unwrap_or_else(|| "invalid".to_owned()),
            http_status: None,
            upstream_code: None,
            upstream_type: None,
            message_sha256,
            message_bytes,
            remote_task_id: None,
            mismatch_field: None,
            expected_type: None,
            expected_sha256: None,
            actual_type: None,
            actual_sha256: None,
        }
    }
}

pub(super) struct DirectVideoSuccess {
    pub(super) bytes: Vec<u8>,
    pub(super) remote_task_id: String,
}

pub(super) struct DirectVideoError {
    pub(super) error_code: &'static str,
    pub(super) definite: bool,
    pub(super) diagnostic: Box<GrokDiagnosticV1>,
}

pub(super) async fn generate_image_to_video(
    spool: &ExecutionSpool,
    request: &ImageToVideoRequestV1,
    auth_sha256: &str,
    overall_timeout: Duration,
) -> Result<DirectVideoSuccess, DirectVideoError> {
    let bearer = private_auth::read_verified_bearer(
        spool.provider_home_path().map_err(local_error)?,
        auth_sha256,
        overall_timeout.saturating_add(Duration::from_secs(30)),
    )
    .map_err(local_error)?;
    let input = read_input(spool, request)?;
    let output = private_auth::read_isolated_grok_video_output(
        spool.provider_home_path().map_err(local_error)?,
    )
    .map_err(local_error)?;
    let urls = output
        .presign_video_output(uuid::Uuid::new_v4())
        .map_err(local_error)?;
    let payload = image_to_video_payload(request, &input, &urls.upload_url);
    let client = Client::builder().build().map_err(local_error)?;
    let start = Instant::now();
    let response = client
        .post(format!("{XAI_API_BASE}/videos/generations"))
        .header(AUTHORIZATION, format!("Bearer {bearer}"))
        .timeout(START_TIMEOUT.min(overall_timeout))
        .json(&payload)
        .send()
        .await
        .map_err(|error| network_error("start", &error))?;
    let status = response.status();
    let body = read_bounded_body(response, MAX_ERROR_BODY_BYTES).await?;
    if !status.is_success() {
        return Err(http_error("start", status, &body, None));
    }
    let response: StartResponse = serde_json::from_slice(&body)
        .map_err(|_| response_error("start", "invalid_response", &body, None))?;
    if !valid_remote_id(&response.request_id) {
        return Err(response_error("start", "invalid_request_id", &body, None));
    }
    let remote_task_id = response.request_id;
    let poll_url = format!("{XAI_API_BASE}/videos/{remote_task_id}");
    loop {
        if start.elapsed().saturating_add(POLL_INTERVAL) >= overall_timeout {
            return Err(uncertain(
                "poll",
                "timeout",
                None,
                None,
                Some(&remote_task_id),
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
        let response = client
            .get(&poll_url)
            .header(AUTHORIZATION, format!("Bearer {bearer}"))
            .timeout(POLL_TIMEOUT.min(overall_timeout.saturating_sub(start.elapsed())))
            .send()
            .await
            .map_err(|error| network_error_with_remote("poll", &error, &remote_task_id))?;
        let status = response.status();
        let body = read_bounded_body(response, MAX_ERROR_BODY_BYTES).await?;
        if !status.is_success() && status != StatusCode::ACCEPTED {
            return Err(http_error("poll", status, &body, Some(&remote_task_id)));
        }
        let response: PollResponse = serde_json::from_slice(&body).map_err(|_| {
            response_error("poll", "invalid_response", &body, Some(&remote_task_id))
        })?;
        match response.status.as_str() {
            "done" => {
                let bytes = download_video(&client, &urls.download_url, &remote_task_id).await?;
                return Ok(DirectVideoSuccess {
                    bytes,
                    remote_task_id,
                });
            }
            "failed" => {
                return Err(definite(
                    "poll",
                    "provider_failed",
                    None,
                    Some(&body),
                    Some(&remote_task_id),
                ));
            }
            "expired" => {
                return Err(definite(
                    "poll",
                    "provider_expired",
                    None,
                    Some(&body),
                    Some(&remote_task_id),
                ));
            }
            "pending" | "running" => {}
            _ => {
                return Err(uncertain(
                    "poll",
                    "invalid_status",
                    None,
                    Some(&body),
                    Some(&remote_task_id),
                ));
            }
        }
    }
}

fn read_input(
    spool: &ExecutionSpool,
    request: &ImageToVideoRequestV1,
) -> Result<Vec<u8>, DirectVideoError> {
    let mut file = spool
        .open_provider_input(request.image().filename())
        .map_err(local_error)?;
    let size = file.metadata().map_err(local_error)?.len();
    if size == 0 || size > MAX_INPUT_BYTES {
        return Err(local_error("invalid input size"));
    }
    let mut bytes = Vec::with_capacity(size as usize);
    file.by_ref()
        .take(MAX_INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(local_error)?;
    if bytes.len() as u64 != size
        || sha256(&bytes) != request.image().sha256()
        || media_type_from_bytes(&bytes).is_err()
    {
        return Err(local_error("input integrity"));
    }
    Ok(bytes)
}

fn image_to_video_payload(
    request: &ImageToVideoRequestV1,
    input: &[u8],
    upload_url: &str,
) -> Value {
    let media_type = media_type_from_bytes(input).unwrap_or("image/jpeg");
    json!({
        "model": XAI_VIDEO_MODEL,
        "prompt": request.prompt().unwrap_or(""),
        "image": {
            "url": format!("data:{media_type};base64,{}", STANDARD.encode(input)),
        },
        "duration": request.duration().seconds(),
        "resolution": request.resolution().as_str(),
        "output": { "upload_url": upload_url },
    })
}

async fn download_video(
    client: &Client,
    url: &str,
    remote_task_id: &str,
) -> Result<Vec<u8>, DirectVideoError> {
    let parsed = Url::parse(url).map_err(local_error)?;
    let secure = parsed.scheme() == "https";
    let local_http = parsed.scheme() == "http"
        && parsed
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    if !secure && !local_http {
        return Err(local_error("invalid download URL"));
    }
    let response = client
        .get(parsed)
        .timeout(DOWNLOAD_TIMEOUT)
        .send()
        .await
        .map_err(|error| network_error_with_remote("download", &error, remote_task_id))?;
    let status = response.status();
    if !status.is_success() {
        let body = read_bounded_body(response, MAX_ERROR_BODY_BYTES).await?;
        return Err(http_error("download", status, &body, Some(remote_task_id)));
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|error| network_error_with_remote("download", &error, remote_task_id))?;
        if bytes.len().saturating_add(chunk.len()) > MAX_VIDEO_BYTES {
            return Err(uncertain(
                "download",
                "too_large",
                Some(status.as_u16()),
                None,
                Some(remote_task_id),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.is_empty() {
        return Err(uncertain(
            "download",
            "empty",
            Some(status.as_u16()),
            None,
            Some(remote_task_id),
        ));
    }
    Ok(bytes)
}

async fn read_bounded_body(
    response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, DirectVideoError> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| network_error("response", &error))?;
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(uncertain("response", "too_large", None, None, None));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn http_error(
    stage: &str,
    status: StatusCode,
    body: &[u8],
    remote_task_id: Option<&str>,
) -> DirectVideoError {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let error = parsed.as_ref().and_then(|value| value.get("error"));
    let upstream_code = error
        .and_then(|value| value.get("code"))
        .and_then(Value::as_str)
        .and_then(safe_token);
    let upstream_type = error
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
        .and_then(safe_token);
    let message = error
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .or_else(|| error.and_then(Value::as_str));
    let (message_sha256, message_bytes) = message_digest(message);
    let retryable = status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error();
    let definite = remote_task_id.is_none() && !retryable;
    DirectVideoError {
        error_code: if retryable {
            "grok_video_upstream_unavailable"
        } else {
            "grok_video_upstream_rejected"
        },
        definite,
        diagnostic: Box::new(GrokDiagnosticV1 {
            schema_version: 1,
            stage: stage.to_owned(),
            class: if retryable {
                "http_retryable"
            } else {
                "http_rejected"
            }
            .to_owned(),
            http_status: Some(status.as_u16()),
            upstream_code,
            upstream_type,
            message_sha256,
            message_bytes,
            remote_task_id: remote_task_id.and_then(safe_remote_id),
            mismatch_field: None,
            expected_type: None,
            expected_sha256: None,
            actual_type: None,
            actual_sha256: None,
        }),
    }
}

fn network_error(stage: &str, error: &reqwest::Error) -> DirectVideoError {
    uncertain(
        stage,
        "network",
        None,
        Some(error.to_string().as_bytes()),
        None,
    )
}

fn network_error_with_remote(
    stage: &str,
    error: &reqwest::Error,
    remote_task_id: &str,
) -> DirectVideoError {
    uncertain(
        stage,
        "network",
        None,
        Some(error.to_string().as_bytes()),
        Some(remote_task_id),
    )
}

fn response_error(
    stage: &str,
    class: &str,
    body: &[u8],
    remote_task_id: Option<&str>,
) -> DirectVideoError {
    uncertain(stage, class, None, Some(body), remote_task_id)
}

fn local_error(_error: impl std::fmt::Display) -> DirectVideoError {
    uncertain("local", "invalid_runtime", None, None, None)
}

fn definite(
    stage: &str,
    class: &str,
    status: Option<u16>,
    message: Option<&[u8]>,
    remote_task_id: Option<&str>,
) -> DirectVideoError {
    diagnostic_error(true, stage, class, status, message, remote_task_id)
}

fn uncertain(
    stage: &str,
    class: &str,
    status: Option<u16>,
    message: Option<&[u8]>,
    remote_task_id: Option<&str>,
) -> DirectVideoError {
    diagnostic_error(false, stage, class, status, message, remote_task_id)
}

fn diagnostic_error(
    definite: bool,
    stage: &str,
    class: &str,
    status: Option<u16>,
    message: Option<&[u8]>,
    remote_task_id: Option<&str>,
) -> DirectVideoError {
    DirectVideoError {
        error_code: if definite {
            "grok_video_generation_failed"
        } else {
            "grok_video_generation_uncertain"
        },
        definite,
        diagnostic: Box::new(GrokDiagnosticV1 {
            schema_version: 1,
            stage: safe_token(stage).unwrap_or_else(|| "invalid".to_owned()),
            class: safe_token(class).unwrap_or_else(|| "invalid".to_owned()),
            http_status: status,
            upstream_code: None,
            upstream_type: None,
            message_sha256: message.map(sha256),
            message_bytes: message.map(|value| value.len() as u64),
            remote_task_id: remote_task_id.and_then(safe_remote_id),
            mismatch_field: None,
            expected_type: None,
            expected_sha256: None,
            actual_type: None,
            actual_sha256: None,
        }),
    }
}

fn message_digest(message: Option<&str>) -> (Option<String>, Option<u64>) {
    (
        message.map(|value| sha256(value.as_bytes())),
        message.map(|value| value.len() as u64),
    )
}

fn safe_token(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':')))
    .then(|| value.to_owned())
}

fn safe_remote_id(value: &str) -> Option<String> {
    valid_remote_id(value).then(|| value.to_owned())
}

fn safe_sha256(value: &str) -> Option<String> {
    (value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')))
    .then(|| value.to_owned())
}

fn valid_remote_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

#[cfg(test)]
fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
fn json_sha256(value: &Value) -> Option<String> {
    serde_json::to_vec(value)
        .ok()
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
}

#[derive(Deserialize)]
struct StartResponse {
    request_id: String,
}

#[derive(Deserialize)]
struct PollResponse {
    #[serde(default)]
    status: String,
}

#[cfg(test)]
mod tests {
    use image_provider_grok_cli::{
        ImageToVideoRequestV1, StagedImageV1, VideoDuration, VideoResolution,
    };

    use super::*;

    #[test]
    fn long_image_to_video_prompt_is_preserved_byte_for_byte() {
        let prompt = format!("{}{}", "长".repeat(1_447), "x".repeat(531));
        assert_eq!(prompt.chars().count(), 1_978);
        assert_eq!(prompt.len(), 4_872);
        let request = ImageToVideoRequestV1::new(
            Some(prompt.clone()),
            StagedImageV1::new("frame.jpg", "a".repeat(64)).unwrap(),
            VideoDuration::Seconds6,
            VideoResolution::P720,
        )
        .unwrap();
        let payload =
            image_to_video_payload(&request, b"\xff\xd8\xff\xd9", "https://upload.invalid");
        assert_eq!(payload["prompt"], prompt);
        assert_eq!(payload["duration"], 6);
        assert_eq!(payload["resolution"], "720p");
        assert_eq!(payload["model"], XAI_VIDEO_MODEL);
    }

    #[test]
    fn diagnostics_never_retain_message_or_argument_values() {
        let expected = json!("private prompt");
        let actual = json!("rewritten prompt");
        let diagnostic =
            GrokDiagnosticV1::receipt_mismatch("prompt", Some(&expected), Some(&actual));
        let serialized = serde_json::to_string(&diagnostic).unwrap();
        assert!(!serialized.contains("private prompt"));
        assert!(!serialized.contains("rewritten prompt"));
        assert!(serialized.contains("expected_sha256"));
        assert!(serialized.contains("actual_sha256"));
    }

    #[test]
    fn upstream_errors_are_redacted_and_post_submit_failures_remain_uncertain() {
        let body = br#"{"error":{"code":"rate_limit_exceeded","type":"rate_limit_error","message":"private upstream detail"}}"#;
        let start = http_error("start", StatusCode::BAD_REQUEST, body, None);
        assert!(start.definite);
        assert_eq!(
            start.diagnostic.upstream_code.as_deref(),
            Some("rate_limit_exceeded")
        );
        assert_eq!(
            start.diagnostic.upstream_type.as_deref(),
            Some("rate_limit_error")
        );
        assert!(
            !serde_json::to_string(&start.diagnostic)
                .unwrap()
                .contains("private upstream detail")
        );

        let poll = http_error(
            "poll",
            StatusCode::TOO_MANY_REQUESTS,
            body,
            Some("task_123"),
        );
        assert!(!poll.definite);
        assert_eq!(poll.diagnostic.remote_task_id.as_deref(), Some("task_123"));
    }

    #[test]
    fn start_response_keeps_request_identity_when_the_provider_adds_metadata() {
        let response: StartResponse = serde_json::from_value(json!({
            "request_id": "task_123",
            "status": "pending"
        }))
        .unwrap();
        assert_eq!(response.request_id, "task_123");
    }
}
