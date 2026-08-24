use axum::{
    Json,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;

const TYPE_AUTHENTICATION: &str = "authentication_error";
const TYPE_INVALID_REQUEST: &str = "invalid_request_error";
const TYPE_RATE_LIMIT: &str = "rate_limit_error";
const TYPE_SERVER: &str = "server_error";

const CODE_INVALID_API_KEY: &str = "invalid_api_key";
const CODE_INVALID_CREDENTIALS: &str = "invalid_credentials";
const CODE_INVALID_TOKEN: &str = "invalid_token";
const CODE_INSUFFICIENT_SCOPE: &str = "insufficient_scope";
const CODE_UNSUPPORTED_PARAMETER: &str = "unsupported_parameter";
const CODE_UNKNOWN_PARAMETER: &str = "unknown_parameter";
const CODE_MODEL_NOT_FOUND: &str = "model_not_found";
const CODE_UNSUPPORTED_MEDIA_TYPE: &str = "unsupported_media_type";
const CODE_REQUEST_TOO_LARGE: &str = "request_too_large";
const CODE_RATE_LIMIT_EXCEEDED: &str = "rate_limit_exceeded";
const CODE_BILLING_LIMIT_EXCEEDED: &str = "billing_limit_exceeded";
const CODE_PROJECT_BUDGET_EXCEEDED: &str = "project_budget_exceeded";
const CODE_USER_API_KEYS_DISABLED: &str = "user_api_keys_disabled";
const CODE_IMAGE_GENERATION_FAILED: &str = "image_generation_failed";
const CODE_CODEX_CLI_FAILED: &str = "codex_cli_failed";
const CODE_CODEX_APP_SERVER_REQUEST_REJECTED: &str = "codex_app_server_request_rejected";
const CODE_CODEX_TURN_FAILED: &str = "codex_turn_failed";
const CODE_CODEX_IMAGE_TOOL_FAILED: &str = "codex_image_tool_failed";
const CODE_CONTENT_POLICY_REJECTED: &str = "content_policy_rejected";
const CODE_CODEX_EVENT_CAPTURE_INVALID: &str = "codex_event_capture_invalid";
const CODE_CODEX_PROCESS_EXITED_WITHOUT_TERMINAL: &str = "codex_process_exited_without_terminal";
const CODE_CODEX_MULTIPLE_IMAGE_OUTPUTS: &str = "codex_multiple_image_outputs";
const CODE_CODEX_STDIN_FAILED: &str = "codex_stdin_failed";
const CODE_CODEX_PROCESS_IDENTITY_UNAVAILABLE: &str = "codex_process_identity_unavailable";
const CODE_CODEX_NO_IMAGE_OUTPUT: &str = "codex_no_image_output";
const CODE_CODEX_IMAGE_TOOL_NOT_INVOKED: &str = "codex_image_tool_not_invoked";
const CODE_CODEX_IMAGE_OUTPUT_DISAPPEARED: &str = "codex_image_output_disappeared";
const CODE_CODEX_AUTHENTICATION_REJECTED: &str = "codex_authentication_rejected";
const CODE_CODEX_CREDENTIALS_UNAVAILABLE: &str = "codex_credentials_unavailable";
const CODE_CODEX_IMAGE_EDIT_RATE_LIMITED: &str = "codex_image_edit_rate_limited";
const CODE_CODEX_IMAGE_EDIT_UPSTREAM_UNAVAILABLE: &str = "codex_image_edit_upstream_unavailable";
const CODE_CODEX_IMAGE_EDIT_REJECTED: &str = "codex_image_edit_rejected";
const CODE_CODEX_IMAGE_EDIT_REQUEST_INVALID: &str = "codex_image_edit_request_invalid";
const CODE_CODEX_IMAGE_EDIT_INVALID_RESPONSE: &str = "codex_image_edit_invalid_response";
const CODE_CODEX_IMAGE_EDIT_OUTCOME_UNKNOWN: &str = "codex_image_edit_outcome_unknown";
const CODE_TIMEOUT: &str = "timeout";
const CODE_SERVICE_UNAVAILABLE: &str = "service_unavailable";
const CODE_CONFIGURATION_ERROR: &str = "configuration_error";
const CODE_INTERNAL_ERROR: &str = "internal_error";
const CODE_INVALID_IDEMPOTENCY_KEY: &str = "invalid_idempotency_key";
const CODE_IDEMPOTENCY_CONFLICT: &str = "idempotency_conflict";
const CODE_IDEMPOTENCY_IN_PROGRESS: &str = "idempotency_in_progress";
const CODE_IDEMPOTENCY_RESULT_UNAVAILABLE: &str = "idempotency_result_unavailable";
const CODE_IDEMPOTENCY_RESULT_EXPIRED: &str = "idempotency_result_expired";
const CODE_ARTIFACT_EXPIRED: &str = "artifact_expired";
const CODE_ARTIFACT_INTEGRITY_ERROR: &str = "artifact_integrity_error";

#[derive(Debug)]
pub struct ImageGatewayError {
    status: StatusCode,
    message: String,
    error_type: &'static str,
    param: Option<String>,
    code: Option<&'static str>,
    headers: Vec<(&'static str, String)>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct QuotaExceededContext {
    pub billing_metric: &'static str,
    pub billing_unit: &'static str,
    pub limit_5h: u32,
    pub limit_7d: u32,
    pub remaining_5h: u32,
    pub remaining_7d: u32,
    pub window: &'static str,
}

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorBody<'a>,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    message: &'a str,
    #[serde(rename = "type")]
    error_type: &'static str,
    param: Option<&'a str>,
    code: Option<&'static str>,
}

impl ImageGatewayError {
    pub fn status_code(&self) -> StatusCode {
        self.status
    }

    pub(crate) fn error_code(&self) -> Option<&'static str> {
        self.code
    }

    pub fn authentication() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "Invalid Authentication",
            TYPE_AUTHENTICATION,
            None,
            CODE_INVALID_API_KEY,
        )
        .with_header("www-authenticate", "Bearer".to_string())
    }

    pub(crate) fn identity_credentials() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "Invalid authentication",
            TYPE_AUTHENTICATION,
            None,
            CODE_INVALID_CREDENTIALS,
        )
    }

    pub(crate) fn identity_authentication() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "Invalid authentication",
            TYPE_AUTHENTICATION,
            None,
            CODE_INVALID_TOKEN,
        )
        .with_header(
            "www-authenticate",
            "Bearer error=\"invalid_token\"".to_string(),
        )
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            message,
            TYPE_AUTHENTICATION,
            None,
            CODE_INSUFFICIENT_SCOPE,
        )
        .with_header(
            "www-authenticate",
            "Bearer error=\"insufficient_scope\"".to_string(),
        )
    }

    pub fn user_api_keys_disabled() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "User API keys are disabled for this project",
            TYPE_INVALID_REQUEST,
            None,
            CODE_USER_API_KEYS_DISABLED,
        )
    }

    pub fn invalid_request(
        message: impl Into<String>,
        param: impl Into<Option<String>>,
        code: &'static str,
    ) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            message,
            TYPE_INVALID_REQUEST,
            param,
            code,
        )
    }

    pub fn conflict(
        message: impl Into<String>,
        param: impl Into<Option<String>>,
        code: &'static str,
    ) -> Self {
        Self::new(
            StatusCode::CONFLICT,
            message,
            TYPE_INVALID_REQUEST,
            param,
            code,
        )
    }

    pub fn unsupported(param: &str, message: impl Into<String>) -> Self {
        Self::invalid_request(message, Some(param.to_string()), CODE_UNSUPPORTED_PARAMETER)
    }

    pub fn unknown_parameter(param: &str) -> Self {
        Self::invalid_request(
            format!("Unknown parameter: '{param}'"),
            Some(param.to_string()),
            CODE_UNKNOWN_PARAMETER,
        )
    }

    pub fn invalid_idempotency_key() -> Self {
        Self::invalid_request(
            "Idempotency-Key must contain 1 to 255 visible ASCII characters",
            Some("Idempotency-Key".to_string()),
            CODE_INVALID_IDEMPOTENCY_KEY,
        )
    }

    pub fn idempotency_conflict() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "Idempotency-Key was already used with different request parameters",
            TYPE_INVALID_REQUEST,
            Some("Idempotency-Key".to_string()),
            CODE_IDEMPOTENCY_CONFLICT,
        )
    }

    pub fn idempotency_in_progress() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "A request with this Idempotency-Key is already in progress",
            TYPE_INVALID_REQUEST,
            Some("Idempotency-Key".to_string()),
            CODE_IDEMPOTENCY_IN_PROGRESS,
        )
    }

    pub fn idempotency_result_unavailable() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "This idempotent request was already accepted and cannot be replayed yet",
            TYPE_INVALID_REQUEST,
            Some("Idempotency-Key".to_string()),
            CODE_IDEMPOTENCY_RESULT_UNAVAILABLE,
        )
    }

    pub fn idempotency_result_expired() -> Self {
        Self::new(
            StatusCode::GONE,
            "The result for this idempotent request has expired and can no longer be replayed",
            TYPE_INVALID_REQUEST,
            Some("Idempotency-Key".to_string()),
            CODE_IDEMPOTENCY_RESULT_EXPIRED,
        )
    }

    pub fn artifact_expired() -> Self {
        Self::new(
            StatusCode::GONE,
            "The requested artifact has expired and is no longer available",
            TYPE_INVALID_REQUEST,
            None,
            CODE_ARTIFACT_EXPIRED,
        )
    }

    pub fn artifact_integrity() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Stored generation result failed integrity verification",
            TYPE_SERVER,
            None,
            CODE_ARTIFACT_INTEGRITY_ERROR,
        )
    }

    pub fn model_not_found(model: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            format!("The model '{model}' does not exist or is not supported by this gateway"),
            TYPE_INVALID_REQUEST,
            Some("model".to_string()),
            CODE_MODEL_NOT_FOUND,
        )
    }

    pub fn not_found(
        message: impl Into<String>,
        param: impl Into<Option<String>>,
        code: &'static str,
    ) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            message,
            TYPE_INVALID_REQUEST,
            param,
            code,
        )
    }

    pub fn unsupported_media_type(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            message,
            TYPE_INVALID_REQUEST,
            None,
            CODE_UNSUPPORTED_MEDIA_TYPE,
        )
    }

    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            message,
            TYPE_INVALID_REQUEST,
            None,
            CODE_REQUEST_TOO_LARGE,
        )
    }

    pub fn queue_overloaded() -> Self {
        Self::queue_overloaded_for("image")
    }

    pub(crate) fn queue_overloaded_for(media_kind: &str) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            format!("Rate limit reached for {media_kind} generation requests"),
            TYPE_RATE_LIMIT,
            None,
            CODE_RATE_LIMIT_EXCEEDED,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn project_model_rate_limit_exceeded(
        model: &str,
        retry_after_seconds: u64,
        request_limit_per_minute: Option<u32>,
        unit_limit_per_minute: Option<u32>,
        unit_kind: &str,
        remaining_requests: Option<i64>,
        remaining_units: Option<i64>,
    ) -> Self {
        let mut error = Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            format!("Rate limit reached for model '{model}' in this project"),
            TYPE_RATE_LIMIT,
            Some("model".to_string()),
            CODE_RATE_LIMIT_EXCEEDED,
        )
        .with_header("retry-after", retry_after_seconds.to_string());
        if let Some(limit) = request_limit_per_minute {
            error = error
                .with_header("x-ratelimit-limit-requests", limit.to_string())
                .with_header(
                    "x-ratelimit-remaining-requests",
                    remaining_requests.unwrap_or(0).max(0).to_string(),
                );
        }
        if let Some(limit) = unit_limit_per_minute {
            let (limit_header, remaining_header) = match unit_kind {
                "image" => ("x-ratelimit-limit-images", "x-ratelimit-remaining-images"),
                "video_second" => (
                    "x-ratelimit-limit-video-seconds",
                    "x-ratelimit-remaining-video-seconds",
                ),
                _ => ("x-ratelimit-limit-units", "x-ratelimit-remaining-units"),
            };
            error = error
                .with_header(limit_header, limit.to_string())
                .with_header(
                    remaining_header,
                    remaining_units.unwrap_or(0).max(0).to_string(),
                );
        }
        error
    }

    pub(crate) fn project_budget_exceeded() -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "You have exceeded your project's monthly usage limit",
            TYPE_RATE_LIMIT,
            None,
            CODE_PROJECT_BUDGET_EXCEEDED,
        )
    }

    pub(crate) fn billing_limit_exceeded() -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "Your organization has reached its billing credit limit",
            TYPE_RATE_LIMIT,
            None,
            CODE_BILLING_LIMIT_EXCEEDED,
        )
    }

    pub fn queue_timeout() -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "Rate limit reached for image generation requests",
            TYPE_RATE_LIMIT,
            None,
            CODE_RATE_LIMIT_EXCEEDED,
        )
    }

    pub(crate) fn quota_exceeded(
        message: impl Into<String>,
        context: QuotaExceededContext,
    ) -> Self {
        let error = Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            message,
            TYPE_RATE_LIMIT,
            None,
            CODE_RATE_LIMIT_EXCEEDED,
        )
        .with_header("x-ratelimit-limit-5h", context.limit_5h.to_string())
        .with_header("x-ratelimit-remaining-5h", context.remaining_5h.to_string())
        .with_header("x-billing-metric", context.billing_metric.to_string())
        .with_header("x-billing-unit", context.billing_unit.to_string())
        .with_header("x-billing-units-limit-5h", context.limit_5h.to_string())
        .with_header(
            "x-billing-units-remaining-5h",
            context.remaining_5h.to_string(),
        )
        .with_header("x-billing-units-limit-7d", context.limit_7d.to_string())
        .with_header(
            "x-billing-units-remaining-7d",
            context.remaining_7d.to_string(),
        )
        .with_header("x-billing-quota-window", context.window.to_string());
        if context.billing_metric == "output" && context.billing_unit == "output" {
            error
                .with_header("x-image-units-limit-5h", context.limit_5h.to_string())
                .with_header(
                    "x-image-units-remaining-5h",
                    context.remaining_5h.to_string(),
                )
                .with_header("x-image-units-limit-7d", context.limit_7d.to_string())
                .with_header(
                    "x-image-units-remaining-7d",
                    context.remaining_7d.to_string(),
                )
                .with_header("x-image-quota-window", context.window.to_string())
        } else {
            error
        }
    }

    pub fn backend(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            message,
            TYPE_SERVER,
            None,
            CODE_IMAGE_GENERATION_FAILED,
        )
    }

    pub fn codex_cli_failed() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            "Codex CLI exited before producing an image",
            TYPE_SERVER,
            None,
            CODE_CODEX_CLI_FAILED,
        )
    }

    pub(crate) fn codex_app_server_failure(code: &str) -> Self {
        let (message, code) = match code {
            CODE_CODEX_APP_SERVER_REQUEST_REJECTED => (
                "Codex app-server rejected the image request",
                CODE_CODEX_APP_SERVER_REQUEST_REJECTED,
            ),
            CODE_CODEX_TURN_FAILED => {
                ("Codex image generation turn failed", CODE_CODEX_TURN_FAILED)
            }
            CODE_CODEX_IMAGE_TOOL_FAILED => (
                "Codex image generation tool failed",
                CODE_CODEX_IMAGE_TOOL_FAILED,
            ),
            CODE_CODEX_EVENT_CAPTURE_INVALID => (
                "Codex app-server event stream failed validation",
                CODE_CODEX_EVENT_CAPTURE_INVALID,
            ),
            CODE_CODEX_PROCESS_EXITED_WITHOUT_TERMINAL => (
                "Codex app-server exited without a terminal event",
                CODE_CODEX_PROCESS_EXITED_WITHOUT_TERMINAL,
            ),
            CODE_CODEX_MULTIPLE_IMAGE_OUTPUTS => (
                "Codex app-server produced conflicting image outputs",
                CODE_CODEX_MULTIPLE_IMAGE_OUTPUTS,
            ),
            CODE_CODEX_STDIN_FAILED => (
                "Codex app-server input channel failed",
                CODE_CODEX_STDIN_FAILED,
            ),
            CODE_CODEX_PROCESS_IDENTITY_UNAVAILABLE => (
                "Codex app-server process identity is unavailable",
                CODE_CODEX_PROCESS_IDENTITY_UNAVAILABLE,
            ),
            _ => return Self::codex_cli_failed(),
        };
        Self::new(StatusCode::BAD_GATEWAY, message, TYPE_SERVER, None, code)
    }

    pub(crate) fn content_policy_rejected() -> Self {
        Self::invalid_request(
            "The image request was rejected by the provider safety policy",
            Some("prompt".to_string()),
            CODE_CONTENT_POLICY_REJECTED,
        )
    }

    pub fn codex_no_image_output() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            "Codex CLI completed but did not save the requested output image",
            TYPE_SERVER,
            None,
            CODE_CODEX_NO_IMAGE_OUTPUT,
        )
    }

    pub fn codex_image_tool_not_invoked() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            "Codex CLI completed without invoking the required image generation tool",
            TYPE_SERVER,
            None,
            CODE_CODEX_IMAGE_TOOL_NOT_INVOKED,
        )
    }

    pub fn codex_image_output_disappeared() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            "Codex generated an image but its output could not be durably recovered",
            TYPE_SERVER,
            None,
            CODE_CODEX_IMAGE_OUTPUT_DISAPPEARED,
        )
    }

    pub(crate) fn codex_image_edit_failure(code: &str) -> Self {
        let (status, message, error_type, code) = match code {
            CODE_CODEX_AUTHENTICATION_REJECTED => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Codex image edit authentication was rejected",
                TYPE_SERVER,
                CODE_CODEX_AUTHENTICATION_REJECTED,
            ),
            CODE_CODEX_CREDENTIALS_UNAVAILABLE => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Codex image edit credentials are unavailable",
                TYPE_SERVER,
                CODE_CODEX_CREDENTIALS_UNAVAILABLE,
            ),
            CODE_CODEX_IMAGE_EDIT_RATE_LIMITED => (
                StatusCode::TOO_MANY_REQUESTS,
                "Codex image edit rate limit reached",
                TYPE_RATE_LIMIT,
                CODE_CODEX_IMAGE_EDIT_RATE_LIMITED,
            ),
            CODE_CODEX_IMAGE_EDIT_UPSTREAM_UNAVAILABLE => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Codex image edit service is unavailable",
                TYPE_SERVER,
                CODE_CODEX_IMAGE_EDIT_UPSTREAM_UNAVAILABLE,
            ),
            CODE_CODEX_IMAGE_EDIT_REJECTED => (
                StatusCode::BAD_GATEWAY,
                "Codex image edit request was rejected",
                TYPE_SERVER,
                CODE_CODEX_IMAGE_EDIT_REJECTED,
            ),
            CODE_CODEX_IMAGE_EDIT_REQUEST_INVALID => (
                StatusCode::BAD_GATEWAY,
                "Codex image edit request contract is invalid",
                TYPE_SERVER,
                CODE_CODEX_IMAGE_EDIT_REQUEST_INVALID,
            ),
            CODE_CODEX_IMAGE_EDIT_INVALID_RESPONSE => (
                StatusCode::BAD_GATEWAY,
                "Codex image edit response is invalid",
                TYPE_SERVER,
                CODE_CODEX_IMAGE_EDIT_INVALID_RESPONSE,
            ),
            CODE_CODEX_IMAGE_EDIT_OUTCOME_UNKNOWN => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Codex image edit outcome is unknown",
                TYPE_SERVER,
                CODE_CODEX_IMAGE_EDIT_OUTCOME_UNKNOWN,
            ),
            _ => return Self::backend("Image generation failed"),
        };
        Self::new(status, message, error_type, None, code)
    }

    pub fn timeout() -> Self {
        Self::new(
            StatusCode::GATEWAY_TIMEOUT,
            "Image generation timed out",
            TYPE_SERVER,
            None,
            CODE_TIMEOUT,
        )
    }

    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            message,
            TYPE_SERVER,
            None,
            CODE_SERVICE_UNAVAILABLE,
        )
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            message,
            TYPE_SERVER,
            None,
            CODE_CONFIGURATION_ERROR,
        )
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            message,
            TYPE_SERVER,
            None,
            CODE_INTERNAL_ERROR,
        )
    }

    fn new(
        status: StatusCode,
        message: impl Into<String>,
        error_type: &'static str,
        param: impl Into<Option<String>>,
        code: impl Into<Option<&'static str>>,
    ) -> Self {
        Self {
            status,
            message: message.into(),
            error_type,
            param: param.into(),
            code: code.into(),
            headers: Vec::new(),
        }
    }

    fn with_header(mut self, name: &'static str, value: String) -> Self {
        self.headers.push((name, value));
        self
    }
}

impl IntoResponse for ImageGatewayError {
    fn into_response(self) -> Response {
        let error_code = self.code.map(str::to_owned);
        let body = Json(ErrorEnvelope {
            error: ErrorBody {
                message: &self.message,
                error_type: self.error_type,
                param: self.param.as_deref(),
                code: self.code,
            },
        });

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-store, max-age=0"),
        );
        headers.insert(
            axum::http::header::PRAGMA,
            HeaderValue::from_static("no-cache"),
        );
        for (name, value) in self.headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_lowercase(name.as_bytes()),
                HeaderValue::from_str(&value),
            ) {
                headers.insert(name, value);
            }
        }

        let mut response = (self.status, headers, body).into_response();
        response
            .extensions_mut()
            .insert(crate::request_observability::ResponseErrorCode(error_code));
        response
    }
}

impl From<std::io::Error> for ImageGatewayError {
    fn from(error: std::io::Error) -> Self {
        Self::backend(format!("Image backend I/O error: {}", error.kind()))
    }
}
