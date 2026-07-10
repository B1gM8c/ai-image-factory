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
const CODE_UNSUPPORTED_PARAMETER: &str = "unsupported_parameter";
const CODE_UNKNOWN_PARAMETER: &str = "unknown_parameter";
const CODE_MODEL_NOT_FOUND: &str = "model_not_found";
const CODE_UNSUPPORTED_MEDIA_TYPE: &str = "unsupported_media_type";
const CODE_REQUEST_TOO_LARGE: &str = "request_too_large";
const CODE_RATE_LIMIT_EXCEEDED: &str = "rate_limit_exceeded";
const CODE_IMAGE_GENERATION_FAILED: &str = "image_generation_failed";
const CODE_CODEX_CLI_FAILED: &str = "codex_cli_failed";
const CODE_CODEX_NO_IMAGE_OUTPUT: &str = "codex_no_image_output";
const CODE_TIMEOUT: &str = "timeout";
const CODE_SERVICE_UNAVAILABLE: &str = "service_unavailable";
const CODE_CONFIGURATION_ERROR: &str = "configuration_error";
const CODE_INTERNAL_ERROR: &str = "internal_error";
const CODE_INVALID_IDEMPOTENCY_KEY: &str = "invalid_idempotency_key";
const CODE_IDEMPOTENCY_CONFLICT: &str = "idempotency_conflict";
const CODE_IDEMPOTENCY_IN_PROGRESS: &str = "idempotency_in_progress";
const CODE_IDEMPOTENCY_RESULT_UNAVAILABLE: &str = "idempotency_result_unavailable";

#[derive(Debug)]
pub struct ImageGatewayError {
    status: StatusCode,
    message: String,
    error_type: &'static str,
    param: Option<String>,
    code: Option<&'static str>,
    headers: Vec<(&'static str, String)>,
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

    pub fn authentication() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "Invalid Authentication",
            TYPE_AUTHENTICATION,
            None,
            CODE_INVALID_API_KEY,
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
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "Rate limit reached for image generation requests",
            TYPE_RATE_LIMIT,
            None,
            CODE_RATE_LIMIT_EXCEEDED,
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

    pub fn quota_exceeded(
        message: impl Into<String>,
        limit_5h: u32,
        limit_7d: u32,
        remaining_5h: u32,
        remaining_7d: u32,
        window: &'static str,
    ) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            message,
            TYPE_RATE_LIMIT,
            None,
            CODE_RATE_LIMIT_EXCEEDED,
        )
        .with_header("x-ratelimit-limit-5h", limit_5h.to_string())
        .with_header("x-ratelimit-remaining-5h", remaining_5h.to_string())
        .with_header("x-image-units-limit-5h", limit_5h.to_string())
        .with_header("x-image-units-remaining-5h", remaining_5h.to_string())
        .with_header("x-image-units-limit-7d", limit_7d.to_string())
        .with_header("x-image-units-remaining-7d", remaining_7d.to_string())
        .with_header("x-image-quota-window", window.to_string())
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

    pub fn codex_no_image_output() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            "Codex CLI completed but did not save the requested output image",
            TYPE_SERVER,
            None,
            CODE_CODEX_NO_IMAGE_OUTPUT,
        )
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
        let body = Json(ErrorEnvelope {
            error: ErrorBody {
                message: &self.message,
                error_type: self.error_type,
                param: self.param.as_deref(),
                code: self.code,
            },
        });

        let mut headers = HeaderMap::new();
        for (name, value) in self.headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_lowercase(name.as_bytes()),
                HeaderValue::from_str(&value),
            ) {
                headers.insert(name, value);
            }
        }

        (self.status, headers, body).into_response()
    }
}

impl From<std::io::Error> for ImageGatewayError {
    fn from(error: std::io::Error) -> Self {
        Self::backend(format!("Image backend I/O error: {}", error.kind()))
    }
}
