use std::sync::Arc;

use axum::{
    body::Body,
    extract::{OriginalUri, RawQuery, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use tokio_util::io::ReaderStream;

use super::AppState;
use crate::provider_uploads::ProviderUploadError;

pub(super) async fn provider_upload(
    State(state): State<Arc<AppState>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let Some(service) = &state.provider_upload_service else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let authorization = match service.authorize(&method, &uri, raw_query.as_deref(), &headers) {
        Ok(authorization) => authorization,
        Err(error) => return error_response(error),
    };
    match method {
        Method::PUT => match service.put(authorization, body).await {
            Ok((byte_size, etag)) => {
                let mut response = StatusCode::OK.into_response();
                response.headers_mut().insert(
                    header::ETAG,
                    HeaderValue::from_str(&format!("\"{etag}\""))
                        .unwrap_or_else(|_| HeaderValue::from_static("\"invalid\"")),
                );
                response.headers_mut().insert(
                    header::CONTENT_LENGTH,
                    HeaderValue::from_str(&byte_size.to_string())
                        .unwrap_or_else(|_| HeaderValue::from_static("0")),
                );
                response
            }
            Err(error) => error_response(error),
        },
        Method::GET | Method::HEAD => match service.open(authorization).await {
            Ok(object) => {
                let mut response = if method == Method::HEAD {
                    StatusCode::OK.into_response()
                } else {
                    Body::from_stream(ReaderStream::new(object.file)).into_response()
                };
                response
                    .headers_mut()
                    .insert(header::CONTENT_TYPE, HeaderValue::from_static("video/mp4"));
                response.headers_mut().insert(
                    header::CONTENT_LENGTH,
                    HeaderValue::from_str(&object.byte_size.to_string())
                        .unwrap_or_else(|_| HeaderValue::from_static("0")),
                );
                response.headers_mut().insert(
                    header::ETAG,
                    HeaderValue::from_str(&format!("\"{}\"", object.etag))
                        .unwrap_or_else(|_| HeaderValue::from_static("\"invalid\"")),
                );
                response
            }
            Err(error) => error_response(error),
        },
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn error_response(error: ProviderUploadError) -> Response {
    match error {
        ProviderUploadError::Unauthorized => StatusCode::FORBIDDEN,
        ProviderUploadError::NotFound => StatusCode::NOT_FOUND,
        ProviderUploadError::Conflict => StatusCode::CONFLICT,
        ProviderUploadError::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        ProviderUploadError::InvalidContent => StatusCode::UNPROCESSABLE_ENTITY,
        ProviderUploadError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    }
    .into_response()
}
