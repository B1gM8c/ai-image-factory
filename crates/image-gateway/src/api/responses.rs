use axum::{
    Json,
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};

use crate::{
    ImageGatewayError,
    auth::AuthContext,
    generator::GeneratedImage,
    models::{ImageStreamKind, ImagesResponse, image_stream_events},
    usage::UsageSnapshot,
};

pub(super) fn add_usage_headers(
    headers: &mut HeaderMap,
    usage: &UsageSnapshot,
    auth: &AuthContext,
) {
    insert_header(headers, "openai-project", &auth.project_id);
    insert_header(headers, "x-ratelimit-limit-5h", &usage.limit_5h.to_string());
    insert_header(
        headers,
        "x-ratelimit-remaining-5h",
        &usage.remaining_5h.to_string(),
    );
    insert_header(
        headers,
        "x-image-units-limit-5h",
        &usage.limit_5h.to_string(),
    );
    insert_header(
        headers,
        "x-image-units-remaining-5h",
        &usage.remaining_5h.to_string(),
    );
    insert_header(
        headers,
        "x-image-units-limit-7d",
        &usage.limit_7d.to_string(),
    );
    insert_header(
        headers,
        "x-image-units-remaining-7d",
        &usage.remaining_7d.to_string(),
    );
}

pub(super) fn response_size_for_images(
    images: &[GeneratedImage],
) -> Result<String, ImageGatewayError> {
    let Some(image) = images.first() else {
        return Err(ImageGatewayError::backend("Codex CLI returned no images"));
    };
    let decoded = image::load_from_memory(&image.bytes)
        .map_err(|_| ImageGatewayError::backend("Codex CLI produced an unreadable image"))?;
    Ok(format!("{}x{}", decoded.width(), decoded.height()))
}

pub(super) fn images_response_into_response(
    response: ImagesResponse,
    stream: bool,
    kind: ImageStreamKind,
) -> Result<Response, ImageGatewayError> {
    if !stream {
        return Ok(Json(response).into_response());
    }

    let mut body = String::new();
    for event in image_stream_events(&response, kind) {
        body.push_str("event: ");
        body.push_str(event.event_type);
        body.push('\n');
        body.push_str("data: ");
        body.push_str(
            &serde_json::to_string(&event)
                .map_err(|_| ImageGatewayError::internal("failed to serialize stream event"))?,
        );
        body.push_str("\n\n");
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from(body))
        .map_err(|_| ImageGatewayError::internal("failed to build stream response"))
}

fn insert_header(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}
