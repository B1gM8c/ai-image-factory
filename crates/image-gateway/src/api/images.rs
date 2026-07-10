use std::sync::Arc;

use axum::{
    Json,
    extract::{Extension, Request, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use serde_json::Value;
use tracing::{Instrument, info_span};

use crate::{
    ImageGatewayError,
    generator::normalize_generated_images,
    models::{HealthResponse, ImageStreamKind, images_response, models_response, parse_generation},
    usage::UsageCharge,
};

use super::{
    AppState, RequestId, authenticate_image_request,
    edit_input::parse_edit_request,
    middleware::new_request_id,
    responses::{add_usage_headers, images_response_into_response, response_size_for_images},
    usage_limits,
};

pub(super) async fn healthz() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

pub(super) async fn models(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match authenticate_image_request(&headers, &state).await {
        Ok(_) => Ok(Json(models_response())),
        Err(error) => Err(error),
    }
}

pub(super) async fn generations(
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    let Json(value) = body.map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })?;

    let request_id = request_id.0;
    let job = parse_generation(value, request_id.clone())?;
    let output_format = job.output_format.clone();
    let output_compression = job.output_compression;
    let quality = job.quality.clone();
    let size = job.size.clone();
    let background = job.background.clone();
    let stream = job.stream;
    let units = job.n;

    let _permit = state.scheduler.acquire(&auth.tenant_id).await?;
    let reservation = state
        .usage_store
        .reserve(UsageCharge {
            tenant_id: auth.tenant_id.clone(),
            request_id,
            operation: "generation",
            units,
            limits: usage_limits(&state.config),
        })
        .await?;

    let generator = state.generator.clone();
    let result = tokio::time::timeout(state.config.request_timeout, generator.generate(job))
        .instrument(info_span!("gateway.handle_generate", image.units = units))
        .await
        .map_err(|_| ImageGatewayError::timeout())
        .and_then(|result| result)
        .and_then(|images| {
            normalize_generated_images(images, &size, &output_format, output_compression)
        })
        .and_then(|images| {
            let response_size = response_size_for_images(&images)?;
            Ok(images_response(
                images,
                output_format,
                quality,
                response_size,
                background,
            ))
        })
        .and_then(|response| {
            images_response_into_response(response, stream, ImageStreamKind::Generation)
        });
    let mut response = match result {
        Ok(response) => response,
        Err(error) => {
            state
                .usage_store
                .release(&reservation, "generation_failed")
                .await?;
            return Err(error);
        }
    };
    let usage = state.usage_store.commit(&reservation).await?;
    add_usage_headers(response.headers_mut(), &usage, &auth);
    Ok(response)
}

pub(super) async fn edits(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Result<Response, ImageGatewayError> {
    let headers = request.headers().clone();
    let auth = authenticate_image_request(&headers, &state).await?;

    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map(|request_id| request_id.0.clone())
        .unwrap_or_else(new_request_id);
    let form = parse_edit_request(request, &state).await?;
    let job = form.into_job(request_id.clone())?;
    let output_format = job.output_format.clone();
    let output_compression = job.output_compression;
    let quality = job.quality.clone();
    let size = job.size.clone();
    let background = job.background.clone();
    let stream = job.stream;
    let units = job.n;

    let _permit = state.scheduler.acquire(&auth.tenant_id).await?;
    let reservation = state
        .usage_store
        .reserve(UsageCharge {
            tenant_id: auth.tenant_id.clone(),
            request_id,
            operation: "edit",
            units,
            limits: usage_limits(&state.config),
        })
        .await?;

    let generator = state.generator.clone();
    let result = tokio::time::timeout(state.config.request_timeout, generator.edit(job))
        .instrument(info_span!("gateway.handle_edit", image.units = units))
        .await
        .map_err(|_| ImageGatewayError::timeout())
        .and_then(|result| result)
        .and_then(|images| {
            normalize_generated_images(images, &size, &output_format, output_compression)
        })
        .and_then(|images| {
            let response_size = response_size_for_images(&images)?;
            Ok(images_response(
                images,
                output_format,
                quality,
                response_size,
                background,
            ))
        })
        .and_then(|response| {
            images_response_into_response(response, stream, ImageStreamKind::Edit)
        });
    let mut response = match result {
        Ok(response) => response,
        Err(error) => {
            state
                .usage_store
                .release(&reservation, "edit_failed")
                .await?;
            return Err(error);
        }
    };
    let usage = state.usage_store.commit(&reservation).await?;
    add_usage_headers(response.headers_mut(), &usage, &auth);
    Ok(response)
}
