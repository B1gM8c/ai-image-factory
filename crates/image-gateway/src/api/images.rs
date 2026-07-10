use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json,
    extract::{Extension, Request, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use image_provider_contracts::openai_codex;
use serde_json::Value;
use tracing::{Instrument, info_span};

use crate::{
    ImageGatewayError,
    admission::{
        AdmissionClaim, AdmissionError, AdmissionTicket, AttachJob, ClaimAdmission,
        GENERATION_OPERATION, GenerationCommandV1, WorkOutcome, idempotency_key_digest,
    },
    generator::normalize_generated_images,
    models::{HealthResponse, ImageStreamKind, images_response, models_response, parse_generation},
    usage::UsageCharge,
};

const OPENAI_IMAGES_API_PROFILE: &str = "openai-images-v1";
const GENERATION_COMMAND_SCHEMA: &str = "openai.images.generation.v1";
const INLINE_LEASE_GRACE: Duration = Duration::from_secs(60);
const ADMISSION_DEADLINE_GRACE: Duration = Duration::from_secs(5);
const ATTACH_RETRY_DELAY: Duration = Duration::from_millis(25);
const ATTACH_ATTEMPTS: usize = 3;

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
    let command = GenerationCommandV1::from_generation_job(
        &job,
        OPENAI_IMAGES_API_PROFILE,
        openai_codex::PROVIDER_ID,
    );
    let request_hash = command.request_hash_hex();
    let command_json = serde_json::to_value(command)
        .map_err(|_| ImageGatewayError::internal("failed to serialize durable command"))?;
    let idempotency_key_digest = idempotency_digest(&headers, &auth.project_id)?;
    let ticket = match state
        .admission_store
        .claim(ClaimAdmission {
            tenant_id: auth.tenant_id.clone(),
            project_id: auth.project_id.clone(),
            api_profile: OPENAI_IMAGES_API_PROFILE.to_string(),
            operation: GENERATION_OPERATION.to_string(),
            request_id: request_id.clone(),
            idempotency_key_digest,
            request_hash,
            deadline_at_ms: admission_deadline(&state.config),
        })
        .await
        .map_err(admission_error)?
    {
        AdmissionClaim::Owner(ticket) => ticket,
        AdmissionClaim::InProgress { .. } => {
            return Err(ImageGatewayError::idempotency_in_progress());
        }
        AdmissionClaim::Existing { state, .. } if state == "accepted" => {
            return Err(ImageGatewayError::idempotency_in_progress());
        }
        AdmissionClaim::Existing { .. } => {
            return Err(ImageGatewayError::idempotency_result_unavailable());
        }
        AdmissionClaim::Conflict { .. } => {
            return Err(ImageGatewayError::idempotency_conflict());
        }
    };
    let output_format = job.output_format.clone();
    let output_compression = job.output_compression;
    let quality = job.quality.clone();
    let size = job.size.clone();
    let background = job.background.clone();
    let stream = job.stream;
    let units = job.n;
    let model = job.model.clone();

    let _permit = match state.scheduler.acquire(&auth.tenant_id).await {
        Ok(permit) => permit,
        Err(error) => {
            abort_before_attach(&state, &ticket).await?;
            return Err(error);
        }
    };
    let reservation = match state
        .usage_store
        .reserve(UsageCharge {
            tenant_id: auth.tenant_id.clone(),
            request_id,
            operation: "generation",
            provider_id: openai_codex::PROVIDER_ID.to_string(),
            model,
            units,
            limits: usage_limits(&state.config),
        })
        .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            abort_before_attach(&state, &ticket).await?;
            return Err(error);
        }
    };
    let attach = AttachJob {
        ticket: ticket.clone(),
        job_id: reservation.job_id,
        command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
        command_json,
        work_kind: "image_batch".to_string(),
    };
    let lease = match attach_and_start_with_retry(&state, attach).await {
        Ok(lease) => lease,
        Err(error) => {
            if !matches!(error, AdmissionError::Unavailable) {
                state
                    .usage_store
                    .release(&reservation, "admission_attach_failed")
                    .await?;
            }
            return Err(admission_error(error));
        }
    };

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
                .admission_store
                .settle(&lease, WorkOutcome::Failed, Some("generation_failed"))
                .await
                .map_err(admission_error)?;
            state
                .usage_store
                .release(&reservation, "generation_failed")
                .await?;
            return Err(error);
        }
    };
    let usage = state.settlement_store.succeed(&lease, &reservation).await?;
    add_usage_headers(response.headers_mut(), &usage, &auth);
    Ok(response)
}

fn idempotency_digest(
    headers: &HeaderMap,
    project_id: &str,
) -> Result<Option<String>, ImageGatewayError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = value
        .to_str()
        .map_err(|_| ImageGatewayError::invalid_idempotency_key())?;
    idempotency_key_digest(
        project_id,
        OPENAI_IMAGES_API_PROFILE,
        GENERATION_OPERATION,
        key,
    )
    .map(Some)
    .map_err(|_| ImageGatewayError::invalid_idempotency_key())
}

async fn abort_before_attach(
    state: &Arc<AppState>,
    ticket: &AdmissionTicket,
) -> Result<(), ImageGatewayError> {
    state
        .admission_store
        .abort(ticket)
        .await
        .map_err(admission_error)
}

async fn attach_and_start_with_retry(
    state: &Arc<AppState>,
    request: AttachJob,
) -> Result<crate::admission::WorkLease, AdmissionError> {
    let mut last_error = AdmissionError::Unavailable;
    for attempt in 0..ATTACH_ATTEMPTS {
        match state
            .admission_store
            .attach_and_start(
                request.clone(),
                &state.worker_id,
                inline_lease_duration(&state.config),
            )
            .await
        {
            Ok(lease) => return Ok(lease),
            Err(AdmissionError::Unavailable) if attempt + 1 < ATTACH_ATTEMPTS => {
                last_error = AdmissionError::Unavailable;
                tokio::time::sleep(ATTACH_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error)
}

fn admission_error(error: AdmissionError) -> ImageGatewayError {
    match error {
        AdmissionError::Expired => ImageGatewayError::timeout(),
        AdmissionError::Unavailable
        | AdmissionError::InvalidOwner
        | AdmissionError::StaleLease
        | AdmissionError::InvalidCommand => {
            ImageGatewayError::service_unavailable("durable admission is unavailable")
        }
    }
}

fn admission_deadline(config: &crate::AppConfig) -> i64 {
    now_ms().saturating_add(duration_ms(
        config
            .queue_timeout
            .saturating_add(ADMISSION_DEADLINE_GRACE),
    ))
}

fn inline_lease_duration(config: &crate::AppConfig) -> i64 {
    duration_ms(config.request_timeout.saturating_add(INLINE_LEASE_GRACE))
}

fn duration_ms(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
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
    let model = job.model.clone();

    let _permit = state.scheduler.acquire(&auth.tenant_id).await?;
    let reservation = state
        .usage_store
        .reserve(UsageCharge {
            tenant_id: auth.tenant_id.clone(),
            request_id,
            operation: "edit",
            provider_id: openai_codex::PROVIDER_ID.to_string(),
            model,
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
