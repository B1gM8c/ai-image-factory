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
        GENERATION_COMMAND_SCHEMA, GENERATION_OPERATION, GenerationCommandV1,
        idempotency_key_digest,
    },
    artifacts::GENERATION_RESPONSE_SCHEMA,
    generator::normalize_generated_images,
    models::{
        HealthResponse, ImageStreamKind, images_response, images_response_at, models_response,
        parse_generation,
    },
    usage::UsageCharge,
};

const OPENAI_IMAGES_API_PROFILE: &str = "openai-images-v1";
const INLINE_LEASE_GRACE: Duration = Duration::from_secs(60);
const ADMISSION_DEADLINE_GRACE: Duration = Duration::from_secs(5);
const ATTACH_RETRY_DELAY: Duration = Duration::from_millis(25);
const ATTACH_ATTEMPTS: usize = 3;
const RESULT_POLL_INTERVAL: Duration = Duration::from_millis(25);

use super::{
    AppState, GenerationExecutionMode, RequestId, authenticate_image_request,
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
        AdmissionClaim::Existing {
            job_id,
            state: claim_state,
        } if claim_state == "succeeded" => {
            return replay_generation(&state, job_id, &auth).await;
        }
        AdmissionClaim::Existing { .. } => {
            return Err(ImageGatewayError::idempotency_result_unavailable());
        }
        AdmissionClaim::Conflict { .. } => {
            return Err(ImageGatewayError::idempotency_conflict());
        }
    };
    let units = job.n;
    let model = job.model.clone();

    let _inline_permit = if state.generation_execution_mode == GenerationExecutionMode::Inline {
        match state.scheduler.acquire(&auth.tenant_id).await {
            Ok(permit) => Some(permit),
            Err(error) => {
                abort_before_attach(&state, &ticket).await?;
                return Err(error);
            }
        }
    } else {
        None
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
        schedule_scope: format!("tenant:{}", auth.tenant_id),
        schedule_weight: 1,
        schedule_priority: 1,
        schedule_cost: u64::from(units),
    };
    if state.generation_execution_mode == GenerationExecutionMode::External {
        if let Err(error) = attach_ready_with_retry(&state, attach).await {
            if !matches!(error, AdmissionError::Unavailable) {
                state
                    .usage_store
                    .release(&reservation, "admission_attach_failed")
                    .await?;
            }
            return Err(admission_error(error));
        }
        return wait_for_generation(&state, reservation.job_id, &auth).await;
    }
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

    let execution = state
        .generation_worker
        .execute(
            &lease,
            &reservation,
            job,
            OPENAI_IMAGES_API_PROFILE,
            GENERATION_RESPONSE_SCHEMA,
        )
        .await?;
    render_generation_response(
        execution.images,
        execution.projection,
        execution.usage,
        &auth,
    )
}

async fn wait_for_generation(
    state: &Arc<AppState>,
    job_id: uuid::Uuid,
    auth: &crate::auth::AuthContext,
) -> Result<Response, ImageGatewayError> {
    let wait = async {
        loop {
            match state.settlement_store.generation_status(job_id).await? {
                crate::settlement::GenerationResultStatus::Pending => {
                    tokio::time::sleep(RESULT_POLL_INTERVAL).await;
                }
                crate::settlement::GenerationResultStatus::Succeeded(result) => {
                    let usage = result.projection.usage.clone();
                    return render_generation_response(
                        result.images,
                        result.projection,
                        usage,
                        auth,
                    );
                }
                crate::settlement::GenerationResultStatus::Failed { error_code } => {
                    return Err(persisted_generation_error(error_code.as_deref()));
                }
                crate::settlement::GenerationResultStatus::Uncertain => {
                    return Err(ImageGatewayError::service_unavailable(
                        "generation outcome requires reconciliation",
                    ));
                }
            }
        }
    };
    tokio::time::timeout(
        external_result_wait_timeout(state.config.queue_timeout, state.config.request_timeout),
        wait,
    )
    .await
    .map_err(|_| ImageGatewayError::timeout())?
}

fn external_result_wait_timeout(queue_timeout: Duration, request_timeout: Duration) -> Duration {
    queue_timeout
        .saturating_add(request_timeout)
        .saturating_add(INLINE_LEASE_GRACE)
}

fn persisted_generation_error(error_code: Option<&str>) -> ImageGatewayError {
    match error_code {
        Some("timeout") => ImageGatewayError::timeout(),
        Some("codex_cli_failed") => ImageGatewayError::codex_cli_failed(),
        Some("codex_no_image_output") => ImageGatewayError::codex_no_image_output(),
        Some("service_unavailable") => {
            ImageGatewayError::service_unavailable("Image generation backend unavailable")
        }
        _ => ImageGatewayError::backend("Image generation failed"),
    }
}

async fn replay_generation(
    state: &Arc<AppState>,
    job_id: uuid::Uuid,
    auth: &crate::auth::AuthContext,
) -> Result<Response, ImageGatewayError> {
    let result = state
        .settlement_store
        .load_generation_result(job_id)
        .await?
        .ok_or_else(ImageGatewayError::idempotency_result_unavailable)?;
    let usage = result.projection.usage.clone();
    render_generation_response(result.images, result.projection, usage, auth)
}

fn render_generation_response(
    images: Vec<crate::generator::GeneratedImage>,
    projection: crate::artifacts::GenerationResponseProjection,
    usage: crate::usage::UsageSnapshot,
    auth: &crate::auth::AuthContext,
) -> Result<Response, ImageGatewayError> {
    let mut response = images_response_into_response(
        images_response_at(
            projection.created_at_seconds,
            images,
            projection.output_format,
            projection.quality,
            projection.size,
            projection.background,
        ),
        projection.stream,
        ImageStreamKind::Generation,
    )?;
    add_usage_headers(response.headers_mut(), &usage, auth);
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

async fn attach_ready_with_retry(
    state: &Arc<AppState>,
    request: AttachJob,
) -> Result<(), AdmissionError> {
    let mut last_error = AdmissionError::Unavailable;
    for attempt in 0..ATTACH_ATTEMPTS {
        match state.admission_store.attach(request.clone()).await {
            Ok(_) => return Ok(()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_wait_budget_includes_queue_execution_and_settlement_grace() {
        assert_eq!(
            external_result_wait_timeout(Duration::from_secs(7), Duration::from_secs(11)),
            Duration::from_secs(78),
        );
    }

    #[test]
    fn persisted_failure_codes_restore_the_original_gateway_error() {
        let timeout = persisted_generation_error(Some("timeout"));
        assert_eq!(
            timeout.status_code(),
            axum::http::StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(timeout.error_code(), Some("timeout"));

        let cli = persisted_generation_error(Some("codex_cli_failed"));
        assert_eq!(cli.status_code(), axum::http::StatusCode::BAD_GATEWAY);
        assert_eq!(cli.error_code(), Some("codex_cli_failed"));

        let no_output = persisted_generation_error(Some("codex_no_image_output"));
        assert_eq!(no_output.error_code(), Some("codex_no_image_output"));

        let unavailable = persisted_generation_error(Some("service_unavailable"));
        assert_eq!(
            unavailable.status_code(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(unavailable.error_code(), Some("service_unavailable"));
    }
}
