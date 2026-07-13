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
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    admission::{
        AdmissionClaim, AdmissionContract, AdmissionError, AdmissionTicket, AttachInputManifest,
        AttachInputObject, AttachJob, ClaimAdmission, EDIT_COMMAND_SCHEMA,
        EDIT_INPUT_MANIFEST_SCHEMA, EDIT_OPERATION, EditCommandV1, EditInputDescriptorV1,
        EditInputRoleV1, GENERATION_COMMAND_SCHEMA, GENERATION_OPERATION, GenerationCommandV1,
        idempotency_key_digest,
    },
    artifacts::{GENERATION_RESPONSE_SCHEMA, sha256_hex},
    generator::{EditJob, InputImage},
    input_blobs::{InputBlobKey, InputBlobWriteError},
    models::{
        HealthResponse, ImageStreamKind, images_response_at, models_response, parse_generation,
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
    responses::{add_usage_headers, images_response_into_response},
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
    let idempotency_key_digest =
        idempotency_digest(&headers, &auth.project_id, GENERATION_OPERATION)?;
    let ticket = match claim_admission_with_retry(
        &state,
        ClaimAdmission {
            owner_token: Uuid::new_v4(),
            tenant_id: auth.tenant_id.clone(),
            project_id: auth.project_id.clone(),
            api_profile: OPENAI_IMAGES_API_PROFILE.to_string(),
            operation: GENERATION_OPERATION.to_string(),
            request_id: request_id.clone(),
            idempotency_key_digest,
            request_hash,
            deadline_at_ms: admission_deadline(&state.config),
        },
    )
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
    let reservation = match reserve_with_retry(
        &state,
        UsageCharge {
            tenant_id: auth.tenant_id.clone(),
            request_id,
            admission_session_id: Some(ticket.session_id),
            operation: "generation",
            provider_id: openai_codex::PROVIDER_ID.to_string(),
            model,
            units,
            limits: usage_limits(&state.config),
        },
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            if error.error_code() != Some("service_unavailable") {
                abort_before_attach(&state, &ticket).await?;
            }
            return Err(error);
        }
    };
    let attach = AttachJob {
        ticket: ticket.clone(),
        job_id: reservation.job_id,
        command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
        command_json,
        input_manifest: None,
        work_kind: "image_batch".to_string(),
        schedule_scope: format!("tenant:{}", auth.tenant_id),
        schedule_weight: 1,
        schedule_priority: 1,
        schedule_cost: u64::from(units),
        contract: AdmissionContract::LegacyV1,
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

    let generation_worker = state
        .generation_worker
        .as_ref()
        .ok_or_else(|| ImageGatewayError::internal("inline generation worker is unavailable"))?;
    let execution = generation_worker
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
    let stream_kind = match projection.operation.as_str() {
        GENERATION_OPERATION => ImageStreamKind::Generation,
        crate::admission::EDIT_OPERATION => ImageStreamKind::Edit,
        _ => {
            return Err(ImageGatewayError::internal(
                "stored image response operation is invalid",
            ));
        }
    };
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
        stream_kind,
    )?;
    add_usage_headers(response.headers_mut(), &usage, auth);
    Ok(response)
}

fn idempotency_digest(
    headers: &HeaderMap,
    project_id: &str,
    operation: &str,
) -> Result<Option<String>, ImageGatewayError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = value
        .to_str()
        .map_err(|_| ImageGatewayError::invalid_idempotency_key())?;
    idempotency_key_digest(project_id, OPENAI_IMAGES_API_PROFILE, operation, key)
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
        AdmissionError::BillingLimitExceeded => ImageGatewayError::queue_overloaded(),
        AdmissionError::Unavailable
        | AdmissionError::PricingUnavailable
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
    let upload_permit = state.upload_scheduler.acquire(&auth.tenant_id).await?;

    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map(|request_id| request_id.0.clone())
        .unwrap_or_else(new_request_id);
    let form = parse_edit_request(request, &state).await?;
    let job = form.into_job(request_id.clone())?;
    let descriptors = edit_input_descriptors(&job)?;
    let command = EditCommandV1::from_edit_job(
        &job,
        descriptors,
        OPENAI_IMAGES_API_PROFILE,
        openai_codex::PROVIDER_ID,
    );
    let request_hash = command.request_hash_hex();
    let idempotency_key_digest = idempotency_digest(&headers, &auth.project_id, EDIT_OPERATION)?;
    let ticket = match claim_admission_with_retry(
        &state,
        ClaimAdmission {
            owner_token: Uuid::new_v4(),
            tenant_id: auth.tenant_id.clone(),
            project_id: auth.project_id.clone(),
            api_profile: OPENAI_IMAGES_API_PROFILE.to_string(),
            operation: EDIT_OPERATION.to_string(),
            request_id: request_id.clone(),
            idempotency_key_digest,
            request_hash,
            deadline_at_ms: admission_deadline(&state.config),
        },
    )
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
    let reservation = match reserve_with_retry(
        &state,
        UsageCharge {
            tenant_id: auth.tenant_id.clone(),
            request_id: request_id.clone(),
            admission_session_id: Some(ticket.session_id),
            operation: EDIT_OPERATION,
            provider_id: openai_codex::PROVIDER_ID.to_string(),
            model,
            units,
            limits: usage_limits(&state.config),
        },
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            if error.error_code() != Some("service_unavailable") {
                abort_before_attach(&state, &ticket).await?;
            }
            return Err(error);
        }
    };
    let inputs = match stage_edit_inputs(&state, &ticket, &job).await {
        Ok(inputs) => inputs,
        Err(error) => {
            rollback_edit_before_attach(&state, &ticket, &reservation).await?;
            return Err(error);
        }
    };
    let attach = AttachJob {
        ticket: ticket.clone(),
        job_id: reservation.job_id,
        command_schema: EDIT_COMMAND_SCHEMA.to_string(),
        command_json: serde_json::to_value(&command)
            .map_err(|_| ImageGatewayError::internal("failed to serialize durable edit command"))?,
        input_manifest: Some(AttachInputManifest {
            manifest_schema: EDIT_INPUT_MANIFEST_SCHEMA.to_string(),
            manifest_hash: command.input_manifest_hash_hex(),
            inputs,
        }),
        work_kind: "image_batch".to_string(),
        schedule_scope: format!("tenant:{}", auth.tenant_id),
        schedule_weight: 1,
        schedule_priority: 1,
        schedule_cost: u64::from(units),
        contract: AdmissionContract::LegacyV1,
    };
    if state.generation_execution_mode == GenerationExecutionMode::External {
        if let Err(error) = attach_ready_with_retry(&state, attach).await {
            if !matches!(error, AdmissionError::Unavailable) {
                rollback_edit_before_attach(&state, &ticket, &reservation).await?;
            }
            return Err(admission_error(error));
        }
        drop(upload_permit);
        return wait_for_generation(&state, reservation.job_id, &auth).await;
    }
    let lease = match attach_and_start_with_retry(&state, attach).await {
        Ok(lease) => lease,
        Err(error) => {
            if !matches!(error, AdmissionError::Unavailable) {
                rollback_edit_before_attach(&state, &ticket, &reservation).await?;
            }
            return Err(admission_error(error));
        }
    };
    drop(upload_permit);
    let generation_worker = state
        .generation_worker
        .as_ref()
        .ok_or_else(|| ImageGatewayError::internal("inline edit worker is unavailable"))?;
    let execution = generation_worker
        .execute_edit(
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

fn edit_input_descriptors(job: &EditJob) -> Result<Vec<EditInputDescriptorV1>, ImageGatewayError> {
    let mut descriptors = Vec::with_capacity(job.images.len() + usize::from(job.mask.is_some()));
    for (index, image) in job.images.iter().enumerate() {
        descriptors.push(edit_input_descriptor(
            image,
            EditInputRoleV1::Image,
            u16::try_from(index)
                .map_err(|_| ImageGatewayError::internal("edit input index overflow"))?,
        )?);
    }
    if let Some(mask) = &job.mask {
        descriptors.push(edit_input_descriptor(mask, EditInputRoleV1::Mask, 0)?);
    }
    Ok(descriptors)
}

fn edit_input_descriptor(
    input: &InputImage,
    role: EditInputRoleV1,
    index: u16,
) -> Result<EditInputDescriptorV1, ImageGatewayError> {
    let media_type = input
        .content_type
        .as_deref()
        .filter(|value| matches!(*value, "image/png" | "image/jpeg" | "image/webp"))
        .ok_or_else(|| ImageGatewayError::internal("normalized edit input has no media type"))?;
    Ok(EditInputDescriptorV1 {
        byte_size: u64::try_from(input.bytes.len())
            .map_err(|_| ImageGatewayError::internal("edit input size overflow"))?,
        index,
        media_type: media_type.to_string(),
        role,
        sha256_hex: sha256_hex(&input.bytes),
    })
}

async fn stage_edit_inputs(
    state: &Arc<AppState>,
    ticket: &AdmissionTicket,
    job: &EditJob,
) -> Result<Vec<AttachInputObject>, ImageGatewayError> {
    let mut inputs = Vec::with_capacity(job.images.len() + usize::from(job.mask.is_some()));
    for (index, image) in job.images.iter().enumerate() {
        inputs.push(
            stage_edit_input(
                state,
                ticket,
                image,
                EditInputRoleV1::Image,
                u16::try_from(index)
                    .map_err(|_| ImageGatewayError::internal("edit input index overflow"))?,
            )
            .await?,
        );
    }
    if let Some(mask) = &job.mask {
        inputs.push(stage_edit_input(state, ticket, mask, EditInputRoleV1::Mask, 0).await?);
    }
    Ok(inputs)
}

async fn stage_edit_input(
    state: &Arc<AppState>,
    ticket: &AdmissionTicket,
    input: &InputImage,
    role: EditInputRoleV1,
    index: u16,
) -> Result<AttachInputObject, ImageGatewayError> {
    let descriptor = edit_input_descriptor(input, role, index)?;
    let key = InputBlobKey {
        admission_session_id: ticket.session_id,
        input_id: uuid::Uuid::new_v4(),
    };
    let blob = state
        .input_blob_store
        .put(key.clone(), &input.bytes)
        .await
        .map_err(map_input_blob_write_error)?;
    if blob.key != key
        || blob.sha256_hex != descriptor.sha256_hex
        || blob.byte_size != descriptor.byte_size
        || blob.storage_backend.is_empty()
        || blob.object_key.is_empty()
    {
        return Err(ImageGatewayError::service_unavailable(
            "input storage returned invalid metadata",
        ));
    }
    Ok(AttachInputObject {
        blob,
        role,
        index,
        media_type: descriptor.media_type,
    })
}

async fn rollback_edit_before_attach(
    state: &Arc<AppState>,
    ticket: &AdmissionTicket,
    reservation: &crate::usage::UsageReservation,
) -> Result<(), ImageGatewayError> {
    let abort = state.admission_store.abort(ticket).await;
    let release = state
        .usage_store
        .release(reservation, "edit_admission_failed")
        .await;
    let delete = state
        .input_blob_store
        .delete_session(ticket.session_id)
        .await;
    let abort_failed = matches!(
        abort,
        Err(AdmissionError::Unavailable
            | AdmissionError::StaleLease
            | AdmissionError::InvalidCommand)
    );
    if abort_failed || release.is_err() || delete.is_err() {
        return Err(ImageGatewayError::service_unavailable(
            "edit admission cleanup unavailable",
        ));
    }
    Ok(())
}

fn map_input_blob_write_error(_: InputBlobWriteError) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("input storage unavailable")
}

async fn reserve_with_retry(
    state: &Arc<AppState>,
    charge: UsageCharge,
) -> Result<crate::usage::UsageReservation, ImageGatewayError> {
    let mut last_error = ImageGatewayError::service_unavailable("quota state unavailable");
    for attempt in 0..ATTACH_ATTEMPTS {
        match state.usage_store.reserve(charge.clone()).await {
            Ok(reservation) => return Ok(reservation),
            Err(error)
                if error.error_code() == Some("service_unavailable")
                    && attempt + 1 < ATTACH_ATTEMPTS =>
            {
                last_error = error;
                tokio::time::sleep(ATTACH_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error)
}

async fn claim_admission_with_retry(
    state: &Arc<AppState>,
    claim: ClaimAdmission,
) -> Result<AdmissionClaim, AdmissionError> {
    for attempt in 0..ATTACH_ATTEMPTS {
        match state.admission_store.claim(claim.clone()).await {
            Err(AdmissionError::Unavailable) if attempt + 1 < ATTACH_ATTEMPTS => {
                tokio::time::sleep(ATTACH_RETRY_DELAY).await;
            }
            result => return result,
        }
    }
    Err(AdmissionError::Unavailable)
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
