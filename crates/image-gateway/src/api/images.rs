use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json,
    extract::{Extension, Request, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use image_api_contracts::{
    ark::{ARK_CONTENT_GENERATION_API_PROFILE, ARK_IMAGES_API_PROFILE},
    dreamina::{DREAMINA_IMAGES_API_PROFILE, DREAMINA_VIDEOS_API_PROFILE},
    xai::XAI_IMAGES_API_PROFILE,
};
use image_provider_contracts::BillingMetric;
use image_provider_contracts::openai_codex;
use image_provider_dreamina_cli::PROVIDER_ID as DREAMINA_PROVIDER_ID;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    GenerationAdmissionContract, ImageGatewayError,
    admission::{
        AdmissionClaim, AdmissionContract, AdmissionError, AdmissionTicket, AttachInputManifest,
        AttachInputObject, AttachJob, ClaimAdmission, CustomerPricingIntent, EDIT_COMMAND_SCHEMA,
        EDIT_INPUT_MANIFEST_SCHEMA, EDIT_OPERATION, EditCommandV1, EditInputDescriptorV1,
        EditInputRoleV1, GENERATION_COMMAND_SCHEMA, GENERATION_OPERATION, GenerationCommandV1,
        PricingProcessingMode, XaiImageEditAdmissionError, XaiImageEditAdmissionPlan,
        XaiImageEditFallbackMode, idempotency_key_digest,
    },
    artifacts::{GENERATION_RESPONSE_SCHEMA, StoredGenerationResult, sha256_hex},
    auth::{ApiKeyCapability, AuthContext},
    generator::{EditJob, InputImage},
    input_blobs::{InputBlobKey, InputBlobWriteError},
    model_routing::{PublicModelRoute, ResolvedModelRoute},
    models::{
        HealthResponse, ImageStreamKind, ModelData, ModelMetadata, ModelSurface, ModelsResponse,
        images_response_at, models_response, parse_generation,
    },
    usage::UsageCharge,
};

const OPENAI_IMAGES_API_PROFILE: &str = "openai-images-v1";
const INLINE_LEASE_GRACE: Duration = Duration::from_secs(60);
const ADMISSION_DEADLINE_GRACE: Duration = Duration::from_secs(5);
const ATTACH_RETRY_DELAY: Duration = Duration::from_millis(25);
const ATTACH_ATTEMPTS: usize = 3;
const RESULT_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsoleSpatialEditMode {
    SemanticMask,
}

use super::{
    AppState, GenerationExecutionMode, IMAGE_EDIT_ROUTE_OPERATION,
    IMAGE_GENERATION_ROUTE_OPERATION, RequestId, authenticate_image_request,
    edit_input::parse_edit_request,
    middleware::new_request_id,
    resolve_request_model, resolve_surface_model,
    responses::{add_usage_headers, images_response_into_response},
    usage_limits, xai_images,
};

pub(super) async fn healthz() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

pub(super) async fn models(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ModelsResponse>, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ModelsRead)?;
    let Some(api_key_id) = auth.api_key_id.as_deref() else {
        return Ok(Json(models_response()));
    };
    let Some(authz_version) = auth.credential_authz_version else {
        return Err(ImageGatewayError::authentication());
    };
    let Some(store) = state.model_routing_store.as_ref() else {
        return Ok(Json(models_response()));
    };
    let models = store
        .list_api_key_models(&auth.project_id, api_key_id, authz_version)
        .await?;
    Ok(Json(routed_models_response(models)))
}

fn routed_models_response(models: Vec<PublicModelRoute>) -> ModelsResponse {
    let mut unique = BTreeMap::new();
    for model in models {
        let surface = dreamina_model_surface(&model);
        let entry = unique.entry(model.id.clone()).or_insert_with(|| ModelData {
            id: model.id,
            object: "model".to_owned(),
            created: model.created_at_ms.div_euclid(1_000),
            owned_by: model.provider_id,
            metadata: None,
        });
        if let Some(surface) = surface {
            entry
                .metadata
                .get_or_insert_with(|| ModelMetadata { surfaces: vec![] })
                .surfaces
                .push(surface);
        }
    }
    let data = unique
        .into_values()
        .map(|mut model| {
            if let Some(metadata) = &mut model.metadata {
                metadata.surfaces.sort();
                metadata.surfaces.dedup();
            }
            model
        })
        .collect();
    ModelsResponse {
        object: "list".to_owned(),
        data,
    }
}

fn dreamina_model_surface(model: &PublicModelRoute) -> Option<ModelSurface> {
    if model.provider_id != DREAMINA_PROVIDER_ID {
        return None;
    }
    let canonical = model.provider_model_id.as_deref()?;
    // Match the native adapter and Ark admission contracts, never the public alias.
    let workflows: &[&str] = match (
        model.api_profile.as_str(),
        model.operation_id.as_str(),
        model.media_kind.as_str(),
        canonical,
    ) {
        (DREAMINA_IMAGES_API_PROFILE, "images.generations", "image", "3.0" | "3.1") => {
            &["text2image"]
        }
        (
            DREAMINA_IMAGES_API_PROFILE,
            "images.generations",
            "image",
            "4.0" | "4.1" | "4.5" | "4.6" | "4.7" | "5.0" | "5.0Pro",
        )
        | (ARK_IMAGES_API_PROFILE, "images.generations", "image", "5.0" | "5.0Pro") => {
            &["text2image", "image2image"]
        }
        (DREAMINA_IMAGES_API_PROFILE, "images.generations", "image", "image_upscale") => {
            &["image_upscale"]
        }
        (DREAMINA_VIDEOS_API_PROFILE, "videos.generations", "video", "seedance1.0fast") => {
            &["image2video"]
        }
        (DREAMINA_VIDEOS_API_PROFILE, "videos.generations", "video", "seedance1.5pro") => {
            &["image2video", "frames2video"]
        }
        (
            DREAMINA_VIDEOS_API_PROFILE,
            "videos.generations",
            "video",
            "seedance2.0"
            | "seedance2.0fast"
            | "seedance2.0_vip"
            | "seedance2.0fast_vip"
            | "seedance2.0mini"
            | "seedance2.5",
        )
        | (
            ARK_CONTENT_GENERATION_API_PROFILE,
            "videos.generations",
            "video",
            "seedance2.0" | "seedance2.0fast" | "seedance2.0mini" | "seedance2.5",
        ) => &[
            "text2video",
            "image2video",
            "frames2video",
            "multimodal2video",
        ],
        (DREAMINA_VIDEOS_API_PROFILE, "videos.generations", "video", "multiframe2video") => {
            &["multiframe2video"]
        }
        _ => return None,
    };
    Some(ModelSurface {
        provider_id: model.provider_id.clone(),
        provider_model_id: canonical.to_owned(),
        api_profile: model.api_profile.clone(),
        operation_id: model.operation_id.clone(),
        media_kind: model.media_kind.clone(),
        workflows: workflows
            .iter()
            .map(|workflow| (*workflow).to_owned())
            .collect(),
    })
}

pub(super) async fn generations(
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesWrite)?;
    let Json(value) = body.map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })?;
    generate_with_auth(&state, auth, &headers, request_id.0, value).await
}

pub(super) async fn generate_with_auth(
    state: &Arc<AppState>,
    mut auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    value: Value,
) -> Result<Response, ImageGatewayError> {
    let requested_model = value
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let resolved_surface = match requested_model.as_deref() {
        Some(model) => {
            resolve_surface_model(
                &state,
                &mut auth,
                IMAGE_GENERATION_ROUTE_OPERATION,
                &[OPENAI_IMAGES_API_PROFILE, XAI_IMAGES_API_PROFILE],
                model,
            )
            .await?
        }
        None => None,
    };
    generate_after_surface_resolution(
        state,
        auth,
        headers,
        request_id,
        value,
        resolved_surface,
        requested_model.is_none(),
        PricingProcessingMode::Synchronous,
    )
    .await
}

pub(super) async fn generate_with_resolved_auth(
    state: &Arc<AppState>,
    auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    value: Value,
    resolved_surface: ResolvedModelRoute,
) -> Result<Response, ImageGatewayError> {
    generate_after_surface_resolution(
        state,
        auth,
        headers,
        request_id,
        value,
        Some(resolved_surface),
        false,
        PricingProcessingMode::Synchronous,
    )
    .await
}

pub(super) async fn generate_batch_with_resolved_auth(
    state: &Arc<AppState>,
    auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    value: Value,
    resolved_surface: ResolvedModelRoute,
) -> Result<Response, ImageGatewayError> {
    if resolved_surface.provider_id != openai_codex::PROVIDER_ID
        || resolved_surface.api_profile != OPENAI_IMAGES_API_PROFILE
    {
        return Err(ImageGatewayError::invalid_request(
            "This model does not support the Batch API",
            Some("model".to_string()),
            "batch_model_unsupported",
        ));
    }
    generate_after_surface_resolution(
        state,
        auth,
        headers,
        request_id,
        value,
        Some(resolved_surface),
        false,
        PricingProcessingMode::Batch,
    )
    .await
}

async fn generate_after_surface_resolution(
    state: &Arc<AppState>,
    mut auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    mut value: Value,
    resolved_surface: Option<ResolvedModelRoute>,
    resolve_default: bool,
    processing_mode: PricingProcessingMode,
) -> Result<Response, ImageGatewayError> {
    let contract = generation_admission_contract(state.config.generation_admission_contract);
    let mut execution_model_id = None;
    let mut pricing_provider_model_id = None;
    let mut public_model_id = resolved_surface
        .as_ref()
        .map(|resolved| resolved.public_model_id.clone())
        .or_else(|| {
            value
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "gpt-image-2".to_string());
    if let Some(resolved) = resolved_surface {
        execution_model_id = Some(resolved.execution_model_id.clone());
        pricing_provider_model_id = Some(resolved.provider_model_id.clone());
        set_request_model(&mut value, resolved.execution_model_id.clone())?;
        match (resolved.provider_id.as_str(), resolved.api_profile.as_str()) {
            (image_provider_grok_cli::PROVIDER_ID, XAI_IMAGES_API_PROFILE) => {
                return xai_images::create_image(
                    state,
                    &auth,
                    headers,
                    request_id,
                    value,
                    public_model_id,
                    resolved.execution_model_id,
                )
                .await;
            }
            (openai_codex::PROVIDER_ID, OPENAI_IMAGES_API_PROFILE) => {}
            _ => {
                return Err(ImageGatewayError::service_unavailable(
                    "model route does not match the images API surface",
                ));
            }
        }
    } else if looks_like_xai_image_request(&value) {
        if contract == AdmissionContract::CustomerPricingV4 {
            return Err(ImageGatewayError::service_unavailable(
                "customer pricing requires an enabled model route",
            ));
        }
        return xai_images::create_image(
            state,
            &auth,
            headers,
            request_id,
            value,
            public_model_id.clone(),
            public_model_id,
        )
        .await;
    } else if resolve_default
        && let Some(resolved) = resolve_request_model(
            &state,
            &mut auth,
            openai_codex::PROVIDER_ID,
            IMAGE_GENERATION_ROUTE_OPERATION,
            OPENAI_IMAGES_API_PROFILE,
            None,
            "gpt-image-2",
        )
        .await?
    {
        public_model_id = resolved.public_model_id;
        pricing_provider_model_id = Some(resolved.provider_model_id);
        execution_model_id = Some(resolved.execution_model_id.clone());
        set_request_model(&mut value, resolved.execution_model_id)?;
    }
    if contract == AdmissionContract::CustomerPricingV4
        && (execution_model_id.is_none() || pricing_provider_model_id.is_none())
    {
        return Err(ImageGatewayError::service_unavailable(
            "customer pricing requires an enabled model route",
        ));
    }

    let job = parse_generation(value, request_id.clone())?;
    let command = GenerationCommandV1::from_generation_job(
        &job,
        OPENAI_IMAGES_API_PROFILE,
        openai_codex::PROVIDER_ID,
    );
    let provider_command_hash = command.request_hash_hex();
    let request_hash = if contract == AdmissionContract::CustomerPricingV4 {
        crate::service_tiers::request_hash_with_project_service_tier(
            &provider_command_hash,
            auth.project_service_tier,
        )
    } else {
        provider_command_hash.clone()
    };
    let command_json = serde_json::to_value(command)
        .map_err(|_| ImageGatewayError::internal("failed to serialize durable command"))?;
    let idempotency_key_digest = idempotency_digest(&headers, &auth, GENERATION_OPERATION)?;
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
    let job_execution_model_id = job.model.clone();

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
            attribution: Some(auth.attribution()),
            request_id,
            admission_session_id: Some(ticket.session_id),
            operation: "generation",
            provider_id: openai_codex::PROVIDER_ID.to_string(),
            model: job_execution_model_id,
            output_count: units,
            billable_units: units,
            billing_metric: BillingMetric::Output,
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
    let service_tier_decision = crate::service_tiers::ServiceTierDecision::for_default_only_project(
        auth.project_service_tier,
    );
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
        contract,
        customer_pricing: (contract == AdmissionContract::CustomerPricingV4).then(|| {
            CustomerPricingIntent {
                public_model_id,
                provider_model_id: pricing_provider_model_id
                    .expect("v4 pricing model was validated before admission"),
                execution_model_id: execution_model_id
                    .expect("v4 execution model was validated before admission"),
                provider_command_hash: Some(provider_command_hash),
                media_kind: "image".to_string(),
                service_tier: service_tier_decision.effective.pricing_key().to_string(),
                service_tier_decision,
                execution_surface: "provider_cli".to_string(),
                currency: "USD".to_string(),
                pricing_dimensions: BTreeMap::from([
                    ("quality".to_string(), job.quality.clone()),
                    ("size".to_string(), job.size.clone()),
                ]),
                processing_mode,
            }
        }),
    };
    if state.generation_execution_mode == GenerationExecutionMode::External {
        if let Err(error) = attach_ready_with_retry(&state, attach).await {
            tracing::warn!(?error, "generation admission attach failed");
            if !matches!(error, AdmissionError::Unavailable) {
                rollback_generation_before_attach(&state, &ticket, &reservation).await?;
            }
            return Err(admission_error(error));
        }
        return wait_for_generation(&state, reservation.job_id, &auth).await;
    }
    let lease = match attach_and_start_with_retry(&state, attach).await {
        Ok(lease) => lease,
        Err(error) => {
            tracing::warn!(?error, "generation admission attach-and-start failed");
            if !matches!(error, AdmissionError::Unavailable) {
                rollback_generation_before_attach(&state, &ticket, &reservation).await?;
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

fn set_request_model(value: &mut Value, model: String) -> Result<(), ImageGatewayError> {
    value
        .as_object_mut()
        .ok_or_else(|| {
            ImageGatewayError::invalid_request(
                "Request body must be a JSON object",
                None,
                "invalid_json",
            )
        })?
        .insert("model".to_owned(), Value::String(model));
    Ok(())
}

fn looks_like_xai_image_request(value: &Value) -> bool {
    value.as_object().is_some_and(|object| {
        object.contains_key("aspect_ratio")
            || object.contains_key("resolution")
            || object.contains_key("storage_options")
            || object
                .get("model")
                .and_then(Value::as_str)
                .is_some_and(|model| model.starts_with("grok-imagine-"))
    })
}

pub(super) fn generation_admission_contract(
    configured: GenerationAdmissionContract,
) -> AdmissionContract {
    match configured {
        GenerationAdmissionContract::LegacyV1 => AdmissionContract::LegacyV1,
        GenerationAdmissionContract::OutputEconomicsV2 => AdmissionContract::OutputEconomicsV2,
        GenerationAdmissionContract::CustomerPricingV4 => AdmissionContract::CustomerPricingV4,
    }
}

pub(super) async fn wait_for_generation(
    state: &Arc<AppState>,
    job_id: uuid::Uuid,
    auth: &crate::auth::AuthContext,
) -> Result<Response, ImageGatewayError> {
    let result = wait_for_generation_result(state, job_id).await?;
    render_stored_generation_response(result, auth)
}

pub(super) async fn wait_for_generation_result(
    state: &Arc<AppState>,
    job_id: uuid::Uuid,
) -> Result<StoredGenerationResult, ImageGatewayError> {
    let wait = async {
        loop {
            match state.settlement_store.generation_status(job_id).await? {
                crate::settlement::GenerationResultStatus::Pending => {
                    tokio::time::sleep(RESULT_POLL_INTERVAL).await;
                }
                crate::settlement::GenerationResultStatus::Succeeded(result) => {
                    return Ok(result);
                }
                crate::settlement::GenerationResultStatus::Expired => {
                    return Err(ImageGatewayError::artifact_expired());
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
        Some("content_policy_rejected") => ImageGatewayError::content_policy_rejected(),
        Some(
            code @ ("codex_app_server_request_rejected"
            | "codex_turn_failed"
            | "codex_image_tool_failed"
            | "codex_event_capture_invalid"
            | "codex_process_exited_without_terminal"
            | "codex_multiple_image_outputs"
            | "codex_stdin_failed"
            | "codex_process_identity_unavailable"),
        ) => ImageGatewayError::codex_app_server_failure(code),
        Some("codex_no_image_output") => ImageGatewayError::codex_no_image_output(),
        Some("codex_image_tool_not_invoked") => ImageGatewayError::codex_image_tool_not_invoked(),
        Some("codex_image_output_disappeared") => {
            ImageGatewayError::codex_image_output_disappeared()
        }
        Some(
            code @ ("codex_authentication_rejected"
            | "codex_credentials_unavailable"
            | "codex_image_edit_rate_limited"
            | "codex_image_edit_upstream_unavailable"
            | "codex_image_edit_rejected"
            | "codex_image_edit_request_invalid"
            | "codex_image_edit_invalid_response"
            | "codex_image_edit_outcome_unknown"),
        ) => ImageGatewayError::codex_image_edit_failure(code),
        Some("service_unavailable") => {
            ImageGatewayError::service_unavailable("Image generation backend unavailable")
        }
        _ => ImageGatewayError::backend("Image generation failed"),
    }
}

pub(super) async fn replay_generation(
    state: &Arc<AppState>,
    job_id: uuid::Uuid,
    auth: &crate::auth::AuthContext,
) -> Result<Response, ImageGatewayError> {
    let result = replay_generation_result(state, job_id).await?;
    render_stored_generation_response(result, auth)
}

pub(super) async fn replay_generation_result(
    state: &Arc<AppState>,
    job_id: uuid::Uuid,
) -> Result<StoredGenerationResult, ImageGatewayError> {
    match state
        .settlement_store
        .load_generation_result(job_id)
        .await?
    {
        crate::settlement::GenerationResultLookup::Available(result) => Ok(result),
        crate::settlement::GenerationResultLookup::Expired => {
            Err(ImageGatewayError::idempotency_result_expired())
        }
        crate::settlement::GenerationResultLookup::Missing => {
            Err(ImageGatewayError::idempotency_result_unavailable())
        }
    }
}

pub(super) fn render_stored_generation_response(
    result: StoredGenerationResult,
    auth: &crate::auth::AuthContext,
) -> Result<Response, ImageGatewayError> {
    let usage = result.projection.usage.clone();
    render_generation_response(result.images, result.projection, usage, auth)
}

fn render_generation_response(
    images: Vec<crate::generator::GeneratedImage>,
    mut projection: crate::artifacts::GenerationResponseProjection,
    usage: crate::usage::UsageSnapshot,
    auth: &crate::auth::AuthContext,
) -> Result<Response, ImageGatewayError> {
    if projection.output_format == "auto" {
        projection.output_format = inferred_output_format(&images)?;
    }
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

fn inferred_output_format(
    images: &[crate::generator::GeneratedImage],
) -> Result<String, ImageGatewayError> {
    let mut detected = None;
    for image in images {
        let format = match crate::artifacts::media_type_from_bytes(&image.bytes)
            .map_err(|_| ImageGatewayError::artifact_integrity())?
        {
            "image/png" => "png",
            "image/jpeg" => "jpeg",
            "image/webp" => "webp",
            _ => return Err(ImageGatewayError::artifact_integrity()),
        };
        if detected.is_some_and(|existing| existing != format) {
            return Err(ImageGatewayError::artifact_integrity());
        }
        detected = Some(format);
    }
    detected
        .map(str::to_owned)
        .ok_or_else(ImageGatewayError::artifact_integrity)
}

fn idempotency_digest(
    headers: &HeaderMap,
    auth: &AuthContext,
    operation: &str,
) -> Result<Option<String>, ImageGatewayError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = value
        .to_str()
        .map_err(|_| ImageGatewayError::invalid_idempotency_key())?;
    let scope = idempotency_scope(auth);
    idempotency_key_digest(&scope, OPENAI_IMAGES_API_PROFILE, operation, key)
        .map(Some)
        .map_err(|_| ImageGatewayError::invalid_idempotency_key())
}

fn idempotency_scope(auth: &AuthContext) -> String {
    auth.actor_user_id.map_or_else(
        || auth.project_id.clone(),
        |user_id| format!("{}:user:{user_id}", auth.project_id),
    )
}

pub(super) async fn abort_before_attach(
    state: &Arc<AppState>,
    ticket: &AdmissionTicket,
) -> Result<(), ImageGatewayError> {
    state
        .admission_store
        .abort(ticket)
        .await
        .map_err(admission_error)
}

async fn rollback_generation_before_attach(
    state: &Arc<AppState>,
    ticket: &AdmissionTicket,
    reservation: &crate::usage::UsageReservation,
) -> Result<(), ImageGatewayError> {
    let abort = state.admission_store.abort(ticket).await;
    let release = state
        .usage_store
        .release(reservation, "admission_attach_failed")
        .await;
    let abort_failed = matches!(
        abort,
        Err(AdmissionError::Unavailable
            | AdmissionError::StaleLease
            | AdmissionError::InvalidCommand)
    );
    if abort_failed || release.is_err() {
        return Err(ImageGatewayError::service_unavailable(
            "generation admission cleanup unavailable",
        ));
    }
    Ok(())
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

pub(super) async fn attach_ready_with_retry(
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

pub(super) fn admission_error(error: AdmissionError) -> ImageGatewayError {
    match error {
        AdmissionError::Expired => ImageGatewayError::timeout(),
        AdmissionError::BillingLimitExceeded => ImageGatewayError::billing_limit_exceeded(),
        AdmissionError::ProjectBudgetExceeded => ImageGatewayError::project_budget_exceeded(),
        AdmissionError::Unavailable
        | AdmissionError::PricingUnavailable
        | AdmissionError::InvalidOwner
        | AdmissionError::StaleLease
        | AdmissionError::InvalidCommand => {
            ImageGatewayError::service_unavailable("durable admission is unavailable")
        }
    }
}

fn xai_edit_admission_error(error: XaiImageEditAdmissionError) -> ImageGatewayError {
    let parameter = match error {
        XaiImageEditAdmissionError::UnsupportedMask => "mask",
        XaiImageEditAdmissionError::UnsupportedOutputCount => "n",
        XaiImageEditAdmissionError::UnsupportedStreaming => "stream",
        XaiImageEditAdmissionError::UnsupportedOutputCompression => "output_compression",
        XaiImageEditAdmissionError::UnsupportedAspectRatio => "size",
        XaiImageEditAdmissionError::InvalidProviderRequest(_) => "image",
        XaiImageEditAdmissionError::InvalidProviderCommand => {
            return ImageGatewayError::internal("failed to encode durable Grok image edit command");
        }
    };
    ImageGatewayError::unsupported(parameter, error.to_string())
}

pub(super) fn admission_deadline(config: &crate::AppConfig) -> i64 {
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
    auth.require_api_key_capability(ApiKeyCapability::ImagesWrite)?;
    edit_with_resolved_auth(&state, auth, request).await
}

pub(super) async fn edit_with_resolved_auth(
    state: &Arc<AppState>,
    mut auth: AuthContext,
    request: Request,
) -> Result<Response, ImageGatewayError> {
    let headers = request.headers().clone();
    let console_spatial_edit_mode = request
        .extensions()
        .get::<ConsoleSpatialEditMode>()
        .copied();
    let upload_permit = state.upload_scheduler.acquire(&auth.tenant_id).await?;

    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map(|request_id| request_id.0.clone())
        .unwrap_or_else(new_request_id);
    let contract = generation_admission_contract(state.config.generation_admission_contract);
    let mut form = parse_edit_request(request, &state).await?;
    let requested_model = form
        .model
        .clone()
        .unwrap_or_else(|| "gpt-image-2".to_string());
    let resolved = resolve_surface_model(
        &state,
        &mut auth,
        IMAGE_EDIT_ROUTE_OPERATION,
        &[OPENAI_IMAGES_API_PROFILE, XAI_IMAGES_API_PROFILE],
        &requested_model,
    )
    .await?;
    let (public_model_id, execution_model_id, provider_id, api_profile, provider_model_id) =
        resolved.map_or_else(
            || {
                (
                    requested_model.clone(),
                    None,
                    openai_codex::PROVIDER_ID.to_owned(),
                    OPENAI_IMAGES_API_PROFILE.to_owned(),
                    requested_model.clone(),
                )
            },
            |resolved| {
                (
                    resolved.public_model_id,
                    Some(resolved.execution_model_id),
                    resolved.provider_id,
                    resolved.api_profile,
                    resolved.provider_model_id,
                )
            },
        );
    if contract == AdmissionContract::CustomerPricingV4 && execution_model_id.is_none() {
        return Err(ImageGatewayError::service_unavailable(
            "customer pricing requires an enabled image edit model route",
        ));
    }
    if provider_id == image_provider_grok_cli::PROVIDER_ID
        && state.generation_execution_mode != GenerationExecutionMode::External
    {
        return Err(ImageGatewayError::service_unavailable(
            "Grok image editing requires external provider execution",
        ));
    }
    let job_execution_model_id = execution_model_id
        .as_deref()
        .unwrap_or(&provider_model_id)
        .to_owned();
    form.model = Some(if provider_id == image_provider_grok_cli::PROVIDER_ID {
        "gpt-image-2".to_owned()
    } else {
        job_execution_model_id.clone()
    });
    let mut job = form.into_job(request_id.clone())?;
    job.model = job_execution_model_id;
    let descriptors = edit_input_descriptors(&job)?;
    let (command_schema, command_json, provider_command_hash, input_manifest_hash) = if provider_id
        == image_provider_grok_cli::PROVIDER_ID
    {
        let plan = match console_spatial_edit_mode {
            Some(ConsoleSpatialEditMode::SemanticMask) => {
                XaiImageEditAdmissionPlan::for_grok_cli_with_fallback(
                    &job,
                    descriptors,
                    XaiImageEditFallbackMode::SemanticMask,
                )
            }
            None => XaiImageEditAdmissionPlan::for_grok_cli(&job, descriptors),
        }
        .map_err(xai_edit_admission_error)?;
        (
            plan.command_schema().to_owned(),
            plan.provider_command().clone(),
            plan.source_request_hash(),
            plan.input_manifest_hash(),
        )
    } else if provider_id == openai_codex::PROVIDER_ID {
        let command = EditCommandV1::from_edit_job(&job, descriptors, &api_profile, &provider_id);
        let provider_command_hash = command.request_hash_hex();
        let input_manifest_hash = command.input_manifest_hash_hex();
        (
            EDIT_COMMAND_SCHEMA.to_owned(),
            serde_json::to_value(command).map_err(|_| {
                ImageGatewayError::internal("failed to serialize durable edit command")
            })?,
            provider_command_hash,
            input_manifest_hash,
        )
    } else {
        return Err(ImageGatewayError::unsupported(
            "model",
            "the selected provider does not expose image editing",
        ));
    };
    let request_hash = if contract == AdmissionContract::CustomerPricingV4 {
        crate::service_tiers::request_hash_with_project_service_tier(
            &provider_command_hash,
            auth.project_service_tier,
        )
    } else {
        provider_command_hash.clone()
    };
    let idempotency_key_digest = idempotency_digest(&headers, &auth, EDIT_OPERATION)?;
    let ticket = match claim_admission_with_retry(
        &state,
        ClaimAdmission {
            owner_token: Uuid::new_v4(),
            tenant_id: auth.tenant_id.clone(),
            project_id: auth.project_id.clone(),
            api_profile: api_profile.clone(),
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
    let job_execution_model_id = job.model.clone();

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
            attribution: Some(auth.attribution()),
            request_id: request_id.clone(),
            admission_session_id: Some(ticket.session_id),
            operation: EDIT_OPERATION,
            provider_id: provider_id.clone(),
            model: job_execution_model_id,
            output_count: units,
            billable_units: units,
            billing_metric: BillingMetric::Output,
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
    let service_tier_decision = crate::service_tiers::ServiceTierDecision::for_default_only_project(
        auth.project_service_tier,
    );
    let attach = AttachJob {
        ticket: ticket.clone(),
        job_id: reservation.job_id,
        command_schema,
        command_json,
        input_manifest: Some(AttachInputManifest {
            manifest_schema: EDIT_INPUT_MANIFEST_SCHEMA.to_string(),
            manifest_hash: input_manifest_hash,
            inputs,
        }),
        work_kind: "image_batch".to_string(),
        schedule_scope: format!("tenant:{}", auth.tenant_id),
        schedule_weight: 1,
        schedule_priority: 1,
        schedule_cost: u64::from(units),
        contract,
        customer_pricing: (contract == AdmissionContract::CustomerPricingV4).then(|| {
            CustomerPricingIntent {
                public_model_id,
                provider_model_id,
                execution_model_id: execution_model_id
                    .expect("v4 edit execution model was validated before admission"),
                provider_command_hash: Some(provider_command_hash),
                media_kind: "image".to_string(),
                service_tier: service_tier_decision.effective.pricing_key().to_string(),
                service_tier_decision,
                execution_surface: "provider_cli".to_string(),
                currency: "USD".to_string(),
                pricing_dimensions: if provider_id == image_provider_grok_cli::PROVIDER_ID {
                    BTreeMap::from([
                        (
                            "aspect_ratio".to_string(),
                            if job.images.len() == 1 {
                                "auto".to_string()
                            } else {
                                job.size.clone()
                            },
                        ),
                        ("resolution".to_string(), "1k".to_string()),
                    ])
                } else {
                    BTreeMap::from([
                        ("quality".to_string(), job.quality.clone()),
                        ("size".to_string(), job.size.clone()),
                    ])
                },
                processing_mode: crate::admission::PricingProcessingMode::Synchronous,
            }
        }),
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
            &api_profile,
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

pub(super) async fn reserve_with_retry(
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

pub(super) async fn claim_admission_with_retry(
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

    fn model_route(
        id: &str,
        canonical: &str,
        profile: &str,
        operation: &str,
        media_kind: &str,
    ) -> PublicModelRoute {
        PublicModelRoute {
            id: id.to_owned(),
            provider_model_id: Some(canonical.to_owned()),
            api_profile: profile.to_owned(),
            provider_id: DREAMINA_PROVIDER_ID.to_owned(),
            operation_id: operation.to_owned(),
            media_kind: media_kind.to_owned(),
            created_at_ms: 1_234,
        }
    }

    #[test]
    fn public_model_metadata_matches_adapter_workflow_families() {
        let image = "images.generations";
        let video = "videos.generations";
        let image_workflows = &["text2image", "image2image"][..];
        let video_workflows = &[
            "text2video",
            "image2video",
            "frames2video",
            "multimodal2video",
        ][..];
        let cases = [
            (
                DREAMINA_IMAGES_API_PROFILE,
                image,
                "image",
                &["3.0", "3.1"][..],
                &["text2image"][..],
            ),
            (
                DREAMINA_IMAGES_API_PROFILE,
                image,
                "image",
                &["4.0", "4.1", "4.5", "4.6", "4.7", "5.0", "5.0Pro"][..],
                image_workflows,
            ),
            (
                DREAMINA_IMAGES_API_PROFILE,
                image,
                "image",
                &["image_upscale"][..],
                &["image_upscale"][..],
            ),
            (
                DREAMINA_VIDEOS_API_PROFILE,
                video,
                "video",
                &["seedance1.0fast"][..],
                &["image2video"][..],
            ),
            (
                DREAMINA_VIDEOS_API_PROFILE,
                video,
                "video",
                &["seedance1.5pro"][..],
                &["image2video", "frames2video"][..],
            ),
            (
                DREAMINA_VIDEOS_API_PROFILE,
                video,
                "video",
                &[
                    "seedance2.0",
                    "seedance2.0fast",
                    "seedance2.0_vip",
                    "seedance2.0fast_vip",
                    "seedance2.0mini",
                    "seedance2.5",
                ][..],
                video_workflows,
            ),
            (
                DREAMINA_VIDEOS_API_PROFILE,
                video,
                "video",
                &["multiframe2video"][..],
                &["multiframe2video"][..],
            ),
            (
                ARK_IMAGES_API_PROFILE,
                image,
                "image",
                &["5.0", "5.0Pro"][..],
                image_workflows,
            ),
            (
                ARK_CONTENT_GENERATION_API_PROFILE,
                video,
                "video",
                &[
                    "seedance2.0",
                    "seedance2.0fast",
                    "seedance2.0mini",
                    "seedance2.5",
                ][..],
                video_workflows,
            ),
        ];
        for (profile, operation, media, canonical_models, expected) in cases {
            for canonical in canonical_models {
                let surface = dreamina_model_surface(&model_route(
                    "custom-alias",
                    canonical,
                    profile,
                    operation,
                    media,
                ))
                .unwrap();
                assert_eq!(surface.provider_model_id, *canonical);
                assert_eq!(surface.api_profile, profile);
                assert_eq!(surface.operation_id, operation);
                assert_eq!(surface.media_kind, media);
                assert_eq!(surface.workflows, expected, "{profile}/{canonical}");
            }
        }
    }

    #[test]
    fn public_model_metadata_retains_distinct_surfaces_and_existing_fields() {
        let native = model_route(
            "shared",
            "5.0Pro",
            DREAMINA_IMAGES_API_PROFILE,
            "images.generations",
            "image",
        );
        let ark = model_route(
            "shared",
            "5.0Pro",
            ARK_IMAGES_API_PROFILE,
            "images.generations",
            "image",
        );
        let video = model_route(
            "shared",
            "seedance2.5",
            DREAMINA_VIDEOS_API_PROFILE,
            "videos.generations",
            "video",
        );
        let mut duplicate = native.clone();
        duplicate.created_at_ms = 8_999;
        let mut legacy = native.clone();
        legacy.id = "legacy".to_owned();
        legacy.provider_id = "grok-cli".to_owned();
        let response = routed_models_response(vec![native, video, legacy, duplicate, ark]);
        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["object"], "list");
        assert_eq!(
            value["data"][0],
            serde_json::json!({
                "id":"legacy", "object":"model", "created":1, "owned_by":"grok-cli"
            })
        );
        let shared = &value["data"][1];
        assert_eq!(shared["id"], "shared");
        assert_eq!(shared["object"], "model");
        assert_eq!(shared["created"], 1);
        assert_eq!(shared["owned_by"], DREAMINA_PROVIDER_ID);
        let surfaces = shared["metadata"]["surfaces"].as_array().unwrap();
        assert_eq!(surfaces.len(), 3);
        assert_eq!(surfaces[0]["api_profile"], DREAMINA_IMAGES_API_PROFILE);
        assert_eq!(surfaces[1]["api_profile"], ARK_IMAGES_API_PROFILE);
        assert_eq!(surfaces[2]["api_profile"], DREAMINA_VIDEOS_API_PROFILE);
        assert!(
            surfaces
                .iter()
                .all(|surface| surface.as_object().unwrap().len() == 6)
        );
    }

    #[test]
    fn public_model_metadata_uses_configured_canonical_identity_for_aliases() {
        let routes = vec![
            model_route(
                "native-custom",
                "5.0Pro",
                DREAMINA_IMAGES_API_PROFILE,
                "images.generations",
                "image",
            ),
            model_route(
                "doubao-seedream-5-0-260128",
                "5.0Pro",
                ARK_IMAGES_API_PROFILE,
                "images.generations",
                "image",
            ),
            model_route(
                "seedance2.5",
                "5.0Pro",
                DREAMINA_IMAGES_API_PROFILE,
                "images.generations",
                "image",
            ),
        ];
        let value = serde_json::to_value(routed_models_response(routes)).unwrap();
        assert_eq!(value["data"].as_array().unwrap().len(), 3);
        for model in value["data"].as_array().unwrap() {
            assert_eq!(
                model["metadata"]["surfaces"][0]["provider_model_id"],
                "5.0Pro"
            );
            assert_eq!(
                model["metadata"]["surfaces"][0]["workflows"],
                serde_json::json!(["text2image", "image2image"])
            );
        }
    }

    #[test]
    fn public_model_metadata_omits_unknown_and_mismatched_surfaces() {
        let mut routes = vec![
            model_route(
                "unknown",
                "future-version",
                DREAMINA_IMAGES_API_PROFILE,
                "images.generations",
                "image",
            ),
            model_route(
                "wrong-profile",
                "5.0",
                "openai-images-v1",
                "images.generations",
                "image",
            ),
            model_route(
                "wrong-operation",
                "5.0",
                DREAMINA_IMAGES_API_PROFILE,
                "images.edits",
                "image",
            ),
            model_route(
                "wrong-media",
                "5.0",
                DREAMINA_IMAGES_API_PROFILE,
                "images.generations",
                "video",
            ),
            model_route(
                "ark-unsupported-image",
                "4.0",
                ARK_IMAGES_API_PROFILE,
                "images.generations",
                "image",
            ),
            model_route(
                "ark-unsupported-video",
                "seedance2.0_vip",
                ARK_CONTENT_GENERATION_API_PROFILE,
                "videos.generations",
                "video",
            ),
            model_route(
                "ark-fixed-image",
                "image_upscale",
                ARK_IMAGES_API_PROFILE,
                "images.generations",
                "image",
            ),
            model_route(
                "ark-fixed-video",
                "multiframe2video",
                ARK_CONTENT_GENERATION_API_PROFILE,
                "videos.generations",
                "video",
            ),
        ];
        let mut missing = model_route(
            "missing",
            "5.0",
            DREAMINA_IMAGES_API_PROFILE,
            "images.generations",
            "image",
        );
        missing.provider_model_id = None;
        routes.push(missing);
        let mut other = model_route(
            "other",
            "5.0",
            DREAMINA_IMAGES_API_PROFILE,
            "images.generations",
            "image",
        );
        other.provider_id = "grok-cli".to_owned();
        routes.push(other);
        let value = serde_json::to_value(routed_models_response(routes)).unwrap();
        assert!(
            value["data"]
                .as_array()
                .unwrap()
                .iter()
                .all(|model| model.as_object().unwrap().len() == 4)
        );
        let fallback = serde_json::to_value(models_response()).unwrap();
        assert!(
            fallback["data"]
                .as_array()
                .unwrap()
                .iter()
                .all(|model| model.get("metadata").is_none())
        );
    }

    #[test]
    fn external_wait_budget_includes_queue_execution_and_settlement_grace() {
        assert_eq!(
            external_result_wait_timeout(Duration::from_secs(7), Duration::from_secs(11)),
            Duration::from_secs(78),
        );
    }

    #[test]
    fn generation_contract_is_selected_explicitly() {
        assert_eq!(
            generation_admission_contract(GenerationAdmissionContract::LegacyV1),
            AdmissionContract::LegacyV1
        );
        assert_eq!(
            generation_admission_contract(GenerationAdmissionContract::OutputEconomicsV2),
            AdmissionContract::OutputEconomicsV2
        );
        assert_eq!(
            generation_admission_contract(GenerationAdmissionContract::CustomerPricingV4),
            AdmissionContract::CustomerPricingV4
        );
    }

    #[test]
    fn billing_limit_is_not_reported_as_request_rate_limiting() {
        let error = admission_error(AdmissionError::BillingLimitExceeded);
        assert_eq!(
            error.status_code(),
            axum::http::StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(error.error_code(), Some("billing_limit_exceeded"));
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

        let content_policy = persisted_generation_error(Some("content_policy_rejected"));
        assert_eq!(
            content_policy.status_code(),
            axum::http::StatusCode::BAD_REQUEST
        );
        assert_eq!(content_policy.error_code(), Some("content_policy_rejected"));

        for code in [
            "codex_app_server_request_rejected",
            "codex_turn_failed",
            "codex_image_tool_failed",
            "codex_event_capture_invalid",
            "codex_process_exited_without_terminal",
            "codex_multiple_image_outputs",
            "codex_stdin_failed",
            "codex_process_identity_unavailable",
        ] {
            let error = persisted_generation_error(Some(code));
            assert_eq!(error.status_code(), axum::http::StatusCode::BAD_GATEWAY);
            assert_eq!(error.error_code(), Some(code));
        }

        let no_output = persisted_generation_error(Some("codex_no_image_output"));
        assert_eq!(no_output.error_code(), Some("codex_no_image_output"));
        let tool_not_invoked = persisted_generation_error(Some("codex_image_tool_not_invoked"));
        assert_eq!(
            tool_not_invoked.error_code(),
            Some("codex_image_tool_not_invoked")
        );
        let output_disappeared = persisted_generation_error(Some("codex_image_output_disappeared"));
        assert_eq!(
            output_disappeared.error_code(),
            Some("codex_image_output_disappeared")
        );

        for code in [
            "codex_authentication_rejected",
            "codex_credentials_unavailable",
            "codex_image_edit_rate_limited",
            "codex_image_edit_upstream_unavailable",
            "codex_image_edit_rejected",
            "codex_image_edit_request_invalid",
            "codex_image_edit_invalid_response",
            "codex_image_edit_outcome_unknown",
        ] {
            let error = persisted_generation_error(Some(code));
            assert_eq!(error.error_code(), Some(code));
        }
        assert_eq!(
            persisted_generation_error(Some("codex_image_edit_rate_limited")).status_code(),
            axum::http::StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            persisted_generation_error(Some("codex_authentication_rejected")).status_code(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );

        let unavailable = persisted_generation_error(Some("service_unavailable"));
        assert_eq!(
            unavailable.status_code(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(unavailable.error_code(), Some("service_unavailable"));
    }
}
