use std::sync::Arc;

use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use image_api_contracts::dreamina::{
    DREAMINA_IMAGES_API_PROFILE, DREAMINA_VIDEOS_API_PROFILE, DreaminaCancellation,
    DreaminaImageData, DreaminaImageGenerationRequest, DreaminaImageRequest, DreaminaImageTask,
    DreaminaImageWorkflowRequest, DreaminaManualCancellation, DreaminaProviderProgress,
    DreaminaTaskCreated, DreaminaTaskError, DreaminaVideoContent, DreaminaVideoGenerationRequest,
    DreaminaVideoRequest, DreaminaVideoTask, DreaminaVideoWorkflowRequest,
};
use image_provider_contracts::BillingMetric;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    admission::{
        AdmissionClaim, AdmissionContract, AdmissionTicket, AttachJob, ClaimAdmission,
        CustomerPricingIntent, DreaminaAdmissionError, DreaminaImageAdmissionPlan,
        DreaminaVideoAdmissionPlan, DreaminaWorkflowIntent, GENERATION_OPERATION,
        VIDEO_GENERATION_OPERATION, idempotency_key_digest,
    },
    artifacts::StoredGenerationResult,
    auth::{ApiKeyCapability, AuthContext},
    model_routing::ResolvedModelRoute,
    settlement::{
        DreaminaManualVideoCancellationProof, ExecutionSettlementStore, ImageResultStatus,
        ImageTaskProjection, VideoPendingStage, VideoProviderProgress, VideoResultStatus,
    },
    usage::{UsageCharge, UsageLimits},
};

use super::{
    AppState, GenerationExecutionMode, RequestId, authenticate_image_request,
    images::{
        abort_before_attach, admission_deadline, admission_error, attach_ready_with_retry,
        claim_admission_with_retry, generation_admission_contract,
        render_stored_generation_response, replay_generation_result, reserve_with_retry,
        wait_for_generation_result,
    },
    resolve_request_model,
};

pub(super) async fn create_image(
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<DreaminaImageRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesWrite)?;
    let Json(request) = body.map_err(invalid_json)?;
    match request {
        DreaminaImageRequest::Legacy(request)
        | DreaminaImageRequest::Workflow(DreaminaImageWorkflowRequest::TextToImage(request)) => {
            create_image_with_auth(&state, auth, &headers, request_id.0, request).await
        }
        DreaminaImageRequest::Workflow(request) => {
            create_image_workflow(&state, auth, &headers, request_id.0, request).await
        }
    }
}

pub(super) async fn create_image_with_auth(
    state: &Arc<AppState>,
    auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    request: DreaminaImageGenerationRequest,
) -> Result<Response, ImageGatewayError> {
    create_image_with_optional_route(
        state,
        auth,
        headers,
        request_id,
        request,
        None,
        prefers_async(headers),
    )
    .await
}

pub(super) async fn create_image_with_resolved_auth(
    state: &Arc<AppState>,
    auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    request: DreaminaImageGenerationRequest,
    resolved: ResolvedModelRoute,
) -> Result<Response, ImageGatewayError> {
    create_image_with_optional_route(
        state,
        auth,
        headers,
        request_id,
        request,
        Some(resolved),
        false,
    )
    .await
}

async fn create_image_with_optional_route(
    state: &Arc<AppState>,
    mut auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    mut request: DreaminaImageGenerationRequest,
    resolved: Option<ResolvedModelRoute>,
    respond_async: bool,
) -> Result<Response, ImageGatewayError> {
    require_external_execution(state)?;
    let contract = generation_admission_contract(state.config.generation_admission_contract);
    let mut public_model_id = request
        .model_version
        .clone()
        .unwrap_or_else(|| "5.0".to_string());
    let mut execution_model_id = None;
    let resolved = match resolved {
        Some(resolved) => {
            validate_resolved_dreamina_route(
                &resolved,
                DREAMINA_IMAGES_API_PROFILE,
                "images.generations",
            )?;
            Some(resolved)
        }
        None => {
            resolve_request_model(
                state,
                &mut auth,
                image_provider_dreamina_cli::PROVIDER_ID,
                "images.generations",
                DREAMINA_IMAGES_API_PROFILE,
                request.model_version.as_deref(),
                "5.0",
            )
            .await?
        }
    };
    if let Some(resolved) = resolved {
        public_model_id = resolved.public_model_id;
        execution_model_id = Some(resolved.execution_model_id);
        request.model_version = Some(resolved.provider_model_id);
    }
    if contract == AdmissionContract::CustomerPricingV4 && execution_model_id.is_none() {
        return Err(ImageGatewayError::service_unavailable(
            "customer pricing requires an enabled model route",
        ));
    }
    let plan = DreaminaImageAdmissionPlan::new(request).map_err(dreamina_admission_error)?;
    let idempotency = idempotency_digest(
        headers,
        &auth.project_id,
        DREAMINA_IMAGES_API_PROFILE,
        GENERATION_OPERATION,
    )?;
    let job_model = execution_model_id
        .clone()
        .unwrap_or_else(|| plan.provider_model().to_string());
    let mut claim = plan.claim(
        Uuid::new_v4(),
        auth.tenant_id.clone(),
        auth.project_id.clone(),
        request_id.clone(),
        idempotency,
        admission_deadline(&state.config),
    );
    if contract == AdmissionContract::CustomerPricingV4 {
        claim.request_hash = customer_pricing_request_hash(
            &auth,
            DREAMINA_IMAGES_API_PROFILE,
            &public_model_id,
            &job_model,
            plan.request_identity_hash(),
        )?;
    }
    let service_tier_decision = crate::service_tiers::ServiceTierDecision::for_default_only_project(
        auth.project_service_tier,
    );
    let result = submit_image_generation_with_mode(
        state,
        &auth,
        request_id,
        claim,
        plan.provider_id(),
        &job_model,
        plan.output_count(),
        respond_async,
        |ticket, job_id, schedule_scope| {
            let mut attach = plan.attach(ticket, job_id, schedule_scope);
            attach.contract = contract;
            attach.customer_pricing = (contract == AdmissionContract::CustomerPricingV4).then_some(
                CustomerPricingIntent {
                    public_model_id,
                    provider_model_id: plan.provider_model_id().to_string(),
                    execution_model_id: job_model.clone(),
                    provider_command_hash: Some(plan.provider_command_hash().to_string()),
                    media_kind: "image".to_string(),
                    service_tier: service_tier_decision.effective.pricing_key().to_string(),
                    service_tier_decision,
                    execution_surface: "provider_cli".to_string(),
                    currency: "USD".to_string(),
                    pricing_dimensions: plan.pricing_dimensions().clone(),
                    processing_mode: crate::admission::PricingProcessingMode::Synchronous,
                },
            );
            attach
        },
    )
    .await?;
    match result {
        ImageSubmission::Completed(result) => render_stored_generation_response(result, &auth),
        ImageSubmission::Accepted(job_id) => accepted_image_response(state, &auth, job_id).await,
    }
}

pub(super) async fn create_video(
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<DreaminaVideoRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<DreaminaTaskCreated>, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::VideosWrite)?;
    let Json(request) = body.map_err(invalid_json)?;
    let result = match request {
        DreaminaVideoRequest::Legacy(request)
        | DreaminaVideoRequest::Workflow(DreaminaVideoWorkflowRequest::TextToVideo(request)) => {
            create_video_with_auth(&state, auth, &headers, request_id.0, request).await
        }
        DreaminaVideoRequest::Workflow(request) => {
            create_video_workflow(&state, auth, &headers, request_id.0, request).await
        }
    }?;
    Ok(Json(result))
}

pub(super) async fn create_video_with_auth(
    state: &Arc<AppState>,
    auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    request: DreaminaVideoGenerationRequest,
) -> Result<DreaminaTaskCreated, ImageGatewayError> {
    create_video_with_optional_route(state, auth, headers, request_id, request, None).await
}

pub(super) async fn create_video_with_resolved_auth(
    state: &Arc<AppState>,
    auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    request: DreaminaVideoGenerationRequest,
    resolved: ResolvedModelRoute,
) -> Result<DreaminaTaskCreated, ImageGatewayError> {
    create_video_with_optional_route(state, auth, headers, request_id, request, Some(resolved))
        .await
}

async fn create_video_with_optional_route(
    state: &Arc<AppState>,
    mut auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    mut request: DreaminaVideoGenerationRequest,
    resolved: Option<ResolvedModelRoute>,
) -> Result<DreaminaTaskCreated, ImageGatewayError> {
    require_external_execution(state)?;
    let contract = generation_admission_contract(state.config.generation_admission_contract);
    let mut public_model_id = request
        .model_version
        .clone()
        .unwrap_or_else(|| "seedance2.0fast".to_string());
    let mut execution_model_id = None;
    let resolved = match resolved {
        Some(resolved) => {
            validate_resolved_dreamina_route(
                &resolved,
                DREAMINA_VIDEOS_API_PROFILE,
                "videos.generations",
            )?;
            Some(resolved)
        }
        None => {
            resolve_request_model(
                state,
                &mut auth,
                image_provider_dreamina_cli::PROVIDER_ID,
                "videos.generations",
                DREAMINA_VIDEOS_API_PROFILE,
                request.model_version.as_deref(),
                "seedance2.0fast",
            )
            .await?
        }
    };
    if let Some(resolved) = resolved {
        public_model_id = resolved.public_model_id;
        execution_model_id = Some(resolved.execution_model_id);
        request.model_version = Some(resolved.provider_model_id);
    }
    if contract == AdmissionContract::CustomerPricingV4 && execution_model_id.is_none() {
        return Err(ImageGatewayError::service_unavailable(
            "customer pricing requires an enabled model route",
        ));
    }
    let plan = DreaminaVideoAdmissionPlan::new(request).map_err(dreamina_admission_error)?;
    let idempotency = idempotency_digest(
        headers,
        &auth.project_id,
        DREAMINA_VIDEOS_API_PROFILE,
        VIDEO_GENERATION_OPERATION,
    )?;
    let job_model = execution_model_id
        .clone()
        .unwrap_or_else(|| plan.provider_model().to_string());
    let mut claim = plan.claim(
        Uuid::new_v4(),
        auth.tenant_id.clone(),
        auth.project_id.clone(),
        request_id.clone(),
        idempotency,
        admission_deadline(&state.config),
    );
    if contract == AdmissionContract::CustomerPricingV4 {
        claim.request_hash = customer_pricing_request_hash(
            &auth,
            DREAMINA_VIDEOS_API_PROFILE,
            &public_model_id,
            &job_model,
            plan.provider_command_hash(),
        )?;
    }
    let service_tier_decision = crate::service_tiers::ServiceTierDecision::for_default_only_project(
        auth.project_service_tier,
    );
    let job_id = submit_video_generation(
        state,
        &auth,
        request_id,
        claim,
        plan.provider_id(),
        &job_model,
        plan.duration(),
        |ticket, job_id, schedule_scope| {
            let mut attach = plan.attach(ticket, job_id, schedule_scope);
            attach.contract = contract;
            attach.customer_pricing = (contract == AdmissionContract::CustomerPricingV4).then_some(
                CustomerPricingIntent {
                    public_model_id,
                    provider_model_id: plan.provider_model_id().to_string(),
                    execution_model_id: job_model.clone(),
                    provider_command_hash: Some(plan.provider_command_hash().to_string()),
                    media_kind: "video".to_string(),
                    service_tier: service_tier_decision.effective.pricing_key().to_string(),
                    service_tier_decision,
                    execution_surface: "provider_cli".to_string(),
                    currency: "USD".to_string(),
                    pricing_dimensions: plan.pricing_dimensions().clone(),
                    processing_mode: crate::admission::PricingProcessingMode::Synchronous,
                },
            );
            attach
        },
    )
    .await?;
    Ok(DreaminaTaskCreated {
        id: job_id.to_string(),
    })
}

async fn create_image_workflow(
    state: &Arc<AppState>,
    mut auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    mut request: DreaminaImageWorkflowRequest,
) -> Result<Response, ImageGatewayError> {
    let (model, default_model) = match &mut request {
        DreaminaImageWorkflowRequest::TextToImage(r) => (Some(&mut r.model_version), "5.0"),
        DreaminaImageWorkflowRequest::ImageToImage(r) => (Some(&mut r.model_version), "5.0"),
        DreaminaImageWorkflowRequest::ImageUpscale(_) => (None, "image_upscale"),
    };
    let resolved = resolve_workflow_route(
        state,
        &mut auth,
        DREAMINA_IMAGES_API_PROFILE,
        "images.generations",
        model,
        default_model,
    )
    .await?;
    let intent = DreaminaWorkflowIntent::image(request).map_err(dreamina_admission_error)?;
    let respond_async = prefers_async(headers);
    let job_id = submit_workflow_with_mode(
        state,
        &auth,
        headers,
        request_id,
        DREAMINA_IMAGES_API_PROFILE,
        resolved.public_model_id,
        resolved.execution_model_id,
        intent,
        respond_async,
    )
    .await?;
    if respond_async {
        return accepted_image_response(state, &auth, job_id).await;
    }
    let result = wait_for_generation_result(state, job_id).await?;
    render_stored_generation_response(result, &auth)
}

async fn create_video_workflow(
    state: &Arc<AppState>,
    mut auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    mut request: DreaminaVideoWorkflowRequest,
) -> Result<DreaminaTaskCreated, ImageGatewayError> {
    let (model, default_model) = match &mut request {
        DreaminaVideoWorkflowRequest::TextToVideo(r) => {
            (Some(&mut r.model_version), "seedance2.0fast")
        }
        DreaminaVideoWorkflowRequest::ImageToVideo(r) => {
            (Some(&mut r.model_version), "seedance2.0_vip")
        }
        DreaminaVideoWorkflowRequest::FramesToVideo(r) => {
            (Some(&mut r.model_version), "seedance2.0_vip")
        }
        DreaminaVideoWorkflowRequest::MultimodalToVideo(r) => {
            (Some(&mut r.model_version), "seedance2.0_vip")
        }
        DreaminaVideoWorkflowRequest::MultiframeToVideo(_) => (None, "multiframe2video"),
    };
    let resolved = resolve_workflow_route(
        state,
        &mut auth,
        DREAMINA_VIDEOS_API_PROFILE,
        "videos.generations",
        model,
        default_model,
    )
    .await?;
    let intent = DreaminaWorkflowIntent::video(request).map_err(dreamina_admission_error)?;
    let job_id = submit_workflow(
        state,
        &auth,
        headers,
        request_id,
        DREAMINA_VIDEOS_API_PROFILE,
        resolved.public_model_id,
        resolved.execution_model_id,
        intent,
    )
    .await?;
    Ok(DreaminaTaskCreated {
        id: job_id.to_string(),
    })
}

async fn resolve_workflow_route(
    state: &Arc<AppState>,
    auth: &mut AuthContext,
    profile: &str,
    operation: &str,
    model: Option<&mut Option<String>>,
    default_model: &str,
) -> Result<ResolvedModelRoute, ImageGatewayError> {
    require_external_execution(state)?;
    let requested = model
        .as_ref()
        .and_then(|m| m.as_deref())
        .unwrap_or(default_model);
    let resolved = resolve_request_model(
        state,
        auth,
        image_provider_dreamina_cli::PROVIDER_ID,
        operation,
        profile,
        Some(requested),
        default_model,
    )
    .await?
    .ok_or_else(|| {
        ImageGatewayError::service_unavailable("Dreamina workflow requires an enabled model route")
    })?;
    validate_resolved_dreamina_route(&resolved, profile, operation)?;
    if let Some(model) = model {
        *model = Some(resolved.provider_model_id.clone());
    } else if resolved.provider_model_id != default_model {
        return Err(ImageGatewayError::service_unavailable(
            "fixed Dreamina workflow model route mismatch",
        ));
    }
    Ok(resolved)
}

/// The source request owns idempotency; immutable bytes and measurements own execution and pricing.
#[allow(clippy::too_many_arguments)]
pub(super) async fn submit_workflow(
    state: &Arc<AppState>,
    auth: &AuthContext,
    headers: &HeaderMap,
    request_id: String,
    api_profile: &str,
    public_model_id: String,
    execution_model_id: String,
    intent: DreaminaWorkflowIntent,
) -> Result<Uuid, ImageGatewayError> {
    submit_workflow_with_mode(
        state,
        auth,
        headers,
        request_id,
        api_profile,
        public_model_id,
        execution_model_id,
        intent,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn submit_workflow_with_mode(
    state: &Arc<AppState>,
    auth: &AuthContext,
    headers: &HeaderMap,
    request_id: String,
    api_profile: &str,
    public_model_id: String,
    execution_model_id: String,
    intent: DreaminaWorkflowIntent,
    return_existing: bool,
) -> Result<Uuid, ImageGatewayError> {
    require_external_execution(state)?;
    if generation_admission_contract(state.config.generation_admission_contract)
        != AdmissionContract::CustomerPricingV4
    {
        return Err(admission_error(
            crate::admission::AdmissionError::PricingUnavailable,
        ));
    }
    let is_video = intent.is_video();
    let operation = if is_video {
        VIDEO_GENERATION_OPERATION
    } else {
        GENERATION_OPERATION
    };
    let claim = ClaimAdmission {
        owner_token: Uuid::new_v4(),
        tenant_id: auth.tenant_id.clone(),
        project_id: auth.project_id.clone(),
        api_profile: api_profile.to_owned(),
        operation: operation.to_owned(),
        request_id: request_id.clone(),
        idempotency_key_digest: idempotency_digest(
            headers,
            &auth.project_id,
            api_profile,
            operation,
        )?,
        request_hash: customer_pricing_request_hash(
            auth,
            api_profile,
            &public_model_id,
            &execution_model_id,
            intent.request_hash(),
        )?,
        deadline_at_ms: admission_deadline(&state.config),
    };
    let ticket = match claim_admission_with_retry(state, claim)
        .await
        .map_err(admission_error)?
    {
        AdmissionClaim::Owner(ticket) => ticket,
        AdmissionClaim::Existing {
            job_id,
            state: status,
            ..
        } if return_existing || is_video || status == "succeeded" => return Ok(job_id),
        AdmissionClaim::Existing { .. } | AdmissionClaim::InProgress { .. } => {
            return Err(ImageGatewayError::idempotency_in_progress());
        }
        AdmissionClaim::Conflict { .. } => return Err(ImageGatewayError::idempotency_conflict()),
    };
    let plan = match super::dreamina_inputs::stage_workflow_inputs(
        state.input_blob_store.as_ref(),
        &ticket,
        intent,
        state.config.max_upload_bytes,
    )
    .await
    {
        Ok(plan) => plan,
        Err(error) => {
            abort_before_attach(state, &ticket).await?;
            return Err(error);
        }
    };
    let facts = match crate::pricing::admission::dreamina_pricing_facts(
        plan.typed_request(),
        api_profile,
    ) {
        Ok(facts) => facts,
        Err(error) => {
            cleanup_workflow(state, &ticket).await?;
            return Err(admission_error(error));
        }
    };
    let Some(billable_units) = facts.output_count.checked_mul(facts.output_billable_units) else {
        cleanup_workflow(state, &ticket).await?;
        return Err(ImageGatewayError::internal(
            "Dreamina billable unit count overflow",
        ));
    };
    let reservation = match reserve_with_retry(
        state,
        UsageCharge {
            tenant_id: auth.tenant_id.clone(),
            attribution: Some(auth.attribution()),
            request_id,
            admission_session_id: Some(ticket.session_id),
            operation,
            provider_id: plan.provider_id().to_owned(),
            model: execution_model_id.clone(),
            output_count: facts.output_count,
            billable_units,
            billing_metric: if is_video {
                BillingMetric::VideoSecond
            } else {
                BillingMetric::Output
            },
            limits: if is_video {
                UsageLimits {
                    five_hour_image_limit: state.config.five_hour_video_second_limit,
                    seven_day_image_limit: state.config.seven_day_video_second_limit,
                }
            } else {
                UsageLimits {
                    five_hour_image_limit: state.config.five_hour_image_limit,
                    seven_day_image_limit: state.config.seven_day_image_limit,
                }
            },
        },
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            cleanup_workflow(state, &ticket).await?;
            return Err(error);
        }
    };
    let decision = crate::service_tiers::ServiceTierDecision::for_default_only_project(
        auth.project_service_tier,
    );
    let mut attach = plan.attach(
        ticket.clone(),
        reservation.job_id,
        format!("tenant:{}", auth.tenant_id),
    );
    attach.customer_pricing = Some(CustomerPricingIntent {
        public_model_id,
        provider_model_id: facts.provider_model_id,
        execution_model_id,
        provider_command_hash: Some(plan.provider_command_hash().to_owned()),
        media_kind: facts.media_kind.to_owned(),
        service_tier: decision.effective.pricing_key().to_owned(),
        service_tier_decision: decision,
        execution_surface: "provider_cli".to_owned(),
        currency: "USD".to_owned(),
        pricing_dimensions: facts.dimensions,
        processing_mode: crate::admission::PricingProcessingMode::Synchronous,
    });
    if let Err(error) = attach_ready_with_retry(state, attach).await {
        // An unavailable store may have committed; retain its frozen inputs for reconciliation.
        if !matches!(error, crate::admission::AdmissionError::Unavailable) {
            state
                .usage_store
                .release(&reservation, "admission_attach_failed")
                .await?;
            cleanup_workflow(state, &ticket).await?;
        }
        return Err(admission_error(error));
    }
    Ok(reservation.job_id)
}

async fn cleanup_workflow(
    state: &Arc<AppState>,
    ticket: &AdmissionTicket,
) -> Result<(), ImageGatewayError> {
    abort_before_attach(state, ticket).await?;
    state
        .input_blob_store
        .delete_session(ticket.session_id)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("Dreamina input cleanup unavailable"))
}

fn validate_resolved_dreamina_route(
    resolved: &ResolvedModelRoute,
    api_profile: &str,
    operation_id: &str,
) -> Result<(), ImageGatewayError> {
    if resolved.provider_id != image_provider_dreamina_cli::PROVIDER_ID
        || resolved.api_profile != api_profile
        || resolved.operation_id != operation_id
        || resolved.command_schema != image_provider_dreamina_cli::DREAMINA_SUBMIT_COMMAND_SCHEMA
    {
        return Err(ImageGatewayError::service_unavailable(
            "resolved model route does not match the Dreamina execution surface",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn submit_image_generation<F>(
    state: &Arc<AppState>,
    auth: &AuthContext,
    request_id: String,
    claim: ClaimAdmission,
    provider_id: &str,
    provider_model: &str,
    output_count: u32,
    attach: F,
) -> Result<StoredGenerationResult, ImageGatewayError>
where
    F: FnOnce(AdmissionTicket, Uuid, String) -> AttachJob,
{
    match submit_image_generation_with_mode(
        state,
        auth,
        request_id,
        claim,
        provider_id,
        provider_model,
        output_count,
        false,
        attach,
    )
    .await?
    {
        ImageSubmission::Completed(result) => Ok(result),
        ImageSubmission::Accepted(_) => unreachable!("synchronous submission waits for its result"),
    }
}

enum ImageSubmission {
    Accepted(Uuid),
    Completed(StoredGenerationResult),
}

#[allow(clippy::too_many_arguments)]
async fn submit_image_generation_with_mode<F>(
    state: &Arc<AppState>,
    auth: &AuthContext,
    request_id: String,
    claim: ClaimAdmission,
    provider_id: &str,
    provider_model: &str,
    output_count: u32,
    respond_async: bool,
    attach: F,
) -> Result<ImageSubmission, ImageGatewayError>
where
    F: FnOnce(AdmissionTicket, Uuid, String) -> AttachJob,
{
    let ticket = match claim_admission_with_retry(state, claim)
        .await
        .map_err(admission_error)?
    {
        AdmissionClaim::Owner(ticket) => ticket,
        AdmissionClaim::Existing { job_id, .. } if respond_async => {
            return Ok(ImageSubmission::Accepted(job_id));
        }
        AdmissionClaim::Existing {
            job_id,
            state: admission_state,
            ..
        } if admission_state == "succeeded" => {
            return replay_generation_result(state, job_id)
                .await
                .map(ImageSubmission::Completed);
        }
        AdmissionClaim::InProgress { .. } | AdmissionClaim::Existing { .. } => {
            return Err(ImageGatewayError::idempotency_in_progress());
        }
        AdmissionClaim::Conflict { .. } => {
            return Err(ImageGatewayError::idempotency_conflict());
        }
    };
    let reservation = match reserve_with_retry(
        state,
        UsageCharge {
            tenant_id: auth.tenant_id.clone(),
            attribution: Some(auth.attribution()),
            request_id,
            admission_session_id: Some(ticket.session_id),
            operation: GENERATION_OPERATION,
            provider_id: provider_id.to_owned(),
            model: provider_model.to_owned(),
            output_count,
            billable_units: output_count,
            billing_metric: BillingMetric::Output,
            limits: UsageLimits {
                five_hour_image_limit: state.config.five_hour_image_limit,
                seven_day_image_limit: state.config.seven_day_image_limit,
            },
        },
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            abort_before_attach(state, &ticket).await?;
            return Err(error);
        }
    };
    if let Err(error) = attach_ready_with_retry(
        state,
        attach(
            ticket.clone(),
            reservation.job_id,
            format!("tenant:{}", auth.tenant_id),
        ),
    )
    .await
    {
        if !matches!(error, crate::admission::AdmissionError::Unavailable) {
            state
                .usage_store
                .release(&reservation, "admission_attach_failed")
                .await?;
        }
        return Err(admission_error(error));
    }
    if respond_async {
        Ok(ImageSubmission::Accepted(reservation.job_id))
    } else {
        wait_for_generation_result(state, reservation.job_id)
            .await
            .map(ImageSubmission::Completed)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn submit_video_generation<F>(
    state: &Arc<AppState>,
    auth: &AuthContext,
    request_id: String,
    claim: ClaimAdmission,
    provider_id: &str,
    provider_model: &str,
    duration: u8,
    attach: F,
) -> Result<Uuid, ImageGatewayError>
where
    F: FnOnce(AdmissionTicket, Uuid, String) -> AttachJob,
{
    let ticket = match claim_admission_with_retry(state, claim)
        .await
        .map_err(admission_error)?
    {
        AdmissionClaim::Owner(ticket) => ticket,
        AdmissionClaim::Existing { job_id, .. } => return Ok(job_id),
        AdmissionClaim::InProgress { .. } => {
            return Err(ImageGatewayError::idempotency_in_progress());
        }
        AdmissionClaim::Conflict { .. } => {
            return Err(ImageGatewayError::idempotency_conflict());
        }
    };
    let reservation = match reserve_with_retry(
        state,
        UsageCharge {
            tenant_id: auth.tenant_id.clone(),
            attribution: Some(auth.attribution()),
            request_id,
            admission_session_id: Some(ticket.session_id),
            operation: VIDEO_GENERATION_OPERATION,
            provider_id: provider_id.to_owned(),
            model: provider_model.to_owned(),
            output_count: 1,
            billable_units: u32::from(duration),
            billing_metric: BillingMetric::VideoSecond,
            limits: UsageLimits {
                five_hour_image_limit: state.config.five_hour_video_second_limit,
                seven_day_image_limit: state.config.seven_day_video_second_limit,
            },
        },
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            abort_before_attach(state, &ticket).await?;
            return Err(error);
        }
    };
    if let Err(error) = attach_ready_with_retry(
        state,
        attach(
            ticket.clone(),
            reservation.job_id,
            format!("tenant:{}", auth.tenant_id),
        ),
    )
    .await
    {
        if !matches!(error, crate::admission::AdmissionError::Unavailable) {
            state
                .usage_store
                .release(&reservation, "admission_attach_failed")
                .await?;
        }
        return Err(admission_error(error));
    }
    Ok(reservation.job_id)
}

fn prefers_async(headers: &HeaderMap) -> bool {
    headers
        .get_all("prefer")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(|value| {
            value.split(',').any(|preference| {
                preference
                    .split(';')
                    .next()
                    .is_some_and(|token| token.trim().eq_ignore_ascii_case("respond-async"))
            })
        })
}

async fn accepted_image_response(
    state: &Arc<AppState>,
    auth: &AuthContext,
    job_id: Uuid,
) -> Result<Response, ImageGatewayError> {
    let mut response =
        project_image_response(state.settlement_store.as_ref(), auth, job_id).await?;
    *response.status_mut() = StatusCode::ACCEPTED;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!("/v1/dreamina/images/{job_id}"))
            .map_err(|_| ImageGatewayError::internal("invalid task location"))?,
    );
    response.headers_mut().insert(
        "preference-applied",
        HeaderValue::from_static("respond-async"),
    );
    Ok(response)
}

pub(super) async fn get_image(
    Path(task_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesRead)?;
    project_image_response(
        state.settlement_store.as_ref(),
        &auth,
        parse_uuid(&task_id)?,
    )
    .await
}

#[derive(Serialize)]
struct DreaminaCompleteImageTask {
    #[serde(flatten)]
    task: DreaminaImageTask,
    additional_images: Vec<DreaminaAdditionalImage>,
    returned_count: usize,
    billable_units: usize,
}

#[derive(Serialize)]
struct DreaminaAdditionalImage {
    source_index: usize,
    b64_json: String,
    sha256: String,
    byte_size: u64,
    mime_type: String,
}

pub(super) async fn get_image_results(
    Path(task_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesRead)?;
    let job_id = parse_uuid(&task_id)?;
    state
        .settlement_store
        .validate_complete_image_result_budget(
            &auth.tenant_id,
            &auth.project_id,
            auth.actor_user_id,
            job_id,
        )
        .await?;
    let projection = state
        .settlement_store
        .project_image_status(
            &auth.tenant_id,
            &auth.project_id,
            auth.actor_user_id,
            job_id,
        )
        .await?
        .ok_or_else(image_task_not_found)?;
    let (task, usage) = image_task(job_id, projection)?;
    let additional = if task.status == "succeeded" {
        state
            .settlement_store
            .project_additional_image_results(
                &auth.tenant_id,
                &auth.project_id,
                auth.actor_user_id,
                job_id,
            )
            .await?
    } else {
        Vec::new()
    };
    let complete = complete_image_task(task, additional)?;
    let mut response = Json(complete).into_response();
    if let Some(usage) = usage {
        super::responses::add_usage_headers(response.headers_mut(), &usage, &auth);
    }
    Ok(response)
}

fn complete_image_byte_total(total: u64, next: u64) -> Result<u64, ImageGatewayError> {
    total
        .checked_add(next)
        .filter(|value| *value <= crate::artifacts::MAX_ARTIFACT_BYTES)
        .ok_or_else(ImageGatewayError::artifact_integrity)
}

fn complete_image_decoded_size(encoded: &str) -> Result<u64, ImageGatewayError> {
    // These primary payloads were just encoded from integrity-checked server blobs.
    // Count canonical base64 padding without decoding or copying the image again.
    if encoded.is_empty() || !encoded.len().is_multiple_of(4) {
        return Err(ImageGatewayError::artifact_integrity());
    }
    let padding = if encoded.ends_with("==") {
        2
    } else {
        usize::from(encoded.ends_with('='))
    };
    let size = (encoded.len() / 4)
        .checked_mul(3)
        .and_then(|value| value.checked_sub(padding))
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(ImageGatewayError::artifact_integrity)?;
    Ok(size)
}

fn complete_image_task(
    task: DreaminaImageTask,
    additional: Vec<crate::settlement::AdditionalImageResult>,
) -> Result<DreaminaCompleteImageTask, ImageGatewayError> {
    use base64::Engine as _;
    let billable_units = task.data.as_ref().map_or(0, Vec::len);
    if !additional.is_empty() && (task.status != "succeeded" || billable_units != 1)
        || additional.len() > 9
    {
        return Err(ImageGatewayError::artifact_integrity());
    }
    let mut total_bytes = 0_u64;
    for image in task.data.as_deref().unwrap_or_default() {
        total_bytes =
            complete_image_byte_total(total_bytes, complete_image_decoded_size(&image.b64_json)?)?;
    }
    let mut additional_images = Vec::with_capacity(additional.len());
    for (offset, image) in additional.into_iter().enumerate() {
        if image.source_index != offset + 1
            || image.bytes.len() as u64 != image.byte_size
            || hex::encode(Sha256::digest(&image.bytes)) != image.sha256
            || !matches!(
                image.mime_type.as_str(),
                "image/png" | "image/jpeg" | "image/webp"
            )
        {
            return Err(ImageGatewayError::artifact_integrity());
        }
        total_bytes = complete_image_byte_total(total_bytes, image.byte_size)?;
        additional_images.push(DreaminaAdditionalImage {
            source_index: image.source_index,
            b64_json: base64::engine::general_purpose::STANDARD.encode(image.bytes),
            sha256: image.sha256,
            byte_size: image.byte_size,
            mime_type: image.mime_type,
        });
    }
    Ok(DreaminaCompleteImageTask {
        returned_count: billable_units + additional_images.len(),
        billable_units,
        task,
        additional_images,
    })
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ImageLookupOperation {
    #[default]
    Generation,
    Edit,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ImageLookupQuery {
    #[serde(default)]
    operation: ImageLookupOperation,
}

pub(super) async fn get_image_by_idempotency_key(
    query: Result<Query<ImageLookupQuery>, axum::extract::rejection::QueryRejection>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesRead)?;
    let Query(query) = query.map_err(|_| {
        ImageGatewayError::invalid_request(
            "operation must be generation or edit",
            Some("operation".to_owned()),
            "invalid_value",
        )
    })?;
    let operation = match query.operation {
        ImageLookupOperation::Generation => GENERATION_OPERATION,
        ImageLookupOperation::Edit => crate::admission::EDIT_OPERATION,
    };
    let digest = idempotency_digest(
        &headers,
        &auth.project_id,
        DREAMINA_IMAGES_API_PROFILE,
        operation,
    )?
    .ok_or_else(ImageGatewayError::invalid_idempotency_key)?;
    let job_id = state
        .settlement_store
        .project_image_job_by_idempotency_key(
            &auth.tenant_id,
            &auth.project_id,
            auth.actor_user_id,
            operation,
            &digest,
        )
        .await?
        .ok_or_else(image_task_not_found)?;
    project_image_response(state.settlement_store.as_ref(), &auth, job_id).await
}

async fn project_image_response(
    settlement_store: &dyn ExecutionSettlementStore,
    auth: &AuthContext,
    job_id: Uuid,
) -> Result<Response, ImageGatewayError> {
    let projection = settlement_store
        .project_image_status(
            &auth.tenant_id,
            &auth.project_id,
            auth.actor_user_id,
            job_id,
        )
        .await?
        .ok_or_else(image_task_not_found)?;
    let (task, usage) = image_task(job_id, projection)?;
    let mut response = Json(task).into_response();
    if let Some(usage) = usage {
        super::responses::add_usage_headers(response.headers_mut(), &usage, auth);
    }
    Ok(response)
}

fn image_task(
    id: Uuid,
    projection: ImageTaskProjection,
) -> Result<(DreaminaImageTask, Option<crate::usage::UsageSnapshot>), ImageGatewayError> {
    let mut task = DreaminaImageTask {
        id: id.to_string(),
        status: "running".to_owned(),
        stage: None,
        provider_progress: None,
        cancellation: DreaminaCancellation {
            supported: false,
            reason: "provider_cancellation_unsupported".to_owned(),
        },
        model: Some(projection.model),
        request_id: Some(projection.request_id),
        request: Some(projection.request),
        error: None,
        created: None,
        data: None,
        output_counts: None,
        output_format: None,
        quality: None,
        size: None,
        background: None,
    };
    let mut usage = None;
    match projection.status {
        ImageResultStatus::Pending {
            stage,
            provider_progress,
        } => {
            task.stage = Some(
                match (&provider_progress, stage) {
                    (_, VideoPendingStage::Queued) => "queued",
                    (_, VideoPendingStage::Dispatching) => "dispatching",
                    (Some(progress), VideoPendingStage::Processing)
                        if progress.progress.provider_status == "Queueing" =>
                    {
                        "provider_queued"
                    }
                    (_, VideoPendingStage::Processing) => "processing",
                }
                .to_owned(),
            );
            task.provider_progress = provider_progress.map(public_provider_progress);
        }
        ImageResultStatus::Succeeded {
            result,
            output_counts,
        } => {
            if !(1..=10).contains(&output_counts.requested)
                || output_counts.delivered == 0
                || output_counts.delivered.checked_add(output_counts.failed)
                    != Some(output_counts.requested)
                || result.images.len() != output_counts.delivered as usize
                || task
                    .request
                    .as_ref()
                    .and_then(|request| request.generate_num)
                    .is_some_and(|count| output_counts.requested != count)
            {
                return Err(ImageGatewayError::artifact_integrity());
            }
            task.status = "succeeded".to_owned();
            task.output_counts = Some(output_counts);
            let mut projection = result.projection;
            if projection.output_format == "auto" {
                projection.output_format = super::images::inferred_output_format(&result.images)?;
            }
            usage = Some(projection.usage);
            let response = crate::models::images_response_at(
                projection.created_at_seconds,
                result.images,
                projection.output_format,
                projection.quality,
                projection.size,
                projection.background,
            );
            task.created = Some(response.created);
            task.data = Some(
                response
                    .data
                    .into_iter()
                    .map(|image| DreaminaImageData {
                        b64_json: image.b64_json,
                    })
                    .collect(),
            );
            task.output_format = Some(response.output_format);
            task.quality = Some(response.quality);
            task.size = Some(response.size);
            task.background = Some(response.background);
        }
        ImageResultStatus::Failed { error_code } => {
            task.status = "failed".to_owned();
            task.error = Some(DreaminaTaskError {
                reason_code: None,
                message: known_terminal_failure_message(error_code.as_deref())
                    .unwrap_or("Dreamina image generation failed")
                    .to_owned(),
                code: error_code.unwrap_or_else(|| "generation_failed".to_owned()),
            });
        }
        ImageResultStatus::Uncertain => {
            task.status = "uncertain".to_owned();
            task.error = Some(DreaminaTaskError {
                reason_code: None,
                code: "generation_uncertain".to_owned(),
                message: VIDEO_UNCERTAIN_MESSAGE.to_owned(),
            });
        }
        ImageResultStatus::Expired => {
            task.status = "expired".to_owned();
            task.error = Some(DreaminaTaskError {
                reason_code: None,
                code: "artifact_expired".to_owned(),
                message: "The stored image result has expired".to_owned(),
            });
        }
    }
    Ok((task, usage))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct VideoLookupQuery {}

#[derive(Serialize)]
pub(super) struct VideoLookupResponse {
    #[serde(flatten)]
    task: DreaminaVideoTask,
    request_id: String,
    request: image_api_contracts::dreamina::DreaminaVideoRequestMetadata,
}

pub(super) async fn get_video_by_idempotency_key(
    query: Result<Query<VideoLookupQuery>, axum::extract::rejection::QueryRejection>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<VideoLookupResponse>, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::VideosRead)?;
    query.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Video identity lookup does not accept query parameters",
            Some("operation".to_owned()),
            "invalid_value",
        )
    })?;
    let digest = idempotency_digest(
        &headers,
        &auth.project_id,
        DREAMINA_VIDEOS_API_PROFILE,
        VIDEO_GENERATION_OPERATION,
    )?
    .ok_or_else(ImageGatewayError::invalid_idempotency_key)?;
    let projection = state
        .settlement_store
        .project_video_job_by_idempotency_key(
            &auth.tenant_id,
            &auth.project_id,
            auth.actor_user_id,
            &digest,
        )
        .await?
        .ok_or_else(video_lookup_not_found)?;
    let task =
        project_video_task(state.settlement_store.as_ref(), &auth, projection.job_id).await?;
    Ok(Json(VideoLookupResponse {
        task,
        request_id: projection.request_id,
        request: projection.request,
    }))
}

fn video_lookup_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Dreamina task was not found",
        Some("Idempotency-Key".to_owned()),
        "not_found",
    )
}

pub(super) async fn get_video(
    Path(task_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<DreaminaVideoTask>, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::VideosRead)?;
    let job_id = parse_uuid(&task_id)?;
    Ok(Json(
        project_video_task(state.settlement_store.as_ref(), &auth, job_id).await?,
    ))
}

async fn project_video_task(
    settlement_store: &dyn ExecutionSettlementStore,
    auth: &AuthContext,
    job_id: Uuid,
) -> Result<DreaminaVideoTask, ImageGatewayError> {
    let status = settlement_store
        .project_video_status(
            &auth.tenant_id,
            &auth.project_id,
            auth.actor_user_id,
            job_id,
        )
        .await?
        .ok_or_else(task_not_found)?;
    let audited_release_possible = matches!(
        &status,
        VideoResultStatus::Failed { error_code: Some(code), .. }
            if code == "dreamina_compliance_confirmation_required"
    );
    let manual_cancellation = if matches!(&status, VideoResultStatus::Failed { .. }) {
        settlement_store
            .project_dreamina_manual_video_cancellation(
                &auth.tenant_id,
                &auth.project_id,
                auth.actor_user_id,
                job_id,
            )
            .await?
            .map(|proof| public_manual_cancellation(job_id, proof))
            .transpose()?
    } else {
        None
    };
    let mut task = video_task(job_id, status);
    task.manual_cancellation = manual_cancellation;
    if audited_release_possible {
        task.settlement = settlement_store
            .project_video_released_settlement(
                &auth.tenant_id,
                &auth.project_id,
                auth.actor_user_id,
                job_id,
            )
            .await?;
    }
    Ok(task)
}

fn video_task(id: Uuid, status: VideoResultStatus) -> DreaminaVideoTask {
    let mut task = match status {
        VideoResultStatus::Pending {
            model,
            stage,
            provider_progress,
            ..
        } => {
            let stage = public_video_pending_stage(stage, provider_progress.as_ref());
            DreaminaVideoTask {
                id: id.to_string(),
                status: "running".to_owned(),
                stage: Some(stage.to_owned()),
                provider_progress: provider_progress.map(public_provider_progress),
                cancellation: None,
                model: Some(model),
                error: None,
                content: None,
                settlement: None,
                manual_cancellation: None,
            }
        }
        VideoResultStatus::Uncertain {
            model,
            provider_progress,
            submission_error_code,
            ..
        } => {
            let diagnostic = submission_uncertainty_diagnostic(submission_error_code.as_deref());
            DreaminaVideoTask {
                id: id.to_string(),
                status: "uncertain".to_owned(),
                stage: diagnostic.map(|_| "submission_uncertain".to_owned()),
                provider_progress: provider_progress.map(public_provider_progress),
                cancellation: None,
                model: Some(model),
                error: Some(DreaminaTaskError {
                    reason_code: diagnostic.map(|(code, _)| code.to_owned()),
                    code: "generation_uncertain".to_owned(),
                    message: diagnostic
                        .map(|(_, message)| message)
                        .unwrap_or(VIDEO_UNCERTAIN_MESSAGE)
                        .to_owned(),
                }),
                content: None,
                settlement: None,
                manual_cancellation: None,
            }
        }
        VideoResultStatus::Succeeded {
            model,
            duration_ms,
            artifact_id,
        } => DreaminaVideoTask {
            id: id.to_string(),
            status: "succeeded".to_owned(),
            stage: None,
            provider_progress: None,
            cancellation: None,
            model: Some(model),
            error: None,
            content: Some(DreaminaVideoContent {
                video_url: format!("/v1/dreamina/files/{artifact_id}/content"),
                duration: duration_seconds(duration_ms),
            }),
            settlement: None,
            manual_cancellation: None,
        },
        VideoResultStatus::SucceededRemote {
            model,
            duration_ms,
            result,
        } => DreaminaVideoTask {
            id: id.to_string(),
            status: "succeeded".to_owned(),
            stage: None,
            provider_progress: None,
            cancellation: None,
            model: Some(model),
            error: None,
            content: Some(DreaminaVideoContent {
                video_url: result.video_url().to_owned(),
                duration: duration_seconds(duration_ms),
            }),
            settlement: None,
            manual_cancellation: None,
        },
        VideoResultStatus::Failed {
            model, error_code, ..
        } => {
            let message = known_terminal_failure_message(error_code.as_deref())
                .unwrap_or("Dreamina video generation failed");
            DreaminaVideoTask {
                id: id.to_string(),
                status: "failed".to_owned(),
                stage: None,
                provider_progress: None,
                cancellation: None,
                model: Some(model),
                error: Some(DreaminaTaskError {
                    reason_code: None,
                    code: error_code.unwrap_or_else(|| "generation_failed".to_owned()),
                    message: message.to_owned(),
                }),
                content: None,
                settlement: None,
                manual_cancellation: None,
            }
        }
    };
    task.cancellation = Some(DreaminaCancellation {
        supported: false,
        reason: "provider_cancellation_unsupported".to_owned(),
    });
    task
}

fn known_terminal_failure_message(code: Option<&str>) -> Option<&'static str> {
    match code? {
        "dreamina_compliance_confirmation_required" => {
            Some(DREAMINA_COMPLIANCE_CONFIRMATION_MESSAGE)
        }
        "dreamina_copyright_rejected" => Some("生成的视频里包含受版权保护元素"),
        _ => None,
    }
}

fn submission_uncertainty_diagnostic(code: Option<&str>) -> Option<(&'static str, &'static str)> {
    match code? {
        "dreamina_cli_permission_denied" => Some((
            "dreamina_cli_permission_denied",
            "Dreamina CLI access was denied for this account. Upstream acceptance is unconfirmed; keep the original task for review and do not resubmit.",
        )),
        "provider_submit_timeout" => Some((
            "provider_submit_timeout",
            "Dreamina CLI submission timed out before an acceptance receipt was recorded. Upstream acceptance is unconfirmed; do not resubmit.",
        )),
        "provider_submit_cli_failed" => Some((
            "provider_submit_cli_failed",
            "Dreamina CLI exited without a valid acceptance receipt. Upstream acceptance is unconfirmed; do not resubmit.",
        )),
        "provider_submit_deadline" => Some((
            "provider_submit_deadline",
            "The Dreamina submission deadline elapsed without a recorded upstream task. Upstream acceptance is unconfirmed; do not resubmit.",
        )),
        _ => None,
    }
}

pub(super) fn public_provider_progress(value: VideoProviderProgress) -> DreaminaProviderProgress {
    use image_api_contracts::dreamina::{ProviderProgressSource, ProviderProgressStage};
    let stage = match value.progress.provider_status.as_str() {
        "Queueing" => ProviderProgressStage::ProviderQueued,
        "Generating" => ProviderProgressStage::Generating,
        _ => ProviderProgressStage::Processing,
    };
    DreaminaProviderProgress {
        progress_percent: value.estimated_percent,
        progress_source: value
            .estimated_percent
            .map(|_| ProviderProgressSource::Estimated),
        stage: Some(stage),
        provider_status: value.progress.provider_status,
        queue_position: value.progress.queue_position,
        queue_total: value.progress.queue_total,
        estimated_remaining_seconds: value.progress.estimated_remaining_seconds,
        observed_at_ms: value.observed_at_ms,
    }
}

pub(super) fn public_video_pending_stage(
    stage: VideoPendingStage,
    progress: Option<&VideoProviderProgress>,
) -> &'static str {
    match stage {
        VideoPendingStage::Queued => "queued",
        VideoPendingStage::Dispatching => "dispatching",
        VideoPendingStage::Processing
            if progress.is_some_and(|value| value.progress.provider_status == "Queueing") =>
        {
            "provider_queued"
        }
        VideoPendingStage::Processing => "processing",
    }
}

pub(super) fn public_manual_cancellation(
    job_id: Uuid,
    proof: DreaminaManualVideoCancellationProof,
) -> Result<DreaminaManualCancellation, ImageGatewayError> {
    if proof.schema != "factory.manual_cancellation.v1"
        || proof.job_id != job_id
        || proof.authority_id.is_nil()
        || proof.submission_id.is_nil()
        || proof.hold_id.is_nil()
        || proof.release_event_id.is_nil()
        || proof.request_id.is_empty()
        || proof.upstream_task_id.is_empty()
        || proof.confirmed_at_ms <= 0
        || proof.currency != "USD"
        || proof.held_micros <= 0
        || proof.captured_micros != 0
        || proof.released_micros != proof.held_micros
    {
        return Err(ImageGatewayError::service_unavailable(
            "Manual cancellation proof is inconsistent",
        ));
    }
    Ok(DreaminaManualCancellation {
        schema: proof.schema,
        status: "confirmed".to_owned(),
        authority_id: proof.authority_id.to_string(),
        job_id: proof.job_id.to_string(),
        request_id: proof.request_id,
        upstream_task_id: proof.upstream_task_id,
        submission_id: proof.submission_id.to_string(),
        hold_id: proof.hold_id.to_string(),
        confirmed_at_ms: proof.confirmed_at_ms,
        currency: proof.currency,
        held_micros: proof.held_micros.to_string(),
        captured_micros: proof.captured_micros.to_string(),
        released_micros: proof.released_micros.to_string(),
        release_event_id: proof.release_event_id.to_string(),
    })
}

pub(super) const VIDEO_UNCERTAIN_MESSAGE: &str = "The generation outcome is unknown. Do not retry or create a replacement task; contact support to reconcile this task.";
pub(super) const DREAMINA_COMPLIANCE_CONFIRMATION_MESSAGE: &str = "Dreamina requires a one-time safety confirmation on its website. Complete it before any future request. This task will not be retried.";

/// Keep whole-second responses byte-compatible and fractional seconds exact.
pub(super) fn duration_seconds(duration_ms: u32) -> serde_json::Number {
    if duration_ms % 1000 == 0 {
        return (duration_ms / 1000).into();
    }
    format!("{}.{:03}", duration_ms / 1000, duration_ms % 1000)
        .trim_end_matches('0')
        .parse()
        .expect("integer milliseconds always encode a finite JSON number")
}

pub(super) fn require_external_execution(state: &AppState) -> Result<(), ImageGatewayError> {
    if state.generation_execution_mode == GenerationExecutionMode::External {
        Ok(())
    } else {
        Err(ImageGatewayError::service_unavailable(
            "CLI generation requires external execution",
        ))
    }
}

pub(super) fn invalid_json(error: axum::extract::rejection::JsonRejection) -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        format!("Invalid JSON request: {error}"),
        None,
        "invalid_json",
    )
}

pub(super) fn dreamina_admission_error(error: DreaminaAdmissionError) -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        error.to_string(),
        Some(error.parameter().to_owned()),
        "invalid_value",
    )
}

pub(super) fn idempotency_digest(
    headers: &HeaderMap,
    project_id: &str,
    api_profile: &str,
    operation: &str,
) -> Result<Option<String>, ImageGatewayError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = value
        .to_str()
        .map_err(|_| ImageGatewayError::invalid_idempotency_key())?;
    idempotency_key_digest(project_id, api_profile, operation, key)
        .map(Some)
        .map_err(|_| ImageGatewayError::invalid_idempotency_key())
}

#[derive(Serialize)]
struct CustomerPricingRequestIdentity<'a> {
    schema: &'static str,
    api_profile: &'a str,
    public_model_id: &'a str,
    execution_model_id: &'a str,
    provider_command_hash: &'a str,
    route_provider_id: &'a str,
    route_operation_id: &'a str,
    route_command_schema: &'a str,
    route_id: Uuid,
    route_revision: i64,
    project_service_tier: &'a str,
}

pub(super) fn customer_pricing_request_hash(
    auth: &AuthContext,
    api_profile: &str,
    public_model_id: &str,
    execution_model_id: &str,
    provider_command_hash: &str,
) -> Result<String, ImageGatewayError> {
    let route = auth.route.as_ref().ok_or_else(|| {
        ImageGatewayError::service_unavailable("customer pricing requires an enabled model route")
    })?;
    let identity = CustomerPricingRequestIdentity {
        schema: "customer-pricing-request-v1",
        api_profile,
        public_model_id,
        execution_model_id,
        provider_command_hash,
        route_provider_id: &route.provider_id,
        route_operation_id: &route.operation_id,
        route_command_schema: &route.command_schema,
        route_id: route.route_id,
        route_revision: route.route_revision,
        project_service_tier: auth.project_service_tier.as_str(),
    };
    let bytes = serde_json::to_vec(&identity).map_err(|_| {
        ImageGatewayError::internal("customer pricing identity serialization failed")
    })?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn parse_uuid(value: &str) -> Result<Uuid, ImageGatewayError> {
    value.parse().map_err(|_| {
        ImageGatewayError::invalid_request(
            "Dreamina task id is invalid",
            Some("task_id".to_owned()),
            "invalid_value",
        )
    })
}

fn image_task_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Dreamina task was not found",
        Some("task_id".to_owned()),
        "not_found",
    )
}

fn task_not_found() -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        "Dreamina task was not found",
        Some("task_id".to_owned()),
        "not_found",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::RequestRouteAttribution;
    use std::sync::Mutex;

    struct VideoQueryStore {
        status: Mutex<Option<VideoResultStatus>>,
        receipt: Option<image_api_contracts::dreamina::DreaminaReleasedSettlement>,
        receipt_reads: Mutex<Vec<(String, String, Option<Uuid>, Uuid)>>,
        manual_receipt: Option<DreaminaManualVideoCancellationProof>,
        manual_reads: Mutex<Vec<(String, String, Option<Uuid>, Uuid)>>,
    }

    #[async_trait::async_trait]
    impl ExecutionSettlementStore for VideoQueryStore {
        fn artifact_storage_identity(&self) -> String {
            "unused".to_owned()
        }

        async fn succeed(
            &self,
            _: &crate::admission::WorkLease,
            _: &crate::usage::UsageReservation,
            _: &crate::artifacts::GenerationResultManifest,
        ) -> Result<crate::usage::UsageSnapshot, ImageGatewayError> {
            unreachable!()
        }

        async fn fail(
            &self,
            _: &crate::admission::WorkLease,
            _: &crate::usage::UsageReservation,
            _: &'static str,
        ) -> Result<(), ImageGatewayError> {
            unreachable!()
        }

        async fn load_generation_result(
            &self,
            _: Uuid,
        ) -> Result<crate::settlement::GenerationResultLookup, ImageGatewayError> {
            unreachable!()
        }

        async fn generation_status(
            &self,
            _: Uuid,
        ) -> Result<crate::settlement::GenerationResultStatus, ImageGatewayError> {
            unreachable!()
        }

        async fn project_video_status(
            &self,
            _: &str,
            _: &str,
            _: Option<Uuid>,
            _: Uuid,
        ) -> Result<Option<VideoResultStatus>, ImageGatewayError> {
            Ok(self.status.lock().unwrap().take())
        }

        async fn project_video_released_settlement(
            &self,
            tenant_id: &str,
            project_id: &str,
            actor_user_id: Option<Uuid>,
            job_id: Uuid,
        ) -> Result<
            Option<image_api_contracts::dreamina::DreaminaReleasedSettlement>,
            ImageGatewayError,
        > {
            self.receipt_reads.lock().unwrap().push((
                tenant_id.to_owned(),
                project_id.to_owned(),
                actor_user_id,
                job_id,
            ));
            Ok(self.receipt.clone())
        }

        async fn project_dreamina_manual_video_cancellation(
            &self,
            tenant_id: &str,
            project_id: &str,
            actor_user_id: Option<Uuid>,
            job_id: Uuid,
        ) -> Result<Option<DreaminaManualVideoCancellationProof>, ImageGatewayError> {
            self.manual_reads.lock().unwrap().push((
                tenant_id.to_owned(),
                project_id.to_owned(),
                actor_user_id,
                job_id,
            ));
            Ok(self.manual_receipt.clone())
        }
    }

    #[tokio::test]
    async fn video_query_reads_no_release_receipt_for_normal_or_unknown_outcomes() {
        for status in [
            Some(VideoResultStatus::Pending {
                model: "seedance2.5".to_owned(),
                duration_ms: 5000,
                provider_progress: None,
                stage: crate::settlement::VideoPendingStage::Processing,
            }),
            Some(VideoResultStatus::Uncertain {
                submission_error_code: None,
                model: "seedance2.5".to_owned(),
                duration_ms: 5000,
                provider_progress: None,
            }),
            Some(VideoResultStatus::Succeeded {
                model: "seedance2.5".to_owned(),
                duration_ms: 5000,
                artifact_id: Uuid::new_v4(),
            }),
            Some(VideoResultStatus::Failed {
                model: "seedance2.5".to_owned(),
                duration_ms: 5000,
                error_code: Some("provider_submit_cli_failed".to_owned()),
            }),
            None,
        ] {
            let visible = status.is_some();
            let store = VideoQueryStore {
                status: Mutex::new(status),
                receipt: None,
                receipt_reads: Mutex::default(),
                manual_receipt: None,
                manual_reads: Mutex::default(),
            };
            let result = project_video_task(&store, &routed_auth(7), Uuid::new_v4()).await;
            assert_eq!(result.is_ok(), visible);
            if let Ok(task) = result {
                assert!(task.settlement.is_none());
                assert!(task.manual_cancellation.is_none());
            }
            assert!(store.receipt_reads.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn video_query_attaches_only_the_store_receipt_under_the_authenticated_scope() {
        let job_id = Uuid::new_v4();
        let mut auth = routed_auth(7);
        auth.actor_user_id = Some(Uuid::new_v4());
        let receipt = image_api_contracts::dreamina::DreaminaReleasedSettlement {
            state: "released".to_owned(),
            resolution_audit_id: Uuid::new_v4().to_string(),
            job_id: job_id.to_string(),
            request_id: "req_test".to_owned(),
            hold_id: Uuid::new_v4().to_string(),
            currency: "USD".to_owned(),
            held_micros: "1200000".to_owned(),
            released_micros: "1200000".to_owned(),
            captured_micros: "0".to_owned(),
        };
        for expected in [None, Some(receipt)] {
            let store = VideoQueryStore {
                status: Mutex::new(Some(VideoResultStatus::Failed {
                    model: "seedance2.0fast_vip".to_owned(),
                    duration_ms: 10000,
                    error_code: Some("dreamina_compliance_confirmation_required".to_owned()),
                })),
                receipt: expected.clone(),
                receipt_reads: Mutex::default(),
                manual_receipt: None,
                manual_reads: Mutex::default(),
            };
            let task = project_video_task(&store, &auth, job_id).await.unwrap();
            assert_eq!(task.status, "failed");
            assert_eq!(task.settlement, expected);
            let json = serde_json::to_value(task).unwrap();
            assert_eq!(json.get("settlement").is_some(), expected.is_some());
            assert_eq!(
                *store.receipt_reads.lock().unwrap(),
                vec![(
                    auth.tenant_id.clone(),
                    auth.project_id.clone(),
                    auth.actor_user_id,
                    job_id
                )]
            );
        }
    }

    fn cancellation_proof(job_id: Uuid) -> DreaminaManualVideoCancellationProof {
        DreaminaManualVideoCancellationProof {
            schema: "factory.manual_cancellation.v1".into(),
            authority_id: Uuid::new_v4(),
            job_id,
            request_id: "original-request".into(),
            upstream_task_id: "original-upstream".into(),
            submission_id: Uuid::new_v4(),
            hold_id: Uuid::new_v4(),
            confirmed_at_ms: 1234,
            currency: "USD".into(),
            held_micros: 9_007_199_254_740_993,
            captured_micros: 0,
            released_micros: 9_007_199_254_740_993,
            release_event_id: Uuid::new_v4(),
        }
    }

    #[tokio::test]
    async fn video_queries_attach_only_confirmed_scoped_proof_and_preserve_failed_status() {
        let job_id = Uuid::new_v4();
        let mut auth = routed_auth(7);
        auth.actor_user_id = Some(Uuid::new_v4());
        for ark in [false, true] {
            for proof in [None, Some(cancellation_proof(job_id))] {
                let store = VideoQueryStore {
                    status: Mutex::new(Some(VideoResultStatus::Failed {
                        model: "seedance2.5".into(),
                        duration_ms: 5000,
                        error_code: Some("provider_failed".into()),
                    })),
                    receipt: None,
                    receipt_reads: Mutex::default(),
                    manual_receipt: proof.clone(),
                    manual_reads: Mutex::default(),
                };
                let task = if ark {
                    serde_json::to_value(
                        super::super::ark::project_content_task(&store, &auth, job_id)
                            .await
                            .unwrap(),
                    )
                    .unwrap()
                } else {
                    serde_json::to_value(project_video_task(&store, &auth, job_id).await.unwrap())
                        .unwrap()
                };
                assert_eq!(task["status"], "failed");
                let public_proof = if ark {
                    &task["metadata"]["manual_cancellation"]
                } else {
                    &task["manual_cancellation"]
                };
                if let Some(proof) = proof {
                    assert_eq!(public_proof["status"], "confirmed");
                    assert_eq!(public_proof["schema"], "factory.manual_cancellation.v1");
                    assert_eq!(public_proof["authority_id"], proof.authority_id.to_string());
                    assert_eq!(
                        public_proof["release_event_id"],
                        proof.release_event_id.to_string()
                    );
                    assert_eq!(public_proof["held_micros"], "9007199254740993");
                    assert_eq!(public_proof["released_micros"], "9007199254740993");
                    assert_eq!(public_proof["captured_micros"], "0");
                } else {
                    assert!(public_proof.is_null());
                    if ark {
                        assert!(task.get("metadata").is_none());
                    }
                }
                if !ark {
                    assert_eq!(task["cancellation"]["supported"], false);
                }
                assert_eq!(
                    *store.manual_reads.lock().unwrap(),
                    vec![(
                        auth.tenant_id.clone(),
                        auth.project_id.clone(),
                        auth.actor_user_id,
                        job_id
                    )]
                );
            }
        }
    }

    #[tokio::test]
    async fn nonfailed_video_queries_never_read_or_attach_manual_cancellation() {
        let job_id = Uuid::new_v4();
        for ark in [false, true] {
            for status in [
                VideoResultStatus::Pending {
                    model: "seedance2.5".into(),
                    duration_ms: 5000,
                    stage: VideoPendingStage::Processing,
                    provider_progress: None,
                },
                VideoResultStatus::Uncertain {
                    model: "seedance2.5".into(),
                    duration_ms: 5000,
                    provider_progress: None,
                    submission_error_code: None,
                },
                VideoResultStatus::Succeeded {
                    model: "seedance2.5".into(),
                    duration_ms: 5000,
                    artifact_id: Uuid::new_v4(),
                },
            ] {
                let store = VideoQueryStore {
                    status: Mutex::new(Some(status)),
                    receipt: None,
                    receipt_reads: Mutex::default(),
                    manual_receipt: Some(cancellation_proof(job_id)),
                    manual_reads: Mutex::default(),
                };
                let task = if ark {
                    serde_json::to_value(
                        super::super::ark::project_content_task(&store, &routed_auth(7), job_id)
                            .await
                            .unwrap(),
                    )
                    .unwrap()
                } else {
                    serde_json::to_value(
                        project_video_task(&store, &routed_auth(7), job_id)
                            .await
                            .unwrap(),
                    )
                    .unwrap()
                };
                assert!(task["manual_cancellation"].is_null());
                assert!(task["metadata"]["manual_cancellation"].is_null());
                assert!(store.manual_reads.lock().unwrap().is_empty());
            }
        }
    }

    #[test]
    fn inconsistent_cancellation_proof_cannot_be_returned() {
        let job_id = Uuid::new_v4();
        let proof = cancellation_proof(job_id);
        assert!(public_manual_cancellation(Uuid::new_v4(), proof.clone()).is_err());
        for mutation in [0, 1, 2, 3] {
            let mut invalid = proof.clone();
            match mutation {
                0 => invalid.captured_micros = 1,
                1 => invalid.released_micros -= 1,
                2 => invalid.schema = "client-assertion".into(),
                _ => invalid.release_event_id = Uuid::nil(),
            }
            assert!(public_manual_cancellation(job_id, invalid).is_err());
        }
    }

    #[test]
    fn terminal_submit_reasons_preserve_uncertainty_without_releasing_funds() {
        for code in [
            "dreamina_cli_permission_denied",
            "provider_submit_timeout",
            "provider_submit_cli_failed",
            "provider_submit_deadline",
        ] {
            let task = video_task(
                Uuid::new_v4(),
                VideoResultStatus::Uncertain {
                    model: "seedance2.0fast_vip".to_owned(),
                    duration_ms: 5000,
                    provider_progress: None,
                    submission_error_code: Some(code.to_owned()),
                },
            );
            assert_eq!(task.status, "uncertain");
            assert_eq!(task.stage.as_deref(), Some("submission_uncertain"));
            let error = task.error.as_ref().unwrap();
            assert_eq!(error.code, "generation_uncertain");
            assert_eq!(error.reason_code.as_deref(), Some(code));
            assert!(error.message.contains("unconfirmed"));
            assert_ne!(error.message, VIDEO_UNCERTAIN_MESSAGE);
            assert!(task.content.is_none());
            assert!(task.settlement.is_none());
            assert!(!task.cancellation.as_ref().unwrap().supported);
        }
    }

    #[test]
    fn unknown_submit_reasons_do_not_echo_private_values_or_change_legacy_shape() {
        let private = "https://example.invalid/private?token=not-public";
        let task = video_task(
            Uuid::new_v4(),
            VideoResultStatus::Uncertain {
                model: "seedance2.5".to_owned(),
                duration_ms: 5000,
                provider_progress: None,
                submission_error_code: Some(private.to_owned()),
            },
        );
        let value = serde_json::to_value(task).unwrap();
        assert_eq!(value["status"], "uncertain");
        assert_eq!(value["error"]["code"], "generation_uncertain");
        assert_eq!(value["error"]["message"], VIDEO_UNCERTAIN_MESSAGE);
        assert!(value.get("stage").is_none());
        assert!(value["error"].get("reason_code").is_none());
        assert!(!value.to_string().contains(private));
    }

    #[test]
    fn video_query_distinguishes_uncertain_from_running_without_inventing_a_result() {
        let id = Uuid::new_v4();
        let pending = video_task(
            id,
            VideoResultStatus::Pending {
                model: "seedance2.0fast_vip".to_owned(),
                duration_ms: 5000,
                provider_progress: None,
                stage: crate::settlement::VideoPendingStage::Processing,
            },
        );
        assert_eq!(pending.status, "running");
        assert!(pending.error.is_none());
        assert!(pending.content.is_none());

        let uncertain = serde_json::to_value(video_task(
            id,
            VideoResultStatus::Uncertain {
                submission_error_code: None,
                model: "seedance2.0fast_vip".to_owned(),
                duration_ms: 5000,
                provider_progress: None,
            },
        ))
        .unwrap();
        assert_eq!(uncertain["id"], id.to_string());
        assert_eq!(uncertain["status"], "uncertain");
        assert_eq!(uncertain["model"], "seedance2.0fast_vip");
        assert_eq!(uncertain["error"]["code"], "generation_uncertain");
        assert_eq!(uncertain["error"]["message"], VIDEO_UNCERTAIN_MESSAGE);
        assert!(uncertain.get("content").is_none());
    }

    #[test]
    fn public_progress_marks_estimates_and_keeps_generation_nonterminal() {
        let mut progress = queue_progress();
        progress.progress.provider_status = "Generating".to_owned();
        progress.estimated_percent = Some(0);
        let json = serde_json::to_value(public_provider_progress(progress)).unwrap();
        assert_eq!(json["progress_percent"], 0);
        assert_eq!(json["progress_source"], "estimated");
        assert_eq!(json["stage"], "generating");
        let queued = serde_json::to_value(public_provider_progress(queue_progress())).unwrap();
        assert!(queued.get("progress_percent").is_none());
        assert!(queued.get("progress_source").is_none());
        assert_eq!(queued["stage"], "provider_queued");
    }

    fn queue_progress() -> VideoProviderProgress {
        VideoProviderProgress {
            estimated_percent: None,
            progress: image_provider_sdk::ProviderTaskProgress {
                provider_status: "Queueing".to_owned(),
                queue_position: Some(3341),
                queue_total: Some(476961),
                estimated_remaining_seconds: Some(27131),
            },
            observed_at_ms: 1_792_828_821_077,
        }
    }

    #[test]
    fn video_query_exposes_the_original_queue_and_observation_time_without_cancel_support() {
        let task = video_task(
            Uuid::new_v4(),
            VideoResultStatus::Pending {
                model: "seedance2.0fast".to_owned(),
                duration_ms: 10_000,
                stage: VideoPendingStage::Processing,
                provider_progress: Some(queue_progress()),
            },
        );
        let json = serde_json::to_value(task).unwrap();
        assert_eq!(json["status"], "running");
        assert_eq!(json["stage"], "provider_queued");
        assert_eq!(json["provider_progress"]["provider_status"], "Queueing");
        assert_eq!(json["provider_progress"]["queue_position"], 3341);
        assert_eq!(json["provider_progress"]["queue_total"], 476961);
        assert_eq!(
            json["provider_progress"]["estimated_remaining_seconds"],
            27131
        );
        assert_eq!(
            json["provider_progress"]["observed_at_ms"],
            1_792_828_821_077_i64
        );
        assert_eq!(json["cancellation"]["supported"], false);
        assert_eq!(
            json["cancellation"]["reason"],
            "provider_cancellation_unsupported"
        );
        assert!(json.get("content").is_none());
        assert!(json.get("settlement").is_none());
        assert!(json["provider_progress"].get("priority").is_none());
    }

    #[test]
    fn local_queue_and_unknown_provider_progress_do_not_invent_an_upstream_phase() {
        let local = video_task(
            Uuid::new_v4(),
            VideoResultStatus::Pending {
                model: "seedance2.0fast".to_owned(),
                duration_ms: 10_000,
                stage: VideoPendingStage::Queued,
                provider_progress: None,
            },
        );
        assert_eq!(local.stage.as_deref(), Some("queued"));
        assert!(local.provider_progress.is_none());

        let mut progress = queue_progress();
        progress.progress.provider_status = "FutureProviderState".to_owned();
        progress.progress.queue_position = None;
        progress.progress.queue_total = None;
        progress.progress.estimated_remaining_seconds = None;
        let unknown = serde_json::to_value(video_task(
            Uuid::new_v4(),
            VideoResultStatus::Pending {
                model: "seedance2.0fast".to_owned(),
                duration_ms: 10_000,
                stage: VideoPendingStage::Processing,
                provider_progress: Some(progress),
            },
        ))
        .unwrap();
        assert_eq!(unknown["stage"], "processing");
        assert_eq!(
            unknown["provider_progress"]["provider_status"],
            "FutureProviderState"
        );
        assert!(unknown["provider_progress"].get("queue_position").is_none());
        assert!(unknown["provider_progress"].get("queue_total").is_none());
        assert!(
            unknown["provider_progress"]
                .get("estimated_remaining_seconds")
                .is_none()
        );
    }

    #[test]
    fn a_last_known_queue_observation_never_hides_uncertainty_or_refreshes_its_timestamp() {
        let task = video_task(
            Uuid::new_v4(),
            VideoResultStatus::Uncertain {
                submission_error_code: None,
                model: "seedance2.0fast".to_owned(),
                duration_ms: 10_000,
                provider_progress: Some(queue_progress()),
            },
        );
        assert_eq!(task.status, "uncertain");
        assert_eq!(task.error.unwrap().code, "generation_uncertain");
        assert!(task.stage.is_none());
        assert_eq!(
            task.provider_progress.unwrap().observed_at_ms,
            1_792_828_821_077
        );
        assert!(task.settlement.is_none());
    }

    #[test]
    fn video_query_explains_compliance_rejection_without_suggesting_an_old_task_retry() {
        for (code, expected_code, expected_message) in [
            (
                Some("dreamina_compliance_confirmation_required"),
                "dreamina_compliance_confirmation_required",
                DREAMINA_COMPLIANCE_CONFIRMATION_MESSAGE,
            ),
            (
                Some("dreamina_copyright_rejected"),
                "dreamina_copyright_rejected",
                "生成的视频里包含受版权保护元素",
            ),
            (
                Some("provider_submit_cli_failed"),
                "provider_submit_cli_failed",
                "Dreamina video generation failed",
            ),
            (
                None,
                "generation_failed",
                "Dreamina video generation failed",
            ),
        ] {
            let task = video_task(
                Uuid::new_v4(),
                VideoResultStatus::Failed {
                    model: "seedance2.0fast_vip".to_owned(),
                    duration_ms: 5000,
                    error_code: code.map(str::to_owned),
                },
            );
            assert_eq!(task.status, "failed");
            assert!(task.content.is_none());
            let error = task.error.unwrap();
            assert_eq!(error.code, expected_code);
            assert_eq!(error.message, expected_message);
        }
    }

    #[test]
    fn video_duration_wire_keeps_whole_seconds_and_exact_millisecond_fractions() {
        for (milliseconds, json) in [(5000, "5"), (2500, "2.5"), (2001, "2.001"), (152000, "152")] {
            assert_eq!(
                serde_json::to_string(&duration_seconds(milliseconds)).unwrap(),
                json
            );
            let task = video_task(
                Uuid::new_v4(),
                VideoResultStatus::Succeeded {
                    model: "multiframe2video".to_owned(),
                    duration_ms: milliseconds,
                    artifact_id: Uuid::new_v4(),
                },
            );
            assert_eq!(
                serde_json::to_value(task).unwrap()["content"]["duration"].to_string(),
                json
            );
        }
    }

    fn routed_auth(route_revision: i64) -> AuthContext {
        AuthContext {
            tenant_id: "tenant-a".to_string(),
            project_id: "project-a".to_string(),
            project_service_tier: crate::service_tiers::ProjectServiceTier::Default,
            service_account_id: None,
            api_key_id: None,
            credential_authz_version: None,
            credential_owner_user_id: None,
            actor_user_id: None,
            actor_session_id: None,
            actor_authz_version: None,
            api_key_permission_mode: crate::auth::ApiKeyPermissionMode::All,
            api_key_permissions: crate::auth::ApiKeyPermissions::default(),
            route: Some(RequestRouteAttribution {
                public_model_id: "public-seedance".to_string(),
                api_profile: DREAMINA_VIDEOS_API_PROFILE.to_string(),
                provider_id: image_provider_dreamina_cli::PROVIDER_ID.to_string(),
                operation_id: "videos.generations".to_string(),
                command_schema: image_provider_dreamina_cli::DREAMINA_SUBMIT_COMMAND_SCHEMA
                    .to_string(),
                media_kind: "video".to_string(),
                route_id: Uuid::from_u128(17),
                route_revision,
            }),
            is_admin: false,
        }
    }

    #[test]
    fn customer_pricing_hash_binds_public_execution_and_route_identity() {
        let auth = routed_auth(7);
        let provider_hash = "a".repeat(64);
        let base = customer_pricing_request_hash(
            &auth,
            DREAMINA_VIDEOS_API_PROFILE,
            "public-seedance",
            "execution-seedance",
            &provider_hash,
        )
        .expect("valid pricing identity");
        let replay = customer_pricing_request_hash(
            &auth,
            DREAMINA_VIDEOS_API_PROFILE,
            "public-seedance",
            "execution-seedance",
            &provider_hash,
        )
        .expect("stable pricing identity");
        let alias_changed = customer_pricing_request_hash(
            &auth,
            DREAMINA_VIDEOS_API_PROFILE,
            "another-public-alias",
            "execution-seedance",
            &provider_hash,
        )
        .expect("valid alias identity");
        let execution_changed = customer_pricing_request_hash(
            &auth,
            DREAMINA_VIDEOS_API_PROFILE,
            "public-seedance",
            "another-execution-model",
            &provider_hash,
        )
        .expect("valid execution identity");
        let route_changed = customer_pricing_request_hash(
            &routed_auth(8),
            DREAMINA_VIDEOS_API_PROFILE,
            "public-seedance",
            "execution-seedance",
            &provider_hash,
        )
        .expect("valid route identity");

        assert_eq!(base, replay);
        assert_eq!(base.len(), 64);
        assert_ne!(base, alias_changed);
        assert_ne!(base, execution_changed);
        assert_ne!(base, route_changed);
    }

    #[test]
    fn native_batch_customer_identity_keeps_the_pre_upgrade_hash() {
        let plan = DreaminaImageAdmissionPlan::new(
            serde_json::from_value(serde_json::json!({
                "prompt": "idempotent image", "generate_num": 2, "resolution_type": "2k"
            }))
            .unwrap(),
        )
        .unwrap();
        let mut historical = plan
            .attach(
                crate::admission::AdmissionTicket {
                    session_id: Uuid::new_v4(),
                    owner_token: Uuid::new_v4(),
                    request_hash: plan.request_identity_hash().to_owned(),
                },
                Uuid::new_v4(),
                "test",
            )
            .command_json;
        historical["schema_version"] = serde_json::json!(1);
        let old_hash = hex::encode(Sha256::digest(serde_json::to_vec(&historical).unwrap()));
        let auth = routed_auth(7);
        let hash = |source: &str| {
            customer_pricing_request_hash(
                &auth,
                DREAMINA_IMAGES_API_PROFILE,
                "public-image",
                "dreamina-image-5.0",
                source,
            )
            .unwrap()
        };
        assert_eq!(hash(&old_hash), hash(plan.request_identity_hash()));
        assert_ne!(hash(&old_hash), hash(plan.provider_command_hash()));
    }
}

#[cfg(test)]
#[path = "dreamina/image_tasks_tests.rs"]
mod image_tasks_tests;

#[cfg(test)]
#[path = "dreamina/complete_image_tasks_tests.rs"]
mod complete_image_tasks_tests;
