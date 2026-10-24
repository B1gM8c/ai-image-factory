use std::sync::Arc;

use axum::{
    Json,
    extract::{Extension, Path, State},
    http::HeaderMap,
    response::Response,
};
use image_api_contracts::dreamina::{
    DREAMINA_IMAGES_API_PROFILE, DREAMINA_VIDEOS_API_PROFILE, DreaminaCancellation,
    DreaminaImageGenerationRequest, DreaminaImageRequest, DreaminaImageWorkflowRequest,
    DreaminaProviderProgress, DreaminaTaskCreated, DreaminaTaskError, DreaminaVideoContent,
    DreaminaVideoGenerationRequest, DreaminaVideoRequest, DreaminaVideoTask,
    DreaminaVideoWorkflowRequest,
};
use image_provider_contracts::BillingMetric;
use serde::Serialize;
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
        ExecutionSettlementStore, VideoPendingStage, VideoProviderProgress, VideoResultStatus,
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
    create_image_with_optional_route(state, auth, headers, request_id, request, None).await
}

pub(super) async fn create_image_with_resolved_auth(
    state: &Arc<AppState>,
    auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    request: DreaminaImageGenerationRequest,
    resolved: ResolvedModelRoute,
) -> Result<Response, ImageGatewayError> {
    create_image_with_optional_route(state, auth, headers, request_id, request, Some(resolved))
        .await
}

async fn create_image_with_optional_route(
    state: &Arc<AppState>,
    mut auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    mut request: DreaminaImageGenerationRequest,
    resolved: Option<ResolvedModelRoute>,
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
    let result = submit_image_generation(
        state,
        &auth,
        request_id,
        claim,
        plan.provider_id(),
        &job_model,
        plan.output_count(),
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
    render_stored_generation_response(result, &auth)
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
    let job_id = submit_workflow(
        state,
        &auth,
        headers,
        request_id,
        DREAMINA_IMAGES_API_PROFILE,
        resolved.public_model_id,
        resolved.execution_model_id,
        intent,
    )
    .await?;
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
        } if is_video || status == "succeeded" => return Ok(job_id),
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
    let ticket = match claim_admission_with_retry(state, claim)
        .await
        .map_err(admission_error)?
    {
        AdmissionClaim::Owner(ticket) => ticket,
        AdmissionClaim::Existing {
            job_id,
            state: admission_state,
            ..
        } if admission_state == "succeeded" => {
            return replay_generation_result(state, job_id).await;
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
    wait_for_generation_result(state, reservation.job_id).await
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
    let mut task = video_task(job_id, status);
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
            let stage = match (&provider_progress, stage) {
                (_, VideoPendingStage::Queued) => "queued",
                (_, VideoPendingStage::Dispatching) => "dispatching",
                (Some(progress), VideoPendingStage::Processing)
                    if progress.progress.provider_status == "Queueing" =>
                {
                    "provider_queued"
                }
                (_, VideoPendingStage::Processing) => "processing",
            };
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
            }
        }
        VideoResultStatus::Uncertain {
            model,
            provider_progress,
            ..
        } => DreaminaVideoTask {
            id: id.to_string(),
            status: "uncertain".to_owned(),
            stage: None,
            provider_progress: provider_progress.map(public_provider_progress),
            cancellation: None,
            model: Some(model),
            error: Some(DreaminaTaskError {
                code: "generation_uncertain".to_owned(),
                message: VIDEO_UNCERTAIN_MESSAGE.to_owned(),
            }),
            content: None,
            settlement: None,
        },
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
        },
        VideoResultStatus::Failed {
            model, error_code, ..
        } => {
            let message = match error_code.as_deref() {
                Some("dreamina_compliance_confirmation_required") => {
                    DREAMINA_COMPLIANCE_CONFIRMATION_MESSAGE
                }
                _ => "Dreamina video generation failed",
            };
            DreaminaVideoTask {
                id: id.to_string(),
                status: "failed".to_owned(),
                stage: None,
                provider_progress: None,
                cancellation: None,
                model: Some(model),
                error: Some(DreaminaTaskError {
                    code: error_code.unwrap_or_else(|| "generation_failed".to_owned()),
                    message: message.to_owned(),
                }),
                content: None,
                settlement: None,
            }
        }
    };
    task.cancellation = Some(DreaminaCancellation {
        supported: false,
        reason: "provider_cancellation_unsupported".to_owned(),
    });
    task
}

fn public_provider_progress(value: VideoProviderProgress) -> DreaminaProviderProgress {
    DreaminaProviderProgress {
        provider_status: value.progress.provider_status,
        queue_position: value.progress.queue_position,
        queue_total: value.progress.queue_total,
        estimated_remaining_seconds: value.progress.estimated_remaining_seconds,
        observed_at_ms: value.observed_at_ms,
    }
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
            };
            let result = project_video_task(&store, &routed_auth(7), Uuid::new_v4()).await;
            assert_eq!(result.is_ok(), visible);
            if let Ok(task) = result {
                assert!(task.settlement.is_none());
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

    fn queue_progress() -> VideoProviderProgress {
        VideoProviderProgress {
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
