use std::sync::Arc;

use axum::{
    Json,
    extract::{Extension, Path, State},
    http::HeaderMap,
    response::Response,
};
use image_api_contracts::dreamina::{
    DREAMINA_IMAGES_API_PROFILE, DREAMINA_VIDEOS_API_PROFILE, DreaminaImageGenerationRequest,
    DreaminaTaskCreated, DreaminaTaskError, DreaminaVideoContent, DreaminaVideoGenerationRequest,
    DreaminaVideoTask,
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
        DreaminaVideoAdmissionPlan, GENERATION_OPERATION, VIDEO_GENERATION_OPERATION,
        idempotency_key_digest,
    },
    artifacts::StoredGenerationResult,
    auth::{ApiKeyCapability, AuthContext},
    model_routing::ResolvedModelRoute,
    settlement::VideoResultStatus,
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
    body: Result<Json<DreaminaImageGenerationRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesWrite)?;
    let Json(request) = body.map_err(invalid_json)?;
    create_image_with_auth(&state, auth, &headers, request_id.0, request).await
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
            plan.provider_command_hash(),
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
    body: Result<Json<DreaminaVideoGenerationRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<DreaminaTaskCreated>, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::VideosWrite)?;
    let Json(request) = body.map_err(invalid_json)?;
    create_video_with_auth(&state, auth, &headers, request_id.0, request)
        .await
        .map(Json)
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

fn validate_resolved_dreamina_route(
    resolved: &ResolvedModelRoute,
    api_profile: &str,
    operation_id: &str,
) -> Result<(), ImageGatewayError> {
    if resolved.provider_id != image_provider_dreamina_cli::PROVIDER_ID
        || resolved.api_profile != api_profile
        || resolved.operation_id != operation_id
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
    let status = state
        .settlement_store
        .project_video_status(
            &auth.tenant_id,
            &auth.project_id,
            auth.actor_user_id,
            job_id,
        )
        .await?
        .ok_or_else(task_not_found)?;
    Ok(Json(video_task(job_id, status)))
}

fn video_task(id: Uuid, status: VideoResultStatus) -> DreaminaVideoTask {
    match status {
        VideoResultStatus::Pending { model, .. } | VideoResultStatus::Uncertain { model, .. } => {
            DreaminaVideoTask {
                id: id.to_string(),
                status: "running".to_owned(),
                model: Some(model),
                error: None,
                content: None,
            }
        }
        VideoResultStatus::Succeeded {
            model,
            duration,
            artifact_id,
        } => DreaminaVideoTask {
            id: id.to_string(),
            status: "succeeded".to_owned(),
            model: Some(model),
            error: None,
            content: Some(DreaminaVideoContent {
                video_url: format!("/v1/dreamina/files/{artifact_id}/content"),
                duration,
            }),
        },
        VideoResultStatus::Failed {
            model, error_code, ..
        } => DreaminaVideoTask {
            id: id.to_string(),
            status: "failed".to_owned(),
            model: Some(model),
            error: Some(DreaminaTaskError {
                code: error_code.unwrap_or_else(|| "generation_failed".to_owned()),
                message: "Dreamina video generation failed".to_owned(),
            }),
            content: None,
        },
    }
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

fn dreamina_admission_error(error: DreaminaAdmissionError) -> ImageGatewayError {
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
}
