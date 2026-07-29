use std::{io::Cursor, sync::Arc};

use axum::{
    Json,
    extract::{Extension, Path, State},
    http::HeaderMap,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use image::ImageReader;
use image_api_contracts::ark::{
    ARK_CONTENT_GENERATION_API_PROFILE, ARK_IMAGES_API_PROFILE, ArkContentGenerationError,
    ArkContentGenerationTask, ArkContentGenerationTaskId, ArkContentGenerationTaskRequest,
    ArkGeneratedContent, ArkImageData, ArkImageGenerationRequest, ArkImageGenerationResponse,
    ArkImageUsage,
};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    admission::{
        AdmissionContract, ArkAdmissionError, ArkImageAdmissionPlan, ArkVideoAdmissionPlan,
        CustomerPricingIntent, GENERATION_OPERATION, VIDEO_GENERATION_OPERATION,
    },
    artifacts::StoredGenerationResult,
    auth::{ApiKeyCapability, AuthContext},
    model_routing::ResolvedModelRoute,
    settlement::VideoResultStatus,
};

use super::{
    AppState, RequestId, authenticate_image_request,
    dreamina::{
        customer_pricing_request_hash, idempotency_digest, invalid_json,
        require_external_execution, submit_image_generation, submit_video_generation,
    },
    images::{admission_deadline, generation_admission_contract},
    resolve_request_model,
};

pub(super) async fn create_image(
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<ArkImageGenerationRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<ArkImageGenerationResponse>, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesWrite)?;
    let Json(request) = body.map_err(invalid_json)?;
    create_image_with_optional_route(&state, auth, &headers, request_id.0, request, None)
        .await
        .map(Json)
}

pub(super) async fn create_image_with_resolved_auth(
    state: &Arc<AppState>,
    auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    request: ArkImageGenerationRequest,
    resolved: ResolvedModelRoute,
) -> Result<ArkImageGenerationResponse, ImageGatewayError> {
    create_image_with_optional_route(state, auth, headers, request_id, request, Some(resolved))
        .await
}

async fn create_image_with_optional_route(
    state: &Arc<AppState>,
    mut auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    mut request: ArkImageGenerationRequest,
    resolved: Option<ResolvedModelRoute>,
) -> Result<ArkImageGenerationResponse, ImageGatewayError> {
    require_external_execution(state)?;
    let contract = generation_admission_contract(state.config.generation_admission_contract);
    let requested_model = request.model.clone();
    let mut public_model_id = requested_model.clone();
    let mut execution_model_id = None;
    let resolved = match resolved {
        Some(resolved) => {
            validate_resolved_ark_route(&resolved, ARK_IMAGES_API_PROFILE, "images.generations")?;
            Some(resolved)
        }
        None => {
            resolve_request_model(
                state,
                &mut auth,
                image_provider_dreamina_cli::PROVIDER_ID,
                "images.generations",
                ARK_IMAGES_API_PROFILE,
                Some(&requested_model),
                "5.0Pro",
            )
            .await?
        }
    };
    if let Some(resolved) = resolved {
        public_model_id = resolved.public_model_id;
        execution_model_id = Some(resolved.execution_model_id);
        request.model = ark_image_model_for_provider(&resolved.provider_model_id)
            .ok_or_else(|| ImageGatewayError::model_not_found(&requested_model))?
            .to_owned();
    }
    if contract == AdmissionContract::CustomerPricingV4 && execution_model_id.is_none() {
        return Err(ImageGatewayError::service_unavailable(
            "customer pricing requires an enabled model route",
        ));
    }
    let plan = ArkImageAdmissionPlan::new(request)
        .map_err(|error| ark_admission_error(error, &requested_model))?;
    let idempotency = idempotency_digest(
        headers,
        &auth.project_id,
        ARK_IMAGES_API_PROFILE,
        GENERATION_OPERATION,
    )?;
    let job_model = execution_model_id
        .clone()
        .unwrap_or_else(|| plan.provider_model().to_string());
    let mut claim = plan.claim(
        ARK_IMAGES_API_PROFILE,
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
            ARK_IMAGES_API_PROFILE,
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
    ark_image_response(requested_model, result)
}

pub(super) async fn create_content_task(
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<ArkContentGenerationTaskRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<ArkContentGenerationTaskId>, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::VideosWrite)?;
    let Json(request) = body.map_err(invalid_json)?;
    create_content_task_with_optional_route(&state, auth, &headers, request_id.0, request, None)
        .await
        .map(Json)
}

pub(super) async fn create_content_task_with_resolved_auth(
    state: &Arc<AppState>,
    auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    request: ArkContentGenerationTaskRequest,
    resolved: ResolvedModelRoute,
) -> Result<ArkContentGenerationTaskId, ImageGatewayError> {
    create_content_task_with_optional_route(
        state,
        auth,
        headers,
        request_id,
        request,
        Some(resolved),
    )
    .await
}

async fn create_content_task_with_optional_route(
    state: &Arc<AppState>,
    mut auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    mut request: ArkContentGenerationTaskRequest,
    resolved: Option<ResolvedModelRoute>,
) -> Result<ArkContentGenerationTaskId, ImageGatewayError> {
    require_external_execution(state)?;
    let contract = generation_admission_contract(state.config.generation_admission_contract);
    let requested_model = request.model.clone();
    let mut public_model_id = requested_model.clone();
    let mut execution_model_id = None;
    let resolved = match resolved {
        Some(resolved) => {
            validate_resolved_ark_route(
                &resolved,
                ARK_CONTENT_GENERATION_API_PROFILE,
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
                ARK_CONTENT_GENERATION_API_PROFILE,
                Some(&requested_model),
                "seedance2.0fast",
            )
            .await?
        }
    };
    if let Some(resolved) = resolved {
        public_model_id = resolved.public_model_id;
        execution_model_id = Some(resolved.execution_model_id);
        request.model = ark_video_model_for_provider(&resolved.provider_model_id)
            .ok_or_else(|| ImageGatewayError::model_not_found(&requested_model))?
            .to_owned();
    }
    if contract == AdmissionContract::CustomerPricingV4 && execution_model_id.is_none() {
        return Err(ImageGatewayError::service_unavailable(
            "customer pricing requires an enabled model route",
        ));
    }
    let plan = ArkVideoAdmissionPlan::new(request)
        .map_err(|error| ark_admission_error(error, &requested_model))?;
    let idempotency = idempotency_digest(
        headers,
        &auth.project_id,
        ARK_CONTENT_GENERATION_API_PROFILE,
        VIDEO_GENERATION_OPERATION,
    )?;
    let job_model = execution_model_id
        .clone()
        .unwrap_or_else(|| plan.provider_model().to_string());
    let mut claim = plan.claim(
        ARK_CONTENT_GENERATION_API_PROFILE,
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
            ARK_CONTENT_GENERATION_API_PROFILE,
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
    Ok(ArkContentGenerationTaskId {
        id: public_task_id(job_id),
        safety_identifier: None,
    })
}

pub(super) async fn get_content_task(
    Path(task_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ArkContentGenerationTask>, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::VideosRead)?;
    let job_id = parse_task_id(&task_id)?;
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
    Ok(Json(ark_video_task(job_id, status)))
}

fn ark_image_response(
    model: String,
    result: StoredGenerationResult,
) -> Result<ArkImageGenerationResponse, ImageGatewayError> {
    let generated_images = u32::try_from(result.images.len())
        .map_err(|_| ImageGatewayError::internal("generated image count overflow"))?;
    let data = result
        .images
        .into_iter()
        .map(|image| {
            let dimensions = ImageReader::new(Cursor::new(&image.bytes))
                .with_guessed_format()
                .map_err(|_| ImageGatewayError::artifact_integrity())?
                .into_dimensions()
                .map_err(|_| ImageGatewayError::artifact_integrity())?;
            Ok(ArkImageData {
                url: None,
                b64_json: Some(STANDARD.encode(image.bytes)),
                size: format!("{}x{}", dimensions.0, dimensions.1),
            })
        })
        .collect::<Result<Vec<_>, ImageGatewayError>>()?;
    Ok(ArkImageGenerationResponse {
        model,
        created: result.projection.created_at_seconds,
        created_at: result.projection.created_at_seconds,
        data,
        error: None,
        usage: ArkImageUsage {
            generated_images,
            output_tokens: None,
            total_tokens: None,
        },
        tool: Vec::new(),
    })
}

fn ark_video_task(id: Uuid, status: VideoResultStatus) -> ArkContentGenerationTask {
    let public_id = public_task_id(id);
    match status {
        VideoResultStatus::Pending {
            model, duration, ..
        }
        | VideoResultStatus::Uncertain { model, duration } => ArkContentGenerationTask {
            id: public_id,
            model: public_video_model(&model).to_owned(),
            status: "running".to_owned(),
            error: None,
            content: None,
            usage: None,
            duration: Some(duration),
            resolution: Some("720p".to_owned()),
            ratio: None,
        },
        VideoResultStatus::Succeeded {
            model,
            duration,
            artifact_id,
        } => ArkContentGenerationTask {
            id: public_id,
            model: public_video_model(&model).to_owned(),
            status: "succeeded".to_owned(),
            error: None,
            content: Some(ArkGeneratedContent {
                video_url: Some(format!("/api/v3/files/{artifact_id}/content")),
                last_frame_url: None,
                file_url: None,
            }),
            usage: None,
            duration: Some(duration),
            resolution: Some("720p".to_owned()),
            ratio: None,
        },
        VideoResultStatus::Failed {
            model,
            duration,
            error_code,
        } => ArkContentGenerationTask {
            id: public_id,
            model: public_video_model(&model).to_owned(),
            status: "failed".to_owned(),
            error: Some(ArkContentGenerationError {
                message: "Video generation failed".to_owned(),
                code: error_code.unwrap_or_else(|| "InternalError".to_owned()),
            }),
            content: None,
            usage: None,
            duration: Some(duration),
            resolution: Some("720p".to_owned()),
            ratio: None,
        },
    }
}

fn public_video_model(provider_model: &str) -> &str {
    match provider_model {
        "seedance2.0" => "doubao-seedance-2-0-260128",
        "seedance2.0fast" => "doubao-seedance-2-0-fast-260128",
        "seedance2.0mini" => "doubao-seedance-2-0-mini-260128",
        other => other,
    }
}

fn validate_resolved_ark_route(
    resolved: &ResolvedModelRoute,
    api_profile: &str,
    operation_id: &str,
) -> Result<(), ImageGatewayError> {
    if resolved.provider_id != image_provider_dreamina_cli::PROVIDER_ID
        || resolved.api_profile != api_profile
        || resolved.operation_id != operation_id
    {
        return Err(ImageGatewayError::service_unavailable(
            "resolved model route does not match the Ark execution surface",
        ));
    }
    Ok(())
}

fn ark_image_model_for_provider(provider_model: &str) -> Option<&'static str> {
    match provider_model {
        "5.0" => Some("doubao-seedream-5-0-lite"),
        "5.0Pro" => Some("doubao-seedream-5-0-260128"),
        _ => None,
    }
}

fn ark_video_model_for_provider(provider_model: &str) -> Option<&'static str> {
    match provider_model {
        "seedance2.0" => Some("doubao-seedance-2-0-260128"),
        "seedance2.0fast" => Some("doubao-seedance-2-0-fast-260128"),
        "seedance2.0mini" => Some("doubao-seedance-2-0-mini-260128"),
        _ => None,
    }
}

fn public_task_id(job_id: Uuid) -> String {
    format!("cgt-{job_id}")
}

fn parse_task_id(value: &str) -> Result<Uuid, ImageGatewayError> {
    value
        .strip_prefix("cgt-")
        .unwrap_or(value)
        .parse()
        .map_err(|_| {
            ImageGatewayError::invalid_request(
                "Content generation task id is invalid",
                Some("task_id".to_owned()),
                "invalid_value",
            )
        })
}

fn task_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Content generation task was not found",
        Some("task_id".to_owned()),
        "not_found",
    )
}

fn ark_admission_error(error: ArkAdmissionError, requested_model: &str) -> ImageGatewayError {
    let parameter = match error.parameter() {
        "model_version" => "model",
        "resolution_type" => "resolution",
        parameter => parameter,
    };
    if parameter == "model" {
        return ImageGatewayError::model_not_found(requested_model);
    }
    match error {
        ArkAdmissionError::Unsupported(_) => {
            ImageGatewayError::unsupported(parameter, error.to_string())
        }
        ArkAdmissionError::InvalidValue(_) | ArkAdmissionError::Dreamina(_) => {
            ImageGatewayError::invalid_request(
                error.to_string(),
                Some(parameter.to_owned()),
                "invalid_value",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        artifacts::GenerationResponseProjection, generator::GeneratedImage, usage::UsageSnapshot,
    };
    use image::{ImageBuffer, ImageFormat, Rgba};

    #[test]
    fn ark_image_response_reports_actual_dimensions_and_base64() {
        let image = ImageBuffer::from_pixel(3, 2, Rgba([255u8, 255, 255, 255]));
        let mut cursor = Cursor::new(Vec::new());
        image.write_to(&mut cursor, ImageFormat::Png).unwrap();
        let response = ark_image_response(
            "doubao-seedream-5-0-260128".to_owned(),
            StoredGenerationResult {
                projection: GenerationResponseProjection {
                    api_profile: ARK_IMAGES_API_PROFILE.to_owned(),
                    operation: GENERATION_OPERATION.to_owned(),
                    response_schema: "test".to_owned(),
                    created_at_seconds: 123,
                    output_format: "png".to_owned(),
                    quality: "auto".to_owned(),
                    size: "2k:1:1".to_owned(),
                    background: "opaque".to_owned(),
                    stream: false,
                    usage: UsageSnapshot {
                        limit_5h: 10,
                        remaining_5h: 9,
                        limit_7d: 50,
                        remaining_7d: 49,
                    },
                },
                images: vec![GeneratedImage {
                    bytes: cursor.into_inner(),
                }],
            },
        )
        .unwrap();
        assert_eq!(response.created, 123);
        assert_eq!(response.data[0].size, "3x2");
        assert!(
            response.data[0]
                .b64_json
                .as_deref()
                .is_some_and(|value| !value.is_empty())
        );
    }

    #[test]
    fn task_ids_are_ark_shaped_but_remain_locally_reversible() {
        let id = Uuid::new_v4();
        let public = public_task_id(id);
        assert!(public.starts_with("cgt-"));
        assert_eq!(parse_task_id(&public).unwrap(), id);
    }
}
