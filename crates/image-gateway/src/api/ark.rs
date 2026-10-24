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
    ArkContentItem, ArkGeneratedContent, ArkImageData, ArkImageGenerationRequest,
    ArkImageGenerationResponse, ArkImageUsage,
};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    admission::{
        AdmissionContract, ArkAdmissionError, ArkImageAdmissionPlan, ArkVideoAdmissionPlan,
        CustomerPricingIntent, DreaminaWorkflowIntent, GENERATION_OPERATION,
        VIDEO_GENERATION_OPERATION, ark_image_workflow, ark_video_workflow,
    },
    artifacts::StoredGenerationResult,
    auth::{ApiKeyCapability, AuthContext},
    model_routing::ResolvedModelRoute,
    settlement::{VideoRequestMetadata, VideoResultStatus},
};

use super::{
    AppState, RequestId, authenticate_image_request,
    dreamina::{
        customer_pricing_request_hash, dreamina_admission_error, idempotency_digest, invalid_json,
        require_external_execution, submit_image_generation, submit_video_generation,
        submit_workflow,
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
    if request.image.is_some() {
        let workflow = ark_image_workflow(request)
            .map_err(|error| ark_admission_error(error, &requested_model))?;
        let intent = DreaminaWorkflowIntent::image(workflow).map_err(dreamina_admission_error)?;
        let execution_model_id = execution_model_id.ok_or_else(|| {
            ImageGatewayError::service_unavailable(
                "Dreamina workflow requires an enabled model route",
            )
        })?;
        let job_id = submit_workflow(
            state,
            &auth,
            headers,
            request_id,
            ARK_IMAGES_API_PROFILE,
            public_model_id,
            execution_model_id,
            intent,
        )
        .await?;
        let result = super::images::wait_for_generation_result(state, job_id).await?;
        return ark_image_response(requested_model, result);
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
    if request
        .content
        .iter()
        .any(|item| !matches!(item, ArkContentItem::Text { .. }))
    {
        let workflow = ark_video_workflow(request)
            .map_err(|error| ark_admission_error(error, &requested_model))?;
        let intent = DreaminaWorkflowIntent::video(workflow).map_err(dreamina_admission_error)?;
        let execution_model_id = execution_model_id.ok_or_else(|| {
            ImageGatewayError::service_unavailable(
                "Dreamina workflow requires an enabled model route",
            )
        })?;
        let job_id = submit_workflow(
            state,
            &auth,
            headers,
            request_id,
            ARK_CONTENT_GENERATION_API_PROFILE,
            public_model_id,
            execution_model_id,
            intent,
        )
        .await?;
        return Ok(ArkContentGenerationTaskId {
            id: public_task_id(job_id),
            safety_identifier: None,
        });
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
    let (status, metadata) = state
        .settlement_store
        .project_video_status_with_metadata(
            &auth.tenant_id,
            &auth.project_id,
            auth.actor_user_id,
            job_id,
        )
        .await?
        .ok_or_else(task_not_found)?;
    Ok(Json(ark_video_task(job_id, status, metadata)))
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

fn ark_video_task(
    id: Uuid,
    status: VideoResultStatus,
    metadata: VideoRequestMetadata,
) -> ArkContentGenerationTask {
    let public_id = public_task_id(id);
    let mut task = match status {
        VideoResultStatus::Pending {
            model, duration_ms, ..
        } => ArkContentGenerationTask {
            id: public_id,
            model: public_video_model(&model).to_owned(),
            status: "running".to_owned(),
            error: None,
            content: None,
            usage: None,
            duration: Some(super::dreamina::duration_seconds(duration_ms)),
            resolution: None,
            ratio: None,
        },
        VideoResultStatus::Uncertain { model, duration_ms } => ArkContentGenerationTask {
            id: public_id,
            model: public_video_model(&model).to_owned(),
            status: "uncertain".to_owned(),
            error: Some(ArkContentGenerationError {
                message: super::dreamina::VIDEO_UNCERTAIN_MESSAGE.to_owned(),
                code: "generation_uncertain".to_owned(),
            }),
            content: None,
            usage: None,
            duration: Some(super::dreamina::duration_seconds(duration_ms)),
            resolution: None,
            ratio: None,
        },
        VideoResultStatus::Succeeded {
            model,
            duration_ms,
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
            duration: Some(super::dreamina::duration_seconds(duration_ms)),
            resolution: None,
            ratio: None,
        },
        VideoResultStatus::SucceededRemote {
            model,
            duration_ms,
            result,
        } => ArkContentGenerationTask {
            id: public_id,
            model: public_video_model(&model).to_owned(),
            status: "succeeded".to_owned(),
            error: None,
            content: Some(ArkGeneratedContent {
                video_url: Some(result.video_url().to_owned()),
                last_frame_url: None,
                file_url: None,
            }),
            usage: None,
            duration: Some(super::dreamina::duration_seconds(duration_ms)),
            resolution: None,
            ratio: None,
        },
        VideoResultStatus::Failed {
            model,
            duration_ms,
            error_code,
        } => {
            let message = match error_code.as_deref() {
                Some("dreamina_compliance_confirmation_required") => {
                    super::dreamina::DREAMINA_COMPLIANCE_CONFIRMATION_MESSAGE
                }
                _ => "Video generation failed",
            };
            ArkContentGenerationTask {
                id: public_id,
                model: public_video_model(&model).to_owned(),
                status: "failed".to_owned(),
                error: Some(ArkContentGenerationError {
                    message: message.to_owned(),
                    code: error_code.unwrap_or_else(|| "InternalError".to_owned()),
                }),
                content: None,
                usage: None,
                duration: Some(super::dreamina::duration_seconds(duration_ms)),
                resolution: None,
                ratio: None,
            }
        }
    };
    if let Some(model) = metadata.provider_model {
        task.model = public_video_model(&model).to_owned();
    }
    task.resolution = metadata.resolution;
    task.ratio = metadata.ratio;
    task
}

fn public_video_model(provider_model: &str) -> &str {
    match provider_model {
        "seedance2.0" => "doubao-seedance-2-0-260128",
        "seedance2.0fast" => "doubao-seedance-2-0-fast-260128",
        "seedance2.0mini" => "doubao-seedance-2-0-mini-260128",
        "seedance2.5" => "doubao-seedance-2-5-260628",
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
        "5.0Pro" => Some("doubao-seedream-5-0-pro-260628"),
        _ => None,
    }
}

fn ark_video_model_for_provider(provider_model: &str) -> Option<&'static str> {
    match provider_model {
        "seedance2.0" => Some("doubao-seedance-2-0-260128"),
        "seedance2.0fast" => Some("doubao-seedance-2-0-fast-260128"),
        "seedance2.0mini" => Some("doubao-seedance-2-0-mini-260128"),
        "seedance2.5" => Some("doubao-seedance-2-5-260628"),
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
    fn resolved_seedream_routes_keep_their_canonical_cli_model() {
        for model in ["5.0", "5.0Pro"] {
            let normalized = ark_image_model_for_provider(model).unwrap();
            let request = serde_json::from_value(serde_json::json!({
                "model": normalized,
                "prompt": "a city",
                "size": "2K"
            }))
            .unwrap();
            let plan = ArkImageAdmissionPlan::new(request).unwrap();
            assert_eq!(plan.provider_model_id(), model);
        }
        assert_eq!(
            ark_image_model_for_provider("5.0Pro"),
            Some("doubao-seedream-5-0-pro-260628")
        );
        assert_eq!(ark_image_model_for_provider("4.7"), None);
    }

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

    #[test]
    fn ark_video_query_exposes_uncertain_without_claiming_failure_or_success() {
        let id = Uuid::new_v4();
        let task = ark_video_task(
            id,
            VideoResultStatus::Uncertain {
                model: "seedance2.5".to_owned(),
                duration_ms: 5000,
            },
            VideoRequestMetadata::default(),
        );
        assert_eq!(task.id, public_task_id(id));
        assert_eq!(task.status, "uncertain");
        assert_eq!(task.model, "doubao-seedance-2-5-260628");
        assert!(task.content.is_none());
        assert!(task.usage.is_none());
        let error = task.error.unwrap();
        assert_eq!(error.code, "generation_uncertain");
        assert_eq!(
            error.message,
            super::super::dreamina::VIDEO_UNCERTAIN_MESSAGE
        );
    }

    #[test]
    fn ark_video_query_explains_compliance_rejection_and_preserves_other_errors() {
        for (code, expected_code, expected_message) in [
            (
                Some("dreamina_compliance_confirmation_required"),
                "dreamina_compliance_confirmation_required",
                super::super::dreamina::DREAMINA_COMPLIANCE_CONFIRMATION_MESSAGE,
            ),
            (
                Some("provider_submit_cli_failed"),
                "provider_submit_cli_failed",
                "Video generation failed",
            ),
            (None, "InternalError", "Video generation failed"),
        ] {
            let task = ark_video_task(
                Uuid::new_v4(),
                VideoResultStatus::Failed {
                    model: "seedance2.5".to_owned(),
                    duration_ms: 5000,
                    error_code: code.map(str::to_owned),
                },
                VideoRequestMetadata::default(),
            );
            assert_eq!(task.status, "failed");
            assert!(task.content.is_none());
            assert!(task.usage.is_none());
            let error = task.error.unwrap();
            assert_eq!(error.code, expected_code);
            assert_eq!(error.message, expected_message);
        }
    }

    #[test]
    fn ark_video_query_preserves_frozen_seedance_25_specs_in_every_state() {
        let artifact_id = Uuid::new_v4();
        let statuses = [
            VideoResultStatus::Pending {
                model: "execution-model-id".to_owned(),
                duration_ms: 5000,
                stage: crate::settlement::VideoPendingStage::Queued,
            },
            VideoResultStatus::Uncertain {
                model: "execution-model-id".to_owned(),
                duration_ms: 5000,
            },
            VideoResultStatus::Succeeded {
                model: "execution-model-id".to_owned(),
                duration_ms: 5000,
                artifact_id,
            },
            VideoResultStatus::Failed {
                model: "execution-model-id".to_owned(),
                duration_ms: 5000,
                error_code: Some("generation_failed".to_owned()),
            },
        ];
        for status in statuses {
            let response = ark_video_task(
                Uuid::new_v4(),
                status,
                VideoRequestMetadata {
                    provider_model: Some("seedance2.5".to_owned()),
                    resolution: Some("480p".to_owned()),
                    ratio: Some("16:9".to_owned()),
                },
            );
            assert_eq!(response.model, "doubao-seedance-2-5-260628");
            assert_eq!(response.resolution.as_deref(), Some("480p"));
            assert_eq!(response.ratio.as_deref(), Some("16:9"));
            assert_eq!(response.duration, Some(5.into()));
            if response.status == "succeeded" {
                assert_eq!(
                    response.content.unwrap().video_url.as_deref(),
                    Some(format!("/api/v3/files/{artifact_id}/content").as_str())
                );
            }
        }
    }

    #[test]
    fn ark_video_query_does_not_invent_missing_legacy_specifications() {
        let response = ark_video_task(
            Uuid::new_v4(),
            VideoResultStatus::Pending {
                model: "seedance2.0fast".to_owned(),
                duration_ms: 5000,
                stage: crate::settlement::VideoPendingStage::Processing,
            },
            VideoRequestMetadata::default(),
        );
        assert_eq!(response.model, "doubao-seedance-2-0-fast-260128");
        assert!(response.resolution.is_none());
        assert!(response.ratio.is_none());
        assert_eq!(
            ark_video_model_for_provider("seedance2.5"),
            Some("doubao-seedance-2-5-260628")
        );
    }

    #[test]
    fn ark_duration_exposes_frozen_milliseconds_as_exact_decimal_seconds() {
        let task = ark_video_task(
            Uuid::new_v4(),
            VideoResultStatus::Pending {
                model: "multiframe2video".to_owned(),
                duration_ms: 2501,
                stage: crate::settlement::VideoPendingStage::Processing,
            },
            VideoRequestMetadata::default(),
        );
        assert_eq!(
            serde_json::to_value(task).unwrap()["duration"].to_string(),
            "2.501"
        );
    }
}
