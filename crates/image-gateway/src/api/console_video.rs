use std::{collections::BTreeSet, sync::Arc};

use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use image_api_contracts::{
    ark::{ARK_CONTENT_GENERATION_API_PROFILE, ArkContentGenerationTaskRequest, ArkContentItem},
    dreamina::{DREAMINA_VIDEOS_API_PROFILE, DreaminaVideoGenerationRequest},
    xai::{
        XAI_VIDEOS_API_PROFILE, XaiVideoGenerationRequest, XaiVideoImageUrl, XaiVideoResolution,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    auth::AuthContext,
    model_routing::{PublicModelRoute, ResolvedModelRoute},
    settlement::{StoredVideoArtifact, VideoPendingStage, VideoResultStatus},
};

use super::{
    AppState, RequestId, VIDEO_GENERATION_ROUTE_OPERATION, admin::authorize_project, ark, dreamina,
    filter_project_models, resolve_surface_model, sessions::private_json,
    videos::create_video_with_auth,
};

#[derive(Serialize)]
struct ConsoleVideoModels {
    object: &'static str,
    data: Vec<ConsoleVideoModel>,
}

#[derive(Serialize)]
struct ConsoleVideoModel {
    id: String,
    provider: String,
    api_profile: String,
    media_kind: String,
    operation: String,
    created: i64,
    controls: ConsoleVideoControls,
}

#[derive(Serialize)]
struct ConsoleVideoControls {
    #[serde(skip_serializing_if = "Option::is_none")]
    aspect_ratio: Option<ConsoleChoiceControl>,
    duration: ConsoleNumericChoiceControl,
    resolution: ConsoleChoiceControl,
    first_frame: ConsoleFirstFrameControl,
}

#[derive(Serialize)]
struct ConsoleFirstFrameControl {
    supported: bool,
    required: bool,
}

#[derive(Serialize)]
struct ConsoleChoiceControl {
    default: &'static str,
    options: &'static [&'static str],
}

#[derive(Serialize)]
struct ConsoleNumericChoiceControl {
    default: u8,
    options: &'static [u8],
}

#[derive(Serialize)]
struct ConsoleVideoStatus {
    task_id: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    stage: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ConsoleVideoError>,
}

#[derive(Serialize)]
struct ConsoleVideoError {
    code: &'static str,
    message: &'static str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConsoleVideoGenerationRequest {
    model: String,
    prompt: String,
    #[serde(default)]
    duration: Option<u8>,
    #[serde(default)]
    aspect_ratio: Option<String>,
    #[serde(default)]
    resolution: Option<String>,
    #[serde(default)]
    image: Option<String>,
}

pub(super) async fn video_models(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    let models = state
        .model_routing_store
        .as_ref()
        .ok_or_else(|| ImageGatewayError::service_unavailable("model routing is unavailable"))?
        .list_console_models(&project_id, VIDEO_GENERATION_ROUTE_OPERATION)
        .await?;
    let models =
        prefer_official_dreamina_aliases(filter_project_models(&state, &project_id, models).await?)
            .into_iter()
            .filter(is_console_video_profile)
            .filter_map(console_video_model)
            .collect();
    Ok(private_json(ConsoleVideoModels {
        object: "list",
        data: models,
    }))
}

pub(super) async fn generate_video(
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<ConsoleVideoGenerationRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project(&headers, &state, &project_id, "workspace:write").await?;
    let Json(request) = body.map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })?;
    if request.model.trim().is_empty() {
        return Err(ImageGatewayError::invalid_request(
            "model is required for console generation",
            Some("model".to_owned()),
            "invalid_request",
        ));
    }
    let project_defaults = state
        .api_key_store
        .project_runtime_defaults(&project_id)
        .await?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Project was not found",
                Some("project_id".to_owned()),
                "project_not_found",
            )
        })?;
    let mut auth = AuthContext {
        tenant_id: project_defaults.tenant_id,
        project_id,
        project_service_tier: project_defaults.service_tier,
        service_account_id: None,
        api_key_id: None,
        credential_authz_version: None,
        credential_owner_user_id: None,
        actor_user_id: Some(principal.user_id),
        actor_session_id: Some(principal.session_id),
        actor_authz_version: Some(principal.authz_version),
        api_key_permission_mode: crate::auth::ApiKeyPermissionMode::All,
        api_key_permissions: crate::auth::ApiKeyPermissions::default(),
        route: None,
        is_admin: principal.roles.iter().any(|role| role == "platform_owner")
            && principal.scopes.iter().any(|scope| scope == "admin:*"),
    };
    crate::request_observability::capture_auth(&auth);
    let resolved = resolve_surface_model(
        &state,
        &mut auth,
        VIDEO_GENERATION_ROUTE_OPERATION,
        &[
            XAI_VIDEOS_API_PROFILE,
            DREAMINA_VIDEOS_API_PROFILE,
            ARK_CONTENT_GENERATION_API_PROFILE,
        ],
        &request.model,
    )
    .await?
    .ok_or_else(|| ImageGatewayError::model_not_found(&request.model))?;
    let task_id = match console_video_request(request, &resolved)? {
        ConsoleVideoDispatchRequest::Xai(request) => {
            create_video_with_auth(&state, auth, &headers, request_id.0, request)
                .await?
                .request_id
        }
        ConsoleVideoDispatchRequest::Dreamina(request) => {
            dreamina::create_video_with_resolved_auth(
                &state,
                auth,
                &headers,
                request_id.0,
                request,
                resolved,
            )
            .await?
            .id
        }
        ConsoleVideoDispatchRequest::Ark(request) => {
            let response = ark::create_content_task_with_resolved_auth(
                &state,
                auth,
                &headers,
                request_id.0,
                request,
                resolved,
            )
            .await?;
            response
                .id
                .strip_prefix("cgt-")
                .unwrap_or(&response.id)
                .to_owned()
        }
    };
    Ok(private_json(json!({
        "task_id": task_id,
        "status": "pending",
        "stage": "queued"
    })))
}

pub(super) async fn get_console_video(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, task_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    let tenant_id = project_tenant(&state, &project_id).await?;
    let actor_user_id = task_actor_scope(&principal);
    let job_id = parse_console_uuid(&task_id, "task_id")?;
    let status = state
        .settlement_store
        .project_video_status(&tenant_id, &project_id, actor_user_id, job_id)
        .await?
        .ok_or_else(|| console_video_not_found("task_id"))?;
    Ok(private_json(console_video_status(
        &project_id,
        &task_id,
        status,
    )))
}

pub(super) async fn get_console_video_content(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, file_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    let tenant_id = project_tenant(&state, &project_id).await?;
    let actor_user_id = task_actor_scope(&principal);
    let artifact_id = parse_console_uuid(&file_id, "file_id")?;
    let StoredVideoArtifact { media_type, bytes } = state
        .settlement_store
        .load_project_video_artifact(&tenant_id, &project_id, actor_user_id, artifact_id)
        .await?
        .ok_or_else(|| console_video_not_found("file_id"))?;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, media_type)
        .header(header::CONTENT_LENGTH, bytes.len().to_string())
        .header(header::CACHE_CONTROL, "private, no-store")
        .body(Body::from(bytes))
        .map_err(|_| ImageGatewayError::internal("failed to build video response"))
}

fn is_console_video_profile(model: &PublicModelRoute) -> bool {
    matches!(
        model.api_profile.as_str(),
        XAI_VIDEOS_API_PROFILE | DREAMINA_VIDEOS_API_PROFILE | ARK_CONTENT_GENERATION_API_PROFILE
    )
}

fn prefer_official_dreamina_aliases(models: Vec<PublicModelRoute>) -> Vec<PublicModelRoute> {
    let ark_routes = models
        .iter()
        .filter(|model| model.api_profile == ARK_CONTENT_GENERATION_API_PROFILE)
        .filter_map(|model| {
            model.provider_model_id.as_ref().map(|provider_model_id| {
                (
                    model.provider_id.clone(),
                    model.operation_id.clone(),
                    provider_model_id.clone(),
                )
            })
        })
        .collect::<BTreeSet<_>>();
    models
        .into_iter()
        .filter(|model| {
            model.api_profile != DREAMINA_VIDEOS_API_PROFILE
                || !model
                    .provider_model_id
                    .as_ref()
                    .is_some_and(|provider_model_id| {
                        ark_routes.contains(&(
                            model.provider_id.clone(),
                            model.operation_id.clone(),
                            provider_model_id.clone(),
                        ))
                    })
        })
        .collect()
}

fn console_video_model(model: PublicModelRoute) -> Option<ConsoleVideoModel> {
    let controls = console_video_controls(&model.api_profile, model.provider_model_id.as_deref())?;
    Some(ConsoleVideoModel {
        id: model.id,
        provider: model.provider_id,
        api_profile: model.api_profile,
        media_kind: model.media_kind,
        operation: model.operation_id,
        created: model.created_at_ms.div_euclid(1_000),
        controls,
    })
}

fn console_video_controls(
    api_profile: &str,
    provider_model_id: Option<&str>,
) -> Option<ConsoleVideoControls> {
    const DREAMINA_RATIOS: &[&str] = &["1:1", "3:4", "16:9", "4:3", "9:16", "21:9"];
    const DREAMINA_DURATIONS: &[u8] = &[4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    if api_profile == XAI_VIDEOS_API_PROFILE {
        if provider_model_id != Some("grok-imagine-video-1.5-preview") {
            return None;
        }
        return Some(ConsoleVideoControls {
            aspect_ratio: None,
            duration: ConsoleNumericChoiceControl {
                default: 6,
                options: &[6, 10],
            },
            resolution: ConsoleChoiceControl {
                default: "480p",
                options: &["480p", "720p"],
            },
            first_frame: ConsoleFirstFrameControl {
                supported: true,
                required: true,
            },
        });
    }
    if !matches!(
        api_profile,
        DREAMINA_VIDEOS_API_PROFILE | ARK_CONTENT_GENERATION_API_PROFILE
    ) {
        return None;
    }
    let provider_model_id = provider_model_id?;
    let resolutions = match (api_profile, provider_model_id) {
        (DREAMINA_VIDEOS_API_PROFILE, "seedance2.0_vip") => &["720p", "1080p", "4k"][..],
        (
            DREAMINA_VIDEOS_API_PROFILE,
            "seedance2.0" | "seedance2.0fast" | "seedance2.0fast_vip" | "seedance2.0mini",
        )
        | (
            ARK_CONTENT_GENERATION_API_PROFILE,
            "seedance2.0" | "seedance2.0fast" | "seedance2.0mini",
        ) => &["720p"][..],
        _ => return None,
    };
    Some(ConsoleVideoControls {
        aspect_ratio: Some(ConsoleChoiceControl {
            default: "16:9",
            options: DREAMINA_RATIOS,
        }),
        duration: ConsoleNumericChoiceControl {
            default: 5,
            options: DREAMINA_DURATIONS,
        },
        resolution: ConsoleChoiceControl {
            default: "720p",
            options: resolutions,
        },
        first_frame: ConsoleFirstFrameControl {
            supported: false,
            required: false,
        },
    })
}

enum ConsoleVideoDispatchRequest {
    Xai(XaiVideoGenerationRequest),
    Dreamina(DreaminaVideoGenerationRequest),
    Ark(ArkContentGenerationTaskRequest),
}

fn console_video_request(
    request: ConsoleVideoGenerationRequest,
    resolved: &ResolvedModelRoute,
) -> Result<ConsoleVideoDispatchRequest, ImageGatewayError> {
    let controls = console_video_controls(&resolved.api_profile, Some(&resolved.provider_model_id))
        .ok_or_else(|| {
            ImageGatewayError::unsupported(
                "model",
                "model is not supported by the console video workflow",
            )
        })?;
    let duration = request.duration.unwrap_or(controls.duration.default);
    if !controls.duration.options.contains(&duration) {
        return Err(ImageGatewayError::invalid_request(
            "duration is not supported by this model",
            Some("duration".to_owned()),
            "invalid_value",
        ));
    }
    let aspect_ratio = match controls.aspect_ratio.as_ref() {
        Some(control) => {
            let value = request
                .aspect_ratio
                .unwrap_or_else(|| control.default.to_owned());
            if !control.options.contains(&value.as_str()) {
                return Err(ImageGatewayError::invalid_request(
                    "aspect_ratio is not supported by this model",
                    Some("aspect_ratio".to_owned()),
                    "invalid_value",
                ));
            }
            Some(value)
        }
        None if request.aspect_ratio.is_some() => {
            return Err(ImageGatewayError::invalid_request(
                "aspect_ratio is not supported by this model",
                Some("aspect_ratio".to_owned()),
                "invalid_value",
            ));
        }
        None => None,
    };
    let resolution = request
        .resolution
        .unwrap_or_else(|| controls.resolution.default.to_owned());
    if !controls.resolution.options.contains(&resolution.as_str()) {
        return Err(ImageGatewayError::invalid_request(
            "resolution is not supported by this model",
            Some("resolution".to_owned()),
            "invalid_value",
        ));
    }
    let prompt = request.prompt.trim().to_owned();
    if prompt.is_empty() {
        return Err(ImageGatewayError::invalid_request(
            "prompt is required for console generation",
            Some("prompt".to_owned()),
            "invalid_request",
        ));
    }
    if request.image.is_none() && controls.first_frame.required {
        return Err(ImageGatewayError::invalid_request(
            "image is required by this model",
            Some("image".to_owned()),
            "missing_required_parameter",
        ));
    }
    if request.image.is_some() && !controls.first_frame.supported {
        return Err(ImageGatewayError::unsupported(
            "image",
            "image is not supported by this model",
        ));
    }
    match resolved.api_profile.as_str() {
        XAI_VIDEOS_API_PROFILE => Ok(ConsoleVideoDispatchRequest::Xai(
            XaiVideoGenerationRequest {
                aspect_ratio: None,
                duration: Some(duration),
                image: request.image.map(|url| XaiVideoImageUrl {
                    file_id: None,
                    url: Some(url),
                }),
                model: Some(resolved.provider_model_id.clone()),
                output: None,
                prompt: Some(prompt),
                reference_images: Vec::new(),
                resolution: Some(match resolution.as_str() {
                    "480p" => XaiVideoResolution::P480,
                    "720p" => XaiVideoResolution::P720,
                    _ => unreachable!("validated console video resolution"),
                }),
                storage_options: None,
                user: None,
            },
        )),
        DREAMINA_VIDEOS_API_PROFILE => Ok(ConsoleVideoDispatchRequest::Dreamina(
            DreaminaVideoGenerationRequest {
                prompt,
                model_version: Some(resolved.provider_model_id.clone()),
                ratio: aspect_ratio,
                duration: Some(duration),
                video_resolution: resolution,
            },
        )),
        ARK_CONTENT_GENERATION_API_PROFILE => Ok(ConsoleVideoDispatchRequest::Ark(
            ArkContentGenerationTaskRequest {
                model: request.model,
                content: vec![ArkContentItem::Text { text: prompt }],
                safety_identifier: None,
                callback_url: None,
                return_last_frame: None,
                service_tier: None,
                execution_expires_after: None,
                priority: None,
                generate_audio: None,
                draft: None,
                camera_fixed: None,
                watermark: None,
                seed: None,
                resolution: Some(resolution),
                ratio: aspect_ratio,
                duration: Some(duration),
                frames: None,
                tools: None,
            },
        )),
        _ => Err(ImageGatewayError::service_unavailable(
            "model route does not match the console videos surface",
        )),
    }
}

async fn project_tenant(
    state: &Arc<AppState>,
    project_id: &str,
) -> Result<String, ImageGatewayError> {
    state
        .api_key_store
        .project_tenant(project_id)
        .await?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Project was not found",
                Some("project_id".to_owned()),
                "project_not_found",
            )
        })
}

fn parse_console_uuid(value: &str, param: &str) -> Result<Uuid, ImageGatewayError> {
    Uuid::parse_str(value).map_err(|_| {
        ImageGatewayError::invalid_request(
            format!("{param} is invalid"),
            Some(param.to_owned()),
            "invalid_value",
        )
    })
}

fn console_video_not_found(param: &str) -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Video task was not found",
        Some(param.to_owned()),
        "video_not_found",
    )
}

fn console_video_status(
    project_id: &str,
    task_id: &str,
    status: VideoResultStatus,
) -> ConsoleVideoStatus {
    match status {
        VideoResultStatus::Pending {
            model,
            duration,
            stage,
        } => ConsoleVideoStatus {
            task_id: task_id.to_owned(),
            status: "pending",
            stage: Some(console_video_pending_stage(stage)),
            model: Some(model),
            duration: Some(duration),
            progress: None,
            content_url: None,
            error: None,
        },
        VideoResultStatus::Uncertain { model, duration } => ConsoleVideoStatus {
            task_id: task_id.to_owned(),
            status: "uncertain",
            stage: None,
            model: Some(model),
            duration: Some(duration),
            progress: None,
            content_url: None,
            error: None,
        },
        VideoResultStatus::Succeeded {
            model,
            duration,
            artifact_id,
        } => ConsoleVideoStatus {
            task_id: task_id.to_owned(),
            status: "done",
            stage: None,
            model: Some(model),
            duration: Some(duration),
            progress: Some(100),
            content_url: Some(format!(
                "/v1/console/projects/{project_id}/videos/files/{artifact_id}/content"
            )),
            error: None,
        },
        VideoResultStatus::Failed {
            model,
            duration,
            error_code,
        } => {
            let error_code = error_code.as_deref();
            ConsoleVideoStatus {
                task_id: task_id.to_owned(),
                status: "failed",
                stage: None,
                model: Some(model),
                duration: Some(duration),
                progress: None,
                content_url: None,
                error: Some(ConsoleVideoError {
                    code: console_video_error_code(error_code),
                    message: console_video_error_message(error_code),
                }),
            }
        }
    }
}

fn console_video_pending_stage(stage: VideoPendingStage) -> &'static str {
    match stage {
        VideoPendingStage::Queued => "queued",
        VideoPendingStage::Dispatching => "dispatching",
        VideoPendingStage::Processing => "processing",
    }
}

fn console_video_error_code(error_code: Option<&str>) -> &'static str {
    match error_code {
        Some("permission_denied" | "authentication_failed") => "permission_denied",
        Some("invalid_argument" | "executor_command_rejected") => "invalid_argument",
        Some("failed_precondition") => "failed_precondition",
        Some("grok_video_output_upload_url_required") => "grok_video_output_upload_url_required",
        Some("service_unavailable" | "timeout" | "grok_cli_failed") => "service_unavailable",
        _ => "internal_error",
    }
}

fn console_video_error_message(error_code: Option<&str>) -> &'static str {
    match error_code {
        Some("grok_video_output_upload_url_required") => {
            "Grok Zero Data Retention accounts require a video upload target"
        }
        _ => "Video generation failed",
    }
}

fn task_actor_scope(principal: &factory_identity::AuthenticatedPrincipal) -> Option<Uuid> {
    let platform_admin = principal.roles.iter().any(|role| role == "platform_owner")
        && principal.scopes.iter().any(|scope| scope == "admin:*");
    (!platform_admin).then_some(principal.user_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_video_request_projects_only_supported_grok_controls() {
        let request = ConsoleVideoGenerationRequest {
            model: "public-video".to_owned(),
            prompt: "slow camera push".to_owned(),
            duration: Some(10),
            aspect_ratio: None,
            resolution: Some("720p".to_owned()),
            image: Some("data:image/png;base64,AA==".to_owned()),
        };
        let ConsoleVideoDispatchRequest::Xai(projected) =
            console_video_request(request, &resolved_route(XAI_VIDEOS_API_PROFILE)).unwrap()
        else {
            panic!("xAI request was dispatched to the wrong adapter");
        };
        assert_eq!(
            projected.model.as_deref(),
            Some("grok-imagine-video-1.5-preview")
        );
        assert_eq!(projected.duration, Some(10));
        assert_eq!(projected.aspect_ratio, None);
        assert_eq!(projected.resolution, Some(XaiVideoResolution::P720));
        assert_eq!(
            projected
                .image
                .as_ref()
                .and_then(|image| image.url.as_deref()),
            Some("data:image/png;base64,AA==")
        );
    }

    #[test]
    fn console_video_request_rejects_controls_the_cli_cannot_execute() {
        let request = ConsoleVideoGenerationRequest {
            model: "public-video".to_owned(),
            prompt: "slow camera push".to_owned(),
            duration: Some(8),
            aspect_ratio: Some("4:3".to_owned()),
            resolution: Some("1080p".to_owned()),
            image: Some("data:image/png;base64,AA==".to_owned()),
        };
        assert!(console_video_request(request, &resolved_route(XAI_VIDEOS_API_PROFILE)).is_err());
    }

    #[test]
    fn dreamina_console_video_projects_dynamic_cli_controls() {
        let request = ConsoleVideoGenerationRequest {
            model: "public-seedance".to_owned(),
            prompt: "slow camera push".to_owned(),
            duration: Some(15),
            aspect_ratio: Some("21:9".to_owned()),
            resolution: Some("720p".to_owned()),
            image: None,
        };
        let ConsoleVideoDispatchRequest::Dreamina(projected) =
            console_video_request(request, &resolved_route(DREAMINA_VIDEOS_API_PROFILE)).unwrap()
        else {
            panic!("Dreamina request was dispatched to the wrong adapter");
        };
        assert_eq!(projected.model_version.as_deref(), Some("seedance2.0fast"));
        assert_eq!(projected.duration, Some(15));
        assert_eq!(projected.ratio.as_deref(), Some("21:9"));
        assert_eq!(projected.video_resolution, "720p");
    }

    #[test]
    fn ark_console_video_preserves_the_official_content_shape() {
        let request = ConsoleVideoGenerationRequest {
            model: "doubao-seedance-2-0-fast-260128".to_owned(),
            prompt: "slow camera push".to_owned(),
            duration: Some(5),
            aspect_ratio: Some("16:9".to_owned()),
            resolution: Some("720p".to_owned()),
            image: None,
        };
        let ConsoleVideoDispatchRequest::Ark(projected) =
            console_video_request(request, &resolved_route(ARK_CONTENT_GENERATION_API_PROFILE))
                .unwrap()
        else {
            panic!("Ark request was dispatched to the wrong adapter");
        };
        assert_eq!(projected.model, "doubao-seedance-2-0-fast-260128");
        assert_eq!(projected.duration, Some(5));
        assert_eq!(projected.ratio.as_deref(), Some("16:9"));
        assert!(matches!(
            projected.content.as_slice(),
            [ArkContentItem::Text { text }] if text == "slow camera push"
        ));
    }

    #[test]
    fn official_ark_alias_hides_the_duplicate_native_dreamina_alias() {
        let native = public_model(
            "seedance2.0fast",
            DREAMINA_VIDEOS_API_PROFILE,
            "seedance2.0fast",
        );
        let ark = public_model(
            "doubao-seedance-2-0-fast-260128",
            ARK_CONTENT_GENERATION_API_PROFILE,
            "seedance2.0fast",
        );
        let preferred = prefer_official_dreamina_aliases(vec![native, ark]);
        assert_eq!(preferred.len(), 1);
        assert_eq!(preferred[0].api_profile, ARK_CONTENT_GENERATION_API_PROFILE);
    }

    #[test]
    fn completed_console_video_uses_the_project_scoped_content_route() {
        let artifact_id = Uuid::new_v4();
        let status = console_video_status(
            "proj_one",
            "task-one",
            VideoResultStatus::Succeeded {
                model: "grok-imagine-video".to_owned(),
                duration: 6,
                artifact_id,
            },
        );
        assert_eq!(status.status, "done");
        assert_eq!(
            status.content_url.as_deref(),
            Some(
                format!("/v1/console/projects/proj_one/videos/files/{artifact_id}/content")
                    .as_str()
            )
        );
    }

    #[test]
    fn zdr_video_failures_keep_an_actionable_console_error() {
        let status = console_video_status(
            "proj_one",
            "task-one",
            VideoResultStatus::Failed {
                model: "grok-imagine-video".to_owned(),
                duration: 6,
                error_code: Some("grok_video_output_upload_url_required".to_owned()),
            },
        );
        assert_eq!(status.status, "failed");
        let error = status.error.unwrap();
        assert_eq!(error.code, "grok_video_output_upload_url_required");
        assert_eq!(
            error.message,
            "Grok Zero Data Retention accounts require a video upload target"
        );
    }

    fn resolved_route(api_profile: &str) -> ResolvedModelRoute {
        let (provider_id, provider_model_id) = match api_profile {
            XAI_VIDEOS_API_PROFILE => ("grok-cli", "grok-imagine-video-1.5-preview"),
            DREAMINA_VIDEOS_API_PROFILE | ARK_CONTENT_GENERATION_API_PROFILE => {
                ("dreamina-cli", "seedance2.0fast")
            }
            _ => panic!("unsupported test profile"),
        };
        ResolvedModelRoute {
            public_model_id: "public-video".to_owned(),
            api_profile: api_profile.to_owned(),
            provider_id: provider_id.to_owned(),
            operation_id: VIDEO_GENERATION_ROUTE_OPERATION.to_owned(),
            command_schema: "xai.videos.generations.v1".to_owned(),
            provider_model_id: provider_model_id.to_owned(),
            execution_model_id: provider_model_id.to_owned(),
            media_kind: "video".to_owned(),
            route_id: Uuid::new_v4(),
            route_revision: 1,
        }
    }

    fn public_model(
        public_model_id: &str,
        api_profile: &str,
        provider_model_id: &str,
    ) -> PublicModelRoute {
        PublicModelRoute {
            id: public_model_id.to_owned(),
            provider_model_id: Some(provider_model_id.to_owned()),
            api_profile: api_profile.to_owned(),
            provider_id: image_provider_dreamina_cli::PROVIDER_ID.to_owned(),
            operation_id: VIDEO_GENERATION_ROUTE_OPERATION.to_owned(),
            media_kind: "video".to_owned(),
            created_at_ms: 0,
        }
    }
}
