use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
    response::Response,
};

use crate::{
    ImageGatewayError,
    model_routing::{ModelRoutingStore, PublicModelRoute},
    project_model_policy::{ProjectModelPolicyService, UpdateProjectModelPolicyRequest},
};

use super::{
    AppState, IMAGE_EDIT_ROUTE_OPERATION, IMAGE_GENERATION_ROUTE_OPERATION,
    VIDEO_GENERATION_ROUTE_OPERATION,
    admin::{authorize_project, authorize_project_owner},
    sessions::private_json,
};

pub(super) async fn get_project_model_policy(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    let models = configurable_models(&state, &project_id).await?;
    Ok(private_json(
        project_model_policy(&state)?
            .get_policy(&project_id, models)
            .await?,
    ))
}

pub(super) async fn update_project_model_policy(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<UpdateProjectModelPolicyRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    let Json(request) = body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid project model policy request body",
            None,
            "invalid_request_body",
        )
    })?;
    let models = configurable_models(&state, &project_id).await?;
    let view = project_model_policy(&state)?
        .update_policy(&project_id, principal.user_id, models, request)
        .await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        project.id = %project_id,
        project.model_policy_version = %view.control_version,
        "project model policy updated"
    );
    Ok(private_json(view))
}

async fn configurable_models(
    state: &AppState,
    project_id: &str,
) -> Result<Vec<PublicModelRoute>, ImageGatewayError> {
    let store = model_routing(state)?;
    let (images, edits, videos) = tokio::try_join!(
        store.list_console_models(project_id, IMAGE_GENERATION_ROUTE_OPERATION),
        store.list_console_models(project_id, IMAGE_EDIT_ROUTE_OPERATION),
        store.list_console_models(project_id, VIDEO_GENERATION_ROUTE_OPERATION),
    )?;
    Ok(combine_configurable_models(images, edits, videos))
}

fn combine_configurable_models(
    mut generations: Vec<PublicModelRoute>,
    edits: Vec<PublicModelRoute>,
    videos: Vec<PublicModelRoute>,
) -> Vec<PublicModelRoute> {
    generations.extend(edits);
    generations.extend(videos);
    generations
}

fn model_routing(state: &AppState) -> Result<&Arc<dyn ModelRoutingStore>, ImageGatewayError> {
    state
        .model_routing_store
        .as_ref()
        .ok_or_else(|| ImageGatewayError::service_unavailable("Model routing is not configured"))
}

fn project_model_policy(
    state: &AppState,
) -> Result<&Arc<dyn ProjectModelPolicyService>, ImageGatewayError> {
    state.project_model_policy_service.as_ref().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Project model policy is not configured")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configurable_models_include_image_edit_routes() {
        let edit = model("images.edits", "gpt-image-2");
        let models = combine_configurable_models(
            vec![model("images.generations", "gpt-image-2")],
            vec![edit.clone()],
            vec![model("videos.generations", "grok-imagine-video")],
        );

        assert!(
            models
                .iter()
                .any(|model| { model.operation_id == edit.operation_id && model.id == edit.id })
        );
    }

    fn model(operation_id: &str, public_model_id: &str) -> PublicModelRoute {
        PublicModelRoute {
            id: public_model_id.to_string(),
            provider_model_id: Some(public_model_id.to_string()),
            api_profile: "openai-images-v1".to_string(),
            provider_id: "openai-codex".to_string(),
            operation_id: operation_id.to_string(),
            media_kind: "image".to_string(),
            created_at_ms: 0,
        }
    }
}
