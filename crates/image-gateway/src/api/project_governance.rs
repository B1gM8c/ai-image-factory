use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
    response::Response,
};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    project_governance::{
        AddProjectMemberRequest, ProjectGovernanceService, UpdateProjectMemberRequest,
    },
};

use super::{
    AppState,
    admin::{authorize_project, authorize_project_owner},
    sessions::private_json,
};

pub(super) async fn list_project_members(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    Ok(private_json(
        project_governance(&state)?
            .list_members(&project_id)
            .await?,
    ))
}

pub(super) async fn add_project_member(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<AddProjectMemberRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    let Json(request) = body.map_err(|_| invalid_member_body())?;
    let member = project_governance(&state)?
        .add_member(&project_id, principal.user_id, request)
        .await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        project.id = %project_id,
        target.user_id = %member.user_id,
        target.role = %member.role,
        "project member added"
    );
    Ok(private_json(member))
}

pub(super) async fn update_project_member(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, target_user_id)): Path<(String, Uuid)>,
    body: Result<Json<UpdateProjectMemberRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    let Json(request) = body.map_err(|_| invalid_member_body())?;
    let member = project_governance(&state)?
        .update_member(&project_id, target_user_id, principal.user_id, request)
        .await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        project.id = %project_id,
        target.user_id = %target_user_id,
        target.role = %member.role,
        "project member role updated"
    );
    Ok(private_json(member))
}

pub(super) async fn remove_project_member(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, target_user_id)): Path<(String, Uuid)>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    let member = project_governance(&state)?
        .remove_member(&project_id, target_user_id, principal.user_id)
        .await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        project.id = %project_id,
        target.user_id = %target_user_id,
        "project member removed"
    );
    Ok(private_json(member))
}

fn project_governance(
    state: &AppState,
) -> Result<&Arc<dyn ProjectGovernanceService>, ImageGatewayError> {
    state.project_governance_service.as_ref().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Project governance is not configured")
    })
}

fn invalid_member_body() -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        "Invalid project member request body",
        None,
        "invalid_project_member_request",
    )
}
