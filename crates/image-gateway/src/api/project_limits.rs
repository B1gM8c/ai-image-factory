use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    project_limits::{ProjectSpendBudgetService, UpdateProjectSpendBudgetRequest},
};

use super::{
    AppState,
    admin::{authorize_project, authorize_project_owner},
    sessions::{authenticate_identity, private_json},
};

#[derive(Debug, Deserialize)]
pub(super) struct ListProjectSpendNotificationsQuery {
    limit: Option<usize>,
}

pub(super) async fn get_project_limits(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    Ok(private_json(
        project_limits(&state)?.get_budget(&project_id).await?,
    ))
}

pub(super) async fn update_project_limits(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<UpdateProjectSpendBudgetRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    let Json(request) = body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid project limits request body",
            None,
            "invalid_request_body",
        )
    })?;
    let view = project_limits(&state)?
        .update_budget(&project_id, principal.user_id, request)
        .await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        project.id = %project_id,
        project.limit_type = ?view.limit_type,
        "project spend budget updated"
    );
    Ok(private_json(view))
}

pub(super) async fn list_project_spend_notifications(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListProjectSpendNotificationsQuery>,
) -> Result<Response, ImageGatewayError> {
    let principal = authenticate_identity(&headers, &state).await?;
    Ok(private_json(
        project_limits(&state)?
            .list_notifications(principal.user_id, query.limit.unwrap_or(20))
            .await?,
    ))
}

pub(super) async fn mark_project_spend_notification_read(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(delivery_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    let principal = authenticate_identity(&headers, &state).await?;
    Ok(private_json(
        project_limits(&state)?
            .mark_notification_read(principal.user_id, delivery_id)
            .await?,
    ))
}

fn project_limits(
    state: &AppState,
) -> Result<&Arc<dyn ProjectSpendBudgetService>, ImageGatewayError> {
    state.project_spend_budget_service.as_ref().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Project spend budgets are not configured")
    })
}
