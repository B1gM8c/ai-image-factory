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
    webhooks::{CreateProjectWebhookRequest, ProjectWebhookService, UpdateProjectWebhookRequest},
};

use super::{
    AppState,
    admin::{authorize_project, authorize_project_owner},
    sessions::private_json,
};

#[derive(Debug, Deserialize)]
pub(super) struct ListWebhooksQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ListWebhookDeliveriesQuery {
    after: Option<Uuid>,
    limit: Option<usize>,
}

pub(super) async fn list_project_webhooks(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    Query(query): Query<ListWebhooksQuery>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    Ok(private_json(
        webhooks(&state)?
            .list_endpoints(
                &project_id,
                query.after.as_deref(),
                query.limit.unwrap_or(20),
            )
            .await?,
    ))
}

pub(super) async fn create_project_webhook(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<CreateProjectWebhookRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    let Json(request) = parse_body(body)?;
    let created = webhooks(&state)?
        .create_endpoint(&project_id, principal.user_id, request)
        .await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        project.id = %project_id,
        webhook.id = %created.endpoint.id,
        "project webhook created"
    );
    Ok(private_json(created))
}

pub(super) async fn update_project_webhook(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, endpoint_id)): Path<(String, String)>,
    body: Result<Json<UpdateProjectWebhookRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    let Json(request) = parse_body(body)?;
    let updated = webhooks(&state)?
        .update_endpoint(&project_id, &endpoint_id, principal.user_id, request)
        .await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        project.id = %project_id,
        webhook.id = %endpoint_id,
        "project webhook updated"
    );
    Ok(private_json(updated))
}

pub(super) async fn delete_project_webhook(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, endpoint_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    Ok(private_json(
        webhooks(&state)?
            .delete_endpoint(&project_id, &endpoint_id, principal.user_id)
            .await?,
    ))
}

pub(super) async fn rotate_project_webhook_secret(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, endpoint_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    Ok(private_json(
        webhooks(&state)?
            .rotate_secret(&project_id, &endpoint_id, principal.user_id)
            .await?,
    ))
}

pub(super) async fn test_project_webhook(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, endpoint_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    Ok(private_json(
        webhooks(&state)?
            .enqueue_test(&project_id, &endpoint_id, principal.user_id)
            .await?,
    ))
}

pub(super) async fn list_project_webhook_deliveries(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, endpoint_id)): Path<(String, String)>,
    Query(query): Query<ListWebhookDeliveriesQuery>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    Ok(private_json(
        webhooks(&state)?
            .list_deliveries(
                &project_id,
                &endpoint_id,
                query.after,
                query.limit.unwrap_or(20),
            )
            .await?,
    ))
}

fn webhooks(state: &AppState) -> Result<&Arc<dyn ProjectWebhookService>, ImageGatewayError> {
    state.project_webhook_service.as_ref().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Project webhooks are not configured")
    })
}

fn parse_body<T>(
    body: Result<Json<T>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<T>, ImageGatewayError> {
    body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid webhook request body",
            None,
            "invalid_request_body",
        )
    })
}
