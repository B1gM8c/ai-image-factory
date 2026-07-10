use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
};
use serde::Deserialize;
use utoipa::ToSchema;

use crate::{
    ImageGatewayError,
    api_keys::{ProjectApiKeyDeleted, ProjectApiKeyList, ProjectServiceAccount},
    auth::authorize_admin,
};

use super::AppState;

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateServiceAccountRequest {
    #[schema(min_length = 1, max_length = 128)]
    pub(crate) name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListApiKeysQuery {
    after: Option<String>,
    limit: Option<usize>,
}

pub(super) async fn create_project_service_account(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<CreateServiceAccountRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<ProjectServiceAccount>, ImageGatewayError> {
    authorize_admin(&headers, &state.config)?;
    let Json(body) = body.map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })?;
    let service_account = state
        .api_key_store
        .create_service_account(&project_id, &body.name)
        .await?;
    Ok(Json(service_account))
}

pub(super) async fn list_project_api_keys(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    Query(query): Query<ListApiKeysQuery>,
) -> Result<Json<ProjectApiKeyList>, ImageGatewayError> {
    authorize_admin(&headers, &state.config)?;
    let keys = state
        .api_key_store
        .list_project_api_keys(
            &project_id,
            query.after.as_deref(),
            query.limit.unwrap_or(20),
        )
        .await?;
    Ok(Json(keys))
}

pub(super) async fn delete_project_api_key(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, api_key_id)): Path<(String, String)>,
) -> Result<Json<ProjectApiKeyDeleted>, ImageGatewayError> {
    authorize_admin(&headers, &state.config)?;
    let deleted = state
        .api_key_store
        .delete_project_api_key(&project_id, &api_key_id)
        .await?;
    Ok(Json(deleted))
}
