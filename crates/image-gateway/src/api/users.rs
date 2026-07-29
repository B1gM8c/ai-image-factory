use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    http::HeaderMap,
    response::Response,
};
use serde::Deserialize;

use crate::ImageGatewayError;

use super::{
    AppState,
    sessions::{authorize_platform_owner, identity_service, map_identity_error, private_json},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UsersQuery {
    after_email: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateUserBody {
    email: String,
    display_name: String,
    password: String,
}

pub(super) async fn list_users(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<UsersQuery>,
) -> Result<Response, ImageGatewayError> {
    let actor = authorize_platform_owner(&headers, &state).await?;
    let users = identity_service(&state)?
        .list_users(query.after_email.as_deref(), query.limit.unwrap_or(50))
        .await
        .map_err(map_identity_error)?;
    tracing::info!(
        admin.user_id = %actor.user_id,
        admin.query = "users",
        admin.result_count = users.len(),
        "admin identity read completed"
    );
    Ok(private_json(users))
}

pub(super) async fn create_user(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<CreateUserBody>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let actor = authorize_platform_owner(&headers, &state).await?;
    let Json(body) = body.map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })?;
    let user = identity_service(&state)?
        .create_member_user(body.email, body.display_name, body.password)
        .await
        .map_err(map_identity_error)?;
    tracing::info!(
        admin.user_id = %actor.user_id,
        target.user_id = %user.user_id,
        "admin created member user"
    );
    Ok(private_json(user))
}
