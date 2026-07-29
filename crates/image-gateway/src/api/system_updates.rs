use std::sync::Arc;

use axum::{Json, extract::State, http::HeaderMap, response::Response};

use crate::{
    ImageGatewayError,
    system_updates::{
        ApplySystemUpdateRequest, SystemUpdateAction, SystemUpdateActor, SystemUpdateService,
    },
};

use super::{
    AppState,
    sessions::{authorize_platform_owner_scope, private_json},
};

pub(super) async fn get_system_update(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner_scope(&headers, &state, "system:read").await?;
    Ok(private_json(system_updates(&state)?.snapshot().await?))
}

pub(super) async fn check_system_update(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner_scope(&headers, &state, "system:update").await?;
    let command = system_updates(&state)?
        .enqueue(
            SystemUpdateActor {
                user_id: principal.user_id,
                session_id: principal.session_id,
            },
            required_idempotency_key(&headers)?,
            SystemUpdateAction::Check,
            None,
        )
        .await?;
    Ok(private_json(command))
}

pub(super) async fn apply_system_update(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<ApplySystemUpdateRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner_scope(&headers, &state, "system:update").await?;
    let Json(request) = body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid system update request body",
            None,
            "invalid_request_body",
        )
    })?;
    let command = system_updates(&state)?
        .enqueue(
            SystemUpdateActor {
                user_id: principal.user_id,
                session_id: principal.session_id,
            },
            required_idempotency_key(&headers)?,
            SystemUpdateAction::Apply,
            Some(request.target_version),
        )
        .await?;
    Ok(private_json(command))
}

fn system_updates(state: &AppState) -> Result<&Arc<dyn SystemUpdateService>, ImageGatewayError> {
    state.system_update_service.as_ref().ok_or_else(|| {
        ImageGatewayError::service_unavailable("System update service is not configured")
    })
}

fn required_idempotency_key(headers: &HeaderMap) -> Result<&str, ImageGatewayError> {
    headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(ImageGatewayError::invalid_idempotency_key)
}
