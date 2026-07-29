use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    provider_cost_allocations::{
        CloseProviderCostAllocationRequest, CreateProviderCostAllocationDraftRequest,
        ListProviderCostAllocationsRequest, PreviewProviderCostAllocationRequest,
        ProviderCostAllocationActor, ProviderCostAllocationError, ProviderCostAllocationService,
    },
};

use super::{
    AppState,
    sessions::{authorize_platform_owner, private_json},
};

pub(super) async fn list_provider_cost_allocations(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListProviderCostAllocationsRequest>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        provider_cost_allocations(&state)?
            .list(query)
            .await
            .map_err(map_provider_cost_allocation_error)?,
    ))
}

pub(super) async fn get_provider_cost_allocation(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(pool_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        provider_cost_allocations(&state)?
            .get(pool_id)
            .await
            .map_err(map_provider_cost_allocation_error)?,
    ))
}

pub(super) async fn preview_provider_cost_allocation(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<
        Json<PreviewProviderCostAllocationRequest>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    let Json(request) = body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid provider cost allocation preview request body",
            None,
            "invalid_request_body",
        )
    })?;
    Ok(private_json(
        provider_cost_allocations(&state)?
            .preview(request)
            .await
            .map_err(map_provider_cost_allocation_error)?,
    ))
}

pub(super) async fn create_provider_cost_allocation_draft(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<
        Json<CreateProviderCostAllocationDraftRequest>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    let Json(request) = body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid provider cost allocation draft request body",
            None,
            "invalid_request_body",
        )
    })?;
    Ok(private_json(
        provider_cost_allocations(&state)?
            .create_draft(request)
            .await
            .map_err(map_provider_cost_allocation_error)?,
    ))
}

pub(super) async fn close_provider_cost_allocation(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(pool_id): Path<Uuid>,
    body: Result<Json<CloseProviderCostAllocationRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(ImageGatewayError::invalid_idempotency_key)?;
    let Json(request) = body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid provider cost allocation close request body",
            None,
            "invalid_request_body",
        )
    })?;
    Ok(private_json(
        provider_cost_allocations(&state)?
            .close(
                pool_id,
                idempotency_key,
                ProviderCostAllocationActor {
                    user_id: principal.user_id,
                    session_id: principal.session_id,
                },
                request,
            )
            .await
            .map_err(map_provider_cost_allocation_error)?,
    ))
}

fn provider_cost_allocations(
    state: &AppState,
) -> Result<&Arc<dyn ProviderCostAllocationService>, ImageGatewayError> {
    state
        .provider_cost_allocation_service
        .as_ref()
        .ok_or_else(|| {
            ImageGatewayError::service_unavailable(
                "Provider cost allocation service is not configured",
            )
        })
}

fn map_provider_cost_allocation_error(error: ProviderCostAllocationError) -> ImageGatewayError {
    match error {
        ProviderCostAllocationError::InvalidInput { message, field } => {
            ImageGatewayError::invalid_request(
                message,
                field.map(str::to_owned),
                "invalid_provider_cost_allocation",
            )
        }
        ProviderCostAllocationError::Conflict { message } => {
            ImageGatewayError::conflict(message, None, "provider_cost_allocation_conflict")
        }
        ProviderCostAllocationError::NotFound => ImageGatewayError::not_found(
            "Provider cost allocation pool was not found",
            Some("pool_id".to_string()),
            "provider_cost_allocation_not_found",
        ),
        ProviderCostAllocationError::Unavailable => ImageGatewayError::service_unavailable(
            "Provider cost allocation service is unavailable",
        ),
    }
}
