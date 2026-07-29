use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    provider_cost_obligations::{
        ListProviderCostObligationsRequest, ProviderCostObligationService,
    },
};

use super::{
    AppState,
    sessions::{authorize_platform_owner, private_json},
};

pub(super) async fn list_provider_cost_obligations(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListProviderCostObligationsRequest>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        provider_cost_obligations(&state)?.list(query).await?,
    ))
}

pub(super) async fn get_provider_cost_obligation(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(receipt_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        provider_cost_obligations(&state)?.get(receipt_id).await?,
    ))
}

fn provider_cost_obligations(
    state: &AppState,
) -> Result<&Arc<dyn ProviderCostObligationService>, ImageGatewayError> {
    state
        .provider_cost_obligation_service
        .as_ref()
        .ok_or_else(|| {
            ImageGatewayError::service_unavailable(
                "Provider cost obligation service is not configured",
            )
        })
}
