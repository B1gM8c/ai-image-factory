use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    billing_integrity::{
        BillingIntegrityActor, BillingIntegrityService, ListBillingIntegrityRunsRequest,
    },
};

use super::{
    AppState,
    sessions::{authorize_platform_owner, private_json},
};

pub(super) async fn create_billing_integrity_run(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        billing_integrity(&state)?
            .run(BillingIntegrityActor {
                user_id: principal.user_id,
                session_id: principal.session_id,
            })
            .await?,
    ))
}

pub(super) async fn list_billing_integrity_runs(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListBillingIntegrityRunsRequest>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        billing_integrity(&state)?.list_runs(query).await?,
    ))
}

pub(super) async fn get_billing_integrity_run(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        billing_integrity(&state)?.get_run(run_id).await?,
    ))
}

fn billing_integrity(
    state: &AppState,
) -> Result<&Arc<dyn BillingIntegrityService>, ImageGatewayError> {
    state.billing_integrity_service.as_ref().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Billing integrity service is not configured")
    })
}
