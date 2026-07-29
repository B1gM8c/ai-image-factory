use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
};

use crate::{
    ImageGatewayError,
    billing_control::{
        BillingAccountControlService, BillingControlActor, ListBillingAccountsRequest,
        UpdateBillingAccountLimitRequest,
    },
};

use super::{
    AppState,
    sessions::{authorize_platform_owner, private_json},
};

pub(super) async fn list_billing_accounts(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListBillingAccountsRequest>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        billing_control(&state)?.list_accounts(query).await?,
    ))
}

pub(super) async fn get_billing_account(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((tenant_id, currency)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        billing_control(&state)?
            .get_account(&tenant_id, &currency)
            .await?,
    ))
}

pub(super) async fn update_billing_account_limit(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((tenant_id, currency)): Path<(String, String)>,
    body: Result<Json<UpdateBillingAccountLimitRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let Json(request) = body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid billing account control request body",
            None,
            "invalid_request_body",
        )
    })?;
    Ok(private_json(
        billing_control(&state)?
            .update_limit(
                &tenant_id,
                &currency,
                BillingControlActor {
                    user_id: principal.user_id,
                    session_id: principal.session_id,
                },
                request,
            )
            .await?,
    ))
}

fn billing_control(
    state: &AppState,
) -> Result<&Arc<dyn BillingAccountControlService>, ImageGatewayError> {
    state
        .billing_account_control_service
        .as_ref()
        .ok_or_else(|| {
            ImageGatewayError::service_unavailable("Billing account control is not configured")
        })
}
