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
    customer_refunds::{
        CreateCustomerRefundRequest, CustomerRefundActor, CustomerRefundService,
        ListCustomerChargesRequest,
    },
};

use super::{
    AppState,
    sessions::{authorize_platform_owner_scope, private_json},
};

const READ_SCOPE: &str = "billing:read";
const REFUND_SCOPE: &str = "billing:refund";

pub(super) async fn list_customer_charges(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListCustomerChargesRequest>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner_scope(&headers, &state, READ_SCOPE).await?;
    Ok(private_json(
        customer_refunds(&state)?.list_charges(query).await?,
    ))
}

pub(super) async fn get_customer_charge(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(transaction_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner_scope(&headers, &state, READ_SCOPE).await?;
    Ok(private_json(
        customer_refunds(&state)?.get_charge(transaction_id).await?,
    ))
}

pub(super) async fn create_customer_refund(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(transaction_id): Path<Uuid>,
    body: Result<Json<CreateCustomerRefundRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner_scope(&headers, &state, REFUND_SCOPE).await?;
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(ImageGatewayError::invalid_idempotency_key)?;
    let Json(request) = body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid customer refund request body",
            None,
            "invalid_request_body",
        )
    })?;
    Ok(private_json(
        customer_refunds(&state)?
            .create_refund(
                transaction_id,
                idempotency_key,
                CustomerRefundActor {
                    user_id: principal.user_id,
                    session_id: principal.session_id,
                },
                request,
            )
            .await?,
    ))
}

fn customer_refunds(
    state: &AppState,
) -> Result<&Arc<dyn CustomerRefundService>, ImageGatewayError> {
    state.customer_refund_service.as_ref().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Customer refund service is not configured")
    })
}
