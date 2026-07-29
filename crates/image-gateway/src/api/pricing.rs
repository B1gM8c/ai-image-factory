use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
    response::Response,
};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    pricing::{
        ApplyOfficialPriceSnapshotRequest, CreatePriceBookRequest, CreatePriceBookVersionRequest,
        CreatePriceRollbackDraftRequest, PricePreviewRequest, PricingTransitionActor,
        TransitionPriceBookVersionRequest, UpdatePriceBookVersionRequest,
    },
};

use super::{
    AppState,
    sessions::{authorize_admin_scope, authorize_platform_owner, private_json},
};

pub(super) async fn list_price_books(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    Ok(private_json(pricing(&state)?.catalog().await?))
}

pub(super) async fn pricing_coverage(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    Ok(private_json(pricing(&state)?.coverage().await?))
}

pub(super) async fn price_book_version_publish_readiness(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(price_book_version_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        pricing(&state)?
            .publish_readiness_as(
                price_book_version_id,
                PricingTransitionActor {
                    user_id: principal.user_id,
                    session_id: principal.session_id,
                },
            )
            .await?,
    ))
}

pub(super) async fn create_price_book(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<CreatePriceBookRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(request) = parse_body(body)?;
    Ok(private_json(
        pricing(&state)?.create_price_book(request).await?,
    ))
}

pub(super) async fn create_price_book_version(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(price_book_id): Path<Uuid>,
    body: Result<Json<CreatePriceBookVersionRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(request) = parse_body(body)?;
    Ok(private_json(
        pricing(&state)?
            .create_version(price_book_id, request)
            .await?,
    ))
}

pub(super) async fn update_price_book_version(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(price_book_version_id): Path<Uuid>,
    body: Result<Json<UpdatePriceBookVersionRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(request) = parse_body(body)?;
    Ok(private_json(
        pricing(&state)?
            .update_draft_version(price_book_version_id, request)
            .await?,
    ))
}

pub(super) async fn publish_price_book_version(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(price_book_version_id): Path<Uuid>,
    body: Result<Json<TransitionPriceBookVersionRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let Json(request) = parse_body(body)?;
    Ok(private_json(
        pricing(&state)?
            .publish_version_as(
                price_book_version_id,
                request,
                PricingTransitionActor {
                    user_id: principal.user_id,
                    session_id: principal.session_id,
                },
            )
            .await?,
    ))
}

pub(super) async fn retire_price_book_version(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(price_book_version_id): Path<Uuid>,
    body: Result<Json<TransitionPriceBookVersionRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let Json(request) = parse_body(body)?;
    Ok(private_json(
        pricing(&state)?
            .retire_version_as(
                price_book_version_id,
                request,
                PricingTransitionActor {
                    user_id: principal.user_id,
                    session_id: principal.session_id,
                },
            )
            .await?,
    ))
}

pub(super) async fn create_price_rollback_draft(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(source_version_id): Path<Uuid>,
    body: Result<Json<CreatePriceRollbackDraftRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let Json(request) = parse_body(body)?;
    Ok(private_json(
        pricing(&state)?
            .create_rollback_draft(
                source_version_id,
                request,
                PricingTransitionActor {
                    user_id: principal.user_id,
                    session_id: principal.session_id,
                },
            )
            .await?,
    ))
}

pub(super) async fn preview_price(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<PricePreviewRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(request) = parse_body(body)?;
    Ok(private_json(pricing(&state)?.preview(request).await?))
}

pub(super) async fn list_official_price_catalogs(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(pricing(&state)?.official_catalogs().await?))
}

pub(super) async fn observe_official_price_catalog(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(catalog_key): Path<String>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    Ok(private_json(
        pricing(&state)?
            .observe_official_catalog(&catalog_key, principal.user_id, principal.session_id)
            .await?,
    ))
}

pub(super) async fn apply_official_price_snapshot(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(snapshot_id): Path<Uuid>,
    body: Result<Json<ApplyOfficialPriceSnapshotRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let Json(request) = parse_body(body)?;
    Ok(private_json(
        pricing(&state)?
            .apply_official_snapshot(
                snapshot_id,
                request,
                principal.user_id,
                principal.session_id,
            )
            .await?,
    ))
}

fn pricing(
    state: &AppState,
) -> Result<&Arc<dyn crate::pricing::PricingAdminService>, ImageGatewayError> {
    state
        .pricing_admin_service
        .as_ref()
        .ok_or_else(|| ImageGatewayError::service_unavailable("Pricing admin is not configured"))
}

fn parse_body<T>(
    body: Result<Json<T>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<T>, ImageGatewayError> {
    body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid pricing request body",
            None,
            "invalid_request_body",
        )
    })
}
