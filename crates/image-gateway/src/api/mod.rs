use std::sync::Arc;

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::HeaderMap,
    middleware as axum_middleware,
    response::IntoResponse,
    routing::{delete, get, post},
};
use tower_http::trace::TraceLayer;

use crate::{
    AppConfig, ImageGatewayError,
    api_keys::{ApiKeyStore, InMemoryApiKeyStore},
    auth::{AuthContext, authorize_legacy, bearer_token},
    docs::{openapi_json, scalar_docs_html},
    generator::ImageGenerator,
    scheduler::TenantJobScheduler,
    usage::{UsageLimits, UsageStore},
};

mod admin;
mod edit_input;
mod images;
mod middleware;
mod responses;

use self::middleware::add_request_id;
use admin::{create_project_service_account, delete_project_api_key, list_project_api_keys};
use images::{edits, generations, healthz, models};

#[derive(Clone)]
pub(super) struct AppState {
    pub(super) config: AppConfig,
    pub(super) generator: Arc<dyn ImageGenerator>,
    pub(super) api_key_store: Arc<dyn ApiKeyStore>,
    pub(super) usage_store: Arc<dyn UsageStore>,
    pub(super) scheduler: Arc<TenantJobScheduler>,
}

#[derive(Clone, Debug)]
pub(super) struct RequestId(pub(super) String);

pub fn build_router(
    config: AppConfig,
    generator: Arc<dyn ImageGenerator>,
    usage_store: Arc<dyn UsageStore>,
) -> Router {
    build_router_with_api_key_store(
        config,
        generator,
        usage_store,
        Arc::new(InMemoryApiKeyStore::default()),
    )
}

pub fn build_router_with_api_key_store(
    config: AppConfig,
    generator: Arc<dyn ImageGenerator>,
    usage_store: Arc<dyn UsageStore>,
    api_key_store: Arc<dyn ApiKeyStore>,
) -> Router {
    let scheduler = Arc::new(TenantJobScheduler::new(
        config.max_concurrent_jobs,
        config.max_queue_size,
        config.max_concurrent_jobs_per_tenant,
        config.max_queue_size_per_tenant,
        config.queue_timeout,
    ));
    let body_limit = config.max_upload_bytes;
    let state = AppState {
        config,
        generator,
        api_key_store,
        usage_store,
        scheduler,
    };

    Router::new()
        .route("/docs", get(scalar_docs))
        .route("/openapi.json", get(openapi))
        .route("/healthz", get(healthz))
        .route("/v1/models", get(models))
        .route("/v1/images/generations", post(generations))
        .route("/v1/images/edits", post(edits))
        .route(
            "/v1/organization/projects/{project_id}/service_accounts",
            post(create_project_service_account),
        )
        .route(
            "/v1/organization/projects/{project_id}/api_keys",
            get(list_project_api_keys),
        )
        .route(
            "/v1/organization/projects/{project_id}/api_keys/{api_key_id}",
            delete(delete_project_api_key),
        )
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(TraceLayer::new_for_http())
        .layer(axum_middleware::from_fn(add_request_id))
        .with_state(Arc::new(state))
}

async fn scalar_docs() -> impl IntoResponse {
    scalar_docs_html()
}

async fn openapi() -> impl IntoResponse {
    openapi_json()
}

pub(super) async fn authenticate_image_request(
    headers: &HeaderMap,
    state: &Arc<AppState>,
) -> Result<AuthContext, ImageGatewayError> {
    let bearer = match bearer_token(headers) {
        Ok(token) => token,
        Err(_) if state.config.auth_token.is_none() && state.config.admin_token.is_none() => {
            return authorize_legacy(headers, &state.config);
        }
        Err(error) => return Err(error),
    };
    if let Some(context) = state.api_key_store.authenticate(bearer).await? {
        return Ok(context);
    }
    if state.config.auth_token.is_some() {
        return authorize_legacy(headers, &state.config);
    }
    if state.config.admin_token.is_none() {
        return Ok(AuthContext::legacy_default());
    }
    Err(ImageGatewayError::authentication())
}

pub(super) fn usage_limits(config: &AppConfig) -> UsageLimits {
    UsageLimits {
        five_hour_image_limit: config.five_hour_image_limit,
        seven_day_image_limit: config.seven_day_image_limit,
    }
}
