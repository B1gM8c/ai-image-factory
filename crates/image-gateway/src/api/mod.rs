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
    admission::{AdmissionStore, InMemoryAdmissionStore},
    api_keys::{ApiKeyStore, InMemoryApiKeyStore},
    artifacts::{ArtifactBlobStore, InMemoryArtifactBlobStore},
    auth::{AuthContext, authorize_legacy, bearer_token},
    docs::{openapi_json, scalar_docs_html},
    generator::ImageGenerator,
    scheduler::TenantJobScheduler,
    settlement::{ExecutionSettlementStore, SequentialExecutionSettlementStore},
    usage::{InMemoryUsageStore, UsageLimits, UsageStore},
    workers::GenerationWorker,
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
    pub(super) admission_store: Arc<dyn AdmissionStore>,
    pub(super) generation_worker: Arc<GenerationWorker>,
    pub(super) settlement_store: Arc<dyn ExecutionSettlementStore>,
    pub(super) scheduler: Arc<TenantJobScheduler>,
    pub(super) worker_id: String,
}

#[derive(Clone, Debug)]
pub(super) struct RequestId(pub(super) String);

pub fn build_router(
    config: AppConfig,
    generator: Arc<dyn ImageGenerator>,
    usage_store: Arc<InMemoryUsageStore>,
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
    usage_store: Arc<InMemoryUsageStore>,
    api_key_store: Arc<dyn ApiKeyStore>,
) -> Router {
    build_router_with_stores(
        config,
        generator,
        usage_store,
        api_key_store,
        Arc::new(InMemoryAdmissionStore::default()),
    )
}

fn build_router_with_stores(
    config: AppConfig,
    generator: Arc<dyn ImageGenerator>,
    usage_store: Arc<dyn UsageStore>,
    api_key_store: Arc<dyn ApiKeyStore>,
    admission_store: Arc<dyn AdmissionStore>,
) -> Router {
    let artifact_store: Arc<dyn ArtifactBlobStore> = Arc::new(InMemoryArtifactBlobStore::default());
    let settlement_store = Arc::new(SequentialExecutionSettlementStore::new(
        admission_store.clone(),
        usage_store.clone(),
        artifact_store.clone(),
    ));
    build_router_with_components(
        config,
        generator,
        usage_store,
        api_key_store,
        admission_store,
        settlement_store,
        artifact_store,
    )
}

pub fn build_router_with_components(
    config: AppConfig,
    generator: Arc<dyn ImageGenerator>,
    usage_store: Arc<dyn UsageStore>,
    api_key_store: Arc<dyn ApiKeyStore>,
    admission_store: Arc<dyn AdmissionStore>,
    settlement_store: Arc<dyn ExecutionSettlementStore>,
    artifact_store: Arc<dyn ArtifactBlobStore>,
) -> Router {
    let scheduler = Arc::new(TenantJobScheduler::new(
        config.max_concurrent_jobs,
        config.max_queue_size,
        config.max_concurrent_jobs_per_tenant,
        config.max_queue_size_per_tenant,
        config.queue_timeout,
    ));
    let generation_worker = Arc::new(GenerationWorker::new(
        generator.clone(),
        admission_store.clone(),
        settlement_store.clone(),
        usage_store.clone(),
        artifact_store,
        config.request_timeout,
    ));
    let body_limit = config.max_upload_bytes;
    let state = AppState {
        config,
        generator,
        api_key_store,
        usage_store,
        admission_store,
        generation_worker,
        settlement_store,
        scheduler,
        worker_id: format!("gateway-{}", uuid::Uuid::new_v4().simple()),
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
    let bearer = bearer_token(headers)?;
    if let Some(context) = state.api_key_store.authenticate(bearer).await? {
        return Ok(context);
    }
    if state.config.auth_token.is_some() {
        return authorize_legacy(headers, &state.config);
    }
    Err(ImageGatewayError::authentication())
}

pub(super) fn usage_limits(config: &AppConfig) -> UsageLimits {
    UsageLimits {
        five_hour_image_limit: config.five_hour_image_limit,
        seven_day_image_limit: config.seven_day_image_limit,
    }
}
