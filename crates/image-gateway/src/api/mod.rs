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
    AdminReadStore, AppConfig, GenerationAdmissionContract, ImageGatewayError,
    ProviderProfileReadinessStore,
    admin_read::ProviderAccountRuntimeEventHub,
    admission::{AdmissionStore, InMemoryAdmissionStore},
    api_keys::{ApiKeyStore, InMemoryApiKeyStore},
    artifacts::{ArtifactBlobStore, InMemoryArtifactBlobStore},
    auth::{AuthContext, RequestRouteAttribution, authorize_legacy, bearer_token},
    batches::BatchService,
    billing_control::BillingAccountControlService,
    billing_integrity::BillingIntegrityService,
    credit_grants::CreditGrantService,
    customer_refunds::CustomerRefundService,
    docs::{openapi_json, scalar_docs_html},
    generator::ImageGenerator,
    input_blobs::InputBlobStore,
    model_routing::{ModelRoutingStore, ResolvedModelRoute},
    pricing::PricingAdminService,
    project_governance::ProjectGovernanceService,
    project_limits::ProjectSpendBudgetService,
    project_model_policy::ProjectModelPolicyService,
    provider_cost_allocations::ProviderCostAllocationService,
    provider_cost_obligations::ProviderCostObligationService,
    provider_management::ProviderManagementService,
    provider_uploads::ProviderUploadService,
    scheduler::TenantJobScheduler,
    settlement::{ExecutionSettlementStore, SequentialExecutionSettlementStore},
    usage::{InMemoryUsageStore, UsageLimits, UsageStore},
    webhooks::ProjectWebhookService,
    workers::GenerationWorker,
};
use factory_identity::IdentityService;

mod admin;
mod admin_read;
mod ark;
mod batch_worker;
mod batches;
mod billing_control;
mod billing_integrity;
mod console_media;
mod console_video;
mod credit_grants;
mod customer_refunds;
mod dreamina;
mod edit_input;
mod files;
mod images;
mod middleware;
mod pricing;
mod project_governance;
mod project_limits;
mod project_models;
mod provider_cost_allocations;
mod provider_cost_obligations;
mod provider_management;
mod provider_uploads;
mod readiness;
mod responses;
mod sessions;
mod users;
mod videos;
mod webhooks;
mod xai_images;

pub(super) const IMAGE_GENERATION_ROUTE_OPERATION: &str = "images.generations";
pub(super) const IMAGE_EDIT_ROUTE_OPERATION: &str = "images.edits";
pub(super) const VIDEO_GENERATION_ROUTE_OPERATION: &str = "videos.generations";

use self::middleware::{add_request_id, observe_request};
use admin::{
    create_project, create_project_service_account, create_user_api_key, delete_project_api_key,
    delete_project_service_account, get_project, list_project_api_keys, list_projects,
    rotate_project_api_key, update_project, update_project_api_key,
};
use admin_read::{
    audit_logs, billing_summary, console_billing_summary, console_job_economics, console_jobs,
    console_overview, console_request_logs, console_usage_analysis, job_economics, list_jobs,
    overview, provider_account_runtime_events, provider_accounts, request_logs, scheduler_queues,
    usage_analysis,
};
use ark::{
    create_content_task as create_ark_content_task, create_image as create_ark_image,
    get_content_task as get_ark_content_task,
};
use batches::{
    cancel_batch, cancel_console_batch, create_batch, create_console_batch, get_batch,
    get_console_batch, list_batches, list_console_batches,
};
use billing_control::{get_billing_account, list_billing_accounts, update_billing_account_limit};
use billing_integrity::{
    create_billing_integrity_run, get_billing_integrity_run, list_billing_integrity_runs,
};
use console_media::{
    edit_image as console_edit_image, generate_image as console_generate_image,
    image_models as console_image_models,
};
use console_video::{
    generate_video as console_generate_video, get_console_video, get_console_video_content,
    video_models as console_video_models,
};
use credit_grants::{
    create_credit_grant, get_credit_grant, list_credit_grants, list_organization_credit_grants,
    revoke_credit_grant,
};
use customer_refunds::{create_customer_refund, get_customer_charge, list_customer_charges};
use dreamina::{
    create_image as create_dreamina_image, create_video as create_dreamina_video,
    get_video as get_dreamina_video,
};
use files::{
    create_console_file, create_file, delete_console_file, delete_file, get_console_file,
    get_console_file_content, get_file, get_file_content, list_console_files, list_files,
};
use images::{edits, generations, healthz, models};
use pricing::{
    apply_official_price_snapshot, create_price_book, create_price_book_version,
    create_price_rollback_draft, list_official_price_catalogs, list_price_books,
    observe_official_price_catalog, preview_price, price_book_version_publish_readiness,
    pricing_coverage, publish_price_book_version, retire_price_book_version,
    update_price_book_version,
};
use project_governance::{
    add_project_member, list_project_members, remove_project_member, update_project_member,
};
use project_limits::{
    get_project_limits, list_project_spend_notifications, mark_project_spend_notification_read,
    update_project_limits,
};
use project_models::{get_project_model_policy, update_project_model_policy};
use provider_cost_allocations::{
    close_provider_cost_allocation, create_provider_cost_allocation_draft,
    get_provider_cost_allocation, list_provider_cost_allocations, preview_provider_cost_allocation,
};
use provider_cost_obligations::{get_provider_cost_obligation, list_provider_cost_obligations};
use provider_management::{
    bind_api_key_route, create_provider_route, get_api_key_route, grok_video_output,
    list_console_provider_models, list_console_provider_routes, list_provider_models,
    list_provider_routes, managed_cli_providers, provider_account_models, provider_login_session,
    provider_model_refresh, refresh_provider_quota, start_codex_login, start_provider_login,
    start_provider_model_refresh, start_provider_reauthorization, update_grok_video_output,
    update_provider_account_model_configuration, update_provider_account_models,
    update_provider_account_scheduling, update_provider_route,
};
use provider_uploads::provider_upload;
use readiness::{EmptyProviderProfileReadinessStore, readyz};
use sessions::{login, logout, me, refresh};
use users::{create_user, list_users};
use videos::{create_video, get_video, get_video_content};
use webhooks::{
    create_project_webhook, delete_project_webhook, list_project_webhook_deliveries,
    list_project_webhooks, rotate_project_webhook_secret, test_project_webhook,
    update_project_webhook,
};

#[derive(Clone)]
pub(super) struct AppState {
    pub(super) config: AppConfig,
    pub(super) api_key_store: Arc<dyn ApiKeyStore>,
    pub(super) usage_store: Arc<dyn UsageStore>,
    pub(super) admission_store: Arc<dyn AdmissionStore>,
    pub(super) generation_worker: Option<Arc<GenerationWorker>>,
    pub(super) settlement_store: Arc<dyn ExecutionSettlementStore>,
    pub(super) input_blob_store: Arc<dyn InputBlobStore>,
    pub(super) provider_readiness_store: Arc<dyn ProviderProfileReadinessStore>,
    pub(super) scheduler: Arc<TenantJobScheduler>,
    pub(super) upload_scheduler: Arc<TenantJobScheduler>,
    pub(super) worker_id: String,
    pub(super) generation_execution_mode: GenerationExecutionMode,
    pub(super) identity_service: Option<Arc<IdentityService>>,
    pub(super) admin_read_store: Option<Arc<dyn AdminReadStore>>,
    pub(super) provider_account_runtime_events: Option<Arc<ProviderAccountRuntimeEventHub>>,
    pub(super) provider_management_service: Option<Arc<dyn ProviderManagementService>>,
    pub(super) provider_upload_service: Option<Arc<ProviderUploadService>>,
    pub(super) model_routing_store: Option<Arc<dyn ModelRoutingStore>>,
    pub(super) pricing_admin_service: Option<Arc<dyn PricingAdminService>>,
    pub(super) billing_account_control_service: Option<Arc<dyn BillingAccountControlService>>,
    pub(super) billing_integrity_service: Option<Arc<dyn BillingIntegrityService>>,
    pub(super) credit_grant_service: Option<Arc<dyn CreditGrantService>>,
    pub(super) customer_refund_service: Option<Arc<dyn CustomerRefundService>>,
    pub(super) provider_cost_allocation_service: Option<Arc<dyn ProviderCostAllocationService>>,
    pub(super) provider_cost_obligation_service: Option<Arc<dyn ProviderCostObligationService>>,
    pub(super) project_governance_service: Option<Arc<dyn ProjectGovernanceService>>,
    pub(super) project_spend_budget_service: Option<Arc<dyn ProjectSpendBudgetService>>,
    pub(super) project_model_policy_service: Option<Arc<dyn ProjectModelPolicyService>>,
    pub(super) project_webhook_service: Option<Arc<dyn ProjectWebhookService>>,
    pub(super) batch_service: Option<Arc<dyn BatchService>>,
    pub(super) request_observation_sink: crate::RequestObservationSink,
    pub(super) legacy_admin_auth_enabled: bool,
}

#[derive(Clone, Default)]
pub struct ExternalControlPlaneServices {
    pub identity_service: Option<Arc<IdentityService>>,
    pub admin_read_store: Option<Arc<dyn AdminReadStore>>,
    pub provider_management_service: Option<Arc<dyn ProviderManagementService>>,
    pub provider_upload_service: Option<Arc<ProviderUploadService>>,
    pub provider_account_runtime_event_hub: Option<Arc<ProviderAccountRuntimeEventHub>>,
    pub model_routing_store: Option<Arc<dyn ModelRoutingStore>>,
    pub pricing_admin_service: Option<Arc<dyn PricingAdminService>>,
    pub billing_account_control_service: Option<Arc<dyn BillingAccountControlService>>,
    pub billing_integrity_service: Option<Arc<dyn BillingIntegrityService>>,
    pub credit_grant_service: Option<Arc<dyn CreditGrantService>>,
    pub customer_refund_service: Option<Arc<dyn CustomerRefundService>>,
    pub provider_cost_allocation_service: Option<Arc<dyn ProviderCostAllocationService>>,
    pub provider_cost_obligation_service: Option<Arc<dyn ProviderCostObligationService>>,
    pub project_governance_service: Option<Arc<dyn ProjectGovernanceService>>,
    pub project_spend_budget_service: Option<Arc<dyn ProjectSpendBudgetService>>,
    pub project_model_policy_service: Option<Arc<dyn ProjectModelPolicyService>>,
    pub project_webhook_service: Option<Arc<dyn ProjectWebhookService>>,
    pub batch_service: Option<Arc<dyn BatchService>>,
    pub request_observation_sink: Option<crate::RequestObservationSink>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationExecutionMode {
    Inline,
    External,
}

pub struct ImageGatewayComponents {
    pub generator: Arc<dyn ImageGenerator>,
    pub usage_store: Arc<dyn UsageStore>,
    pub api_key_store: Arc<dyn ApiKeyStore>,
    pub admission_store: Arc<dyn AdmissionStore>,
    pub settlement_store: Arc<dyn ExecutionSettlementStore>,
    pub artifact_store: Arc<dyn ArtifactBlobStore>,
    pub input_blob_store: Arc<dyn InputBlobStore>,
}

pub struct ExternalImageGatewayComponents {
    pub usage_store: Arc<dyn UsageStore>,
    pub api_key_store: Arc<dyn ApiKeyStore>,
    pub admission_store: Arc<dyn AdmissionStore>,
    pub settlement_store: Arc<dyn ExecutionSettlementStore>,
    pub input_blob_store: Arc<dyn InputBlobStore>,
    pub provider_readiness_store: Arc<dyn ProviderProfileReadinessStore>,
}

struct GatewayStores {
    usage_store: Arc<dyn UsageStore>,
    api_key_store: Arc<dyn ApiKeyStore>,
    admission_store: Arc<dyn AdmissionStore>,
    settlement_store: Arc<dyn ExecutionSettlementStore>,
    input_blob_store: Arc<dyn InputBlobStore>,
    provider_readiness_store: Arc<dyn ProviderProfileReadinessStore>,
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
    let blobs = Arc::new(InMemoryArtifactBlobStore::default());
    let artifact_store: Arc<dyn ArtifactBlobStore> = blobs.clone();
    let input_blob_store: Arc<dyn InputBlobStore> = blobs;
    let settlement_store = Arc::new(SequentialExecutionSettlementStore::new(
        admission_store.clone(),
        usage_store.clone(),
        artifact_store.clone(),
    ));
    build_router_with_components(
        config,
        ImageGatewayComponents {
            generator,
            usage_store,
            api_key_store,
            admission_store,
            settlement_store,
            artifact_store,
            input_blob_store,
        },
    )
    .expect("in-memory gateway components use one artifact backend")
}

pub fn build_router_with_components(
    config: AppConfig,
    components: ImageGatewayComponents,
) -> Result<Router, ImageGatewayError> {
    let ImageGatewayComponents {
        generator,
        usage_store,
        api_key_store,
        admission_store,
        settlement_store,
        artifact_store,
        input_blob_store,
    } = components;
    validate_component_storage(
        artifact_store.as_ref(),
        input_blob_store.as_ref(),
        settlement_store.as_ref(),
    )?;
    let generation_worker = Arc::new(GenerationWorker::new(
        generator,
        admission_store.clone(),
        settlement_store.clone(),
        artifact_store,
        config.request_timeout,
    ));
    build_router_with_execution_mode(
        config,
        GatewayStores {
            usage_store,
            api_key_store,
            admission_store,
            settlement_store,
            input_blob_store,
            provider_readiness_store: Arc::new(EmptyProviderProfileReadinessStore),
        },
        Some(generation_worker),
        GenerationExecutionMode::Inline,
        ExternalControlPlaneServices::default(),
    )
}

pub fn build_router_with_external_execution(
    config: AppConfig,
    components: ExternalImageGatewayComponents,
) -> Result<Router, ImageGatewayError> {
    build_router_with_external_execution_and_identity(config, components, None)
}

pub fn build_router_with_external_execution_and_identity(
    config: AppConfig,
    components: ExternalImageGatewayComponents,
    identity_service: Option<Arc<IdentityService>>,
) -> Result<Router, ImageGatewayError> {
    build_router_with_external_execution_and_identity_and_admin_read(
        config,
        components,
        identity_service,
        None,
    )
}

pub fn build_router_with_external_execution_and_identity_and_admin_read(
    config: AppConfig,
    components: ExternalImageGatewayComponents,
    identity_service: Option<Arc<IdentityService>>,
    admin_read_store: Option<Arc<dyn AdminReadStore>>,
) -> Result<Router, ImageGatewayError> {
    build_router_with_external_execution_and_control_plane(
        config,
        components,
        identity_service,
        admin_read_store,
        None,
    )
}

pub fn build_router_with_external_execution_and_control_plane(
    config: AppConfig,
    components: ExternalImageGatewayComponents,
    identity_service: Option<Arc<IdentityService>>,
    admin_read_store: Option<Arc<dyn AdminReadStore>>,
    provider_management_service: Option<Arc<dyn ProviderManagementService>>,
) -> Result<Router, ImageGatewayError> {
    build_router_with_external_execution_and_control_plane_and_runtime_events(
        config,
        components,
        identity_service,
        admin_read_store,
        provider_management_service,
        None,
    )
}

pub fn build_router_with_external_execution_and_control_plane_and_runtime_events(
    config: AppConfig,
    components: ExternalImageGatewayComponents,
    identity_service: Option<Arc<IdentityService>>,
    admin_read_store: Option<Arc<dyn AdminReadStore>>,
    provider_management_service: Option<Arc<dyn ProviderManagementService>>,
    provider_account_runtime_event_hub: Option<Arc<ProviderAccountRuntimeEventHub>>,
) -> Result<Router, ImageGatewayError> {
    build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing(
        config,
        components,
        identity_service,
        admin_read_store,
        provider_management_service,
        provider_account_runtime_event_hub,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing(
    config: AppConfig,
    components: ExternalImageGatewayComponents,
    identity_service: Option<Arc<IdentityService>>,
    admin_read_store: Option<Arc<dyn AdminReadStore>>,
    provider_management_service: Option<Arc<dyn ProviderManagementService>>,
    provider_account_runtime_event_hub: Option<Arc<ProviderAccountRuntimeEventHub>>,
    model_routing_store: Option<Arc<dyn ModelRoutingStore>>,
) -> Result<Router, ImageGatewayError> {
    build_router_with_external_execution_and_services(
        config,
        components,
        ExternalControlPlaneServices {
            identity_service,
            admin_read_store,
            provider_management_service,
            provider_upload_service: None,
            provider_account_runtime_event_hub,
            model_routing_store,
            pricing_admin_service: None,
            billing_account_control_service: None,
            billing_integrity_service: None,
            credit_grant_service: None,
            customer_refund_service: None,
            provider_cost_allocation_service: None,
            provider_cost_obligation_service: None,
            project_governance_service: None,
            project_spend_budget_service: None,
            project_model_policy_service: None,
            project_webhook_service: None,
            batch_service: None,
            request_observation_sink: None,
        },
    )
}

pub fn build_router_with_external_execution_and_services(
    config: AppConfig,
    components: ExternalImageGatewayComponents,
    control_plane: ExternalControlPlaneServices,
) -> Result<Router, ImageGatewayError> {
    let ExternalImageGatewayComponents {
        usage_store,
        api_key_store,
        admission_store,
        settlement_store,
        input_blob_store,
        provider_readiness_store,
    } = components;
    if settlement_store.artifact_storage_identity() != input_blob_store.storage_identity() {
        return Err(ImageGatewayError::config(
            "input and settlement stores must use the same storage backend instance",
        ));
    }
    build_router_with_execution_mode(
        config,
        GatewayStores {
            usage_store,
            api_key_store,
            admission_store,
            settlement_store,
            input_blob_store,
            provider_readiness_store,
        },
        None,
        GenerationExecutionMode::External,
        control_plane,
    )
}

fn build_router_with_execution_mode(
    config: AppConfig,
    stores: GatewayStores,
    generation_worker: Option<Arc<GenerationWorker>>,
    generation_execution_mode: GenerationExecutionMode,
    control_plane: ExternalControlPlaneServices,
) -> Result<Router, ImageGatewayError> {
    if matches!(
        config.generation_admission_contract,
        GenerationAdmissionContract::OutputEconomicsV2
            | GenerationAdmissionContract::CustomerPricingV4
    ) && generation_execution_mode != GenerationExecutionMode::External
    {
        return Err(ImageGatewayError::config(
            "durable image economics generation requires external execution",
        ));
    }
    let GatewayStores {
        usage_store,
        api_key_store,
        admission_store,
        settlement_store,
        input_blob_store,
        provider_readiness_store,
    } = stores;
    let scheduler = Arc::new(TenantJobScheduler::new(
        config.max_concurrent_jobs,
        config.max_queue_size,
        config.max_concurrent_jobs_per_tenant,
        config.max_queue_size_per_tenant,
        config.queue_timeout,
    ));
    let upload_scheduler = Arc::new(TenantJobScheduler::new(
        config.max_concurrent_jobs,
        config.max_queue_size,
        config.max_concurrent_jobs_per_tenant,
        config.max_queue_size_per_tenant,
        config.queue_timeout,
    ));
    let body_limit = config.max_upload_bytes;
    let enable_xai_video_api = config.enable_xai_video_api;
    let legacy_admin_auth_enabled = config.legacy_admin_auth_enabled;
    let ExternalControlPlaneServices {
        identity_service,
        admin_read_store,
        provider_management_service,
        provider_upload_service,
        provider_account_runtime_event_hub,
        model_routing_store,
        pricing_admin_service,
        billing_account_control_service,
        billing_integrity_service,
        credit_grant_service,
        customer_refund_service,
        provider_cost_allocation_service,
        provider_cost_obligation_service,
        project_governance_service,
        project_spend_budget_service,
        project_model_policy_service,
        project_webhook_service,
        batch_service,
        request_observation_sink,
    } = control_plane;
    let state = AppState {
        config,
        api_key_store,
        usage_store,
        admission_store,
        generation_worker,
        settlement_store,
        input_blob_store,
        provider_readiness_store,
        scheduler,
        upload_scheduler,
        worker_id: format!("gateway-{}", uuid::Uuid::new_v4().simple()),
        generation_execution_mode,
        identity_service,
        admin_read_store,
        provider_account_runtime_events: provider_account_runtime_event_hub,
        provider_management_service,
        provider_upload_service,
        model_routing_store,
        pricing_admin_service,
        billing_account_control_service,
        billing_integrity_service,
        credit_grant_service,
        customer_refund_service,
        provider_cost_allocation_service,
        provider_cost_obligation_service,
        project_governance_service,
        project_spend_budget_service,
        project_model_policy_service,
        project_webhook_service,
        batch_service,
        request_observation_sink: request_observation_sink.unwrap_or_default(),
        legacy_admin_auth_enabled,
    };

    let state = Arc::new(state);
    batch_worker::spawn(state.clone());
    let router = Router::new()
        .route("/docs", get(scalar_docs))
        .route("/openapi.json", get(openapi))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route(
            "/v1/internal/provider-uploads/s3/{*path}",
            get(provider_upload).put(provider_upload),
        )
        .route("/v1/models", get(models))
        .route("/v1/images/generations", post(generations))
        .route("/v1/images/edits", post(edits))
        .route(
            "/v1/files",
            get(list_files)
                .post(create_file)
                .layer(DefaultBodyLimit::max(
                    crate::batches::MAX_BATCH_FILE_BYTES as usize + 1024 * 1024,
                )),
        )
        .route("/v1/files/{file_id}", get(get_file).delete(delete_file))
        .route("/v1/files/{file_id}/content", get(get_file_content))
        .route("/v1/batches", get(list_batches).post(create_batch))
        .route("/v1/batches/{batch_id}", get(get_batch))
        .route("/v1/batches/{batch_id}/cancel", post(cancel_batch))
        .route("/admin/v1/auth/login", post(login))
        .route("/admin/v1/auth/refresh", post(refresh))
        .route("/admin/v1/auth/logout", post(logout))
        .route("/admin/v1/auth/me", get(me))
        .route("/admin/v1/users", get(list_users).post(create_user))
        .route("/v1/console/overview", get(console_overview))
        .route("/v1/console/billing/summary", get(console_billing_summary))
        .route(
            "/v1/organizations/{organization_id}/billing/credit-grants",
            get(list_organization_credit_grants),
        )
        .route("/v1/console/usage", get(console_usage_analysis))
        .route("/v1/console/jobs", get(console_jobs))
        .route("/v1/console/logs", get(console_request_logs))
        .route(
            "/v1/console/jobs/{job_id}/economics",
            get(console_job_economics),
        )
        .route(
            "/v1/console/provider-routes",
            get(list_console_provider_routes),
        )
        .route(
            "/v1/console/provider-models",
            get(list_console_provider_models),
        )
        .route(
            "/v1/console/projects/{project_id}/images/models",
            get(console_image_models),
        )
        .route(
            "/v1/console/projects/{project_id}/images/generations",
            post(console_generate_image),
        )
        .route(
            "/v1/console/projects/{project_id}/images/edits",
            post(console_edit_image),
        )
        .route(
            "/v1/console/projects/{project_id}/files",
            get(list_console_files)
                .post(create_console_file)
                .layer(DefaultBodyLimit::max(
                    crate::batches::MAX_BATCH_FILE_BYTES as usize + 1024 * 1024,
                )),
        )
        .route(
            "/v1/console/projects/{project_id}/files/{file_id}",
            get(get_console_file).delete(delete_console_file),
        )
        .route(
            "/v1/console/projects/{project_id}/files/{file_id}/content",
            get(get_console_file_content),
        )
        .route(
            "/v1/console/projects/{project_id}/batches",
            get(list_console_batches).post(create_console_batch),
        )
        .route(
            "/v1/console/projects/{project_id}/batches/{batch_id}",
            get(get_console_batch),
        )
        .route(
            "/v1/console/projects/{project_id}/batches/{batch_id}/cancel",
            post(cancel_console_batch),
        )
        .route(
            "/v1/console/projects/{project_id}/videos/models",
            get(console_video_models),
        )
        .route(
            "/v1/console/projects/{project_id}/videos/generations",
            post(console_generate_video),
        )
        .route(
            "/v1/console/projects/{project_id}/videos/{task_id}",
            get(get_console_video),
        )
        .route(
            "/v1/console/projects/{project_id}/videos/files/{file_id}/content",
            get(get_console_video_content),
        )
        .route("/admin/v1/overview", get(overview))
        .route("/admin/v1/billing/summary", get(billing_summary))
        .route("/admin/v1/billing/accounts", get(list_billing_accounts))
        .route(
            "/admin/v1/billing/accounts/{tenant_id}/{currency}",
            get(get_billing_account).put(update_billing_account_limit),
        )
        .route(
            "/admin/v1/billing/credit-grants",
            get(list_credit_grants).post(create_credit_grant),
        )
        .route(
            "/admin/v1/billing/credit-grants/{grant_id}",
            get(get_credit_grant),
        )
        .route(
            "/admin/v1/billing/credit-grants/{grant_id}/revoke",
            post(revoke_credit_grant),
        )
        .route(
            "/admin/v1/billing/integrity-runs",
            get(list_billing_integrity_runs).post(create_billing_integrity_run),
        )
        .route(
            "/admin/v1/billing/integrity-runs/{run_id}",
            get(get_billing_integrity_run),
        )
        .route(
            "/admin/v1/billing/customer-charges",
            get(list_customer_charges),
        )
        .route(
            "/admin/v1/billing/customer-charges/{transaction_id}",
            get(get_customer_charge),
        )
        .route(
            "/admin/v1/billing/customer-charges/{transaction_id}/refunds",
            post(create_customer_refund),
        )
        .route(
            "/admin/v1/billing/provider-cost-obligations",
            get(list_provider_cost_obligations),
        )
        .route(
            "/admin/v1/billing/provider-cost-obligations/{receipt_id}",
            get(get_provider_cost_obligation),
        )
        .route(
            "/admin/v1/billing/provider-cost-allocation-pools",
            get(list_provider_cost_allocations).post(create_provider_cost_allocation_draft),
        )
        .route(
            "/admin/v1/billing/provider-cost-allocation-pools/preview",
            post(preview_provider_cost_allocation),
        )
        .route(
            "/admin/v1/billing/provider-cost-allocation-pools/{pool_id}",
            get(get_provider_cost_allocation).post(close_provider_cost_allocation),
        )
        .route("/admin/v1/usage", get(usage_analysis))
        .route(
            "/admin/v1/pricing/price-books",
            get(list_price_books).post(create_price_book),
        )
        .route("/admin/v1/pricing/coverage", get(pricing_coverage))
        .route(
            "/admin/v1/pricing/price-book-versions/{price_book_version_id}/publish-readiness",
            get(price_book_version_publish_readiness),
        )
        .route(
            "/admin/v1/pricing/price-books/{price_book_id}/versions",
            post(create_price_book_version),
        )
        .route(
            "/admin/v1/pricing/price-book-versions/{price_book_version_id}",
            axum::routing::put(update_price_book_version),
        )
        .route(
            "/admin/v1/pricing/price-book-versions/{price_book_version_id}/publish",
            post(publish_price_book_version),
        )
        .route(
            "/admin/v1/pricing/price-book-versions/{price_book_version_id}/retire",
            post(retire_price_book_version),
        )
        .route(
            "/admin/v1/pricing/price-book-versions/{price_book_version_id}/rollback-draft",
            post(create_price_rollback_draft),
        )
        .route("/admin/v1/pricing/preview", post(preview_price))
        .route(
            "/admin/v1/pricing/official-catalogs",
            get(list_official_price_catalogs),
        )
        .route(
            "/admin/v1/pricing/official-catalogs/{catalog_key}/snapshots",
            post(observe_official_price_catalog),
        )
        .route(
            "/admin/v1/pricing/source-snapshots/{snapshot_id}/apply",
            post(apply_official_price_snapshot),
        )
        .route("/admin/v1/provider-accounts", get(provider_accounts))
        .route(
            "/admin/v1/provider-account-runtime-events",
            get(provider_account_runtime_events),
        )
        .route(
            "/admin/v1/managed-cli-providers",
            get(managed_cli_providers),
        )
        .route("/admin/v1/provider-models", get(list_provider_models))
        .route(
            "/admin/v1/provider-accounts/{provider_account_id}/model-refreshes",
            post(start_provider_model_refresh),
        )
        .route(
            "/admin/v1/provider-accounts/{provider_account_id}/models",
            get(provider_account_models).put(update_provider_account_models),
        )
        .route(
            "/admin/v1/provider-accounts/{provider_account_id}/model-configuration",
            axum::routing::put(update_provider_account_model_configuration),
        )
        .route(
            "/admin/v1/provider-model-refreshes/{refresh_id}",
            get(provider_model_refresh),
        )
        .route(
            "/admin/v1/provider-account-login-sessions",
            post(start_provider_login),
        )
        .route(
            "/admin/v1/provider-accounts/codex/login-sessions",
            post(start_codex_login),
        )
        .route(
            "/admin/v1/provider-account-login-sessions/{login_session_id}",
            get(provider_login_session),
        )
        .route(
            "/admin/v1/provider-accounts/{provider_account_id}/quota-refresh",
            post(refresh_provider_quota),
        )
        .route(
            "/admin/v1/provider-accounts/{provider_account_id}/reauthorization-sessions",
            post(start_provider_reauthorization),
        )
        .route(
            "/admin/v1/provider-accounts/{provider_account_id}",
            axum::routing::patch(update_provider_account_scheduling),
        )
        .route(
            "/admin/v1/provider-accounts/{provider_account_id}/grok-video-output",
            get(grok_video_output).put(update_grok_video_output),
        )
        .route(
            "/admin/v1/provider-routes",
            get(list_provider_routes).post(create_provider_route),
        )
        .route(
            "/admin/v1/provider-routes/{route_id}",
            axum::routing::put(update_provider_route),
        )
        .route("/admin/v1/scheduler/queues", get(scheduler_queues))
        .route("/v1/organization/audit_logs", get(audit_logs))
        .route("/admin/v1/jobs", get(list_jobs))
        .route("/admin/v1/logs", get(request_logs))
        .route("/admin/v1/jobs/{job_id}/economics", get(job_economics))
        .route(
            "/v1/organization/projects",
            get(list_projects).post(create_project),
        )
        .route(
            "/v1/organization/projects/{project_id}",
            get(get_project).patch(update_project),
        )
        .route(
            "/v1/organization/projects/{project_id}/limits",
            get(get_project_limits).put(update_project_limits),
        )
        .route(
            "/v1/organization/projects/{project_id}/model-policy",
            get(get_project_model_policy).put(update_project_model_policy),
        )
        .route(
            "/v1/organization/projects/{project_id}/members",
            get(list_project_members).post(add_project_member),
        )
        .route(
            "/v1/organization/projects/{project_id}/members/{user_id}",
            axum::routing::patch(update_project_member).delete(remove_project_member),
        )
        .route(
            "/v1/organization/projects/{project_id}/webhooks",
            get(list_project_webhooks).post(create_project_webhook),
        )
        .route(
            "/v1/organization/projects/{project_id}/webhooks/{endpoint_id}",
            axum::routing::patch(update_project_webhook).delete(delete_project_webhook),
        )
        .route(
            "/v1/organization/projects/{project_id}/webhooks/{endpoint_id}/rotate",
            post(rotate_project_webhook_secret),
        )
        .route(
            "/v1/organization/projects/{project_id}/webhooks/{endpoint_id}/test",
            post(test_project_webhook),
        )
        .route(
            "/v1/organization/projects/{project_id}/webhooks/{endpoint_id}/deliveries",
            get(list_project_webhook_deliveries),
        )
        .route(
            "/v1/console/notifications",
            get(list_project_spend_notifications),
        )
        .route(
            "/v1/console/notifications/{delivery_id}/read",
            post(mark_project_spend_notification_read),
        )
        .route(
            "/v1/organization/projects/{project_id}/service_accounts",
            post(create_project_service_account),
        )
        .route(
            "/v1/organization/projects/{project_id}/service_accounts/{service_account_id}",
            delete(delete_project_service_account),
        )
        .route(
            "/v1/organization/projects/{project_id}/api_keys",
            get(list_project_api_keys).post(create_user_api_key),
        )
        .route(
            "/v1/organization/projects/{project_id}/api_keys/{api_key_id}",
            delete(delete_project_api_key).patch(update_project_api_key),
        )
        .route(
            "/v1/organization/projects/{project_id}/api_keys/{api_key_id}/rotate",
            post(rotate_project_api_key),
        )
        .route(
            "/v1/organization/projects/{project_id}/api_keys/{api_key_id}/provider-route",
            get(get_api_key_route).put(bind_api_key_route),
        )
        .route(
            "/v1/dreamina/images/generations",
            post(create_dreamina_image),
        )
        .route(
            "/v1/dreamina/videos/generations",
            post(create_dreamina_video),
        )
        .route("/v1/dreamina/videos/{task_id}", get(get_dreamina_video))
        .route(
            "/v1/dreamina/files/{file_id}/content",
            get(get_video_content),
        )
        .route("/api/v3/images/generations", post(create_ark_image))
        .route(
            "/api/v3/contents/generations/tasks",
            post(create_ark_content_task),
        )
        .route(
            "/api/v3/contents/generations/tasks/{task_id}",
            get(get_ark_content_task),
        )
        .route("/api/v3/files/{file_id}/content", get(get_video_content));
    let router = if enable_xai_video_api {
        router
            .route("/v1/videos/generations", post(create_video))
            .route("/v1/videos/{request_id}", get(get_video))
    } else {
        router
    };
    Ok(router
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(TraceLayer::new_for_http())
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            observe_request,
        ))
        .layer(axum_middleware::from_fn(add_request_id))
        .with_state(state))
}

fn validate_component_storage(
    artifact_store: &dyn ArtifactBlobStore,
    input_blob_store: &dyn InputBlobStore,
    settlement_store: &dyn ExecutionSettlementStore,
) -> Result<(), ImageGatewayError> {
    let artifact_identity = artifact_store.storage_identity();
    if artifact_identity == settlement_store.artifact_storage_identity()
        && artifact_identity == input_blob_store.storage_identity()
    {
        return Ok(());
    }
    Err(ImageGatewayError::config(
        "artifact, input, and settlement stores must use the same storage backend instance",
    ))
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
        crate::request_observability::capture_auth(&context);
        return Ok(context);
    }
    if state.config.auth_token.is_some() {
        let context = authorize_legacy(headers, &state.config)?;
        crate::request_observability::capture_auth(&context);
        return Ok(context);
    }
    Err(ImageGatewayError::authentication())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn resolve_request_model(
    state: &Arc<AppState>,
    auth: &mut AuthContext,
    provider_id: &str,
    operation_id: &str,
    api_profile: &str,
    requested_public_model_id: Option<&str>,
    default_provider_model_id: &str,
) -> Result<Option<ResolvedModelRoute>, ImageGatewayError> {
    let Some(store) = state.model_routing_store.as_ref() else {
        return Ok(None);
    };
    let resolved = if let (Some(api_key_id), Some(authz_version)) =
        (auth.api_key_id.as_deref(), auth.credential_authz_version)
    {
        store
            .resolve_api_key_model(
                &auth.project_id,
                api_key_id,
                authz_version,
                provider_id,
                operation_id,
                api_profile,
                requested_public_model_id,
                default_provider_model_id,
            )
            .await?
    } else if auth.actor_user_id.is_some() {
        store
            .resolve_console_model(
                &auth.project_id,
                provider_id,
                operation_id,
                api_profile,
                requested_public_model_id,
                default_provider_model_id,
            )
            .await?
    } else {
        None
    };
    set_route_attribution(auth, resolved.as_ref());
    Ok(resolved)
}

pub(super) async fn resolve_surface_model(
    state: &Arc<AppState>,
    auth: &mut AuthContext,
    operation_id: &str,
    api_profiles: &[&str],
    requested_public_model_id: &str,
) -> Result<Option<ResolvedModelRoute>, ImageGatewayError> {
    let Some(store) = state.model_routing_store.as_ref() else {
        return Ok(None);
    };
    let api_profiles = api_profiles
        .iter()
        .map(|profile| (*profile).to_owned())
        .collect::<Vec<_>>();
    let resolved = if let (Some(api_key_id), Some(authz_version)) =
        (auth.api_key_id.as_deref(), auth.credential_authz_version)
    {
        store
            .resolve_api_key_surface_model(
                &auth.project_id,
                api_key_id,
                authz_version,
                operation_id,
                &api_profiles,
                requested_public_model_id,
            )
            .await?
    } else if auth.actor_user_id.is_some() {
        store
            .resolve_console_surface_model(
                &auth.project_id,
                operation_id,
                &api_profiles,
                requested_public_model_id,
            )
            .await?
    } else {
        None
    };
    set_route_attribution(auth, resolved.as_ref());
    Ok(resolved)
}

pub(super) async fn filter_project_models(
    state: &Arc<AppState>,
    project_id: &str,
    models: Vec<crate::model_routing::PublicModelRoute>,
) -> Result<Vec<crate::model_routing::PublicModelRoute>, ImageGatewayError> {
    let Some(service) = state.project_model_policy_service.as_ref() else {
        return Ok(models);
    };
    let policy = service.get_policy(project_id, models.clone()).await?;
    let allowed = policy
        .models
        .into_iter()
        .filter(|model| model.allowed)
        .map(|model| {
            (
                model.model.operation_id,
                model.model.api_profile,
                model.model.public_model_id,
                model.model.media_kind,
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    Ok(models
        .into_iter()
        .filter(|model| {
            allowed.contains(&(
                model.operation_id.clone(),
                model.api_profile.clone(),
                model.id.clone(),
                model.media_kind.clone(),
            ))
        })
        .collect())
}

fn set_route_attribution(auth: &mut AuthContext, route: Option<&ResolvedModelRoute>) {
    auth.route = route.map(|route| RequestRouteAttribution {
        public_model_id: route.public_model_id.clone(),
        api_profile: route.api_profile.clone(),
        provider_id: route.provider_id.clone(),
        operation_id: route.operation_id.clone(),
        command_schema: route.command_schema.clone(),
        media_kind: route.media_kind.clone(),
        route_id: route.route_id,
        route_revision: route.route_revision,
    });
}

pub(super) fn usage_limits(config: &AppConfig) -> UsageLimits {
    UsageLimits {
        five_hour_image_limit: config.five_hour_image_limit,
        seven_day_image_limit: config.seven_day_image_limit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_storage_validation_rejects_distinct_backend_instances() {
        let admission: Arc<dyn AdmissionStore> = Arc::new(InMemoryAdmissionStore::default());
        let usage: Arc<dyn UsageStore> = Arc::new(InMemoryUsageStore::default());
        let expected = Arc::new(InMemoryArtifactBlobStore::default());
        let settlement =
            SequentialExecutionSettlementStore::new(admission, usage, expected.clone());
        let other = Arc::new(InMemoryArtifactBlobStore::default());

        assert!(
            validate_component_storage(expected.as_ref(), expected.as_ref(), &settlement).is_ok()
        );
        assert!(validate_component_storage(other.as_ref(), other.as_ref(), &settlement).is_err());
    }
}
