use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ImageGatewayError;

mod codex_app_server;
mod dreamina_login;
mod grok_billing;
mod grok_login;
mod grok_video_output;
mod model_catalog;
mod postgres;
mod route_reconciliation;

pub(crate) use codex_app_server::{CodexAppServer, resolve_executable as resolve_codex_executable};
pub use model_catalog::{
    ProviderAccountModelView, ProviderAccountModelsView, ProviderModelRefreshView,
    ProviderModelView, ProviderModelsSnapshot,
};
pub use postgres::PostgresProviderManagementService;
pub use route_reconciliation::{
    ExecutionProfileRouteReconciliationReport, reconcile_execution_profile_routes,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartCodexLoginRequest {
    pub display_name: String,
    #[serde(default)]
    pub provider_account_id: Option<Uuid>,
    #[serde(default)]
    pub login_method: CodexLoginMethod,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: i32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartProviderLoginRequest {
    pub provider_id: String,
    pub display_name: String,
    #[serde(default)]
    pub operation_ids: Vec<String>,
    #[serde(default)]
    pub provider_account_id: Option<Uuid>,
    #[serde(default)]
    pub login_method: CodexLoginMethod,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: i32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartProviderReauthorizationRequest {
    #[serde(default)]
    pub login_method: CodexLoginMethod,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexLoginMethod {
    #[default]
    BrowserOauth,
    DeviceCode,
}

impl CodexLoginMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BrowserOauth => "browser_oauth",
            Self::DeviceCode => "device_code",
        }
    }

    fn from_database(value: &str) -> Result<Self, ImageGatewayError> {
        match value {
            "browser_oauth" => Ok(Self::BrowserOauth),
            "device_code" => Ok(Self::DeviceCode),
            _ => Err(ImageGatewayError::service_unavailable(
                "Provider login session is invalid",
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderLoginSession {
    pub login_session_id: Uuid,
    pub provider_id: String,
    pub account_key: String,
    pub display_name: String,
    pub status: String,
    pub login_method: CodexLoginMethod,
    pub authorization_url: Option<String>,
    pub user_code: Option<String>,
    pub provider_account_id: Option<Uuid>,
    pub error_code: Option<String>,
    pub expires_at_ms: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ManagedCliProviderCapability {
    pub provider_id: String,
    pub display_name: String,
    pub availability: String,
    pub unavailable_reason: Option<String>,
    pub login_methods: Vec<CodexLoginMethod>,
    pub operation_ids: Vec<String>,
    pub quota_kind: String,
    pub executable_version: Option<String>,
    pub max_concurrency_limit: i32,
}

#[derive(Clone, Debug, Serialize)]
pub struct ManagedCliProvidersSnapshot {
    pub providers: Vec<ManagedCliProviderCapability>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateProviderRouteRequest {
    pub route_key: String,
    pub display_name: String,
    pub provider_id: String,
    pub operation_id: String,
    #[serde(default = "default_selection_strategy")]
    pub selection_strategy: String,
    #[serde(default = "default_quota_freshness_ms")]
    pub quota_freshness_ms: i64,
    #[serde(default = "default_unknown_quota_policy")]
    pub unknown_quota_policy: String,
    pub members: Vec<CreateProviderRouteMemberRequest>,
    #[serde(default)]
    pub model_mappings: Option<Vec<ProviderRouteModelMappingRequest>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateProviderRouteMemberRequest {
    pub provider_account_id: Uuid,
    #[serde(default)]
    pub priority: i16,
    #[serde(default = "default_member_weight")]
    pub weight: i32,
    #[serde(default)]
    pub minimum_remaining_percent: i16,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateProviderAccountSchedulingRequest {
    pub expected_control_version: i64,
    pub max_concurrency: i32,
    pub accepting_new_work: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderAccountSchedulingView {
    pub provider_account_id: Uuid,
    pub max_concurrency: i32,
    pub allocated_count: i32,
    pub accepting_new_work: bool,
    pub scheduling_state: String,
    pub control_version: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateGrokVideoOutputRequest {
    pub enabled: bool,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default = "default_grok_upload_url_expiry_secs")]
    pub expires_secs: i64,
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct GrokVideoOutputView {
    pub provider_account_id: Uuid,
    pub enabled: bool,
    pub ready: bool,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub key_prefix: Option<String>,
    pub expires_secs: Option<i64>,
    pub has_read_write_credentials: bool,
    pub has_read_only_credentials: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateProviderRouteRequest {
    pub expected_revision: i64,
    pub display_name: String,
    #[serde(default = "default_selection_strategy")]
    pub selection_strategy: String,
    #[serde(default = "default_quota_freshness_ms")]
    pub quota_freshness_ms: i64,
    #[serde(default = "default_unknown_quota_policy")]
    pub unknown_quota_policy: String,
    pub members: Vec<CreateProviderRouteMemberRequest>,
    #[serde(default)]
    pub model_mappings: Option<Vec<ProviderRouteModelMappingRequest>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRouteModelMappingRequest {
    pub api_profile: String,
    pub public_model_id: String,
    pub provider_model_id: String,
    pub media_kind: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderRouteMemberView {
    pub provider_account_id: Uuid,
    pub account_key: String,
    pub execution_profile_id: Uuid,
    pub priority: i16,
    pub weight: i32,
    pub minimum_remaining_percent: i16,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderRouteModelMappingView {
    pub api_profile: String,
    pub public_model_id: String,
    pub provider_model_id: String,
    pub execution_model_id: String,
    pub provider_model_display_name: String,
    pub media_kind: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderRouteView {
    pub route_id: Uuid,
    pub revision: i64,
    pub route_key: String,
    pub display_name: String,
    pub provider_id: String,
    pub operation_id: String,
    pub command_schema: String,
    pub route_kind: String,
    pub selection_strategy: String,
    pub quota_freshness_ms: i64,
    pub unknown_quota_policy: String,
    pub state: String,
    pub members: Vec<ProviderRouteMemberView>,
    pub model_mappings: Vec<ProviderRouteModelMappingView>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderRoutesSnapshot {
    pub as_of_ms: i64,
    pub routes: Vec<ProviderRouteView>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindApiKeyRouteRequest {
    pub route_id: Uuid,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateProviderAccountModelsRequest {
    pub expected_version: i64,
    pub mode: String,
    #[serde(default)]
    pub enabled_models: Vec<ProviderAccountModelSelection>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateProviderAccountModelConfigurationRequest {
    pub expected_model_version: i64,
    pub mode: String,
    #[serde(default)]
    pub enabled_models: Vec<ProviderAccountModelSelection>,
    pub route_id: Uuid,
    pub expected_route_revision: i64,
    pub model_mappings: Vec<ProviderRouteModelMappingRequest>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderAccountModelConfigurationView {
    pub provider_account_id: Uuid,
    pub model_version: i64,
    pub route_id: Uuid,
    pub route_revision: i64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAccountModelSelection {
    pub model_id: String,
    pub media_kind: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ApiKeyRouteBindingView {
    pub api_key_id: String,
    pub project_id: String,
    pub provider_id: String,
    pub operation_id: String,
    pub command_schema: String,
    pub route_id: Uuid,
    pub route_revision: i64,
    pub route_name: String,
    pub bound_at_ms: i64,
}

#[async_trait]
pub trait ProviderManagementService: Send + Sync + 'static {
    async fn managed_cli_providers(&self)
    -> Result<ManagedCliProvidersSnapshot, ImageGatewayError>;

    async fn provider_models(&self) -> Result<ProviderModelsSnapshot, ImageGatewayError>;

    async fn start_provider_model_refresh(
        &self,
        provider_account_id: Uuid,
    ) -> Result<ProviderModelRefreshView, ImageGatewayError>;

    async fn provider_model_refresh(
        &self,
        refresh_id: Uuid,
    ) -> Result<ProviderModelRefreshView, ImageGatewayError>;

    async fn provider_account_models(
        &self,
        provider_account_id: Uuid,
    ) -> Result<ProviderAccountModelsView, ImageGatewayError>;

    async fn update_provider_account_models(
        &self,
        provider_account_id: Uuid,
        request: UpdateProviderAccountModelsRequest,
    ) -> Result<ProviderAccountModelsView, ImageGatewayError>;

    async fn update_provider_account_model_configuration(
        &self,
        provider_account_id: Uuid,
        request: UpdateProviderAccountModelConfigurationRequest,
    ) -> Result<ProviderAccountModelConfigurationView, ImageGatewayError>;

    async fn start_provider_login(
        &self,
        request: StartProviderLoginRequest,
    ) -> Result<ProviderLoginSession, ImageGatewayError>;

    async fn start_provider_reauthorization(
        &self,
        provider_account_id: Uuid,
        request: StartProviderReauthorizationRequest,
    ) -> Result<ProviderLoginSession, ImageGatewayError>;

    async fn start_codex_login(
        &self,
        request: StartCodexLoginRequest,
    ) -> Result<ProviderLoginSession, ImageGatewayError>;

    async fn login_session(
        &self,
        login_session_id: Uuid,
    ) -> Result<ProviderLoginSession, ImageGatewayError>;

    async fn refresh_codex_quota(&self, provider_account_id: Uuid)
    -> Result<(), ImageGatewayError>;

    async fn refresh_provider_quota(
        &self,
        provider_account_id: Uuid,
    ) -> Result<(), ImageGatewayError>;

    async fn update_account_scheduling(
        &self,
        provider_account_id: Uuid,
        request: UpdateProviderAccountSchedulingRequest,
    ) -> Result<ProviderAccountSchedulingView, ImageGatewayError>;

    async fn grok_video_output(
        &self,
        provider_account_id: Uuid,
    ) -> Result<GrokVideoOutputView, ImageGatewayError>;

    async fn update_grok_video_output(
        &self,
        provider_account_id: Uuid,
        request: UpdateGrokVideoOutputRequest,
    ) -> Result<GrokVideoOutputView, ImageGatewayError>;

    async fn list_routes(&self) -> Result<ProviderRoutesSnapshot, ImageGatewayError>;

    async fn create_route(
        &self,
        request: CreateProviderRouteRequest,
    ) -> Result<ProviderRouteView, ImageGatewayError>;

    async fn update_route(
        &self,
        route_id: Uuid,
        request: UpdateProviderRouteRequest,
    ) -> Result<ProviderRouteView, ImageGatewayError>;

    async fn bind_api_key_route(
        &self,
        project_id: &str,
        api_key_id: &str,
        route_id: Uuid,
    ) -> Result<ApiKeyRouteBindingView, ImageGatewayError>;

    async fn api_key_route(
        &self,
        project_id: &str,
        api_key_id: &str,
    ) -> Result<Option<ApiKeyRouteBindingView>, ImageGatewayError>;
}

fn default_max_concurrency() -> i32 {
    1
}

fn default_selection_strategy() -> String {
    "quota_aware_least_loaded".to_string()
}

fn default_quota_freshness_ms() -> i64 {
    900_000
}

fn default_unknown_quota_policy() -> String {
    "allow".to_string()
}

fn default_member_weight() -> i32 {
    100
}

fn default_grok_upload_url_expiry_secs() -> i64 {
    900
}
