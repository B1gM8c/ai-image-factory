use async_trait::async_trait;
use serde::Serialize;
use uuid::Uuid;

use crate::ImageGatewayError;

mod postgres;

pub use postgres::PostgresModelRoutingStore;

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct PublicModelRoute {
    pub id: String,
    #[serde(skip_serializing)]
    pub provider_model_id: Option<String>,
    pub api_profile: String,
    pub provider_id: String,
    pub operation_id: String,
    pub media_kind: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct ResolvedModelRoute {
    pub public_model_id: String,
    pub api_profile: String,
    pub provider_id: String,
    pub operation_id: String,
    pub command_schema: String,
    pub provider_model_id: String,
    pub execution_model_id: String,
    pub media_kind: String,
    pub route_id: Uuid,
    pub route_revision: i64,
}

#[async_trait]
pub trait ModelRoutingStore: Send + Sync + 'static {
    async fn list_api_key_models(
        &self,
        project_id: &str,
        api_key_id: &str,
        credential_authz_version: i64,
    ) -> Result<Vec<PublicModelRoute>, ImageGatewayError>;

    #[allow(clippy::too_many_arguments)]
    async fn resolve_api_key_model(
        &self,
        project_id: &str,
        api_key_id: &str,
        credential_authz_version: i64,
        provider_id: &str,
        operation_id: &str,
        api_profile: &str,
        requested_public_model_id: Option<&str>,
        default_provider_model_id: &str,
    ) -> Result<Option<ResolvedModelRoute>, ImageGatewayError>;

    async fn resolve_api_key_surface_model(
        &self,
        project_id: &str,
        api_key_id: &str,
        credential_authz_version: i64,
        operation_id: &str,
        api_profiles: &[String],
        requested_public_model_id: &str,
    ) -> Result<Option<ResolvedModelRoute>, ImageGatewayError>;

    async fn list_console_models(
        &self,
        project_id: &str,
        operation_id: &str,
    ) -> Result<Vec<PublicModelRoute>, ImageGatewayError>;

    #[allow(clippy::too_many_arguments)]
    async fn resolve_console_model(
        &self,
        project_id: &str,
        provider_id: &str,
        operation_id: &str,
        api_profile: &str,
        requested_public_model_id: Option<&str>,
        default_provider_model_id: &str,
    ) -> Result<Option<ResolvedModelRoute>, ImageGatewayError>;

    async fn resolve_console_surface_model(
        &self,
        project_id: &str,
        operation_id: &str,
        api_profiles: &[String],
        requested_public_model_id: &str,
    ) -> Result<Option<ResolvedModelRoute>, ImageGatewayError>;
}
