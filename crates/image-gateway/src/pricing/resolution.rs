use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use super::PriceBookVersionView;

#[derive(Clone, Debug, Deserialize, ToSchema, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PriceResolutionRequest {
    pub purpose: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub provider_id: Option<String>,
    pub currency: String,
    pub api_profile: String,
    pub operation: String,
    pub provider_model_id: Option<String>,
    pub public_model_id: String,
    pub media_kind: String,
    pub service_tier: String,
    pub execution_surface: String,
    pub billing_mode: String,
    pub at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResolvedPriceVersion {
    pub price_book_id: Uuid,
    pub price_book_key: String,
    pub purpose: String,
    pub scope_type: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub provider_id: Option<String>,
    pub currency: String,
    pub version: PriceBookVersionView,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PriceResolutionError {
    #[error("price resolution request is invalid")]
    InvalidRequest,
    #[error("no published price version matches the request")]
    NotFound,
    #[error("more than one published price version has equal precedence")]
    Ambiguous,
    #[error("pricing state is unavailable")]
    StoreUnavailable,
}

#[async_trait]
pub trait PriceResolver: Send + Sync + 'static {
    async fn resolve_price_version(
        &self,
        request: &PriceResolutionRequest,
    ) -> Result<ResolvedPriceVersion, PriceResolutionError>;
}
