use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ImageGatewayError;

pub(crate) mod admission;
mod coverage;
pub(crate) mod customer_usage;
pub(crate) mod inline_settlement;
mod official_catalog;
pub(crate) mod official_metering;
mod postgres;
#[allow(dead_code)]
pub(crate) mod postgres_quote;
#[allow(dead_code)]
pub(crate) mod postgres_rating;
#[allow(dead_code)]
pub(crate) mod provider_cost;
mod quote;
mod rating;
mod readiness;
mod resolution;
mod surface_contract;

pub use official_catalog::{
    ApplyOfficialPriceSnapshotRequest, OfficialPriceCatalogDescriptor, OfficialPriceCatalogs,
    OfficialPriceComponentDiffView, OfficialPriceSnapshotApplicationView,
    OfficialPriceSnapshotDiffView, OfficialPriceSnapshotPreview, OfficialPriceSnapshotSummary,
    OfficialPriceSyncRunSummary,
};
pub use postgres::PostgresPricingAdminService;
pub(crate) use postgres::resolve_provider_actual_price_version_in_transaction;
pub use quote::{
    FrozenQuoteLine, FrozenQuotePlan, FrozenRatedLine, FrozenRatingPlan, QuoteError, QuoteQuantity,
    QuoteRateAdjustment, plan_customer_quote, plan_customer_quote_with_adjustment,
    rate_frozen_customer_quote,
};
pub use rating::{
    LedgerMoneyConversion, ProviderReportedCostAggregate, RatedLine, RatingError, RatingResult,
    UsageFact, aggregate_provider_reported_cost, rate_usage, usd_ticks_to_ledger_micros,
};
pub use resolution::{
    PriceResolutionError, PriceResolutionRequest, PriceResolver, ResolvedPriceVersion,
};

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreatePriceBookRequest {
    pub price_book_key: String,
    pub display_name: String,
    pub purpose: String,
    pub scope_type: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub provider_id: Option<String>,
    pub currency: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PriceComponentDraft {
    pub component_key: String,
    pub metric: String,
    pub unit: String,
    pub unit_size: String,
    pub unit_price_micros: String,
    pub outcome: String,
    pub quantity_source: String,
    #[serde(default = "default_required_confidence")]
    pub required_confidence: String,
    pub rounding_mode: String,
    #[serde(default = "empty_object")]
    pub dimensions: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PriceBookVersionDraft {
    pub api_profile: String,
    pub operation: String,
    pub provider_id: Option<String>,
    pub provider_model_id: Option<String>,
    pub public_model_id: String,
    pub media_kind: String,
    #[serde(default = "default_service_tier")]
    pub service_tier: String,
    pub execution_surface: String,
    pub billing_mode: String,
    #[serde(default)]
    pub is_free: bool,
    pub effective_from_ms: i64,
    pub source_kind: String,
    pub source_url: Option<String>,
    pub source_checked_at_ms: Option<i64>,
    pub notes: Option<String>,
    #[serde(default)]
    pub components: Vec<PriceComponentDraft>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreatePriceBookVersionRequest {
    #[serde(flatten)]
    pub draft: PriceBookVersionDraft,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdatePriceBookVersionRequest {
    pub expected_control_version: i64,
    #[serde(flatten)]
    pub draft: PriceBookVersionDraft,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TransitionPriceBookVersionRequest {
    pub expected_control_version: i64,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreatePriceRollbackDraftRequest {
    pub effective_from_ms: i64,
}

#[derive(Clone, Copy, Debug)]
pub struct PricingTransitionActor {
    pub user_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct PriceComponentView {
    #[schema(value_type = String)]
    pub price_component_id: Uuid,
    pub component_key: String,
    pub metric: String,
    pub unit: String,
    pub unit_size: String,
    pub unit_price_micros: String,
    pub outcome: String,
    pub quantity_source: String,
    pub required_confidence: String,
    pub rounding_mode: String,
    pub dimensions: Value,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct PriceBookVersionView {
    #[schema(value_type = String)]
    pub price_book_version_id: Uuid,
    #[schema(value_type = String)]
    pub price_book_id: Uuid,
    pub version: i32,
    pub api_profile: String,
    pub operation: String,
    pub provider_id: Option<String>,
    pub provider_model_id: Option<String>,
    pub public_model_id: String,
    pub media_kind: String,
    pub service_tier: String,
    pub execution_surface: String,
    pub billing_mode: String,
    pub is_free: bool,
    pub state: String,
    pub effective_from_ms: i64,
    pub effective_until_ms: Option<i64>,
    pub source_kind: String,
    pub source_url: Option<String>,
    pub source_checked_at_ms: Option<i64>,
    pub notes: Option<String>,
    pub control_version: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub components: Vec<PriceComponentView>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct PriceRollbackDraftResult {
    #[schema(value_type = String)]
    pub source_version_id: Uuid,
    pub draft: PriceBookVersionView,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct PriceBookView {
    #[schema(value_type = String)]
    pub price_book_id: Uuid,
    pub price_book_key: String,
    pub display_name: String,
    pub purpose: String,
    pub scope_type: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub provider_id: Option<String>,
    pub currency: String,
    pub state: String,
    pub control_version: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub versions: Vec<PriceBookVersionView>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct PriceBookCatalog {
    pub as_of_ms: i64,
    pub price_books: Vec<PriceBookView>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct PricingCoverageSummary {
    pub surfaces: i64,
    pub routable_surfaces: i64,
    pub sale_priced_surfaces: i64,
    pub actual_cost_surfaces: i64,
    pub benchmark_only_surfaces: i64,
    pub blocked_surfaces: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct PricingCoverageRow {
    pub provider_id: String,
    pub provider_display_name: String,
    pub provider_model_id: String,
    pub provider_model_display_name: String,
    pub public_model_id: Option<String>,
    pub api_profile: Option<String>,
    pub operation: String,
    pub pricing_operation: Option<String>,
    pub pricing_dimensions: Vec<String>,
    pub customer_metering_bases: Vec<PricingMeteringBasis>,
    pub media_kind: String,
    pub route_status: String,
    pub routable_account_count: i64,
    pub customer_price_status: String,
    pub customer_price_currencies: Vec<String>,
    pub metering_status: String,
    pub provider_cost_status: String,
    pub provider_cost_currencies: Vec<String>,
    pub source_status: String,
    pub readiness: String,
    pub blocking_reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct PricingMeteringBasis {
    pub metric: String,
    pub unit: String,
    pub quantity_source: String,
    pub confidence: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct PricePublishReadiness {
    #[schema(value_type = String)]
    pub price_book_version_id: Uuid,
    #[schema(value_type = String)]
    pub price_book_id: Uuid,
    pub purpose: String,
    pub ready: bool,
    pub matching_surface_count: i64,
    pub metering_status: String,
    pub request_dimensions: Vec<String>,
    pub blocking_reasons: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct PricingCoverageSnapshot {
    pub as_of_ms: i64,
    pub scope: String,
    pub summary: PricingCoverageSummary,
    pub rows: Vec<PricingCoverageRow>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PricePreviewRequest {
    pub resolution: PriceResolutionRequest,
    pub usage_facts: Vec<UsageFact>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct PricePreviewResult {
    #[schema(value_type = String)]
    pub price_book_version_id: Uuid,
    pub purpose: String,
    pub is_simulation: bool,
    pub billing_mode: String,
    pub currency: String,
    pub fact_set_hash: String,
    pub total_amount_micros: Option<String>,
    pub native_cost: Option<ProviderReportedCostAggregate>,
    pub ledger_conversion: Option<LedgerMoneyConversion>,
    pub lines: Vec<RatedLine>,
}

#[async_trait]
pub trait PricingAdminService: Send + Sync + 'static {
    async fn catalog(&self) -> Result<PriceBookCatalog, ImageGatewayError>;

    async fn coverage(&self) -> Result<PricingCoverageSnapshot, ImageGatewayError>;

    async fn publish_readiness(
        &self,
        price_book_version_id: Uuid,
    ) -> Result<PricePublishReadiness, ImageGatewayError>;

    async fn publish_readiness_as(
        &self,
        price_book_version_id: Uuid,
        actor: PricingTransitionActor,
    ) -> Result<PricePublishReadiness, ImageGatewayError>;

    async fn create_price_book(
        &self,
        request: CreatePriceBookRequest,
    ) -> Result<PriceBookView, ImageGatewayError>;

    async fn create_version(
        &self,
        price_book_id: Uuid,
        request: CreatePriceBookVersionRequest,
    ) -> Result<PriceBookVersionView, ImageGatewayError>;

    async fn update_draft_version(
        &self,
        price_book_version_id: Uuid,
        request: UpdatePriceBookVersionRequest,
    ) -> Result<PriceBookVersionView, ImageGatewayError>;

    async fn publish_version(
        &self,
        price_book_version_id: Uuid,
        request: TransitionPriceBookVersionRequest,
    ) -> Result<PriceBookVersionView, ImageGatewayError>;

    async fn publish_version_as(
        &self,
        price_book_version_id: Uuid,
        request: TransitionPriceBookVersionRequest,
        actor: PricingTransitionActor,
    ) -> Result<PriceBookVersionView, ImageGatewayError>;

    async fn retire_version(
        &self,
        price_book_version_id: Uuid,
        request: TransitionPriceBookVersionRequest,
    ) -> Result<PriceBookVersionView, ImageGatewayError>;

    async fn retire_version_as(
        &self,
        price_book_version_id: Uuid,
        request: TransitionPriceBookVersionRequest,
        actor: PricingTransitionActor,
    ) -> Result<PriceBookVersionView, ImageGatewayError>;

    async fn create_rollback_draft(
        &self,
        source_version_id: Uuid,
        request: CreatePriceRollbackDraftRequest,
        actor: PricingTransitionActor,
    ) -> Result<PriceRollbackDraftResult, ImageGatewayError>;

    async fn preview(
        &self,
        request: PricePreviewRequest,
    ) -> Result<PricePreviewResult, ImageGatewayError>;

    async fn official_catalogs(&self) -> Result<OfficialPriceCatalogs, ImageGatewayError>;

    async fn observe_official_catalog(
        &self,
        catalog_key: &str,
        actor_user_id: Uuid,
        actor_session_id: Uuid,
    ) -> Result<OfficialPriceSnapshotPreview, ImageGatewayError>;

    async fn apply_official_snapshot(
        &self,
        snapshot_id: Uuid,
        request: ApplyOfficialPriceSnapshotRequest,
        actor_user_id: Uuid,
        actor_session_id: Uuid,
    ) -> Result<OfficialPriceSnapshotPreview, ImageGatewayError>;
}

fn empty_object() -> Value {
    serde_json::json!({})
}

fn default_service_tier() -> String {
    "standard".to_string()
}

fn default_required_confidence() -> String {
    "exact".to_string()
}
