use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

pub mod postgres;
mod runtime_events;
mod usage_analysis;

pub use postgres::PostgresAdminReadStore;
pub use runtime_events::ProviderAccountRuntimeEventHub;

pub const MAX_OVERVIEW_WINDOW_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
pub const MAX_BILLING_WINDOW_MS: i64 = 31 * 24 * 60 * 60 * 1_000;
pub const MAX_SCHEDULER_WINDOW_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
pub const MAX_JOBS_WINDOW_MS: i64 = 31 * 24 * 60 * 60 * 1_000;
pub const MAX_JOBS_PAGE_SIZE: u32 = 100;
pub const MAX_REQUEST_LOG_WINDOW_MS: i64 = 90 * 24 * 60 * 60 * 1_000;
pub const MAX_AUDIT_LOG_WINDOW_MS: i64 = 365 * 24 * 60 * 60 * 1_000;
pub const MAX_AUDIT_LOG_PAGE_SIZE: u32 = 100;
pub const MAX_USAGE_SERIES_ROWS: usize = 5_000;

#[derive(Debug, thiserror::Error)]
pub enum AdminReadError {
    #[error("invalid admin read query: {0}")]
    InvalidQuery(String),
    #[error("admin read resource was not found")]
    NotFound,
    #[error("admin read store is unavailable")]
    Unavailable,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StateCount {
    pub state: String,
    pub count: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct UsageAggregate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub billing_metric: String,
    pub billing_unit: String,
    pub outcome: String,
    pub quantity: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct LedgerAggregate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub transaction_type: String,
    pub currency: String,
    pub amount_micros: String,
    pub transaction_count: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct OverviewSnapshot {
    pub as_of_ms: i64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub job_states: Vec<StateCount>,
    pub charged_usage: Vec<UsageAggregate>,
    pub sealed_ledger: Vec<LedgerAggregate>,
    pub terminal_job_elapsed_p95_ms: Option<i64>,
    pub terminal_job_elapsed_samples: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct BillingAccountSnapshot {
    pub tenant_id: String,
    pub currency: String,
    pub credit_limit_micros: String,
    pub held_micros: String,
    pub captured_micros: String,
    pub refunded_micros: String,
    pub available_micros: String,
    pub control_version: String,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct RatedUsageAggregate {
    pub tenant_id: String,
    pub billing_metric: String,
    pub billing_unit: String,
    pub outcome: String,
    pub currency: String,
    pub quantity: String,
    pub amount_micros: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProviderCostAggregate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub provider_id: String,
    pub outcome: String,
    pub cost_basis: String,
    pub attribution_state: String,
    pub currency: String,
    pub amount_micros: String,
    pub transaction_count: String,
    pub linked_receipts: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProviderCostCoverage {
    pub terminal_receipts: String,
    pub covered_receipts: String,
    pub uncovered_receipts: String,
    pub provider_actual_transactions: String,
    pub provider_allocated_transactions: String,
    pub legacy_unverified_transactions: String,
    pub unattributed_transactions: String,
    pub authority_conflicts: String,
}

impl ProviderCostCoverage {
    fn empty() -> Self {
        Self {
            terminal_receipts: "0".to_string(),
            covered_receipts: "0".to_string(),
            uncovered_receipts: "0".to_string(),
            provider_actual_transactions: "0".to_string(),
            provider_allocated_transactions: "0".to_string(),
            legacy_unverified_transactions: "0".to_string(),
            unattributed_transactions: "0".to_string(),
            authority_conflicts: "0".to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct BillingSnapshot {
    pub as_of_ms: i64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub account_snapshots: Vec<BillingAccountSnapshot>,
    pub charged_usage: Vec<UsageAggregate>,
    pub rated_usage: Vec<RatedUsageAggregate>,
    pub sealed_ledger: Vec<LedgerAggregate>,
    pub provider_costs: Vec<ProviderCostAggregate>,
    pub provider_cost_coverage: ProviderCostCoverage,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ConsoleBillingSnapshot {
    pub as_of_ms: i64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub account_snapshots: Vec<BillingAccountSnapshot>,
    pub charged_usage: Vec<UsageAggregate>,
    pub rated_usage: Vec<RatedUsageAggregate>,
    pub sealed_ledger: Vec<LedgerAggregate>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub enum UsageInterval {
    Minute,
    Hour,
    Day,
}

impl UsageInterval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Minute => "1m",
            Self::Hour => "1h",
            Self::Day => "1d",
        }
    }

    pub fn as_millis(self) -> i64 {
        match self {
            Self::Minute => 60 * 1_000,
            Self::Hour => 60 * 60 * 1_000,
            Self::Day => 24 * 60 * 60 * 1_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub enum UsageGroupBy {
    None,
    LineItem,
    Project,
    ApiKey,
    User,
    Provider,
    Model,
    Operation,
    ServiceTier,
}

impl UsageGroupBy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::LineItem => "line_item",
            Self::Project => "project",
            Self::ApiKey => "api_key",
            Self::User => "user",
            Self::Provider => "provider",
            Self::Model => "model",
            Self::Operation => "operation",
            Self::ServiceTier => "service_tier",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageAnalysisQuery {
    pub window_ms: i64,
    pub interval: UsageInterval,
    pub group_by: UsageGroupBy,
    pub project_id: Option<String>,
    pub api_key_id: Option<String>,
    pub filter_user_id: Option<Uuid>,
    pub provider_id: Option<String>,
    pub model: Option<String>,
    pub operation: Option<String>,
    pub service_tier: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct UsageActivityPoint {
    pub bucket_start_ms: i64,
    pub group_kind: String,
    pub group_value: String,
    pub group_label: String,
    pub billing_metric: String,
    pub billing_unit: String,
    pub outcome: String,
    pub quantity: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct UsageSpendPoint {
    pub bucket_start_ms: i64,
    pub group_kind: String,
    pub group_value: String,
    pub group_label: String,
    pub billing_metric: String,
    pub billing_unit: String,
    pub outcome: String,
    pub currency: String,
    pub quantity: String,
    pub amount_micros: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct UsageFilterOption {
    pub value: String,
    pub label: String,
}

#[derive(Clone, Debug, Default, Serialize, ToSchema, PartialEq, Eq)]
pub struct UsageFilterOptions {
    pub projects: Vec<UsageFilterOption>,
    pub api_keys: Vec<UsageFilterOption>,
    pub users: Vec<UsageFilterOption>,
    pub providers: Vec<UsageFilterOption>,
    pub models: Vec<UsageFilterOption>,
    pub operations: Vec<UsageFilterOption>,
    pub service_tiers: Vec<UsageFilterOption>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct UsageAnalysisSnapshot {
    pub as_of_ms: i64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub interval: String,
    pub interval_ms: i64,
    pub group_by: String,
    pub activity: Vec<UsageActivityPoint>,
    pub spend: Vec<UsageSpendPoint>,
    pub filter_options: UsageFilterOptions,
}

impl From<BillingSnapshot> for ConsoleBillingSnapshot {
    fn from(snapshot: BillingSnapshot) -> Self {
        Self {
            as_of_ms: snapshot.as_of_ms,
            from_ms: snapshot.from_ms,
            to_ms: snapshot.to_ms,
            account_snapshots: snapshot.account_snapshots,
            charged_usage: snapshot.charged_usage,
            rated_usage: snapshot.rated_usage,
            sealed_ledger: snapshot
                .sealed_ledger
                .into_iter()
                .filter(|item| {
                    matches!(
                        item.transaction_type.as_str(),
                        "customer_charge" | "customer_job_charge" | "customer_refund"
                    )
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct UpstreamQuotaWindow {
    pub limit_id: String,
    pub limit_name: Option<String>,
    pub window_role: String,
    pub window_duration_mins: Option<i64>,
    pub used_percent: i32,
    pub resets_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct UpstreamQuotaObservation {
    pub status: String,
    pub limit: Option<String>,
    pub remaining: Option<String>,
    pub reset_at_ms: Option<i64>,
    pub observed_at_ms: Option<i64>,
    pub plan_type: Option<String>,
    pub credits_balance: Option<String>,
    pub credits_unlimited: Option<bool>,
    pub windows: Vec<UpstreamQuotaWindow>,
}

impl UpstreamQuotaObservation {
    fn unknown() -> Self {
        Self {
            status: "unknown".to_string(),
            limit: None,
            remaining: None,
            reset_at_ms: None,
            observed_at_ms: None,
            plan_type: None,
            credits_balance: None,
            credits_unlimited: None,
            windows: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProviderAccountView {
    pub provider_account_id: String,
    pub account_key: String,
    pub provider_id: String,
    pub display_name: Option<String>,
    pub account_email: Option<String>,
    pub environment_state: Option<String>,
    pub account_state: String,
    pub credential_pool_state: String,
    pub credential_lifecycle_state: String,
    pub credential_refresh_strategy: String,
    pub operational_credential_revision: i64,
    pub credential_access_expires_at_ms: Option<i64>,
    pub credential_next_refresh_at_ms: Option<i64>,
    pub credential_last_success_at_ms: Option<i64>,
    pub credential_consecutive_failures: i32,
    pub credential_last_error_code: Option<String>,
    pub execution_profile_id: String,
    pub profile_key: String,
    pub operation_id: String,
    pub completion_mode: String,
    pub profile_state: String,
    pub resource_policy_state: String,
    pub scheduling_state: String,
    pub control_version: i64,
    pub configuration_status: String,
    pub runtime_status: String,
    pub max_concurrency: String,
    pub allocated_count: String,
    pub available_capacity: String,
    pub active_submitters: String,
    pub active_pollers: String,
    pub draining_submitters: String,
    pub draining_pollers: String,
    pub upstream_quota: UpstreamQuotaObservation,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProviderAccountsSnapshot {
    pub as_of_ms: i64,
    pub accounts: Vec<ProviderAccountView>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProviderAccountConcurrency {
    pub provider_account_id: String,
    pub max_concurrency: String,
    pub allocated_count: String,
    pub available_capacity: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProviderQueuePressure {
    pub queued_work_items: String,
    pub pending_batch_requests: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProviderAccountConcurrencySnapshot {
    pub as_of_ms: i64,
    pub accounts: Vec<ProviderAccountConcurrency>,
    pub queue: ProviderQueuePressure,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProviderAccountRuntimeEvent {
    pub kind: String,
    pub sequence: u64,
    pub as_of_ms: i64,
    pub accounts: Vec<ProviderAccountConcurrency>,
    pub queue: ProviderQueuePressure,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct WorkStateCount {
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_timing: Option<String>,
    pub count: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct StageCount {
    pub stage: String,
    pub count: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ProviderStateCount {
    pub stage: String,
    pub state: String,
    pub count: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct SchedulerCapacity {
    pub provider_account_id: String,
    pub account_key: String,
    pub provider_id: String,
    pub display_name: Option<String>,
    pub account_email: Option<String>,
    pub max_concurrency: String,
    pub allocated_count: String,
    pub available_capacity: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct BlockedTerminalReduction {
    pub submission_id: String,
    pub executor_execution_id: String,
    pub job_id: String,
    pub request_id: String,
    pub provider_id: String,
    pub model: String,
    pub resolved_state: String,
    pub error_code: String,
    pub blocked_at_ms: i64,
    pub blocked_by: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct SchedulerActiveJob {
    pub job_id: String,
    pub request_id: String,
    pub organization_id: Option<String>,
    pub organization_name: Option<String>,
    pub project_id: Option<String>,
    pub project_name: Option<String>,
    pub user_display_name: Option<String>,
    pub user_email: Option<String>,
    pub service_account_name: Option<String>,
    pub api_key_name: Option<String>,
    pub operation: String,
    pub provider_id: String,
    pub model: String,
    pub job_state: String,
    pub work_state: Option<String>,
    pub provider_account_id: Option<String>,
    pub provider_account_name: Option<String>,
    pub attempt_count: String,
    pub available_at_ms: Option<i64>,
    pub lease_expires_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct SchedulerSnapshot {
    pub as_of_ms: i64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub work_items: Vec<WorkStateCount>,
    pub expired_leases: String,
    pub provider_polls_due: String,
    pub pending_terminal_reductions: String,
    pub blocked_terminal_reductions: String,
    pub blocked_terminals: Vec<BlockedTerminalReduction>,
    pub active_jobs: Vec<SchedulerActiveJob>,
    pub capacity_reconciliations_due: String,
    pub artifact_retention_due: String,
    pub artifact_retention_deleting: String,
    pub artifact_retention_failures: String,
    pub recent_uncertain: Vec<StageCount>,
    pub capacity: Vec<SchedulerCapacity>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct JobCursor {
    pub created_at_ms: i64,
    #[schema(value_type = String)]
    pub job_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobsQuery {
    pub window_ms: i64,
    pub to_ms: Option<i64>,
    pub limit: u32,
    pub cursor: Option<JobCursor>,
    pub provider_id: Option<String>,
    pub state: Option<String>,
    pub operation: Option<String>,
    pub model: Option<String>,
    pub project_id: Option<String>,
    pub api_key_id: Option<String>,
    pub request_or_job_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct JobListItem {
    pub job_id: String,
    pub tenant_id: String,
    pub project_id: Option<String>,
    pub service_account_id: Option<String>,
    pub api_key_id: Option<String>,
    pub auth_kind: Option<String>,
    #[schema(value_type = Option<String>)]
    pub actor_user_id: Option<Uuid>,
    #[schema(value_type = Option<String>)]
    pub credential_owner_user_id: Option<Uuid>,
    pub request_id: String,
    pub operation: String,
    pub provider_id: String,
    pub model: String,
    pub job_state: String,
    pub work_state: Option<String>,
    pub provider_states: Vec<ProviderStateCount>,
    pub output_count: String,
    pub billable_units: String,
    pub billing_metric: String,
    pub billing_unit: String,
    pub charged_units: String,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub last_error_code: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct JobsSnapshot {
    pub as_of_ms: i64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub items: Vec<JobListItem>,
    pub next_cursor: Option<JobCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RequestLogCursor {
    pub created_at_ms: i64,
    pub request_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestLogVisibility {
    Mine,
    Project,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestLogsQuery {
    pub window_ms: i64,
    pub to_ms: Option<i64>,
    pub limit: u32,
    pub cursor: Option<RequestLogCursor>,
    pub visibility: RequestLogVisibility,
    pub source: Option<String>,
    pub status: Option<String>,
    pub provider_id: Option<String>,
    pub model: Option<String>,
    pub project_id: Option<String>,
    pub api_key_id: Option<String>,
    pub request_or_job_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct RequestLogItem {
    pub request_id: String,
    pub source: String,
    pub method: String,
    pub route_pattern: String,
    pub request_path: String,
    pub status_code: u16,
    pub duration_ms: i64,
    pub error_code: Option<String>,
    pub idempotency_key_digest: Option<String>,
    pub tenant_id: Option<String>,
    pub project_id: Option<String>,
    pub service_account_id: Option<String>,
    pub api_key_id: Option<String>,
    #[schema(value_type = Option<String>)]
    pub actor_user_id: Option<Uuid>,
    #[schema(value_type = Option<String>)]
    pub credential_owner_user_id: Option<Uuid>,
    pub auth_kind: Option<String>,
    pub content_captured: bool,
    pub job_id: Option<String>,
    pub operation: Option<String>,
    pub provider_id: Option<String>,
    pub model: Option<String>,
    pub job_state: Option<String>,
    pub work_state: Option<String>,
    pub output_count: Option<String>,
    pub billable_units: Option<String>,
    pub billing_unit: Option<String>,
    pub requested_service_tier: Option<String>,
    pub project_service_tier: Option<String>,
    pub effective_service_tier: Option<String>,
    pub service_tier_fallback_reason: Option<String>,
    pub created_at_ms: i64,
    pub completed_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct RequestLogsSnapshot {
    pub as_of_ms: i64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub items: Vec<RequestLogItem>,
    pub next_cursor: Option<RequestLogCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditLogsQuery {
    pub window_ms: i64,
    pub to_ms: Option<i64>,
    pub limit: u32,
    pub after: Option<Uuid>,
    pub event_type: Option<String>,
    pub outcome: Option<String>,
    pub actor_user_id: Option<Uuid>,
    pub project_id: Option<String>,
    pub resource_type: Option<String>,
    pub request_id: Option<String>,
    pub query: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct AuditLogActor {
    #[serde(rename = "type")]
    pub actor_type: String,
    #[schema(value_type = Option<String>)]
    pub user_id: Option<Uuid>,
    pub email: Option<String>,
    pub display_name: Option<String>,
    #[schema(value_type = Option<String>)]
    pub session_id: Option<Uuid>,
    pub ip_address: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct AuditLogProject {
    pub id: String,
    pub name: Option<String>,
    pub organization_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct AuditLogResource {
    #[serde(rename = "type")]
    pub resource_type: Option<String>,
    pub id: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct AuditLogItem {
    pub id: String,
    pub object: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub effective_at: i64,
    pub actor: AuditLogActor,
    pub project: Option<AuditLogProject>,
    pub resource: AuditLogResource,
    pub request_id: Option<String>,
    pub outcome: String,
    pub reason_code: Option<String>,
    pub details: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct AuditLogsSnapshot {
    pub object: String,
    pub as_of_ms: i64,
    pub from_ms: i64,
    pub to_ms: i64,
    pub data: Vec<AuditLogItem>,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
    pub has_more: bool,
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct JobQuoteLine {
    pub component_key: String,
    pub partition_key: String,
    pub terminal_outcome: String,
    pub metric: String,
    pub unit: String,
    pub unit_size: String,
    pub unit_price_micros: String,
    pub reservation_quantity_source: String,
    pub reservation_confidence: String,
    pub max_quantity: String,
    pub max_amount_micros: String,
    pub actual_quantity: Option<String>,
    pub actual_amount_micros: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct JobCustomerQuote {
    pub quote_id: String,
    pub price_book_version_id: String,
    pub public_model_id: String,
    pub media_kind: String,
    pub service_tier: String,
    pub currency: String,
    pub is_free: bool,
    pub max_total_micros: String,
    pub created_at_ms: i64,
    pub lines: Vec<JobQuoteLine>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct JobCustomerHold {
    pub state: String,
    pub currency: String,
    pub held_micros: String,
    pub captured_micros: String,
    pub released_micros: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct JobUsageFact {
    pub metric: String,
    pub quantity: String,
    pub unit: String,
    pub quantity_source: String,
    pub confidence: String,
    pub billing_partition_key: String,
    pub terminal_outcome: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct JobCustomerRating {
    pub currency: String,
    pub total_amount_micros: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct JobLedgerTransaction {
    pub transaction_id: String,
    pub transaction_type: String,
    pub currency: String,
    pub amount_micros: String,
    pub created_at_ms: i64,
    pub sealed_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct JobProviderCost {
    pub cost_id: String,
    pub cost_basis: String,
    pub attribution_state: String,
    pub currency: String,
    pub observed_amount_micros: String,
    pub attributed_amount_micros: Option<String>,
    pub authority: String,
    pub confidence: String,
    pub price_book_version_id: Option<String>,
    pub transaction_id: Option<String>,
    pub sealed_at_ms: Option<i64>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct JobEconomicsSnapshot {
    pub as_of_ms: i64,
    pub job_id: String,
    pub economics_contract_version: i16,
    pub economics_state: String,
    pub customer_quote: Option<JobCustomerQuote>,
    pub customer_hold: Option<JobCustomerHold>,
    pub usage_facts: Vec<JobUsageFact>,
    pub customer_rating: Option<JobCustomerRating>,
    pub ledger_transactions: Vec<JobLedgerTransaction>,
    pub provider_costs: Vec<JobProviderCost>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ConsoleJobEconomicsSnapshot {
    pub as_of_ms: i64,
    pub job_id: String,
    pub economics_contract_version: i16,
    pub economics_state: String,
    pub customer_quote: Option<JobCustomerQuote>,
    pub customer_hold: Option<JobCustomerHold>,
    pub usage_facts: Vec<JobUsageFact>,
    pub customer_rating: Option<JobCustomerRating>,
    pub ledger_transactions: Vec<JobLedgerTransaction>,
}

impl From<JobEconomicsSnapshot> for ConsoleJobEconomicsSnapshot {
    fn from(snapshot: JobEconomicsSnapshot) -> Self {
        Self {
            as_of_ms: snapshot.as_of_ms,
            job_id: snapshot.job_id,
            economics_contract_version: snapshot.economics_contract_version,
            economics_state: snapshot.economics_state,
            customer_quote: snapshot.customer_quote,
            customer_hold: snapshot.customer_hold,
            usage_facts: snapshot.usage_facts,
            customer_rating: snapshot.customer_rating,
            ledger_transactions: snapshot
                .ledger_transactions
                .into_iter()
                .filter(|transaction| {
                    matches!(
                        transaction.transaction_type.as_str(),
                        "customer_charge" | "customer_job_charge" | "customer_refund"
                    )
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdminReadScope {
    Platform,
    Tenants(Vec<String>),
    User {
        user_id: Uuid,
        tenant_ids: Vec<String>,
        project_ids: Vec<String>,
    },
}

impl AdminReadScope {
    pub fn tenant_ids(&self) -> Option<&[String]> {
        match self {
            Self::Platform => None,
            Self::Tenants(tenant_ids) => Some(tenant_ids),
            Self::User { tenant_ids, .. } => Some(tenant_ids),
        }
    }

    pub fn actor_user_id(&self) -> Option<Uuid> {
        match self {
            Self::User { user_id, .. } => Some(*user_id),
            Self::Platform | Self::Tenants(_) => None,
        }
    }

    pub fn actor_user_id_for_project(
        &self,
        project_id: Option<&str>,
    ) -> Result<Option<Uuid>, AdminReadError> {
        let Self::User {
            user_id,
            project_ids,
            ..
        } = self
        else {
            return Ok(None);
        };
        let Some(project_id) = project_id else {
            return Ok(Some(*user_id));
        };
        if !project_ids.iter().any(|allowed| allowed == project_id) {
            return Err(AdminReadError::NotFound);
        }
        Ok(Some(*user_id))
    }

    pub fn ensure_project_access(&self, project_id: &str) -> Result<(), AdminReadError> {
        match self {
            Self::Platform | Self::Tenants(_) => Ok(()),
            Self::User { project_ids, .. }
                if project_ids.iter().any(|allowed| allowed == project_id) =>
            {
                Ok(())
            }
            Self::User { .. } => Err(AdminReadError::NotFound),
        }
    }
}

#[async_trait]
pub trait AdminReadStore: Send + Sync {
    async fn overview(&self, window_ms: i64) -> Result<OverviewSnapshot, AdminReadError>;
    async fn overview_scoped(
        &self,
        scope: &AdminReadScope,
        window_ms: i64,
    ) -> Result<OverviewSnapshot, AdminReadError>;
    async fn billing(&self, window_ms: i64) -> Result<BillingSnapshot, AdminReadError>;
    async fn billing_scoped(
        &self,
        scope: &AdminReadScope,
        window_ms: i64,
        project_id: Option<&str>,
    ) -> Result<BillingSnapshot, AdminReadError>;
    async fn usage_analysis_scoped(
        &self,
        scope: &AdminReadScope,
        query: UsageAnalysisQuery,
    ) -> Result<UsageAnalysisSnapshot, AdminReadError>;
    async fn provider_accounts(&self) -> Result<ProviderAccountsSnapshot, AdminReadError>;
    async fn provider_account_concurrency(
        &self,
        provider_account_ids: Option<&[Uuid]>,
    ) -> Result<ProviderAccountConcurrencySnapshot, AdminReadError>;
    async fn scheduler(&self, window_ms: i64) -> Result<SchedulerSnapshot, AdminReadError>;
    async fn jobs(&self, query: JobsQuery) -> Result<JobsSnapshot, AdminReadError>;
    async fn jobs_scoped(
        &self,
        scope: &AdminReadScope,
        query: JobsQuery,
    ) -> Result<JobsSnapshot, AdminReadError>;
    async fn request_logs(
        &self,
        query: RequestLogsQuery,
    ) -> Result<RequestLogsSnapshot, AdminReadError>;
    async fn request_logs_scoped(
        &self,
        scope: &AdminReadScope,
        query: RequestLogsQuery,
    ) -> Result<RequestLogsSnapshot, AdminReadError>;
    async fn audit_logs(&self, query: AuditLogsQuery) -> Result<AuditLogsSnapshot, AdminReadError>;
    async fn job_economics(&self, job_id: Uuid) -> Result<JobEconomicsSnapshot, AdminReadError>;
    async fn job_economics_scoped(
        &self,
        scope: &AdminReadScope,
        job_id: Uuid,
        project_id: Option<String>,
    ) -> Result<JobEconomicsSnapshot, AdminReadError>;
}

pub(super) fn unknown_upstream_quota() -> UpstreamQuotaObservation {
    UpstreamQuotaObservation::unknown()
}

#[cfg(test)]
mod tests {
    use super::{
        BillingSnapshot, ConsoleBillingSnapshot, ConsoleJobEconomicsSnapshot, JobEconomicsSnapshot,
        JobLedgerTransaction, JobProviderCost, LedgerAggregate, ProviderCostAggregate,
        ProviderCostCoverage,
    };

    #[test]
    fn console_billing_excludes_provider_cost_facts() {
        let snapshot = BillingSnapshot {
            as_of_ms: 3,
            from_ms: 1,
            to_ms: 2,
            account_snapshots: Vec::new(),
            charged_usage: Vec::new(),
            rated_usage: Vec::new(),
            sealed_ledger: vec![
                LedgerAggregate {
                    tenant_id: Some("tenant-a".to_string()),
                    transaction_type: "customer_charge".to_string(),
                    currency: "USD".to_string(),
                    amount_micros: "25".to_string(),
                    transaction_count: "1".to_string(),
                },
                LedgerAggregate {
                    tenant_id: Some("tenant-a".to_string()),
                    transaction_type: "provider_cost".to_string(),
                    currency: "USD".to_string(),
                    amount_micros: "10".to_string(),
                    transaction_count: "1".to_string(),
                },
                LedgerAggregate {
                    tenant_id: Some("tenant-a".to_string()),
                    transaction_type: "customer_refund".to_string(),
                    currency: "USD".to_string(),
                    amount_micros: "5".to_string(),
                    transaction_count: "1".to_string(),
                },
            ],
            provider_costs: vec![ProviderCostAggregate {
                tenant_id: Some("tenant-a".to_string()),
                provider_id: "provider-a".to_string(),
                outcome: "succeeded".to_string(),
                cost_basis: "provider_actual".to_string(),
                attribution_state: "attributed".to_string(),
                currency: "USD".to_string(),
                amount_micros: "10".to_string(),
                transaction_count: "1".to_string(),
                linked_receipts: "1".to_string(),
            }],
            provider_cost_coverage: ProviderCostCoverage {
                terminal_receipts: "1".to_string(),
                covered_receipts: "1".to_string(),
                uncovered_receipts: "0".to_string(),
                provider_actual_transactions: "1".to_string(),
                provider_allocated_transactions: "0".to_string(),
                legacy_unverified_transactions: "0".to_string(),
                unattributed_transactions: "0".to_string(),
                authority_conflicts: "0".to_string(),
            },
        };

        let console = ConsoleBillingSnapshot::from(snapshot);
        assert_eq!(console.sealed_ledger.len(), 2);
        assert_eq!(console.sealed_ledger[0].transaction_type, "customer_charge");
        assert_eq!(console.sealed_ledger[1].transaction_type, "customer_refund");
        let serialized = serde_json::to_value(console).expect("console billing should serialize");
        assert!(serialized.get("provider_costs").is_none());
        assert!(serialized.get("provider_cost_coverage").is_none());
    }

    #[test]
    fn console_job_economics_excludes_provider_cost_facts() {
        let snapshot = JobEconomicsSnapshot {
            as_of_ms: 10,
            job_id: "job-a".to_string(),
            economics_contract_version: 4,
            economics_state: "rated".to_string(),
            customer_quote: None,
            customer_hold: None,
            usage_facts: Vec::new(),
            customer_rating: None,
            ledger_transactions: vec![
                JobLedgerTransaction {
                    transaction_id: "customer".to_string(),
                    transaction_type: "customer_job_charge".to_string(),
                    currency: "USD".to_string(),
                    amount_micros: "100".to_string(),
                    created_at_ms: 8,
                    sealed_at_ms: Some(9),
                },
                JobLedgerTransaction {
                    transaction_id: "provider".to_string(),
                    transaction_type: "provider_cost".to_string(),
                    currency: "USD".to_string(),
                    amount_micros: "40".to_string(),
                    created_at_ms: 8,
                    sealed_at_ms: Some(9),
                },
                JobLedgerTransaction {
                    transaction_id: "refund".to_string(),
                    transaction_type: "customer_refund".to_string(),
                    currency: "USD".to_string(),
                    amount_micros: "25".to_string(),
                    created_at_ms: 9,
                    sealed_at_ms: Some(10),
                },
            ],
            provider_costs: vec![JobProviderCost {
                cost_id: "cost-a".to_string(),
                cost_basis: "provider_actual".to_string(),
                attribution_state: "attributed".to_string(),
                currency: "USD".to_string(),
                observed_amount_micros: "40".to_string(),
                attributed_amount_micros: Some("40".to_string()),
                authority: "provider_reported".to_string(),
                confidence: "exact".to_string(),
                price_book_version_id: None,
                transaction_id: Some("provider".to_string()),
                sealed_at_ms: Some(9),
                created_at_ms: 8,
            }],
        };

        let console = ConsoleJobEconomicsSnapshot::from(snapshot);
        assert_eq!(console.ledger_transactions.len(), 2);
        assert_eq!(
            console.ledger_transactions[0].transaction_type,
            "customer_job_charge"
        );
        assert_eq!(
            console.ledger_transactions[1].transaction_type,
            "customer_refund"
        );
        let serialized =
            serde_json::to_value(console).expect("console job economics should serialize");
        assert!(serialized.get("provider_costs").is_none());
    }
}
