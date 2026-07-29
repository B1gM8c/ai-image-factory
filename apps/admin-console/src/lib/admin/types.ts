export type EpochMs = number;
export type Int64String = string;

export type StateCount = {
  state: string;
  count: Int64String;
};

export type UsageAggregate = {
  tenant_id?: string;
  billing_metric: string;
  billing_unit: string;
  outcome: string;
  quantity: Int64String;
};

export type LedgerAggregate = {
  tenant_id?: string;
  transaction_type: string;
  currency: string;
  amount_micros: Int64String;
  transaction_count: Int64String;
};

export type OverviewSnapshot = {
  as_of_ms: EpochMs;
  from_ms: EpochMs;
  to_ms: EpochMs;
  job_states: StateCount[];
  charged_usage: UsageAggregate[];
  sealed_ledger: LedgerAggregate[];
  terminal_job_elapsed_p95_ms: number | null;
  terminal_job_elapsed_samples: Int64String;
};

export type BillingAccountSnapshot = {
  tenant_id: string;
  currency: string;
  credit_limit_micros: Int64String;
  held_micros: Int64String;
  captured_micros: Int64String;
  refunded_micros: Int64String;
  available_micros: Int64String;
  control_version: Int64String;
  updated_at_ms: EpochMs;
};

export type BillingAccountControlView = {
  object: "billing.account";
  tenant_id: string;
  currency: string;
  configured: boolean;
  credit_limit_micros: Int64String;
  held_micros: Int64String;
  captured_micros: Int64String;
  refunded_micros: Int64String;
  available_micros: Int64String;
  control_version: Int64String;
  updated_at_ms: EpochMs | null;
};

export type BillingOrganizationAccountView = {
  organization_id: string;
  display_name: string;
  organization_kind: "personal" | "team" | "system";
  account: BillingAccountControlView;
};

export type BillingAccountControlList = {
  object: "list";
  data: BillingOrganizationAccountView[];
  has_more: boolean;
  next_after: string | null;
};

export type CustomerChargeView = {
  object: "billing.customer_charge";
  transaction_id: string;
  job_id: string;
  tenant_id: string;
  currency: string;
  amount_micros: Int64String;
  refunded_micros: Int64String;
  remaining_refundable_micros: Int64String;
  refund_state: "refundable" | "partially_refunded" | "fully_refunded";
  created_at_ms: EpochMs;
};

export type CustomerRefundView = {
  object: "billing.customer_refund";
  refund_id: string;
  original_transaction_id: string;
  refund_transaction_id: string;
  tenant_id: string;
  currency: string;
  amount_micros: Int64String;
  grant_restored_micros: Int64String;
  account_refunded_micros: Int64String;
  refunded_total_micros: Int64String;
  remaining_refundable_micros: Int64String;
  reason_code: string;
  reason: string;
  actor_user_id: string;
  session_id: string;
  created_at_ms: EpochMs;
};

export type CreditGrantState =
  | "active"
  | "consuming"
  | "exhausted"
  | "expired"
  | "revoked";

export type CreditGrantView = {
  object: "billing.credit_grant";
  grant_id: string;
  organization_id: string;
  organization_display_name: string | null;
  currency: string;
  source_kind: "promotional";
  source_reference: string;
  original_amount_micros: Int64String;
  available_micros: Int64String;
  reserved_micros: Int64String;
  consumed_micros: Int64String;
  restored_micros: Int64String;
  expired_micros: Int64String;
  revoked_micros: Int64String;
  state: CreditGrantState;
  received_at_ms: EpochMs;
  expires_at_ms: EpochMs;
};

export type CreditGrantSummary = {
  original_amount_micros: Int64String;
  available_micros: Int64String;
  reserved_micros: Int64String;
  consumed_micros: Int64String;
  restored_micros: Int64String;
  expired_micros: Int64String;
  revoked_micros: Int64String;
};

export type CreditGrantList = {
  object: "list";
  as_of_ms: EpochMs;
  organization_id: string | null;
  currency: string;
  summary: CreditGrantSummary;
  data: CreditGrantView[];
  has_more: boolean;
  next_after: string | null;
};

export type OrganizationCreditGrantView = {
  object: "billing.credit_grant";
  grant_id: string;
  currency: string;
  original_amount_micros: Int64String;
  available_micros: Int64String;
  state: CreditGrantState;
  received_at_ms: EpochMs;
  expires_at_ms: EpochMs;
};

export type OrganizationCreditGrantSummary = {
  original_amount_micros: Int64String;
  available_micros: Int64String;
};

export type OrganizationCreditGrantList = {
  object: "list";
  as_of_ms: EpochMs;
  organization_id: string;
  currency: string;
  summary: OrganizationCreditGrantSummary;
  data: OrganizationCreditGrantView[];
  has_more: boolean;
  next_after: string | null;
};

export type CustomerChargeDetail = CustomerChargeView & {
  refunds: CustomerRefundView[];
};

export type CustomerChargeList = {
  object: "list";
  data: CustomerChargeView[];
  has_more: boolean;
  next_after: string | null;
};

export type BillingIntegrityRun = {
  object: "billing.integrity_run";
  run_id: string;
  check_version: number;
  scanner_version: string;
  check_set: string[];
  scope_type: string;
  scope_id: string | null;
  actor_kind: string;
  state: "completed";
  initiated_by_user_id: string | null;
  as_of_ms: EpochMs;
  started_at_ms: EpochMs;
  completed_at_ms: EpochMs;
  critical_count: number;
  warning_count: number;
  finding_count: number;
  summary: Record<string, unknown>;
};

export type BillingIntegrityFinding = {
  object: "billing.integrity_finding";
  finding_id: string;
  run_id: string;
  severity: "critical" | "warning";
  category:
    | "account_balance"
    | "hold_lifecycle"
    | "customer_charge"
    | "attribution"
    | string;
  finding_code: string;
  tenant_id: string | null;
  currency: string | null;
  resource_type: string;
  resource_id: string;
  expected: Record<string, unknown>;
  actual: Record<string, unknown>;
  details: Record<string, unknown>;
  detected_at_ms: EpochMs;
};

export type BillingIntegrityRunDetail = BillingIntegrityRun & {
  findings: BillingIntegrityFinding[];
};

export type BillingIntegrityRunList = {
  object: "list";
  data: BillingIntegrityRun[];
  has_more: boolean;
  next_after: string | null;
};

export type ProviderCostObligationSummary = {
  open: number;
  overdue: number;
  escalated: number;
  settled: number;
  waived: number;
};

export type ProviderCostObligation = {
  object: "billing.provider_cost_obligation";
  receipt_id: string;
  submission_id: string;
  output_id: string;
  job_id: string;
  tenant_id: string;
  provider_id: string;
  provider_account_id: string | null;
  receipt_outcome: string;
  state: "expected" | "pending" | "settled" | "waived";
  urgency: "within_sla" | "overdue" | "escalated" | "resolved";
  expected_authority_kind: "provider_actual" | "provider_allocated" | null;
  settlement_claim_id: string | null;
  currency: string | null;
  pending_reason_code: string | null;
  waiver_reason_code: string | null;
  due_at_ms: EpochMs;
  escalate_at_ms: EpochMs;
  pending_since_ms: EpochMs | null;
  last_reviewed_at_ms: EpochMs | null;
  next_review_at_ms: EpochMs | null;
  review_attempt_count: number;
  control_version: Int64String;
  created_at_ms: EpochMs;
  updated_at_ms: EpochMs;
  settled_at_ms: EpochMs | null;
  waived_at_ms: EpochMs | null;
};

export type ProviderCostObligationEvent = {
  event_id: Int64String;
  control_version: Int64String;
  previous_state: string | null;
  state: string;
  event_kind: string;
  details: Record<string, unknown>;
  created_at_ms: EpochMs;
};

export type ProviderCostObligationDetail = ProviderCostObligation & {
  events: ProviderCostObligationEvent[];
};

export type ProviderCostObligationList = {
  object: "list";
  as_of_ms: EpochMs;
  summary: ProviderCostObligationSummary;
  data: ProviderCostObligation[];
  has_more: boolean;
  next_after: string | null;
};

export type ProviderCostAllocationBasis =
  | "successful_job"
  | "successful_output";

export type ProviderCostAllocationState = "draft" | "closed";

export type ProviderCostAllocationLinePreview = {
  job_id: string;
  output_id: string | null;
  basis_receipt_id: string;
  basis_receipt_payload_hash: string;
  basis_quote_id: string;
  basis_quote_hash: string;
  basis_quantity: string;
  basis_unit: string;
  amount_micros: Int64String;
};

export type ProviderCostAllocationPreview = {
  object: "billing.provider_cost_allocation_preview";
  provider_id: string;
  provider_account_id: string;
  price_book_version_id: string;
  period_start_ms: EpochMs;
  period_end_ms: EpochMs;
  currency: string;
  total_amount_micros: Int64String;
  allocation_basis: ProviderCostAllocationBasis;
  candidate_count: number;
  allocated_amount_micros: Int64String;
  residual_amount_micros: Int64String;
  preview_hash: string;
  lines: ProviderCostAllocationLinePreview[];
};

export type ProviderCostAllocationSummary = {
  object: "billing.provider_cost_allocation";
  provider_cost_allocation_pool_id: string;
  semantic_key: string;
  provider_id: string;
  provider_account_id: string;
  price_book_version_id: string;
  period_start_ms: EpochMs;
  period_end_ms: EpochMs;
  currency: string;
  total_amount_micros: Int64String;
  residual_amount_micros: Int64String;
  allocated_amount_micros: Int64String;
  allocation_basis: ProviderCostAllocationBasis;
  state: ProviderCostAllocationState;
  control_version: number;
  candidate_count: number;
  created_at_ms: EpochMs;
  closed_at_ms: EpochMs | null;
};

export type ProviderCostAllocationLine = {
  provider_cost_allocation_line_id: string;
  job_id: string;
  output_id: string | null;
  basis_receipt_id: string;
  basis_receipt_payload_hash: string;
  basis_quote_id: string;
  basis_quote_hash: string;
  basis_quantity: string;
  basis_unit: string;
  amount_micros: Int64String;
  created_at_ms: EpochMs;
};

export type ProviderCostAllocationClosure = {
  source_kind:
    | "provider_invoice"
    | "provider_contract"
    | "provider_subscription"
    | "provider_statement";
  source_reference: string;
  source_evidence_hash: string;
  closed_by_user_id: string;
  closed_by_session_id: string;
  created_at_ms: EpochMs;
};

export type ProviderCostAllocationDetail =
  ProviderCostAllocationSummary & {
    preview_hash: string;
    lines: ProviderCostAllocationLine[];
    closure: ProviderCostAllocationClosure | null;
  };

export type ProviderCostAllocationList = {
  object: "list";
  as_of_ms: EpochMs;
  data: ProviderCostAllocationSummary[];
  has_more: boolean;
  next_after: string | null;
};

export type PreviewProviderCostAllocationRequest = {
  provider_id: string;
  provider_account_id: string;
  price_book_version_id: string;
  period_start_ms: EpochMs;
  period_end_ms: EpochMs;
  currency: string;
  total_amount_micros: Int64String;
  allocation_basis: ProviderCostAllocationBasis;
};

export type CreateProviderCostAllocationDraftRequest =
  PreviewProviderCostAllocationRequest & {
    expected_preview_hash: string;
    idempotency_key: string;
  };

export type CloseProviderCostAllocationRequest = {
  expected_control_version: number;
  expected_snapshot_hash: string;
  source_kind: ProviderCostAllocationClosure["source_kind"];
  source_reference: string;
  source_evidence_hash: string;
};

export type RatedUsageAggregate = {
  tenant_id: string;
  billing_metric: string;
  billing_unit: string;
  outcome: string;
  currency: string;
  quantity: Int64String;
  amount_micros: Int64String;
};

export type ProviderCostAggregate = {
  tenant_id?: string;
  provider_id: string;
  outcome: string;
  cost_basis: "provider_actual" | "provider_allocated" | "legacy_unverified";
  attribution_state: "attributed" | "unattributed";
  currency: string;
  amount_micros: Int64String;
  transaction_count: Int64String;
  linked_receipts: Int64String;
};

export type ProviderCostCoverage = {
  terminal_receipts: Int64String;
  covered_receipts: Int64String;
  uncovered_receipts: Int64String;
  provider_actual_transactions: Int64String;
  provider_allocated_transactions: Int64String;
  legacy_unverified_transactions: Int64String;
  unattributed_transactions: Int64String;
  authority_conflicts: Int64String;
};

export type BillingSnapshot = {
  as_of_ms: EpochMs;
  from_ms: EpochMs;
  to_ms: EpochMs;
  account_snapshots: BillingAccountSnapshot[];
  charged_usage: UsageAggregate[];
  rated_usage: RatedUsageAggregate[];
  sealed_ledger: LedgerAggregate[];
  provider_costs: ProviderCostAggregate[];
  provider_cost_coverage: ProviderCostCoverage;
};

export type ConsoleBillingSnapshot = Omit<
  BillingSnapshot,
  "provider_costs" | "provider_cost_coverage"
>;

export type UsageActivityPoint = {
  bucket_start_ms: EpochMs;
  group_kind: string;
  group_value: string;
  group_label: string;
  billing_metric: string;
  billing_unit: string;
  outcome: string;
  quantity: Int64String;
};

export type UsageSpendPoint = UsageActivityPoint & {
  currency: string;
  amount_micros: Int64String;
};

export type UsageFilterOption = {
  value: string;
  label: string;
};

export type UsageFilterOptions = {
  projects: UsageFilterOption[];
  api_keys: UsageFilterOption[];
  users: UsageFilterOption[];
  providers: UsageFilterOption[];
  models: UsageFilterOption[];
  operations: UsageFilterOption[];
  service_tiers: UsageFilterOption[];
};

export type UsageAnalysisSnapshot = {
  as_of_ms: EpochMs;
  from_ms: EpochMs;
  to_ms: EpochMs;
  interval: "1m" | "1h" | "1d";
  interval_ms: number;
  group_by:
    | "none"
    | "line_item"
    | "project"
    | "api_key"
    | "user"
    | "provider"
    | "model"
    | "operation"
    | "service_tier";
  activity: UsageActivityPoint[];
  spend: UsageSpendPoint[];
  filter_options: UsageFilterOptions;
};

export type PriceBookPurpose =
  | "customer_sale"
  | "provider_actual"
  | "provider_estimated"
  | "provider_allocated"
  | "provider_benchmark";

export type PriceBookState = "active" | "archived";
export type PriceBookVersionState = "draft" | "active" | "retired";

export type PriceComponent = {
  price_component_id: string;
  component_key: string;
  metric: string;
  unit: string;
  unit_size: Int64String;
  unit_price_micros: Int64String;
  outcome: string;
  quantity_source: string;
  required_confidence: string;
  rounding_mode: string;
  dimensions: Record<string, unknown>;
  created_at_ms: EpochMs;
};

export type PriceBookVersion = {
  price_book_version_id: string;
  price_book_id: string;
  version: number;
  api_profile: string;
  operation: string;
  provider_id: string | null;
  provider_model_id: string | null;
  public_model_id: string;
  media_kind: "image" | "video";
  service_tier: string;
  execution_surface: "provider_api" | "provider_cli" | "manual_import";
  billing_mode:
    | "customer_rate"
    | "provider_reported"
    | "published_rate"
    | "contract_rate"
    | "subscription_allocation"
    | "membership_points";
  is_free: boolean;
  state: PriceBookVersionState;
  effective_from_ms: EpochMs;
  effective_until_ms: EpochMs | null;
  source_kind: "manual" | "official_document" | "provider_contract" | "imported";
  source_url: string | null;
  source_checked_at_ms: EpochMs | null;
  notes: string | null;
  control_version: Int64String;
  created_at_ms: EpochMs;
  updated_at_ms: EpochMs;
  components: PriceComponent[];
};

export type PriceBook = {
  price_book_id: string;
  price_book_key: string;
  display_name: string;
  purpose: PriceBookPurpose;
  scope_type: "platform" | "organization" | "project";
  organization_id: string | null;
  project_id: string | null;
  provider_id: string | null;
  currency: string;
  state: PriceBookState;
  control_version: Int64String;
  created_at_ms: EpochMs;
  updated_at_ms: EpochMs;
  versions: PriceBookVersion[];
};

export type PriceBookCatalog = {
  as_of_ms: EpochMs;
  price_books: PriceBook[];
};

export type PriceRollbackDraftResult = {
  source_version_id: string;
  draft: PriceBookVersion;
};

export type PricingCoverageSummary = {
  surfaces: number;
  routable_surfaces: number;
  sale_priced_surfaces: number;
  actual_cost_surfaces: number;
  benchmark_only_surfaces: number;
  blocked_surfaces: number;
};

export type PricingCoverageRow = {
  provider_id: string;
  provider_display_name: string;
  provider_model_id: string;
  provider_model_display_name: string;
  public_model_id: string | null;
  api_profile: string | null;
  operation: string;
  pricing_operation: string | null;
  pricing_dimensions: string[];
  customer_metering_bases: Array<{
    metric: string;
    unit: string;
    quantity_source: string;
    confidence: string;
  }>;
  media_kind: "image" | "video";
  route_status: "routable" | "unavailable" | "missing";
  routable_account_count: number;
  customer_price_status: "ready" | "ambiguous" | "missing";
  customer_price_currencies: string[];
  metering_status: "exact" | "estimated" | "ambiguous" | "incompatible" | "missing";
  provider_cost_status:
    | "provider_actual"
    | "provider_allocated"
    | "provider_estimated"
    | "benchmark_only"
    | "actual_price_missing"
    | "not_emitted"
    | "ambiguous";
  provider_cost_currencies: string[];
  source_status: "verified" | "manual" | "missing";
  readiness: "ready" | "warning" | "blocked";
  blocking_reasons: string[];
};

export type PricingCoverageSnapshot = {
  as_of_ms: EpochMs;
  scope: "platform_baseline";
  summary: PricingCoverageSummary;
  rows: PricingCoverageRow[];
};

export type PricePublishReadiness = {
  price_book_version_id: string;
  price_book_id: string;
  purpose: PriceBookPurpose;
  ready: boolean;
  matching_surface_count: number;
  metering_status: "exact" | "estimated" | "incompatible" | "missing" | "not_applicable";
  request_dimensions: string[];
  blocking_reasons: string[];
  warnings: string[];
};

export type OfficialPriceCatalogDescriptor = {
  catalog_key: string;
  source_provider_id: string;
  display_name: string;
  currency: string;
  source_url: string;
  retrieval_method: "curated_manifest" | "official_api" | "official_document";
  source_checked_at_ms: EpochMs | null;
  source_revision: string | null;
  parser_version: string;
  item_count: number;
  available: boolean;
  unavailable_reason: string | null;
  latest_sync_run: OfficialPriceSyncRunSummary | null;
};

export type OfficialPriceCatalogs = {
  as_of_ms: EpochMs;
  catalogs: OfficialPriceCatalogDescriptor[];
};

export type OfficialPriceSnapshotSummary = {
  snapshot_id: string;
  catalog_key: string;
  source_provider_id: string;
  currency: string;
  source_url: string;
  source_checked_at_ms: EpochMs;
  source_revision: string | null;
  parser_version: string;
  content_sha256: string;
  state: "observed" | "partially_applied" | "applied" | "rejected";
  item_count: number;
  created_by_user_id: string;
  created_at_ms: EpochMs;
  updated_at_ms: EpochMs;
};

export type OfficialPriceSnapshotDiff = {
  item_key: string;
  display_name: string;
  public_model_id: string;
  media_kind: "image" | "video";
  target_provider_id: string;
  component_count: number;
  status: "new" | "changed" | "removed" | "unchanged" | "draft_exists" | "conflict";
  price_book_id: string | null;
  price_book_version_id: string | null;
  existing_version: number | null;
  existing_state: PriceBookVersionState | null;
  component_differences: OfficialPriceComponentDiff[];
};

export type OfficialPriceComponentDiff = {
  component_key: string;
  status: "added" | "removed" | "changed" | "unchanged";
  previous: PriceComponentDraft | null;
  observed: PriceComponentDraft | null;
};

export type OfficialPriceSnapshotApplication = {
  item_key: string;
  action: "created_draft" | "linked_draft" | "linked_active";
  price_book_id: string;
  price_book_version_id: string;
  applied_by_user_id: string;
  applied_at_ms: EpochMs;
};

export type OfficialPriceSnapshotPreview = {
  snapshot: OfficialPriceSnapshotSummary;
  sync_run: OfficialPriceSyncRunSummary | null;
  differences: OfficialPriceSnapshotDiff[];
  applications: OfficialPriceSnapshotApplication[];
};

export type OfficialPriceSyncRunSummary = {
  sync_run_id: string;
  catalog_key: string;
  source_provider_id: string;
  retrieval_method: "curated_manifest" | "official_api" | "official_document";
  parser_version: string;
  source_checked_at_ms: EpochMs;
  source_revision: string | null;
  evidence_sha256: string;
  normalized_content_sha256: string | null;
  state: "changed" | "unchanged" | "invalid";
  previous_snapshot_id: string | null;
  snapshot_id: string | null;
  failure_code: string | null;
  initiated_by_user_id: string;
  created_at_ms: EpochMs;
  completed_at_ms: EpochMs;
};

export type PriceComponentDraft = {
  component_key: string;
  metric: string;
  unit: string;
  unit_size: Int64String;
  unit_price_micros: Int64String;
  outcome: string;
  quantity_source: string;
  required_confidence: string;
  rounding_mode: string;
  dimensions: Record<string, unknown>;
};

export type PriceBookVersionDraft = {
  api_profile: string;
  operation: string;
  provider_id: string | null;
  provider_model_id: string | null;
  public_model_id: string;
  media_kind: "image" | "video";
  service_tier: string;
  execution_surface: "provider_api" | "provider_cli" | "manual_import";
  billing_mode: PriceBookVersion["billing_mode"];
  is_free: boolean;
  effective_from_ms: EpochMs;
  source_kind: PriceBookVersion["source_kind"];
  source_url: string | null;
  source_checked_at_ms: EpochMs | null;
  notes: string | null;
  components: PriceComponentDraft[];
};

export type UpstreamQuotaObservation = {
  status: string;
  limit: Int64String | null;
  remaining: Int64String | null;
  reset_at_ms: EpochMs | null;
  observed_at_ms: EpochMs | null;
  plan_type: string | null;
  credits_balance: string | null;
  credits_unlimited: boolean | null;
  windows: UpstreamQuotaWindow[];
};

export type UpstreamQuotaWindow = {
  limit_id: string;
  limit_name: string | null;
  window_role: "primary" | "secondary";
  window_duration_mins: number | null;
  used_percent: number;
  resets_at_ms: EpochMs | null;
};

export type ProviderAccountView = {
  provider_account_id: string;
  account_key: string;
  provider_id: string;
  display_name: string | null;
  account_email: string | null;
  environment_state: string | null;
  account_state: string;
  credential_pool_state: string;
  credential_lifecycle_state:
    "active" | "refresh_due" | "refreshing" | "reauth_required" | "unsupported";
  credential_refresh_strategy: "broker_managed" | "cli_managed" | "reauth_only";
  operational_credential_revision: number;
  credential_access_expires_at_ms: EpochMs | null;
  credential_next_refresh_at_ms: EpochMs | null;
  credential_last_success_at_ms: EpochMs | null;
  credential_consecutive_failures: number;
  credential_last_error_code: string | null;
  execution_profile_id: string;
  profile_key: string;
  operation_id: string;
  completion_mode: string;
  profile_state: string;
  resource_policy_state: string;
  scheduling_state: "active" | "draining" | "disabled";
  control_version: number;
  configuration_status: string;
  runtime_status: string;
  max_concurrency: Int64String;
  allocated_count: Int64String;
  available_capacity: Int64String;
  active_submitters: Int64String;
  active_pollers: Int64String;
  draining_submitters: Int64String;
  draining_pollers: Int64String;
  upstream_quota: UpstreamQuotaObservation;
};

export type ProviderAccountsSnapshot = {
  as_of_ms: EpochMs;
  accounts: ProviderAccountView[];
};

export type GrokVideoOutput = {
  provider_account_id: string;
  enabled: boolean;
  ready: boolean;
  bucket: string | null;
  region: string | null;
  endpoint: string | null;
  key_prefix: string | null;
  expires_secs: number | null;
  has_read_write_credentials: boolean;
  has_read_only_credentials: boolean;
};

export type ProviderAccountConcurrency = {
  provider_account_id: string;
  max_concurrency: Int64String;
  allocated_count: Int64String;
  available_capacity: Int64String;
};

export type ProviderQueuePressure = {
  queued_work_items: Int64String;
  pending_batch_requests: Int64String;
};

export type ProviderAccountRuntimeEvent = {
  kind: "snapshot" | "delta" | "resync_required";
  sequence: number;
  as_of_ms: EpochMs;
  accounts: ProviderAccountConcurrency[];
  queue: ProviderQueuePressure;
};

export type ProviderLoginSession = {
  login_session_id: string;
  provider_id: string;
  account_key: string;
  display_name: string;
  status:
    | "starting"
    | "waiting_for_user"
    | "validating"
    | "succeeded"
    | "failed"
    | "expired";
  login_method: "browser_oauth" | "device_code";
  authorization_url: string | null;
  user_code: string | null;
  provider_account_id: string | null;
  error_code: string | null;
  expires_at_ms: EpochMs;
  created_at_ms: EpochMs;
  updated_at_ms: EpochMs;
};

export type ManagedCliProviderCapability = {
  provider_id: string;
  display_name: string;
  availability: "available" | "unavailable";
  unavailable_reason: string | null;
  login_methods: ProviderLoginSession["login_method"][];
  operation_ids: string[];
  quota_kind: "rate_limit_windows" | "weekly_usage" | "credits" | string;
  executable_version: string | null;
  max_concurrency_limit: number;
};

export type ManagedCliProvidersSnapshot = {
  providers: ManagedCliProviderCapability[];
};

export type ProviderModelView = {
  provider_id: string;
  provider_display_name: string;
  model_id: string;
  display_name: string;
  media_kind: "image" | "video";
  operation_ids: string[];
  discovery_source: "adapter_contract" | "cli_help" | "cli_models";
  adapter_state: "supported" | "discovered";
  lifecycle_state: "enabled" | "disabled";
  observed_account_count?: number;
  routable_account_count?: number;
  latest_cli_version: string | null;
  last_observed_at_ms?: EpochMs | null;
  last_successful_refresh_at_ms?: EpochMs | null;
  availability: "routable" | "observed" | "unobserved" | "not_supported";
};

export type ProviderModelsSnapshot = {
  as_of_ms: EpochMs;
  models: ProviderModelView[];
};

export type ProviderModelRefresh = {
  refresh_id: string;
  provider_account_id: string;
  provider_id: string;
  status: "queued" | "running" | "succeeded" | "failed";
  discovered_count: number;
  error_code: string | null;
  started_at_ms: EpochMs | null;
  completed_at_ms: EpochMs | null;
  created_at_ms: EpochMs;
  updated_at_ms: EpochMs;
};

export type ProviderAccountModel = {
  model_id: string;
  display_name: string;
  media_kind: "image" | "video";
  operation_ids: string[];
  enabled: boolean;
  configurable: boolean;
  observed: boolean;
};

export type ProviderAccountModels = {
  provider_account_id: string;
  provider_id: string;
  mode: "automatic" | "allowlist";
  version: number;
  models: ProviderAccountModel[];
};

export type ProviderRouteMember = {
  provider_account_id: string;
  account_key: string;
  execution_profile_id: string;
  priority: number;
  weight: number;
  minimum_remaining_percent: number;
};

export type ProviderRouteModelMapping = {
  api_profile: string;
  public_model_id: string;
  provider_model_id: string;
  execution_model_id: string;
  provider_model_display_name: string;
  media_kind: "image" | "video";
};

export type ProviderRoute = {
  route_id: string;
  revision: number;
  route_key: string;
  display_name: string;
  provider_id: string;
  operation_id: string;
  command_schema: string;
  route_kind: "account" | "group";
  selection_strategy: string;
  quota_freshness_ms: number;
  unknown_quota_policy: "allow" | "block";
  state: string;
  members: ProviderRouteMember[];
  model_mappings: ProviderRouteModelMapping[];
  created_at_ms: EpochMs;
};

export type ProviderRoutesSnapshot = {
  as_of_ms: EpochMs;
  routes: ProviderRoute[];
};

export type WorkStateCount = {
  state: string;
  ready_timing?: string;
  count: Int64String;
};

export type StageCount = {
  stage: string;
  count: Int64String;
};

export type ProviderStateCount = {
  stage: string;
  state: string;
  count: Int64String;
};

export type SchedulerCapacity = {
  provider_account_id: string;
  account_key: string;
  provider_id: string;
  display_name: string | null;
  account_email: string | null;
  max_concurrency: Int64String;
  allocated_count: Int64String;
  available_capacity: Int64String;
};

export type BlockedTerminalReduction = {
  submission_id: string;
  executor_execution_id: string;
  job_id: string;
  request_id: string;
  provider_id: string;
  model: string;
  resolved_state: string;
  error_code: string;
  blocked_at_ms: EpochMs;
  blocked_by: string;
};

export type SchedulerActiveJob = {
  job_id: string;
  request_id: string;
  organization_id: string | null;
  organization_name: string | null;
  project_id: string | null;
  project_name: string | null;
  user_display_name: string | null;
  user_email: string | null;
  service_account_name: string | null;
  api_key_name: string | null;
  operation: string;
  provider_id: string;
  model: string;
  job_state: string;
  work_state: string | null;
  provider_account_id: string | null;
  provider_account_name: string | null;
  attempt_count: Int64String;
  available_at_ms: EpochMs | null;
  lease_expires_at_ms: EpochMs | null;
  created_at_ms: EpochMs;
  started_at_ms: EpochMs | null;
};

export type SchedulerSnapshot = {
  as_of_ms: EpochMs;
  from_ms: EpochMs;
  to_ms: EpochMs;
  work_items: WorkStateCount[];
  expired_leases: Int64String;
  provider_polls_due: Int64String;
  pending_terminal_reductions: Int64String;
  blocked_terminal_reductions: Int64String;
  blocked_terminals: BlockedTerminalReduction[];
  active_jobs: SchedulerActiveJob[];
  capacity_reconciliations_due: Int64String;
  artifact_retention_due: Int64String;
  artifact_retention_deleting: Int64String;
  artifact_retention_failures: Int64String;
  recent_uncertain: StageCount[];
  capacity: SchedulerCapacity[];
};

export type JobCursor = {
  created_at_ms: EpochMs;
  job_id: string;
};

export type JobListItem = {
  job_id: string;
  tenant_id: string;
  project_id: string | null;
  service_account_id: string | null;
  api_key_id: string | null;
  auth_kind: string | null;
  actor_user_id: string | null;
  request_id: string;
  operation: string;
  provider_id: string;
  model: string;
  job_state: string;
  work_state: string | null;
  provider_states: ProviderStateCount[];
  output_count: Int64String;
  billable_units: Int64String;
  billing_metric: string;
  billing_unit: string;
  charged_units: Int64String;
  created_at_ms: EpochMs;
  started_at_ms: EpochMs | null;
  finished_at_ms: EpochMs | null;
  last_error_code: string | null;
};

export type JobsSnapshot = {
  as_of_ms: EpochMs;
  from_ms: EpochMs;
  to_ms: EpochMs;
  items: JobListItem[];
  next_cursor: JobCursor | null;
};

export type RequestLogCursor = {
  created_at_ms: EpochMs;
  request_id: string;
};

export type RequestLogItem = {
  request_id: string;
  source: "models" | "images" | "videos" | "files" | "batches";
  method: string;
  route_pattern: string;
  request_path: string;
  status_code: number;
  duration_ms: number;
  error_code: string | null;
  idempotency_key_digest: string | null;
  tenant_id: string | null;
  project_id: string | null;
  service_account_id: string | null;
  api_key_id: string | null;
  actor_user_id: string | null;
  auth_kind: string | null;
  content_captured: boolean;
  job_id: string | null;
  operation: string | null;
  provider_id: string | null;
  model: string | null;
  job_state: string | null;
  work_state: string | null;
  output_count: Int64String | null;
  billable_units: Int64String | null;
  billing_unit: string | null;
  requested_service_tier: "auto" | "default" | "flex" | "priority" | null;
  project_service_tier: "default" | "priority" | null;
  effective_service_tier: "default" | "flex" | "priority" | null;
  service_tier_fallback_reason: string | null;
  created_at_ms: EpochMs;
  completed_at_ms: EpochMs;
};

export type RequestLogsSnapshot = {
  as_of_ms: EpochMs;
  from_ms: EpochMs;
  to_ms: EpochMs;
  items: RequestLogItem[];
  next_cursor: RequestLogCursor | null;
};

export type JobQuoteLine = {
  component_key: string;
  partition_key: string;
  terminal_outcome: string;
  metric: string;
  unit: string;
  unit_size: Int64String;
  unit_price_micros: Int64String;
  reservation_quantity_source: string;
  reservation_confidence: string;
  max_quantity: Int64String;
  max_amount_micros: Int64String;
  actual_quantity: Int64String | null;
  actual_amount_micros: Int64String | null;
};

export type JobCustomerQuote = {
  quote_id: string;
  price_book_version_id: string;
  public_model_id: string;
  media_kind: string;
  service_tier: string;
  currency: string;
  is_free: boolean;
  max_total_micros: Int64String;
  created_at_ms: EpochMs;
  lines: JobQuoteLine[];
};

export type JobCustomerHold = {
  state: string;
  currency: string;
  held_micros: Int64String;
  captured_micros: Int64String;
  released_micros: Int64String;
  created_at_ms: EpochMs;
  updated_at_ms: EpochMs;
};

export type JobUsageFact = {
  metric: string;
  quantity: Int64String;
  unit: string;
  quantity_source: string;
  confidence: string;
  billing_partition_key: string;
  terminal_outcome: string;
  created_at_ms: EpochMs;
};

export type JobCustomerRating = {
  currency: string;
  total_amount_micros: Int64String;
  created_at_ms: EpochMs;
};

export type JobLedgerTransaction = {
  transaction_id: string;
  transaction_type: string;
  currency: string;
  amount_micros: Int64String;
  created_at_ms: EpochMs;
  sealed_at_ms: EpochMs | null;
};

export type JobProviderCost = {
  cost_id: string;
  cost_basis:
    | "provider_actual"
    | "provider_allocated"
    | "legacy_unverified";
  attribution_state: "attributed" | "shared";
  currency: string;
  observed_amount_micros: Int64String;
  attributed_amount_micros: Int64String | null;
  authority: string;
  confidence: string;
  price_book_version_id: string | null;
  transaction_id: string | null;
  sealed_at_ms: EpochMs | null;
  created_at_ms: EpochMs;
};

export type ConsoleJobEconomicsSnapshot = {
  as_of_ms: EpochMs;
  job_id: string;
  economics_contract_version: number;
  economics_state:
    | "legacy_contract"
    | "awaiting_quote"
    | "quoted"
    | "metered"
    | "rated";
  customer_quote: JobCustomerQuote | null;
  customer_hold: JobCustomerHold | null;
  usage_facts: JobUsageFact[];
  customer_rating: JobCustomerRating | null;
  ledger_transactions: JobLedgerTransaction[];
};

export type JobEconomicsSnapshot = ConsoleJobEconomicsSnapshot & {
  provider_costs: JobProviderCost[];
};

export type ProjectFilePurpose =
  | "assistants"
  | "batch"
  | "batch_output"
  | "fine-tune"
  | "vision"
  | "user_data"
  | "evals";

export type ProjectFile = {
  id: string;
  object: "file";
  bytes: number;
  created_at: number;
  expires_at?: number | null;
  filename: string;
  purpose: ProjectFilePurpose;
};

export type ProjectFileList = {
  object: "list";
  data: ProjectFile[];
  first_id?: string | null;
  last_id?: string | null;
  has_more?: boolean;
};

export type DeletedProjectFile = {
  id: string;
  object: "file";
  deleted: boolean;
};

export type ProjectBatchStatus =
  | "validating"
  | "failed"
  | "in_progress"
  | "finalizing"
  | "completed"
  | "expired"
  | "cancelling"
  | "cancelled";

export type ProjectBatchError = {
  code?: string | null;
  message: string;
  param?: string | null;
  line?: number | null;
};

export type ProjectBatch = {
  id: string;
  object: "batch";
  endpoint: string;
  errors?: {
    object?: "list";
    data: ProjectBatchError[];
  } | null;
  input_file_id: string;
  completion_window: "24h";
  status: ProjectBatchStatus;
  output_file_id: string | null;
  error_file_id: string | null;
  created_at: number;
  in_progress_at: number | null;
  expires_at: number | null;
  finalizing_at: number | null;
  completed_at: number | null;
  failed_at: number | null;
  expired_at: number | null;
  cancelling_at: number | null;
  cancelled_at: number | null;
  request_counts: {
    total: number;
    completed: number;
    failed: number;
  } | null;
  metadata: Record<string, string> | null;
};

export type ProjectBatchList = {
  object: "list";
  data: ProjectBatch[];
  first_id?: string | null;
  last_id?: string | null;
  has_more?: boolean;
};

export type CreateProjectBatchRequest = {
  input_file_id: string;
  endpoint: "/v1/images/generations";
  completion_window: "24h";
  metadata?: Record<string, string>;
};
