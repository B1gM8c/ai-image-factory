use std::{env, time::Duration};

use gpt_image_2_gateway::{
    ApiKeyKeyring, ApiKeyPermissionMode, ApiKeyPermissions, ApiKeyStore, CredentialResolveError,
    ImageGatewayError, OperationalCredentialResolver, PostgresApiKeyStore, PostgresCredentialStore,
    PostgresUsageStore, UsageCharge, UsageLimits, UsageStore,
    database::{
        connect_pool, connect_test_pool_with_search_path, run_migrations, verify_migrations,
    },
};
use sqlx::{AssertSqlSafe, PgPool, postgres::PgListener};
use tokio::time::timeout;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const REQUIRED_COLUMNS: [(&str, &str); 940] = [
    ("usage_events", "tenant_id"),
    ("usage_events", "job_id"),
    ("quota_reservations", "tenant_id"),
    ("quota_reservations", "job_id"),
    ("quota_reservations", "committed_units"),
    ("jobs", "tenant_id"),
    ("jobs", "operation"),
    ("jobs", "provider_id"),
    ("jobs", "model"),
    ("jobs", "reservation_id"),
    ("jobs", "created_at_ms"),
    ("jobs", "updated_at_ms"),
    ("jobs", "last_error_code"),
    ("jobs", "last_error_message"),
    ("gateway_api_keys", "hash_algorithm"),
    ("gateway_api_keys", "pepper_version"),
    ("gateway_projects", "id"),
    ("gateway_projects", "tenant_id"),
    ("gateway_projects", "archived_at"),
    ("gateway_projects", "service_tier"),
    ("gateway_projects", "user_api_keys_disabled"),
    ("gateway_projects", "settings_version"),
    ("gateway_projects", "file_storage_limit_bytes"),
    ("gateway_projects", "file_storage_limit_count"),
    ("job_service_tier_decisions", "job_id"),
    ("job_service_tier_decisions", "requested_service_tier"),
    ("job_service_tier_decisions", "project_service_tier"),
    ("job_service_tier_decisions", "effective_service_tier"),
    ("job_service_tier_decisions", "fallback_reason"),
    ("job_service_tier_decisions", "created_at_ms"),
    ("gateway_service_accounts", "tenant_id"),
    ("gateway_service_accounts", "owner_type"),
    ("gateway_service_accounts", "owner_user_id"),
    ("gateway_api_keys", "tenant_id"),
    ("gateway_api_keys", "expires_at"),
    ("gateway_api_keys", "authz_version"),
    ("gateway_api_keys", "permission_mode"),
    ("gateway_api_keys", "permissions"),
    ("gateway_api_keys", "created_by_user_id"),
    ("gateway_api_keys", "revoked_by_user_id"),
    ("gateway_api_keys", "revocation_reason"),
    ("job_auth_attributions", "job_id"),
    ("job_auth_attributions", "tenant_id"),
    ("job_auth_attributions", "project_id"),
    ("job_auth_attributions", "service_account_id"),
    ("job_auth_attributions", "api_key_id"),
    ("job_auth_attributions", "credential_authz_version"),
    ("job_auth_attributions", "credential_owner_user_id"),
    ("job_auth_attributions", "actor_user_id"),
    ("job_auth_attributions", "actor_session_id"),
    ("job_auth_attributions", "actor_authz_version"),
    ("job_auth_attributions", "route_provider_id"),
    ("job_auth_attributions", "route_operation_id"),
    ("job_auth_attributions", "route_command_schema"),
    ("job_auth_attributions", "route_id"),
    ("job_auth_attributions", "route_revision"),
    ("job_auth_attributions", "auth_kind"),
    ("job_auth_attributions", "admitted_at_ms"),
    ("provider_account_environments", "provider_account_id"),
    ("provider_account_environments", "environment_ref"),
    ("provider_account_environments", "upstream_identity_sha256"),
    ("provider_account_login_sessions", "login_session_id"),
    ("provider_account_login_sessions", "status"),
    ("provider_account_login_sessions", "login_method"),
    ("provider_account_login_sessions", "authorization_url"),
    ("provider_account_login_sessions", "provider_account_id"),
    ("provider_account_quota_snapshots", "provider_account_id"),
    ("provider_account_quota_snapshots", "status"),
    ("provider_account_quota_windows", "provider_account_id"),
    ("provider_account_quota_windows", "limit_id"),
    ("provider_account_quota_windows", "window_role"),
    ("provider_account_quota_windows", "used_percent"),
    (
        "provider_account_credential_revisions",
        "provider_account_id",
    ),
    ("provider_account_credential_revisions", "revision"),
    ("provider_account_credential_revisions", "material_kind"),
    (
        "provider_account_credential_revisions",
        "material_fingerprint_sha256",
    ),
    (
        "provider_account_credential_revisions",
        "access_expires_at_ms",
    ),
    ("provider_account_credential_heads", "provider_account_id"),
    ("provider_account_credential_heads", "active_revision"),
    ("provider_account_credential_heads", "lifecycle_state"),
    ("provider_account_credential_heads", "refresh_strategy"),
    ("provider_account_credential_heads", "next_refresh_at_ms"),
    ("provider_account_credential_heads", "lease_epoch"),
    ("provider_account_credential_heads", "control_version"),
    ("provider_account_credential_events", "credential_event_id"),
    ("provider_account_credential_events", "event_type"),
    ("provider_routes", "route_id"),
    ("provider_routes", "revision"),
    ("provider_routes", "route_kind"),
    ("provider_routes", "selection_strategy"),
    ("provider_routes", "quota_freshness_ms"),
    ("provider_routes", "unknown_quota_policy"),
    ("provider_route_heads", "current_revision"),
    ("provider_route_heads", "state"),
    ("provider_route_members", "route_id"),
    ("provider_route_members", "execution_profile_id"),
    ("provider_route_members", "minimum_remaining_percent"),
    ("provider_route_model_mappings", "route_id"),
    ("provider_route_model_mappings", "route_revision"),
    ("provider_route_model_mappings", "api_profile"),
    ("provider_route_model_mappings", "public_model_id"),
    ("provider_route_model_mappings", "provider_model_id"),
    ("provider_route_model_mappings", "execution_model_id"),
    ("provider_route_model_mappings", "media_kind"),
    ("provider_account_operations", "provider_account_id"),
    ("provider_account_operations", "provider_id"),
    ("provider_account_operations", "operation_id"),
    ("provider_account_operations", "state"),
    ("price_books", "control_version"),
    ("price_book_versions", "control_version"),
    ("price_book_version_rollbacks", "rollback_version_id"),
    ("price_book_version_rollbacks", "source_version_id"),
    ("price_book_version_rollbacks", "created_by_user_id"),
    ("price_book_version_rollbacks", "created_by_session_id"),
    ("price_book_version_rollbacks", "created_at_ms"),
    ("billing_accounts", "control_version"),
    ("billing_accounts", "refunded_micros"),
    ("ledger_transactions", "reverses_transaction_id"),
    ("billing_account_limit_changes", "change_id"),
    ("billing_account_limit_changes", "tenant_id"),
    ("billing_account_limit_changes", "currency"),
    (
        "billing_account_limit_changes",
        "previous_credit_limit_micros",
    ),
    ("billing_account_limit_changes", "new_credit_limit_micros"),
    ("billing_account_limit_changes", "control_version"),
    ("billing_account_limit_changes", "actor_user_id"),
    ("billing_account_limit_changes", "session_id"),
    ("billing_account_limit_changes", "reason"),
    ("billing_account_limit_changes", "created_at_ms"),
    ("billing_integrity_runs", "run_id"),
    ("billing_integrity_runs", "check_version"),
    ("billing_integrity_runs", "scanner_version"),
    ("billing_integrity_runs", "check_set"),
    ("billing_integrity_runs", "scope_type"),
    ("billing_integrity_runs", "scope_id"),
    ("billing_integrity_runs", "state"),
    ("billing_integrity_runs", "actor_kind"),
    ("billing_integrity_runs", "initiated_by_user_id"),
    ("billing_integrity_runs", "session_id"),
    ("billing_integrity_runs", "as_of_ms"),
    ("billing_integrity_runs", "started_at_ms"),
    ("billing_integrity_runs", "completed_at_ms"),
    ("billing_integrity_runs", "critical_count"),
    ("billing_integrity_runs", "warning_count"),
    ("billing_integrity_runs", "finding_count"),
    ("billing_integrity_runs", "summary"),
    ("billing_integrity_findings", "finding_id"),
    ("billing_integrity_findings", "run_id"),
    ("billing_integrity_findings", "finding_key"),
    ("billing_integrity_findings", "severity"),
    ("billing_integrity_findings", "category"),
    ("billing_integrity_findings", "finding_code"),
    ("billing_integrity_findings", "tenant_id"),
    ("billing_integrity_findings", "currency"),
    ("billing_integrity_findings", "resource_type"),
    ("billing_integrity_findings", "resource_id"),
    ("billing_integrity_findings", "expected"),
    ("billing_integrity_findings", "actual"),
    ("billing_integrity_findings", "details"),
    ("billing_integrity_findings", "detected_at_ms"),
    ("provider_cost_obligations", "receipt_id"),
    ("provider_cost_obligations", "submission_id"),
    ("provider_cost_obligations", "output_id"),
    ("provider_cost_obligations", "job_id"),
    ("provider_cost_obligations", "provider_id"),
    ("provider_cost_obligations", "provider_account_id"),
    ("provider_cost_obligations", "currency"),
    ("provider_cost_obligations", "state"),
    ("provider_cost_obligations", "expected_authority_kind"),
    ("provider_cost_obligations", "settlement_claim_id"),
    ("provider_cost_obligations", "pending_reason_code"),
    ("provider_cost_obligations", "waiver_reason_code"),
    ("provider_cost_obligations", "waiver_source_kind"),
    ("provider_cost_obligations", "waiver_source_id"),
    ("provider_cost_obligations", "waiver_evidence_hash"),
    ("provider_cost_obligations", "waived_by_user_id"),
    ("provider_cost_obligations", "waived_by_session_id"),
    ("provider_cost_obligations", "due_at_ms"),
    ("provider_cost_obligations", "escalate_at_ms"),
    ("provider_cost_obligations", "pending_since_ms"),
    ("provider_cost_obligations", "last_reviewed_at_ms"),
    ("provider_cost_obligations", "next_review_at_ms"),
    ("provider_cost_obligations", "review_attempt_count"),
    ("provider_cost_obligations", "control_version"),
    ("provider_cost_obligations", "created_at_ms"),
    ("provider_cost_obligations", "updated_at_ms"),
    ("provider_cost_obligations", "settled_at_ms"),
    ("provider_cost_obligations", "waived_at_ms"),
    ("provider_cost_obligation_events", "event_id"),
    ("provider_cost_obligation_events", "receipt_id"),
    ("provider_cost_obligation_events", "control_version"),
    ("provider_cost_obligation_events", "previous_state"),
    ("provider_cost_obligation_events", "state"),
    ("provider_cost_obligation_events", "event_kind"),
    ("provider_cost_obligation_events", "details"),
    ("provider_cost_obligation_events", "created_at_ms"),
    ("customer_refunds", "refund_id"),
    ("customer_refunds", "original_transaction_id"),
    ("customer_refunds", "refund_transaction_id"),
    ("customer_refunds", "tenant_id"),
    ("customer_refunds", "currency"),
    ("customer_refunds", "amount_micros"),
    ("customer_refunds", "reason_code"),
    ("customer_refunds", "reason"),
    ("customer_refunds", "idempotency_key_digest"),
    ("customer_refunds", "request_hash"),
    ("customer_refunds", "actor_user_id"),
    ("customer_refunds", "session_id"),
    ("customer_refunds", "created_at_ms"),
    ("customer_refunds", "grant_restored_micros"),
    ("customer_refunds", "account_refunded_micros"),
    ("customer_price_quotes", "quote_id"),
    ("customer_price_quotes", "job_id"),
    ("customer_price_quotes", "project_id"),
    ("customer_price_quotes", "price_book_version_id"),
    ("customer_price_quotes", "max_total_micros"),
    ("customer_price_quotes", "quote_hash"),
    ("customer_price_quote_lines", "quote_line_id"),
    ("customer_price_quote_lines", "quote_id"),
    ("customer_price_quote_lines", "price_component_id"),
    ("customer_price_quote_lines", "partition_key"),
    ("customer_price_quote_lines", "terminal_outcome"),
    ("customer_price_quote_lines", "reservation_quantity_source"),
    ("customer_price_quote_lines", "reservation_confidence"),
    ("customer_price_quote_lines", "rate_adjustment_numerator"),
    ("customer_price_quote_lines", "rate_adjustment_denominator"),
    ("customer_price_quote_lines", "max_quantity"),
    ("customer_price_quote_lines", "max_amount_micros"),
    ("customer_billing_holds", "hold_id"),
    ("customer_billing_holds", "quote_id"),
    ("customer_billing_holds", "held_micros"),
    ("customer_billing_holds", "captured_micros"),
    ("customer_billing_holds", "released_micros"),
    ("customer_billing_holds", "state"),
    ("customer_billing_holds", "grant_held_micros"),
    ("customer_billing_holds", "account_held_micros"),
    ("customer_billing_holds", "grant_captured_micros"),
    ("customer_billing_holds", "account_captured_micros"),
    ("customer_billing_holds", "grant_released_micros"),
    ("customer_billing_holds", "account_released_micros"),
    ("credit_grants", "grant_id"),
    ("credit_grants", "semantic_key"),
    ("credit_grants", "tenant_id"),
    ("credit_grants", "currency"),
    ("credit_grants", "source_kind"),
    ("credit_grants", "source_reference"),
    ("credit_grants", "received_at_ms"),
    ("credit_grants", "expires_at_ms"),
    ("credit_grants", "original_amount_micros"),
    ("credit_grants", "reserved_micros"),
    ("credit_grants", "consumed_micros"),
    ("credit_grants", "restored_micros"),
    ("credit_grants", "expired_micros"),
    ("credit_grants", "revoked_micros"),
    ("credit_grants", "available_micros"),
    ("credit_grants", "state"),
    ("credit_grants", "control_version"),
    ("credit_grants", "created_at_ms"),
    ("credit_grants", "updated_at_ms"),
    (
        "customer_billing_hold_grant_reservations",
        "grant_reservation_id",
    ),
    ("customer_billing_hold_grant_reservations", "hold_id"),
    ("customer_billing_hold_grant_reservations", "grant_id"),
    ("customer_billing_hold_grant_reservations", "tenant_id"),
    ("customer_billing_hold_grant_reservations", "currency"),
    (
        "customer_billing_hold_grant_reservations",
        "reserved_micros",
    ),
    (
        "customer_billing_hold_grant_reservations",
        "consumed_micros",
    ),
    (
        "customer_billing_hold_grant_reservations",
        "released_micros",
    ),
    ("customer_billing_hold_grant_reservations", "state"),
    ("customer_billing_hold_grant_reservations", "created_at_ms"),
    ("customer_billing_hold_grant_reservations", "updated_at_ms"),
    ("credit_grant_events", "grant_event_id"),
    ("credit_grant_events", "grant_id"),
    ("credit_grant_events", "tenant_id"),
    ("credit_grant_events", "currency"),
    ("credit_grant_events", "event_sequence"),
    ("credit_grant_events", "event_type"),
    ("credit_grant_events", "amount_micros"),
    ("credit_grant_events", "grant_reservation_id"),
    ("credit_grant_events", "hold_id"),
    ("credit_grant_events", "refund_id"),
    ("credit_grant_events", "related_grant_event_id"),
    ("credit_grant_events", "payload_hash"),
    ("credit_grant_events", "occurred_at_ms"),
    ("credit_grant_events", "created_at_ms"),
    ("credit_grant_operations", "operation_id"),
    ("credit_grant_operations", "grant_id"),
    ("credit_grant_operations", "grant_event_id"),
    ("credit_grant_operations", "tenant_id"),
    ("credit_grant_operations", "currency"),
    ("credit_grant_operations", "operation"),
    ("credit_grant_operations", "idempotency_key_digest"),
    ("credit_grant_operations", "request_hash"),
    ("credit_grant_operations", "actor_user_id"),
    ("credit_grant_operations", "actor_session_id"),
    ("credit_grant_operations", "reason"),
    ("credit_grant_operations", "created_at_ms"),
    ("ledger_transactions", "source_credit_grant_event_id"),
    ("customer_rated_usage", "rated_usage_id"),
    ("customer_rated_usage", "quote_id"),
    ("customer_rated_usage", "fact_set_hash"),
    ("customer_rated_usage", "total_amount_micros"),
    ("customer_rated_usage", "rating_hash"),
    ("customer_rated_usage_lines", "rated_usage_line_id"),
    ("customer_rated_usage_lines", "rated_usage_id"),
    ("customer_rated_usage_lines", "quote_line_id"),
    ("customer_rated_usage_lines", "actual_quantity"),
    ("customer_rated_usage_lines", "amount_micros"),
    ("customer_rated_usage_fact_links", "rated_usage_line_id"),
    ("customer_rated_usage_fact_links", "usage_fact_id"),
    ("provider_usage_facts", "billing_partition_key"),
    ("provider_usage_facts", "terminal_outcome"),
    ("provider_usage_facts", "fact_domain"),
    ("provider_cost_observations", "provider_cost_observation_id"),
    ("provider_cost_observations", "observation_key"),
    ("provider_cost_observations", "provider_id"),
    ("provider_cost_observations", "provider_account_id"),
    ("provider_cost_observations", "execution_surface"),
    ("provider_cost_observations", "provider_operation_id"),
    ("provider_cost_observations", "purpose"),
    ("provider_cost_observations", "price_book_version_id"),
    ("provider_cost_observations", "fact_set_hash"),
    ("provider_cost_observations", "currency"),
    ("provider_cost_observations", "native_unit"),
    ("provider_cost_observations", "native_quantity"),
    ("provider_cost_observations", "authority"),
    ("provider_cost_observations", "confidence"),
    ("provider_cost_observations", "evidence_hash"),
    ("provider_cost_observations", "evidence_path"),
    ("provider_cost_observations", "amount_micros"),
    ("provider_cost_observations", "rounding_mode"),
    ("provider_cost_observations", "rounding_delta_native_atoms"),
    ("provider_cost_observations", "created_at_ms"),
    (
        "provider_cost_observation_fact_links",
        "provider_cost_observation_id",
    ),
    ("provider_cost_observation_fact_links", "usage_fact_id"),
    ("provider_cost_observation_fact_links", "provider_id"),
    (
        "provider_cost_observation_fact_links",
        "provider_account_id",
    ),
    ("provider_cost_observation_fact_links", "execution_surface"),
    ("provider_cost_observation_fact_links", "created_at_ms"),
    (
        "provider_cost_observation_receipts",
        "provider_cost_observation_id",
    ),
    ("provider_cost_observation_receipts", "receipt_id"),
    ("provider_cost_observation_receipts", "provider_id"),
    ("provider_cost_observation_receipts", "created_at_ms"),
    ("ledger_transactions", "source_provider_cost_observation_id"),
    (
        "provider_cost_allocation_pools",
        "provider_cost_allocation_pool_id",
    ),
    ("provider_cost_allocation_pools", "semantic_key"),
    ("provider_cost_allocation_pools", "provider_id"),
    ("provider_cost_allocation_pools", "provider_account_id"),
    ("provider_cost_allocation_pools", "price_book_version_id"),
    ("provider_cost_allocation_pools", "period_start_ms"),
    ("provider_cost_allocation_pools", "period_end_ms"),
    ("provider_cost_allocation_pools", "currency"),
    ("provider_cost_allocation_pools", "total_amount_micros"),
    ("provider_cost_allocation_pools", "residual_amount_micros"),
    ("provider_cost_allocation_pools", "allocation_basis"),
    ("provider_cost_allocation_pools", "state"),
    ("provider_cost_allocation_pools", "control_version"),
    ("provider_cost_allocation_pools", "created_at_ms"),
    ("provider_cost_allocation_pools", "closed_at_ms"),
    ("provider_cost_allocation_pools", "candidate_snapshot_hash"),
    (
        "provider_cost_allocation_lines",
        "provider_cost_allocation_line_id",
    ),
    (
        "provider_cost_allocation_lines",
        "provider_cost_allocation_pool_id",
    ),
    ("provider_cost_allocation_lines", "provider_id"),
    ("provider_cost_allocation_lines", "provider_account_id"),
    ("provider_cost_allocation_lines", "job_id"),
    ("provider_cost_allocation_lines", "output_id"),
    ("provider_cost_allocation_lines", "basis_usage_fact_id"),
    ("provider_cost_allocation_lines", "basis_quantity"),
    ("provider_cost_allocation_lines", "basis_unit"),
    ("provider_cost_allocation_lines", "amount_micros"),
    ("provider_cost_allocation_lines", "created_at_ms"),
    ("provider_cost_allocation_lines", "basis_receipt_id"),
    (
        "provider_cost_allocation_lines",
        "basis_receipt_payload_hash",
    ),
    ("provider_cost_allocation_lines", "basis_quote_id"),
    ("provider_cost_allocation_lines", "basis_quote_hash"),
    ("provider_cost_authority_claims", "claim_id"),
    ("provider_cost_authority_claims", "provider_id"),
    ("provider_cost_authority_claims", "provider_account_id"),
    ("provider_cost_authority_claims", "job_id"),
    ("provider_cost_authority_claims", "currency"),
    ("provider_cost_authority_claims", "authority_kind"),
    ("provider_cost_authority_claims", "authority_period"),
    (
        "provider_cost_authority_claims",
        "source_provider_cost_observation_id",
    ),
    ("provider_cost_authority_claims", "source_usage_fact_id"),
    (
        "provider_cost_authority_claims",
        "source_provider_cost_allocation_pool_id",
    ),
    (
        "provider_cost_authority_claims",
        "source_provider_cost_allocation_line_id",
    ),
    (
        "provider_cost_authority_claims",
        "source_legacy_transaction_id",
    ),
    ("provider_cost_authority_claims", "source_receipt_id"),
    ("provider_cost_authority_claims", "created_at_ms"),
    (
        "provider_cost_allocation_closures",
        "provider_cost_allocation_pool_id",
    ),
    (
        "provider_cost_allocation_closures",
        "idempotency_key_digest",
    ),
    ("provider_cost_allocation_closures", "request_hash"),
    (
        "provider_cost_allocation_closures",
        "candidate_snapshot_hash",
    ),
    ("provider_cost_allocation_closures", "source_kind"),
    ("provider_cost_allocation_closures", "source_reference"),
    ("provider_cost_allocation_closures", "source_evidence_hash"),
    (
        "provider_cost_allocation_closures",
        "source_period_start_ms",
    ),
    ("provider_cost_allocation_closures", "source_period_end_ms"),
    ("provider_cost_allocation_closures", "source_currency"),
    ("provider_cost_allocation_closures", "source_amount_micros"),
    ("provider_cost_allocation_closures", "closed_by_user_id"),
    ("provider_cost_allocation_closures", "closed_by_session_id"),
    ("provider_cost_allocation_closures", "created_at_ms"),
    ("executor_provider_cost_evidence", "manifest_id"),
    ("executor_provider_cost_evidence", "executor_execution_id"),
    ("executor_provider_cost_evidence", "submission_id"),
    ("executor_provider_cost_evidence", "scope"),
    ("executor_provider_cost_evidence", "provider_id"),
    ("executor_provider_cost_evidence", "execution_surface"),
    ("executor_provider_cost_evidence", "provider_operation_id"),
    ("executor_provider_cost_evidence", "currency"),
    ("executor_provider_cost_evidence", "native_unit"),
    ("executor_provider_cost_evidence", "native_quantity"),
    ("executor_provider_cost_evidence", "authority"),
    ("executor_provider_cost_evidence", "confidence"),
    ("executor_provider_cost_evidence", "evidence_hash"),
    ("executor_provider_cost_evidence", "evidence_path"),
    ("executor_provider_cost_evidence", "created_at_ms"),
    (
        "ledger_transactions",
        "source_provider_cost_allocation_pool_id",
    ),
    (
        "ledger_transactions",
        "source_provider_cost_allocation_line_id",
    ),
    (
        "provider_account_execution_controls",
        "desired_max_concurrency",
    ),
    ("provider_account_execution_controls", "lifecycle_state"),
    ("provider_account_execution_controls", "control_version"),
    (
        "provider_account_execution_control_events",
        "control_version",
    ),
    ("gateway_api_key_provider_routes", "api_key_id"),
    ("gateway_api_key_provider_routes", "route_id"),
    ("gateway_platform_provider_routes", "provider_id"),
    ("gateway_platform_provider_routes", "operation_id"),
    ("gateway_platform_provider_routes", "command_schema"),
    ("gateway_platform_provider_routes", "route_id"),
    ("gateway_platform_provider_routes", "route_revision"),
    ("gateway_platform_provider_routes", "state"),
    ("gateway_platform_provider_routes", "created_at_ms"),
    ("gateway_platform_provider_routes", "updated_at_ms"),
    ("gateway_project_provider_routes", "project_id"),
    ("gateway_project_provider_routes", "provider_id"),
    ("gateway_project_provider_routes", "operation_id"),
    ("gateway_project_provider_routes", "command_schema"),
    ("gateway_project_provider_routes", "route_id"),
    ("gateway_project_provider_routes", "route_revision"),
    ("gateway_project_provider_routes", "state"),
    ("gateway_project_provider_routes", "created_at_ms"),
    ("gateway_project_provider_routes", "updated_at_ms"),
    ("job_provider_route_attributions", "job_id"),
    ("job_provider_route_attributions", "route_id"),
    ("job_response_projections", "response_schema"),
    ("job_response_projections", "created_at_seconds"),
    ("job_response_projections", "artifact_count"),
    ("artifacts", "execution_id"),
    ("artifacts", "output_index"),
    ("artifacts", "sha256_hex"),
    ("quota_reservations", "limit_5h"),
    ("quota_reservations", "remaining_5h"),
    ("quota_reservations", "limit_7d"),
    ("quota_reservations", "remaining_7d"),
    ("quota_reservations", "admission_session_id"),
    ("admission_sessions", "input_cleanup_state"),
    ("admission_sessions", "input_cleanup_owner"),
    ("admission_sessions", "input_cleanup_lease_expires_at_ms"),
    ("admission_sessions", "input_cleanup_completed_at_ms"),
    ("job_input_manifests", "manifest_schema"),
    ("job_input_manifests", "manifest_hash"),
    ("job_input_objects", "role"),
    ("job_input_objects", "object_key"),
    ("job_input_objects", "sha256_hex"),
    ("job_response_projections", "operation"),
    ("executor_artifact_authorities", "authority_id"),
    ("executor_artifact_authorities", "storage_namespace"),
    ("executor_artifact_authorities", "sha256_hex"),
    ("executor_artifact_authorities", "media_duration_ms"),
    ("executor_result_manifests", "artifact_authority_id"),
    ("executor_executions", "launch_owner"),
    ("executor_executions", "resolution_decision_id"),
    ("provider_submissions", "resolution_decision_id"),
    ("executor_runner_observations", "payload_hash"),
    ("executor_resolution_decisions", "source"),
    ("executor_resolution_decisions", "resolution_fingerprint"),
    ("provider_submissions", "execution_profile_id"),
    ("provider_submissions", "adapter_revision"),
    ("work_items", "execution_profile_id"),
    ("provider_execution_profiles", "credential_ref"),
    ("provider_accounts", "credential_ref"),
    ("provider_accounts", "credential_auth_sha256"),
    ("executor_resource_policies", "allocated_count"),
    ("executor_capacity_allocations", "state"),
    ("executor_capacity_allocations", "release_decision_id"),
    ("executor_capacity_allocations", "release_reconciliation_id"),
    ("work_items", "handed_off_at_ms"),
    ("job_attempts", "handed_off_at_ms"),
    ("executor_terminal_reductions", "submission_id"),
    ("executor_terminal_reductions", "executor_execution_id"),
    ("executor_terminal_reductions", "resolution_decision_id"),
    ("executor_terminal_reductions", "resolved_state"),
    ("executor_terminal_reductions", "state"),
    ("executor_terminal_reductions", "lease_owner"),
    ("executor_terminal_reductions", "lease_epoch"),
    ("executor_terminal_reductions", "lease_expires_at_ms"),
    ("executor_terminal_reductions", "blocked_error_code"),
    ("executor_terminal_reductions", "blocked_by"),
    ("executor_terminal_reductions", "blocked_at_ms"),
    ("executor_terminal_reductions", "completion_owner"),
    ("executor_terminal_reductions", "provider_receipt_id"),
    ("executor_terminal_reductions", "customer_artifact_id"),
    ("executor_terminal_reductions", "quota_reservation_id"),
    ("provider_remote_tasks", "remote_operation_id"),
    ("provider_remote_tasks", "state"),
    ("provider_remote_tasks", "poll_lease_epoch"),
    ("provider_remote_tasks", "state_observation_id"),
    ("provider_remote_tasks", "attach_recovery_owner"),
    ("provider_remote_tasks", "attach_recovery_lease_epoch"),
    ("provider_task_observations", "event_identity"),
    ("provider_task_observations", "payload_hash"),
    ("provider_task_observations", "result_manifest_id"),
    ("provider_task_observations", "artifact_sha256_hex"),
    ("provider_task_observations", "artifact_byte_size"),
    ("provider_task_observations", "artifact_media_type"),
    ("provider_remote_submit_intents", "idempotency_key"),
    ("provider_remote_submit_intents", "state"),
    ("provider_remote_submit_intents", "provider_request_id"),
    ("provider_remote_submit_intents", "send_started_at_ms"),
    ("provider_remote_submit_intents", "receipt_event_identity"),
    ("provider_remote_submit_intents", "failure_event_identity"),
    ("provider_remote_submit_intents", "failure_error_code"),
    ("provider_submit_recoveries", "submission_id"),
    ("provider_submit_recoveries", "invocation_attempt"),
    ("provider_submit_recoveries", "provider_timeout_ms"),
    ("provider_submit_recoveries", "provider_deadline_at_ms"),
    ("provider_submit_recoveries", "next_recovery_at_ms"),
    ("provider_submit_recoveries", "recovery_owner"),
    ("provider_submit_recoveries", "recovery_lease_epoch"),
    ("provider_remote_tasks", "provider_deadline_at_ms"),
    ("provider_remote_tasks", "deadline_quarantine_id"),
    ("provider_remote_task_quarantines", "quarantine_id"),
    ("provider_remote_task_quarantines", "submission_id"),
    ("provider_remote_task_quarantines", "executor_execution_id"),
    ("provider_remote_task_quarantines", "provider_id"),
    ("provider_remote_task_quarantines", "provider_account_id"),
    ("provider_remote_task_quarantines", "remote_operation_id"),
    (
        "provider_remote_task_quarantines",
        "provider_deadline_at_ms",
    ),
    ("provider_remote_task_quarantines", "error_code"),
    ("provider_remote_task_quarantines", "quarantined_at_ms"),
    (
        "executor_resolution_decisions",
        "provider_remote_task_quarantine_id",
    ),
    ("provider_submit_recovery_commands", "provider_id"),
    ("provider_submit_recovery_commands", "provider_account_id"),
    ("provider_submit_recovery_commands", "command_owner"),
    ("provider_submit_recovery_commands", "command_id"),
    ("provider_submit_recovery_commands", "command_kind"),
    ("provider_submit_recovery_commands", "request_duration_ms"),
    ("provider_submit_recovery_commands", "submission_id"),
    ("provider_submit_recovery_commands", "executor_execution_id"),
    ("provider_submit_recovery_commands", "recovery_lease_epoch"),
    ("provider_submit_recovery_commands", "claim_claimed_at_ms"),
    (
        "provider_submit_recovery_commands",
        "claim_lease_expires_at_ms",
    ),
    ("provider_submit_recovery_commands", "intent_state"),
    (
        "provider_submit_recovery_commands",
        "intent_remote_operation_id",
    ),
    (
        "provider_submit_recovery_commands",
        "intent_provider_request_id",
    ),
    (
        "provider_submit_recovery_commands",
        "intent_send_started_at_ms",
    ),
    (
        "provider_submit_recovery_commands",
        "intent_receipt_event_identity",
    ),
    (
        "provider_submit_recovery_commands",
        "intent_failure_event_identity",
    ),
    (
        "provider_submit_recovery_commands",
        "intent_failure_error_code",
    ),
    ("provider_submit_recovery_commands", "intent_updated_at_ms"),
    ("provider_submit_recovery_commands", "created_at_ms"),
    ("provider_capacity_reconciliations", "reconciliation_id"),
    ("provider_capacity_reconciliations", "submission_id"),
    ("provider_capacity_reconciliations", "executor_execution_id"),
    ("provider_capacity_reconciliations", "provider_id"),
    ("provider_capacity_reconciliations", "provider_account_id"),
    (
        "provider_capacity_reconciliations",
        "provider_deadline_at_ms",
    ),
    ("provider_capacity_reconciliations", "state"),
    ("provider_capacity_reconciliations", "available_at_ms"),
    ("provider_capacity_reconciliations", "reconciliation_owner"),
    (
        "provider_capacity_reconciliations",
        "reconciliation_lease_epoch",
    ),
    ("provider_capacity_reconciliations", "evidence_revision"),
    (
        "provider_capacity_reconciliations",
        "claimed_evidence_revision",
    ),
    ("provider_capacity_reconciliations", "last_command_kind"),
    ("provider_capacity_reconciliations", "last_command_id"),
    ("provider_capacity_reconciliations", "last_command_owner"),
    (
        "provider_capacity_reconciliations",
        "last_command_lease_epoch",
    ),
    (
        "provider_capacity_reconciliations",
        "claim_command_claimed_at_ms",
    ),
    (
        "provider_capacity_reconciliations",
        "claim_command_lease_expires_at_ms",
    ),
    ("provider_capacity_reconciliations", "evidence_kind"),
    ("provider_capacity_reconciliations", "remote_operation_id"),
    ("provider_capacity_reconciliations", "remote_terminal_state"),
    ("provider_capacity_reconciliations", "event_identity"),
    ("provider_capacity_reconciliations", "payload_hash"),
    ("provider_capacity_reconciliations", "created_at_ms"),
    ("provider_capacity_reconciliations", "updated_at_ms"),
    ("provider_capacity_reconciliations", "released_at_ms"),
    (
        "executor_resolution_decisions",
        "provider_task_observation_id",
    ),
    ("executor_resolution_decisions", "provider_submit_intent_id"),
    ("provider_execution_profiles", "operation_id"),
    (
        "provider_execution_profiles",
        "operation_descriptor_revision",
    ),
    (
        "provider_execution_profiles",
        "operation_descriptor_sha256_v1",
    ),
    ("provider_execution_profiles", "completion_mode"),
    ("provider_execution_profiles", "idempotency_mode"),
    ("provider_submissions", "operation_id"),
    ("provider_submissions", "operation_descriptor_revision"),
    ("provider_submissions", "operation_descriptor_sha256_v1"),
    ("provider_submissions", "completion_mode"),
    ("provider_submissions", "idempotency_mode"),
    ("provider_submissions", "operation_binding_version"),
    ("provider_remote_submit_intents", "provider_command_sha256"),
    ("provider_remote_submit_intents", "execution_binding_sha256"),
    ("provider_remote_submit_intents", "provider_timeout_ms"),
    ("provider_runtime_leases", "runtime_id"),
    ("provider_runtime_leases", "execution_profile_id"),
    ("provider_runtime_leases", "runtime_role"),
    ("provider_runtime_leases", "runtime_owner"),
    ("provider_runtime_leases", "state"),
    ("provider_runtime_leases", "heartbeat_at_ms"),
    ("provider_runtime_leases", "lease_expires_at_ms"),
    ("provider_runtime_leases", "created_at_ms"),
    ("provider_runtime_leases", "updated_at_ms"),
    ("jobs", "output_count"),
    ("jobs", "billable_units"),
    ("jobs", "billing_metric"),
    ("jobs", "billing_unit"),
    ("job_outputs", "billable_units"),
    ("quota_reservations", "billing_metric"),
    ("quota_reservations", "billing_unit"),
    ("usage_events", "billing_metric"),
    ("usage_events", "billing_unit"),
    ("metering_events", "billing_metric"),
    ("metering_events", "billing_unit"),
    ("price_versions", "billing_metric"),
    ("price_versions", "billing_unit"),
    ("api_profile_pricing_aliases", "api_profile"),
    ("api_profile_pricing_aliases", "pricing_api_profile"),
    ("api_profile_pricing_aliases", "created_at_ms"),
    ("price_quotes", "billing_metric"),
    ("price_quotes", "billing_unit"),
    ("price_quotes", "billable_units"),
    ("identity_users", "user_id"),
    ("identity_users", "normalized_email"),
    ("identity_users", "roles"),
    ("identity_users", "scopes"),
    ("identity_users", "authz_version"),
    ("identity_organizations", "organization_id"),
    ("identity_organizations", "organization_kind"),
    ("identity_organizations", "owner_user_id"),
    ("identity_organization_memberships", "organization_id"),
    ("identity_organization_memberships", "user_id"),
    ("identity_organization_memberships", "role"),
    ("identity_project_memberships", "organization_id"),
    ("identity_project_memberships", "project_id"),
    ("identity_project_memberships", "user_id"),
    ("identity_project_memberships", "is_default"),
    ("provider_accounts", "tenant_id"),
    ("provider_accounts", "owner_user_id"),
    ("identity_password_credentials", "password_hash"),
    ("identity_session_families", "session_id"),
    ("identity_session_families", "user_id"),
    ("identity_session_families", "authz_version_at_login"),
    ("identity_session_families", "idle_expires_at_ms"),
    ("identity_session_families", "absolute_expires_at_ms"),
    ("identity_session_families", "revoked_at_ms"),
    ("identity_refresh_tokens", "token_id"),
    ("identity_refresh_tokens", "session_id"),
    ("identity_refresh_tokens", "secret_hash"),
    ("identity_refresh_tokens", "pepper_version"),
    ("identity_refresh_tokens", "consumed_at_ms"),
    ("identity_refresh_tokens", "replaced_by_token_id"),
    ("identity_login_throttles", "throttle_key"),
    ("identity_login_throttles", "dimension"),
    ("identity_login_throttles", "blocked_until_ms"),
    ("identity_audit_events", "event_id"),
    ("identity_audit_events", "action"),
    ("identity_audit_events", "outcome"),
    ("identity_audit_events", "metadata"),
    ("artifact_retention_policies", "policy_key"),
    ("artifact_retention_policies", "policy_version"),
    ("artifact_retention_policies", "retain_for_ms"),
    ("artifact_retention_policies", "read_drain_ms"),
    ("artifact_retention_policies", "retry_delay_ms"),
    ("job_artifact_retention", "job_id"),
    ("job_artifact_retention", "policy_version"),
    ("job_artifact_retention", "retain_for_ms"),
    ("job_artifact_retention", "read_drain_ms"),
    ("job_artifact_retention", "retry_delay_ms"),
    ("job_artifact_retention", "state"),
    ("job_artifact_retention", "expires_at_ms"),
    ("job_artifact_retention", "purge_after_ms"),
    ("job_artifact_retention", "lease_owner"),
    ("job_artifact_retention", "lease_epoch"),
    ("job_artifact_retention", "lease_expires_at_ms"),
    ("job_artifact_retention", "delete_attempts"),
    ("job_artifact_retention", "last_error_code"),
    ("job_artifact_retention", "deleted_at_ms"),
    ("provider_models", "provider_id"),
    ("provider_models", "model_id"),
    ("provider_models", "execution_model_id"),
    ("provider_models", "media_kind"),
    ("provider_models", "display_name"),
    ("provider_models", "adapter_state"),
    ("provider_models", "lifecycle_state"),
    ("provider_models", "operation_ids"),
    ("provider_models", "source_kind"),
    ("provider_models", "first_seen_at_ms"),
    ("provider_models", "last_seen_at_ms"),
    ("provider_models", "last_successful_refresh_at_ms"),
    ("provider_models", "metadata_json"),
    ("provider_model_refreshes", "refresh_id"),
    ("provider_model_refreshes", "provider_account_id"),
    ("provider_model_refreshes", "provider_id"),
    ("provider_model_refreshes", "status"),
    ("provider_model_refreshes", "discovered_count"),
    ("provider_model_refreshes", "error_code"),
    ("provider_model_refreshes", "started_at_ms"),
    ("provider_model_refreshes", "completed_at_ms"),
    ("provider_model_refreshes", "created_at_ms"),
    ("provider_model_refreshes", "updated_at_ms"),
    ("provider_account_model_observations", "provider_account_id"),
    ("provider_account_model_observations", "provider_id"),
    ("provider_account_model_observations", "model_id"),
    ("provider_account_model_observations", "media_kind"),
    ("provider_account_model_observations", "available"),
    ("provider_account_model_observations", "source_kind"),
    ("provider_account_model_observations", "cli_version"),
    ("provider_account_model_observations", "observed_at_ms"),
    ("provider_account_model_observations", "refresh_id"),
    ("provider_account_model_observations", "metadata_json"),
    (
        "provider_account_model_configurations",
        "provider_account_id",
    ),
    ("provider_account_model_configurations", "provider_id"),
    ("provider_account_model_configurations", "mode"),
    ("provider_account_model_configurations", "version"),
    ("provider_account_model_configurations", "updated_at_ms"),
    ("provider_account_model_bindings", "provider_account_id"),
    ("provider_account_model_bindings", "provider_id"),
    ("provider_account_model_bindings", "model_id"),
    ("provider_account_model_bindings", "media_kind"),
    ("provider_account_model_bindings", "configured_at_ms"),
    (
        "provider_cost_observation_sources",
        "provider_cost_observation_id",
    ),
    ("provider_cost_observation_sources", "source_kind"),
    (
        "provider_cost_observation_sources",
        "executor_provider_cost_evidence_manifest_id",
    ),
    ("provider_cost_observation_sources", "legacy_reason"),
    ("provider_cost_observation_sources", "created_at_ms"),
    ("project_spend_budgets", "project_id"),
    ("project_spend_budgets", "organization_id"),
    ("project_spend_budgets", "currency"),
    ("project_spend_budgets", "monthly_budget_micros"),
    ("project_spend_budgets", "limit_type"),
    ("project_spend_budgets", "period_kind"),
    ("project_spend_budgets", "control_version"),
    ("project_spend_budgets", "created_by_user_id"),
    ("project_spend_budgets", "updated_by_user_id"),
    ("project_spend_budgets", "created_at_ms"),
    ("project_spend_budgets", "updated_at_ms"),
    ("project_spend_alert_thresholds", "project_id"),
    ("project_spend_alert_thresholds", "threshold_percent"),
    ("project_spend_alert_thresholds", "created_at_ms"),
    ("project_spend_evaluation_queue", "project_id"),
    ("project_spend_evaluation_queue", "requested_at_ms"),
    ("project_spend_alert_events", "event_id"),
    ("project_spend_alert_events", "project_id"),
    ("project_spend_alert_events", "organization_id"),
    ("project_spend_alert_events", "currency"),
    ("project_spend_alert_events", "period_start_ms"),
    ("project_spend_alert_events", "period_end_ms"),
    ("project_spend_alert_events", "threshold_percent"),
    ("project_spend_alert_events", "budget_control_version"),
    ("project_spend_alert_events", "monthly_budget_micros"),
    ("project_spend_alert_events", "spend_micros"),
    ("project_spend_alert_events", "notification_state"),
    ("project_spend_alert_events", "created_at_ms"),
    ("project_spend_alert_events", "acknowledged_at_ms"),
    ("project_spend_alert_events", "acknowledged_by_user_id"),
    ("project_spend_notification_deliveries", "delivery_id"),
    ("project_spend_notification_deliveries", "event_id"),
    ("project_spend_notification_deliveries", "recipient_user_id"),
    ("project_spend_notification_deliveries", "channel"),
    ("project_spend_notification_deliveries", "state"),
    ("project_spend_notification_deliveries", "attempt_count"),
    (
        "project_spend_notification_deliveries",
        "next_attempt_at_ms",
    ),
    ("project_spend_notification_deliveries", "lease_owner"),
    (
        "project_spend_notification_deliveries",
        "lease_expires_at_ms",
    ),
    ("project_spend_notification_deliveries", "last_error_code"),
    ("project_spend_notification_deliveries", "created_at_ms"),
    ("project_spend_notification_deliveries", "delivered_at_ms"),
    ("project_spend_notification_deliveries", "read_at_ms"),
    ("project_model_policies", "project_id"),
    ("project_model_policies", "organization_id"),
    ("project_model_policies", "control_version"),
    ("project_model_policies", "created_by_user_id"),
    ("project_model_policies", "updated_by_user_id"),
    ("project_model_policies", "created_at_ms"),
    ("project_model_policies", "updated_at_ms"),
    ("project_model_access_entries", "project_id"),
    ("project_model_access_entries", "operation_id"),
    ("project_model_access_entries", "api_profile"),
    ("project_model_access_entries", "public_model_id"),
    ("project_model_access_entries", "media_kind"),
    ("project_model_access_entries", "created_at_ms"),
    ("platform_model_limit_members", "operation_id"),
    ("platform_model_limit_members", "api_profile"),
    ("platform_model_limit_members", "public_model_id"),
    ("platform_model_limit_members", "media_kind"),
    ("platform_model_limit_members", "bucket_key"),
    ("platform_model_limit_members", "bucket_display_name"),
    ("platform_model_limit_members", "unit_kind"),
    ("platform_model_limit_members", "request_ceiling_per_minute"),
    ("platform_model_limit_members", "unit_ceiling_per_minute"),
    ("platform_model_limit_members", "created_at_ms"),
    ("project_model_rate_limits", "project_id"),
    ("project_model_rate_limits", "bucket_key"),
    ("project_model_rate_limits", "unit_kind"),
    ("project_model_rate_limits", "request_limit_per_minute"),
    ("project_model_rate_limits", "unit_limit_per_minute"),
    ("project_model_rate_limits", "created_at_ms"),
    ("project_model_rate_limits", "updated_at_ms"),
    ("project_model_rate_states", "project_id"),
    ("project_model_rate_states", "bucket_key"),
    ("project_model_rate_states", "request_tokens_microunits"),
    ("project_model_rate_states", "unit_tokens_microunits"),
    ("project_model_rate_states", "last_refill_at_ms"),
    ("project_model_rate_states", "updated_at_ms"),
    ("project_model_rate_admissions", "project_id"),
    ("project_model_rate_admissions", "bucket_key"),
    ("project_model_rate_admissions", "admission_session_id"),
    ("project_model_rate_admissions", "request_units"),
    ("project_model_rate_admissions", "unit_count"),
    ("project_model_rate_admissions", "admitted_at_ms"),
    ("gateway_request_observations", "request_id"),
    ("gateway_request_observations", "source"),
    ("gateway_request_observations", "method"),
    ("gateway_request_observations", "route_pattern"),
    ("gateway_request_observations", "request_path"),
    ("gateway_request_observations", "status_code"),
    ("gateway_request_observations", "duration_ms"),
    ("gateway_request_observations", "error_code"),
    ("gateway_request_observations", "idempotency_key_digest"),
    ("gateway_request_observations", "tenant_id"),
    ("gateway_request_observations", "project_id"),
    ("gateway_request_observations", "service_account_id"),
    ("gateway_request_observations", "api_key_id"),
    ("gateway_request_observations", "credential_owner_user_id"),
    ("gateway_request_observations", "actor_user_id"),
    ("gateway_request_observations", "auth_kind"),
    ("gateway_request_observations", "job_id"),
    ("gateway_request_observations", "content_captured"),
    ("gateway_request_observations", "content_expires_at_ms"),
    ("gateway_request_observations", "created_at_ms"),
    ("gateway_request_observations", "completed_at_ms"),
    ("project_webhook_endpoints", "endpoint_id"),
    ("project_webhook_endpoints", "project_id"),
    ("project_webhook_endpoints", "organization_id"),
    ("project_webhook_endpoints", "name"),
    ("project_webhook_endpoints", "url"),
    ("project_webhook_endpoints", "event_types"),
    ("project_webhook_endpoints", "state"),
    ("project_webhook_endpoints", "signing_key_version"),
    ("project_webhook_endpoints", "secret_revision"),
    ("project_webhook_endpoints", "created_by_user_id"),
    ("project_webhook_endpoints", "created_at_ms"),
    ("project_webhook_endpoints", "updated_at_ms"),
    ("project_webhook_endpoints", "disabled_at_ms"),
    ("project_webhook_endpoints", "deleted_at_ms"),
    ("project_webhook_endpoints", "control_version"),
    ("project_webhook_endpoint_runtime", "endpoint_id"),
    ("project_webhook_endpoint_runtime", "paused_until_ms"),
    ("project_webhook_endpoint_runtime", "consecutive_failures"),
    ("project_webhook_endpoint_runtime", "updated_at_ms"),
    ("project_webhook_events", "event_id"),
    ("project_webhook_events", "project_id"),
    ("project_webhook_events", "organization_id"),
    ("project_webhook_events", "source_kind"),
    ("project_webhook_events", "outbox_event_id"),
    ("project_webhook_events", "event_type"),
    ("project_webhook_events", "payload_json"),
    ("project_webhook_events", "payload_body"),
    ("project_webhook_events", "created_at_ms"),
    ("project_webhook_deliveries", "delivery_id"),
    ("project_webhook_deliveries", "event_id"),
    ("project_webhook_deliveries", "endpoint_id"),
    ("project_webhook_deliveries", "project_id"),
    ("project_webhook_deliveries", "organization_id"),
    ("project_webhook_deliveries", "state"),
    ("project_webhook_deliveries", "attempt_count"),
    ("project_webhook_deliveries", "next_attempt_at_ms"),
    ("project_webhook_deliveries", "retry_deadline_at_ms"),
    ("project_webhook_deliveries", "lease_owner"),
    ("project_webhook_deliveries", "lease_epoch"),
    ("project_webhook_deliveries", "lease_expires_at_ms"),
    ("project_webhook_deliveries", "last_http_status"),
    ("project_webhook_deliveries", "last_error_code"),
    ("project_webhook_deliveries", "last_attempt_at_ms"),
    ("project_webhook_deliveries", "delivered_at_ms"),
    ("project_webhook_deliveries", "created_at_ms"),
    ("project_webhook_deliveries", "updated_at_ms"),
    ("project_webhook_attempts", "attempt_id"),
    ("project_webhook_attempts", "delivery_id"),
    ("project_webhook_attempts", "attempt_number"),
    ("project_webhook_attempts", "outcome"),
    ("project_webhook_attempts", "webhook_timestamp"),
    ("project_webhook_attempts", "http_status"),
    ("project_webhook_attempts", "error_code"),
    ("project_webhook_attempts", "duration_ms"),
    ("project_webhook_attempts", "next_attempt_at_ms"),
    ("project_webhook_attempts", "created_at_ms"),
    ("project_webhook_outbox_receipts", "outbox_event_id"),
    ("project_webhook_outbox_receipts", "processed_at_ms"),
    ("project_files", "cleanup_completed_at_ms"),
    ("platform_release_state", "singleton"),
    ("platform_release_state", "repository"),
    ("platform_release_state", "target_triple"),
    ("platform_release_state", "current_version"),
    ("platform_release_state", "current_commit_sha"),
    ("platform_release_state", "previous_version"),
    ("platform_release_state", "previous_commit_sha"),
    ("platform_release_state", "latest_version"),
    ("platform_release_state", "latest_commit_sha"),
    ("platform_release_state", "latest_verified"),
    ("platform_release_state", "last_checked_at_ms"),
    ("platform_release_state", "last_applied_at_ms"),
    ("platform_release_state", "last_error_code"),
    ("platform_release_state", "last_error_message"),
    ("platform_release_state", "updated_at_ms"),
    ("platform_update_commands", "command_id"),
    ("platform_update_commands", "action"),
    ("platform_update_commands", "target_version"),
    ("platform_update_commands", "status"),
    ("platform_update_commands", "phase"),
    ("platform_update_commands", "idempotency_key_digest"),
    ("platform_update_commands", "request_digest"),
    ("platform_update_commands", "requested_by_user_id"),
    ("platform_update_commands", "requested_by_session_id"),
    ("platform_update_commands", "lease_owner"),
    ("platform_update_commands", "lease_epoch"),
    ("platform_update_commands", "lease_expires_at_ms"),
    ("platform_update_commands", "attempt_count"),
    ("platform_update_commands", "progress"),
    ("platform_update_commands", "failure_code"),
    ("platform_update_commands", "failure_message"),
    ("platform_update_commands", "requested_at_ms"),
    ("platform_update_commands", "started_at_ms"),
    ("platform_update_commands", "completed_at_ms"),
    ("platform_update_commands", "updated_at_ms"),
    ("platform_update_events", "event_id"),
    ("platform_update_events", "command_id"),
    ("platform_update_events", "phase"),
    ("platform_update_events", "outcome"),
    ("platform_update_events", "details"),
    ("platform_update_events", "created_at_ms"),
];

const REQUIRED_INDEXES: [&str; 152] = [
    "usage_events_tenant_created_at_ms_idx",
    "gateway_api_keys_project_id_idx",
    "quota_reservations_active_tenant_idx",
    "jobs_tenant_state_created_idx",
    "metering_events_tenant_created_idx",
    "artifacts_job_output_uidx",
    "artifacts_execution_output_uidx",
    "job_input_objects_session_idx",
    "admission_input_cleanup_pending_idx",
    "admission_input_cleanup_lease_idx",
    "executor_executions_pending_evidence_idx",
    "executor_executions_active_owner_idx",
    "executor_capacity_allocations_held_execution_idx",
    "executor_capacity_allocations_orphan_idx",
    "executor_resource_policies_enabled_account_uidx",
    "provider_remote_tasks_poll_claim_idx",
    "provider_submit_recovery_commands_pkey",
    "provider_submit_recovery_commands_transition_uidx",
    "provider_remote_task_quarantines_pkey",
    "provider_remote_tasks_deadline_claim_idx",
    "provider_task_observations_manifest_uidx",
    "provider_submit_intents_remote_operation_uidx",
    "provider_submit_recoveries_claim_idx",
    "provider_submit_recoveries_deadline_idx",
    "provider_capacity_reconciliations_claim_idx",
    "provider_capacity_reconciliations_remote_operation_idx",
    "provider_capacity_reconciliations_claim_command_idx",
    "provider_runtime_leases_profile_role_idx",
    "usage_events_tenant_metric_created_at_ms_idx",
    "quota_reservations_active_tenant_metric_idx",
    "price_versions_active_route_metric_uidx",
    "identity_session_families_user_active_idx",
    "identity_refresh_tokens_session_idx",
    "identity_login_throttles_blocked_idx",
    "identity_audit_events_actor_created_idx",
    "identity_audit_events_session_created_idx",
    "identity_refresh_tokens_session_token_unique",
    "identity_refresh_tokens_parent_unique_idx",
    "identity_refresh_tokens_replacement_unique_idx",
    "identity_session_families_absolute_expiry_idx",
    "identity_session_families_revoked_idx",
    "identity_login_throttles_gc_idx",
    "identity_audit_events_created_idx",
    "identity_audit_events_action_created_idx",
    "identity_audit_events_outcome_created_idx",
    "identity_audit_events_project_created_idx",
    "jobs_admin_global_created_idx",
    "jobs_admin_provider_created_idx",
    "jobs_admin_state_created_idx",
    "jobs_admin_request_id_idx",
    "jobs_admin_uncertain_updated_idx",
    "usage_events_admin_created_idx",
    "rated_usage_admin_created_idx",
    "provider_receipts_admin_created_idx",
    "ledger_transaction_seals_admin_sealed_idx",
    "work_items_admin_awaiting_executor_idx",
    "work_items_admin_uncertain_updated_idx",
    "provider_remote_tasks_admin_uncertain_terminal_idx",
    "gateway_projects_active_created_idx",
    "gateway_api_keys_active_project_created_idx",
    "job_auth_attributions_api_key_admitted_idx",
    "job_auth_attributions_project_admitted_idx",
    "job_auth_attributions_actor_admitted_idx",
    "job_auth_attributions_credential_owner_admitted_idx",
    "usage_events_job_created_idx",
    "job_artifact_retention_expire_idx",
    "job_artifact_retention_purge_idx",
    "job_artifact_retention_reclaim_idx",
    "job_artifact_retention_failure_idx",
    "provider_account_credential_heads_due_idx",
    "provider_account_credential_events_account_idx",
    "provider_account_login_sessions_active_reauth_idx",
    "provider_models_provider_state_idx",
    "provider_model_refreshes_status_idx",
    "provider_model_refreshes_active_account_idx",
    "provider_account_model_observations_model_idx",
    "provider_models_execution_lookup_idx",
    "provider_account_model_bindings_scheduler_idx",
    "provider_account_operations_provider_lookup_idx",
    "ledger_transactions_provider_receipt_uidx",
    "provider_cost_allocation_pools_period_idx",
    "provider_cost_allocation_lines_job_idx",
    "provider_cost_allocation_lines_receipt_idx",
    "provider_cost_allocation_lines_quote_idx",
    "ledger_transactions_provider_cost_allocation_line_uidx",
    "provider_cost_observations_operation_account_unique",
    "provider_cost_observation_fact_links_fact_uidx",
    "provider_cost_observation_receipts_receipt_uidx",
    "provider_cost_authority_claims_pkey",
    "provider_cost_authority_actual_fact_uidx",
    "provider_cost_authority_allocation_line_uidx",
    "provider_cost_authority_legacy_transaction_uidx",
    "provider_cost_authority_receipt_uidx",
    "provider_cost_authority_period_excl",
    "provider_cost_allocation_pools_closed_period_excl",
    "executor_provider_cost_evidence_pkey",
    "executor_provider_cost_evidence_operation_idx",
    "provider_cost_observation_sources_kind_idx",
    "project_spend_alert_events_project_period_idx",
    "project_spend_notification_deliveries_pending_idx",
    "project_spend_notification_deliveries_inbox_idx",
    "platform_model_limit_members_bucket_idx",
    "project_model_rate_admissions_bucket_idx",
    "job_auth_attributions_project_job_idx",
    "customer_rated_usage_created_quote_idx",
    "gateway_request_observations_created_idx",
    "gateway_request_observations_project_created_idx",
    "gateway_request_observations_project_source_created_idx",
    "gateway_request_observations_actor_created_idx",
    "gateway_request_observations_credential_owner_created_idx",
    "gateway_request_observations_api_key_created_idx",
    "gateway_request_observations_project_status_created_idx",
    "price_book_version_rollbacks_source_idx",
    "billing_account_limit_changes_account_created_idx",
    "billing_integrity_runs_completed_idx",
    "billing_integrity_findings_run_severity_idx",
    "provider_cost_obligations_pkey",
    "provider_cost_obligation_events_pkey",
    "provider_cost_obligations_settlement_claim_uidx",
    "provider_cost_obligations_queue_idx",
    "provider_cost_obligations_account_idx",
    "provider_cost_obligation_events_receipt_idx",
    "ledger_transactions_reversal_source_idx",
    "customer_refunds_pkey",
    "customer_refunds_refund_transaction_id_key",
    "customer_refunds_original_idempotency_key",
    "customer_refunds_original_created_idx",
    "customer_refunds_account_created_idx",
    "provider_cost_allocation_closures_pkey",
    "provider_cost_allocation_closures_idempotency_key_digest_key",
    "credit_grants_fefo_idx",
    "credit_grants_expiry_idx",
    "credit_grant_reservations_hold_idx",
    "credit_grant_reservations_grant_idx",
    "credit_grant_events_grant_idx",
    "credit_grant_events_hold_idx",
    "credit_grant_events_refund_idx",
    "credit_grant_operations_grant_idx",
    "ledger_transactions_credit_grant_event_uidx",
    "project_webhook_endpoints_project_created_idx",
    "project_webhook_endpoints_active_events_idx",
    "project_webhook_events_project_created_idx",
    "project_webhook_deliveries_ready_idx",
    "project_webhook_deliveries_lease_expiry_idx",
    "project_webhook_deliveries_endpoint_created_idx",
    "project_webhook_attempts_delivery_created_idx",
    "project_files_project_storage_pending_idx",
    "project_files_cleanup_recovery_idx",
    "platform_update_commands_active_uidx",
    "platform_update_commands_requested_idx",
    "platform_update_commands_claim_idx",
    "platform_update_events_command_created_idx",
];

#[tokio::test]
async fn legacy_schema_without_sqlx_metadata_migrates_from_zero() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = legacy_schema_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn concurrent_fresh_migrations_are_repeatable() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = concurrent_migration_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn platform_route_backfill_only_selects_unambiguous_mapped_routes() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = async {
        apply_migrations_through(&test_schema.pool, 115).await?;

        let unique_route_id = Uuid::new_v4();
        let ambiguous_route_ids = [Uuid::new_v4(), Uuid::new_v4()];
        sqlx::query(
            r#"
            INSERT INTO provider_models (
                provider_id, model_id, execution_model_id, media_kind,
                display_name, adapter_state, lifecycle_state, operation_ids,
                source_kind, first_seen_at_ms, last_seen_at_ms, metadata_json
            )
            VALUES
              ('test-unique', 'test-image', 'test-image', 'image',
               'Test unique image', 'supported', 'enabled',
               ARRAY['images.generations'], 'adapter_contract', 1, 1, '{}'::JSONB),
              ('test-ambiguous', 'test-image', 'test-image', 'image',
               'Test ambiguous image', 'supported', 'enabled',
               ARRAY['images.generations'], 'adapter_contract', 1, 1, '{}'::JSONB)
            "#,
        )
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed provider models: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_routes (
                route_id, revision, route_key, display_name, provider_id,
                operation_id, command_schema, route_kind,
                selection_strategy, state, created_at_ms
            )
            VALUES
              ($1, 1, $2, 'Unique route', 'test-unique',
               'images.generations', 'test.images.generation.v1',
               'account', 'quota_aware_least_loaded', 'enabled', 1),
              ($3, 1, $4, 'Ambiguous route one', 'test-ambiguous',
               'images.generations', 'test.images.generation.v1',
               'account', 'quota_aware_least_loaded', 'enabled', 1),
              ($5, 1, $6, 'Ambiguous route two', 'test-ambiguous',
               'images.generations', 'test.images.generation.v1',
               'account', 'quota_aware_least_loaded', 'enabled', 1)
            "#,
        )
        .bind(unique_route_id)
        .bind(format!("test-unique-{}", unique_route_id.simple()))
        .bind(ambiguous_route_ids[0])
        .bind(format!(
            "test-ambiguous-{}",
            ambiguous_route_ids[0].simple()
        ))
        .bind(ambiguous_route_ids[1])
        .bind(format!(
            "test-ambiguous-{}",
            ambiguous_route_ids[1].simple()
        ))
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed provider routes: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_route_heads (
                route_id, route_key, provider_id, operation_id, command_schema,
                route_kind, current_revision, state, created_at_ms, updated_at_ms
            )
            SELECT route_id, route_key, provider_id, operation_id, command_schema,
                   route_kind, revision, state, created_at_ms, created_at_ms
            FROM provider_routes
            WHERE route_id = ANY($1)
            "#,
        )
        .bind(vec![
            unique_route_id,
            ambiguous_route_ids[0],
            ambiguous_route_ids[1],
        ])
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed provider route heads: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_route_model_mappings (
                route_id, route_revision, provider_id, operation_id,
                command_schema, api_profile, public_model_id,
                provider_model_id, execution_model_id, media_kind, created_at_ms
            )
            SELECT route.route_id, route.revision, route.provider_id,
                   route.operation_id, route.command_schema, 'test-images-v1',
                   'test-image', 'test-image', 'test-image', 'image', 1
            FROM provider_routes route
            WHERE route.route_id = ANY($1)
            "#,
        )
        .bind(vec![
            unique_route_id,
            ambiguous_route_ids[0],
            ambiguous_route_ids[1],
        ])
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed provider route mappings: {error}"))?;

        apply_migration_range(&test_schema.pool, 116, 116).await?;

        let selected_unique_route: Option<Uuid> = sqlx::query_scalar(
            r#"
            SELECT route_id
            FROM gateway_platform_provider_routes
            WHERE provider_id = 'test-unique'
              AND operation_id = 'images.generations'
            "#,
        )
        .fetch_optional(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to read unique platform binding: {error}"))?;
        require(
            selected_unique_route == Some(unique_route_id),
            "the unique mapped route must become the platform default",
        )?;
        let ambiguous_binding_count: i64 = sqlx::query_scalar(
            r#"
            SELECT count(*)
            FROM gateway_platform_provider_routes
            WHERE provider_id = 'test-ambiguous'
              AND operation_id = 'images.generations'
            "#,
        )
        .fetch_one(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to inspect ambiguous platform binding: {error}"))?;
        require(
            ambiguous_binding_count == 0,
            "ambiguous mapped routes must not receive a platform default",
        )
    }
    .await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn default_codex_customer_pricing_preserves_an_existing_generation_price() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = async {
        apply_migrations_through(&test_schema.pool, 84).await?;

        let book_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO price_books (
                price_book_id, price_book_key, display_name, purpose,
                scope_type, currency, state, created_at_ms, updated_at_ms
            )
            VALUES ($1, 'customer_sale.platform.operator', 'Operator pricing',
                    'customer_sale', 'platform', 'USD', 'active', 1, 1)
            "#,
        )
        .bind(book_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed operator price book: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO price_book_versions (
                price_book_version_id, price_book_id, version, api_profile,
                operation, provider_id, provider_model_id, public_model_id,
                media_kind, service_tier, execution_surface, billing_mode,
                is_free, state, effective_from_ms, source_kind,
                created_at_ms, updated_at_ms
            )
            VALUES (
                $1, $2, 1, 'openai-images-v1', 'generation',
                'openai-codex', 'gpt-image-2', 'gpt-image-2', 'image',
                'standard', 'provider_cli', 'customer_rate', FALSE,
                'active', 1, 'manual', 1, 1
            )
            "#,
        )
        .bind(version_id)
        .bind(book_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed operator price version: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO price_components (
                price_component_id, price_book_version_id, component_key,
                metric, unit, unit_size, unit_price_micros, outcome,
                quantity_source, required_confidence, rounding_mode,
                dimensions_json, created_at_ms
            )
            VALUES (
                $1, $2, 'operator-image-output', 'image_output', 'image',
                1, 12345, 'succeeded', 'request_derived', 'exact',
                'exact', '{}'::JSONB, 1
            )
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(version_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed operator price component: {error}"))?;

        apply_migration_range(&test_schema.pool, 85, 117).await?;

        let generation_prices: Vec<(Uuid, i64)> = sqlx::query_as(
            r#"
            SELECT version.price_book_version_id, component.unit_price_micros
            FROM price_books book
            JOIN price_book_versions version USING (price_book_id)
            JOIN price_components component USING (price_book_version_id)
            WHERE book.purpose = 'customer_sale'
              AND book.scope_type = 'platform'
              AND book.state = 'active'
              AND version.state = 'active'
              AND version.api_profile = 'openai-images-v1'
              AND version.operation = 'generation'
              AND version.provider_id = 'openai-codex'
              AND version.provider_model_id = 'gpt-image-2'
              AND version.public_model_id = 'gpt-image-2'
              AND component.outcome = 'succeeded'
            "#,
        )
        .fetch_all(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to inspect preserved generation price: {error}"))?;
        require(
            generation_prices == vec![(version_id, 12345)],
            "default pricing migration must preserve an existing generation price",
        )?;

        let edit_price: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT component.unit_price_micros
            FROM price_books book
            JOIN price_book_versions version USING (price_book_id)
            JOIN price_components component USING (price_book_version_id)
            WHERE book.purpose = 'customer_sale'
              AND book.scope_type = 'platform'
              AND book.state = 'active'
              AND version.state = 'active'
              AND version.operation = 'edit'
              AND version.provider_id = 'openai-codex'
              AND version.public_model_id = 'gpt-image-2'
              AND component.outcome = 'succeeded'
            "#,
        )
        .fetch_optional(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to inspect default edit price: {error}"))?;
        require(
            edit_price == Some(40000),
            "default pricing migration must fill a missing edit price",
        )
    }
    .await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn receipt_snapshot_migration_upgrades_an_existing_draft_pool() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = async {
        apply_migrations_through(&test_schema.pool, 93).await?;

        let credential_pool_id = Uuid::new_v4();
        let provider_account_id = Uuid::new_v4();
        let price_book_id = Uuid::new_v4();
        let price_book_version_id = Uuid::new_v4();
        let allocation_pool_id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO provider_credential_pools
              (credential_pool_id, pool_key, provider_id, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'openai-codex', 'enabled', 1, 1)
            "#,
        )
        .bind(credential_pool_id)
        .bind(format!("migration-pool-{}", credential_pool_id.simple()))
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed credential pool: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_accounts
              (provider_account_id, credential_pool_id, provider_id,
               account_key, credential_ref, credential_revision,
               credential_auth_sha256, state, created_at_ms, updated_at_ms)
            VALUES (
                $1, $2, 'openai-codex', $3, $4, 1, repeat('a', 64),
                'enabled', 1, 1
            )
            "#,
        )
        .bind(provider_account_id)
        .bind(credential_pool_id)
        .bind(format!(
            "migration-account-{}",
            provider_account_id.simple()
        ))
        .bind(format!("managed://migration-account/{provider_account_id}"))
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed provider account: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO price_books (
                price_book_id, price_book_key, display_name, purpose,
                scope_type, provider_id, currency, state,
                created_at_ms, updated_at_ms
            )
            VALUES (
                $1, $2, 'Migration allocation fixture',
                'provider_allocated', 'platform', 'openai-codex',
                'USD', 'active', 1, 1
            )
            "#,
        )
        .bind(price_book_id)
        .bind(format!("migration.allocation.{}", price_book_id.simple()))
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed price book: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO price_book_versions (
                price_book_version_id, price_book_id, version,
                api_profile, operation, provider_id, provider_model_id,
                public_model_id, media_kind, service_tier,
                execution_surface, billing_mode, is_free, state,
                effective_from_ms, source_kind, created_at_ms, updated_at_ms
            )
            VALUES (
                $1, $2, 1, 'openai-images-v1', 'generation',
                'openai-codex', 'gpt-image-2', 'gpt-image-2',
                'image', 'standard', 'provider_cli',
                'subscription_allocation', FALSE, 'active',
                1, 'manual', 1, 1
            )
            "#,
        )
        .bind(price_book_version_id)
        .bind(price_book_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed price book version: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_cost_allocation_pools (
                provider_cost_allocation_pool_id, semantic_key,
                provider_id, provider_account_id, price_book_version_id,
                period_start_ms, period_end_ms, currency,
                total_amount_micros, residual_amount_micros,
                allocation_basis, state, control_version, created_at_ms
            )
            VALUES (
                $1, $2, 'openai-codex', $3, $4,
                1, 2, 'USD', 0, 0,
                'successful_output', 'draft', 1, 1
            )
            "#,
        )
        .bind(allocation_pool_id)
        .bind(format!("migration-allocation:{allocation_pool_id}"))
        .bind(provider_account_id)
        .bind(price_book_version_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed draft allocation pool: {error}"))?;

        apply_migration_range(&test_schema.pool, 94, 94).await?;

        let (snapshot_hash, control_version): (String, i64) = sqlx::query_as(
            r#"
            SELECT candidate_snapshot_hash, control_version
            FROM provider_cost_allocation_pools
            WHERE provider_cost_allocation_pool_id = $1
            "#,
        )
        .bind(allocation_pool_id)
        .fetch_one(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to inspect migrated allocation pool: {error}"))?;
        require(
            snapshot_hash.len() == 64
                && snapshot_hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "0094 must backfill a lowercase SHA-256 candidate snapshot",
        )?;
        require(
            control_version == 2,
            "0094 must advance the draft pool control version during backfill",
        )
    }
    .await;

    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn incremental_customer_pricing_migrations_preserve_existing_usage_fact_outcomes()
-> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = async {
        apply_migrations_through(&test_schema.pool, 61).await?;

        // This fixture isolates the v4 backfill itself; the parent execution graph
        // is immaterial because 0061 already made the receipt outcome canonical.
        sqlx::raw_sql(
            r#"
            DO $$
            DECLARE
                target REGCLASS;
                constraint_row RECORD;
            BEGIN
                FOREACH target IN ARRAY ARRAY[
                    'provider_receipts'::REGCLASS,
                    'provider_usage_facts'::REGCLASS
                ]
                LOOP
                    FOR constraint_row IN
                        SELECT conname
                        FROM pg_constraint
                        WHERE conrelid = target
                          AND contype = 'f'
                    LOOP
                        EXECUTE format(
                            'ALTER TABLE %s DROP CONSTRAINT %I',
                            target,
                            constraint_row.conname
                        );
                    END LOOP;
                END LOOP;
            END;
            $$;
            "#,
        )
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to isolate legacy usage fact fixture: {error}"))?;

        let mut expected = Vec::new();
        for (index, outcome) in ["succeeded", "failed", "no_effect"].into_iter().enumerate() {
            let job_id = Uuid::new_v4();
            let output_id = Uuid::new_v4();
            let submission_id = Uuid::new_v4();
            let receipt_id = Uuid::new_v4();
            let usage_fact_id = Uuid::new_v4();
            sqlx::query(
                r#"
                INSERT INTO provider_receipts (
                    receipt_id, semantic_key, submission_id, output_id, job_id,
                    provider_id, outcome, receipt_schema, payload_hash, evidence,
                    created_at_ms
                )
                VALUES (
                    $1, $2, $3, $4, $5, 'fixture-provider', $6,
                    'fixture.v1', $7, '{}'::JSONB, $8
                )
                "#,
            )
            .bind(receipt_id)
            .bind(format!("incremental-receipt-{outcome}"))
            .bind(submission_id)
            .bind(output_id)
            .bind(job_id)
            .bind(outcome)
            .bind(format!("{:064x}", index + 1))
            .bind(index as i64 + 1)
            .execute(&test_schema.pool)
            .await
            .map_err(|error| format!("failed to seed {outcome} legacy receipt: {error}"))?;
            sqlx::query(
                r#"
                INSERT INTO provider_usage_facts (
                    usage_fact_id, semantic_key, job_id, output_id, submission_id,
                    receipt_id, provider_id, execution_surface, metric, quantity,
                    unit, quantity_source, confidence, created_at_ms
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, 'fixture-provider',
                    'manual_import', 'request', 1, 'request',
                    'operator_adjustment', 'exact', $7
                )
                "#,
            )
            .bind(usage_fact_id)
            .bind(format!("incremental-fact-{outcome}"))
            .bind(job_id)
            .bind(output_id)
            .bind(submission_id)
            .bind(receipt_id)
            .bind(index as i64 + 1)
            .execute(&test_schema.pool)
            .await
            .map_err(|error| format!("failed to seed {outcome} legacy usage fact: {error}"))?;
            expected.push((usage_fact_id, outcome.to_owned()));
        }

        apply_migration_range(&test_schema.pool, 62, 64).await?;

        let migrated: Vec<(Uuid, String, String, String)> = sqlx::query_as(
            r#"
            SELECT fact.usage_fact_id, receipt.outcome,
                   fact.terminal_outcome, fact.billing_partition_key
            FROM provider_usage_facts fact
            JOIN provider_receipts receipt ON receipt.receipt_id = fact.receipt_id
            ORDER BY fact.created_at_ms
            "#,
        )
        .fetch_all(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to inspect migrated usage facts: {error}"))?;
        let expected: Vec<(Uuid, String, String, String)> = expected
            .into_iter()
            .map(|(usage_fact_id, outcome)| {
                (usage_fact_id, outcome.clone(), outcome, "legacy".to_owned())
            })
            .collect();
        require(
            migrated == expected,
            &format!(
                "0062-0064 changed existing provider usage fact semantics; \
                 expected {expected:?}, got {migrated:?}"
            ),
        )
    }
    .await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn request_dimension_migration_rejects_an_unsafe_existing_quote_backfill() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };
    let result = async {
        sqlx::query("CREATE TABLE customer_price_quotes (quote_id BIGINT PRIMARY KEY)")
            .execute(&test_schema.pool)
            .await
            .map_err(|error| format!("failed to create legacy quote fixture: {error}"))?;
        sqlx::query("INSERT INTO customer_price_quotes (quote_id) VALUES (1)")
            .execute(&test_schema.pool)
            .await
            .map_err(|error| format!("failed to seed legacy quote fixture: {error}"))?;

        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0065_customer_pricing_request_dimensions.sql"
            ))
            .execute(&test_schema.pool)
            .await
            .is_err(),
            "0065 silently assigned unknown request dimensions to an existing paid quote",
        )
    }
    .await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn provider_account_runtime_changes_emit_commit_notifications() -> TestResult {
    let Some(test_schema) = TestSchema::new(4).await? else {
        return Ok(());
    };

    let result = async {
        gateway_result(run_migrations(&test_schema.pool).await, "migration failed")?;
        let mut listener = PgListener::connect_with(&test_schema.pool)
            .await
            .map_err(|error| format!("failed to create runtime listener: {error}"))?;
        listener
            .listen("ai_image_factory_provider_account_runtime")
            .await
            .map_err(|error| format!("failed to listen for runtime changes: {error}"))?;

        let credential_pool_id = Uuid::new_v4();
        let provider_account_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO provider_credential_pools
              (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms)
            VALUES ($1, 'runtime-event-pool', 'openai-codex', 'enabled', 1, 1)
            "#,
        )
        .bind(credential_pool_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed credential pool: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_accounts
              (provider_account_id, credential_pool_id, provider_id, account_key,
               credential_ref, credential_revision, credential_auth_sha256,
               state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'openai-codex', 'runtime-event-account',
                    'managed://runtime-event-account', 1, $3, 'enabled', 1, 1)
            "#,
        )
        .bind(provider_account_id)
        .bind(credential_pool_id)
        .bind("a".repeat(64))
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed provider account: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_execution_controls
              (provider_account_id, desired_max_concurrency, lifecycle_state,
               control_version, created_at_ms, updated_at_ms)
            VALUES ($1, 10, 'active', 1, 1, 1)
            "#,
        )
        .bind(provider_account_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed execution control: {error}"))?;

        sqlx::query(
            "UPDATE provider_account_execution_controls SET updated_at_ms = 2 WHERE provider_account_id = $1",
        )
        .bind(provider_account_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to apply no-op runtime update: {error}"))?;
        require(
            timeout(Duration::from_millis(100), listener.recv())
                .await
                .is_err(),
            "unrelated control updates must not emit runtime notifications",
        )?;

        sqlx::query(
            r#"
            UPDATE provider_account_execution_controls
            SET desired_max_concurrency = 9,
                control_version = 2,
                updated_at_ms = 2
            WHERE provider_account_id = $1
            "#,
        )
        .bind(provider_account_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to update execution control: {error}"))?;

        let notification = timeout(Duration::from_secs(2), listener.recv())
            .await
            .map_err(|_| "runtime notification timed out".to_string())?
            .map_err(|error| format!("failed to receive runtime notification: {error}"))?;
        require(
            notification.payload() == provider_account_id.to_string(),
            "runtime notification did not identify the changed provider account",
        )
    }
    .await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn credential_broker_serializes_refresh_and_promotes_observed_metadata() -> TestResult {
    let Some(test_schema) = TestSchema::new(4).await? else {
        return Ok(());
    };

    let result = async {
        gateway_result(run_migrations(&test_schema.pool).await, "migration failed")?;
        let credential_pool_id = Uuid::new_v4();
        let provider_account_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO provider_credential_pools
              (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms)
            VALUES ($1, 'broker-test-pool', 'openai-codex', 'enabled', 1, 1)
            "#,
        )
        .bind(credential_pool_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed credential pool: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_accounts
              (provider_account_id, credential_pool_id, provider_id, account_key,
               credential_ref, credential_revision, credential_auth_sha256,
               state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'openai-codex', 'broker-test-account',
                    'managed://broker-test-account', 1, $3, 'enabled', 1, 1)
            "#,
        )
        .bind(provider_account_id)
        .bind(credential_pool_id)
        .bind("a".repeat(64))
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed provider account: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_environments
              (provider_account_id, provider_id, environment_kind, environment_ref,
               upstream_identity_sha256, display_name, account_email, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, 'openai-codex', 'codex_home_v1', '/tmp/broker-test-account',
                    $2, 'Broker test', NULL, 'active', 1, 1)
            "#,
        )
        .bind(provider_account_id)
        .bind("b".repeat(64))
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed provider environment: {error}"))?;

        let store = PostgresCredentialStore::new(test_schema.pool.clone());
        let initial = store
            .resolve(provider_account_id)
            .await
            .map_err(|error| format!("initial credential should resolve: {error:?}"))?;
        require(
            initial.revision == 1 && initial.access_expires_at_ms.is_none(),
            "trigger must initialize the first operational credential revision",
        )?;

        let first_store = store.clone();
        let second_store = store.clone();
        let (first, second) = tokio::join!(
            first_store.claim_refresh(provider_account_id, "broker-a", 60_000, true),
            second_store.claim_refresh(provider_account_id, "broker-b", 60_000, true)
        );
        let first = first.map_err(|error| format!("first refresh claim failed: {error:?}"))?;
        let second = second.map_err(|error| format!("second refresh claim failed: {error:?}"))?;
        require(
            first.is_some() ^ second.is_some(),
            "two brokers acquired the same account refresh lease",
        )?;
        let lease = first.or(second).ok_or("refresh lease was not acquired")?;
        require(
            store.resolve(provider_account_id).await
                == Err(CredentialResolveError::Unavailable),
            "execution must fail closed while a credential refresh owns the account",
        )?;

        let expires_at_ms: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp() + interval '2 hours') * 1000)::BIGINT",
        )
        .fetch_one(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to read database clock: {error}"))?;
        let promoted_revision = store
            .promote_auth_file(&lease, &"f".repeat(64), Some(expires_at_ms))
            .await
            .map_err(|error| format!("metadata-only promotion failed: {error:?}"))?;
        require(
            promoted_revision == 2,
            "new expiry metadata must create an immutable credential revision",
        )?;
        let promoted = store
            .resolve(provider_account_id)
            .await
            .map_err(|error| format!("promoted credential should resolve: {error:?}"))?;
        require(
            promoted.revision == 2
                && promoted.material_fingerprint_sha256 == "f".repeat(64)
                && promoted.access_expires_at_ms == Some(expires_at_ms),
            "credential head did not expose the promoted revision",
        )?;
        require(
            store
                .promote_auth_file(&lease, &"c".repeat(64), Some(expires_at_ms))
                .await
                == Err(CredentialResolveError::Unavailable),
            "a released refresh lease mutated the credential head",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_account_credential_revisions SET access_expires_at_ms = NULL WHERE provider_account_id = $1 AND revision = 2",
            )
            .bind(provider_account_id)
            .execute(&test_schema.pool)
            .await
            .is_err(),
            "credential revision ledger accepted an update",
        )?;
        let expired_lease = store
            .claim_refresh(
                provider_account_id,
                "broker-authorization-check",
                60_000,
                true,
            )
            .await
            .map_err(|error| format!("authorization check claim failed: {error:?}"))?
            .ok_or_else(|| "authorization check lease was not acquired".to_owned())?;
        store
            .fail_refresh(
                &expired_lease,
                "codex_reauthorization_required",
                true,
            )
            .await
            .map_err(|error| format!("authorization failure was not recorded: {error:?}"))?;
        require(
            store.resolve(provider_account_id).await
                == Err(CredentialResolveError::ReauthorizationRequired),
            "expired Codex authorization remained schedulable",
        )?;
        let expired_state: (String, i32, Option<String>) = sqlx::query_as(
            r#"
            SELECT lifecycle_state, consecutive_failures, last_error_code
            FROM provider_account_credential_heads
            WHERE provider_account_id = $1
            "#,
        )
        .bind(provider_account_id)
        .fetch_one(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to inspect expired credential state: {error}"))?;
        require(
            expired_state
                == (
                    "reauth_required".to_owned(),
                    1,
                    Some("codex_reauthorization_required".to_owned()),
                ),
            &format!("expired Codex authorization was not surfaced: {expired_state:?}"),
        )?;
        let event_types: Vec<String> = sqlx::query_scalar(
            "SELECT event_type FROM provider_account_credential_events WHERE provider_account_id = $1 ORDER BY created_at_ms, event_type",
        )
        .bind(provider_account_id)
        .fetch_all(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to read credential events: {error}"))?;
        require(
            event_types.iter().any(|event| event == "refresh_claimed")
                && event_types.iter().any(|event| event == "refresh_succeeded"),
            "credential refresh audit events are incomplete",
        )?;

        let first_reauthorization = Uuid::new_v4();
        let second_reauthorization = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO provider_account_login_sessions
              (login_session_id, provider_id, account_key, display_name,
               environment_ref, status, login_method, max_concurrency,
               provider_account_id, expires_at_ms, created_at_ms, updated_at_ms)
            VALUES ($1, 'openai-codex', 'reauth-test-a', 'Reauth test',
                    '/tmp/reauth-test-a', 'waiting_for_user', 'browser_oauth', 1,
                    $2, 10000, 1, 1)
            "#,
        )
        .bind(first_reauthorization)
        .bind(provider_account_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed reauthorization session: {error}"))?;
        let duplicate_reauthorization = sqlx::query(
            r#"
            INSERT INTO provider_account_login_sessions
              (login_session_id, provider_id, account_key, display_name,
               environment_ref, status, login_method, max_concurrency,
               provider_account_id, expires_at_ms, created_at_ms, updated_at_ms)
            VALUES ($1, 'openai-codex', 'reauth-test-b', 'Reauth test',
                    '/tmp/reauth-test-b', 'starting', 'browser_oauth', 1,
                    $2, 10000, 1, 1)
            "#,
        )
        .bind(second_reauthorization)
        .bind(provider_account_id)
        .execute(&test_schema.pool)
        .await;
        require(
            duplicate_reauthorization.is_err(),
            "the same provider account accepted concurrent reauthorization sessions",
        )?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_credential_events
              (credential_event_id, provider_account_id, event_type, from_revision,
               to_revision, created_at_ms)
            VALUES ($1, $2, 'reauth_succeeded', 2, 2, 2)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(provider_account_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("reauthorization audit event was rejected: {error}"))?;

        let dreamina_pool_id = Uuid::new_v4();
        let dreamina_account_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO provider_credential_pools
              (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms)
            VALUES ($1, 'dreamina-broker-test-pool', 'dreamina-cli', 'enabled', 1, 1)
            "#,
        )
        .bind(dreamina_pool_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed Dreamina credential pool: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_accounts
              (provider_account_id, credential_pool_id, provider_id, account_key,
               credential_ref, credential_revision, credential_auth_sha256,
               state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'dreamina-cli', 'dreamina-broker-test-account',
                    'managed://dreamina-broker-test-account', 1, $3, 'enabled', 1, 1)
            "#,
        )
        .bind(dreamina_account_id)
        .bind(dreamina_pool_id)
        .bind("d".repeat(64))
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed Dreamina provider account: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_environments
              (provider_account_id, provider_id, environment_kind, environment_ref,
               upstream_identity_sha256, display_name, account_email, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, 'dreamina-cli', 'dreamina_home_v1', '/tmp/dreamina-broker-test-account',
                    $2, 'Dreamina broker test', NULL, 'active', 1, 1)
            "#,
        )
        .bind(dreamina_account_id)
        .bind("e".repeat(64))
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed Dreamina provider environment: {error}"))?;
        let dreamina_state: (String, String, String) = sqlx::query_as(
            r#"
            SELECT revision.material_kind, head.lifecycle_state, head.refresh_strategy
            FROM provider_account_credential_heads head
            JOIN provider_account_credential_revisions revision
              ON revision.provider_account_id = head.provider_account_id
             AND revision.revision = head.active_revision
            WHERE head.provider_account_id = $1
            "#,
        )
        .bind(dreamina_account_id)
        .fetch_one(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to read Dreamina credential state: {error}"))?;
        require(
            dreamina_state
                == (
                    "system_keyring".to_string(),
                    "active".to_string(),
                    "cli_managed".to_string(),
                ),
            "new Dreamina accounts must use the isolated CLI-managed credential contract",
        )?;
        let dreamina_credential = store
            .resolve(dreamina_account_id)
            .await
            .map_err(|error| format!("Dreamina credential should resolve: {error:?}"))?;
        require(
            dreamina_credential.provider_id == "dreamina-cli"
                && dreamina_credential.home()
                    == std::path::Path::new("/tmp/dreamina-broker-test-account"),
            "Dreamina credential did not resolve to its isolated account home",
        )?;
        let dreamina_lease = store
            .claim_refresh(dreamina_account_id, "dreamina-cli-manager", 60_000, true)
            .await
            .map_err(|error| format!("Dreamina refresh claim failed: {error:?}"))?
            .ok_or("Dreamina CLI-managed credential was not claimable")?;
        require(
            store.resolve(dreamina_account_id).await
                == Err(CredentialResolveError::Unavailable),
            "Dreamina execution remained available while its keyring refresh lease was active",
        )?;
        store
            .complete_cli_managed_refresh(&dreamina_lease)
            .await
            .map_err(|error| format!("Dreamina refresh completion failed: {error:?}"))?;
        let refreshed_dreamina = store
            .resolve(dreamina_account_id)
            .await
            .map_err(|error| format!("refreshed Dreamina credential should resolve: {error:?}"))?;
        require(
            refreshed_dreamina.revision == dreamina_credential.revision
                && refreshed_dreamina.material_kind == "system_keyring",
            "CLI-managed refresh changed the opaque Dreamina keyring revision",
        )?;
        let refresh_state: (String, i32, bool) = sqlx::query_as(
            r#"
            SELECT lifecycle_state, consecutive_failures,
                   next_refresh_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            FROM provider_account_credential_heads
            WHERE provider_account_id = $1
            "#,
        )
        .bind(dreamina_account_id)
        .fetch_one(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to inspect Dreamina refresh state: {error}"))?;
        require(
            refresh_state == ("active".to_owned(), 0, true),
            "Dreamina CLI-managed refresh did not schedule its next health check",
        )
    }
    .await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn active_video_pricing_is_fail_closed_and_requires_a_positive_success_price() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };
    let result = async {
        gateway_result(run_migrations(&test_schema.pool).await, "migration failed")?;
        let seeded_free_prices: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM price_versions WHERE state = 'active' AND billing_metric = 'video_second' AND success_micros = 0",
        )
        .fetch_one(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to inspect video pricing: {error}"))?;
        require(
            seeded_free_prices == 0,
            "migration must not publish a free active video price",
        )?;

        let zero_price_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO price_versions
              (price_version_id, price_key, version, api_profile, operation, provider_id, model,
               billing_metric, billing_unit, currency,
               success_micros, failed_micros, no_effect_micros, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, 'zero-video-price', 1, 'xai-videos-v1', 'video_generation',
                    'grok-cli', '*', 'video_second', 'second', 'USD',
                    0, 0, 0, 'draft', 1, 1)
            "#,
        )
        .bind(zero_price_id)
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("failed to seed draft video price: {error}"))?;
        require(
            sqlx::query("UPDATE price_versions SET state = 'active', updated_at_ms = 2 WHERE price_version_id = $1")
                .bind(zero_price_id)
                .execute(&test_schema.pool)
                .await
                .is_err(),
            "zero-success video price became active",
        )?;

        sqlx::query(
            r#"
            INSERT INTO price_versions
              (price_version_id, price_key, version, api_profile, operation, provider_id, model,
               billing_metric, billing_unit, currency,
               success_micros, failed_micros, no_effect_micros, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, 'paid-video-price', 1, 'xai-videos-v1', 'video_generation',
                    'grok-cli', '*', 'video_second', 'second', 'USD',
                    10, 0, 0, 'active', 1, 1)
            "#,
        )
        .bind(Uuid::new_v4())
        .execute(&test_schema.pool)
        .await
        .map_err(|error| format!("positive video price was rejected: {error}"))?;
        Ok(())
    }
    .await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn verification_fails_closed_for_invalid_migration_metadata() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = verification_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn both_stores_share_one_connection_pool() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = shared_pool_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn default_pool_pins_public_despite_url_search_path_options() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = default_pool_case(&test_schema).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn execution_context_migration_requires_legacy_active_jobs_to_be_drained() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = execution_context_upgrade_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn artifact_authority_migration_rejects_untrusted_existing_manifests() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = artifact_authority_upgrade_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn observation_migration_rejects_existing_projection_splits() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = observation_resolution_upgrade_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn execution_profile_migration_waits_for_old_writers_before_drain_check() -> TestResult {
    let Some(test_schema) = TestSchema::new(3).await? else {
        return Ok(());
    };

    let result = execution_profile_upgrade_race_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn economic_ledger_is_balanced_at_commit_and_append_only() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };
    let result = economic_ledger_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

async fn economic_ledger_case(pool: &PgPool) -> TestResult {
    gateway_result(
        run_migrations(pool).await,
        "economic migration should succeed",
    )?;
    let debit_id = Uuid::new_v4();
    let credit_id = Uuid::new_v4();
    for (account_id, key, account_type) in [
        (debit_id, "test:receivable", "receivable"),
        (credit_id, "test:revenue", "revenue"),
    ] {
        sqlx::query(
            r#"
            INSERT INTO ledger_accounts
              (account_id, account_key, owner_type, owner_id, account_type, currency, created_at_ms)
            VALUES ($1, $2, 'platform', 'test', $3, 'USD', 1)
            "#,
        )
        .bind(account_id)
        .bind(key)
        .bind(account_type)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to seed ledger account: {error}"))?;
    }

    let ungoverned_adjustment = sqlx::query(
        r#"
        INSERT INTO ledger_transactions
          (transaction_id, semantic_key, transaction_type, currency, payload_hash, created_at_ms)
        VALUES ($1, $2, 'adjustment', 'USD', $3, 1)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(format!("ungoverned:{}", Uuid::new_v4()))
    .bind("0".repeat(64))
    .execute(pool)
    .await
    .expect_err("generic adjustments must be rejected without business evidence");
    require(
        ungoverned_adjustment
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref()
            == Some("55000"),
        &format!("unexpected generic adjustment error: {ungoverned_adjustment}"),
    )?;

    // The remaining assertions exercise the lower-level ledger guards in this
    // isolated schema, independently of the product-level adjustment gate.
    sqlx::query(
        "ALTER TABLE ledger_transactions DISABLE TRIGGER ledger_transactions_reject_ungoverned_adjustment",
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to isolate the ledger guard test: {error}"))?;

    let empty_transaction_id = Uuid::new_v4();
    let mut empty = pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin empty ledger transaction: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions
          (transaction_id, semantic_key, transaction_type, currency, payload_hash, created_at_ms)
        VALUES ($1, $2, 'adjustment', 'USD', $3, 1)
        "#,
    )
    .bind(empty_transaction_id)
    .bind(format!("empty:{empty_transaction_id}"))
    .bind("1".repeat(64))
    .execute(&mut *empty)
    .await
    .map_err(|error| format!("failed to stage empty ledger transaction: {error}"))?;
    require(
        empty.commit().await.is_err(),
        "empty ledger transaction committed",
    )?;

    let transaction_id = Uuid::new_v4();
    let mut balanced = pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin balanced ledger transaction: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions
          (transaction_id, semantic_key, transaction_type, currency, payload_hash, created_at_ms)
        VALUES ($1, $2, 'adjustment', 'USD', $3, 1)
        "#,
    )
    .bind(transaction_id)
    .bind(format!("balanced:{transaction_id}"))
    .bind("2".repeat(64))
    .execute(&mut *balanced)
    .await
    .map_err(|error| format!("failed to stage balanced ledger transaction: {error}"))?;
    for (posting_no, account_id, amount) in [(1_i16, debit_id, 9_i64), (2, credit_id, -9)] {
        sqlx::query(
            r#"
            INSERT INTO ledger_postings
              (transaction_id, posting_no, account_id, currency, amount_micros, created_at_ms)
            VALUES ($1, $2, $3, 'USD', $4, 1)
            "#,
        )
        .bind(transaction_id)
        .bind(posting_no)
        .bind(account_id)
        .bind(amount)
        .execute(&mut *balanced)
        .await
        .map_err(|error| format!("failed to stage balanced posting: {error}"))?;
    }
    sqlx::query(
        "INSERT INTO ledger_transaction_seals (transaction_id, sealed_at_ms) VALUES ($1, 1)",
    )
    .bind(transaction_id)
    .execute(&mut *balanced)
    .await
    .map_err(|error| format!("failed to seal balanced ledger transaction: {error}"))?;
    balanced
        .commit()
        .await
        .map_err(|error| format!("balanced ledger transaction was rejected: {error}"))?;
    require(
        sqlx::query("UPDATE ledger_postings SET amount_micros = 10 WHERE transaction_id = $1")
            .bind(transaction_id)
            .execute(pool)
            .await
            .is_err(),
        "append-only ledger posting was mutated",
    )?;
    require(
        sqlx::query(
            r#"
            INSERT INTO ledger_postings
              (transaction_id, posting_no, account_id, currency, amount_micros, created_at_ms)
            VALUES ($1, 3, $2, 'USD', 1, 2)
            "#,
        )
        .bind(transaction_id)
        .bind(debit_id)
        .execute(pool)
        .await
        .is_err(),
        "sealed ledger transaction accepted another posting",
    )?;
    sqlx::query(
        "ALTER TABLE ledger_transactions ENABLE TRIGGER ledger_transactions_reject_ungoverned_adjustment",
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to restore the adjustment guard: {error}"))?;
    Ok(())
}

async fn execution_context_upgrade_case(pool: &PgPool) -> TestResult {
    for migration in [
        include_str!("../migrations/0000_legacy_reconciliation.sql"),
        include_str!("../migrations/0001_usage.sql"),
        include_str!("../migrations/0002_durable_admission.sql"),
        include_str!("../migrations/0003_durable_scheduling.sql"),
        include_str!("../migrations/0004_api_key_hmac.sql"),
        include_str!("../migrations/0005_artifact_replay.sql"),
    ] {
        sqlx::raw_sql(migration)
            .execute(pool)
            .await
            .map_err(|error| format!("pre-0006 migration failed: {error}"))?;
    }
    let job_id = Uuid::new_v4();
    let reservation_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model, state,
           requested_units, reservation_id, created_at_ms, updated_at_ms)
        VALUES ($1, 'tenant_upgrade', 'request_upgrade', 'generation',
                'openai-codex', 'gpt-image-2', 'reserved', 1, $2, 1, 1)
        "#,
    )
    .bind(job_id)
    .bind(reservation_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert legacy job: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO quota_reservations
          (reservation_id, tenant_id, request_id, job_id, requested_units,
           state, created_at_ms, updated_at_ms, expires_at_ms)
        VALUES ($1, 'tenant_upgrade', 'request_upgrade', $2, 1,
                'reserved', 1, 1, 9999999999999)
        "#,
    )
    .bind(reservation_id)
    .bind(job_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert legacy reservation: {error}"))?;

    require(
        sqlx::raw_sql(include_str!("../migrations/0006_execution_context.sql"))
            .execute(pool)
            .await
            .is_err(),
        "0006 must reject an active legacy reservation without a quota snapshot",
    )?;
    let snapshot_column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'quota_reservations' AND column_name = 'limit_5h')",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect rolled-back migration: {error}"))?;
    require(
        !snapshot_column_exists,
        "failed 0006 migration must roll back its schema changes",
    )?;

    sqlx::query("UPDATE jobs SET state = 'failed' WHERE job_id = $1")
        .bind(job_id)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to terminalize legacy job: {error}"))?;
    sqlx::query(
        "UPDATE quota_reservations SET state = 'released', released_units = requested_units WHERE reservation_id = $1",
    )
    .bind(reservation_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to release legacy reservation: {error}"))?;
    sqlx::raw_sql(include_str!("../migrations/0006_execution_context.sql"))
        .execute(pool)
        .await
        .map_err(|error| format!("0006 should accept a drained legacy queue: {error}"))?;
    let snapshots: (Option<i32>, Option<i32>, Option<i32>, Option<i32>) = sqlx::query_as(
        "SELECT limit_5h, remaining_5h, limit_7d, remaining_7d FROM quota_reservations WHERE reservation_id = $1",
    )
    .bind(reservation_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to read migrated legacy reservation: {error}"))?;
    require(
        snapshots == (None, None, None, None),
        "terminal legacy snapshots must remain consistently NULL",
    )
}

async fn artifact_authority_upgrade_case(pool: &PgPool) -> TestResult {
    for migration in [
        include_str!("../migrations/0000_legacy_reconciliation.sql"),
        include_str!("../migrations/0001_usage.sql"),
        include_str!("../migrations/0002_durable_admission.sql"),
        include_str!("../migrations/0003_durable_scheduling.sql"),
        include_str!("../migrations/0004_api_key_hmac.sql"),
        include_str!("../migrations/0005_artifact_replay.sql"),
        include_str!("../migrations/0006_execution_context.sql"),
        include_str!("../migrations/0007_edit_inputs.sql"),
        include_str!("../migrations/0008_provider_submissions.sql"),
        include_str!("../migrations/0009_economic_kernel.sql"),
    ] {
        sqlx::raw_sql(migration)
            .execute(pool)
            .await
            .map_err(|error| format!("pre-0010 migration failed: {error}"))?;
    }
    sqlx::raw_sql(
        r#"
        DO $$
        DECLARE constraint_name TEXT;
        BEGIN
            FOR constraint_name IN
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'executor_result_manifests'::regclass
                  AND contype = 'f'
            LOOP
                EXECUTE format(
                    'ALTER TABLE executor_result_manifests DROP CONSTRAINT %I',
                    constraint_name
                );
            END LOOP;
        END;
        $$;
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to isolate legacy manifest fixture: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO executor_result_manifests
          (manifest_id, executor_execution_id, submission_id, storage_backend,
           object_key, sha256_hex, byte_size, media_type, created_at_ms)
        VALUES ($1, $2, $3, 'legacy', 'legacy/object', $4, 1, 'image/png', 1)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind("a".repeat(64))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed untrusted legacy manifest: {error}"))?;

    require(
        sqlx::raw_sql(include_str!(
            "../migrations/0010_executor_artifact_authority.sql"
        ))
        .execute(pool)
        .await
        .is_err(),
        "0010 accepted caller-supplied legacy artifact metadata",
    )?;
    let authority_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('executor_artifact_authorities') IS NOT NULL")
            .fetch_one(pool)
            .await
            .map_err(|error| {
                format!("failed to inspect rolled-back authority migration: {error}")
            })?;
    require(
        !authority_table_exists,
        "failed 0010 migration did not roll back its schema changes",
    )
}

async fn observation_resolution_upgrade_case(pool: &PgPool) -> TestResult {
    for migration in [
        include_str!("../migrations/0000_legacy_reconciliation.sql"),
        include_str!("../migrations/0001_usage.sql"),
        include_str!("../migrations/0002_durable_admission.sql"),
        include_str!("../migrations/0003_durable_scheduling.sql"),
        include_str!("../migrations/0004_api_key_hmac.sql"),
        include_str!("../migrations/0005_artifact_replay.sql"),
        include_str!("../migrations/0006_execution_context.sql"),
        include_str!("../migrations/0007_edit_inputs.sql"),
        include_str!("../migrations/0008_provider_submissions.sql"),
        include_str!("../migrations/0009_economic_kernel.sql"),
        include_str!("../migrations/0010_executor_artifact_authority.sql"),
    ] {
        sqlx::raw_sql(migration)
            .execute(pool)
            .await
            .map_err(|error| format!("pre-0011 migration failed: {error}"))?;
    }
    sqlx::raw_sql(
        r#"
        DO $$
        DECLARE constraint_name TEXT;
        BEGIN
            FOR constraint_name IN
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'provider_submissions'::regclass
                  AND contype = 'f'
            LOOP
                EXECUTE format(
                    'ALTER TABLE provider_submissions DROP CONSTRAINT %I',
                    constraint_name
                );
            END LOOP;
        END;
        $$;
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to isolate projection split fixture: {error}"))?;
    let submission_id = Uuid::new_v4();
    let executor_execution_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_submissions
          (submission_id, executor_execution_id, output_id, job_id,
           tenant_id, provider_id, model, work_item_id,
           created_by_execution_id, created_by_lease_epoch,
           command_schema, command_hash, state,
           prepared_at_ms, started_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, 'tenant', 'provider', 'model', $5,
                $6, 1, 'command-v1', $7, 'running', 1, 1, 1)
        "#,
    )
    .bind(submission_id)
    .bind(executor_execution_id)
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind("a".repeat(64))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed split submission: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO executor_executions
          (executor_execution_id, submission_id, state, executor_owner,
           lease_epoch, lease_expires_at_ms, created_at_ms, leased_at_ms,
           updated_at_ms)
        VALUES ($1, $2, 'leased', 'executor', 1, 9999999999999, 1, 1, 1)
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed split execution: {error}"))?;

    require(
        sqlx::raw_sql(include_str!(
            "../migrations/0011_executor_observation_resolution.sql"
        ))
        .execute(pool)
        .await
        .is_err(),
        "0011 accepted an existing executor/submission projection split",
    )?;
    let launch_column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'executor_executions' AND column_name = 'launch_owner')",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect rolled-back observation migration: {error}"))?;
    require(
        !launch_column_exists,
        "failed 0011 migration did not roll back its schema changes",
    )
}

async fn execution_profile_upgrade_race_case(pool: &PgPool) -> TestResult {
    for migration in [
        include_str!("../migrations/0000_legacy_reconciliation.sql"),
        include_str!("../migrations/0001_usage.sql"),
        include_str!("../migrations/0002_durable_admission.sql"),
        include_str!("../migrations/0003_durable_scheduling.sql"),
        include_str!("../migrations/0004_api_key_hmac.sql"),
        include_str!("../migrations/0005_artifact_replay.sql"),
        include_str!("../migrations/0006_execution_context.sql"),
        include_str!("../migrations/0007_edit_inputs.sql"),
        include_str!("../migrations/0008_provider_submissions.sql"),
        include_str!("../migrations/0009_economic_kernel.sql"),
        include_str!("../migrations/0010_executor_artifact_authority.sql"),
        include_str!("../migrations/0011_executor_observation_resolution.sql"),
        include_str!("../migrations/0012_executor_pending_evidence_index.sql"),
    ] {
        sqlx::raw_sql(migration)
            .execute(pool)
            .await
            .map_err(|error| format!("pre-0013 migration failed: {error}"))?;
    }
    sqlx::raw_sql(
        r#"
        DO $$
        DECLARE constraint_name TEXT;
        BEGIN
            FOR constraint_name IN
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'provider_submissions'::regclass
                  AND contype = 'f'
            LOOP
                EXECUTE format(
                    'ALTER TABLE provider_submissions DROP CONSTRAINT %I',
                    constraint_name
                );
            END LOOP;
        END;
        $$;
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to isolate old executor writer: {error}"))?;

    let submission_id = Uuid::new_v4();
    let executor_execution_id = Uuid::new_v4();
    let mut old_writer = pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin old writer: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_submissions
          (submission_id, executor_execution_id, output_id, job_id,
           tenant_id, provider_id, model, work_item_id,
           created_by_execution_id, created_by_lease_epoch,
           command_schema, command_hash, state, prepared_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, 'tenant', 'provider', 'model', $5,
                $6, 1, 'command-v1', $7, 'prepared', 1, 1)
        "#,
    )
    .bind(submission_id)
    .bind(executor_execution_id)
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind("a".repeat(64))
    .execute(&mut *old_writer)
    .await
    .map_err(|error| format!("failed to stage old submission: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO executor_executions
          (executor_execution_id, submission_id, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'prepared', 1, 1)
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .execute(&mut *old_writer)
    .await
    .map_err(|error| format!("failed to stage old execution: {error}"))?;

    let migration_pool = pool.clone();
    let mut migration = tokio::spawn(async move {
        sqlx::raw_sql(include_str!(
            "../migrations/0013_executor_execution_profiles.sql"
        ))
        .execute(&migration_pool)
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    require(
        !migration.is_finished(),
        "0013 did not serialize with the in-flight old executor writer",
    )?;
    old_writer
        .commit()
        .await
        .map_err(|error| format!("failed to commit old writer: {error}"))?;
    let migration_result = timeout(Duration::from_secs(5), &mut migration)
        .await
        .map_err(|_| "0013 did not finish after old writer committed".to_string())?
        .map_err(|error| format!("0013 task failed: {error}"))?;
    require(
        migration_result.is_err(),
        "0013 missed the active row committed by an old executor writer",
    )?;
    let profile_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('provider_execution_profiles') IS NOT NULL")
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to inspect rolled-back 0013: {error}"))?;
    require(
        !profile_table_exists,
        "failed 0013 migration did not roll back its schema changes",
    )
}

async fn default_pool_case(test_schema: &TestSchema) -> TestResult {
    let database_url = env::var("TEST_DATABASE_URL")
        .map_err(|_| "TEST_DATABASE_URL disappeared during test".to_string())?;
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let injected_url = format!(
        "{database_url}{separator}options=-csearch_path%3D{}",
        test_schema.name
    );
    let pool = connect_pool(&injected_url, 1)
        .await
        .map_err(|error| format!("default pool should connect: {error:?}"))?;
    let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(&pool)
        .await
        .map_err(|error| format!("failed to read default pool schema: {error}"))?;
    pool.close().await;
    require(
        current_schema == "public",
        &format!("default pool resolved to {current_schema:?}, expected public"),
    )
}

async fn legacy_schema_case(pool: &PgPool) -> TestResult {
    sqlx::raw_sql(
        r#"
        CREATE TABLE usage_events (
            event_id UUID PRIMARY KEY,
            request_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            units INTEGER NOT NULL CHECK (units > 0),
            outcome TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL
        );

        CREATE TABLE quota_reservations (
            reservation_id UUID PRIMARY KEY,
            request_id TEXT NOT NULL,
            requested_units INTEGER NOT NULL CHECK (requested_units > 0),
            started_units INTEGER NOT NULL DEFAULT 0,
            released_units INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            expires_at_ms BIGINT NOT NULL
        );

        CREATE TABLE jobs (
            job_id UUID PRIMARY KEY,
            request_id TEXT NOT NULL,
            state TEXT NOT NULL,
            requested_units INTEGER NOT NULL,
            charged_units INTEGER NOT NULL DEFAULT 0,
            queue_entered_at_ms BIGINT,
            started_at_ms BIGINT,
            finished_at_ms BIGINT
        );
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to create legacy schema: {error}"))?;

    require(
        !migration_table_exists(pool).await?,
        "legacy schema must start without _sqlx_migrations",
    )?;
    gateway_result(
        run_migrations(pool).await,
        "legacy schema migration should succeed",
    )?;
    gateway_result(
        verify_migrations(pool).await,
        "legacy schema verification should succeed",
    )?;
    assert_expected_schema(pool).await
}

async fn concurrent_migration_case(pool: &PgPool) -> TestResult {
    let (first, second) = tokio::join!(run_migrations(pool), run_migrations(pool));
    gateway_result(first, "first concurrent migration should succeed")?;
    gateway_result(second, "second concurrent migration should succeed")?;
    gateway_result(
        run_migrations(pool).await,
        "repeated migration should succeed",
    )?;
    gateway_result(
        verify_migrations(pool).await,
        "fresh schema verification should succeed",
    )?;
    assert_expected_schema(pool).await
}

async fn verification_case(pool: &PgPool) -> TestResult {
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a missing migration table",
    )?;
    require(
        !migration_table_exists(pool).await?,
        "verification must not create the migration table",
    )?;

    gateway_result(
        run_migrations(pool).await,
        "initial migration should succeed",
    )?;
    gateway_result(
        verify_migrations(pool).await,
        "current migrations should verify",
    )?;

    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 1")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create pending state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a pending migration",
    )?;
    gateway_result(
        run_migrations(pool).await,
        "pending migration should be restorable",
    )?;

    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 0")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create missing state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a missing migration",
    )?;
    gateway_result(
        run_migrations(pool).await,
        "missing migration should be restorable",
    )?;

    sqlx::query("UPDATE _sqlx_migrations SET success = false WHERE version = 1")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create dirty state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject an unsuccessful migration",
    )?;
    sqlx::query("UPDATE _sqlx_migrations SET success = true WHERE version = 1")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to restore dirty state: {error}"))?;

    let checksum: Vec<u8> =
        sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = 1")
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to read migration checksum: {error}"))?;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 1")
        .bind(vec![0_u8])
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create checksum mismatch: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a checksum mismatch",
    )?;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 1")
        .bind(checksum)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to restore migration checksum: {error}"))?;

    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (999, 'extra', true, $1, 0)",
    )
    .bind(vec![0_u8])
    .execute(pool)
    .await
    .map_err(|error| format!("failed to create extra migration state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a database newer than the running binary",
    )?;
    sqlx::query("UPDATE _sqlx_migrations SET success = false WHERE version = 999")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to alter newer migration state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject unsuccessful future migration metadata",
    )?;
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 999")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to remove extra migration: {error}"))?;

    gateway_result(
        verify_migrations(pool).await,
        "restored migration metadata should verify",
    )
}

async fn shared_pool_case(pool: &PgPool) -> TestResult {
    gateway_result(
        run_migrations(pool).await,
        "store schema migration should succeed",
    )?;
    let usage_store = PostgresUsageStore::new(pool.clone());
    let api_key_store = PostgresApiKeyStore::new(
        pool.clone(),
        ApiKeyKeyring::new(1, [(1, vec![0x22; 32])]).expect("test keyring must be valid"),
    );
    sqlx::query(
        r#"
        INSERT INTO gateway_projects (id, tenant_id, name, created_at)
        VALUES ('proj_blocked', 'proj_blocked', 'Blocked', 1),
               ('proj_ready', 'proj_ready', 'Ready', 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed credential projects: {error}"))?;
    let held_connection = pool
        .acquire()
        .await
        .map_err(|error| format!("failed to acquire sole test connection: {error}"))?;

    require(
        timeout(Duration::from_millis(100), pool.acquire())
            .await
            .is_err(),
        "max_connections(1) must prevent a second pool connection",
    )?;
    require(
        timeout(
            Duration::from_millis(100),
            usage_store.reserve(test_charge("usage-blocked")),
        )
        .await
        .is_err(),
        "usage store must use the shared pool",
    )?;
    require(
        timeout(
            Duration::from_millis(100),
            api_key_store.create_service_account(
                "proj_blocked",
                "Blocked",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            ),
        )
        .await
        .is_err(),
        "API key store must use the shared pool",
    )?;

    drop(held_connection);
    let (usage_result, api_key_result) = tokio::join!(
        usage_store.reserve(test_charge("usage-ready")),
        api_key_store.create_service_account(
            "proj_ready",
            "Ready",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        ),
    );
    usage_result.map_err(|error| format!("usage store should be usable: {error:?}"))?;
    api_key_result.map_err(|error| format!("API key store should be usable: {error:?}"))?;
    Ok(())
}

async fn assert_expected_schema(pool: &PgPool) -> TestResult {
    require(
        migration_versions(pool).await? == (0_i64..=119_i64).collect::<Vec<_>>(),
        "applied migration versions must be exactly 0 through 119",
    )?;

    let default_codex_prices: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT version.operation, component.unit_price_micros,
               count(binding.contract_key)::BIGINT
        FROM price_books book
        JOIN price_book_versions version USING (price_book_id)
        JOIN price_components component USING (price_book_version_id)
        LEFT JOIN price_book_version_surface_contract_bindings binding
          USING (price_book_version_id)
        WHERE book.purpose = 'customer_sale'
          AND book.scope_type = 'platform'
          AND book.state = 'active'
          AND version.state = 'active'
          AND version.api_profile = 'openai-images-v1'
          AND version.operation IN ('generation', 'edit')
          AND version.provider_id = 'openai-codex'
          AND version.provider_model_id = 'gpt-image-2'
          AND version.public_model_id = 'gpt-image-2'
          AND version.media_kind = 'image'
          AND version.execution_surface = 'provider_cli'
          AND component.outcome = 'succeeded'
        GROUP BY version.operation, component.unit_price_micros
        ORDER BY version.operation
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| format!("failed to inspect default Codex pricing: {error}"))?;
    require(
        default_codex_prices
            == vec![
                ("edit".to_string(), 40000, 1),
                ("generation".to_string(), 40000, 1),
            ],
        "fresh migrations must publish bound Codex generation and edit prices",
    )?;

    let default_grok_prices: Vec<(String, String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT version.operation, version.provider_model_id,
               component.unit_price_micros,
               count(binding.contract_key)::BIGINT
        FROM price_books book
        JOIN price_book_versions version USING (price_book_id)
        JOIN price_components component USING (price_book_version_id)
        LEFT JOIN price_book_version_surface_contract_bindings binding
          USING (price_book_version_id)
        WHERE book.purpose = 'customer_sale'
          AND book.scope_type = 'platform'
          AND book.state = 'active'
          AND version.state = 'active'
          AND version.api_profile = 'xai-images-v1'
          AND version.operation IN ('generation', 'edit')
          AND version.provider_id = 'grok-cli'
          AND version.provider_model_id IN (
              'grok-imagine-image', 'grok-imagine-image-quality'
          )
          AND version.public_model_id = version.provider_model_id
          AND version.media_kind = 'image'
          AND version.execution_surface = 'provider_cli'
          AND component.outcome = 'succeeded'
        GROUP BY version.operation, version.provider_model_id,
                 component.unit_price_micros
        ORDER BY version.operation, version.provider_model_id
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| format!("failed to inspect default Grok pricing: {error}"))?;
    require(
        default_grok_prices
            == vec![
                (
                    "edit".to_string(),
                    "grok-imagine-image-quality".to_string(),
                    50000,
                    1,
                ),
                (
                    "generation".to_string(),
                    "grok-imagine-image".to_string(),
                    20000,
                    1,
                ),
                (
                    "generation".to_string(),
                    "grok-imagine-image-quality".to_string(),
                    50000,
                    1,
                ),
            ],
        "fresh migrations must publish bound Grok generation and edit prices",
    )?;

    let grok_provider_actual: Vec<(String, String, String, i64)> = sqlx::query_as(
        r#"
        SELECT version.media_kind, version.api_profile, version.operation,
               count(component.price_component_id)::BIGINT
        FROM price_books book
        JOIN price_book_versions version USING (price_book_id)
        LEFT JOIN price_components component USING (price_book_version_id)
        WHERE book.price_book_key = 'provider_actual.grok-cli.reported'
          AND book.purpose = 'provider_actual'
          AND book.scope_type = 'platform'
          AND book.provider_id = 'grok-cli'
          AND book.state = 'active'
          AND version.state = 'active'
          AND version.provider_id = 'grok-cli'
          AND version.provider_model_id IS NULL
          AND version.public_model_id = '*'
          AND version.service_tier = '*'
          AND version.execution_surface = 'provider_cli'
          AND version.billing_mode = 'provider_reported'
        GROUP BY version.media_kind, version.api_profile, version.operation
        ORDER BY version.media_kind
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| format!("failed to inspect Grok provider actual pricing: {error}"))?;
    require(
        grok_provider_actual
            == vec![
                ("image".to_string(), "*".to_string(), "*".to_string(), 0),
                ("video".to_string(), "*".to_string(), "*".to_string(), 0),
            ],
        "fresh migrations must accept provider-reported Grok image and video costs",
    )?;

    let retention_policy: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT policy_version, retain_for_ms, read_drain_ms, retry_delay_ms
        FROM artifact_retention_policies
        WHERE policy_key = 'default'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect active artifact retention policy: {error}"))?;
    require(
        retention_policy == (2, 1_800_000, 60_000, 60_000),
        "active artifact retention policy must retain new results for 30 minutes",
    )?;

    for (table, column) in REQUIRED_COLUMNS {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2)",
        )
        .bind(table)
        .bind(column)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to query {table}.{column}: {error}"))?;
        require(exists, &format!("{table}.{column} must exist"))?;
    }

    for index in REQUIRED_INDEXES {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1)",
        )
        .bind(index)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to query index {index}: {error}"))?;
        require(exists, &format!("index {index} must exist"))?;
    }

    for table in [
        "project_files",
        "project_batches",
        "project_batch_requests",
        "project_batch_output_files",
    ] {
        let exists: bool =
            sqlx::query_scalar("SELECT to_regclass(current_schema() || '.' || $1) IS NOT NULL")
                .bind(table)
                .fetch_one(pool)
                .await
                .map_err(|error| format!("failed to inspect batch table {table}: {error}"))?;
        require(exists, &format!("batch table {table} must exist"))?;
    }

    for (table, column) in [
        ("project_files", "object_key"),
        ("project_files", "sha256_hex"),
        ("project_files", "cleanup_lease_epoch"),
        ("project_files", "cleanup_completed_at_ms"),
        ("project_batches", "auth_snapshot"),
        ("project_batches", "route_snapshot"),
        ("project_batches", "lease_epoch"),
        ("project_batches", "request_count_cancelled"),
        ("project_batches", "result_bytes"),
        ("project_batch_requests", "custom_id"),
        ("project_batch_requests", "request_hash"),
        ("project_batch_requests", "available_at_ms"),
        ("project_batch_requests", "attempt_count"),
        ("project_batch_requests", "last_error"),
        ("project_batch_requests", "lease_epoch"),
        ("project_batch_output_files", "role"),
    ] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2)",
        )
        .bind(table)
        .bind(column)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to inspect {table}.{column}: {error}"))?;
        require(exists, &format!("{table}.{column} must exist"))?;
    }

    for index in [
        "project_files_project_created_idx",
        "project_files_expiry_cleanup_idx",
        "project_files_project_storage_pending_idx",
        "project_files_cleanup_recovery_idx",
        "project_batches_project_created_idx",
        "project_batches_recovery_idx",
        "project_batch_requests_claim_idx",
        "project_batch_requests_lease_expiry_idx",
    ] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1)",
        )
        .bind(index)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to inspect batch index {index}: {error}"))?;
        require(exists, &format!("batch index {index} must exist"))?;
    }
    let claim_index: String = sqlx::query_scalar(
        r#"
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND indexname = 'project_batch_requests_claim_idx'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect batch claim index: {error}"))?;
    require(
        claim_index.contains("available_at_ms"),
        "batch claim index must support retry availability ordering",
    )?;
    let recovery_index: String = sqlx::query_scalar(
        r#"
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND indexname = 'project_batches_recovery_idx'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect batch recovery index: {error}"))?;
    require(
        recovery_index.contains("updated_at_ms")
            && recovery_index.contains("batch_id")
            && recovery_index.contains("validating"),
        "batch recovery index must match runnable scan ordering and statuses",
    )?;
    let cleanup_recovery_index: String = sqlx::query_scalar(
        r#"
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND indexname = 'project_files_cleanup_recovery_idx'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect file cleanup recovery index: {error}"))?;
    require(
        cleanup_recovery_index.contains("cleanup_lease_expires_at_ms")
            && cleanup_recovery_index.contains("cleanup_completed_at_ms IS NULL")
            && cleanup_recovery_index.contains("deleted_at_ms")
            && cleanup_recovery_index.contains("expires_at_ms"),
        "file cleanup recovery index must support pending and expired lease scans",
    )?;
    let project_file_limits: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT column_name::TEXT, column_default::TEXT
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'gateway_projects'
          AND column_name IN ('file_storage_limit_bytes', 'file_storage_limit_count')
        ORDER BY column_name
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| format!("failed to inspect project file storage limits: {error}"))?;
    require(
        project_file_limits.len() == 2
            && project_file_limits.iter().any(|(column, default)| {
                column == "file_storage_limit_bytes" && default.contains("2147483648")
            })
            && project_file_limits.iter().any(|(column, default)| {
                column == "file_storage_limit_count" && default.contains("1000")
            }),
        "project file storage limits must persist the 2 GiB and 1000 file defaults",
    )?;

    let source_check: String = sqlx::query_scalar(
        r#"
        SELECT pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'gateway_request_observations'::regclass
          AND conname = 'gateway_request_observations_source_check'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect request source check: {error}"))?;
    let source_check = source_check.to_ascii_lowercase();
    require(
        ["models", "images", "videos", "files", "batches"]
            .into_iter()
            .all(|source| source_check.contains(source)),
        "request observation source check must preserve existing sources and allow batches",
    )?;

    let batch_foreign_keys: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT lower(pg_get_constraintdef(oid))
        FROM pg_constraint
        WHERE conrelid IN (
            'project_files'::regclass,
            'project_batches'::regclass,
            'project_batch_requests'::regclass,
            'project_batch_output_files'::regclass
        )
          AND contype = 'f'
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| format!("failed to inspect batch foreign keys: {error}"))?;
    require(
        batch_foreign_keys.iter().all(|definition| {
            !definition.contains("project_batches")
                || (definition.contains("project_id") && definition.contains("tenant_id"))
        }) && batch_foreign_keys
            .iter()
            .filter(|definition| definition.contains("references project_files"))
            .all(|definition| {
                definition.contains("project_id") && definition.contains("tenant_id")
            }),
        "batch and file references must preserve project and tenant ownership",
    )?;

    let output_identity: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM pg_constraint
        WHERE conrelid = 'project_batch_output_files'::regclass
          AND contype IN ('p', 'u')
          AND pg_get_constraintdef(oid) ILIKE '%batch_id%role%'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect batch output role identity: {error}"))?;
    require(
        output_identity >= 1,
        "batch output roles must be unique per batch",
    )?;

    let readiness_columns: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT column_name::TEXT
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'provider_profile_readiness'
        ORDER BY ordinal_position
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| format!("failed to inspect provider readiness view: {error}"))?;
    require(
        readiness_columns
            == [
                "execution_profile_id",
                "profile_key",
                "provider_id",
                "status",
                "active_submitters",
                "active_pollers",
                "draining_submitters",
                "draining_pollers",
            ],
        "provider readiness view must expose only its fixed projection",
    )?;

    let capacity_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('enforce_executor_capacity_counter_balance()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect capacity counter guard: {error}"))?;
    require(
        capacity_guard.contains("OLD.state = 'held'")
            && capacity_guard.contains("NEW.state = 'held'")
            && capacity_guard.contains("LEFT JOIN executor_capacity_allocations"),
        "capacity counter guard must skip heartbeat-only updates and compare one snapshot",
    )?;

    let active_owner_index: (bool, bool, String, String) = sqlx::query_as(
        r#"
        SELECT metadata.indisvalid, metadata.indisready,
               pg_get_indexdef(metadata.indexrelid),
               pg_get_expr(metadata.indpred, metadata.indrelid)
        FROM pg_index metadata
        JOIN pg_class index_relation
          ON index_relation.oid = metadata.indexrelid
        JOIN pg_namespace namespace
          ON namespace.oid = index_relation.relnamespace
        WHERE namespace.nspname = current_schema()
          AND index_relation.relname = 'executor_executions_active_owner_idx'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect active owner index: {error}"))?;
    let active_owner_definition = active_owner_index.2.to_ascii_lowercase();
    let active_owner_predicate = active_owner_index.3.to_ascii_lowercase();
    require(
        active_owner_index.0
            && active_owner_index.1
            && active_owner_definition.contains("(executor_owner)")
            && !active_owner_definition.contains("lease_expires_at_ms")
            && active_owner_predicate.contains("executor_owner is not null")
            && active_owner_predicate.contains("leased")
            && active_owner_predicate.contains("running"),
        "active owner index must be valid, partial, and heartbeat-update friendly",
    )?;

    let recovery_deadline_constraint: Option<String> = sqlx::query_scalar(
        r#"
        SELECT pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'provider_submit_recoveries'::regclass
          AND conname = 'provider_submit_recoveries_lease_deadline_check'
        "#,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| format!("failed to query recovery deadline constraint: {error}"))?;
    require(
        recovery_deadline_constraint.is_some_and(|definition| {
            definition.contains("recovery_lease_expires_at_ms <= provider_deadline_at_ms")
        }),
        "provider recovery leases must be bounded by the absolute provider deadline",
    )?;
    let provider_heartbeat_triggers: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM pg_trigger
        WHERE NOT tgisinternal
          AND tgrelid IN (
            'executor_capacity_allocations'::regclass,
            'provider_remote_tasks'::regclass
          )
          AND tgname IN (
              'executor_capacity_allocations_heartbeat_time_guard',
              'executor_capacity_submit_deadline_hold_guard',
              'provider_remote_task_recovery_deadline_guard'
          )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to query provider heartbeat triggers: {error}"))?;
    require(
        provider_heartbeat_triggers == 3,
        "provider heartbeat, capacity quarantine, and attach deadline guards must exist",
    )?;

    let provider_cost_integrity_triggers: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM pg_trigger trigger
        JOIN pg_class relation ON relation.oid = trigger.tgrelid
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE NOT tgisinternal
          AND namespace.nspname = current_schema()
          AND tgname IN (
              'ledger_transactions_provider_cost_amount_guard',
              'ledger_postings_provider_cost_amount_guard',
              'ledger_transaction_seals_provider_cost_amount_guard',
              'provider_cost_observation_receipts_validate_fact_set',
              'provider_cost_observation_fact_links_validate_contract',
              'provider_cost_allocation_lines_validate_period',
              'provider_cost_observation_fact_links_claim_authority',
              'provider_cost_allocation_pools_claim_authority',
              'ledger_transactions_claim_legacy_provider_cost_authority',
              'provider_cost_authority_claims_reject_mutation',
              'provider_cost_authority_claims_reject_truncate',
              'executor_provider_cost_evidence_validate_contract',
              'executor_provider_cost_evidence_reject_mutation',
              'executor_provider_cost_evidence_reject_truncate',
              'provider_receipts_reject_legacy_cost',
              'ledger_transactions_reject_legacy_provider_cost',
              'provider_cost_observation_sources_require_verified',
              'provider_cost_observations_require_source',
              'provider_cost_observation_sources_validate',
              'provider_cost_observation_fact_links_validate_source',
              'provider_cost_observation_receipts_validate_source',
              'provider_cost_observation_sources_reject_mutation',
              'provider_cost_observation_sources_reject_truncate',
              'provider_cost_obligations_validate',
              'provider_cost_obligations_preserve',
              'provider_cost_obligations_record_event',
              'provider_cost_obligation_events_immutable',
              'provider_cost_obligation_events_reject_truncate',
              'provider_receipts_create_cost_obligation',
              'provider_cost_authority_claims_settle_obligations',
              'provider_receipts_require_cost_obligation'
          )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to query provider cost integrity triggers: {error}"))?;
    require(
        provider_cost_integrity_triggers == 31,
        "provider cost amount, attribution, obligation, and accounting-period guards must exist",
    )?;

    let customer_refund_integrity_triggers: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM pg_trigger trigger
        JOIN pg_class relation ON relation.oid = trigger.tgrelid
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE NOT tgisinternal
          AND namespace.nspname = current_schema()
          AND tgname IN (
              'customer_refunds_validate',
              'customer_refunds_validate_account_total',
              'billing_accounts_validate_refund_total',
              'customer_refunds_reject_mutation',
              'customer_refunds_reject_truncate',
              'ledger_transactions_validate_customer_refund_source',
              'ledger_transactions_require_customer_refund_evidence'
          )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to query customer refund integrity triggers: {error}"))?;
    require(
        customer_refund_integrity_triggers == 7,
        "customer refund shape, account counter, and immutability guards must exist",
    )?;
    let billing_integrity_category_constraint: String = sqlx::query_scalar(
        r#"
        SELECT pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'billing_integrity_findings'::regclass
          AND conname = 'billing_integrity_findings_category_check'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to query billing integrity category constraint: {error}"))?;
    require(
        billing_integrity_category_constraint.contains("'customer_refund'::text"),
        "billing integrity findings must accept customer refund findings",
    )?;

    let provider_cost_source_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('validate_provider_cost_observation_source()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to query provider cost source guard: {error}"))?;
    let provider_cost_source_required: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('require_provider_cost_observation_source()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to query provider cost source requirement: {error}"))?;
    let provider_cost_source_verified: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('reject_new_unverified_provider_cost_source()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to query provider cost source verification guard: {error}"))?;
    require(
        provider_cost_source_guard
            .contains("provider cost observation source is not executor verified")
            && provider_cost_source_guard.contains("fact.fact_domain = 'provider_actual'")
            && provider_cost_source_guard.contains("fact.quantity::NUMERIC")
            && provider_cost_source_required
                .contains("provider cost observation requires one source")
            && provider_cost_source_verified
                .contains("new provider cost observations require executor evidence"),
        "provider cost observations must be bound to exact executor evidence",
    )?;

    let receipt_legacy_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('reject_new_legacy_provider_receipt_cost()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to query receipt legacy cost guard: {error}"))?;
    let ledger_legacy_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('reject_new_legacy_provider_cost_ledger()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to query ledger legacy cost guard: {error}"))?;
    require(
        receipt_legacy_guard.contains("legacy provider receipt cost writes are disabled")
            && ledger_legacy_guard.contains("legacy provider cost ledger writes are disabled")
            && ledger_legacy_guard.contains("source_provider_cost_observation_id IS NULL")
            && ledger_legacy_guard.contains("source_provider_cost_allocation_line_id IS NULL"),
        "new legacy receipt and ledger provider cost writes must fail closed",
    )?;

    let provider_cost_amount_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('validate_provider_cost_ledger_amount()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect provider cost amount guard: {error}"))?;
    require(
        provider_cost_amount_guard.contains("posting_count <> 2")
            && provider_cost_amount_guard.contains("positive_amount <>")
            && provider_cost_amount_guard.contains("negative_amount <>")
            && provider_cost_amount_guard.contains("positive_account_key")
            && provider_cost_amount_guard.contains("negative_account_key")
            && provider_cost_amount_guard.contains("provider-expense")
            && provider_cost_amount_guard.contains("payable")
            && provider_cost_amount_guard.contains("provider_cost_ledger_payload_hash")
            && provider_cost_amount_guard.contains("seal_count <> 1"),
        "provider cost ledger guard must bind amount, accounts, payload, and seal to its source",
    )?;

    let provider_cost_fact_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('validate_provider_cost_fact_link()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect provider cost fact guard: {error}"))?;
    require(
        provider_cost_fact_guard.contains("fact.fact_domain <> 'provider_actual'")
            && provider_cost_fact_guard.contains("fact.metric <> 'provider_reported_cost'")
            && provider_cost_fact_guard.contains("fact.unit <> observation.native_unit")
            && provider_cost_fact_guard
                .contains("fact.provider_account_id <> observation.provider_account_id"),
        "provider cost links must reject facts outside the exact observation contract",
    )?;

    let provider_cost_fact_set_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('validate_provider_cost_observation_fact_set()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect provider cost fact-set guard: {error}"))?;
    require(
        provider_cost_fact_set_guard.contains("valid_count <> linked_count")
            && provider_cost_fact_set_guard.contains("linked_fact_set_hash")
            && provider_cost_fact_set_guard.contains("sha256")
            && provider_cost_fact_set_guard.contains("ledger_count <> 1")
            && provider_cost_fact_set_guard.contains("ledger_count <> 0")
            && provider_cost_fact_set_guard.contains("EXCEPT")
            && provider_cost_fact_set_guard
                .contains("provider cost receipt links do not equal the fact set"),
        "provider cost observations must hash the entire exact fact and receipt set",
    )?;

    let provider_cost_receipt_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('validate_provider_cost_observation_receipt_link()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect provider cost receipt guard: {error}"))?;
    require(
        provider_cost_receipt_guard.contains("provider_cost_observation_fact_links")
            && provider_cost_receipt_guard.contains("fact.receipt_id = NEW.receipt_id")
            && provider_cost_receipt_guard
                .contains("fact.provider_account_id = observation.provider_account_id"),
        "provider cost receipt guard must derive attribution from the immutable fact set",
    )?;

    let provider_cost_period_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('validate_provider_cost_allocation_line_period()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect provider cost period guard: {error}"))?;
    require(
        provider_cost_period_guard.contains("receipt.created_at_ms >= pool.period_start_ms")
            && provider_cost_period_guard.contains("receipt.created_at_ms < pool.period_end_ms")
            && provider_cost_period_guard.contains("fact.created_at_ms >= pool.period_start_ms")
            && provider_cost_period_guard.contains("fact.created_at_ms < pool.period_end_ms"),
        "provider cost allocation guard must use half-open accounting periods for every basis",
    )?;

    let provider_actual_authority_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('claim_provider_actual_cost_authority()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect actual cost authority guard: {error}"))?;
    let provider_actual_authority_guard = provider_actual_authority_guard.to_ascii_lowercase();
    require(
        provider_actual_authority_guard.contains("provider_usage_facts")
            && provider_actual_authority_guard.contains("provider_receipts")
            && provider_actual_authority_guard.contains("provider_actual")
            && provider_actual_authority_guard.contains("int8range"),
        "provider actual authority must derive its account, job, currency, and point period",
    )?;

    let provider_allocation_authority_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('claim_closed_provider_allocation_authority()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect allocated cost authority guard: {error}"))?;
    let provider_allocation_authority_guard =
        provider_allocation_authority_guard.to_ascii_lowercase();
    require(
        provider_allocation_authority_guard.contains("new.state <> 'closed'")
            && provider_allocation_authority_guard.contains("provider_cost_allocation_lines")
            && provider_allocation_authority_guard.contains("new.period_start_ms")
            && provider_allocation_authority_guard.contains("new.period_end_ms"),
        "provider allocated authority must be claimed only when its pool closes",
    )?;

    let receipt_authority_column: (String, String) = sqlx::query_as(
        r#"
        SELECT is_nullable::TEXT, data_type::TEXT
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'provider_cost_authority_claims'
          AND column_name = 'source_receipt_id'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect receipt authority column: {error}"))?;
    require(
        receipt_authority_column == ("NO".to_string(), "uuid".to_string()),
        "every provider cost authority must reference one immutable receipt",
    )?;

    let receipt_authority_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('validate_provider_cost_authority_receipt()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect receipt authority guard: {error}"))?;
    let receipt_authority_guard = receipt_authority_guard.to_ascii_lowercase();
    require(
        receipt_authority_guard.contains("fact.receipt_id <> new.source_receipt_id")
            && receipt_authority_guard.contains("line.basis_receipt_id <> new.source_receipt_id")
            && receipt_authority_guard
                .contains("ledger_tx.source_receipt_id <> new.source_receipt_id")
            && receipt_authority_guard.contains("pool.state <> 'closed'")
            && receipt_authority_guard.contains("pool.allocation_basis <> 'successful_output'"),
        "actual, allocated, and legacy provider authority must match one exact receipt",
    )?;

    let obligation_settlement_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('settle_provider_cost_obligations_from_claim()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect obligation settlement guard: {error}"))?;
    let obligation_settlement_guard = obligation_settlement_guard.to_ascii_lowercase();
    require(
        obligation_settlement_guard.contains("obligation.receipt_id = new.source_receipt_id")
            && obligation_settlement_guard.contains("settlement_claim_id = new.claim_id"),
        "provider cost obligations must settle by the authority's exact receipt",
    )?;

    let allocation_close_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('validate_provider_cost_allocation_closure()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect provider allocation close guard: {error}"))?;
    let allocation_close_guard = allocation_close_guard.to_ascii_lowercase();
    require(
        allocation_close_guard.contains("pool.state")
            && allocation_close_guard.contains("'draft'")
            && allocation_close_guard.contains("pool.allocation_basis")
            && allocation_close_guard.contains("'successful_output'")
            && allocation_close_guard.contains("pool.residual_amount_micros")
            && allocation_close_guard.contains("new.candidate_snapshot_hash")
            && allocation_close_guard.contains("pool.candidate_snapshot_hash"),
        "provider allocation close must bind an output snapshot with no residual",
    )?;

    let allocation_line_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('preserve_provider_cost_allocation_line()'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect provider allocation line guard: {error}"))?;
    let allocation_line_guard = allocation_line_guard.to_ascii_lowercase();
    require(
        allocation_line_guard.contains("tg_op in ('update', 'delete')")
            && allocation_line_guard.contains("provider allocation lines are immutable")
            && allocation_line_guard.contains("pool_state")
            && allocation_line_guard.contains("'draft'"),
        "provider allocation lines must be append-only while their pool is draft",
    )?;

    let closed_allocation_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('validate_closed_provider_cost_allocation_evidence(uuid)'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect closed allocation evidence guard: {error}"))?;
    let closed_allocation_guard = closed_allocation_guard.to_ascii_lowercase();
    require(
        closed_allocation_guard.contains("closure_count <> 1")
            && closed_allocation_guard.contains("line_count = 0")
            && closed_allocation_guard.contains("claim_count <> line_count")
            && closed_allocation_guard.contains("invalid_claim_count <> 0")
            && closed_allocation_guard.contains("invalid_snapshot_count <> 0")
            && closed_allocation_guard.contains("basis_receipt_payload_hash")
            && closed_allocation_guard.contains("basis_quote_hash"),
        "closed provider allocations must retain complete receipt, quote, closure, and claim evidence",
    )?;

    let closed_allocation_ledger_guard: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('validate_closed_provider_cost_allocation_ledger(uuid)'::regprocedure)",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect closed allocation ledger guard: {error}"))?;
    let closed_allocation_ledger_guard = closed_allocation_ledger_guard.to_ascii_lowercase();
    require(
        closed_allocation_ledger_guard.contains("residual <> 0")
            && closed_allocation_ledger_guard.contains("line_count = 0")
            && closed_allocation_ledger_guard.contains("invalid_line_count <> 0")
            && closed_allocation_ledger_guard.contains("transaction_count <> 1")
            && closed_allocation_ledger_guard.contains("seal_count <> 1")
            && closed_allocation_ledger_guard.contains("amount_micros = 0")
            && closed_allocation_ledger_guard.contains("transaction_count <> 0"),
        "closed provider allocations must have exact sealed ledger coverage for positive lines only",
    )?;

    let provider_cost_period_exclusion: String = sqlx::query_scalar(
        r#"
        SELECT pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'provider_cost_allocation_pools'::regclass
          AND conname = 'provider_cost_allocation_pools_closed_period_excl'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect provider cost period exclusion: {error}"))?;
    let provider_cost_period_exclusion = provider_cost_period_exclusion.to_ascii_lowercase();
    require(
        provider_cost_period_exclusion.contains("int8range")
            && provider_cost_period_exclusion.contains("period_start_ms")
            && provider_cost_period_exclusion.contains("period_end_ms")
            && provider_cost_period_exclusion.contains("with &&")
            && provider_cost_period_exclusion.contains("state = 'closed'"),
        "closed provider allocation periods must use a half-open overlap exclusion",
    )?;

    let provider_cost_authority_exclusion: String = sqlx::query_scalar(
        r#"
        SELECT pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'provider_cost_authority_claims'::regclass
          AND conname = 'provider_cost_authority_period_excl'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect provider cost authority exclusion: {error}"))?;
    let provider_cost_authority_exclusion = provider_cost_authority_exclusion.to_ascii_lowercase();
    require(
        provider_cost_authority_exclusion.contains("provider_account_id with =")
            && provider_cost_authority_exclusion.contains("job_id with =")
            && provider_cost_authority_exclusion.contains("currency with =")
            && provider_cost_authority_exclusion.contains("authority_period with &&")
            && provider_cost_authority_exclusion.contains("authority_kind with <>"),
        "provider cost authorities must exclude overlapping mixed sources per account, job, and currency",
    )?;

    for (index, expression) in [
        (
            "provider_submit_recoveries_claim_idx",
            "greatest(next_recovery_at_ms, coalesce(recovery_lease_expires_at_ms, next_recovery_at_ms))",
        ),
        (
            "provider_remote_tasks_poll_claim_idx",
            "greatest(next_poll_at_ms, coalesce(poll_lease_expires_at_ms, next_poll_at_ms))",
        ),
    ] {
        let definition: String = sqlx::query_scalar(
            "SELECT lower(indexdef) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1",
        )
        .bind(index)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to inspect index {index}: {error}"))?;
        require(
            definition.contains(expression),
            &format!("index {index} must preserve the effective due expression"),
        )?;
    }
    Ok(())
}

async fn migration_table_exists(pool: &PgPool) -> TestResult<bool> {
    sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to inspect migration table: {error}"))
}

async fn migration_versions(pool: &PgPool) -> TestResult<Vec<i64>> {
    sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
        .fetch_all(pool)
        .await
        .map_err(|error| format!("failed to query migration versions: {error}"))
}

async fn apply_migrations_through(pool: &PgPool, last_version: i64) -> TestResult {
    apply_migration_range(pool, 0, last_version).await
}

async fn apply_migration_range(pool: &PgPool, first_version: i64, last_version: i64) -> TestResult {
    let migration_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let mut migrations = std::fs::read_dir(&migration_dir)
        .map_err(|error| format!("failed to read {}: {error}", migration_dir.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to enumerate migrations: {error}"))?;
    migrations.sort();

    for path in migrations {
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(version) = file_name
            .split_once('_')
            .and_then(|(version, _)| version.parse::<i64>().ok())
        else {
            continue;
        };
        if !(first_version..=last_version).contains(&version) {
            continue;
        }
        let sql = std::fs::read_to_string(&path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        let mut transaction = pool
            .begin()
            .await
            .map_err(|error| format!("failed to begin migration {version}: {error}"))?;
        sqlx::raw_sql(AssertSqlSafe(sql))
            .execute(&mut *transaction)
            .await
            .map_err(|error| format!("migration {version} failed: {error}"))?;
        transaction
            .commit()
            .await
            .map_err(|error| format!("failed to commit migration {version}: {error}"))?;
    }
    Ok(())
}

fn gateway_result(result: Result<(), ImageGatewayError>, context: &str) -> TestResult {
    result.map_err(|error| format!("{context}: {error:?}"))
}

fn require(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.to_string())
    }
}

fn test_charge(request_id: &str) -> UsageCharge {
    UsageCharge {
        tenant_id: "proj_test".to_string(),
        attribution: None,
        request_id: request_id.to_string(),
        admission_session_id: None,
        operation: "generation",
        provider_id: "openai-codex".to_string(),
        model: "gpt-image-2".to_string(),
        output_count: 1,
        billable_units: 1,
        billing_metric: image_provider_contracts::BillingMetric::Output,
        limits: UsageLimits {
            five_hour_image_limit: 10,
            seven_day_image_limit: 10,
        },
    }
}

struct TestSchema {
    name: String,
    pool: PgPool,
}

impl TestSchema {
    async fn new(max_connections: u32) -> TestResult<Option<Self>> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set when CI is present".to_string());
            }
            eprintln!("skipping PostgreSQL migration test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("image_gateway_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, max_connections, &name)
            .await
            .map_err(|error| format!("test database should be reachable: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("failed to identify test database: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because current_database() is {database_name:?}, which does not contain 'test'"
            ));
        }

        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("failed to create isolated schema {name}: {error}"))?;
        let setup = async {
            let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
                .fetch_one(&pool)
                .await
                .map_err(|error| format!("failed to inspect current schema: {error}"))?;
            require(
                current_schema == name,
                &format!(
                    "test connection search_path resolved to {current_schema:?}, expected {name:?}"
                ),
            )
        }
        .await;
        if let Err(error) = setup {
            let cleanup = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{name}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return match cleanup {
                Ok(_) => Err(error),
                Err(cleanup_error) => Err(format!(
                    "{error}; additionally failed to clean isolated schema {name}: {cleanup_error}"
                )),
            };
        }
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .map_err(|error| format!("failed to clean isolated schema {}: {error}", self.name));
        self.pool.close().await;
        result.map(|_| ())
    }
}
