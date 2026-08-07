use std::{env, sync::Arc, time::Duration};

#[allow(dead_code, unused_imports)]
#[path = "../src/admin_read/mod.rs"]
mod admin_read;

use admin_read::{
    AdminReadScope, AdminReadStore, AuditLogsQuery, BlockedTerminalReduction, JobListItem,
    JobsQuery, PostgresAdminReadStore, ProviderStateCount, UsageAnalysisQuery, UsageGroupBy,
    UsageInterval,
};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use factory_identity::{
    AccessTokenCodec, AuthPolicy, IdentityService, LoginRequest, RefreshTokenKeyring,
};
use gpt_image_2_gateway::{
    ApiKeyKeyring, AppConfig, ExecutorTerminalBlockReason, ExecutorTerminalStore,
    ExternalControlPlaneServices, ExternalImageGatewayComponents,
    PostgresAdminReadStore as GatewayPostgresAdminReadStore, PostgresApiKeyStore,
    PostgresCustomerRefundService, PostgresExecutorTerminalStore, PostgresProjectGovernanceService,
    PostgresProjectSpendBudgetService, PostgresProviderCostObligationService,
    PostgresProviderTaskStore, PostgresUsageStore, ProxyConfig, RequestObservationSink,
    admission::PostgresAdmissionStore,
    admission::WorkLease,
    artifacts::FilesystemArtifactBlobStore,
    build_router_with_external_execution_and_services,
    database::{connect_test_pool_with_search_path, run_migrations},
    executor::{
        ExecutorClaimScope, ExecutorHandoffStore, ExecutorSubmissionOutcome,
        ExecutorSubmissionStore, PostgresExecutorSubmissionStore,
    },
    identity::PostgresIdentityRepository,
    settlement::PostgresExecutionSettlementStore,
};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, PgPool};
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const PRIVATE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgJ6r5c63M0tPZV05C
Y0U72GBHm9iqV7QaUgFxk/9dBn+hRANCAAT5ufmoZxTrAkeOwJFSjVcbQ1Pvl2sw
892/nV1rvRJwDokKy+s00P46StleDgXLe9hOly8yM81frZfcMeI1krz+
-----END PRIVATE KEY-----
"#;
const PUBLIC_KEY: &[u8] = br#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE+bn5qGcU6wJHjsCRUo1XG0NT75dr
MPPdv51da70ScA6JCsvrNND+OkrZXg4Fy3vYTpcvMjPNX62X3DHiNZK8/g==
-----END PUBLIC KEY-----
"#;
const ADMIN_PASSWORD: &str = "correct horse battery staple";

#[tokio::test]
async fn postgres_admin_reads_preserve_contracts() -> TestResult {
    let Some(schema) = TestSchema::new(4).await? else {
        return Ok(());
    };
    let result = admin_read_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn platform_admin_reads_require_identity_jwt_with_owner_scope() -> TestResult {
    let Some(schema) = TestSchema::new(8).await? else {
        return Ok(());
    };
    let result = platform_owner_api_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[test]
fn job_dto_keeps_job_work_and_provider_states_separate() -> TestResult {
    let item = JobListItem {
        job_id: Uuid::nil().to_string(),
        tenant_id: "tenant-test".to_string(),
        project_id: Some("proj-test".to_string()),
        service_account_id: None,
        api_key_id: None,
        auth_kind: Some("legacy".to_string()),
        actor_user_id: None,
        credential_owner_user_id: None,
        request_id: "request-test".to_string(),
        operation: "generation".to_string(),
        provider_id: "provider-test".to_string(),
        model: "model-test".to_string(),
        job_state: "running".to_string(),
        work_state: Some("awaiting_executor".to_string()),
        provider_states: vec![
            ProviderStateCount {
                stage: "submission".to_string(),
                state: "failed".to_string(),
                count: "1".to_string(),
            },
            ProviderStateCount {
                stage: "remote_task".to_string(),
                state: "provider_waiting".to_string(),
                count: "2".to_string(),
            },
        ],
        output_count: "3".to_string(),
        billable_units: "3".to_string(),
        billing_metric: "output".to_string(),
        billing_unit: "output".to_string(),
        charged_units: "0".to_string(),
        created_at_ms: 1,
        started_at_ms: None,
        finished_at_ms: None,
        last_error_code: None,
    };
    let value = serde_json::to_value(item).map_err(debug_error)?;
    require(value["job_state"] == "running", "job state was flattened")?;
    require(
        value["work_state"] == "awaiting_executor",
        "work state was flattened",
    )?;
    require(
        value["provider_states"][0]["stage"] == "submission"
            && value["provider_states"][0]["state"] == "failed"
            && value["provider_states"][1]["stage"] == "remote_task"
            && value["provider_states"][1]["state"] == "provider_waiting",
        "provider states were flattened",
    )
}

#[test]
fn blocked_terminal_dto_exposes_stable_operational_fields() -> TestResult {
    let item = BlockedTerminalReduction {
        submission_id: Uuid::nil().to_string(),
        executor_execution_id: Uuid::from_u128(1).to_string(),
        job_id: Uuid::from_u128(2).to_string(),
        request_id: "request-blocked".to_string(),
        provider_id: "provider-test".to_string(),
        model: "model-test".to_string(),
        resolved_state: "failed".to_string(),
        error_code: "canonical_conflict".to_string(),
        blocked_at_ms: 123,
        blocked_by: "reducer-test".to_string(),
    };
    let value = serde_json::to_value(item).map_err(debug_error)?;
    require(
        value["error_code"] == "canonical_conflict"
            && value["blocked_at_ms"] == 123
            && value["blocked_by"] == "reducer-test",
        "blocked terminal DTO omitted its stable diagnostic fields",
    )?;
    require(
        value.get("error_message").is_none(),
        "blocked terminal DTO exposed unstable free-form error text",
    )
}

async fn admin_read_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("admin read migrations failed: {error:?}"))?;
    assert_admin_migration(pool).await?;

    let store = PostgresAdminReadStore::new(pool.clone());
    let empty_overview = store.overview(60_000).await.map_err(debug_error)?;
    require(
        empty_overview.job_states.is_empty(),
        "fresh overview must not invent jobs",
    )?;
    let empty_billing = store.billing(60_000).await.map_err(debug_error)?;
    require(
        empty_billing.account_snapshots.is_empty(),
        "fresh billing must not invent accounts",
    )?;
    let empty_usage = store
        .usage_analysis_scoped(
            &AdminReadScope::Platform,
            UsageAnalysisQuery {
                window_ms: 60_000,
                interval: UsageInterval::Minute,
                group_by: UsageGroupBy::LineItem,
                project_id: None,
                api_key_id: None,
                filter_user_id: None,
                provider_id: None,
                model: None,
                operation: None,
                service_tier: None,
            },
        )
        .await
        .map_err(debug_error)?;
    let audit_event_id = Uuid::new_v4();
    let audit_created_at_ms = database_now(pool).await? - 1;
    sqlx::query(
        r#"
        INSERT INTO identity_audit_events (
            event_id, action, resource_type, resource_id, outcome,
            metadata, created_at_ms
        )
        VALUES ($1, 'project.settings.update', 'project', 'project-audit-read',
                'success', '{"settings_version":2}'::jsonb, $2)
        "#,
    )
    .bind(audit_event_id)
    .bind(audit_created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let audit_page = store
        .audit_logs(AuditLogsQuery {
            window_ms: 60_000,
            to_ms: None,
            limit: 50,
            after: None,
            event_type: Some("project.settings.update".to_string()),
            outcome: Some("success".to_string()),
            actor_user_id: None,
            project_id: Some("project-audit-read".to_string()),
            resource_type: Some("project".to_string()),
            request_id: None,
            query: Some("settings".to_string()),
        })
        .await
        .map_err(debug_error)?;
    require(
        audit_page.data.len() == 1
            && audit_page.data[0].id == audit_event_id.to_string()
            && audit_page.data[0].event_type == "project.settings.update"
            && audit_page.data[0]
                .project
                .as_ref()
                .is_some_and(|project| project.id == "project-audit-read")
            && audit_page.data[0].details["settings_version"] == 2,
        "audit log projection lost event, project, or metadata fields",
    )?;
    require(
        empty_usage.activity.is_empty() && empty_usage.spend.is_empty(),
        "fresh usage analysis must not invent activity or spend",
    )?;
    let empty_providers = store.provider_accounts().await.map_err(debug_error)?;
    require(
        empty_providers.accounts.is_empty(),
        "fresh provider read must be empty",
    )?;
    let empty_runtime = store
        .provider_account_concurrency(None)
        .await
        .map_err(debug_error)?;
    require(
        empty_runtime.accounts.is_empty()
            && empty_runtime.queue.queued_work_items == "0"
            && empty_runtime.queue.pending_batch_requests == "0",
        "fresh provider runtime read must be empty",
    )?;
    let empty_scheduler = store.scheduler(60_000).await.map_err(debug_error)?;
    require(
        empty_scheduler.work_items.is_empty(),
        "fresh scheduler read must be empty",
    )?;
    require(
        empty_scheduler.blocked_terminal_reductions == "0"
            && empty_scheduler.blocked_terminals.is_empty()
            && empty_scheduler.active_jobs.is_empty(),
        "fresh scheduler read invented active or blocked work",
    )?;
    let empty_jobs = store
        .jobs(JobsQuery {
            window_ms: 60_000,
            to_ms: None,
            limit: 10,
            cursor: None,
            provider_id: None,
            state: None,
            operation: None,
            model: None,
            project_id: None,
            api_key_id: None,
            request_or_job_id: None,
        })
        .await
        .map_err(debug_error)?;
    require(empty_jobs.items.is_empty(), "fresh jobs read must be empty")?;

    seed_large_billing_account(pool).await?;
    seed_sensitive_provider_profile(pool).await?;
    let billing = store.billing(60_000).await.map_err(debug_error)?;
    let account = billing
        .account_snapshots
        .first()
        .ok_or_else(|| "billing account snapshot is missing".to_string())?;
    require(
        account.credit_limit_micros == "9007199254740993",
        "credit limit must remain an exact decimal string",
    )?;
    let billing_json = serde_json::to_value(&billing).map_err(debug_error)?;
    require(
        billing_json["account_snapshots"][0]["credit_limit_micros"] == "9007199254740993",
        "serialized amount must be a JSON string",
    )?;
    let foreign_billing = store
        .billing_scoped(
            &AdminReadScope::Tenants(vec!["tenant-other".to_string()]),
            60_000,
            None,
        )
        .await
        .map_err(debug_error)?;
    require(
        foreign_billing.account_snapshots.is_empty(),
        "tenant-scoped billing leaked another tenant",
    )?;
    let own_billing = store
        .billing_scoped(
            &AdminReadScope::Tenants(vec!["tenant-admin-read".to_string()]),
            60_000,
            None,
        )
        .await
        .map_err(debug_error)?;
    require(
        own_billing.account_snapshots.len() == 1,
        "tenant-scoped billing omitted the authorized account",
    )?;

    let providers = store.provider_accounts().await.map_err(debug_error)?;
    require(
        providers.accounts.len() == 1,
        "provider account projection must not duplicate accounts with multiple profiles",
    )?;
    let provider = providers
        .accounts
        .first()
        .ok_or_else(|| "provider account projection is missing".to_string())?;
    require(
        provider.runtime_status == "unobserved",
        "inline profiles must not claim runtime readiness",
    )?;
    require(
        provider.upstream_quota.status == "unknown",
        "unobserved upstream quota must remain unknown",
    )?;
    let provider_account_id =
        Uuid::parse_str(&provider.provider_account_id).map_err(debug_error)?;
    let runtime = store
        .provider_account_concurrency(Some(&[provider_account_id]))
        .await
        .map_err(debug_error)?;
    require(
        runtime.accounts.len() == 1
            && runtime.accounts[0].max_concurrency == "7"
            && runtime.accounts[0].allocated_count == "0"
            && runtime.accounts[0].available_capacity == "7",
        "provider runtime projection must preserve the authoritative concurrency counters",
    )?;
    let provider_json = serde_json::to_string(&providers).map_err(debug_error)?;
    for forbidden in [
        "vault.admin-read.secret",
        "\"credential_ref\":",
        "\"credential_auth_sha256\":",
        "\"runtime_owner\":",
        "\"provider_request_id\":",
    ] {
        require(
            !provider_json.contains(forbidden),
            &format!("provider DTO leaked forbidden material: {forbidden}"),
        )?;
    }

    let created_at_ms = database_now(pool).await? - 1_000;
    let ids = seed_same_millisecond_jobs(pool, created_at_ms).await?;
    sqlx::query("UPDATE jobs SET operation = 'edit', model = 'model-alt' WHERE job_id = $1")
        .bind(ids[0])
        .execute(pool)
        .await
        .map_err(debug_error)?;
    seed_job_attribution(pool, ids[2]).await?;
    seed_pending_batch_request(pool, created_at_ms).await?;
    let runtime_with_queue = store
        .provider_account_concurrency(None)
        .await
        .map_err(debug_error)?;
    require(
        runtime_with_queue.queue.queued_work_items == "2"
            && runtime_with_queue.queue.pending_batch_requests == "1",
        "provider runtime projection omitted immediate or Batch queue pressure",
    )?;
    let active_scheduler = store.scheduler(60_000).await.map_err(debug_error)?;
    require(
        active_scheduler.active_jobs.len() == 2
            && active_scheduler
                .active_jobs
                .iter()
                .any(|item| item.job_id == ids[0].to_string() && item.model == "model-alt")
            && active_scheduler.active_jobs.iter().any(|item| {
                item.job_id == ids[1].to_string()
                    && item.job_state == "reserved"
                    && item.work_state.as_deref() == Some("leased")
            })
            && active_scheduler
                .active_jobs
                .iter()
                .all(|item| item.job_id != ids[2].to_string()),
        "scheduler active-job projection lost state or included terminal work",
    )?;
    let first = store
        .jobs(JobsQuery {
            window_ms: 60_000,
            to_ms: None,
            limit: 2,
            cursor: None,
            provider_id: None,
            state: None,
            operation: None,
            model: None,
            project_id: None,
            api_key_id: None,
            request_or_job_id: None,
        })
        .await
        .map_err(debug_error)?;
    require(
        first.items.len() == 2,
        "first keyset page must contain two jobs",
    )?;
    require(
        first.items[0].job_id == ids[2].to_string() && first.items[1].job_id == ids[1].to_string(),
        "same-millisecond jobs must use job_id as the keyset tiebreaker",
    )?;
    require(
        first.items[0].job_state == "uncertain"
            && first.items[0].work_state.as_deref() == Some("ready"),
        "job and work state must remain distinct",
    )?;
    require(
        first.items[0].project_id.as_deref() == Some("proj-admin-read")
            && first.items[0].api_key_id.as_deref() == Some("key-admin-read"),
        "job attribution was not exposed in the admin read model",
    )?;
    let foreign_jobs = store
        .jobs_scoped(
            &AdminReadScope::Tenants(vec!["tenant-other".to_string()]),
            JobsQuery {
                window_ms: 60_000,
                to_ms: None,
                limit: 10,
                cursor: None,
                provider_id: None,
                state: None,
                operation: None,
                model: None,
                project_id: None,
                api_key_id: None,
                request_or_job_id: None,
            },
        )
        .await
        .map_err(debug_error)?;
    require(
        foreign_jobs.items.is_empty(),
        "tenant-scoped jobs leaked another tenant",
    )?;
    let attributed = store
        .jobs(JobsQuery {
            window_ms: 60_000,
            to_ms: None,
            limit: 10,
            cursor: None,
            provider_id: None,
            state: None,
            operation: None,
            model: None,
            project_id: Some("proj-admin-read".to_string()),
            api_key_id: Some("key-admin-read".to_string()),
            request_or_job_id: None,
        })
        .await
        .map_err(debug_error)?;
    require(
        attributed.items.len() == 1 && attributed.items[0].job_id == ids[2].to_string(),
        "project/key filter did not isolate the attributed job",
    )?;
    let operation_and_model = store
        .jobs(JobsQuery {
            window_ms: 60_000,
            to_ms: None,
            limit: 10,
            cursor: None,
            provider_id: None,
            state: None,
            operation: Some("edit".to_string()),
            model: Some("model-alt".to_string()),
            project_id: None,
            api_key_id: None,
            request_or_job_id: None,
        })
        .await
        .map_err(debug_error)?;
    require(
        operation_and_model.items.len() == 1
            && operation_and_model.items[0].job_id == ids[0].to_string(),
        "operation/model filters did not isolate the matching job",
    )?;
    let anchor_to_ms = first.to_ms;
    let cursor = first
        .next_cursor
        .ok_or_else(|| "first page must return a cursor".to_string())?;
    let second = store
        .jobs(JobsQuery {
            window_ms: 60_000,
            to_ms: Some(anchor_to_ms),
            limit: 2,
            cursor: Some(cursor),
            provider_id: None,
            state: None,
            operation: None,
            model: None,
            project_id: None,
            api_key_id: None,
            request_or_job_id: None,
        })
        .await
        .map_err(debug_error)?;
    require(
        second.to_ms == anchor_to_ms,
        "subsequent pages must preserve the first-page upper bound",
    )?;
    require(
        second.items.len() == 1 && second.items[0].job_id == ids[0].to_string(),
        "keyset pagination omitted a same-millisecond job",
    )?;
    require(
        second.next_cursor.is_none(),
        "last keyset page must not return another cursor",
    )?;

    let delayed_job_id = seed_delayed_usage_event(pool).await?;
    let delayed_usage = store
        .usage_analysis_scoped(
            &AdminReadScope::Platform,
            UsageAnalysisQuery {
                window_ms: 60_000,
                interval: UsageInterval::Minute,
                group_by: UsageGroupBy::Model,
                project_id: None,
                api_key_id: None,
                filter_user_id: None,
                provider_id: None,
                model: Some("model-delayed".to_string()),
                operation: None,
                service_tier: None,
            },
        )
        .await
        .map_err(debug_error)?;
    require(
        delayed_usage.activity.iter().any(|point| {
            point.group_value == "model-delayed"
                && point.billing_metric == "output"
                && point.quantity == "3"
        }),
        "usage created inside the window was omitted because its job was older",
    )?;
    require(
        !delayed_usage
            .activity
            .iter()
            .any(|point| point.billing_metric == "request"),
        "an old request was incorrectly moved into the usage event window",
    )?;
    require(
        delayed_usage
            .filter_options
            .models
            .iter()
            .any(|option| option.value == "model-delayed"),
        "filter options omitted a model with delayed usage",
    )?;
    require(
        delayed_job_id != Uuid::nil(),
        "delayed usage seed returned an invalid job",
    )?;

    let scheduler_before_blocked = store.scheduler(60_000).await.map_err(debug_error)?;
    let blocked = seed_blocked_terminal_reduction(pool).await?;
    let scheduler = store.scheduler(60_000).await.map_err(debug_error)?;
    require(
        scheduler.blocked_terminal_reductions == "1" && scheduler.blocked_terminals.len() == 1,
        "scheduler did not count the blocked terminal reduction",
    )?;
    let projected = &scheduler.blocked_terminals[0];
    require(
        projected.submission_id == blocked.submission_id.to_string()
            && projected.executor_execution_id == blocked.executor_execution_id.to_string()
            && projected.job_id == blocked.job_id.to_string()
            && projected.request_id == blocked.request_id
            && projected.error_code == "canonical_conflict"
            && projected.blocked_by == "admin-read-reducer"
            && projected.blocked_at_ms > 0,
        "scheduler blocked terminal list lost stable diagnostic fields",
    )?;
    require(
        scheduler.work_items == scheduler_before_blocked.work_items
            && scheduler
                .active_jobs
                .iter()
                .all(|job| job.job_id != blocked.job_id.to_string()),
        "blocked terminal work must not remain in the live scheduler projection",
    )
}

async fn platform_owner_api_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("admin API migrations failed: {error:?}"))?;
    seed_sensitive_provider_profile(pool).await?;

    let policy = AuthPolicy::default();
    let access_tokens = AccessTokenCodec::new(
        "admin-read-test-key",
        PRIVATE_KEY,
        [("admin-read-test-key".to_string(), PUBLIC_KEY.to_vec())],
        "https://identity.admin-read.test",
        "urn:aif:admin-read-test",
        &policy,
    )
    .map_err(debug_error)?;
    let refresh_tokens = RefreshTokenKeyring::new(1, [(1, vec![0x35; 32])]).map_err(debug_error)?;
    let identity = Arc::new(
        IdentityService::new(
            Arc::new(PostgresIdentityRepository::new(pool.clone())),
            access_tokens,
            refresh_tokens,
            policy.clone(),
        )
        .map_err(debug_error)?,
    );
    identity
        .bootstrap_admin(
            "owner@admin-read.test".to_string(),
            "Admin Read Owner".to_string(),
            ADMIN_PASSWORD.to_string(),
        )
        .await
        .map_err(debug_error)?;
    let member_access = identity
        .create_member_user(
            "member@admin-read.test".to_string(),
            "Admin Read Member".to_string(),
            ADMIN_PASSWORD.to_string(),
        )
        .await
        .map_err(debug_error)?;
    let member_tenant = member_access
        .organizations
        .first()
        .ok_or_else(|| "member personal workspace is missing".to_string())?
        .organization_id
        .clone();
    let created_at_ms = database_now(pool).await? - 1_000;
    let sibling_access = identity
        .create_member_user(
            "sibling@admin-read.test".to_string(),
            "Admin Read Sibling".to_string(),
            ADMIN_PASSWORD.to_string(),
        )
        .await
        .map_err(debug_error)?;
    let sibling_tenant = sibling_access
        .organizations
        .first()
        .ok_or_else(|| "sibling personal workspace is missing".to_string())?
        .organization_id
        .clone();
    sqlx::query(
        r#"
        INSERT INTO identity_organization_memberships (
            organization_id, user_id, role, state, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'member', 'active', $3, $3)
        "#,
    )
    .bind(&member_tenant)
    .bind(sibling_access.user_id)
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO identity_project_memberships (
            organization_id, project_id, user_id, role, state, is_default,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, 'member', 'active', FALSE, $4, $4)
        "#,
    )
    .bind(&member_tenant)
    .bind(&member_access.projects[0].project_id)
    .bind(sibling_access.user_id)
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    seed_same_millisecond_jobs(pool, created_at_ms).await?;

    sqlx::query(
        "UPDATE identity_users SET scopes = ARRAY['admin:keys:read'] WHERE normalized_email = 'owner@admin-read.test'",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let limited = identity
        .login(LoginRequest {
            email: "owner@admin-read.test".to_string(),
            password: ADMIN_PASSWORD.to_string(),
            client_id: policy.client_id.clone(),
        })
        .await
        .map_err(debug_error)?;

    let artifact_root = TempDir::new().map_err(debug_error)?;
    let blobs =
        Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
    let app = build_router_with_external_execution_and_services(
        admin_api_config(),
        ExternalImageGatewayComponents {
            usage_store: Arc::new(PostgresUsageStore::new(pool.clone())),
            api_key_store: Arc::new(PostgresApiKeyStore::new(
                pool.clone(),
                ApiKeyKeyring::new(1, [(1, vec![0x42; 32])]).map_err(debug_error)?,
            )),
            admission_store: Arc::new(PostgresAdmissionStore::new(pool.clone())),
            settlement_store: Arc::new(PostgresExecutionSettlementStore::new(
                pool.clone(),
                blobs.clone(),
            )),
            input_blob_store: blobs,
            provider_readiness_store: Arc::new(PostgresProviderTaskStore::new(pool.clone())),
        },
        ExternalControlPlaneServices {
            identity_service: Some(identity.clone()),
            admin_read_store: Some(Arc::new(GatewayPostgresAdminReadStore::new(pool.clone()))),
            billing_account_control_service: Some(Arc::new(
                gpt_image_2_gateway::PostgresBillingAccountControlService::new(pool.clone()),
            )),
            billing_integrity_service: Some(Arc::new(
                gpt_image_2_gateway::PostgresBillingIntegrityService::new(pool.clone()),
            )),
            provider_cost_obligation_service: Some(Arc::new(
                PostgresProviderCostObligationService::new(pool.clone()),
            )),
            customer_refund_service: Some(Arc::new(PostgresCustomerRefundService::new(
                pool.clone(),
            ))),
            request_observation_sink: Some(
                RequestObservationSink::from_env(pool.clone()).map_err(debug_error)?,
            ),
            project_governance_service: Some(Arc::new(PostgresProjectGovernanceService::new(
                pool.clone(),
            ))),
            project_spend_budget_service: Some(Arc::new(PostgresProjectSpendBudgetService::new(
                pool.clone(),
            ))),
            ..ExternalControlPlaneServices::default()
        },
    )
    .map_err(debug_error)?;

    let member = identity
        .login(LoginRequest {
            email: "member@admin-read.test".to_string(),
            password: ADMIN_PASSWORD.to_string(),
            client_id: policy.client_id.clone(),
        })
        .await
        .map_err(debug_error)?;
    let sibling = identity
        .login(LoginRequest {
            email: "sibling@admin-read.test".to_string(),
            password: ADMIN_PASSWORD.to_string(),
            client_id: policy.client_id.clone(),
        })
        .await
        .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts(
            tenant_id, currency, credit_limit_micros,
            held_micros, captured_micros, created_at_ms, updated_at_ms
        )
        VALUES ($1, 'USD', 5000000, 0, 0, $2, $2)
        "#,
    )
    .bind(&member_tenant)
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let (owner_billing_status, _, owner_billing_body) = get_admin(
        app.clone(),
        "/v1/console/billing/summary?window=24h",
        &member.access_token,
    )
    .await?;
    let (member_billing_status, _, member_billing_body) = get_admin(
        app.clone(),
        "/v1/console/billing/summary?window=24h",
        &sibling.access_token,
    )
    .await?;
    require(
        owner_billing_status == StatusCode::OK
            && owner_billing_body["account_snapshots"]
                .as_array()
                .is_some_and(|accounts| accounts.len() == 1),
        "organization owner could not read the organization billing balance",
    )?;
    require(
        member_billing_status == StatusCode::OK
            && member_billing_body["account_snapshots"]
                .as_array()
                .is_some_and(Vec::is_empty),
        "ordinary organization member could read the shared billing balance",
    )?;
    let project_members_path = format!(
        "/v1/organization/projects/{}/members",
        member_access.projects[0].project_id
    );
    let (members_status, _, members_body) =
        get_admin(app.clone(), &project_members_path, &sibling.access_token).await?;
    require(
        members_status == StatusCode::OK
            && members_body["data"]
                .as_array()
                .is_some_and(|members| members.len() == 2),
        "project member could not read the shared project membership list",
    )?;
    let (member_add_status, _) = post_admin(
        app.clone(),
        &project_members_path,
        &sibling.access_token,
        json!({
            "email": "owner@admin-read.test",
            "role": "member"
        }),
    )
    .await?;
    require(
        member_add_status == StatusCode::FORBIDDEN,
        "ordinary project member changed owner-managed project memberships",
    )?;
    let project_limits_path = format!(
        "/v1/organization/projects/{}/limits",
        member_access.projects[0].project_id
    );
    let (sibling_limits_status, _, sibling_limits_body) =
        get_admin(app.clone(), &project_limits_path, &sibling.access_token).await?;
    require(
        sibling_limits_status == StatusCode::OK
            && sibling_limits_body["configured"] == false
            && sibling_limits_body["limit_type"] == "soft",
        "project member could not read the shared project soft budget",
    )?;
    let (sibling_update_status, _) = put_admin(
        app.clone(),
        &project_limits_path,
        &sibling.access_token,
        json!({
            "currency": "USD",
            "monthly_budget_micros": "1000000",
            "alert_thresholds": [75],
            "expected_control_version": "0"
        }),
    )
    .await?;
    require(
        sibling_update_status == StatusCode::FORBIDDEN,
        "project member changed an owner-only project budget",
    )?;
    let (owner_update_status, owner_update_body) = put_admin(
        app.clone(),
        &project_limits_path,
        &member.access_token,
        json!({
            "currency": "USD",
            "monthly_budget_micros": "1000000",
            "alert_thresholds": [75],
            "expected_control_version": "0"
        }),
    )
    .await?;
    require(
        owner_update_status == StatusCode::OK
            && owner_update_body["configured"] == true
            && owner_update_body["control_version"] == "1"
            && owner_update_body["alert_thresholds"] == json!([75, 100]),
        "project owner could not create the project soft budget",
    )?;
    let notification_event_id = Uuid::new_v4();
    let notification_delivery_id = Uuid::new_v4();
    sqlx::query(
        r#"
        WITH period AS (
            SELECT
                (EXTRACT(EPOCH FROM DATE_TRUNC(
                    'month', transaction_timestamp() AT TIME ZONE 'UTC'
                )) * 1000)::BIGINT AS start_ms,
                (EXTRACT(EPOCH FROM (
                    DATE_TRUNC('month', transaction_timestamp() AT TIME ZONE 'UTC')
                    + INTERVAL '1 month'
                )) * 1000)::BIGINT AS end_ms,
                (EXTRACT(EPOCH FROM transaction_timestamp()) * 1000)::BIGINT AS now_ms
        )
        INSERT INTO project_spend_alert_events(
            event_id, project_id, organization_id, currency,
            period_start_ms, period_end_ms, threshold_percent,
            budget_control_version, monthly_budget_micros, spend_micros,
            created_at_ms
        )
        SELECT $1, $2, $3, 'USD', start_ms, end_ms, 100,
               1, 1000000, 1000000, now_ms
        FROM period
        "#,
    )
    .bind(notification_event_id)
    .bind(&member_access.projects[0].project_id)
    .bind(&member_tenant)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO project_spend_notification_deliveries(
            delivery_id, event_id, recipient_user_id, state, attempt_count,
            next_attempt_at_ms, created_at_ms, delivered_at_ms
        )
        SELECT $1, $2, $3, 'delivered', 1, now_ms, now_ms, now_ms
        FROM (
            SELECT (EXTRACT(EPOCH FROM transaction_timestamp()) * 1000)::BIGINT AS now_ms
        ) clock
        "#,
    )
    .bind(notification_delivery_id)
    .bind(notification_event_id)
    .bind(member_access.user_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let (owner_notifications_status, _, owner_notifications_body) = get_admin(
        app.clone(),
        "/v1/console/notifications",
        &member.access_token,
    )
    .await?;
    let (sibling_notifications_status, _, sibling_notifications_body) = get_admin(
        app.clone(),
        "/v1/console/notifications",
        &sibling.access_token,
    )
    .await?;
    require(
        owner_notifications_status == StatusCode::OK
            && owner_notifications_body["unread_count"] == 1
            && owner_notifications_body["data"][0]["delivery_id"]
                == notification_delivery_id.to_string()
            && sibling_notifications_status == StatusCode::OK
            && sibling_notifications_body["unread_count"] == 0,
        "recipient-scoped notification inbox did not isolate authenticated users",
    )?;
    let (cross_read_status, _) = post_empty_admin(
        app.clone(),
        &format!("/v1/console/notifications/{notification_delivery_id}/read"),
        &sibling.access_token,
    )
    .await?;
    let (owner_read_status, owner_read_body) = post_empty_admin(
        app.clone(),
        &format!("/v1/console/notifications/{notification_delivery_id}/read"),
        &member.access_token,
    )
    .await?;
    require(
        cross_read_status == StatusCode::NOT_FOUND
            && owner_read_status == StatusCode::OK
            && owner_read_body["read_at_ms"].is_number(),
        "notification read endpoint crossed recipient boundaries or failed to persist",
    )?;
    let member_job_id = seed_job_for_user(
        pool,
        &member_tenant,
        &member_access.projects[0].project_id,
        member_access.user_id,
        Uuid::parse_str(&member.session.id).map_err(debug_error)?,
        member_access.authz_version,
        "request-member",
        created_at_ms,
    )
    .await?;
    let sibling_job_id = seed_job_for_user(
        pool,
        &member_tenant,
        &member_access.projects[0].project_id,
        sibling_access.user_id,
        Uuid::new_v4(),
        sibling_access.authz_version,
        "request-sibling",
        created_at_ms,
    )
    .await?;
    seed_request_observation(
        pool,
        "request-member",
        &member_tenant,
        &member_access.projects[0].project_id,
        Some(member_access.user_id),
        None,
        None,
        Some(member_job_id),
        200,
        None,
        created_at_ms + 10,
    )
    .await?;
    seed_request_observation(
        pool,
        "request-sibling",
        &member_tenant,
        &member_access.projects[0].project_id,
        Some(sibling_access.user_id),
        None,
        None,
        Some(sibling_job_id),
        200,
        None,
        created_at_ms + 20,
    )
    .await?;
    seed_request_observation(
        pool,
        "request-service-denied",
        &member_tenant,
        &member_access.projects[0].project_id,
        None,
        Some("svc-project-render"),
        Some("key-project-render"),
        None,
        403,
        Some("insufficient_scope"),
        created_at_ms + 30,
    )
    .await?;
    let (member_log_status, _, member_log_body) = get_admin(
        app.clone(),
        &format!(
            "/v1/console/logs?window=24h&visibility=mine&project_id={}",
            member_access.projects[0].project_id
        ),
        &member.access_token,
    )
    .await?;
    require(
        member_log_status == StatusCode::OK
            && member_log_body["items"].as_array().is_some_and(|items| {
                items.len() == 1 && items[0]["request_id"] == "request-member"
            }),
        "personal request logs crossed the authenticated actor boundary",
    )?;
    let (project_log_status, _, project_log_body) = get_admin(
        app.clone(),
        &format!(
            "/v1/console/logs?window=24h&visibility=project&project_id={}",
            member_access.projects[0].project_id
        ),
        &member.access_token,
    )
    .await?;
    require(
        project_log_status == StatusCode::OK
            && project_log_body["items"].as_array().is_some_and(|items| {
                items.len() == 3
                    && items.iter().any(|item| {
                        item["request_id"] == "request-service-denied"
                            && item["status_code"] == 403
                            && item["error_code"] == "insufficient_scope"
                            && item["job_id"].is_null()
                            && item["service_account_id"] == "svc-project-render"
                    })
            }),
        "project request logs omitted a member, service account, or pre-job failure",
    )?;
    let (failed_log_status, _, failed_log_body) = get_admin(
        app.clone(),
        &format!(
            "/v1/console/logs?window=24h&visibility=project&status=failed&project_id={}",
            member_access.projects[0].project_id
        ),
        &member.access_token,
    )
    .await?;
    require(
        failed_log_status == StatusCode::OK
            && failed_log_body["items"].as_array().is_some_and(|items| {
                items.len() == 1 && items[0]["request_id"] == "request-service-denied"
            }),
        "failed request filter did not retain a pre-job authorization failure",
    )?;
    let (member_jobs_status, _, member_jobs_body) = get_admin(
        app.clone(),
        "/v1/console/jobs?window=24h",
        &member.access_token,
    )
    .await?;
    require(
        member_jobs_status == StatusCode::OK,
        &format!("member console read failed: {member_jobs_status} {member_jobs_body}"),
    )?;
    let member_items = member_jobs_body["items"]
        .as_array()
        .ok_or_else(|| "member jobs response has no items".to_string())?;
    require(
        member_items.len() == 1 && member_items[0]["job_id"] == member_job_id.to_string(),
        "member console read crossed the workspace boundary",
    )?;
    let (member_economics_status, _, member_economics_body) = get_admin(
        app.clone(),
        &format!(
            "/v1/console/jobs/{member_job_id}/economics?project_id={}",
            member_access.projects[0].project_id
        ),
        &member.access_token,
    )
    .await?;
    require(
        member_economics_status == StatusCode::OK,
        &format!(
            "member job economics read failed: {member_economics_status} {member_economics_body}"
        ),
    )?;
    require(
        member_economics_body["economics_state"] == "legacy_contract"
            && member_economics_body.get("provider_costs").is_none(),
        "member job economics leaked provider costs or invented a v4 contract",
    )?;
    let foreign_job_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT job_id FROM jobs WHERE tenant_id = 'tenant-admin-read' ORDER BY job_id LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let (foreign_economics_status, _, _) = get_admin(
        app.clone(),
        &format!("/v1/console/jobs/{foreign_job_id}/economics"),
        &member.access_token,
    )
    .await?;
    require(
        foreign_economics_status == StatusCode::NOT_FOUND,
        "job economics must hide out-of-scope jobs behind not found",
    )?;
    let (member_usage_status, _, member_usage_body) = get_admin(
        app.clone(),
        &format!(
            "/v1/console/usage?window=24h&interval=1h&group_by=model&project_id={}",
            member_access.projects[0].project_id
        ),
        &member.access_token,
    )
    .await?;
    require(
        member_usage_status == StatusCode::OK,
        &format!("member usage read failed: {member_usage_status} {member_usage_body}"),
    )?;
    require(
        member_usage_body["activity"]
            .as_array()
            .is_some_and(|rows| {
                rows.iter()
                    .filter(|row| {
                        row["group_kind"] == "model"
                            && row["group_value"] == "model-test"
                            && row["billing_metric"] == "request"
                            && row["quantity"] == "1"
                    })
                    .count()
                    == 1
            }),
        "member usage analysis did not isolate the actor inside the selected project",
    )?;
    require(
        member_usage_body["filter_options"]["users"]
            .as_array()
            .is_some_and(|options| {
                options
                    .iter()
                    .all(|option| option["value"] != sibling_access.user_id.to_string())
            }),
        "member usage filter options exposed another project member",
    )?;
    let (member_project_jobs_status, _, member_project_jobs_body) = get_admin(
        app.clone(),
        &format!(
            "/v1/console/jobs?window=24h&project_id={}",
            member_access.projects[0].project_id
        ),
        &member.access_token,
    )
    .await?;
    require(
        member_project_jobs_status == StatusCode::OK
            && member_project_jobs_body["items"]
                .as_array()
                .is_some_and(|items| {
                    items.len() == 1
                        && items[0]["job_id"] == member_job_id.to_string()
                        && items[0]["job_id"] != sibling_job_id.to_string()
                }),
        "member project job list exposed another project member",
    )?;
    require(
        member_usage_body["filter_options"]["providers"]
            .as_array()
            .is_some_and(|options| {
                options
                    .iter()
                    .any(|option| option["value"] == "provider-test")
            }),
        "member usage filter options omitted the visible provider",
    )?;
    let foreign_member_project_id = format!("proj-foreign-{}", Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO gateway_projects (id, tenant_id, name, created_at) VALUES ($1, $2, 'Foreign member project', $3)",
    )
    .bind(&foreign_member_project_id)
    .bind(&member_tenant)
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let (foreign_usage_status, _, _) = get_admin(
        app.clone(),
        &format!("/v1/console/usage?window=24h&project_id={foreign_member_project_id}"),
        &member.access_token,
    )
    .await?;
    require(
        foreign_usage_status == StatusCode::NOT_FOUND,
        "member must not read another project in the same organization",
    )?;
    let (foreign_jobs_status, _, _) = get_admin(
        app.clone(),
        &format!("/v1/console/jobs?window=24h&project_id={foreign_member_project_id}"),
        &member.access_token,
    )
    .await?;
    require(
        foreign_jobs_status == StatusCode::NOT_FOUND,
        "member job list must not reveal another project in the same organization",
    )?;
    let (foreign_logs_status, _, _) = get_admin(
        app.clone(),
        &format!(
            "/v1/console/logs?window=24h&visibility=project&project_id={foreign_member_project_id}"
        ),
        &member.access_token,
    )
    .await?;
    require(
        foreign_logs_status == StatusCode::NOT_FOUND,
        "member request logs must not reveal another project in the same organization",
    )?;
    let (member_projects_status, _, member_projects_body) = get_admin(
        app.clone(),
        "/v1/organization/projects",
        &member.access_token,
    )
    .await?;
    require(
        member_projects_status == StatusCode::OK
            && member_projects_body["data"]
                .as_array()
                .is_some_and(|projects| {
                    projects.len() == 1 && projects[0]["id"] == member_access.projects[0].project_id
                }),
        "member project list crossed the workspace boundary",
    )?;
    let (cross_organization_status, cross_organization_body) = post_admin(
        app.clone(),
        "/v1/organization/projects",
        &member.access_token,
        json!({
            "organization_id": sibling_tenant,
            "name": "Cross organization project"
        }),
    )
    .await?;
    require(
        cross_organization_status == StatusCode::NOT_FOUND
            && cross_organization_body["error"]["code"] == "organization_not_found",
        "cross-organization project creation did not follow anti-enumeration semantics",
    )?;
    let (create_project_status, create_project_body) = post_admin(
        app.clone(),
        "/v1/organization/projects",
        &member.access_token,
        json!({
            "organization_id": member_tenant,
            "name": "Explicit organization project"
        }),
    )
    .await?;
    let created_project_id = create_project_body["id"]
        .as_str()
        .ok_or_else(|| "created project response omitted its id".to_string())?;
    let persisted_tenant: String =
        sqlx::query_scalar("SELECT tenant_id FROM gateway_projects WHERE id = $1")
            .bind(created_project_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        create_project_status == StatusCode::OK && persisted_tenant == member_tenant,
        "project creation did not persist the explicit organization",
    )?;
    let service_accounts_path = format!(
        "/v1/organization/projects/{}/service_accounts",
        member_access.projects[0].project_id
    );
    let (implicit_permission_status, _) = post_admin(
        app.clone(),
        &service_accounts_path,
        &member.access_token,
        json!({
            "name": "Implicit all must fail"
        }),
    )
    .await?;
    require(
        implicit_permission_status == StatusCode::BAD_REQUEST,
        "service account creation still defaulted the first key to all permissions",
    )?;
    let (service_account_status, service_account_body) = post_admin(
        app.clone(),
        &service_accounts_path,
        &member.access_token,
        json!({
            "name": "Restricted automation",
            "permission_mode": "restricted",
            "permissions": {
                "models": "read",
                "images": "read",
                "videos": "none"
            }
        }),
    )
    .await?;
    let service_api_key_id = service_account_body["api_key"]["id"]
        .as_str()
        .ok_or_else(|| "service account response omitted the first API key id".to_string())?;
    let (permission_mode, permissions): (String, Value) =
        sqlx::query_as("SELECT permission_mode, permissions FROM gateway_api_keys WHERE id = $1")
            .bind(service_api_key_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        service_account_status == StatusCode::OK
            && permission_mode == "restricted"
            && permissions
                == json!({
                    "models": "read",
                    "images": "read",
                    "videos": "none"
                }),
        "service account first key did not persist restricted permissions atomically",
    )?;
    let (member_keys_status, _, member_keys_body) = get_admin(
        app.clone(),
        &format!(
            "/v1/organization/projects/{}/api_keys",
            member_access.projects[0].project_id
        ),
        &sibling.access_token,
    )
    .await?;
    require(
        member_keys_status == StatusCode::OK
            && member_keys_body["data"]
                .as_array()
                .is_some_and(Vec::is_empty),
        "project member did not receive an owner-scoped API key list",
    )?;
    let member_keys_path = format!(
        "/v1/organization/projects/{}/api_keys",
        member_access.projects[0].project_id
    );
    let (create_personal_key_status, create_personal_key_body) = post_admin(
        app.clone(),
        &member_keys_path,
        &sibling.access_token,
        json!({
            "name": "Sibling development",
            "permission_mode": "restricted",
            "permissions": {
                "models": "read",
                "images": "write",
                "videos": "none"
            }
        }),
    )
    .await?;
    let personal_key = create_personal_key_body["value"]
        .as_str()
        .filter(|value| value.starts_with("sk-gw-"))
        .ok_or_else(|| "created project API key did not expose its one-time value".to_string())?
        .to_string();
    require(
        create_personal_key_status == StatusCode::OK,
        "project member could not create a personal API key",
    )?;
    let (models_status, models_request_id) =
        get_api_request(app.clone(), "/v1/models", &personal_key).await?;
    require(
        models_status == StatusCode::OK && models_request_id.starts_with("req_"),
        "project API key could not issue an observable models request",
    )?;
    wait_for_request_observation(pool, &models_request_id).await?;
    let (observed_models_status, _, observed_models_body) = get_admin(
        app.clone(),
        &format!(
            "/v1/console/logs?window=24h&visibility=project&project_id={}&q={models_request_id}",
            member_access.projects[0].project_id
        ),
        &sibling.access_token,
    )
    .await?;
    require(
        observed_models_status == StatusCode::OK
            && observed_models_body["items"]
                .as_array()
                .is_some_and(|items| {
                    items.len() == 1
                        && items[0]["request_id"] == models_request_id
                        && items[0]["source"] == "models"
                        && items[0]["status_code"] == 200
                        && items[0]["project_id"] == member_access.projects[0].project_id
                        && items[0]["actor_user_id"].is_null()
                        && items[0]["credential_owner_user_id"] == sibling.user.id
                        && items[0]["api_key_id"].as_str().is_some()
                }),
        "request observation writer lost the models request or its API-key attribution",
    )?;
    let (personal_models_status, _, personal_models_body) = get_admin(
        app.clone(),
        &format!(
            "/v1/console/logs?window=24h&visibility=mine&project_id={}&q={models_request_id}",
            member_access.projects[0].project_id
        ),
        &sibling.access_token,
    )
    .await?;
    require(
        personal_models_status == StatusCode::OK
            && personal_models_body["items"]
                .as_array()
                .is_some_and(|items| {
                    items.len() == 1
                        && items[0]["request_id"] == models_request_id
                        && items[0]["credential_owner_user_id"] == sibling.user.id
                }),
        "personal API key request was omitted from its owner's logs",
    )?;
    let (personal_keys_status, _, personal_keys_body) =
        get_admin(app.clone(), &member_keys_path, &sibling.access_token).await?;
    require(
        personal_keys_status == StatusCode::OK
            && personal_keys_body["data"].as_array().is_some_and(|keys| {
                keys.len() == 1
                    && keys[0]["owner"]["type"] == "user"
                    && keys[0]["owner"]["user"]["id"] == sibling.user.id
                    && keys[0].get("value").is_none()
            }),
        "project member API key list was not restricted to personal keys",
    )?;
    let owner_project_id = format!("proj_{}", limited.user.id.replace('-', ""));
    let (foreign_project_status, _, _) = get_admin(
        app.clone(),
        &format!("/v1/organization/projects/{owner_project_id}/api_keys"),
        &member.access_token,
    )
    .await?;
    require(
        foreign_project_status == StatusCode::NOT_FOUND,
        "foreign project lookup must fail closed without revealing existence",
    )?;
    let (member_admin_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/provider-accounts",
        &member.access_token,
    )
    .await?;
    require(
        member_admin_status == StatusCode::FORBIDDEN,
        "member must not read platform provider accounts",
    )?;
    let (member_scheduler_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/scheduler/queues?window=24h",
        &member.access_token,
    )
    .await?;
    require(
        member_scheduler_status == StatusCode::FORBIDDEN,
        "member must not read the platform terminal reduction queue",
    )?;
    let (member_audit_status, _, _) = get_admin(
        app.clone(),
        "/v1/organization/audit_logs?window=24h",
        &member.access_token,
    )
    .await?;
    require(
        member_audit_status == StatusCode::FORBIDDEN,
        "member must not read organization audit logs",
    )?;
    let (member_admin_usage_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/usage?window=24h",
        &member.access_token,
    )
    .await?;
    require(
        member_admin_usage_status == StatusCode::FORBIDDEN,
        "member must not read platform usage",
    )?;
    let (member_pricing_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/pricing/official-catalogs",
        &member.access_token,
    )
    .await?;
    require(
        member_pricing_status == StatusCode::FORBIDDEN,
        "member must not observe official pricing catalogs",
    )?;
    let billing_control_path = format!("/admin/v1/billing/accounts/{member_tenant}/USD");
    let (member_billing_control_status, _, _) =
        get_admin(app.clone(), &billing_control_path, &member.access_token).await?;
    require(
        member_billing_control_status == StatusCode::FORBIDDEN,
        "member must not read organization credit-limit controls",
    )?;
    let (member_billing_list_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/billing/accounts?currency=USD&limit=10",
        &member.access_token,
    )
    .await?;
    require(
        member_billing_list_status == StatusCode::FORBIDDEN,
        "member must not list organization credit-limit controls",
    )?;
    let (member_integrity_list_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/billing/integrity-runs?limit=10",
        &member.access_token,
    )
    .await?;
    let (member_integrity_run_status, _) = post_empty_admin(
        app.clone(),
        "/admin/v1/billing/integrity-runs",
        &member.access_token,
    )
    .await?;
    require(
        member_integrity_list_status == StatusCode::FORBIDDEN
            && member_integrity_run_status == StatusCode::FORBIDDEN,
        "member observed or initiated platform billing integrity scans",
    )?;
    let (member_cost_obligations_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/billing/provider-cost-obligations?state=open&limit=10",
        &member.access_token,
    )
    .await?;
    require(
        member_cost_obligations_status == StatusCode::FORBIDDEN,
        "member observed platform provider cost obligations",
    )?;
    let (member_customer_charges_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/billing/customer-charges?state=all&limit=10",
        &member.access_token,
    )
    .await?;
    require(
        member_customer_charges_status == StatusCode::FORBIDDEN,
        "member observed platform customer charges or refunds",
    )?;
    let (member_billing_update_status, _) = put_admin(
        app.clone(),
        &billing_control_path,
        &member.access_token,
        json!({
            "credit_limit_micros": "6000000",
            "expected_control_version": "1",
            "reason": "Unauthorized member update"
        }),
    )
    .await?;
    require(
        member_billing_update_status == StatusCode::FORBIDDEN,
        "member changed an organization credit limit",
    )?;
    let (member_idor_status, _, _) = get_admin(
        app.clone(),
        &format!("/v1/console/jobs?window=24h&user_id={}", limited.user.id),
        &member.access_token,
    )
    .await?;
    require(
        member_idor_status == StatusCode::FORBIDDEN,
        "member must not select another user's console scope",
    )?;

    let (limited_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/provider-accounts",
        &limited.access_token,
    )
    .await?;
    require(
        limited_status == StatusCode::FORBIDDEN,
        "non-owner admin scope must not read platform data",
    )?;
    let (limited_pricing_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/pricing/official-catalogs",
        &limited.access_token,
    )
    .await?;
    require(
        limited_pricing_status == StatusCode::FORBIDDEN,
        "platform owner without admin:* must not observe official pricing catalogs",
    )?;
    let (limited_cost_obligations_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/billing/provider-cost-obligations?state=open&limit=10",
        &limited.access_token,
    )
    .await?;
    require(
        limited_cost_obligations_status == StatusCode::FORBIDDEN,
        "platform owner without admin:* observed provider cost obligations",
    )?;

    sqlx::query(
        "UPDATE identity_users SET scopes = ARRAY['billing:read'], authz_version = authz_version + 1 WHERE normalized_email = 'owner@admin-read.test'",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let billing_reader = identity
        .login(LoginRequest {
            email: "owner@admin-read.test".to_string(),
            password: ADMIN_PASSWORD.to_string(),
            client_id: policy.client_id.clone(),
        })
        .await
        .map_err(debug_error)?;
    let (billing_reader_status, _, billing_reader_body) = get_admin(
        app.clone(),
        "/admin/v1/billing/customer-charges?state=all&limit=10",
        &billing_reader.access_token,
    )
    .await?;
    let (billing_reader_refund_status, _) = post_admin(
        app.clone(),
        &format!(
            "/admin/v1/billing/customer-charges/{}/refunds",
            Uuid::new_v4()
        ),
        &billing_reader.access_token,
        json!({
            "amount_micros": "1",
            "reason_code": "other",
            "reason": "read-only billing scope"
        }),
    )
    .await?;
    require(
        billing_reader_status == StatusCode::OK
            && billing_reader_body["object"] == "list"
            && billing_reader_refund_status == StatusCode::FORBIDDEN,
        "billing:read did not permit read-only refund visibility or allowed refund mutation",
    )?;

    sqlx::query(
        "UPDATE identity_users SET scopes = ARRAY['admin:*'], authz_version = authz_version + 1 WHERE normalized_email = 'owner@admin-read.test'",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let owner = identity
        .login(LoginRequest {
            email: "owner@admin-read.test".to_string(),
            password: ADMIN_PASSWORD.to_string(),
            client_id: policy.client_id,
        })
        .await
        .map_err(debug_error)?;
    let (owner_billing_control_status, _, owner_billing_control_body) =
        get_admin(app.clone(), &billing_control_path, &owner.access_token).await?;
    require(
        owner_billing_control_status == StatusCode::OK
            && owner_billing_control_body["configured"] == true
            && owner_billing_control_body["credit_limit_micros"] == "5000000"
            && owner_billing_control_body["control_version"] == "1",
        "platform owner could not read organization credit-limit controls",
    )?;
    let (owner_billing_list_status, _, owner_billing_list_body) = get_admin(
        app.clone(),
        &format!("/admin/v1/billing/accounts?currency=USD&query={member_tenant}&limit=10"),
        &owner.access_token,
    )
    .await?;
    require(
        owner_billing_list_status == StatusCode::OK
            && owner_billing_list_body["data"]
                .as_array()
                .is_some_and(|items| {
                    items.iter().any(|item| {
                        item["organization_id"] == member_tenant
                            && item["account"]["control_version"] == "1"
                    })
                }),
        "platform owner could not list organization credit-limit controls",
    )?;
    let (
        owner_customer_charges_status,
        owner_customer_charges_cache_control,
        owner_customer_charges_body,
    ) = get_admin(
        app.clone(),
        "/admin/v1/billing/customer-charges?state=all&limit=10",
        &owner.access_token,
    )
    .await?;
    require(
        owner_customer_charges_status == StatusCode::OK
            && owner_customer_charges_body["object"] == "list"
            && owner_customer_charges_body["data"].is_array()
            && owner_customer_charges_cache_control.as_deref() == Some("no-store, max-age=0"),
        "platform owner could not read private customer charge and refund history",
    )?;
    let (owner_billing_update_status, owner_billing_update_body) = put_admin(
        app.clone(),
        &billing_control_path,
        &owner.access_token,
        json!({
            "credit_limit_micros": "6000000",
            "expected_control_version": "1",
            "reason": "Approved by platform billing operations"
        }),
    )
    .await?;
    require(
        owner_billing_update_status == StatusCode::OK
            && owner_billing_update_body["credit_limit_micros"] == "6000000"
            && owner_billing_update_body["control_version"] == "2",
        "platform owner could not update an organization credit limit",
    )?;
    let (stale_billing_update_status, _) = put_admin(
        app.clone(),
        &billing_control_path,
        &owner.access_token,
        json!({
            "credit_limit_micros": "7000000",
            "expected_control_version": "1",
            "reason": "Stale platform billing update"
        }),
    )
    .await?;
    require(
        stale_billing_update_status == StatusCode::CONFLICT,
        "stale platform billing update did not return conflict",
    )?;
    let (owner_integrity_run_status, owner_integrity_run_body) = post_empty_admin(
        app.clone(),
        "/admin/v1/billing/integrity-runs",
        &owner.access_token,
    )
    .await?;
    let integrity_run_id = owner_integrity_run_body["run_id"]
        .as_str()
        .ok_or_else(|| "integrity run response omitted run_id".to_string())?;
    require(
        owner_integrity_run_status == StatusCode::OK
            && owner_integrity_run_body["state"] == "completed"
            && owner_integrity_run_body["findings"].is_array(),
        "platform owner could not run a billing integrity snapshot",
    )?;
    let (owner_integrity_list_status, _, owner_integrity_list_body) = get_admin(
        app.clone(),
        "/admin/v1/billing/integrity-runs?limit=10",
        &owner.access_token,
    )
    .await?;
    let (owner_integrity_detail_status, _, owner_integrity_detail_body) = get_admin(
        app.clone(),
        &format!("/admin/v1/billing/integrity-runs/{integrity_run_id}"),
        &owner.access_token,
    )
    .await?;
    require(
        owner_integrity_list_status == StatusCode::OK
            && owner_integrity_list_body["data"]
                .as_array()
                .is_some_and(|runs| runs.iter().any(|run| run["run_id"] == integrity_run_id))
            && owner_integrity_detail_status == StatusCode::OK
            && owner_integrity_detail_body["run_id"] == integrity_run_id,
        "platform owner could not list or replay billing integrity evidence",
    )?;
    let (
        owner_cost_obligations_status,
        owner_cost_obligations_cache_control,
        owner_cost_obligations_body,
    ) = get_admin(
        app.clone(),
        "/admin/v1/billing/provider-cost-obligations?state=open&limit=10",
        &owner.access_token,
    )
    .await?;
    require(
        owner_cost_obligations_status == StatusCode::OK
            && owner_cost_obligations_body["object"] == "list"
            && owner_cost_obligations_body["summary"]["open"].is_number()
            && owner_cost_obligations_body["data"].is_array(),
        "platform owner could not list provider cost obligations",
    )?;
    require(
        owner_cost_obligations_cache_control.as_deref() == Some("no-store, max-age=0"),
        "provider cost obligations response must be private and non-cacheable",
    )?;
    let (owner_economics_status, _, owner_economics_body) = get_admin(
        app.clone(),
        &format!("/admin/v1/jobs/{member_job_id}/economics"),
        &owner.access_token,
    )
    .await?;
    require(
        owner_economics_status == StatusCode::OK
            && owner_economics_body["provider_costs"].is_array(),
        "platform owner could not read the complete job economics projection",
    )?;
    let (owner_member_status, _, owner_member_body) = get_admin(
        app.clone(),
        &format!(
            "/v1/console/jobs?window=24h&user_id={}",
            member_access.user_id
        ),
        &owner.access_token,
    )
    .await?;
    require(
        owner_member_status == StatusCode::OK
            && owner_member_body["items"].as_array().is_some_and(|items| {
                items.len() == 1 && items[0]["job_id"] == member_job_id.to_string()
            }),
        "platform owner could not inspect the selected user scope",
    )?;
    let (owner_logs_status, _, owner_logs_body) = get_admin(
        app.clone(),
        "/admin/v1/logs?window=24h&visibility=project",
        &owner.access_token,
    )
    .await?;
    require(
        owner_logs_status == StatusCode::OK
            && owner_logs_body["items"].as_array().is_some_and(|items| {
                items
                    .iter()
                    .any(|item| item["request_id"] == "request-member")
                    && items
                        .iter()
                        .any(|item| item["request_id"] == "request-sibling")
                    && items
                        .iter()
                        .any(|item| item["request_id"] == "request-service-denied")
            }),
        "platform owner request logs omitted project actors",
    )?;
    let (owner_pricing_status, _, _) = get_admin(
        app.clone(),
        "/admin/v1/pricing/official-catalogs",
        &owner.access_token,
    )
    .await?;
    require(
        owner_pricing_status == StatusCode::SERVICE_UNAVAILABLE,
        "platform owner must pass pricing authorization before the optional service boundary",
    )?;
    let (owner_scheduler_status, _, owner_scheduler_body) = get_admin(
        app.clone(),
        "/admin/v1/scheduler/queues?window=24h",
        &owner.access_token,
    )
    .await?;
    require(
        owner_scheduler_status == StatusCode::OK
            && owner_scheduler_body["blocked_terminal_reductions"] == "0"
            && owner_scheduler_body["blocked_terminals"]
                .as_array()
                .is_some_and(Vec::is_empty),
        "platform owner scheduler read omitted the blocked terminal projection",
    )?;
    let (owner_audit_status, audit_cache_control, owner_audit_body) = get_admin(
        app.clone(),
        "/v1/organization/audit_logs?window=24h&limit=10",
        &owner.access_token,
    )
    .await?;
    require(
        owner_audit_status == StatusCode::OK
            && owner_audit_body["object"] == "list"
            && owner_audit_body["data"]
                .as_array()
                .is_some_and(|events| !events.is_empty()),
        "platform owner audit log read did not expose existing control-plane events",
    )?;
    require(
        audit_cache_control.as_deref() == Some("no-store, max-age=0"),
        "audit log responses must be private and non-cacheable",
    )?;
    let (owner_status, cache_control, owner_body) =
        get_admin(app, "/admin/v1/provider-accounts", &owner.access_token).await?;
    require(
        owner_status == StatusCode::OK,
        &format!("platform owner read failed: {owner_status} {owner_body}"),
    )?;
    require(
        cache_control.as_deref() == Some("no-store, max-age=0"),
        "admin response must be private and non-cacheable",
    )?;
    let serialized = serde_json::to_string(&owner_body).map_err(debug_error)?;
    require(
        !serialized.contains("\"credential_ref\":")
            && !serialized.contains("vault.admin-read.secret"),
        "provider account response leaked credential material",
    )
}

#[allow(clippy::too_many_arguments)]
async fn seed_request_observation(
    pool: &PgPool,
    request_id: &str,
    tenant_id: &str,
    project_id: &str,
    actor_user_id: Option<Uuid>,
    service_account_id: Option<&str>,
    api_key_id: Option<&str>,
    job_id: Option<Uuid>,
    status_code: i16,
    error_code: Option<&str>,
    created_at_ms: i64,
) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO gateway_request_observations (
            request_id, source, method, route_pattern, request_path,
            status_code, duration_ms, error_code, tenant_id, project_id,
            service_account_id, api_key_id, actor_user_id, auth_kind,
            job_id, created_at_ms, completed_at_ms
        )
        VALUES (
            $1, 'images', 'POST', '/v1/images/generations',
            '/v1/images/generations', $2, 12, $3, $4, $5,
            $6, $7, $8,
            CASE WHEN $8::UUID IS NULL THEN 'api_key' ELSE 'user_session' END,
            $9, $10, $10 + 12
        )
        "#,
    )
    .bind(request_id)
    .bind(status_code)
    .bind(error_code)
    .bind(tenant_id)
    .bind(project_id)
    .bind(service_account_id)
    .bind(api_key_id)
    .bind(actor_user_id)
    .bind(job_id)
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn seed_job_for_user(
    pool: &PgPool,
    tenant_id: &str,
    project_id: &str,
    actor_user_id: Uuid,
    actor_session_id: Uuid,
    actor_authz_version: i64,
    request_id: &str,
    created_at_ms: i64,
) -> TestResult<Uuid> {
    let job_id = Uuid::new_v4();
    let route_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model,
           state, requested_units, output_count, billable_units,
           billing_metric, billing_unit, economics_contract_version,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 'generation', 'provider-test', 'model-test',
                'queued', 1, 1, 1, 'output', 'output', 2, $4, $4)
        "#,
    )
    .bind(job_id)
    .bind(tenant_id)
    .bind(request_id)
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_routes
          (route_id, revision, route_key, display_name, provider_id,
           operation_id, command_schema, route_kind, selection_strategy,
           state, created_at_ms)
        VALUES ($1, 1, $2, 'User console route', 'provider-test',
                'images.generations', 'provider.command.v1', 'account',
                'quota_aware_least_loaded', 'enabled', $3)
        "#,
    )
    .bind(route_id)
    .bind(format!("user-console-{route_id}"))
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions
          (job_id, tenant_id, project_id, actor_user_id, actor_session_id,
           actor_authz_version, route_provider_id, route_operation_id,
           route_command_schema, route_id, route_revision, auth_kind,
           admitted_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, 'provider-test',
                'images.generations', 'provider.command.v1', $7, 1,
                'user_session', $8)
        "#,
    )
    .bind(job_id)
    .bind(tenant_id)
    .bind(project_id)
    .bind(actor_user_id)
    .bind(actor_session_id)
    .bind(actor_authz_version)
    .bind(route_id)
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(job_id)
}

async fn get_admin(
    app: Router,
    path: &str,
    access_token: &str,
) -> TestResult<(StatusCode, Option<String>, Value)> {
    let response = app
        .oneshot(
            Request::builder()
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {access_token}"))
                .body(Body::empty())
                .map_err(debug_error)?,
        )
        .await
        .map_err(debug_error)?;
    let status = response.status();
    let cache_control = response
        .headers()
        .get(header::CACHE_CONTROL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(debug_error)?;
    let body = serde_json::from_slice(&body).map_err(|error| {
        format!(
            "GET {path} returned non-JSON body at status {status}: {error}; body={:?}",
            String::from_utf8_lossy(&body)
        )
    })?;
    Ok((status, cache_control, body))
}

async fn get_api_request(
    app: Router,
    path: &str,
    api_key: &str,
) -> TestResult<(StatusCode, String)> {
    let response = app
        .oneshot(
            Request::builder()
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {api_key}"))
                .body(Body::empty())
                .map_err(debug_error)?,
        )
        .await
        .map_err(debug_error)?;
    let status = response.status();
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "gateway response omitted x-request-id".to_string())?
        .to_string();
    Ok((status, request_id))
}

async fn wait_for_request_observation(pool: &PgPool, request_id: &str) -> TestResult {
    for _ in 0..50 {
        let observed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM gateway_request_observations WHERE request_id = $1)",
        )
        .bind(request_id)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
        if observed {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(format!(
        "request observation was not persisted for {request_id}"
    ))
}

async fn put_admin(
    app: Router,
    path: &str,
    access_token: &str,
    payload: Value,
) -> TestResult<(StatusCode, Value)> {
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {access_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&payload).map_err(debug_error)?,
                ))
                .map_err(debug_error)?,
        )
        .await
        .map_err(debug_error)?;
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(debug_error)?;
    let body = serde_json::from_slice(&body).map_err(debug_error)?;
    Ok((status, body))
}

async fn post_admin(
    app: Router,
    path: &str,
    access_token: &str,
    payload: Value,
) -> TestResult<(StatusCode, Value)> {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {access_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&payload).map_err(debug_error)?,
                ))
                .map_err(debug_error)?,
        )
        .await
        .map_err(debug_error)?;
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(debug_error)?;
    let body = serde_json::from_slice(&body).map_err(debug_error)?;
    Ok((status, body))
}

async fn post_empty_admin(
    app: Router,
    path: &str,
    access_token: &str,
) -> TestResult<(StatusCode, Value)> {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {access_token}"))
                .body(Body::empty())
                .map_err(debug_error)?,
        )
        .await
        .map_err(debug_error)?;
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(debug_error)?;
    let body = serde_json::from_slice(&body).map_err(debug_error)?;
    Ok((status, body))
}

fn admin_api_config() -> AppConfig {
    AppConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        admin_token: Some("legacy-admin-token".to_string()),
        legacy_admin_auth_enabled: true,
        database_url: None,
        generation_admission_contract: Default::default(),
        enable_xai_video_api: false,
        five_hour_image_limit: 100,
        seven_day_image_limit: 100,
        five_hour_video_second_limit: 100,
        seven_day_video_second_limit: 100,
        max_concurrent_jobs: 2,
        max_queue_size: 4,
        max_concurrent_jobs_per_tenant: 2,
        max_queue_size_per_tenant: 4,
        queue_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(30),
        readiness_timeout: Duration::from_millis(500),
        max_upload_bytes: 1024 * 1024,
        proxy: ProxyConfig::default(),
        codex_home: None,
        cleanup_codex_outputs: false,
    }
}

async fn assert_admin_migration(pool: &PgPool) -> TestResult {
    let applied: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version = 35 AND success)",
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(applied, "admin read migration 35 was not applied")?;
    let usage_indexes_applied: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version = 72 AND success)",
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        usage_indexes_applied,
        "usage analysis migration 72 was not applied",
    )?;
    let indexes: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND indexname IN (
              'jobs_admin_global_created_idx',
              'jobs_admin_request_id_idx',
              'ledger_transaction_seals_admin_sealed_idx',
              'provider_remote_tasks_admin_uncertain_terminal_idx',
              'customer_rated_usage_lines_created_job_idx',
              'rated_usage_job_created_idx'
          )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(indexes == 6, "required admin read indexes are missing")
}

async fn seed_delayed_usage_event(pool: &PgPool) -> TestResult<Uuid> {
    let job_id = Uuid::new_v4();
    let now_ms = database_now(pool).await?;
    let job_created_at_ms = now_ms - 2 * 24 * 60 * 60 * 1_000;
    let usage_created_at_ms = now_ms - 10_000;
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model,
           state, requested_units, output_count, billable_units,
           billing_metric, billing_unit, economics_contract_version,
           created_at_ms, updated_at_ms)
        VALUES ($1, 'tenant-admin-read', $2, 'generation', 'provider-test',
                'model-delayed', 'succeeded', 1, 1, 1, 'output', 'output',
                2, $3, $4)
        "#,
    )
    .bind(job_id)
    .bind(format!("request-delayed-{job_id}"))
    .bind(job_created_at_ms)
    .bind(now_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO usage_events
          (event_id, tenant_id, request_id, operation, units, outcome,
           created_at_ms, billing_metric, billing_unit, job_id)
        VALUES ($1, 'tenant-admin-read', $2, 'generation', 3, 'succeeded',
                $3, 'output', 'output', $4)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(format!("request-delayed-{job_id}"))
    .bind(usage_created_at_ms)
    .bind(job_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(job_id)
}

struct BlockedTerminalSeed {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    job_id: Uuid,
    request_id: String,
}

async fn seed_blocked_terminal_reduction(pool: &PgPool) -> TestResult<BlockedTerminalSeed> {
    let now = database_now(pool).await?;
    let job_id = Uuid::new_v4();
    let output_id = Uuid::new_v4();
    let work_item_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let admission_session_id = Uuid::new_v4();
    let request_id = format!("request-blocked-{}", Uuid::new_v4().simple());
    let command_json = json!({
        "schema_version": 1,
        "operation": "generation",
        "n": 1,
        "prompt": "blocked terminal admin read"
    });

    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model, state,
           requested_units, output_count, billable_units, billing_metric, billing_unit,
           economics_contract_version, created_at_ms, updated_at_ms)
        VALUES ($1, 'tenant-admin-read', $2, 'generation', 'provider-test',
                'model-test', 'reserved', 1, 1, 1, 'output', 'output', 2, $3, $3)
        "#,
    )
    .bind(job_id)
    .bind(&request_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_outputs
          (output_id, job_id, output_index, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 0, 'pending', $3, $3)
        "#,
    )
    .bind(output_id)
    .bind(job_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO admission_sessions
          (session_id, owner_token, tenant_id, project_id, api_profile, operation,
           request_id, request_hash, state, job_id, deadline_at_ms,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'tenant-admin-read', 'project-admin-read',
                'admin-read-v1', 'generation', $3, $4, 'attached', $5,
                $6, $7, $7)
        "#,
    )
    .bind(admission_session_id)
    .bind(Uuid::new_v4())
    .bind(&request_id)
    .bind("d".repeat(64))
    .bind(job_id)
    .bind(now + 300_000)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_payloads
          (job_id, admission_session_id, command_schema, command_json,
           request_hash, created_at_ms)
        VALUES ($1, $2, 'provider.command.v1', $3, $4, $5)
        "#,
    )
    .bind(job_id)
    .bind(admission_session_id)
    .bind(&command_json)
    .bind("d".repeat(64))
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms, lease_epoch,
           lease_owner, lease_expires_at_ms, execution_id,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'generation', 'leased', $3, 1,
                'admin-read-worker', $4, $5, $3, $3)
        "#,
    )
    .bind(work_item_id)
    .bind(job_id)
    .bind(now)
    .bind(now + 300_000)
    .bind(execution_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts
          (attempt_id, execution_id, work_item_id, lease_epoch, worker_id,
           state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 1, 'admin-read-worker', 'claimed', $4, $4)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(execution_id)
    .bind(work_item_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let (execution_profile_id, provider_id, command_schema, adapter_revision): (
        Uuid,
        String,
        String,
        String,
    ) = sqlx::query_as(
        r#"
        SELECT execution_profile_id, provider_id, command_schema, adapter_revision
        FROM provider_execution_profiles
        WHERE profile_key = 'admin-read-profile'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let lease = WorkLease {
        work_item_id,
        job_id,
        execution_id,
        lease_epoch: 1,
        worker_id: "admin-read-worker".to_string(),
        command_schema: command_schema.clone(),
        command_json,
    };
    let submissions = PostgresExecutorSubmissionStore::new(pool.clone());
    submissions
        .prepare_and_handoff(&lease, execution_profile_id)
        .await
        .map_err(debug_error)?;
    let executor_lease = submissions
        .claim_prepared(
            &ExecutorClaimScope {
                execution_profile_id,
                provider_id,
                command_schema,
                adapter_revision,
            },
            "admin-read-executor",
            60_000,
        )
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "admin read blocked seed was not claimable".to_string())?;
    submissions
        .start(&executor_lease)
        .await
        .map_err(debug_error)?;
    submissions
        .record_outcome(
            &executor_lease,
            &ExecutorSubmissionOutcome::Failed {
                error_code: "provider_failed".to_string(),
            },
        )
        .await
        .map_err(debug_error)?;

    let reductions = PostgresExecutorTerminalStore::new(pool.clone());
    let terminal = reductions
        .claim_terminal("admin-read-reducer", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "admin read terminal reduction was not queued".to_string())?;
    reductions
        .block_terminal(&terminal, ExecutorTerminalBlockReason::CanonicalConflict)
        .await
        .map_err(debug_error)?;
    Ok(BlockedTerminalSeed {
        submission_id: terminal.submission_id,
        executor_execution_id: terminal.executor_execution_id,
        job_id,
        request_id,
    })
}

async fn seed_large_billing_account(pool: &PgPool) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO billing_accounts
          (tenant_id, currency, credit_limit_micros, held_micros,
           captured_micros, created_at_ms, updated_at_ms)
        VALUES ('tenant-admin-read', 'USD', 9007199254740993, 1, 2, 1, 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn seed_sensitive_provider_profile(pool: &PgPool) -> TestResult {
    let pool_id = Uuid::new_v4();
    let account_id = Uuid::new_v4();
    let policy_id = Uuid::new_v4();
    let profile_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO provider_credential_pools (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms) VALUES ($1, 'admin-read-pool', 'provider-test', 'enabled', 1, 1)",
    )
    .bind(pool_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_accounts
          (provider_account_id, credential_pool_id, provider_id, account_key,
           credential_ref, credential_revision, credential_auth_sha256,
           state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-test', 'admin-read-account',
                'vault.admin-read.secret', 1, $3, 'enabled', 1, 1)
        "#,
    )
    .bind(account_id)
    .bind(pool_id)
    .bind("a".repeat(64))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_resource_policies
          (resource_policy_id, revision, credential_pool_id, provider_account_id,
           provider_id, execution_class, max_concurrency, allocated_count,
           state, created_at_ms)
        VALUES ($1, 1, $2, $3, 'provider-test', 'admin-read-class',
                7, 0, 'enabled', 1)
        "#,
    )
    .bind(policy_id)
    .bind(pool_id)
    .bind(account_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_execution_profiles
          (execution_profile_id, profile_key, provider_id, command_schema,
           adapter_revision, credential_pool_id, provider_account_id,
           credential_ref, credential_revision, resource_policy_id,
           resource_policy_revision, state, created_at_ms, updated_at_ms,
           operation_id, operation_descriptor_revision,
           operation_descriptor_sha256_v1, completion_mode, idempotency_mode)
        VALUES ($1, 'admin-read-profile', 'provider-test', 'provider.command.v1',
                'adapter-v1', $2, $3, 'vault.admin-read.secret', 1, $4, 1,
                'enabled', 1, 1, 'images.generations',
                'provider-test/images.generations/v1', $5, 'inline',
                'submission_bound')
        "#,
    )
    .bind(profile_id)
    .bind(pool_id)
    .bind(account_id)
    .bind(policy_id)
    .bind("b".repeat(64))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_execution_profiles
          (execution_profile_id, profile_key, provider_id, command_schema,
           adapter_revision, credential_pool_id, provider_account_id,
           credential_ref, credential_revision, resource_policy_id,
           resource_policy_revision, state, created_at_ms, updated_at_ms,
           operation_id, operation_descriptor_revision,
           operation_descriptor_sha256_v1, completion_mode, idempotency_mode)
        SELECT $1, 'admin-read-profile-secondary', provider_id, 'provider.video.v1',
               adapter_revision, credential_pool_id, provider_account_id,
               credential_ref, credential_revision, resource_policy_id,
               resource_policy_revision, state, created_at_ms, updated_at_ms,
               'videos.generations', 'provider-test/videos.generations/v1',
               $3, completion_mode, idempotency_mode
        FROM provider_execution_profiles
        WHERE execution_profile_id = $2
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(profile_id)
    .bind("c".repeat(64))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn seed_same_millisecond_jobs(pool: &PgPool, created_at_ms: i64) -> TestResult<[Uuid; 3]> {
    let ids = [
        Uuid::parse_str("00000000-0000-4000-8000-000000000001").map_err(debug_error)?,
        Uuid::parse_str("00000000-0000-4000-8000-000000000002").map_err(debug_error)?,
        Uuid::parse_str("00000000-0000-4000-8000-000000000003").map_err(debug_error)?,
    ];
    for (index, (job_id, state)) in ids
        .iter()
        .zip(["reserved", "reserved", "uncertain"])
        .enumerate()
    {
        sqlx::query(
            r#"
            INSERT INTO jobs
              (job_id, tenant_id, request_id, operation, provider_id, model,
               state, requested_units, output_count, billable_units,
               billing_metric, billing_unit, economics_contract_version,
               created_at_ms, updated_at_ms)
            VALUES ($1, 'tenant-admin-read', $2, 'generation', 'provider-test',
                    'model-test', $3, 1, 1, 1, 'output', 'output', 2, $4, $4)
            "#,
        )
        .bind(job_id)
        .bind(format!("request-admin-read-{index}"))
        .bind(state)
        .bind(created_at_ms)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    }
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'generation', 'ready', $3, $3, $3)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(ids[0])
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms, lease_epoch,
           lease_owner, lease_expires_at_ms, execution_id,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'generation', 'leased', $3, 1,
                'admin-read-worker', $4, $5, $3, $3)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(ids[1])
    .bind(created_at_ms)
    .bind(created_at_ms + 60_000)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'generation', 'ready', $3, $3, $3)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(ids[2])
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(ids)
}

async fn seed_job_attribution(pool: &PgPool, job_id: Uuid) -> TestResult {
    sqlx::query(
        "INSERT INTO gateway_projects (id, tenant_id, name, created_at) VALUES ('proj-admin-read', 'tenant-admin-read', 'Admin read', 1)",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO gateway_service_accounts
          (id, project_id, tenant_id, name, role, created_at)
        VALUES ('svc-admin-read', 'proj-admin-read', 'tenant-admin-read',
                'Admin read', 'member', 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO gateway_api_keys
          (id, project_id, tenant_id, service_account_id, name, key_hash,
           hash_algorithm, pepper_version, redacted_value, created_at)
        VALUES ('key-admin-read', 'proj-admin-read', 'tenant-admin-read',
                'svc-admin-read', 'Admin read', $1, 'hmac-sha256-v1', 1,
                'sk-gw-...read', 1)
        "#,
    )
    .bind("a".repeat(64))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions
          (job_id, tenant_id, project_id, service_account_id, api_key_id,
           credential_authz_version, auth_kind, admitted_at_ms)
        VALUES ($1, 'tenant-admin-read', 'proj-admin-read', 'svc-admin-read',
                'key-admin-read', 1, 'api_key', 1)
        "#,
    )
    .bind(job_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn seed_pending_batch_request(pool: &PgPool, created_at_ms: i64) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO project_files
          (file_id, tenant_id, project_id, purpose, filename, storage_backend,
           object_key, sha256_hex, byte_size, created_at_ms, updated_at_ms)
        VALUES
          ('file-admin-read-batch', 'tenant-admin-read', 'proj-admin-read',
           'batch', 'admin-read.jsonl', 'filesystem-v1',
           'batch-files/aa/11111111111111111111111111111111',
           $1, 128, $2, $2)
        "#,
    )
    .bind("1".repeat(64))
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO project_batches
          (batch_id, tenant_id, project_id, input_file_id, endpoint, model,
           status, auth_snapshot, route_snapshot, request_count_total,
           created_at_ms, validated_at_ms, in_progress_at_ms, expires_at_ms,
           updated_at_ms)
        VALUES
          ('batch-admin-read', 'tenant-admin-read', 'proj-admin-read',
           'file-admin-read-batch', '/v1/images/generations', 'model-test',
           'in_progress', '{}'::JSONB, '{}'::JSONB, 1,
           $1, $1, $1, $2, $1)
        "#,
    )
    .bind(created_at_ms)
    .bind(created_at_ms + 86_400_000)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO project_batch_requests
          (request_id, tenant_id, project_id, batch_id, ordinal, custom_id,
           method, request_url, model, request_body, request_hash, state,
           available_at_ms, created_at_ms, updated_at_ms)
        VALUES
          ($1, 'tenant-admin-read', 'proj-admin-read', 'batch-admin-read', 0,
           'request-admin-read', 'POST', '/v1/images/generations',
           'model-test', '{"model":"model-test","prompt":"test"}'::JSONB,
           $2, 'pending', $3, $3, $3)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind("2".repeat(64))
    .bind(created_at_ms)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn database_now(pool: &PgPool) -> TestResult<i64> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM transaction_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(debug_error)
}

fn require(condition: bool, message: &str) -> TestResult {
    condition.then_some(()).ok_or_else(|| message.to_string())
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

struct TestSchema {
    name: String,
    pool: PgPool,
}

impl TestSchema {
    async fn new(max_connections: u32) -> TestResult<Option<Self>> {
        let database_url = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
            .or_else(|| {
                env::var("GATEWAY_TEST_DATABASE_URL")
                    .ok()
                    .filter(|url| !url.trim().is_empty())
            });
        let Some(database_url) = database_url else {
            if env::var_os("CI").is_some() {
                return Err(
                    "TEST_DATABASE_URL or GATEWAY_TEST_DATABASE_URL must be set in CI".to_string(),
                );
            }
            eprintln!(
                "skipping PostgreSQL admin read test: TEST_DATABASE_URL and GATEWAY_TEST_DATABASE_URL are not set"
            );
            return Ok(None);
        };
        let name = format!("admin_read_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, max_connections, &name)
            .await
            .map_err(|error| format!("test database should be reachable: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because database {database_name:?} is not a test database"
            ));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .map_err(debug_error);
        self.pool.close().await;
        result.map(|_| ())
    }
}
