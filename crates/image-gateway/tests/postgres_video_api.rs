use std::{
    env,
    io::Cursor,
    sync::{Arc, OnceLock},
    time::Duration,
};

use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gpt_image_2_gateway::{
    ApiKeyKeyring, ApiKeyPermissionMode, ApiKeyPermissions, ApiKeyStore, AppConfig,
    ExternalImageGatewayComponents, GenerationAdmissionContract, PostgresApiKeyStore,
    PostgresArtifactRetentionStore, PostgresExecutionContextStore, PostgresProviderTaskStore,
    PostgresUsageStore, ProxyConfig, Workerd,
    admission::{
        AdmissionContract, AdmissionStore, PostgresAdmissionStore,
        XAI_VIDEO_INPUT_MANIFEST_SCHEMA_V2,
    },
    artifacts::{ExecutorArtifactPublisher, FilesystemArtifactBlobStore},
    build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing,
    database::{connect_test_pool_with_search_path, run_migrations},
    executor::{
        ExecutorClaimScope, ExecutorHandoffStore, ExecutorSubmissionOutcome,
        ExecutorSubmissionStore, GrokExecutionProfileProvisioning, PostgresExecutorSubmissionStore,
        provision_grok_video_v2_execution_profile,
    },
    model_routing::PostgresModelRoutingStore,
    pricing::{
        CreatePriceBookRequest, CreatePriceBookVersionRequest, PostgresPricingAdminService,
        PriceBookVersionDraft, PriceComponentDraft, PricingAdminService,
        TransitionPriceBookVersionRequest,
    },
    provision_grok_video_execution_profile, reconcile_artifact_retention,
    reconcile_execution_profile_routes,
    reduction::{CustomerArtifactPublisher, ExecutorTerminalStore, PostgresExecutorTerminalStore},
    settlement::{ExecutionSettlementStore, PostgresExecutionSettlementStore},
};
use image::{ImageBuffer, ImageFormat, Rgba};
use image_provider_grok_cli::{
    GROK_VIDEO_GENERATION_COMMAND_SCHEMA, GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2,
    GROK_VIDEO_GENERATION_OPERATION_V2, PROVIDER_ID, VIDEO_ADAPTER_REVISION_V2,
};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, PgPool};
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const UNIT_PRICE_MICROS: i64 = 10;
const IMAGE_INPUT_PRICE_MICROS: i64 = 2;
const DURATION_SECONDS: i32 = 6;

static POSTGRES_VIDEO_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn postgres_video_test_lock() -> &'static tokio::sync::Mutex<()> {
    POSTGRES_VIDEO_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[tokio::test]
async fn xai_video_api_runs_one_tenant_scoped_billed_mp4_job_end_to_end() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let keys = Arc::new(PostgresApiKeyStore::new(
            database.pool.clone(),
            ApiKeyKeyring::new(1, [(1, vec![0x44; 32])]).map_err(debug_error)?,
        ));
        let owner_project = keys
            .create_project("Video project")
            .await
            .map_err(debug_error)?;
        let v1_project = keys
            .create_project("Legacy V1 video project")
            .await
            .map_err(debug_error)?;
        let other_project = keys
            .create_project("Other video project")
            .await
            .map_err(debug_error)?;
        let profile = provision_grok_video_execution_profile(
            &database.pool,
            &GrokExecutionProfileProvisioning {
                profile_key: "grok-video-e2e".to_owned(),
                credential_pool_key: "grok-video-e2e-pool".to_owned(),
                provider_account_key: "grok-video-e2e-account".to_owned(),
                credential_ref: "private:grok-video-e2e".to_owned(),
                credential_revision: 1,
                credential_auth_sha256: "a".repeat(64),
                max_concurrency: 1,
            },
        )
        .await
        .map_err(debug_error)?;

        let artifact_root = TempDir::new().map_err(debug_error)?;
        let blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let owner = keys
            .create_service_account(
                &owner_project.id,
                "Video owner",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        let v1_owner = keys
            .create_service_account(
                &v1_project.id,
                "Legacy V1 video owner",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        let other = keys
            .create_service_account(
                &other_project.id,
                "Other tenant",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        seed_video_route(
            &database.pool,
            &owner_project.id,
            &owner.id,
            &owner.api_key.id,
            profile.provider_account_id,
            profile.execution_profile_id,
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO gateway_api_key_provider_routes
              (api_key_id, service_account_id, project_id, tenant_id, provider_id,
               operation_id, command_schema, route_id, route_revision, bound_at_ms)
            SELECT $1, $2, $3, $3, provider_id, operation_id, command_schema,
                   route_id, route_revision, bound_at_ms
            FROM gateway_api_key_provider_routes
            WHERE api_key_id = $4 AND provider_id = 'grok-cli'
              AND operation_id = 'videos.generations'
            "#,
        )
        .bind(&v1_owner.api_key.id)
        .bind(&v1_owner.id)
        .bind(&v1_project.id)
        .bind(&owner.api_key.id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        seed_video_economics(&database.pool, &owner_project.id).await?;
        sqlx::query(
            "INSERT INTO billing_accounts
               (tenant_id, currency, credit_limit_micros, held_micros, captured_micros,
                created_at_ms, updated_at_ms)
             SELECT $1, currency, credit_limit_micros, 0, 0, created_at_ms, updated_at_ms
             FROM billing_accounts WHERE tenant_id = $2 AND currency = 'USD'",
        )
        .bind(&v1_project.id)
        .bind(&owner_project.id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let settlement = Arc::new(PostgresExecutionSettlementStore::new(
            database.pool.clone(),
            blobs.clone(),
        ));
        let app = build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing(
            config(),
            ExternalImageGatewayComponents {
                usage_store: Arc::new(PostgresUsageStore::new(database.pool.clone())),
                api_key_store: keys.clone(),
                admission_store: Arc::new(PostgresAdmissionStore::new(database.pool.clone())),
                settlement_store: settlement.clone(),
                input_blob_store: blobs.clone(),
                provider_readiness_store: Arc::new(PostgresProviderTaskStore::new(
                    database.pool.clone(),
                )),
            },
            None,
            None,
            None,
            None,
            Some(Arc::new(PostgresModelRoutingStore::new(database.pool.clone()))),
        )
        .map_err(debug_error)?;
        // The route seeded above is the legacy V1 binding.  Exercise it before
        // opting this key into the additive V2 fixture so a V2 rollout cannot
        // silently reinterpret existing video traffic.
        let v1_body = video_request();
        let (v1_status, v1_created) = json_request(
            app.clone(),
            Method::POST,
            "/v1/videos/generations",
            &v1_owner.api_key.value,
            Some("video-v1-compatibility"),
            Some(&v1_body),
        )
        .await?;
        require(
            v1_status == StatusCode::OK,
            format!("legacy V1 video creation failed: {v1_status} {v1_created}"),
        )?;
        let v1_job_id = Uuid::parse_str(
            v1_created["request_id"]
                .as_str()
                .ok_or_else(|| "legacy V1 creation omitted request_id".to_owned())?,
        )
        .map_err(debug_error)?;
        let v1_schema: String =
            sqlx::query_scalar("SELECT command_schema FROM job_payloads WHERE job_id = $1")
                .bind(v1_job_id)
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        require(
            v1_schema == GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
            format!("legacy V1 route was not preserved: {v1_schema}"),
        )?;
        let (v1_replay_status, v1_replay) = json_request(
            app.clone(),
            Method::POST,
            "/v1/videos/generations",
            &v1_owner.api_key.value,
            Some("video-v1-compatibility"),
            Some(&v1_body),
        )
        .await?;
        require(
            v1_replay_status == StatusCode::OK && v1_replay == v1_created,
            format!("legacy V1 idempotent replay diverged: {v1_replay_status} {v1_replay}"),
        )?;
        let v2_profile_id = activate_v2_fixture(
            &database.pool,
            &owner.api_key.id,
            profile.provider_account_id,
        )
        .await?;
        let (v1_after_status, v1_after_created) = json_request(
            app.clone(),
            Method::POST,
            "/v1/videos/generations",
            &v1_owner.api_key.value,
            Some("video-v1-compatibility-after-v2"),
            Some(&v1_body),
        )
        .await?;
        require(
            v1_after_status == StatusCode::OK,
            format!(
                "legacy V1 route failed after V2 activation: {v1_after_status} {v1_after_created}"
            ),
        )?;
        let v1_after_job_id = Uuid::parse_str(
            v1_after_created["request_id"]
                .as_str()
                .ok_or_else(|| "legacy V1 replay omitted request_id".to_owned())?,
        )
        .map_err(debug_error)?;
        let v1_after_schema: String = sqlx::query_scalar(
            "SELECT command_schema FROM job_payloads WHERE job_id = $1",
        )
        .bind(v1_after_job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            v1_after_schema == GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
            format!("legacy V1 route changed after V2 activation: {v1_after_schema}"),
        )?;
        let body = video_request_v2();
        let (created_status, created) = json_request(
            app.clone(),
            Method::POST,
            "/v1/videos/generations",
            &owner.api_key.value,
            Some("video-e2e-idempotency"),
            Some(&body),
        )
        .await?;
        require(
            created_status == StatusCode::OK,
            format!("video creation failed: {created_status} {created}"),
        )?;
        let request_id = created["request_id"]
            .as_str()
            .ok_or_else(|| format!("video creation omitted request_id: {created}"))?;
        let job_id = Uuid::parse_str(request_id).map_err(debug_error)?;
        let (replay_status, replay) = json_request(
            app.clone(),
            Method::POST,
            "/v1/videos/generations",
            &owner.api_key.value,
            Some("video-e2e-idempotency"),
            Some(&body),
        )
        .await?;
        require(
            replay_status == StatusCode::OK && replay == created,
            format!("idempotent replay diverged: {replay_status} {replay}"),
        )?;

        let payload: String =
            sqlx::query_scalar("SELECT command_json::TEXT FROM job_payloads WHERE job_id = $1")
                .bind(job_id)
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        require(
            !payload.contains("data:image") && payload.contains("factory-staged-sha256:"),
            "durable video command retained raw input bytes or lost its staged binding",
        )?;
        assert_pending(&app, request_id, &owner.api_key.value).await?;

        let admission = PostgresAdmissionStore::new(database.pool.clone());
        let work = admission
            .claim_ready_for_profile(
                "video-e2e-workerd",
                60_000,
                AdmissionContract::CustomerPricingV4,
                GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2,
                v2_profile_id,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "video job was not ready for executor handoff".to_owned())?;
        require(
            work.job_id == job_id,
            "work claim returned a different video job",
        )?;

        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = executor
            .prepare_and_handoff(&work, v2_profile_id)
            .await
            .map_err(debug_error)?;
        require(
            prepared.len() == 1,
            "video job did not create exactly one output",
        )?;
        let lease = executor
            .claim_prepared(
                &ExecutorClaimScope {
                    execution_profile_id: v2_profile_id,
                    provider_id: PROVIDER_ID.to_owned(),
                    command_schema: GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2.to_owned(),
                    adapter_revision: VIDEO_ADAPTER_REVISION_V2.to_owned(),
                },
                "video-e2e-executor",
                60_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "video executor claim returned no submission".to_owned())?;
        executor.start(&lease).await.map_err(debug_error)?;
        let mp4 = minimal_mp4();
        let manifest = ExecutorArtifactPublisher::with_filesystem_store(
            blobs.clone(),
            PostgresExecutorSubmissionStore::new(database.pool.clone()),
        )
        .publish(&lease, &mp4)
        .await
        .map_err(debug_error)?;
        executor
            .record_outcome(&lease, &ExecutorSubmissionOutcome::Succeeded(manifest))
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("video-e2e-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "video terminal reduction was not queued".to_owned())?;
        let customer = CustomerArtifactPublisher::new(blobs.clone())
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        let completion = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        let replay_completion = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        require(
            completion == replay_completion,
            "terminal reduction replay changed its durable identity",
        )?;
        assert_v2_billing(&database.pool, job_id, &owner_project.id).await?;

        let (done_status, done) = json_request(
            app.clone(),
            Method::GET,
            &format!("/v1/videos/{request_id}"),
            &owner.api_key.value,
            None,
            None,
        )
        .await?;
        require(
            done_status == StatusCode::OK
                && done["status"] == "done"
                && done["progress"] == 100
                && done["video"]["duration"] == DURATION_SECONDS
                && done["video"]["respect_moderation"] == true,
            format!("completed video response was invalid: {done_status} {done}"),
        )?;
        let content_path = done["video"]["url"]
            .as_str()
            .ok_or_else(|| format!("completed video omitted URL: {done}"))?;
        let artifact_id = content_path
            .split('/')
            .nth_back(1)
            .ok_or_else(|| format!("completed video URL omitted artifact id: {content_path}"))
            .and_then(|value| Uuid::parse_str(value).map_err(debug_error))?;
        let (content_status, content_type, bytes) =
            raw_request(app.clone(), Method::GET, content_path, &owner.api_key.value).await?;
        require(
            content_status == StatusCode::OK
                && content_type.as_deref() == Some("video/mp4")
                && bytes == mp4,
            "video content endpoint changed the verified MP4 bytes",
        )?;
        assert_project_video_isolation(
            &database.pool,
            settlement.as_ref(),
            &owner_project.id,
            job_id,
            artifact_id,
        )
        .await?;

        for path in [format!("/v1/videos/{request_id}"), content_path.to_owned()] {
            let (status, _) = json_request(
                app.clone(),
                Method::GET,
                &path,
                &other.api_key.value,
                None,
                None,
            )
            .await?;
            require(
                status == StatusCode::NOT_FOUND,
                format!("cross-tenant read was not hidden for {path}: {status}"),
            )?;
        }

        let executor_object_key: String = sqlx::query_scalar(
            "SELECT object_key FROM executor_artifact_authorities WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let customer_path = artifact_root.path().join(&customer.object_key);
        let executor_path = artifact_root.path().join(&executor_object_key);
        require(
            customer_path.exists() && executor_path.exists(),
            "video retention fixture did not contain both artifact copies",
        )?;
        sqlx::query(
            r#"
            UPDATE job_artifact_retention
            SET state = 'expired',
                expired_at_ms =
                  (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
                purge_after_ms =
                  (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
                updated_at_ms =
                  (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
            WHERE job_id = $1
            "#,
        )
        .bind(job_id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        assert_video_content_expired(&app, content_path, &owner.api_key.value).await?;

        let outcome = reconcile_artifact_retention(
            &PostgresArtifactRetentionStore::new(database.pool.clone()),
            blobs.as_ref(),
            "video-e2e-retention",
            60_000,
            10,
        )
        .await
        .map_err(debug_error)?;
        require(
            outcome.claimed == 1 && outcome.deleted == 1 && outcome.failed == 0,
            format!("video retention did not complete: {outcome:?}"),
        )?;
        require(
            !customer_path.exists() && !executor_path.exists(),
            "video retention left a platform artifact copy on disk",
        )?;
        assert_video_content_expired(&app, content_path, &owner.api_key.value).await?;
        assert_v2_billing(&database.pool, job_id, &owner_project.id).await?;
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn xai_video_v2_migration_stays_disabled_then_claims_exactly() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let keys = Arc::new(PostgresApiKeyStore::new(
            database.pool.clone(),
            ApiKeyKeyring::new(1, [(1, vec![0x55; 32])]).map_err(debug_error)?,
        ));
        let project = keys
            .create_project("V2 video project")
            .await
            .map_err(debug_error)?;
        let owner = keys
            .create_service_account(
                &project.id,
                "V2 video owner",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        let provisioning = GrokExecutionProfileProvisioning {
            profile_key: "grok-video-v2-migration-v1".to_owned(),
            credential_pool_key: "grok-video-v2-migration-pool".to_owned(),
            provider_account_key: "grok-video-v2-migration-account".to_owned(),
            credential_ref: "private:grok-video-v2-migration".to_owned(),
            credential_revision: 1,
            credential_auth_sha256: "d".repeat(64),
            max_concurrency: 1,
        };
        let v1_profile = provision_grok_video_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        seed_video_route(
            &database.pool,
            &project.id,
            &owner.id,
            &owner.api_key.id,
            v1_profile.provider_account_id,
            v1_profile.execution_profile_id,
        )
        .await?;
        seed_video_economics(&database.pool, &project.id).await?;
        sqlx::raw_sql(include_str!(
            "../migrations/0132_grok_video_v2_bindings.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let v2_profile_id: Uuid = sqlx::query_scalar(
            "SELECT execution_profile_id FROM provider_execution_profiles
             WHERE provider_account_id = $1 AND command_schema = $2",
        )
        .bind(v1_profile.provider_account_id)
        .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let v2_route_id: Uuid = sqlx::query_scalar(
            "SELECT route_id FROM provider_routes WHERE command_schema = $1 LIMIT 1",
        )
        .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2)
        .fetch_optional(&database.pool)
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "V2 migration did not create an account route".to_owned())?;
        let disabled: (String, String, String, String) = sqlx::query_as(
            r#"
            SELECT profile.state, route.state, head.state, member.state
            FROM provider_execution_profiles profile
            JOIN provider_route_members member
              ON member.execution_profile_id = profile.execution_profile_id
            JOIN provider_routes route
              ON route.route_id = member.route_id AND route.revision = member.route_revision
            JOIN provider_route_heads head
              ON head.route_id = route.route_id AND head.current_revision = route.revision
            WHERE profile.execution_profile_id = $1
            "#,
        )
        .bind(v2_profile_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            disabled
                == (
                    "disabled".into(),
                    "disabled".into(),
                    "disabled".into(),
                    "disabled".into(),
                ),
            format!("migration did not leave V2 route disabled: {disabled:?}"),
        )?;

        let admission = PostgresAdmissionStore::new(database.pool.clone());
        require(
            admission
                .claim_ready_for_profile(
                    "v2-disabled-check",
                    60_000,
                    AdmissionContract::CustomerPricingV4,
                    GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2,
                    v2_profile_id,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "disabled V2 profile unexpectedly claimed work",
        )?;

        let artifact_root = TempDir::new().map_err(debug_error)?;
        let blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let settlement = Arc::new(PostgresExecutionSettlementStore::new(
            database.pool.clone(),
            blobs.clone(),
        ));
        let app = build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing(
            config(),
            ExternalImageGatewayComponents {
                usage_store: Arc::new(PostgresUsageStore::new(database.pool.clone())),
                api_key_store: keys,
                admission_store: Arc::new(PostgresAdmissionStore::new(database.pool.clone())),
                settlement_store: settlement,
                input_blob_store: blobs.clone(),
                provider_readiness_store: Arc::new(PostgresProviderTaskStore::new(
                    database.pool.clone(),
                )),
            },
            None,
            None,
            None,
            None,
            Some(Arc::new(PostgresModelRoutingStore::new(database.pool.clone()))),
        )
        .map_err(debug_error)?;
        let now: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "UPDATE gateway_api_key_provider_routes
             SET command_schema = $2, route_id = $3, route_revision = 1, bound_at_ms = $4
             WHERE api_key_id = $1 AND provider_id = 'grok-cli' AND operation_id = 'videos.generations'",
        )
        .bind(&owner.api_key.id)
        .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2)
        .bind(v2_route_id)
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "DELETE FROM gateway_platform_provider_routes
             WHERE provider_id = 'grok-cli' AND operation_id = 'videos.generations'",
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let disabled_counts_before: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM jobs), (SELECT COUNT(*) FROM quota_reservations),
                    (SELECT COUNT(*) FROM customer_price_quotes),
                    (SELECT COUNT(*) FROM job_provider_route_attributions)",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let (disabled_status, disabled_body) = json_request(
            app.clone(),
            Method::POST,
            "/v1/videos/generations",
            &owner.api_key.value,
            Some("v2-disabled-idempotency"),
            Some(&video_request_v2()),
        )
        .await?;
        let disabled_counts_after: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM jobs), (SELECT COUNT(*) FROM quota_reservations),
                    (SELECT COUNT(*) FROM customer_price_quotes),
                    (SELECT COUNT(*) FROM job_provider_route_attributions)",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let disabled_states: Vec<(String, String)> = sqlx::query_as(
            "SELECT 'job'::TEXT, state FROM jobs
             UNION ALL SELECT 'quota'::TEXT, state FROM quota_reservations",
        )
        .fetch_all(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            disabled_status == StatusCode::BAD_REQUEST
                && disabled_body["error"]["code"] == "invalid_value"
                && disabled_counts_before == disabled_counts_after
                && disabled_states.is_empty(),
            format!("disabled V2 route admitted active work: {disabled_status} {disabled_body} {disabled_counts_before:?}->{disabled_counts_after:?} states={disabled_states:?}"),
        )?;

        let v2_provisioning: (
            String,
            String,
            String,
            String,
            i64,
            String,
            i32,
        ) = sqlx::query_as(
            r#"
            SELECT profile.profile_key, pool.pool_key, account.account_key,
                   account.credential_ref, account.credential_revision,
                   account.credential_auth_sha256, policy.max_concurrency
            FROM provider_execution_profiles profile
            JOIN provider_credential_pools pool
              ON pool.credential_pool_id = profile.credential_pool_id
             AND pool.provider_id = profile.provider_id
            JOIN provider_accounts account
              ON account.provider_account_id = profile.provider_account_id
             AND account.credential_pool_id = profile.credential_pool_id
             AND account.provider_id = profile.provider_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = profile.resource_policy_id
             AND policy.revision = profile.resource_policy_revision
            WHERE profile.execution_profile_id = $1
            "#,
        )
        .bind(v2_profile_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let v2_provisioning = GrokExecutionProfileProvisioning {
            profile_key: v2_provisioning.0,
            credential_pool_key: v2_provisioning.1,
            provider_account_key: v2_provisioning.2,
            credential_ref: v2_provisioning.3,
            credential_revision: v2_provisioning.4,
            credential_auth_sha256: v2_provisioning.5,
            max_concurrency: v2_provisioning.6,
        };
        let activated = provision_grok_video_v2_execution_profile(
            &database.pool,
            &v2_provisioning,
        )
        .await
        .map_err(debug_error)?;
        require(
            activated.execution_profile_id == v2_profile_id,
            "exact V2 provisioning did not activate the migration profile",
        )?;
        let mut activation = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_routes
              (route_id, revision, route_key, display_name, provider_id, operation_id,
               command_schema, route_kind, selection_strategy, quota_freshness_ms,
               unknown_quota_policy, state, created_at_ms)
            SELECT route_id, 2, route_key, display_name, provider_id, operation_id,
                   command_schema, route_kind, selection_strategy, quota_freshness_ms,
                   unknown_quota_policy, 'enabled', $2
            FROM provider_routes
            WHERE route_id = $1 AND revision = 1
            "#,
        )
        .bind(v2_route_id)
        .bind(now)
        .execute(&mut *activation)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "UPDATE provider_route_heads SET current_revision = 2, state = 'enabled'
             WHERE route_id = $1",
        )
        .bind(v2_route_id)
        .execute(&mut *activation)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_route_members
              (route_id, route_revision, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, priority, weight, state,
               created_at_ms, minimum_remaining_percent)
            SELECT route_id, 2, provider_id, operation_id, command_schema,
                   provider_account_id, execution_profile_id, priority, weight,
                   'enabled', $2, minimum_remaining_percent
            FROM provider_route_members
            WHERE route_id = $1 AND route_revision = 1
            "#,
        )
        .bind(v2_route_id)
        .bind(now)
        .execute(&mut *activation)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_route_model_mappings
              (route_id, route_revision, provider_id, operation_id, command_schema,
               api_profile, public_model_id, provider_model_id, execution_model_id,
               media_kind, created_at_ms)
            SELECT route_id, 2, provider_id, operation_id, command_schema,
                   api_profile, public_model_id, provider_model_id, execution_model_id,
                   media_kind, $2
            FROM provider_route_model_mappings
            WHERE route_id = $1 AND route_revision = 1
            "#,
        )
        .bind(v2_route_id)
        .bind(now)
        .execute(&mut *activation)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "UPDATE gateway_api_key_provider_routes
             SET command_schema = $2, route_id = $3, route_revision = 2, bound_at_ms = $4
             WHERE api_key_id = $1 AND provider_id = 'grok-cli' AND operation_id = 'videos.generations'",
        )
        .bind(&owner.api_key.id)
        .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2)
        .bind(v2_route_id)
        .bind(now)
        .execute(&mut *activation)
        .await
        .map_err(debug_error)?;
        activation.commit().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO gateway_platform_provider_routes
              (provider_id, operation_id, command_schema, route_id,
               route_revision, state, created_at_ms, updated_at_ms)
            VALUES ('grok-cli', 'videos.generations', $1, $2, 2, 'enabled', $3, $3)
            "#,
        )
        .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2)
        .bind(v2_route_id)
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        seed_v2_active_economics(&database.pool, now).await?;
        sqlx::query(
            "DELETE FROM gateway_platform_provider_routes
             WHERE provider_id = 'grok-cli' AND operation_id = 'videos.generations'
               AND command_schema = $1",
        )
        .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        let body = video_request_v2();
        let (created_status, created) = json_request(
            app.clone(),
            Method::POST,
            "/v1/videos/generations",
            &owner.api_key.value,
            Some("v2-enabled-idempotency"),
            Some(&body),
        )
        .await?;
        require(
            created_status == StatusCode::OK,
            format!("V2 video creation failed: {created_status} {created}"),
        )?;
        let request_id = created["request_id"]
            .as_str()
            .ok_or_else(|| format!("V2 creation omitted request_id: {created}"))?;
        let job_id = Uuid::parse_str(request_id).map_err(debug_error)?;
        let (replay_status, replay) = json_request(
            app.clone(),
            Method::POST,
            "/v1/videos/generations",
            &owner.api_key.value,
            Some("v2-enabled-idempotency"),
            Some(&body),
        )
        .await?;
        require(
            replay_status == StatusCode::OK && replay == created,
            format!("V2 idempotent replay changed response: {replay_status} {replay}"),
        )?;
        let route_facts: (String, String, String, String, String, String) = sqlx::query_as(
            r#"
            SELECT payload.command_schema, attribution.command_schema,
                   quote.api_profile, quote.provider_model_id,
                   binding.contract_key, binding.contract_hash
            FROM job_payloads payload
            JOIN job_provider_route_attributions attribution ON attribution.job_id = payload.job_id
            JOIN customer_price_quotes quote ON quote.job_id = payload.job_id
            JOIN price_book_version_surface_contract_bindings binding
              ON binding.price_book_version_id = quote.price_book_version_id
            WHERE payload.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            route_facts
                == (
                    GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2.to_owned(),
                    GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2.to_owned(),
                    "xai-videos-v1".to_owned(),
                    "grok-imagine-video-1.5".to_owned(),
                    "grok-cli.videos.generations.v2.pricing-surface:95737ab45d7c904a".to_owned(),
                    "7d1f94c4cc807d9b82d4c199bd05181867b53c6268ae89b1665a1e7c2f42aa87".to_owned(),
                ),
            format!("V2 route/pricing resolver crossed identity: {route_facts:?}"),
        )?;
        let manifest: (String, i16) = sqlx::query_as(
            "SELECT manifest_schema, input_count FROM job_input_manifests WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            manifest == (XAI_VIDEO_INPUT_MANIFEST_SCHEMA_V2.to_owned(), 3),
            format!("V2 input manifest was not canonical: {manifest:?}"),
        )?;
        let executor = Arc::new(PostgresExecutorSubmissionStore::new(database.pool.clone()));
        let crossed_v1 = Workerd::new_handoff_only_with_contract(
            "v2-cross-v1-schema".to_owned(),
            Arc::new(PostgresAdmissionStore::new(database.pool.clone())),
            Arc::new(PostgresExecutionContextStore::new(database.pool.clone())),
            executor.clone(),
            v2_profile_id,
            GROK_VIDEO_GENERATION_COMMAND_SCHEMA.to_owned(),
            Duration::from_secs(5),
            AdmissionContract::CustomerPricingV4,
        )
        .map_err(debug_error)?;
        require(
            crossed_v1.run_once().await.map_err(debug_error)?.is_none(),
            "crossed V1 schema/V2 profile workerd claimed the V2 job",
        )?;
        let crossed_v2 = Workerd::new_handoff_only_with_contract(
            "v2-cross-v2-schema".to_owned(),
            Arc::new(PostgresAdmissionStore::new(database.pool.clone())),
            Arc::new(PostgresExecutionContextStore::new(database.pool.clone())),
            executor.clone(),
            v1_profile.execution_profile_id,
            GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2.to_owned(),
            Duration::from_secs(5),
            AdmissionContract::CustomerPricingV4,
        )
        .map_err(debug_error)?;
        require(
            crossed_v2.run_once().await.map_err(debug_error)?.is_none(),
            "crossed V2 schema/V1 profile workerd claimed the V2 job",
        )?;
        let pending: (String, i64, i64, i64) = sqlx::query_as(
            r#"SELECT work.state,
                      (SELECT COUNT(*) FROM provider_submissions WHERE job_id = work.job_id),
                      (SELECT COUNT(*) FROM executor_executions execution
                       JOIN provider_submissions submission
                         ON submission.submission_id = execution.submission_id
                       WHERE submission.job_id = work.job_id),
                      (SELECT COUNT(*) FROM job_attempts attempt
                       WHERE attempt.work_item_id = work.work_item_id)
               FROM work_items work WHERE work.job_id = $1"#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            pending == ("ready".to_owned(), 0, 0, 0),
            format!("crossed profile claim changed V2 work: {pending:?}"),
        )?;
        let workerd = Workerd::new_handoff_only_with_contract(
            "v2-workerd".to_owned(),
            Arc::new(PostgresAdmissionStore::new(database.pool.clone())),
            Arc::new(PostgresExecutionContextStore::new(database.pool.clone())),
            executor.clone(),
            v2_profile_id,
            GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2.to_owned(),
            Duration::from_secs(5),
            AdmissionContract::CustomerPricingV4,
        )
        .map_err(debug_error)?;
        require(
            workerd.run_once().await.map_err(debug_error)? == Some(job_id),
            "V2 workerd handed off the wrong job",
        )?;
        let handed_off: (String, i64) = sqlx::query_as(
            r#"SELECT work.state,
                      (SELECT COUNT(*) FROM provider_submissions WHERE job_id = work.job_id)
               FROM work_items work WHERE work.job_id = $1"#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            handed_off.1 == 1
                && matches!(handed_off.0.as_str(), "awaiting_executor" | "handed_off"),
            format!("V2 workerd handoff state was not durable: {handed_off:?}"),
        )?;
        let lease = executor
            .claim_prepared(
                &ExecutorClaimScope {
                    execution_profile_id: v2_profile_id,
                    provider_id: PROVIDER_ID.to_owned(),
                    command_schema: GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2.to_owned(),
                    adapter_revision: VIDEO_ADAPTER_REVISION_V2.to_owned(),
                },
                "v2-executor",
                60_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "V2 executor claim returned no submission".to_owned())?;
        executor.start(&lease).await.map_err(debug_error)?;
        let mp4 = minimal_mp4();
        let manifest = ExecutorArtifactPublisher::with_filesystem_store(
            blobs.clone(),
            PostgresExecutorSubmissionStore::new(database.pool.clone()),
        )
        .publish(&lease, &mp4)
        .await
        .map_err(debug_error)?;
        executor
            .record_outcome(&lease, &ExecutorSubmissionOutcome::Succeeded(manifest))
            .await
            .map_err(debug_error)?;
        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("v2-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "V2 terminal reduction was not queued".to_owned())?;
        let customer = CustomerArtifactPublisher::new(blobs)
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        let first_completion = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        let replay_completion = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        require(
            first_completion == replay_completion,
            "V2 terminal replay changed the reduction result",
        )?;
        let replay_counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT
               (SELECT COUNT(*) FROM provider_submissions WHERE job_id = $1),
               (SELECT COUNT(*) FROM executor_executions execution
                JOIN provider_submissions submission
                  ON submission.submission_id = execution.submission_id
                WHERE submission.job_id = $1),
               (SELECT COUNT(*) FROM executor_terminal_reductions reduction
                JOIN provider_submissions submission
                  ON submission.submission_id = reduction.submission_id
                WHERE submission.job_id = $1)",
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            replay_counts == (1, 1, 1),
            format!("V2 replay duplicated durable terminal rows: {replay_counts:?}"),
        )?;
        let (done_status, done) = json_request(
            app,
            Method::GET,
            &format!("/v1/videos/{request_id}"),
            &owner.api_key.value,
            None,
            None,
        )
        .await?;
        require(
            done_status == StatusCode::OK
                && done["status"] == "done"
                && done["progress"] == 100
                && done["video"]["duration"] == DURATION_SECONDS,
            format!("V2 completed response shape changed: {done_status} {done}"),
        )?;
        assert_v2_billing(&database.pool, job_id, &project.id).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn xai_video_v2_wrong_descriptor_revision_is_not_reconciled() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = GrokExecutionProfileProvisioning {
            profile_key: "grok-video-v2-revision-v1".to_owned(),
            credential_pool_key: "grok-video-v2-revision-pool".to_owned(),
            provider_account_key: "grok-video-v2-revision-account".to_owned(),
            credential_ref: "private:grok-video-v2-revision".to_owned(),
            credential_revision: 1,
            credential_auth_sha256: "e".repeat(64),
            max_concurrency: 1,
        };
        let v1 = provision_grok_video_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        let project = PostgresApiKeyStore::new(
            database.pool.clone(),
            ApiKeyKeyring::new(1, [(1, vec![0x56; 32])]).map_err(debug_error)?,
        )
        .create_project("V2 revision project")
        .await
        .map_err(debug_error)?;
        let account = PostgresApiKeyStore::new(
            database.pool.clone(),
            ApiKeyKeyring::new(1, [(1, vec![0x57; 32])]).map_err(debug_error)?,
        );
        let owner = account
            .create_service_account(
                &project.id,
                "V2 revision owner",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        seed_video_route(
            &database.pool,
            &project.id,
            &owner.id,
            &owner.api_key.id,
            v1.provider_account_id,
            v1.execution_profile_id,
        )
        .await?;
        sqlx::raw_sql(include_str!(
            "../migrations/0132_grok_video_v2_bindings.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let now: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let (v2_profile, v2_route): (Uuid, Uuid) = sqlx::query_as(
            "SELECT profile.execution_profile_id, route.route_id
             FROM provider_execution_profiles profile
             JOIN provider_route_members member ON member.execution_profile_id = profile.execution_profile_id
             JOIN provider_routes route ON route.route_id = member.route_id AND route.revision = member.route_revision
             WHERE profile.provider_account_id = $1 AND profile.command_schema = $2
             LIMIT 1",
        )
        .bind(v1.provider_account_id)
        .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let exact_hash = GROK_VIDEO_GENERATION_OPERATION_V2.canonical_sha256_v1_hex();
        let exact_descriptor = GROK_VIDEO_GENERATION_OPERATION_V2.descriptor_revision;
        let exact_adapter = VIDEO_ADAPTER_REVISION_V2;
        let cases = [
            (
                "descriptor revision",
                "grok-cli/videos.generations/v1".to_owned(),
                exact_hash.clone(),
                exact_adapter.to_owned(),
            ),
            (
                "descriptor hash",
                exact_descriptor.to_owned(),
                "0".repeat(64),
                exact_adapter.to_owned(),
            ),
            (
                "adapter revision",
                exact_descriptor.to_owned(),
                exact_hash,
                "grok-cli-1.0.34.agentic-video.v0".to_owned(),
            ),
        ];
        for (index, (label, descriptor, hash, adapter)) in cases.into_iter().enumerate() {
            let (wrong_route, wrong_profile) = insert_v2_reconciliation_negative(
                &database.pool,
                v2_profile,
                v2_route,
                now,
                (index + 2) as i64,
                &descriptor,
                &hash,
                &adapter,
            )
            .await?;
            let report = reconcile_execution_profile_routes(&database.pool)
                .await
                .map_err(debug_error)?;
            let selected: (i64, String, Uuid) = sqlx::query_as(
                "SELECT head.current_revision, head.state, member.execution_profile_id
                 FROM provider_route_heads head
                 JOIN provider_route_members member
                   ON member.route_id = head.route_id
                  AND member.route_revision = head.current_revision
                  AND member.state = 'enabled'
                 WHERE head.route_id = $1",
            )
            .bind(wrong_route)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
            require(
                selected == (1, "enabled".to_owned(), wrong_profile)
                    && report.unresolved_routes == 1,
                format!("wrong {label} was reconciled: {report:?} profile={selected:?}"),
            )?;
            sqlx::query(
                "UPDATE provider_route_heads SET state = 'disabled', updated_at_ms = $2
                 WHERE route_id = $1",
            )
            .bind(wrong_route)
            .bind(now)
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        }
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

async fn insert_v2_reconciliation_negative(
    pool: &PgPool,
    exact_profile: Uuid,
    exact_route: Uuid,
    now: i64,
    policy_revision: i64,
    descriptor_revision: &str,
    descriptor_hash: &str,
    adapter_revision: &str,
) -> TestResult<(Uuid, Uuid)> {
    let policy_id: Uuid = sqlx::query_scalar(
        "SELECT resource_policy_id FROM provider_execution_profiles
         WHERE execution_profile_id = $1",
    )
    .bind(exact_profile)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO executor_resource_policies
         (resource_policy_id, revision, credential_pool_id, provider_account_id, provider_id,
          execution_class, max_concurrency, allocated_count, state, created_at_ms)
         SELECT resource_policy_id, $2, credential_pool_id, provider_account_id, provider_id,
                execution_class, max_concurrency, 0, 'disabled', $3
         FROM executor_resource_policies WHERE resource_policy_id = $1 AND revision = 1",
    )
    .bind(policy_id)
    .bind(policy_revision)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let wrong_profile = Uuid::new_v4();
    let wrong_key = format!("grok.v2.wrong-binding.{}", wrong_profile.simple());
    sqlx::query(
        "INSERT INTO provider_execution_profiles
         (execution_profile_id, profile_key, provider_id, command_schema, adapter_revision,
          credential_pool_id, provider_account_id, credential_ref, credential_revision,
          resource_policy_id, resource_policy_revision, state, created_at_ms, updated_at_ms,
          operation_id, operation_descriptor_revision, operation_descriptor_sha256_v1,
          completion_mode, idempotency_mode)
         SELECT $1, $2, provider_id, command_schema, $3,
                credential_pool_id, provider_account_id, credential_ref, credential_revision,
                resource_policy_id, $4, 'enabled', $5, $5,
                operation_id, $6, $7, completion_mode, idempotency_mode
         FROM provider_execution_profiles WHERE execution_profile_id = $8",
    )
    .bind(wrong_profile)
    .bind(&wrong_key)
    .bind(adapter_revision)
    .bind(policy_revision)
    .bind(now)
    .bind(descriptor_revision)
    .bind(descriptor_hash)
    .bind(exact_profile)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let wrong_route = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO provider_routes
         (route_id, revision, route_key, display_name, provider_id, operation_id,
          command_schema, route_kind, selection_strategy, state, created_at_ms,
          quota_freshness_ms, unknown_quota_policy)
         SELECT $1, 1, $2, display_name, provider_id, operation_id, command_schema,
                route_kind, selection_strategy, 'enabled', $3, quota_freshness_ms,
                unknown_quota_policy
         FROM provider_routes WHERE route_id = $4 AND revision = 1",
    )
    .bind(wrong_route)
    .bind(format!("wrong-binding-{wrong_route}"))
    .bind(now)
    .bind(exact_route)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_route_heads
         (route_id, route_key, provider_id, operation_id, command_schema, route_kind,
          current_revision, state, created_at_ms, updated_at_ms)
         SELECT $1, route_key, provider_id, operation_id, command_schema, route_kind,
                1, 'enabled', $2, $2
         FROM provider_routes WHERE route_id = $1 AND revision = 1",
    )
    .bind(wrong_route)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_route_members
         (route_id, route_revision, provider_id, operation_id, command_schema,
          provider_account_id, execution_profile_id, priority, weight, state,
          created_at_ms, minimum_remaining_percent)
         SELECT $1, 1, provider_id, operation_id, command_schema, provider_account_id,
                $2, priority, weight, 'enabled', $3, minimum_remaining_percent
         FROM provider_route_members WHERE route_id = $4 AND route_revision = 1",
    )
    .bind(wrong_route)
    .bind(wrong_profile)
    .bind(now)
    .bind(exact_route)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok((wrong_route, wrong_profile))
}

async fn assert_project_video_isolation(
    pool: &PgPool,
    settlement: &PostgresExecutionSettlementStore,
    tenant_id: &str,
    job_id: Uuid,
    artifact_id: Uuid,
) -> TestResult {
    let sibling_project_id = format!("proj_{}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO gateway_projects (id, tenant_id, name, created_at)
        VALUES ($1, $2, 'Sibling project', 1)
        "#,
    )
    .bind(&sibling_project_id)
    .bind(tenant_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    require(
        settlement
            .project_video_status(tenant_id, tenant_id, None, job_id)
            .await
            .map_err(debug_error)?
            .is_some(),
        "owning project could not read its video task",
    )?;
    require(
        settlement
            .project_video_status(tenant_id, &sibling_project_id, None, job_id)
            .await
            .map_err(debug_error)?
            .is_none(),
        "sibling project could read the video task",
    )?;
    require(
        settlement
            .load_project_video_artifact(tenant_id, tenant_id, None, artifact_id)
            .await
            .map_err(debug_error)?
            .is_some(),
        "owning project could not read its video artifact",
    )?;
    require(
        settlement
            .load_project_video_artifact(tenant_id, &sibling_project_id, None, artifact_id)
            .await
            .map_err(debug_error)?
            .is_none(),
        "sibling project could read the video artifact",
    )?;
    let unrelated_user = Uuid::new_v4();
    require(
        settlement
            .project_video_status(tenant_id, tenant_id, Some(unrelated_user), job_id)
            .await
            .map_err(debug_error)?
            .is_none(),
        "unrelated project member could read the video task",
    )?;
    require(
        settlement
            .load_project_video_artifact(tenant_id, tenant_id, Some(unrelated_user), artifact_id)
            .await
            .map_err(debug_error)?
            .is_none(),
        "unrelated project member could read the video artifact",
    )
}

fn config() -> AppConfig {
    AppConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        admin_token: Some("video-e2e-admin".to_owned()),
        legacy_admin_auth_enabled: true,
        database_url: None,
        generation_admission_contract: GenerationAdmissionContract::CustomerPricingV4,
        enable_xai_video_api: true,
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
        readiness_stall_threshold: Duration::from_secs(60),
        max_upload_bytes: 1024 * 1024,
        proxy: ProxyConfig::default(),
        codex_home: None,
        cleanup_codex_outputs: false,
    }
}

fn video_request() -> Value {
    let image = ImageBuffer::from_pixel(1, 1, Rgba([1_u8, 2, 3, 255]));
    let mut encoded = Cursor::new(Vec::new());
    image
        .write_to(&mut encoded, ImageFormat::Png)
        .expect("encode video input PNG");
    json!({
        "model": "grok-imagine-video-1.5",
        "duration": DURATION_SECONDS,
        "resolution": "480p",
        "prompt": "slow camera push",
        "image": {
            "url": format!("data:image/png;base64,{}", STANDARD.encode(encoded.into_inner()))
        }
    })
}

fn video_request_v2() -> Value {
    let image = video_request()["image"]["url"].clone();
    json!({
        "model": "grok-imagine-video-1.5",
        "duration": DURATION_SECONDS,
        "resolution": "480p",
        "aspect_ratio": "16:9",
        "generate_audio": true,
        "image": {"url": image.clone()},
        "last_frame": {"url": image.clone()},
        "reference_images": [{"url": image}]
    })
}

async fn seed_video_economics(pool: &PgPool, tenant_id: &str) -> TestResult {
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "customer.grok-video-e2e.usd".to_owned(),
            display_name: "Grok video E2E customer price".to_owned(),
            purpose: "customer_sale".to_owned(),
            scope_type: "platform".to_owned(),
            organization_id: None,
            project_id: None,
            provider_id: Some("grok-cli".to_owned()),
            currency: "USD".to_owned(),
        })
        .await
        .map_err(debug_error)?;
    let mut components = Vec::new();
    for outcome in ["succeeded", "failed", "no_effect"] {
        components.push(PriceComponentDraft {
            component_key: format!("image-input-{outcome}"),
            metric: "image_input".to_owned(),
            unit: "image".to_owned(),
            unit_size: "1".to_owned(),
            unit_price_micros: if outcome == "succeeded" {
                IMAGE_INPUT_PRICE_MICROS.to_string()
            } else {
                "0".to_owned()
            },
            outcome: outcome.to_owned(),
            quantity_source: "request_derived".to_owned(),
            required_confidence: "exact".to_owned(),
            rounding_mode: "exact".to_owned(),
            dimensions: json!({}),
        });
        components.push(PriceComponentDraft {
            component_key: format!("video-second-{outcome}"),
            metric: "video_requested_second".to_owned(),
            unit: "second".to_owned(),
            unit_size: "1".to_owned(),
            unit_price_micros: if outcome == "succeeded" {
                UNIT_PRICE_MICROS.to_string()
            } else {
                "0".to_owned()
            },
            outcome: outcome.to_owned(),
            quantity_source: "request_derived".to_owned(),
            required_confidence: "exact".to_owned(),
            rounding_mode: "exact".to_owned(),
            dimensions: json!({}),
        });
    }
    let version = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: PriceBookVersionDraft {
                    api_profile: "xai-videos-v1".to_owned(),
                    operation: "video_generation".to_owned(),
                    provider_id: Some("grok-cli".to_owned()),
                    provider_model_id: Some("grok-imagine-video-1.5-preview".to_owned()),
                    public_model_id: "grok-imagine-video-1.5".to_owned(),
                    media_kind: "video".to_owned(),
                    service_tier: "standard".to_owned(),
                    execution_surface: "provider_cli".to_owned(),
                    billing_mode: "customer_rate".to_owned(),
                    is_free: false,
                    effective_from_ms: now - 1,
                    source_kind: "official_document".to_owned(),
                    source_url: Some("https://docs.x.ai/developers/pricing".to_owned()),
                    source_checked_at_ms: Some(now),
                    notes: Some("PostgreSQL V4 video E2E fixture".to_owned()),
                    components,
                },
            },
        )
        .await
        .map_err(debug_error)?;
    service
        .publish_version(
            version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts
          (tenant_id, currency, credit_limit_micros, held_micros, captured_micros,
           created_at_ms, updated_at_ms)
        VALUES ($1, 'USD', 1000000, 0, 0, $2, $2)
        "#,
    )
    .bind(tenant_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn seed_v2_active_economics(pool: &PgPool, now: i64) -> TestResult {
    let service = PostgresPricingAdminService::new(pool.clone());
    let book_id: Uuid = sqlx::query_scalar(
        "SELECT price_book_id FROM price_books WHERE price_book_key = 'customer.grok-video-e2e.usd'",
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let mut components = Vec::new();
    for outcome in ["succeeded", "failed", "no_effect"] {
        components.push(PriceComponentDraft {
            component_key: format!("image-input-{outcome}"),
            metric: "image_input".to_owned(),
            unit: "image".to_owned(),
            unit_size: "1".to_owned(),
            unit_price_micros: if outcome == "succeeded" { "10000" } else { "0" }.to_owned(),
            outcome: outcome.to_owned(),
            quantity_source: "request_derived".to_owned(),
            required_confidence: "exact".to_owned(),
            rounding_mode: "exact".to_owned(),
            dimensions: json!({}),
        });
        components.push(PriceComponentDraft {
            component_key: format!("video-second-{outcome}"),
            metric: "video_requested_second".to_owned(),
            unit: "second".to_owned(),
            unit_size: "1".to_owned(),
            unit_price_micros: if outcome == "succeeded" { "80000" } else { "0" }.to_owned(),
            outcome: outcome.to_owned(),
            quantity_source: "request_derived".to_owned(),
            required_confidence: "exact".to_owned(),
            rounding_mode: "exact".to_owned(),
            dimensions: json!({}),
        });
    }
    components.push(PriceComponentDraft {
        component_key: "video-second-succeeded-720p".to_owned(),
        metric: "video_requested_second".to_owned(),
        unit: "second".to_owned(),
        unit_size: "1".to_owned(),
        unit_price_micros: "140000".to_owned(),
        outcome: "succeeded".to_owned(),
        quantity_source: "request_derived".to_owned(),
        required_confidence: "exact".to_owned(),
        rounding_mode: "exact".to_owned(),
        dimensions: json!({"resolution": "720p"}),
    });
    let version = service
        .create_version(
            book_id,
            CreatePriceBookVersionRequest {
                draft: PriceBookVersionDraft {
                    api_profile: "xai-videos-v1".to_owned(),
                    operation: "video_generation".to_owned(),
                    provider_id: Some("grok-cli".to_owned()),
                    provider_model_id: Some("grok-imagine-video-1.5".to_owned()),
                    public_model_id: "grok-imagine-video-1.5".to_owned(),
                    media_kind: "video".to_owned(),
                    service_tier: "standard".to_owned(),
                    execution_surface: "provider_cli".to_owned(),
                    billing_mode: "customer_rate".to_owned(),
                    is_free: false,
                    effective_from_ms: now - 1,
                    source_kind: "official_document".to_owned(),
                    source_url: Some("https://docs.x.ai/developers/pricing".to_owned()),
                    source_checked_at_ms: Some(now),
                    notes: Some("PostgreSQL V2 video E2E fixture".to_owned()),
                    components,
                },
            },
        )
        .await
        .map_err(debug_error)?;
    service
        .publish_version(
            version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(debug_error)?;
    Ok(())
}

async fn activate_v2_fixture(
    pool: &PgPool,
    api_key_id: &str,
    provider_account_id: Uuid,
) -> TestResult<Uuid> {
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    sqlx::raw_sql(include_str!(
        "../migrations/0132_grok_video_v2_bindings.sql"
    ))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let v2_profile_id: Uuid = sqlx::query_scalar(
        "SELECT execution_profile_id FROM provider_execution_profiles
         WHERE provider_account_id = $1 AND command_schema = $2",
    )
    .bind(provider_account_id)
    .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let v2_route_id: Uuid = sqlx::query_scalar(
        "SELECT route_id FROM provider_routes WHERE command_schema = $1 LIMIT 1",
    )
    .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let fields: (String, String, String, String, i64, String, i32) = sqlx::query_as(
        "SELECT profile.profile_key, pool.pool_key, account.account_key, account.credential_ref,
                account.credential_revision, account.credential_auth_sha256, policy.max_concurrency
         FROM provider_execution_profiles profile
         JOIN provider_credential_pools pool ON pool.credential_pool_id = profile.credential_pool_id
         JOIN provider_accounts account ON account.provider_account_id = profile.provider_account_id
         JOIN executor_resource_policies policy ON policy.resource_policy_id = profile.resource_policy_id
          AND policy.revision = profile.resource_policy_revision
         WHERE profile.execution_profile_id = $1",
    )
    .bind(v2_profile_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    provision_grok_video_v2_execution_profile(
        pool,
        &GrokExecutionProfileProvisioning {
            profile_key: fields.0,
            credential_pool_key: fields.1,
            provider_account_key: fields.2,
            credential_ref: fields.3,
            credential_revision: fields.4,
            credential_auth_sha256: fields.5,
            max_concurrency: fields.6,
        },
    )
    .await
    .map_err(debug_error)?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_routes
         (route_id, revision, route_key, display_name, provider_id, operation_id, command_schema,
          route_kind, selection_strategy, quota_freshness_ms, unknown_quota_policy, state, created_at_ms)
         SELECT route_id, 2, route_key, display_name, provider_id, operation_id, command_schema,
                route_kind, selection_strategy, quota_freshness_ms, unknown_quota_policy, 'enabled', $2
         FROM provider_routes WHERE route_id = $1 AND revision = 1",
    )
    .bind(v2_route_id).bind(now).execute(&mut *tx).await.map_err(debug_error)?;
    sqlx::query("UPDATE provider_route_heads SET current_revision = 2, state = 'enabled' WHERE route_id = $1")
        .bind(v2_route_id).execute(&mut *tx).await.map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_route_members
         (route_id, route_revision, provider_id, operation_id, command_schema, provider_account_id,
          execution_profile_id, priority, weight, state, created_at_ms, minimum_remaining_percent)
         SELECT route_id, 2, provider_id, operation_id, command_schema, provider_account_id,
                execution_profile_id, priority, weight, 'enabled', $2, minimum_remaining_percent
         FROM provider_route_members WHERE route_id = $1 AND route_revision = 1",
    )
    .bind(v2_route_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_route_model_mappings
         (route_id, route_revision, provider_id, operation_id, command_schema, api_profile,
          public_model_id, provider_model_id, execution_model_id, media_kind, created_at_ms)
         SELECT route_id, 2, provider_id, operation_id, command_schema, api_profile, public_model_id,
                provider_model_id, execution_model_id, media_kind, $2
         FROM provider_route_model_mappings WHERE route_id = $1 AND route_revision = 1",
    )
    .bind(v2_route_id).bind(now).execute(&mut *tx).await.map_err(debug_error)?;
    sqlx::query(
        "UPDATE gateway_api_key_provider_routes
         SET command_schema = $2, route_id = $3, route_revision = 2, bound_at_ms = $4
         WHERE api_key_id = $1 AND provider_id = 'grok-cli' AND operation_id = 'videos.generations'",
    )
    .bind(api_key_id).bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2).bind(v2_route_id).bind(now)
    .execute(&mut *tx).await.map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)?;
    sqlx::query(
        "DELETE FROM gateway_platform_provider_routes
         WHERE provider_id = 'grok-cli' AND operation_id = 'videos.generations'",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO gateway_platform_provider_routes
         (provider_id, operation_id, command_schema, route_id, route_revision, state, created_at_ms, updated_at_ms)
         VALUES ('grok-cli', 'videos.generations', $1, $2, 2, 'enabled', $3, $3)",
    )
    .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2).bind(v2_route_id).bind(now)
    .execute(pool).await.map_err(debug_error)?;
    seed_v2_active_economics(pool, now).await?;
    sqlx::query("DELETE FROM gateway_platform_provider_routes WHERE provider_id = 'grok-cli' AND operation_id = 'videos.generations' AND command_schema = $1")
        .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2).execute(pool).await.map_err(debug_error)?;
    Ok(v2_profile_id)
}

async fn seed_video_route(
    pool: &PgPool,
    project_id: &str,
    service_account_id: &str,
    api_key_id: &str,
    provider_account_id: Uuid,
    execution_profile_id: Uuid,
) -> TestResult {
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let route_id = Uuid::new_v4();
    let route_key = format!("grok-video-e2e-{}", route_id.simple());
    sqlx::query(
        r#"
        INSERT INTO provider_account_environments
          (provider_account_id, provider_id, environment_kind, environment_ref,
           upstream_identity_sha256, display_name, state, created_at_ms, updated_at_ms)
        VALUES ($1, 'grok-cli', 'grok_home_v1', $2, $3,
                'Grok video E2E', 'active', $4, $4)
        "#,
    )
    .bind(provider_account_id)
    .bind(format!(
        "/tmp/grok-video-e2e-{}",
        provider_account_id.simple()
    ))
    .bind("c".repeat(64))
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_routes
          (route_id, revision, route_key, display_name, provider_id, operation_id,
           command_schema, route_kind, selection_strategy, state, created_at_ms)
        VALUES ($1, 1, $2, 'Grok video E2E', 'grok-cli', 'videos.generations',
                $3, 'account', 'quota_aware_least_loaded', 'enabled', $4)
        "#,
    )
    .bind(route_id)
    .bind(&route_key)
    .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_heads
          (route_id, route_key, provider_id, operation_id, command_schema, route_kind,
           current_revision, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'grok-cli', 'videos.generations', $3, 'account',
                1, 'enabled', $4, $4)
        "#,
    )
    .bind(route_id)
    .bind(&route_key)
    .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_members
          (route_id, route_revision, provider_id, operation_id, command_schema,
           provider_account_id, execution_profile_id, state, created_at_ms)
        VALUES ($1, 1, 'grok-cli', 'videos.generations', $2, $3, $4, 'enabled', $5)
        "#,
    )
    .bind(route_id)
    .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA)
    .bind(provider_account_id)
    .bind(execution_profile_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_models
          (provider_id, model_id, execution_model_id, media_kind, display_name,
           adapter_state, lifecycle_state, operation_ids, source_kind,
           first_seen_at_ms, last_seen_at_ms, metadata_json)
        VALUES ('grok-cli', 'grok-imagine-video-1.5-preview',
                'grok-imagine-video-1.5-preview', 'video', 'Grok Imagine Video 1.5',
                'supported', 'enabled', ARRAY['videos.generations'],
                'adapter_contract', $1, $1, '{}'::JSONB)
        "#,
    )
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings
          (route_id, route_revision, provider_id, operation_id, command_schema,
           api_profile, public_model_id, provider_model_id, execution_model_id,
           media_kind, created_at_ms)
        VALUES ($1, 1, 'grok-cli', 'videos.generations', $2, 'xai-videos-v1',
                'grok-imagine-video-1.5', 'grok-imagine-video-1.5-preview',
                'grok-imagine-video-1.5-preview', 'video', $3)
        "#,
    )
    .bind(route_id)
    .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO gateway_api_key_provider_routes
          (api_key_id, service_account_id, project_id, tenant_id, provider_id,
           operation_id, command_schema, route_id, route_revision, bound_at_ms)
        VALUES ($1, $2, $3, $3, 'grok-cli', 'videos.generations', $4, $5, 1, $6)
        "#,
    )
    .bind(api_key_id)
    .bind(service_account_id)
    .bind(project_id)
    .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA)
    .bind(route_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO gateway_platform_provider_routes (
            provider_id, operation_id, command_schema,
            route_id, route_revision, state, created_at_ms, updated_at_ms
        )
        VALUES (
            'grok-cli', 'videos.generations', $1,
            $2, 1, 'enabled', $3, $3
        )
        "#,
    )
    .bind(GROK_VIDEO_GENERATION_COMMAND_SCHEMA)
    .bind(route_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn assert_pending(app: &axum::Router, request_id: &str, token: &str) -> TestResult {
    let (status, body) = json_request(
        app.clone(),
        Method::GET,
        &format!("/v1/videos/{request_id}"),
        token,
        None,
        None,
    )
    .await?;
    require(
        status == StatusCode::OK && body["status"] == "pending" && body["video"].is_null(),
        format!("new video was not pending: {status} {body}"),
    )
}

async fn assert_v2_billing(pool: &PgPool, job_id: Uuid, tenant_id: &str) -> TestResult {
    let job: (i32, i32, String, i32, String, i16) = sqlx::query_as(
        "SELECT output_count, billable_units, billing_metric, charged_units, state,
                economics_contract_version
         FROM jobs WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        job == (
            1,
            DURATION_SECONDS,
            "video_second".to_owned(),
            DURATION_SECONDS,
            "succeeded".to_owned(),
            4,
        ),
        format!("V2 video job economics diverged: {job:?}"),
    )?;
    let quota: (i32, i32, String) = sqlx::query_as(
        "SELECT committed_units, released_units, state FROM quota_reservations WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        quota == (DURATION_SECONDS, 0, "committed".to_owned()),
        format!("V2 video quota did not commit once: {quota:?}"),
    )?;
    let quota_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM quota_reservations WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        quota_count == 1,
        format!("V2 replay created duplicate quota reservations: {quota_count}"),
    )?;
    let rating: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT quote_line.metric, rating_line.actual_quantity, rating_line.amount_micros
        FROM customer_rated_usage_lines rating_line
        JOIN customer_price_quote_lines quote_line
          ON quote_line.quote_line_id = rating_line.quote_line_id
         AND quote_line.quote_id = rating_line.quote_id
         AND quote_line.job_id = rating_line.job_id
        WHERE rating_line.job_id = $1
        ORDER BY quote_line.metric
        "#,
    )
    .bind(job_id)
    .fetch_all(pool)
    .await
    .map_err(debug_error)?;
    require(
        rating
            == vec![
                ("image_input".to_owned(), 3, 30_000),
                ("video_requested_second".to_owned(), 6, 480_000),
            ],
        format!("V2 rating did not use canonical pricing: {rating:?}"),
    )?;
    let account: (i64, i64) = sqlx::query_as(
        "SELECT held_micros, captured_micros FROM billing_accounts WHERE tenant_id = $1 AND currency = 'USD'",
    )
    .bind(tenant_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        account == (0, 510_000),
        format!("V2 account capture was not exact: {account:?}"),
    )?;
    let ledger_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM ledger_transactions
         WHERE source_job_id = $1 AND transaction_type = 'customer_job_charge'",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        ledger_count == 1,
        "V2 settlement was not posted exactly once",
    )
}

async fn json_request(
    app: axum::Router,
    method: Method,
    uri: &str,
    token: &str,
    idempotency_key: Option<&str>,
    body: Option<&Value>,
) -> TestResult<(StatusCode, Value)> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(key) = idempotency_key {
        builder = builder.header("idempotency-key", key);
    }
    let body = match body {
        Some(body) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(body.to_string())
        }
        None => Body::empty(),
    };
    let response = app
        .oneshot(builder.body(body).map_err(debug_error)?)
        .await
        .map_err(debug_error)?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(debug_error)?;
    let body = serde_json::from_slice(&bytes).map_err(debug_error)?;
    Ok((status, body))
}

async fn raw_request(
    app: axum::Router,
    method: Method,
    uri: &str,
    token: &str,
) -> TestResult<(StatusCode, Option<String>, Vec<u8>)> {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .map_err(debug_error)?,
        )
        .await
        .map_err(debug_error)?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(debug_error)?
        .to_vec();
    Ok((status, content_type, bytes))
}

async fn assert_video_content_expired(
    app: &axum::Router,
    content_path: &str,
    token: &str,
) -> TestResult {
    let (status, _, bytes) = raw_request(app.clone(), Method::GET, content_path, token).await?;
    let body: Value = serde_json::from_slice(&bytes).map_err(debug_error)?;
    require(
        status == StatusCode::GONE && body["error"]["code"] == "artifact_expired",
        format!("expired video content returned {status}: {body}"),
    )
}

fn minimal_mp4() -> Vec<u8> {
    let config = mp4::Mp4Config {
        major_brand: "isom".parse().unwrap(),
        minor_version: 512,
        compatible_brands: vec!["isom".parse().unwrap(), "avc1".parse().unwrap()],
        timescale: 1_000,
    };
    let mut writer =
        mp4::Mp4Writer::write_start(std::io::Cursor::new(Vec::new()), &config).unwrap();
    writer
        .add_track(
            &mp4::AvcConfig {
                width: 16,
                height: 16,
                seq_param_set: vec![0x67, 0x42, 0x00, 0x1e],
                pic_param_set: vec![0x68, 0xce, 0x3c, 0x80],
            }
            .into(),
        )
        .unwrap();
    writer
        .write_sample(
            1,
            &mp4::Mp4Sample {
                start_time: 0,
                duration: 8_000,
                rendering_offset: 0,
                is_sync: true,
                bytes: vec![0, 0, 0, 1, 0x65].into(),
            },
        )
        .unwrap();
    writer.write_end().unwrap();
    writer.into_writer().into_inner()
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

fn combine(result: TestResult, cleanup: TestResult) -> TestResult {
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(format!("{error}; cleanup failed: {cleanup}")),
    }
}

struct TestDatabase {
    schema: String,
    pool: PgPool,
}

impl TestDatabase {
    async fn new() -> TestResult<Option<Self>> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set when CI is present".to_owned());
            }
            eprintln!("skipping PostgreSQL video API test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_video_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, 8, &schema)
            .await
            .map_err(debug_error)?;
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
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        if let Err(error) = run_migrations(&pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return Err(format!("migrations failed: {error:?}"));
        }
        Ok(Some(Self { schema, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(debug_error);
        self.pool.close().await;
        result.map(|_| ())
    }
}
