use super::*;
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

use gpt_image_2_gateway::{
    DreaminaCliPollDriverV1, DreaminaCliPollProcessConfig, DreaminaCliSubmitCodecV1,
    DreaminaCliSubmitProcessConfig, DreaminaExecutionProfileProvisioning,
    FilesystemProviderArtifactStagerFactory, GatedCliSubmitDriver, ProviderAccountHomeCapability,
    ProviderPollOrchestrator, ProviderPollOrchestratorConfig, ProviderPollRun,
    ProviderRuntimeProfileStore, ProviderRuntimeReadinessStore, ProviderRuntimeRegistration,
    ProviderRuntimeRole, ProviderSubmitIterationCommand, ProviderSubmitRun, ProviderSubmitService,
    ProviderSubmitServiceConfig, ProviderTaskState, provision_dreamina_video_execution_profile,
};
use image_cli_runtime::WorkingDirectory;
use image_provider_dreamina_cli::DREAMINA_SUBMIT_COMMAND_SCHEMA;
use sha2::{Digest, Sha256};

const MODEL: &str = "doubao-seedance-2-5-260628";
const TASKS: &str = "/api/v3/contents/generations/tasks";

// Runs the actual adapter, journalled CLI runner and poll driver against a local
// executable fixture. No official CLI, credentials or upstream generation is used.
#[tokio::test]
async fn ark_seedance25_api_runs_one_cli_submission_and_settlement() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = Box::pin(run_seedance25(&database.pool)).await;
    combine(result, database.cleanup().await)
}

async fn run_seedance25(pool: &PgPool) -> TestResult {
    let root = TempDir::new().map_err(debug_error)?;
    let home = root.path().join("account");
    let submit_workspace = root.path().join("submit");
    let poll_workspace = root.path().join("poll");
    let journal = root.path().join("journal");
    let artifact_root = root.path().join("artifacts");
    for path in [
        &home,
        &submit_workspace,
        &poll_workspace,
        &journal,
        &artifact_root,
    ] {
        fs::create_dir(path).map_err(debug_error)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
    }
    // This is a structural transport fixture, not evidence of upstream video
    // dimensions or duration. Ark metadata below describes the frozen request.
    let mp4 = minimal_mp4();
    fs::write(home.join("source.mp4"), &mp4).map_err(debug_error)?;
    let executable = root.path().join("dreamina-fixture");
    let script = br#"#!/bin/sh
set -eu
operation=$1
shift
case "$operation" in
  text2video)
    printf '%s\n' "$@" > "$HOME/submit-arguments"
    printf 'submit\n' >> "$HOME/submit-calls"
    printf '{"submit_id":"seedance25-api-fixture","gen_status":"querying"}'
    ;;
  query_result)
    submit=
    download=
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --submit_id) shift; submit=$1 ;;
        --download_dir) shift; download=$1 ;;
      esac
      shift
    done
    [ "$submit" = seedance25-api-fixture ]
    printf 'poll\n' >> "$HOME/poll-calls"
    /bin/cp "$HOME/source.mp4" "$download/result.mp4"
    printf '{"submit_id":"seedance25-api-fixture","gen_status":"success"}'
    ;;
  *) exit 2 ;;
esac
"#;
    fs::write(&executable, script).map_err(debug_error)?;
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).map_err(debug_error)?;
    let executable_sha: [u8; 32] = Sha256::digest(script).into();
    let provisioned = provision_dreamina_video_execution_profile(
        pool,
        &DreaminaExecutionProfileProvisioning {
            profile_key: "dreamina-video-api-fixture".to_owned(),
            credential_pool_key: "dreamina-video-api-fixture".to_owned(),
            provider_account_key: "dreamina-video-api-fixture".to_owned(),
            credential_ref: "private:dreamina-video-api-fixture".to_owned(),
            credential_revision: 1,
            credential_auth_sha256: "a".repeat(64),
            max_concurrency: 1,
        },
    )
    .await
    .map_err(debug_error)?;
    let provider_store = PostgresProviderTaskStore::new(pool.clone());
    let profile = provider_store
        .load_active_runtime_profile("dreamina-video-api-fixture")
        .await
        .map_err(debug_error)?;
    let account_home = ProviderAccountHomeCapability::new(
        "dreamina-cli",
        provisioned.credential_pool_id,
        provisioned.provider_account_id,
        "private:dreamina-video-api-fixture".to_owned(),
        1,
        "a".repeat(64),
        &home,
    )
    .map_err(debug_error)?;
    let codec = DreaminaCliSubmitCodecV1::from_runtime_profile(
        &profile,
        &account_home,
        DreaminaCliSubmitProcessConfig::new(
            &executable,
            executable_sha,
            WorkingDirectory::new_private(&submit_workspace).map_err(debug_error)?,
            Duration::from_secs(5),
            Duration::from_millis(100),
        ),
    )
    .map_err(debug_error)?;
    let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
    let driver = GatedCliSubmitDriver::new(
        codec.clone(),
        runner,
        &hex::encode(Sha256::digest(fs::read(runner).map_err(debug_error)?)),
        WorkingDirectory::new_private(&submit_workspace).map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    let executor = Arc::new(PostgresExecutorSubmissionStore::new(pool.clone()));
    let submit = ProviderSubmitService::new(
        PostgresExecutorSubmissionStore::new(pool.clone()),
        provider_store.clone(),
        driver,
        codec,
        ProviderSubmitServiceConfig {
            executor_scope: profile.executor_claim_scope(),
            provider_scope: profile.claim_scope(),
            provider_timeout_ms: 30_000,
            executor_lease_ms: 30_000,
            recovery_lease_ms: 30_000,
            heartbeat_interval: Duration::from_secs(1),
            recovery_retry_after_ms: 10,
        },
        &journal,
    )
    .map_err(debug_error)?;
    let blobs = Arc::new(FilesystemArtifactBlobStore::new(&artifact_root).map_err(debug_error)?);
    let poll = ProviderPollOrchestrator::new(
        provider_store.clone(),
        DreaminaCliPollDriverV1::from_runtime_profile(
            &profile,
            &account_home,
            DreaminaCliPollProcessConfig::new(
                &executable,
                executable_sha,
                WorkingDirectory::new_private(&poll_workspace).map_err(debug_error)?,
                Duration::from_secs(5),
                Duration::from_millis(100),
                1024 * 1024,
            ),
        )
        .map_err(debug_error)?,
        FilesystemProviderArtifactStagerFactory::new(blobs.clone(), 1024 * 1024)
            .map_err(debug_error)?,
        ProviderPollOrchestratorConfig {
            scope: profile.claim_scope(),
            owner: "dreamina-video-api-poll".to_owned(),
            lease_ms: 30_000,
            heartbeat_interval: Duration::from_secs(1),
            max_materializations: 1,
        },
    )
    .map_err(debug_error)?;
    for role in [ProviderRuntimeRole::Submit, ProviderRuntimeRole::Poll] {
        provider_store
            .register_runtime(
                &ProviderRuntimeRegistration {
                    runtime_id: Uuid::new_v4(),
                    execution_profile_id: provisioned.execution_profile_id,
                    role,
                    runtime_owner: format!("dreamina-video-api-{role:?}"),
                },
                60_000,
            )
            .await
            .map_err(debug_error)?;
    }

    let keys = Arc::new(PostgresApiKeyStore::new(
        pool.clone(),
        ApiKeyKeyring::new(1, [(1, vec![0x46; 32])]).map_err(debug_error)?,
    ));
    let project = keys
        .create_project("Dreamina API fixture")
        .await
        .map_err(debug_error)?;
    let other_project = keys
        .create_project("Other tenant")
        .await
        .map_err(debug_error)?;
    let owner = keys
        .create_service_account(
            &project.id,
            "Owner",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_error)?;
    let other = keys
        .create_service_account(
            &other_project.id,
            "Other",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_error)?;
    seed_route(
        pool,
        &project.id,
        &owner.id,
        &owner.api_key.id,
        provisioned.provider_account_id,
        provisioned.execution_profile_id,
        &home,
    )
    .await?;
    seed_economics(pool, &project.id).await?;
    let settlement = Arc::new(PostgresExecutionSettlementStore::new(
        pool.clone(),
        blobs.clone(),
    ));
    let app = build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing(
        config(),
        ExternalImageGatewayComponents {
            usage_store: Arc::new(PostgresUsageStore::new(pool.clone())),
            api_key_store: keys,
            admission_store: Arc::new(PostgresAdmissionStore::new(pool.clone())),
            settlement_store: settlement,
            input_blob_store: blobs.clone(),
            provider_readiness_store: Arc::new(provider_store),
        }, None, None, None, None,
        Some(Arc::new(PostgresModelRoutingStore::new(pool.clone()))),
    ).map_err(debug_error)?;
    let (models_status, models) = json_request(
        app.clone(),
        Method::GET,
        "/v1/models",
        &owner.api_key.value,
        None,
        None,
    )
    .await?;
    require(
        models_status == StatusCode::OK
            && models["data"]
                .as_array()
                .is_some_and(|models| models.iter().any(|model| model["id"] == MODEL)),
        format!("Seedance 2.5 missing from routed model catalog: {models_status} {models}"),
    )?;
    let body = json!({"model": MODEL,
        "content": [{"type": "text", "text": "An orange paper airplane on a blue background"}],
        "duration": 5, "resolution": "480p", "ratio": "16:9"});
    let (status, created) = json_request(
        app.clone(),
        Method::POST,
        TASKS,
        &owner.api_key.value,
        Some("seedance25-api-fixture"),
        Some(&body),
    )
    .await?;
    require(
        status == StatusCode::OK,
        format!("Ark admission: {status} {created}"),
    )?;
    let task_id = created["id"]
        .as_str()
        .ok_or("Ark omitted task id")?
        .to_owned();
    let job_id = Uuid::parse_str(task_id.strip_prefix("cgt-").ok_or("Ark task id prefix")?)
        .map_err(debug_error)?;
    let replay = json_request(
        app.clone(),
        Method::POST,
        TASKS,
        &owner.api_key.value,
        Some("seedance25-api-fixture"),
        Some(&body),
    )
    .await?;
    require(
        replay == (status, created),
        format!("Ark admission replay diverged: {replay:?}"),
    )?;
    let task_path = format!("{TASKS}/{task_id}");
    let (status, pending) = json_request(
        app.clone(),
        Method::GET,
        &task_path,
        &owner.api_key.value,
        None,
        None,
    )
    .await?;
    require(
        status == StatusCode::OK
            && pending["status"] == "running"
            && pending["resolution"] == "480p"
            && pending["ratio"] == "16:9",
        format!("Ark frozen request metadata: {status} {pending}"),
    )?;

    let workerd = Workerd::new_handoff_only_with_contract(
        "dreamina-video-api-workerd".to_owned(),
        Arc::new(PostgresAdmissionStore::new(pool.clone())),
        Arc::new(PostgresExecutionContextStore::new(pool.clone())),
        executor,
        provisioned.execution_profile_id,
        DREAMINA_SUBMIT_COMMAND_SCHEMA.to_owned(),
        Duration::from_secs(30),
        AdmissionContract::CustomerPricingV4,
    )
    .map_err(debug_error)?;
    require(
        workerd.run_once().await.map_err(debug_error)? == Some(job_id),
        "Dreamina workerd did not hand off the API task",
    )?;
    let command = ProviderSubmitIterationCommand::new("dreamina-api-submit", "claim-1", "defer-1")
        .map_err(debug_error)?;
    let run = Box::pin(submit.run_once(&command))
        .await
        .map_err(|error| format!("Dreamina submit: {error:?}"))?;
    require(
        run == ProviderSubmitRun::FreshSubmitted,
        format!("Dreamina CLI submit: {run:?}"),
    )?;
    let arguments = fs::read_to_string(home.join("submit-arguments")).map_err(debug_error)?;
    for expected in [
        "--model_version\nseedance2.5\n",
        "--video_resolution\n480p\n",
        "--duration\n5\n",
        "--ratio\n16:9\n",
        "--poll=0\n",
    ] {
        require(
            arguments.contains(expected),
            format!("CLI argument missing: {expected:?}"),
        )?;
    }
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let run = Box::pin(poll.run_once())
                .await
                .map_err(|error| format!("Dreamina poll: {error:?}"))?;
            if !matches!(run, ProviderPollRun::Idle) {
                return Ok::<_, String>(run);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .map_err(debug_error)??;
    require(
        matches!(observed, ProviderPollRun::Observed(ref task)
        if task.state == ProviderTaskState::ArtifactReady),
        format!("Dreamina CLI poll: {observed:?}"),
    )?;
    let reductions = PostgresExecutorTerminalStore::new(pool.clone());
    let terminal = reductions
        .claim_terminal("dreamina-api-reducer", 30_000)
        .await
        .map_err(debug_error)?
        .ok_or("No Dreamina terminal reduction")?;
    let artifact = CustomerArtifactPublisher::new(blobs)
        .publish(&terminal)
        .await
        .map_err(debug_error)?;
    let completion = reductions
        .complete_terminal(&terminal, Some(&artifact))
        .await
        .map_err(debug_error)?;
    require(
        completion
            == reductions
                .complete_terminal(&terminal, Some(&artifact))
                .await
                .map_err(debug_error)?,
        "Repeated reduction changed settlement identity",
    )?;
    let (status, done) = json_request(
        app.clone(),
        Method::GET,
        &task_path,
        &owner.api_key.value,
        None,
        None,
    )
    .await?;
    require(
        status == StatusCode::OK
            && done["status"] == "succeeded"
            && done["model"] == MODEL
            && done["duration"] == 5
            && done["resolution"] == "480p"
            && done["ratio"] == "16:9",
        format!("Ark result: {status} {done}"),
    )?;
    let content = done["content"]["video_url"]
        .as_str()
        .ok_or("Ark omitted video URL")?;
    let (status, mime, bytes) =
        raw_request(app.clone(), Method::GET, content, &owner.api_key.value).await?;
    require(
        status == StatusCode::OK && mime.as_deref() == Some("video/mp4") && bytes == mp4,
        "Ark artifact did not preserve MP4 bytes",
    )?;
    for path in [&task_path, content] {
        let (status, _) = json_request(
            app.clone(),
            Method::GET,
            path,
            &other.api_key.value,
            None,
            None,
        )
        .await?;
        require(
            status == StatusCode::NOT_FOUND,
            format!("Cross-tenant Ark read: {status}"),
        )?;
    }
    require(
        Box::pin(submit.run_once(&command))
            .await
            .map_err(|error| format!("Post-completion submit: {error:?}"))?
            == ProviderSubmitRun::Idle,
        "Completed task was submitted again",
    )?;
    require(
        matches!(
            Box::pin(poll.run_once())
                .await
                .map_err(|error| format!("Post-completion poll: {error:?}"))?,
            ProviderPollRun::Idle
        ),
        "Completed task was polled again",
    )?;
    require(
        fs::read_to_string(home.join("submit-calls")).map_err(debug_error)? == "submit\n"
            && fs::read_to_string(home.join("poll-calls")).map_err(debug_error)? == "poll\n",
        "Expected exactly one CLI submission and one materializing query",
    )?;
    let ledger: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM provider_submissions WHERE job_id = $1),
                (SELECT COUNT(*) FROM ledger_transactions WHERE source_job_id = $1
                 AND transaction_type = 'customer_job_charge'), held_micros, captured_micros
         FROM billing_accounts WHERE tenant_id = $2 AND currency = 'USD'",
    )
    .bind(job_id)
    .bind(&project.id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        ledger == (1, 1, 0, 5 * UNIT_PRICE_MICROS),
        format!("Single settlement: {ledger:?}"),
    )
}

async fn seed_route(
    pool: &PgPool,
    project: &str,
    service_account: &str,
    key: &str,
    account: Uuid,
    profile: Uuid,
    home: &Path,
) -> TestResult {
    let route = Uuid::new_v4();
    let route_key = format!("dreamina-api-{}", route.simple());
    sqlx::query("INSERT INTO provider_account_environments
        (provider_account_id, provider_id, environment_kind, environment_ref,
         upstream_identity_sha256, display_name, state, created_at_ms, updated_at_ms)
        VALUES ($1, 'dreamina-cli', 'dreamina_home_v1', $2, $3, 'Dreamina API fixture', 'active', 1, 1)")
        .bind(account).bind(home.to_string_lossy().as_ref()).bind("c".repeat(64))
        .execute(pool).await.map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_routes
        (route_id, revision, route_key, display_name, provider_id, operation_id,
         command_schema, route_kind, selection_strategy, state, created_at_ms)
        VALUES ($1, 1, $2, 'Dreamina API fixture', 'dreamina-cli', 'videos.generations',
                $3, 'account', 'quota_aware_least_loaded', 'enabled', 1)",
    )
    .bind(route)
    .bind(&route_key)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_route_heads
        (route_id, route_key, provider_id, operation_id, command_schema, route_kind,
         current_revision, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'dreamina-cli', 'videos.generations', $3, 'account', 1, 'enabled', 1, 1)",
    )
    .bind(route)
    .bind(&route_key)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_route_members
        (route_id, route_revision, provider_id, operation_id, command_schema,
         provider_account_id, execution_profile_id, state, created_at_ms)
        VALUES ($1, 1, 'dreamina-cli', 'videos.generations', $2, $3, $4, 'enabled', 1)",
    )
    .bind(route)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(account)
    .bind(profile)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query("INSERT INTO provider_models
        (provider_id, model_id, execution_model_id, media_kind, display_name,
         adapter_state, lifecycle_state, operation_ids, source_kind,
         first_seen_at_ms, last_seen_at_ms, metadata_json)
        VALUES ('dreamina-cli', 'seedance2.5', 'seedance2.5', 'video', 'Seedance 2.5',
                'supported', 'enabled', ARRAY['videos.generations'], 'adapter_contract', 1, 1, '{}')")
        .execute(pool).await.map_err(debug_error)?;
    sqlx::query("INSERT INTO provider_route_model_mappings
        (route_id, route_revision, provider_id, operation_id, command_schema,
         api_profile, public_model_id, provider_model_id, execution_model_id, media_kind, created_at_ms)
        VALUES ($1, 1, 'dreamina-cli', 'videos.generations', $2,
                'volcengine-ark-content-generation-v3', $3, 'seedance2.5', 'seedance2.5', 'video', 1)")
        .bind(route).bind(DREAMINA_SUBMIT_COMMAND_SCHEMA).bind(MODEL)
        .execute(pool).await.map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO gateway_api_key_provider_routes
        (api_key_id, service_account_id, project_id, tenant_id, provider_id,
         operation_id, command_schema, route_id, route_revision, bound_at_ms)
        VALUES ($1, $2, $3, $3, 'dreamina-cli', 'videos.generations', $4, $5, 1, 1)",
    )
    .bind(key)
    .bind(service_account)
    .bind(project)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(route)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO gateway_platform_provider_routes
        (provider_id, operation_id, command_schema, route_id, route_revision,
         state, created_at_ms, updated_at_ms)
        VALUES ('dreamina-cli', 'videos.generations', $1, $2, 1, 'enabled', 1, 1)",
    )
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(route)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn seed_economics(pool: &PgPool, tenant: &str) -> TestResult {
    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "customer.dreamina-api-fixture.usd".to_owned(),
            display_name: "Dreamina API fixture".to_owned(),
            purpose: "customer_sale".to_owned(),
            scope_type: "platform".to_owned(),
            organization_id: None,
            project_id: None,
            provider_id: Some("dreamina-cli".to_owned()),
            currency: "USD".to_owned(),
        })
        .await
        .map_err(debug_error)?;
    let components = ["succeeded", "failed", "no_effect"]
        .into_iter()
        .map(|outcome| PriceComponentDraft {
            component_key: format!("video-second-{outcome}"),
            metric: "video_requested_second".to_owned(),
            unit: "second".to_owned(),
            unit_size: "1".to_owned(),
            unit_price_micros: if outcome == "succeeded" {
                UNIT_PRICE_MICROS
            } else {
                0
            }
            .to_string(),
            outcome: outcome.to_owned(),
            quantity_source: "request_derived".to_owned(),
            required_confidence: "exact".to_owned(),
            rounding_mode: "exact".to_owned(),
            dimensions: json!({}),
        })
        .collect();
    let version = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: PriceBookVersionDraft {
                    api_profile: "volcengine-ark-content-generation-v3".to_owned(),
                    operation: "video_generation".to_owned(),
                    provider_id: Some("dreamina-cli".to_owned()),
                    provider_model_id: Some("seedance2.5".to_owned()),
                    public_model_id: MODEL.to_owned(),
                    media_kind: "video".to_owned(),
                    service_tier: "standard".to_owned(),
                    execution_surface: "provider_cli".to_owned(),
                    billing_mode: "customer_rate".to_owned(),
                    is_free: false,
                    effective_from_ms: 0,
                    source_kind: "manual".to_owned(),
                    source_url: None,
                    source_checked_at_ms: Some(1),
                    notes: Some("Disposable test rate, not a production price".to_owned()),
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
    sqlx::query("INSERT INTO billing_accounts
        (tenant_id, currency, credit_limit_micros, held_micros, captured_micros, created_at_ms, updated_at_ms)
        VALUES ($1, 'USD', 1000000, 0, 0, 1, 1)")
        .bind(tenant).execute(pool).await.map_err(debug_error)?;
    Ok(())
}
