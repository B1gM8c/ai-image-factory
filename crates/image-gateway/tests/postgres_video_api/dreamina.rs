use super::*;
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

use gpt_image_2_gateway::{
    CanonicalExecutorOutcome, DreaminaCliPollDriverV1, DreaminaCliPollProcessConfig,
    DreaminaCliSubmitCodecV1, DreaminaCliSubmitProcessConfig, DreaminaExecutionProfileProvisioning,
    FilesystemProviderArtifactStagerFactory, GatedCliSubmitDriver, ProviderAccountHomeCapability,
    ProviderPollOrchestrator, ProviderPollOrchestratorConfig, ProviderPollRun,
    ProviderRuntimeProfileStore, ProviderRuntimeReadinessStore, ProviderRuntimeRegistration,
    ProviderRuntimeRole, ProviderSubmitIterationCommand, ProviderSubmitRun, ProviderSubmitService,
    ProviderSubmitServiceConfig, ProviderTaskState, provision_dreamina_video_execution_profile,
};
use image_cli_runtime::WorkingDirectory;
use image_provider_dreamina_cli::DREAMINA_SUBMIT_COMMAND_SCHEMA;
use sha2::{Digest, Sha256};

#[path = "dreamina_submission_uncertain.rs"]
mod dreamina_submission_uncertain;
#[path = "dreamina_images_async.rs"]
mod images_async;
#[path = "platform_binding.rs"]
mod platform_binding;
#[path = "dreamina_video_fixture.rs"]
mod video_fixture;
#[path = "dreamina_video_lookup.rs"]
mod video_lookup;
#[path = "dreamina_workflows.rs"]
mod workflows;

const MODEL: &str = "doubao-seedance-2-5-260628";
const TASKS: &str = "/api/v3/contents/generations/tasks";
// The opaque upstream URL is never contacted. An explicit debug-only loopback
// fixture supplies real MP4 bytes to the normal stream/stager/reducer chain.
const VIDEO_URL: &str = "https://video.example.invalid/clip%2fpart.mp4?sign=A%2Bb%2fC%3D&x=2&x=1";
const VIDEO_BYTES: &[u8] = include_bytes!("../../src/media_inputs/fixtures/blue-audio-5s.mp4");

// Runs the actual adapter, journalled CLI runner and poll driver against a local
// executable fixture. No official CLI, credentials or upstream generation is used.
#[tokio::test]
async fn ark_seedance25_api_runs_one_cli_submission_and_settlement() -> TestResult {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .with_test_writer()
        .try_init();
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = Box::pin(run_seedance25(&database.pool, &_guard)).await;
    combine(result, database.cleanup().await)
}

async fn run_seedance25(pool: &PgPool, serial: &tokio::sync::MutexGuard<'_, ()>) -> TestResult {
    let transport = video_fixture::VideoTransportFixture::start(serial).await?;
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
    // The CLI reports upstream identity; downloading still crosses real HTTP.
    fs::write(
        home.join("query-response.json"),
        serde_json::to_vec(&json!({
            "submit_id": "seedance25-api-fixture", "gen_status": "success",
            "result_json": {"videos": [{"video_url": VIDEO_URL}]}
        }))
        .map_err(debug_error)?,
    )
    .map_err(debug_error)?;
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
    printf '%s\n' "$@" > "$HOME/poll-arguments"
    submit=
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --submit_id) shift; submit=$1 ;;
        --download_dir|--download_dir=*) exit 90 ;;
      esac
      shift
    done
    [ "$submit" = seedance25-api-fixture ]
    printf 'poll\n' >> "$HOME/poll-calls"
    /bin/cat "$HOME/query-response.json"
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
    let runner = video_fixture::private_submit_runner(root.path())?;
    let driver = GatedCliSubmitDriver::new(
        codec.clone(),
        &runner,
        &hex::encode(Sha256::digest(fs::read(&runner).map_err(debug_error)?)),
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
    let sibling_project_id = format!("proj_{}", Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO gateway_projects (id, tenant_id, name, created_at)
        VALUES ($1, $2, 'Sibling project', 1)",
    )
    .bind(&sibling_project_id)
    .bind(&project.id)
    .execute(pool)
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
    let sibling = keys
        .create_service_account(
            &sibling_project_id,
            "Sibling project",
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
    let catalog = json!({"object":"list", "data":[{
        "id":MODEL, "object":"model", "created":0, "owned_by":"dreamina-cli",
        "metadata":{"surfaces":[{
            "provider_id":"dreamina-cli", "provider_model_id":"seedance2.5",
            "api_profile":"volcengine-ark-content-generation-v3",
            "operation_id":"videos.generations", "media_kind":"video",
            "workflows":["text2video", "image2video", "frames2video", "multimodal2video"]
        }]}
    }]});
    let empty_catalog = json!({"object":"list", "data":[]});
    assert_model_catalog(app.clone(), &owner.api_key.value, &catalog).await?;
    for key in [&other.api_key.value, &sibling.api_key.value] {
        assert_model_catalog(app.clone(), key, &empty_catalog).await?;
    }
    // Metadata follows the same route/account/project filters as public model IDs.
    for state in ["disabled", "enabled"] {
        sqlx::query(
            "UPDATE provider_route_heads SET state = $1 WHERE route_id IN
            (SELECT route_id FROM gateway_api_key_provider_routes WHERE api_key_id = $2)",
        )
        .bind(state)
        .bind(&owner.api_key.id)
        .execute(pool)
        .await
        .map_err(debug_error)?;
        assert_model_catalog(
            app.clone(),
            &owner.api_key.value,
            if state == "disabled" {
                &empty_catalog
            } else {
                &catalog
            },
        )
        .await?;
    }
    sqlx::query(
        "INSERT INTO provider_account_model_configurations
        (provider_account_id, provider_id, mode, version, updated_at_ms)
        VALUES ($1, 'dreamina-cli', 'allowlist', 1, 1)",
    )
    .bind(provisioned.provider_account_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    assert_model_catalog(app.clone(), &owner.api_key.value, &empty_catalog).await?;
    sqlx::query(
        "INSERT INTO provider_account_model_bindings
        (provider_account_id, provider_id, model_id, media_kind, configured_at_ms)
        VALUES ($1, 'dreamina-cli', 'seedance2.5', 'video', 1)",
    )
    .bind(provisioned.provider_account_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    assert_model_catalog(app.clone(), &owner.api_key.value, &catalog).await?;
    let policy_actor = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO identity_users
        (user_id, normalized_email, display_name, roles, scopes, created_at_ms, updated_at_ms)
        VALUES ($1, 'catalog-policy@example.test', 'Catalog policy actor',
                ARRAY['platform_owner'], ARRAY['admin:*'], 1, 1)",
    )
    .bind(policy_actor)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query("INSERT INTO project_model_policies
        (project_id, organization_id, created_by_user_id, updated_by_user_id, created_at_ms, updated_at_ms)
        VALUES ($1, $1, $2, $2, 1, 1)")
        .bind(&project.id).bind(policy_actor).execute(pool).await.map_err(debug_error)?;
    assert_model_catalog(app.clone(), &owner.api_key.value, &empty_catalog).await?;
    sqlx::query(
        "INSERT INTO project_model_access_entries
        (project_id, operation_id, api_profile, public_model_id, media_kind, created_at_ms)
        VALUES ($1, 'videos.generations', 'volcengine-ark-content-generation-v3', $2, 'video', 1)",
    )
    .bind(&project.id)
    .bind(MODEL)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    assert_model_catalog(app.clone(), &owner.api_key.value, &catalog).await?;
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
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        let mut tick = 0;
        loop {
            let recovery = ProviderSubmitIterationCommand::new(
                "dreamina-api-submit",
                format!("poll-claim-{tick}"),
                format!("poll-defer-{tick}"),
            )
            .map_err(debug_error)?;
            Box::pin(submit.run_once(&recovery))
                .await
                .map_err(debug_error)?;
            tick += 1;
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
    let poll_arguments = fs::read_to_string(home.join("poll-arguments")).map_err(debug_error)?;
    require(
        poll_arguments.contains("--submit_id\nseedance25-api-fixture\n")
            && !poll_arguments.contains("download"),
        "Dreamina query must identify the task without requesting a download",
    )?;
    let (status, unreduced) = json_request(
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
            && unreduced["status"] == "running"
            && unreduced["content"].is_null(),
        format!("Remote video escaped before terminal settlement: {status} {unreduced}"),
    )?;
    let before: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM ledger_transactions WHERE source_job_id = $1
                 AND transaction_type = 'customer_job_charge'),
                (SELECT COUNT(*) FROM job_response_projections WHERE job_id = $1)",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        before == (0, 0),
        format!("Premature terminal billing/projection: {before:?}"),
    )?;
    let reductions = PostgresExecutorTerminalStore::new(pool.clone());
    let terminal = reductions
        .claim_terminal("dreamina-api-reducer", 30_000)
        .await
        .map_err(|error| format!("Dreamina claim reduction: {error:?}"))?
        .ok_or("No Dreamina terminal reduction")?;
    require(
        matches!(&terminal.outcome, CanonicalExecutorOutcome::Succeeded(artifact)
            if artifact.media_type == "video/mp4" && artifact.byte_size == VIDEO_BYTES.len() as u64
              && artifact.sha256_hex == hex::encode(Sha256::digest(VIDEO_BYTES))),
        "Reducer did not receive the verified canonical MP4",
    )?;
    let customer = CustomerArtifactPublisher::new(blobs.clone())
        .publish(&terminal)
        .await
        .map_err(debug_error)?;
    assert_failed_charge_rolls_back(pool, &reductions, &terminal, &customer).await?;
    let completion = reductions
        .complete_terminal(&terminal, Some(&customer))
        .await
        .map_err(|error| format!("Dreamina complete reduction: {error:?}"))?;
    require(
        completion.customer_artifact_id == Some(terminal.output_id),
        "Canonical reduction omitted its original customer artifact",
    )?;
    require(
        completion
            == reductions
                .complete_terminal(&terminal, Some(&customer))
                .await
                .map_err(|error| format!("Dreamina replay reduction: {error:?}"))?,
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
    let ark_content = format!("/api/v3/files/{}/content", terminal.output_id);
    require(
        done["content"]["video_url"] == ark_content && !done.to_string().contains(VIDEO_URL),
        "Ark did not return the scoped canonical artifact",
    )?;
    let native_path = format!("/v1/dreamina/videos/{job_id}");
    let (native_status, native) = json_request(
        app.clone(),
        Method::GET,
        &native_path,
        &owner.api_key.value,
        None,
        None,
    )
    .await?;
    require(
        native_status == StatusCode::OK
            && native["content"]["video_url"]
                == format!("/v1/dreamina/files/{}/content", terminal.output_id),
        "Native Dreamina API did not return its scoped canonical artifact",
    )?;
    assert_video_download(app.clone(), &ark_content, &owner.api_key.value).await?;
    assert_video_download(
        app.clone(),
        native["content"]["video_url"]
            .as_str()
            .ok_or("missing native download")?,
        &owner.api_key.value,
    )
    .await?;
    for key in [&other.api_key.value, &sibling.api_key.value] {
        for path in [&task_path, &native_path] {
            let (status, response) =
                json_request(app.clone(), Method::GET, path, key, None, None).await?;
            let expected_status = if path == &task_path {
                StatusCode::NOT_FOUND
            } else {
                // The native API's existing not-found error uses HTTP 400.
                StatusCode::BAD_REQUEST
            };
            require(
                status == expected_status
                    && response["error"]["code"] == "not_found"
                    && !response.to_string().contains(VIDEO_URL),
                format!("Cross-tenant/project video read {path}: {status} {response}"),
            )?;
        }
        for path in [
            &ark_content,
            native["content"]["video_url"]
                .as_str()
                .ok_or("missing native download")?,
        ] {
            let (status, _, bytes) = raw_request(app.clone(), Method::GET, path, key).await?;
            require(
                status == StatusCode::NOT_FOUND && bytes != VIDEO_BYTES,
                "cross-tenant/project canonical download was not isolated",
            )?;
        }
    }
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
        replay.0 == StatusCode::OK && replay.1["id"] == task_id,
        "Completed admission replay created another task",
    )?;
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
        "Expected exactly one CLI submission and one metadata-only query",
    )?;
    assert_canonical_rejects_remote_evidence(pool, job_id).await?;
    assert_canonical_storage(pool, job_id, &artifact_root).await?;
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
    )?;
    transport.finish().await
}

async fn assert_video_download(app: axum::Router, path: &str, key: &str) -> TestResult {
    let (status, content_type, bytes) = raw_request(app, Method::GET, path, key).await?;
    require(
        status == StatusCode::OK
            && content_type.as_deref() == Some("video/mp4")
            && bytes == VIDEO_BYTES,
        "scoped content endpoint did not return the actual verified MP4 bytes",
    )
}

async fn assert_canonical_storage(pool: &PgPool, job_id: Uuid, artifact_root: &Path) -> TestResult {
    let stored: (i64, i64, i64, i64, i32, i32) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM artifacts WHERE job_id = $1),
                (SELECT COUNT(*) FROM executor_result_manifests manifest
                 JOIN provider_submissions submission USING (submission_id, executor_execution_id)
                 WHERE submission.job_id = $1),
                (SELECT COUNT(*) FROM executor_artifact_authorities WHERE job_id = $1),
                (SELECT COUNT(*) FROM provider_remote_video_results remote
                 JOIN provider_submissions submission USING (submission_id, executor_execution_id)
                 WHERE submission.job_id = $1), projection.artifact_count, projection.remote_result_count
         FROM job_response_projections projection WHERE projection.job_id = $1")
        .bind(job_id).fetch_one(pool).await.map_err(debug_error)?;
    require(
        stored == (1, 1, 1, 0, 1, 0),
        format!("Canonical artifact storage invariant failed: {stored:?}"),
    )?;
    let objects: Vec<(String, String, i64, String)> = sqlx::query_as(
        "SELECT object_key, sha256_hex, byte_size, media_type FROM artifacts WHERE job_id = $1
         UNION ALL SELECT object_key, sha256_hex, byte_size, media_type FROM executor_artifact_authorities WHERE job_id = $1")
        .bind(job_id).fetch_all(pool).await.map_err(debug_error)?;
    require(
        objects.len() == 2,
        "missing executor or customer immutable object",
    )?;
    for (key, digest, size, mime) in objects {
        require(
            size == VIDEO_BYTES.len() as i64
                && digest == hex::encode(Sha256::digest(VIDEO_BYTES))
                && mime == "video/mp4"
                && fs::read(artifact_root.join(key)).map_err(debug_error)? == VIDEO_BYTES,
            "canonical disk bytes differ from the transported MP4",
        )?;
    }
    let evidence: String =
        sqlx::query_scalar("SELECT evidence::text FROM provider_receipts WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        !evidence.contains(VIDEO_URL) && !evidence.contains("sign="),
        "Signed upstream URL leaked into receipt evidence",
    )?;
    fn has_files(path: &Path) -> TestResult<bool> {
        for entry in fs::read_dir(path).map_err(debug_error)? {
            let path = entry.map_err(debug_error)?.path();
            if path.is_file() || has_files(&path)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
    require(
        !has_files(&artifact_root.join("executor-staging"))?,
        "completed download retained an unfinished staging file",
    )
}

async fn assert_failed_charge_rolls_back(
    pool: &PgPool,
    reductions: &PostgresExecutorTerminalStore,
    terminal: &gpt_image_2_gateway::ExecutorTerminalLease,
    customer: &gpt_image_2_gateway::artifacts::ArtifactMetadata,
) -> TestResult {
    // A failure at customer charging must roll back earlier receipt, projection
    // and parent completion writes, including any released SQL savepoints.
    let prior_state: String = sqlx::query_scalar("SELECT state FROM jobs WHERE job_id = $1")
        .bind(terminal.job_id)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
    sqlx::raw_sql(
        "CREATE FUNCTION reject_test_customer_charge() RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.transaction_type = 'customer_job_charge' THEN
                RAISE EXCEPTION 'injected customer charge failure';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        CREATE TRIGGER reject_test_customer_charge BEFORE INSERT ON ledger_transactions
        FOR EACH ROW EXECUTE FUNCTION reject_test_customer_charge();",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let failed = reductions.complete_terminal(terminal, Some(customer)).await;
    sqlx::raw_sql(
        "DROP TRIGGER reject_test_customer_charge ON ledger_transactions;
        DROP FUNCTION reject_test_customer_charge();",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    require(
        failed.is_err(),
        "Injected charging failure did not reject completion",
    )?;
    let rolled_back: (String, String, i64, i64, i64, i64, i64, String, i32, i32) = sqlx::query_as(
        "SELECT job.state, reduction.state,
                (SELECT COUNT(*) FROM provider_receipts WHERE job_id = $1),
                (SELECT COUNT(*) FROM job_response_projections WHERE job_id = $1),
                (SELECT COUNT(*) FROM ledger_transactions WHERE source_job_id = $1
                    AND transaction_type = 'customer_job_charge'),
                account.captured_micros, account.held_micros,
                quota.state, quota.committed_units, quota.released_units
         FROM jobs job JOIN provider_submissions submission USING (job_id)
         JOIN executor_terminal_reductions reduction USING (submission_id, executor_execution_id)
         JOIN billing_accounts account ON account.tenant_id = job.tenant_id AND account.currency = 'USD'
         JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
         WHERE job.job_id = $1",
    )
    .bind(terminal.job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        rolled_back
            == (
                prior_state,
                "leased".to_owned(),
                0,
                0,
                0,
                0,
                5 * UNIT_PRICE_MICROS,
                "reserved".to_owned(),
                0,
                0,
            ),
        format!("Charging failure left partial terminal effects: {rolled_back:?}"),
    )
}

async fn assert_canonical_rejects_remote_evidence(pool: &PgPool, job_id: Uuid) -> TestResult {
    let result = sqlx::query(
        "INSERT INTO provider_remote_video_results
           (remote_result_id, executor_execution_id, submission_id, video_url, evidence_sha256_hex, observed_at_ms)
         SELECT executor_execution_id, executor_execution_id, submission_id, $2,
           encode(sha256(convert_to('dreamina-remote-video-result-v1','UTF8')
             || int8send(octet_length($2::TEXT)::BIGINT) || convert_to($2,'UTF8') || int8send(-1)), 'hex'),
           floor(extract(epoch FROM clock_timestamp())*1000)::BIGINT
         FROM provider_submissions WHERE job_id = $1")
        .bind(job_id).bind(VIDEO_URL).execute(pool).await;
    require(result.as_ref().err().and_then(sqlx::Error::as_database_error).is_some_and(|error|
        error.code().as_deref() == Some("P0001") && error.message() == "remote video result requires a live Dreamina video poll without local artifact evidence"),
        format!("Canonical local artifact allowed/confused remote evidence insertion: {result:?}"))
}

async fn assert_model_catalog(app: axum::Router, key: &str, expected: &Value) -> TestResult {
    let (status, models) = json_request(app, Method::GET, "/v1/models", key, None, None).await?;
    require(
        status == StatusCode::OK && &models == expected,
        format!("Unexpected authorized model catalog: {status} {models}; expected {expected}"),
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
    seed_model_route(
        pool,
        project,
        service_account,
        key,
        account,
        profile,
        home,
        FixtureModel::legacy(),
    )
    .await
}

#[derive(Clone, Copy)]
struct FixtureModel {
    api_profile: &'static str,
    public_model: &'static str,
    provider_model: &'static str,
    execution_model: &'static str,
    media_kind: &'static str,
    workflow: Option<&'static str>,
    rate_micros: i64,
}

impl FixtureModel {
    fn legacy() -> Self {
        Self {
            api_profile: "volcengine-ark-content-generation-v3",
            public_model: MODEL,
            provider_model: "seedance2.5",
            execution_model: "seedance2.5",
            media_kind: "video",
            workflow: None,
            rate_micros: UNIT_PRICE_MICROS,
        }
    }
    fn operation(self) -> &'static str {
        if self.media_kind == "image" {
            "images.generations"
        } else {
            "videos.generations"
        }
    }
    fn metric_unit_size(self) -> (&'static str, &'static str, &'static str) {
        if self.media_kind == "image" {
            ("image_output", "image", "1")
        } else if self.workflow == Some("multiframe2video") {
            ("video_requested_millisecond", "millisecond", "1000")
        } else {
            ("video_requested_second", "second", "1")
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn seed_model_route(
    pool: &PgPool,
    project: &str,
    service_account: &str,
    key: &str,
    account: Uuid,
    profile: Uuid,
    home: &Path,
    model: FixtureModel,
) -> TestResult {
    let route = seed_model_route_unbound(
        pool,
        project,
        service_account,
        key,
        account,
        profile,
        home,
        model,
    )
    .await?;
    platform_binding::publish_fixture_route(pool, home, route).await
}

#[allow(clippy::too_many_arguments)]
async fn seed_model_route_unbound(
    pool: &PgPool,
    project: &str,
    service_account: &str,
    key: &str,
    account: Uuid,
    profile: Uuid,
    home: &Path,
    model: FixtureModel,
) -> TestResult<Uuid> {
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
        VALUES ($1, 1, $2, 'Dreamina API fixture', 'dreamina-cli', $4,
                $3, 'account', 'quota_aware_least_loaded', 'enabled', 1)",
    )
    .bind(route)
    .bind(&route_key)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(model.operation())
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_route_heads
        (route_id, route_key, provider_id, operation_id, command_schema, route_kind,
         current_revision, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'dreamina-cli', $4, $3, 'account', 1, 'enabled', 1, 1)",
    )
    .bind(route)
    .bind(&route_key)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(model.operation())
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_route_members
        (route_id, route_revision, provider_id, operation_id, command_schema,
         provider_account_id, execution_profile_id, state, created_at_ms)
        VALUES ($1, 1, 'dreamina-cli', $5, $2, $3, $4, 'enabled', 1)",
    )
    .bind(route)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(account)
    .bind(profile)
    .bind(model.operation())
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO provider_models
        (provider_id, model_id, execution_model_id, media_kind, display_name,
         adapter_state, lifecycle_state, operation_ids, source_kind,
         first_seen_at_ms, last_seen_at_ms, metadata_json)
        VALUES ('dreamina-cli', $1, $2, $3, 'Dreamina workflow fixture',
                'supported', 'enabled', ARRAY[$4::TEXT], 'adapter_contract', 1, 1, '{}')",
    )
    .bind(model.provider_model)
    .bind(model.execution_model)
    .bind(model.media_kind)
    .bind(model.operation())
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query("INSERT INTO provider_route_model_mappings
        (route_id, route_revision, provider_id, operation_id, command_schema,
         api_profile, public_model_id, provider_model_id, execution_model_id, media_kind, created_at_ms)
        VALUES ($1, 1, 'dreamina-cli', $4, $2, $5, $3, $6, $7, $8, 1)")
        .bind(route).bind(DREAMINA_SUBMIT_COMMAND_SCHEMA).bind(model.public_model)
        .bind(model.operation()).bind(model.api_profile).bind(model.provider_model)
        .bind(model.execution_model).bind(model.media_kind)
        .execute(pool).await.map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO gateway_api_key_provider_routes
        (api_key_id, service_account_id, project_id, tenant_id, provider_id,
         operation_id, command_schema, route_id, route_revision, bound_at_ms)
        VALUES ($1, $2, $3, $3, 'dreamina-cli', $6, $4, $5, 1, 1)",
    )
    .bind(key)
    .bind(service_account)
    .bind(project)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(route)
    .bind(model.operation())
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(route)
}

async fn seed_economics(pool: &PgPool, tenant: &str) -> TestResult {
    seed_model_economics(pool, tenant, FixtureModel::legacy()).await
}

async fn seed_model_economics(pool: &PgPool, tenant: &str, model: FixtureModel) -> TestResult {
    let (metric, unit, unit_size) = model.metric_unit_size();
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
            component_key: format!("workflow-{outcome}"),
            metric: metric.to_owned(),
            unit: unit.to_owned(),
            unit_size: unit_size.to_owned(),
            unit_price_micros: if outcome == "succeeded" {
                model.rate_micros
            } else {
                0
            }
            .to_string(),
            outcome: outcome.to_owned(),
            quantity_source: "request_derived".to_owned(),
            required_confidence: "exact".to_owned(),
            rounding_mode: "exact".to_owned(),
            dimensions: model
                .workflow
                .map_or_else(|| json!({}), |workflow| json!({"workflow": workflow})),
        })
        .collect();
    let version = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: PriceBookVersionDraft {
                    api_profile: model.api_profile.to_owned(),
                    operation: if model.media_kind == "image" {
                        "generation"
                    } else {
                        "video_generation"
                    }
                    .to_owned(),
                    provider_id: Some("dreamina-cli".to_owned()),
                    provider_model_id: Some(model.provider_model.to_owned()),
                    public_model_id: model.public_model.to_owned(),
                    media_kind: model.media_kind.to_owned(),
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
