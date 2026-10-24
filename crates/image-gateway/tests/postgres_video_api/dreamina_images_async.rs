use super::*;
use gpt_image_2_gateway::{
    ProviderTaskStore,
    provider_tasks::{
        ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskObservationSource,
    },
    provision_dreamina_execution_profile,
};
use image_provider_sdk::ProviderTaskProgress;

const IMAGES: &str = "/v1/dreamina/images/generations";
const LOOKUP: &str = "/v1/dreamina/images/by-idempotency-key";
const KEY: &str = "async-image-original-id";

#[tokio::test]
async fn dreamina_async_images_keep_original_id_progress_scope_and_artifact() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = Box::pin(run_async_image(&database.pool, true)).await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn dreamina_async_uncertain_images_keep_the_hold_and_original_id() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = Box::pin(run_async_image(&database.pool, false)).await;
    combine(result, database.cleanup().await)
}

async fn run_async_image(pool: &PgPool, succeeds: bool) -> TestResult {
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
    let mut encoded = Cursor::new(Vec::new());
    ImageBuffer::from_pixel(32, 18, Rgba([30_u8, 100, 200, 255]))
        .write_to(&mut encoded, ImageFormat::Png)
        .map_err(debug_error)?;
    let png = encoded.into_inner();
    fs::write(home.join("result.png"), &png).map_err(debug_error)?;
    let executable = root.path().join("dreamina-async-image-fixture");
    // The only generation runs against this local fixture. GETs must never invoke it.
    let script = br#"#!/bin/sh
set -eu
operation=$1
shift
case "$operation" in
  text2image)
    printf 'submit\n' >> "$HOME/submit-calls"
    printf '{"submit_id":"async-image-original","gen_status":"querying"}'
    ;;
  query_result)
    submit= download=
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --submit_id) shift; submit=$1 ;;
        --download_dir) shift; download=$1 ;;
        --download_dir=*) download=${1#*=} ;;
      esac
      shift
    done
    [ "$submit" = async-image-original ]
    [ -n "$download" ]
    printf 'poll\n' >> "$HOME/poll-calls"
    /bin/cp "$HOME/result.png" "$download/result.png"
    printf '{"submit_id":"async-image-original","gen_status":"success"}'
    ;;
  *) exit 2 ;;
esac
"#;
    fs::write(&executable, script).map_err(debug_error)?;
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).map_err(debug_error)?;
    let executable_sha: [u8; 32] = Sha256::digest(script).into();
    let provisioned = provision_dreamina_execution_profile(
        pool,
        &DreaminaExecutionProfileProvisioning {
            profile_key: "dreamina-async-image-fixture".into(),
            credential_pool_key: "dreamina-async-image-fixture".into(),
            provider_account_key: "dreamina-async-image-fixture".into(),
            credential_ref: "private:dreamina-async-image-fixture".into(),
            credential_revision: 1,
            credential_auth_sha256: "a".repeat(64),
            max_concurrency: 1,
        },
    )
    .await
    .map_err(debug_error)?;
    let provider_store = PostgresProviderTaskStore::new(pool.clone());
    let profile = provider_store
        .load_active_runtime_profile("dreamina-async-image-fixture")
        .await
        .map_err(debug_error)?;
    let account_home = ProviderAccountHomeCapability::new(
        "dreamina-cli",
        provisioned.credential_pool_id,
        provisioned.provider_account_id,
        "private:dreamina-async-image-fixture",
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
    let blobs = Arc::new(FilesystemArtifactBlobStore::new(&artifact_root).map_err(debug_error)?);
    let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
    let driver = GatedCliSubmitDriver::new(
        codec.clone(),
        runner,
        &hex::encode(Sha256::digest(fs::read(runner).map_err(debug_error)?)),
        WorkingDirectory::new_private(&submit_workspace).map_err(debug_error)?,
    )
    .map_err(debug_error)?;
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
            owner: "async-image-poll".into(),
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
                    runtime_owner: format!("async-image-{role:?}"),
                },
                60_000,
            )
            .await
            .map_err(debug_error)?;
    }
    let keys = Arc::new(PostgresApiKeyStore::new(
        pool.clone(),
        ApiKeyKeyring::new(1, [(1, vec![0x48; 32])]).map_err(debug_error)?,
    ));
    let project = keys
        .create_project("Async image fixture")
        .await
        .map_err(debug_error)?;
    let other_project = keys
        .create_project("Other image fixture")
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
    let reader = keys
        .create_service_account(
            &project.id,
            "Reader",
            ApiKeyPermissionMode::ReadOnly,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_error)?;
    let denied = keys
        .create_service_account(
            &project.id,
            "Denied",
            ApiKeyPermissionMode::Restricted,
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
    let model = FixtureModel {
        api_profile: "dreamina-cli-images-v1",
        public_model: "5.0",
        provider_model: "5.0",
        execution_model: "dreamina-image-5.0",
        media_kind: "image",
        workflow: None,
        rate_micros: 1000,
    };
    seed_model_route(
        pool,
        &project.id,
        &owner.id,
        &owner.api_key.id,
        provisioned.provider_account_id,
        provisioned.execution_profile_id,
        &home,
        model,
    )
    .await?;
    seed_model_economics(pool, &project.id, model).await?;
    let settlement = Arc::new(PostgresExecutionSettlementStore::new(
        pool.clone(),
        blobs.clone(),
    ));
    let app = build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing(
        config(), ExternalImageGatewayComponents {
            usage_store: Arc::new(PostgresUsageStore::new(pool.clone())), api_key_store: keys,
            admission_store: Arc::new(PostgresAdmissionStore::new(pool.clone())),
            settlement_store: settlement.clone(), input_blob_store: blobs.clone(),
            provider_readiness_store: Arc::new(provider_store.clone()),
        }, None, None, None, None, Some(Arc::new(PostgresModelRoutingStore::new(pool.clone()))),
    ).map_err(debug_error)?;
    let body = json!({"prompt":"private prompt must not appear in task metadata", "model_version":"5.0",
        "ratio":"16:9", "resolution_type":"2k", "generate_num":1});
    let (status, location, created) = image_request(
        &app,
        Method::POST,
        IMAGES,
        &owner.api_key.value,
        Some(KEY),
        Some(&body),
        true,
    )
    .await?;
    require(
        status == StatusCode::ACCEPTED && created["status"] == "running",
        format!("async admission did not return immediately: {status} {created}"),
    )?;
    let task_id = created["id"]
        .as_str()
        .ok_or("async response omitted original id")?;
    let job = Uuid::parse_str(task_id).map_err(debug_error)?;
    let path = format!("/v1/dreamina/images/{job}");
    require(
        location.as_deref() == Some(path.as_str()),
        "async Location changed original task ID",
    )?;
    require(
        created["request"]["generate_num"] == 1
            && created["request"]["ratio"] == "16:9"
            && created["request"]["resolution_type"] == "2k"
            && created["request_id"].is_string(),
        format!("missing frozen image metadata: {created}"),
    )?;
    require(
        !created.to_string().contains("private prompt"),
        "task metadata leaked its prompt",
    )?;
    require(
        created["cancellation"]["supported"] == false && created.get("provider_progress").is_none(),
        "new task invented progress or cancellation support",
    )?;
    let replay = image_request(
        &app,
        Method::POST,
        IMAGES,
        &owner.api_key.value,
        Some(KEY),
        Some(&body),
        true,
    )
    .await?;
    require(
        replay.0 == StatusCode::ACCEPTED && replay.1 == location && replay.2 == created,
        format!("async replay changed task identity/state: {replay:?}"),
    )?;
    for lookup in [LOOKUP.to_owned(), format!("{LOOKUP}?operation=generation")] {
        let found = image_request(
            &app,
            Method::GET,
            &lookup,
            &reader.api_key.value,
            Some(KEY),
            None,
            false,
        )
        .await?;
        require(
            found.0 == StatusCode::OK && found.2 == created,
            "key lookup did not recover original task",
        )?;
    }
    for (uri, key, expected) in [
        (LOOKUP, None, StatusCode::BAD_REQUEST),
        (LOOKUP, Some(""), StatusCode::BAD_REQUEST),
        (LOOKUP, Some("unknown-key"), StatusCode::NOT_FOUND),
        (
            "/v1/dreamina/images/by-idempotency-key?operation=video",
            Some(KEY),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/dreamina/images/by-idempotency-key?operation=edit",
            Some(KEY),
            StatusCode::NOT_FOUND,
        ),
    ] {
        let response = image_request(
            &app,
            Method::GET,
            uri,
            &reader.api_key.value,
            key,
            None,
            false,
        )
        .await?;
        require(
            response.0 == expected,
            format!("lookup validation: {uri} {response:?}"),
        )?;
    }
    for uri in [&path, LOOKUP] {
        for (token, expected) in [
            (&denied.api_key.value, StatusCode::FORBIDDEN),
            (&other.api_key.value, StatusCode::NOT_FOUND),
        ] {
            let response =
                image_request(&app, Method::GET, uri, token, Some(KEY), None, false).await?;
            require(
                response.0 == expected,
                format!("image read permission/scope leak: {uri} {response:?}"),
            )?;
        }
    }
    let digest = gpt_image_2_gateway::admission::idempotency_key_digest(
        &project.id,
        "dreamina-cli-images-v1",
        "generation",
        KEY,
    )
    .map_err(debug_error)?;
    for (tenant, visible_project, actor) in [
        ("other-tenant", project.id.as_str(), None),
        (project.id.as_str(), "other-project", None),
        (
            project.id.as_str(),
            project.id.as_str(),
            Some(Uuid::new_v4()),
        ),
    ] {
        require(
            settlement
                .project_image_status(tenant, visible_project, actor, job)
                .await
                .map_err(debug_error)?
                .is_none(),
            "image store leaked tenant/project/actor scope",
        )?;
        require(
            settlement
                .project_image_job_by_idempotency_key(
                    tenant,
                    visible_project,
                    actor,
                    "generation",
                    &digest,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "key lookup leaked tenant/project/actor scope",
        )?;
    }
    let workerd = Workerd::new_handoff_only_with_contract(
        "async-image-workerd".into(),
        Arc::new(PostgresAdmissionStore::new(pool.clone())),
        Arc::new(PostgresExecutionContextStore::new(pool.clone())),
        Arc::new(PostgresExecutorSubmissionStore::new(pool.clone())),
        provisioned.execution_profile_id,
        DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
        Duration::from_secs(30),
        AdmissionContract::CustomerPricingV4,
    )
    .map_err(debug_error)?;
    require(
        workerd.run_once().await.map_err(debug_error)? == Some(job),
        "async task was not handed off",
    )?;
    let command = ProviderSubmitIterationCommand::new("async-image-submit", "claim-0", "defer-0")
        .map_err(debug_error)?;
    require(
        Box::pin(submit.run_once(&command))
            .await
            .map_err(debug_error)?
            == ProviderSubmitRun::FreshSubmitted,
        "async image was not submitted once",
    )?;
    let mut progress_time = None;
    for (index, progress) in [
        Some(ProviderTaskProgress {
            provider_status: "Queueing".into(),
            queue_position: Some(3341),
            queue_total: Some(476961),
            estimated_remaining_seconds: Some(27131),
        }),
        None,
    ]
    .into_iter()
    .enumerate()
    {
        let lease = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(lease) = provider_store
                    .claim_due(&profile.claim_scope(), "async-image-observer", 30_000)
                    .await
                    .map_err(debug_error)?
                {
                    return Ok::<_, String>(lease);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(debug_error)??;
        provider_store
            .record_observation(
                &lease,
                &ProviderTaskObservation {
                    event_identity: format!("async-image-progress-{index}"),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                    progress,
                },
            )
            .await
            .map_err(debug_error)?;
        let before = image_read_snapshot(pool, job).await?;
        for uri in [&path, LOOKUP] {
            let response = image_request(
                &app,
                Method::GET,
                uri,
                &reader.api_key.value,
                Some(KEY),
                None,
                false,
            )
            .await?;
            require(
                response.0 == StatusCode::OK
                    && response.2["status"] == "running"
                    && response.2["stage"] == "provider_queued"
                    && response.2["provider_progress"]["queue_position"] == 3341
                    && response.2["provider_progress"]["queue_total"] == 476961
                    && response.2["provider_progress"]["estimated_remaining_seconds"] == 27131,
                format!("running queue projection changed: {response:?}"),
            )?;
            let observed = response.2["provider_progress"]["observed_at_ms"]
                .as_i64()
                .ok_or("queue progress missing durable time")?;
            if let Some(previous) = progress_time {
                require(
                    observed == previous,
                    "GET refreshed old valid progress time",
                )?;
            }
            progress_time = Some(observed);
        }
        require(
            image_read_snapshot(pool, job).await? == before,
            "GET modified task/observation/billing state",
        )?;
    }
    if !succeeds {
        let lease = provider_store
            .claim_due(&profile.claim_scope(), "async-image-uncertain", 30_000)
            .await
            .map_err(debug_error)?
            .ok_or("uncertain poll lease missing")?;
        provider_store
            .record_observation(
                &lease,
                &ProviderTaskObservation {
                    event_identity: "async-image-uncertain".into(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Uncertain {
                        error_code: "fixture_outcome_unknown".into(),
                    },
                    progress: None,
                },
            )
            .await
            .map_err(debug_error)?;
        let reductions = PostgresExecutorTerminalStore::new(pool.clone());
        let terminal = reductions
            .claim_terminal("async-image-uncertain-reducer", 30_000)
            .await
            .map_err(debug_error)?
            .ok_or("uncertain reduction missing")?;
        reductions
            .complete_terminal(&terminal, None)
            .await
            .map_err(debug_error)?;
        let before = image_read_snapshot(pool, job).await?;
        let unknown = image_request(
            &app,
            Method::GET,
            &path,
            &reader.api_key.value,
            None,
            None,
            false,
        )
        .await?;
        require(
            unknown.0 == StatusCode::OK
                && unknown.2["id"] == task_id
                && unknown.2["status"] == "uncertain"
                && unknown.2["error"]["code"] == "generation_uncertain"
                && unknown.2.get("provider_progress").is_none()
                && unknown.2.get("data").is_none(),
            format!("uncertain image became a failure or retained old progress: {unknown:?}"),
        )?;
        let lookup = image_request(
            &app,
            Method::GET,
            LOOKUP,
            &reader.api_key.value,
            Some(KEY),
            None,
            false,
        )
        .await?;
        require(
            lookup.2 == unknown.2,
            "uncertain key lookup changed original task",
        )?;
        let replay = image_request(
            &app,
            Method::POST,
            IMAGES,
            &owner.api_key.value,
            Some(KEY),
            Some(&body),
            true,
        )
        .await?;
        require(
            replay.0 == StatusCode::ACCEPTED && replay.2 == unknown.2,
            "uncertain replay replaced or waited for original task",
        )?;
        require(
            image_read_snapshot(pool, job).await? == before,
            "uncertain read/replay changed task/billing",
        )?;
        let hold: (String, i64, i64) = sqlx::query_as("SELECT state, captured_micros, released_micros FROM customer_billing_holds WHERE job_id=$1")
            .bind(job).fetch_one(pool).await.map_err(debug_error)?;
        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM jobs),
            (SELECT count(*) FROM quota_reservations), (SELECT count(*) FROM provider_submissions)",
        )
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
        return require(
            hold == ("held".into(), 0, 0)
                && counts == (1, 1, 1)
                && fs::read_to_string(home.join("submit-calls")).map_err(debug_error)?
                    == "submit\n"
                && !home.join("poll-calls").exists(),
            format!("uncertain image released hold or repeated provider CLI: {hold:?} {counts:?}"),
        );
    }
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let run = Box::pin(poll.run_once()).await.map_err(debug_error)?;
            if !matches!(run, ProviderPollRun::Idle) {
                return Ok::<_, String>(run);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(debug_error)??;
    require(
        matches!(observed, ProviderPollRun::Observed(ref task) if task.state == ProviderTaskState::ArtifactReady),
        format!("image fixture did not publish real artifact: {observed:?}"),
    )?;
    let reductions = PostgresExecutorTerminalStore::new(pool.clone());
    let terminal = reductions
        .claim_terminal("async-image-reducer", 30_000)
        .await
        .map_err(debug_error)?
        .ok_or("no image terminal reduction")?;
    let customer = CustomerArtifactPublisher::new(blobs.clone())
        .publish(&terminal)
        .await
        .map_err(debug_error)?;
    reductions
        .complete_terminal(&terminal, Some(&customer))
        .await
        .map_err(debug_error)?;
    let before = image_read_snapshot(pool, job).await?;
    let done = image_request(
        &app,
        Method::GET,
        &path,
        &reader.api_key.value,
        None,
        None,
        false,
    )
    .await?;
    require(
        done.0 == StatusCode::OK
            && done.2["status"] == "succeeded"
            && done.2["data"][0]["b64_json"] == STANDARD.encode(&png)
            && done.2["created"].is_i64()
            && done.2.get("provider_progress").is_none()
            && done.2.get("stage").is_none(),
        format!("successful image GET lost original artifact: {done:?}"),
    )?;
    let found = image_request(
        &app,
        Method::GET,
        LOOKUP,
        &reader.api_key.value,
        Some(KEY),
        None,
        false,
    )
    .await?;
    require(
        found.2 == done.2,
        "completed key lookup differs from original ID GET",
    )?;
    let replay = image_request(
        &app,
        Method::POST,
        IMAGES,
        &owner.api_key.value,
        Some(KEY),
        Some(&body),
        true,
    )
    .await?;
    require(
        replay.0 == StatusCode::ACCEPTED && replay.2 == done.2,
        "completed async POST waited or replaced the task",
    )?;
    // Existing default synchronous replay keeps the original images response shape.
    let sync = image_request(
        &app,
        Method::POST,
        IMAGES,
        &owner.api_key.value,
        Some(KEY),
        Some(&body),
        false,
    )
    .await?;
    require(
        sync.0 == StatusCode::OK
            && sync.1.is_none()
            && sync.2.get("id").is_none()
            && sync.2.get("status").is_none()
            && sync.2["data"] == done.2["data"]
            && sync.2["created"] == done.2["created"],
        format!("default sync image contract changed: {sync:?}"),
    )?;
    require(
        image_read_snapshot(pool, job).await? == before,
        "completed reads/replays modified task or billing",
    )?;
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM jobs),
        (SELECT count(*) FROM quota_reservations), (SELECT count(*) FROM provider_submissions)",
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        counts == (1, 1, 1)
            && fs::read_to_string(home.join("submit-calls")).map_err(debug_error)? == "submit\n"
            && fs::read_to_string(home.join("poll-calls")).map_err(debug_error)? == "poll\n",
        format!("GET/replay repeated a submission: {counts:?}"),
    )?;
    sqlx::query(
        "UPDATE job_artifact_retention SET state='expired',
        expired_at_ms=(extract(epoch FROM clock_timestamp())*1000)::BIGINT,
        purge_after_ms=(extract(epoch FROM clock_timestamp())*1000)::BIGINT,
        updated_at_ms=(extract(epoch FROM clock_timestamp())*1000)::BIGINT WHERE job_id=$1",
    )
    .bind(job)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let expired = image_request(
        &app,
        Method::GET,
        &path,
        &reader.api_key.value,
        None,
        None,
        false,
    )
    .await?;
    require(
        expired.0 == StatusCode::OK
            && expired.2["status"] == "expired"
            && expired.2["error"]["code"] == "artifact_expired"
            && expired.2.get("data").is_none()
            && expired.2.get("provider_progress").is_none(),
        format!("expired task contract changed: {expired:?}"),
    )
}

async fn image_request(
    app: &axum::Router,
    method: Method,
    uri: &str,
    token: &str,
    key: Option<&str>,
    body: Option<&Value>,
    asynchronous: bool,
) -> TestResult<(StatusCode, Option<String>, Value)> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(key) = key {
        builder = builder.header("idempotency-key", key);
    }
    if asynchronous {
        builder = builder.header("prefer", "respond-async");
    }
    let body = match body {
        Some(body) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(body.to_string())
        }
        None => Body::empty(),
    };
    let response = app
        .clone()
        .oneshot(builder.body(body).map_err(debug_error)?)
        .await
        .map_err(debug_error)?;
    let status = response.status();
    let location = response
        .headers()
        .get(header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(debug_error)?;
    Ok((
        status,
        location,
        serde_json::from_slice(&bytes).map_err(debug_error)?,
    ))
}

async fn image_read_snapshot(pool: &PgPool, job: Uuid) -> TestResult<Value> {
    sqlx::query_scalar("SELECT jsonb_build_object(
        'job', (SELECT to_jsonb(j) FROM jobs j WHERE job_id=$1),
        'work', (SELECT to_jsonb(w) FROM work_items w WHERE job_id=$1),
        'quota', (SELECT to_jsonb(q) FROM quota_reservations q WHERE job_id=$1),
        'hold', (SELECT jsonb_agg(to_jsonb(h)) FROM customer_billing_holds h WHERE job_id=$1),
        'tasks', (SELECT jsonb_agg(to_jsonb(task)) FROM provider_remote_tasks task JOIN provider_submissions s USING(submission_id) WHERE s.job_id=$1),
        'observations', (SELECT jsonb_agg(to_jsonb(observation) ORDER BY observed_at_ms, observation_id)
            FROM provider_task_observations observation JOIN provider_submissions s USING(submission_id) WHERE s.job_id=$1))")
        .bind(job).fetch_one(pool).await.map_err(debug_error)
}
