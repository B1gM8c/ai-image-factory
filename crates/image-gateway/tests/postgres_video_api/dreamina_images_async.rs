use super::*;
use gpt_image_2_gateway::{
    ProviderPollDriver, ProviderPollDriverCall, ProviderTaskStore,
    dreamina_canvas::{
        CANVAS_ADAPTER_REVISION, CANVAS_IMAGE_MODEL, CANVAS_PROVIDER_ID,
        CANVAS_SUBMIT_COMMAND_SCHEMA, CanvasImagePlan,
    },
    executor::{ExecutionProfileProvisioning, provision_dreamina_canvas_execution_profile},
    provider_management::{PostgresProviderManagementService, ProviderManagementService},
    provider_tasks::{
        ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskObservationSource,
        RemoteTaskAttach, RemoteTaskSubmitReceipt, RemoteTaskSubmitReservation,
    },
    provision_dreamina_execution_profile,
};
use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, CanonicalCommandPayload, Completed, EffectCertainty,
    OutputSlot, PollObservation, ProviderFailure, ProviderFailureClass, ProviderTaskProgress,
    RetryDirective, SingleOutputCommand,
};

const IMAGES: &str = "/v1/dreamina/images/generations";
const LOOKUP: &str = "/v1/dreamina/images/by-idempotency-key";
const KEY: &str = "async-image-original-id";

#[tokio::test]
async fn dreamina_canvas_routes_55_and_admits_only_one_job_and_hold_per_key() -> TestResult {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = Box::pin(run_canvas_image(&database.pool)).await;
    combine(result, database.cleanup().await)
}

async fn run_canvas_image(pool: &PgPool) -> TestResult {
    let root = TempDir::new().map_err(debug_error)?;
    let home = root.path().join("canvas");
    let artifacts = root.path().join("artifacts");
    for path in [&home, &artifacts] {
        fs::create_dir(path).map_err(debug_error)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
    }
    let provisioned = provision_dreamina_canvas_execution_profile(
        pool,
        &ExecutionProfileProvisioning {
            profile_key: "canvas-async-image-fixture".into(),
            credential_pool_key: "canvas-async-image-fixture".into(),
            provider_account_key: "canvas-async-image-fixture".into(),
            credential_ref: "managed.canvas-async-image-fixture.1".into(),
            credential_revision: 1,
            credential_auth_sha256: "c".repeat(64),
            max_concurrency: 1,
        },
        &home,
    )
    .await
    .map_err(debug_error)?;
    let provider_store = PostgresProviderTaskStore::new(pool.clone());
    // Register readiness locally. No executor or provider process is started.
    for role in [ProviderRuntimeRole::Submit, ProviderRuntimeRole::Poll] {
        provider_store
            .register_runtime(
                &ProviderRuntimeRegistration {
                    runtime_id: Uuid::new_v4(),
                    execution_profile_id: provisioned.execution_profile_id,
                    role,
                    runtime_owner: format!("canvas-async-{role:?}"),
                },
                60_000,
            )
            .await
            .map_err(debug_error)?;
    }
    let management = PostgresProviderManagementService::new(
        pool.clone(),
        root.path().into(),
        "/nonexistent/test-cli".into(),
    );
    let route = management
        .create_route(
            serde_json::from_value(json!({
                "route_key": "canvas-async-image-fixture",
                "display_name": "Canvas asynchronous image fixture",
                "provider_id": CANVAS_PROVIDER_ID,
                "operation_id": "images.generations",
                "members": [{ "provider_account_id": provisioned.provider_account_id }],
            }))
            .map_err(debug_error)?,
        )
        .await
        .map_err(debug_error)?;
    platform_binding::publish_fixture_route(pool, &home, route.route_id).await?;

    let keys = Arc::new(PostgresApiKeyStore::new(
        pool.clone(),
        ApiKeyKeyring::new(1, [(1, vec![0x48; 32])]).map_err(debug_error)?,
    ));
    let project = keys
        .create_project("Canvas image fixture")
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
    let model = FixtureModel {
        api_profile: "dreamina-cli-images-v1",
        public_model: "5.5",
        provider_model: "5.5",
        execution_model: CANVAS_IMAGE_MODEL,
        media_kind: "image",
        workflow: None,
        rate_micros: 1000,
    };
    seed_provider_model_economics(pool, &project.id, CANVAS_PROVIDER_ID, model).await?;
    let blobs = Arc::new(FilesystemArtifactBlobStore::new(&artifacts).map_err(debug_error)?);
    let app = build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing(
        config(), ExternalImageGatewayComponents {
            usage_store: Arc::new(PostgresUsageStore::new(pool.clone())), api_key_store: keys,
            admission_store: Arc::new(PostgresAdmissionStore::new(pool.clone())),
            settlement_store: Arc::new(PostgresExecutionSettlementStore::new(pool.clone(), blobs.clone())),
            input_blob_store: blobs.clone(),
            provider_readiness_store: Arc::new(provider_store.clone()),
        }, None, None, None, None, Some(Arc::new(PostgresModelRoutingStore::new(pool.clone()))),
    ).map_err(debug_error)?;
    let models = image_request(
        &app,
        Method::GET,
        "/v1/models",
        &reader.api_key.value,
        None,
        None,
        false,
    )
    .await?;
    let surface = json!({
        "provider_id": "dreamina-cli",
        "provider_model_id": "5.5",
        "api_profile": "dreamina-cli-images-v1",
        "operation_id": "images.generations",
        "media_kind": "image",
        "workflows": ["text2image"],
    });
    require(
        models.0 == StatusCode::OK
            && models.2["data"].as_array().is_some_and(|entries| {
                entries.iter().any(|entry| {
                    entry["id"] == "5.5" && entry["metadata"]["surfaces"] == json!([surface])
                })
            }),
        format!("5.5 exposed an incompatible model catalog surface: {models:?}"),
    )?;

    let body = json!({
        "prompt": "A paper boat in a rain puddle",
        "model_version": "5.5",
        "ratio": "9:16",
        "resolution_type": "1.5k",
        "generate_num": 1,
    });
    for (field, value, expected_code) in [
        ("generate_num", json!(2), "unsupported_parameter"),
        ("resolution_type", json!("2k"), "unsupported_parameter"),
        ("width", json!(1536), "unsupported_parameter"),
        ("ratio", json!("9:20"), "invalid_value"),
    ] {
        let mut invalid = body.clone();
        invalid[field] = value;
        let rejected = image_request(
            &app,
            Method::POST,
            IMAGES,
            &owner.api_key.value,
            Some(field),
            Some(&invalid),
            true,
        )
        .await?;
        require(
            rejected.0 == StatusCode::BAD_REQUEST && rejected.2["error"]["code"] == expected_code,
            format!("unsupported 5.5 shape was not rejected: {rejected:?}"),
        )?;
    }
    let counts = || async {
        sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM customer_billing_holds),
                (SELECT count(*) FROM provider_submissions)",
        )
        .fetch_one(pool)
        .await
        .map_err(debug_error)
    };
    require(
        counts().await? == (0, 0, 0),
        "invalid Canvas request created work or a hold",
    )?;

    let submit = || {
        image_request(
            &app,
            Method::POST,
            IMAGES,
            &owner.api_key.value,
            Some(KEY),
            Some(&body),
            true,
        )
    };
    let (first, second) = tokio::join!(submit(), submit());
    let (first, second) = (first?, second?);
    let (created, competing) = if second.0 == StatusCode::ACCEPTED {
        (second, first)
    } else {
        (first, second)
    };
    require(
        created.0 == StatusCode::ACCEPTED && created.2["status"] == "running",
        format!("Canvas did not return an async task: {created:?}, {competing:?}"),
    )?;
    require(
        competing == created
            || (competing.0 == StatusCode::CONFLICT
                && competing.2["error"]["code"] == "idempotency_in_progress"),
        format!("concurrent Canvas submission changed identity or failed: {competing:?}"),
    )?;
    let job_id = created.2["id"]
        .as_str()
        .ok_or("Canvas response omitted task ID")?;
    let job = Uuid::parse_str(job_id).map_err(debug_error)?;
    let path = format!("/v1/dreamina/images/{job_id}");
    require(
        created.1.as_deref() == Some(path.as_str())
            && created.2["request"]["model_version"] == "5.5"
            && created.2["request"]["workflow"] == "text2image"
            && created.2["request"]["ratio"] == "9:16"
            && created.2["request"]["resolution_type"] == "1.5k"
            && created.2["request"]["generate_num"] == 1,
        format!("Canvas async task omitted compatible request metadata: {created:?}"),
    )?;
    require(
        submit().await? == created,
        "Canvas replay changed the original task",
    )?;
    for lookup in [&path, LOOKUP] {
        let found = image_request(
            &app,
            Method::GET,
            lookup,
            &reader.api_key.value,
            Some(KEY),
            None,
            false,
        )
        .await?;
        require(
            found.0 == StatusCode::OK && found.2 == created.2,
            format!("Canvas lookup did not recover the original task: {found:?}"),
        )?;
    }
    let pinned: (String, String, String, String, String, i64, i64) = sqlx::query_as(
        "SELECT attribution.provider_id, payload.command_schema, quote.api_profile,
             quote.provider_model_id, quote.public_model_id, account.held_micros, account.captured_micros
         FROM job_payloads payload
         JOIN job_provider_route_attributions attribution USING (job_id)
         JOIN customer_price_quotes quote USING (job_id)
         JOIN billing_accounts account ON account.tenant_id = quote.tenant_id
         WHERE payload.job_id = $1",
    )
    .bind(job)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        pinned
            == (
                CANVAS_PROVIDER_ID.to_owned(),
                CANVAS_SUBMIT_COMMAND_SCHEMA.to_owned(),
                model.api_profile.to_owned(),
                model.provider_model.to_owned(),
                model.public_model.to_owned(),
                model.rate_micros,
                0,
            )
            && counts().await? == (1, 1, 0),
        format!(
            "Canvas replay created duplicate work or changed the original route and hold: {pinned:?}"
        ),
    )?;

    let png = settle_canvas_fixture(
        pool,
        provider_store,
        provisioned.execution_profile_id,
        job,
        blobs,
    )
    .await?;
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
    let replay = submit().await?;
    let billed: (i64, i64) = sqlx::query_as(
        "SELECT held_micros, captured_micros FROM billing_accounts WHERE tenant_id = $1",
    )
    .bind(&project.id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        done.0 == StatusCode::OK
            && done == found
            && done.2["id"] == job_id
            && done.2["status"] == "succeeded"
            && done.2["data"][0]["b64_json"] == STANDARD.encode(png)
            && done.2["output_counts"] == json!({"requested":1,"delivered":1,"failed":0})
            && replay.0 == StatusCode::ACCEPTED
            && replay.2 == done.2
            && billed == (0, model.rate_micros)
            && counts().await? == (1, 1, 1),
        format!(
            "Canvas artifact, original identity or one-time settlement was lost: {done:?} {billed:?}"
        ),
    )
}

struct CanvasFixturePayload(String, CanvasImagePlan);

impl CanonicalCommandPayload for CanvasFixturePayload {
    const SCHEMA_ID: &'static str = CANVAS_SUBMIT_COMMAND_SCHEMA;
    const ADAPTER_REVISION: &'static str = CANVAS_ADAPTER_REVISION;

    fn source_command_sha256(&self) -> &str {
        &self.0
    }

    fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
        serde_json::to_vec(&self.1).expect("Canvas fixture plan serializes")
    }
}

struct CanvasFixturePoll {
    remote_id: String,
    png: Vec<u8>,
}

impl ProviderPollDriver for CanvasFixturePoll {
    fn provider_id(&self) -> &'static str {
        CANVAS_PROVIDER_ID
    }

    async fn poll<S: ArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        assert_eq!(call.operation().operation_id(), self.remote_id);
        let artifact_failure = |_| {
            ProviderFailure::new(
                ProviderFailureClass::ArtifactInvalid,
                "offline_canvas_fixture_artifact",
                EffectCertainty::NoRemoteEffect,
                RetryDirective::Never,
            )
            .expect("fixture failure code is valid")
        };
        sink.write_chunk(&self.png)
            .await
            .map_err(artifact_failure)?;
        let artifact = sink
            .finalize(ArtifactMetadata {
                media_type: "image/png",
            })
            .await
            .map_err(artifact_failure)?;
        Ok(PollObservation::Completed(Completed::new(artifact, None)))
    }
}

async fn settle_canvas_fixture(
    pool: &PgPool,
    tasks: PostgresProviderTaskStore,
    profile_id: Uuid,
    job: Uuid,
    blobs: Arc<FilesystemArtifactBlobStore>,
) -> TestResult<Vec<u8>> {
    let executor = Arc::new(PostgresExecutorSubmissionStore::new(pool.clone()));
    let worker = Workerd::new_handoff_only_with_contract(
        "canvas-fixture-worker".into(),
        Arc::new(PostgresAdmissionStore::new(pool.clone())),
        Arc::new(PostgresExecutionContextStore::new(pool.clone())),
        executor.clone(),
        profile_id,
        CANVAS_SUBMIT_COMMAND_SCHEMA.into(),
        Duration::from_secs(30),
        AdmissionContract::CustomerPricingV4,
    )
    .map_err(debug_error)?;
    require(
        worker.run_once().await.map_err(debug_error)? == Some(job),
        "Canvas work was not handed off",
    )?;
    let lease = executor
        .claim_prepared(
            &ExecutorClaimScope {
                execution_profile_id: profile_id,
                provider_id: CANVAS_PROVIDER_ID.into(),
                command_schema: CANVAS_SUBMIT_COMMAND_SCHEMA.into(),
                adapter_revision: CANVAS_ADAPTER_REVISION.into(),
            },
            "canvas-fixture-submit",
            30_000,
        )
        .await
        .map_err(debug_error)?
        .ok_or("Canvas submission was not claimable")?;
    executor.start(&lease).await.map_err(debug_error)?;
    let command: Value =
        sqlx::query_scalar("SELECT command_json FROM job_payloads WHERE job_id = $1")
            .bind(job)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).map_err(debug_error)?,
        CanvasFixturePayload(
            lease.command_hash.clone(),
            serde_json::from_value(command).map_err(debug_error)?,
        ),
    )
    .map_err(debug_error)?;
    let reservation = RemoteTaskSubmitReservation::new(
        &lease,
        format!("offline-canvas-{}", lease.submission_id),
        command.output(),
        command.identity(),
        30_000,
    );
    let intent = tasks
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    tasks
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    // Fixture supplies the acceptance receipt and media. The real stores enforce all fences and accounting.
    let remote_id = format!("{}:{}", Uuid::new_v4(), lease.submission_id);
    tasks
        .record_submit_receipt(&RemoteTaskSubmitReceipt {
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            executor_owner: lease.executor_owner.clone(),
            executor_lease_epoch: lease.executor_lease_epoch,
            remote_operation_id: remote_id.clone(),
            provider_request_id: None,
            event_identity: "canvas-fixture-receipt".into(),
            execution_binding_sha256: intent.execution_binding_sha256.clone(),
        })
        .await
        .map_err(debug_error)?;
    tasks
        .attach(&RemoteTaskAttach {
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            executor_owner: lease.executor_owner,
            executor_lease_epoch: lease.executor_lease_epoch,
            remote_operation_id: remote_id.clone(),
            provider_request_id: None,
            event_identity: "canvas-fixture-attach".into(),
            execution_binding_sha256: intent.execution_binding_sha256,
            poll_after_ms: 0,
            recovery_fence: None,
        })
        .await
        .map_err(debug_error)?;
    let profile = tasks
        .load_active_runtime_profile("canvas-async-image-fixture")
        .await
        .map_err(debug_error)?;
    let mut png = Cursor::new(Vec::new());
    ImageBuffer::from_pixel(16, 24, Rgba([30_u8, 100, 200, 255]))
        .write_to(&mut png, ImageFormat::Png)
        .map_err(debug_error)?;
    let png = png.into_inner();
    let poll = ProviderPollOrchestrator::new(
        tasks,
        CanvasFixturePoll {
            remote_id,
            png: png.clone(),
        },
        FilesystemProviderArtifactStagerFactory::new(blobs.clone(), 1024 * 1024)
            .map_err(debug_error)?,
        ProviderPollOrchestratorConfig {
            scope: profile.claim_scope(),
            owner: "canvas-fixture-poll".into(),
            lease_ms: 30_000,
            heartbeat_interval: Duration::from_secs(1),
            max_materializations: 1,
        },
    )
    .map_err(debug_error)?;
    let result = Box::pin(poll.run_once()).await.map_err(debug_error)?;
    require(
        matches!(&result, ProviderPollRun::Observed(task) if task.state == ProviderTaskState::ArtifactReady),
        format!("Canvas poll did not publish an artifact: {result:?}"),
    )?;

    let reductions = PostgresExecutorTerminalStore::new(pool.clone());
    let terminal = reductions
        .claim_terminal("canvas-fixture-reducer", 30_000)
        .await
        .map_err(debug_error)?
        .ok_or("Canvas reduction not queued")?;
    let artifact = CustomerArtifactPublisher::new(blobs)
        .publish(&terminal)
        .await
        .map_err(debug_error)?;
    let completion = reductions
        .complete_terminal(&terminal, Some(&artifact))
        .await
        .map_err(debug_error)?;
    let replay = reductions
        .complete_terminal(&terminal, Some(&artifact))
        .await
        .map_err(debug_error)?;
    require(completion == replay, "Canvas settlement changed on replay")?;
    Ok(png)
}

#[tokio::test]
async fn dreamina_async_images_keep_original_id_progress_scope_and_artifact() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = Box::pin(run_async_image(&database.pool, true, 1)).await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn dreamina_async_uncertain_images_keep_the_hold_and_original_id() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = Box::pin(run_async_image(&database.pool, false, 1)).await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn dreamina_async_partial_native_images_keep_original_count_and_settled_delivery()
-> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = Box::pin(run_async_image(&database.pool, true, 3)).await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn dreamina_async_uncertain_native_batch_never_publishes_partial_counts_or_retries()
-> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = Box::pin(run_async_image(&database.pool, false, 3)).await;
    combine(result, database.cleanup().await)
}

async fn run_async_image(pool: &PgPool, succeeds: bool, requested: u32) -> TestResult {
    let delivered = if requested == 1 { 1 } else { 2 };

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
    if requested > 1 {
        fs::write(home.join("native-batch"), b"3").map_err(debug_error)?;
        let mut encoded = Cursor::new(Vec::new());
        ImageBuffer::from_pixel(32, 18, Rgba([200_u8, 100, 30, 255]))
            .write_to(&mut encoded, ImageFormat::Png)
            .map_err(debug_error)?;
        fs::write(home.join("result-2.png"), encoded.into_inner()).map_err(debug_error)?;
    }

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
    if [ -f "$HOME/native-batch" ]; then
      /bin/cp "$HOME/result.png" "$download/result-1.png"
      /bin/cp "$HOME/result-2.png" "$download/result-2.png"
    else
      /bin/cp "$HOME/result.png" "$download/result.png"
    fi
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
        "ratio":"16:9", "resolution_type":"2k", "generate_num":requested});
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
        created["request"]["generate_num"] == requested
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
    let generating = provider_store
        .claim_due(&profile.claim_scope(), "async-image-generating", 30_000)
        .await
        .map_err(debug_error)?
        .ok_or("generating poll lease missing")?;
    provider_store
        .record_observation(
            &generating,
            &ProviderTaskObservation {
                event_identity: "async-image-generating-progress".into(),
                source: ProviderTaskObservationSource::Poll,
                outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                progress: Some(ProviderTaskProgress {
                    provider_status: "Generating".into(),
                    queue_position: None,
                    queue_total: None,
                    estimated_remaining_seconds: Some(60),
                }),
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
                && response.2["provider_progress"]["stage"] == "generating"
                && response.2["provider_progress"]["progress_percent"] == 0
                && response.2["provider_progress"]["progress_source"] == "estimated"
                && response.2["provider_progress"]["estimated_remaining_seconds"] == 60,
            format!("image generation estimate missing or became a terminal outcome: {response:?}"),
        )?;
    }
    require(
        image_read_snapshot(pool, job).await? == before,
        "estimated progress read changed task or billing",
    )?;
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
        for index in 0..requested {
            let terminal = reductions
                .claim_terminal(&format!("async-image-uncertain-reducer-{index}"), 30_000)
                .await
                .map_err(debug_error)?
                .ok_or("uncertain reduction missing")?;
            reductions
                .complete_terminal(&terminal, None)
                .await
                .map_err(debug_error)?;
        }
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
                && unknown.2.get("data").is_none()
                && unknown.2.get("output_counts").is_none(),
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
                && counts == (1, 1, i64::from(requested))
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
    let publisher = CustomerArtifactPublisher::new(blobs.clone());
    for index in 0..requested {
        let terminal = reductions
            .claim_terminal(&format!("async-image-reducer-{index}"), 30_000)
            .await
            .map_err(debug_error)?
            .ok_or("no image terminal reduction")?;
        let customer = if matches!(&terminal.outcome, CanonicalExecutorOutcome::Succeeded(_)) {
            Some(publisher.publish(&terminal).await.map_err(debug_error)?)
        } else {
            require(
                matches!(&terminal.outcome, CanonicalExecutorOutcome::Failed { error_code }
                    if error_code == "dreamina_native_batch_result_missing"),
                "partial native image did not retain explicit missing-output failure",
            )?;
            None
        };
        reductions
            .complete_terminal(&terminal, customer.as_ref())
            .await
            .map_err(debug_error)?;
        if index + 1 < requested {
            let before = image_read_snapshot(pool, job).await?;
            let incomplete = image_request(
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
                incomplete.0 == StatusCode::OK
                    && incomplete.2["status"] == "running"
                    && incomplete.2.get("data").is_none()
                    && incomplete.2.get("output_counts").is_none(),
                format!("unfinished native settlement exposed partial delivery: {incomplete:?}"),
            )?;
            require(
                image_read_snapshot(pool, job).await? == before,
                "unfinished native GET changed settlement or task state",
            )?;
        }
    }
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
            && done.2["data"].as_array().map(Vec::len) == Some(delivered as usize)
            && done.2["request"]["generate_num"] == requested
            && done.2["output_counts"]
                == json!({"requested":requested,"delivered":delivered,"failed":requested-delivered})
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
        counts == (1, 1, i64::from(requested))
            && fs::read_to_string(home.join("submit-calls")).map_err(debug_error)? == "submit\n"
            && fs::read_to_string(home.join("poll-calls")).map_err(debug_error)? == "poll\n",
        format!("GET/replay repeated a submission: {counts:?}"),
    )?;
    let money: (i64,i64,i32,i32,i64,i64,i64) = sqlx::query_as(
        "SELECT hold.captured_micros,hold.released_micros,quota.committed_units,quota.released_units,
            (SELECT count(*) FROM provider_remote_tasks task JOIN provider_submissions submission USING(submission_id) WHERE submission.job_id=$1),
            (SELECT count(*) FROM provider_remote_submit_intents intent JOIN provider_submissions submission USING(submission_id) WHERE submission.job_id=$1),
            (SELECT count(*) FROM customer_rated_usage WHERE job_id=$1)
         FROM customer_billing_holds hold JOIN quota_reservations quota USING(job_id) WHERE hold.job_id=$1")
        .bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        money
            == (
                i64::from(delivered) * 1000,
                i64::from(requested - delivered) * 1000,
                delivered as i32,
                (requested - delivered) as i32,
                1,
                1,
                1,
            ),
        format!("native asynchronous delivery changed per-image frozen settlement: {money:?}"),
    )?;
    if requested > 1 {
        let key: String = sqlx::query_scalar(
            "SELECT object_key FROM artifacts WHERE job_id=$1 AND output_index=0",
        )
        .bind(job)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
        let blob_path = artifact_root.join(key);
        let bytes = fs::read(&blob_path).map_err(debug_error)?;
        fs::remove_file(&blob_path).map_err(debug_error)?;
        let before = image_read_snapshot(pool, job).await?;
        let missing = image_request(
            &app,
            Method::GET,
            &path,
            &reader.api_key.value,
            None,
            None,
            false,
        )
        .await?;
        fs::write(&blob_path, &bytes).map_err(debug_error)?;
        fs::set_permissions(&blob_path, fs::Permissions::from_mode(0o600)).map_err(debug_error)?;
        require(
            missing.0.is_server_error()
                && missing.2.get("data").is_none()
                && missing.2.get("output_counts").is_none(),
            format!("missing native blob was accepted as a smaller delivery: {missing:?}"),
        )?;
        require(
            image_read_snapshot(pool, job).await? == before
                && fs::read_to_string(home.join("submit-calls")).map_err(debug_error)?
                    == "submit\n"
                && fs::read_to_string(home.join("poll-calls")).map_err(debug_error)? == "poll\n",
            "failed partial GET changed billing or invoked CLI",
        )?;
    }
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
        'usage_facts', (SELECT count(*) FROM provider_usage_facts WHERE job_id=$1),
        'rating', (SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),
        'ledger_transactions', (SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),
        'tasks', (SELECT jsonb_agg(to_jsonb(task)) FROM provider_remote_tasks task JOIN provider_submissions s USING(submission_id) WHERE s.job_id=$1),
        'observations', (SELECT jsonb_agg(to_jsonb(observation) ORDER BY observed_at_ms, observation_id)
            FROM provider_task_observations observation JOIN provider_submissions s USING(submission_id) WHERE s.job_id=$1))")
        .bind(job).fetch_one(pool).await.map_err(debug_error)
}
