use super::*;
use gpt_image_2_gateway::provision_dreamina_execution_profile;

const IMAGES: &str = "/v1/dreamina/images/generations";
const VIDEOS: &str = "/v1/dreamina/videos/generations";

#[derive(Clone, Copy, Debug)]
enum Case {
    ImageToVideo,
    ArkFrames,
    ImageBatch,
    Upscale,
    Multiframe,
    Multimodal,
}

impl Case {
    fn model(self) -> FixtureModel {
        let (profile, public, provider, execution, media, workflow) = match self {
            Self::ImageToVideo => (
                "dreamina-cli-videos-v1",
                "seedance1.5pro",
                "seedance1.5pro",
                "seedance1.5pro",
                "video",
                "image2video",
            ),
            Self::Multimodal => (
                "dreamina-cli-videos-v1",
                "seedance2.5",
                "seedance2.5",
                "seedance2.5",
                "video",
                "multimodal2video",
            ),
            Self::ArkFrames => (
                "volcengine-ark-content-generation-v3",
                MODEL,
                "seedance2.5",
                "seedance2.5",
                "video",
                "frames2video",
            ),
            Self::ImageBatch => (
                "dreamina-cli-images-v1",
                "5.0",
                "5.0",
                "dreamina-image-5.0",
                "image",
                "image2image",
            ),
            Self::Upscale => (
                "dreamina-cli-images-v1",
                "image_upscale",
                "image_upscale",
                "dreamina-image-image_upscale",
                "image",
                "image_upscale",
            ),
            Self::Multiframe => (
                "dreamina-cli-videos-v1",
                "multiframe2video",
                "multiframe2video",
                "multiframe2video",
                "video",
                "multiframe2video",
            ),
        };
        FixtureModel {
            api_profile: profile,
            public_model: public,
            provider_model: provider,
            execution_model: execution,
            media_kind: media,
            workflow: Some(workflow),
            rate_micros: 1000,
        }
    }
    fn is_image(self) -> bool {
        self.model().media_kind == "image"
    }
    fn count(self) -> usize {
        if matches!(self, Self::ImageBatch) {
            2
        } else {
            1
        }
    }
    fn inputs(self) -> usize {
        if matches!(self, Self::ImageToVideo | Self::Upscale) {
            1
        } else {
            2
        }
    }
    fn path(self) -> &'static str {
        if self.is_image() {
            IMAGES
        } else if matches!(self, Self::ArkFrames) {
            TASKS
        } else {
            VIDEOS
        }
    }
    fn duration(self) -> Value {
        if matches!(self, Self::Multiframe) {
            json!(2.501)
        } else {
            json!(5)
        }
    }
    fn quota(self) -> i32 {
        match self {
            Self::ImageBatch => 2,
            Self::Multiframe => 3,
            Self::Upscale => 1,
            Self::ImageToVideo | Self::ArkFrames | Self::Multimodal => 5,
        }
    }
    fn captured(self) -> i64 {
        if matches!(self, Self::Multiframe) {
            2501
        } else {
            i64::from(self.quota()) * 1000
        }
    }
    fn body(self, first: &str, last: &str) -> Value {
        match self {
            Self::ImageToVideo => {
                json!({"workflow":"image2video", "image":first, "prompt":"slow camera push", "model_version":"seedance1.5pro", "ratio":"16:9", "duration":5, "video_resolution":"720p", "session":0, "poll":0})
            }
            Self::Multimodal => {
                json!({"workflow":"multimodal2video","image":[first,last],"prompt":"slow camera push","model_version":"seedance2.5","ratio":"16:9","duration":5,"video_resolution":"480p"})
            }
            // Reverse HTTP order intentionally; canonical input staging must follow roles.
            Self::ArkFrames => json!({"model":MODEL, "content":[
                {"type":"image_url","role":"last_frame","image_url":{"url":last}},
                {"type":"text","text":"slow camera push"},
                {"type":"image_url","role":"first_frame","image_url":{"url":first}}],
                "duration":5,"resolution":"480p","ratio":"adaptive"}),
            Self::ImageBatch => {
                json!({"workflow":"image2image","images":[first,last],"prompt":"orange paper planes","model_version":"5.0","ratio":"16:9","resolution_type":"2k","generate_num":2})
            }
            // This route test verifies 8k parameter forwarding, not synthetic output dimensions.
            Self::Upscale => {
                json!({"workflow":"image_upscale","image":first,"resolution_type":"8k"})
            }
            Self::Multiframe => {
                json!({"workflow":"multiframe2video","images":[first,last],"video_resolution":"720p","transitions":[{"prompt":"slow camera push","duration":2.501}]})
            }
        }
    }
}

struct WorkflowStaging {
    image: bool,
    inner: FilesystemProviderArtifactStagerFactory,
}
impl ProviderArtifactStagerFactory for WorkflowStaging {
    type Stager =
        <FilesystemProviderArtifactStagerFactory as ProviderArtifactStagerFactory>::Stager;
    async fn begin(
        &self,
        context: &ProviderArtifactStageContext,
    ) -> Result<Self::Stager, ArtifactSinkError> {
        assert!(self.image, "remote video must not open an artifact sink");
        self.inner.begin(context).await
    }
}

#[tokio::test]
async fn native_and_ark_reference_workflows_preserve_inputs_native_batch_and_exact_billing()
-> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    for case in [
        Case::ImageToVideo,
        Case::ArkFrames,
        Case::ImageBatch,
        Case::Upscale,
        Case::Multiframe,
        Case::Multimodal,
    ] {
        let Some(database) = TestDatabase::new().await? else {
            return Ok(());
        };
        let result = Box::pin(run_workflow(&database.pool, case))
            .await
            .map_err(|error| format!("{case:?}: {error}"));
        combine(result, database.cleanup().await)?;
    }
    Ok(())
}

fn png(color: [u8; 4]) -> TestResult<Vec<u8>> {
    let image = ImageBuffer::from_pixel(32, 18, Rgba(color));
    let mut encoded = Cursor::new(Vec::new());
    image
        .write_to(&mut encoded, ImageFormat::Png)
        .map_err(debug_error)?;
    Ok(encoded.into_inner())
}

async fn run_workflow(pool: &PgPool, case: Case) -> TestResult {
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
    let first_png = png([0, 30, 200, 255])?;
    let last_png = png([240, 100, 0, 255])?;
    fs::write(home.join("first.png"), &first_png).map_err(debug_error)?;
    fs::write(home.join("last.png"), &last_png).map_err(debug_error)?;
    fs::write(home.join("kind"), case.model().media_kind).map_err(debug_error)?;
    fs::write(home.join("output-count"), case.count().to_string()).map_err(debug_error)?;
    fs::write(
        home.join("video.json"),
        serde_json::to_vec(&json!({
            "gen_status":"success", "result_json":{"videos":[{"video_url":VIDEO_URL}]}
        }))
        .map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    let executable = root.path().join("dreamina-workflow-fixture");
    // Inputs are compared inside the gated process: a successful submission proves
    // the immutable objects reached the private cwd with original bytes and order.
    let script = br#"#!/bin/sh
set -eu
operation=$1
shift
case "$operation" in
  image2video|frames2video|image2image|image_upscale|multiframe2video|multimodal2video)
    printf '%s\n' "$operation" "$@" >> "$HOME/submit-arguments"
    first= last= images= count=1
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --image) shift; if [ -z "$first" ]; then first=$1; else last=$1; fi ;;
        --first) shift; first=$1 ;;
        --last) shift; last=$1 ;;
        --images) shift; images=$1 ;;
        --generate_num) shift; count=$1 ;;
      esac
      shift
    done
    [ "$count" = "$(/bin/cat "$HOME/output-count")" ]
    if [ -n "$images" ]; then
      first=${images%%,*}
      last=${images#*,}
    fi
    case "$first" in */*|..|'') exit 91 ;; esac
    /usr/bin/cmp "$first" "$HOME/first.png"
    if [ -n "$last" ]; then
      case "$last" in */*|..|'') exit 92 ;; esac
      /usr/bin/cmp "$last" "$HOME/last.png"
    fi
    printf 'submit\n' >> "$HOME/submit-calls"
    id=$(/usr/bin/wc -l < "$HOME/submit-calls" | /usr/bin/tr -d ' ')
    printf '{"submit_id":"workflow-%s","gen_status":"querying"}' "$id"
    ;;
  query_result)
    printf '%s\n' "$@" >> "$HOME/poll-arguments"
    printf 'poll\n' >> "$HOME/poll-calls"
    submit= download=
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --submit_id) shift; submit=$1 ;;
        --download_dir) shift; download=$1 ;;
        --download_dir=*) download=${1#*=} ;;
      esac
      shift
    done
    case "$submit" in workflow-*) ;; *) exit 93 ;; esac
    if [ "$(/bin/cat "$HOME/kind")" = image ]; then
      [ -n "$download" ]
      if [ "$(/bin/cat "$HOME/output-count")" = 2 ]; then
        /bin/cp "$HOME/first.png" "$download/result-0.png"
        /bin/cp "$HOME/last.png" "$download/result-1.png"
      else
        /bin/cp "$HOME/first.png" "$download/result.png"
      fi
      printf '{"submit_id":"%s","gen_status":"success"}' "$submit"
    else
      [ -z "$download" ]
      printf '{"submit_id":"%s",' "$submit"
      /usr/bin/cut -c 2- "$HOME/video.json"
    fi
    ;;
  *) exit 2 ;;
esac
"#;
    fs::write(&executable, script).map_err(debug_error)?;
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).map_err(debug_error)?;
    let executable_sha: [u8; 32] = Sha256::digest(script).into();
    let provisioning = DreaminaExecutionProfileProvisioning {
        profile_key: "dreamina-workflow-fixture".into(),
        credential_pool_key: "dreamina-workflow-fixture".into(),
        provider_account_key: "dreamina-workflow-fixture".into(),
        credential_ref: "private:dreamina-workflow-fixture".into(),
        credential_revision: 1,
        credential_auth_sha256: "a".repeat(64),
        max_concurrency: 1,
    };
    let provisioned = if case.is_image() {
        provision_dreamina_execution_profile(pool, &provisioning).await
    } else {
        provision_dreamina_video_execution_profile(pool, &provisioning).await
    }
    .map_err(debug_error)?;
    let provider_store = PostgresProviderTaskStore::new(pool.clone());
    let profile = provider_store
        .load_active_runtime_profile("dreamina-workflow-fixture")
        .await
        .map_err(debug_error)?;
    let account_home = ProviderAccountHomeCapability::new(
        "dreamina-cli",
        provisioned.credential_pool_id,
        provisioned.provider_account_id,
        provisioning.credential_ref,
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
    .map_err(debug_error)?
    .with_input_blobs(blobs.clone());
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
        WorkflowStaging {
            image: case.is_image(),
            inner: FilesystemProviderArtifactStagerFactory::new(blobs.clone(), 1024 * 1024)
                .map_err(debug_error)?,
        },
        ProviderPollOrchestratorConfig {
            scope: profile.claim_scope(),
            owner: "dreamina-workflow-poll".into(),
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
                    runtime_owner: format!("workflow-{role:?}"),
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
        .create_project("Workflow HTTP fixture")
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
    seed_model_route(
        pool,
        &project.id,
        &owner.id,
        &owner.api_key.id,
        provisioned.provider_account_id,
        provisioned.execution_profile_id,
        &home,
        case.model(),
    )
    .await?;
    seed_model_economics(pool, &project.id, case.model()).await?;
    let app = build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing(
        config(),ExternalImageGatewayComponents {
            usage_store:Arc::new(PostgresUsageStore::new(pool.clone())),api_key_store:keys,
            admission_store:Arc::new(PostgresAdmissionStore::new(pool.clone())),
            settlement_store:Arc::new(PostgresExecutionSettlementStore::new(pool.clone(),blobs.clone())),
            input_blob_store:blobs.clone(),provider_readiness_store:Arc::new(provider_store),
        },None,None,None,None,Some(Arc::new(PostgresModelRoutingStore::new(pool.clone())))).map_err(debug_error)?;
    let first = format!("data:image/png;base64,{}", STANDARD.encode(&first_png));
    let last = format!("data:image/png;base64,{}", STANDARD.encode(&last_png));
    if matches!(case, Case::ArkFrames) {
        assert_invalid_requests(pool, &app, &owner.api_key.value, &first, &last, &home).await?;
    }
    let body = case.body(&first, &last);
    if matches!(case, Case::Multiframe) {
        let mut submillisecond = body.clone();
        submillisecond["transitions"][0]["duration"] = json!(2.0001);
        let rejected = json_request(
            app.clone(),
            Method::POST,
            VIDEOS,
            &owner.api_key.value,
            Some("submillisecond"),
            Some(&submillisecond),
        )
        .await?;
        require(
            rejected.0 == StatusCode::BAD_REQUEST,
            format!("submillisecond duration was not rejected: {rejected:?}"),
        )?;
        assert_no_admission(pool, &home).await?;
    }
    let pending_app = app.clone();
    let pending_token = owner.api_key.value.clone();
    let pending_body = body.clone();
    let response = tokio::spawn(async move {
        json_request(
            pending_app,
            Method::POST,
            case.path(),
            &pending_token,
            Some("workflow-idempotency"),
            Some(&pending_body),
        )
        .await
    });
    // Image creation waits for completion; advance the actual worker while that
    // HTTP request remains live. Invalid admissions are surfaced without a 30s wait.
    let job_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let job: Option<Uuid> =
                sqlx::query_scalar("SELECT job_id FROM jobs JOIN job_input_manifests USING(job_id) JOIN job_payloads USING(job_id) ORDER BY jobs.created_at_ms LIMIT 1")
                    .fetch_optional(pool)
                    .await
                    .map_err(debug_error)?;
            if let Some(job) = job {
                return Ok::<_, String>(job);
            }
            if response.is_finished() {
                return Err("HTTP returned without admitting a job".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(debug_error)?;
    let job_id = match job_id {
        Ok(id) => id,
        Err(error) => {
            return Err(format!(
                "{error}: {:?}",
                response.await.map_err(debug_error)?
            ));
        }
    };
    assert_frozen_inputs(pool, job_id, case, &first_png, &last_png).await?;
    let workerd = Workerd::new_handoff_only_with_contract(
        "workflow-workerd".into(),
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
        workerd.run_once().await.map_err(debug_error)? == Some(job_id),
        "HTTP task was not handed off",
    )?;
    let reductions = PostgresExecutorTerminalStore::new(pool.clone());
    let command = ProviderSubmitIterationCommand::new("workflow-submit", "claim-0", "defer-0")
        .map_err(debug_error)?;
    let submitted = Box::pin(submit.run_once(&command))
        .await
        .map_err(debug_error)?;
    require(
        submitted == ProviderSubmitRun::FreshSubmitted,
        format!("workflow submit: {submitted:?}"),
    )?;
    if matches!(case, Case::ImageBatch) {
        let command = ProviderSubmitIterationCommand::new(
            "workflow-submit",
            "claim-follower",
            "defer-follower",
        )
        .map_err(debug_error)?;
        require(
            Box::pin(submit.run_once(&command))
                .await
                .map_err(debug_error)?
                == ProviderSubmitRun::Idle,
            "native batch follower acquired independent submit authority",
        )?;
    }
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let run = Box::pin(poll.run_once()).await.map_err(debug_error)?;
            if !matches!(run, ProviderPollRun::Idle) {
                return Ok::<_, String>(run);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(debug_error)??;
    require(
        matches!(observed,ProviderPollRun::Observed(ref task) if task.state == if case.is_image() { ProviderTaskState::ArtifactReady } else { ProviderTaskState::RemoteReady }),
        format!("workflow poll: {observed:?}"),
    )?;
    for _ in 0..case.count() {
        let terminal = reductions
            .claim_terminal("workflow-reducer", 30_000)
            .await
            .map_err(debug_error)?
            .ok_or("no terminal reduction")?;
        let customer = if case.is_image() {
            Some(
                CustomerArtifactPublisher::new(blobs.clone())
                    .publish(&terminal)
                    .await
                    .map_err(debug_error)?,
            )
        } else {
            require(
                matches!(&terminal.outcome,CanonicalExecutorOutcome::SucceededRemoteVideo(remote)
                if remote.result.video_url()==VIDEO_URL && remote.result.expires_at_ms().is_none()),
                "remote URL lost",
            )?;
            None
        };
        let completion = reductions
            .complete_terminal(&terminal, customer.as_ref())
            .await
            .map_err(debug_error)?;
        require(
            completion
                == reductions
                    .complete_terminal(&terminal, customer.as_ref())
                    .await
                    .map_err(debug_error)?,
            "repeat reduction charged twice",
        )?;
    }
    let (status, created) = response.await.map_err(debug_error)??;
    require(
        status == StatusCode::OK,
        format!("HTTP create: {status} {created}"),
    )?;
    if case.is_image() {
        let data = created["data"]
            .as_array()
            .ok_or("image response missing data")?;
        require(
            data.len() == case.count(),
            format!("wrong output count: {created}"),
        )?;
        for (index, item) in data.iter().enumerate() {
            let expected = if matches!(case, Case::ImageBatch) && index == 1 {
                &last_png
            } else {
                &first_png
            };
            require(
                item["b64_json"] == STANDARD.encode(expected),
                format!("image output {index} bytes or order changed"),
            )?;
        }
    } else {
        let native_path = format!("/v1/dreamina/videos/{job_id}");
        let (status, native) = json_request(
            app.clone(),
            Method::GET,
            &native_path,
            &owner.api_key.value,
            None,
            None,
        )
        .await?;
        require(
            status == StatusCode::OK
                && native["content"]["video_url"] == VIDEO_URL
                && native["content"]["duration"] == case.duration(),
            format!("native exact result: {status} {native}"),
        )?;
        let (status, ark) = json_request(
            app.clone(),
            Method::GET,
            &format!("{TASKS}/cgt-{job_id}"),
            &owner.api_key.value,
            None,
            None,
        )
        .await?;
        let ratio = if matches!(case, Case::ImageToVideo | Case::Multimodal) {
            json!("16:9")
        } else {
            Value::Null
        };
        let resolution = if matches!(case, Case::ArkFrames | Case::Multimodal) {
            "480p"
        } else {
            "720p"
        };
        require(
            status == StatusCode::OK
                && ark["content"]["video_url"] == VIDEO_URL
                && ark["duration"] == case.duration()
                && ark["resolution"] == resolution
                && ark["ratio"] == ratio,
            format!("Ark frozen result metadata: {status} {ark}"),
        )?;
        assert_remote_storage(pool, job_id, &artifact_root).await?;
    }
    let replay = json_request(
        app.clone(),
        Method::POST,
        case.path(),
        &owner.api_key.value,
        Some("workflow-idempotency"),
        Some(&body),
    )
    .await?;
    require(
        replay == (status, created),
        format!("idempotent HTTP response changed: {replay:?}"),
    )?;
    let mut changed = body.clone();
    if matches!(case, Case::ArkFrames) {
        changed["content"][1]["text"] = json!("different");
    } else if matches!(case, Case::Upscale) {
        changed["resolution_type"] = json!("4k");
    } else if matches!(case, Case::Multiframe) {
        changed["transitions"][0]["prompt"] = json!("different");
    } else {
        changed["prompt"] = json!("different");
    }
    let conflict = json_request(
        app,
        Method::POST,
        case.path(),
        &owner.api_key.value,
        Some("workflow-idempotency"),
        Some(&changed),
    )
    .await?;
    require(
        conflict.0 == StatusCode::CONFLICT,
        format!("changed idempotent request accepted: {conflict:?}"),
    )?;
    let calls = fs::read_to_string(home.join("submit-calls")).map_err(debug_error)?;
    require(
        calls.lines().count() == 1,
        "workflow or replay submitted more than one CLI operation",
    )?;
    let polls = fs::read_to_string(home.join("poll-calls")).map_err(debug_error)?;
    require(
        polls.lines().count() == 1,
        "workflow downloaded more than once",
    )?;
    let args = fs::read_to_string(home.join("submit-arguments")).map_err(debug_error)?;
    require(
        args.matches("--poll=0\n").count() == 1,
        "poll ownership was not enforced",
    )?;
    if matches!(case, Case::ImageBatch) {
        require(
            args.matches("--generate_num\n2\n").count() == 1,
            "native image batch did not submit both outputs together",
        )?;
    }
    let remote: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM provider_remote_tasks task JOIN provider_submissions submission USING (submission_id) WHERE submission.job_id = $1), (SELECT COUNT(DISTINCT task.remote_operation_id) FROM provider_remote_tasks task JOIN provider_submissions submission USING (submission_id) WHERE submission.job_id = $1), (SELECT COUNT(*) FROM provider_remote_submit_intents intent JOIN provider_submissions submission USING (submission_id) WHERE submission.job_id = $1)",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        remote == (1, 1, 1),
        format!("workflow did not retain one real remote task and submit intent: {remote:?}"),
    )?;
    if matches!(case, Case::Upscale) {
        require(
            args.contains("--resolution_type\n8k\n"),
            "upscale resolution lost",
        )?;
    }
    if matches!(case, Case::Multiframe) {
        require(
            args.contains("--duration\n2.501\n"),
            "CLI duration was rounded",
        )?;
    }
    if matches!(case, Case::ArkFrames) {
        require(
            !args.contains("--ratio"),
            "adaptive ratio became a fixed ratio",
        )?;
    }
    assert_workflow_billing(pool, job_id, &project.id, case).await
}

async fn assert_frozen_inputs(
    pool: &PgPool,
    job: Uuid,
    case: Case,
    first: &[u8],
    last: &[u8],
) -> TestResult {
    let inputs:Vec<(i16,String,String,String)> = sqlx::query_as("SELECT input_index,role,media_type,sha256_hex FROM job_input_objects WHERE job_id=$1 ORDER BY input_index")
        .bind(job).fetch_all(pool).await.map_err(debug_error)?;
    let expected: Vec<_> = [first, last]
        .into_iter()
        .take(case.inputs())
        .enumerate()
        .map(|(i, bytes)| {
            (
                i as i16,
                "image".into(),
                "image/png".into(),
                hex::encode(Sha256::digest(bytes)),
            )
        })
        .collect();
    require(
        inputs == expected,
        format!("immutable input role/order/hash differs: {inputs:?}"),
    )?;
    let (schema,command):(String,Value)=sqlx::query_as("SELECT manifest.manifest_schema,payload.command_json FROM job_input_manifests manifest JOIN job_payloads payload USING(job_id) WHERE manifest.job_id=$1")
        .bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        schema == "dreamina-cli.inputs.v1"
            && command["operation"] == case.model().workflow.unwrap(),
        format!("frozen workflow missing: {schema} {command}"),
    )?;
    // The canonical descriptor carries server-probed dimensions, not HTTP metadata.
    fn collect(value: &Value, out: &mut Vec<(u64, u64)>) {
        if let Some(object) = value.as_object() {
            if let (Some(w), Some(h)) = (
                object.get("width").and_then(Value::as_u64),
                object.get("height").and_then(Value::as_u64),
            ) {
                out.push((w, h));
            }
            for child in object.values() {
                collect(child, out);
            }
        } else if let Some(array) = value.as_array() {
            for child in array {
                collect(child, out);
            }
        }
    }
    let mut dimensions = Vec::new();
    collect(&command, &mut dimensions);
    require(
        dimensions.iter().filter(|d| **d == (32, 18)).count() == case.inputs(),
        format!("input dimensions not frozen: {dimensions:?}"),
    )
}

async fn assert_workflow_billing(pool: &PgPool, job: Uuid, tenant: &str, case: Case) -> TestResult {
    let (count, quota): (i32, i32) =
        sqlx::query_as("SELECT output_count,billable_units FROM jobs WHERE job_id=$1")
            .bind(job)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        (count, quota) == (case.count() as i32, case.quota()),
        format!("job quota/count mismatch: {count}/{quota}"),
    )?;
    let (captured,held):(i64,i64)=sqlx::query_as("SELECT captured_micros,held_micros FROM billing_accounts WHERE tenant_id=$1 AND currency='USD'").bind(tenant).fetch_one(pool).await.map_err(debug_error)?;
    require(
        (captured, held) == (case.captured(), 0),
        format!("exact billing mismatch: captured={captured},held={held}"),
    )?;
    let charges:i64=sqlx::query_scalar("SELECT COUNT(*) FROM ledger_transactions WHERE source_job_id=$1 AND transaction_type='customer_job_charge'").bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        charges == 1,
        format!("expected one parent charge, found {charges}"),
    )?;
    if matches!(case, Case::ImageBatch) {
        let outputs: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM provider_receipts receipt JOIN provider_submissions submission USING (submission_id) WHERE submission.job_id = $1), (SELECT COALESCE(SUM(quantity), 0)::BIGINT FROM provider_usage_facts WHERE job_id = $1 AND metric = 'image_output' AND terminal_outcome = 'succeeded'), (SELECT COUNT(DISTINCT sha256_hex) FROM artifacts WHERE job_id = $1), (SELECT COUNT(*) FROM customer_rated_usage_lines line JOIN customer_rated_usage rating USING (rated_usage_id) WHERE rating.job_id = $1)",
        )
        .bind(job)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
        require(
            outputs == (2, 2, 2, 2),
            format!("native batch did not independently settle two distinct images: {outputs:?}"),
        )?;
    }
    if matches!(case, Case::Multiframe) {
        let facts: Vec<(String, String, i64)> =
            sqlx::query_as("SELECT metric,unit,quantity FROM provider_usage_facts WHERE job_id=$1")
                .bind(job)
                .fetch_all(pool)
                .await
                .map_err(debug_error)?;
        require(
            facts
                == vec![(
                    "video_requested_millisecond".into(),
                    "millisecond".into(),
                    2501,
                )],
            format!("exact milliseconds lost: {facts:?}"),
        )?;
    }
    Ok(())
}

async fn assert_invalid_requests(
    pool: &PgPool,
    app: &axum::Router,
    token: &str,
    first: &str,
    last: &str,
    home: &Path,
) -> TestResult {
    let valid = Case::ArkFrames.body(first, last);
    let mut unknown_role = valid.clone();
    unknown_role["content"][0]["role"] = json!("unrecognized");
    let mut mixed_role = valid.clone();
    mixed_role["content"][0]["role"] = json!("reference_image");
    let mut duplicate_text = valid.clone();
    duplicate_text["content"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"text","text":"second prompt"}));
    let mut unsupported = valid.clone();
    unsupported["watermark"] = json!(false);
    let mut wrong_mime = valid.clone();
    wrong_mime["content"][0]["image_url"]["url"] =
        json!(last.replacen("image/png", "image/jpeg", 1));
    let mut fixed_ratio = valid.clone();
    fixed_ratio["ratio"] = json!("16:9");
    let mut poll = Case::ImageToVideo.body(first, last);
    poll["poll"] = json!(1);
    let mut session = Case::ImageToVideo.body(first, last);
    session["session"] = json!(1);
    let mut malformed = valid.clone();
    malformed["content"][0]["image_url"]["url"] = json!("data:image/png;base64,bm90LXBuZw==");
    for (index, (path, body)) in [
        (TASKS, unknown_role),
        (TASKS, mixed_role),
        (TASKS, duplicate_text),
        (TASKS, unsupported),
        (TASKS, wrong_mime),
        (TASKS, fixed_ratio),
        (VIDEOS, poll),
        (VIDEOS, session),
        (TASKS, malformed),
    ]
    .into_iter()
    .enumerate()
    {
        let (status, response) = json_request(
            app.clone(),
            Method::POST,
            path,
            token,
            Some(&format!("invalid-{index}")),
            Some(&body),
        )
        .await?;
        require(
            status.is_client_error(),
            format!("invalid request {index} not rejected: {status} {response}"),
        )?;
    }
    assert_no_admission(pool, home).await
}

async fn assert_no_admission(pool: &PgPool, home: &Path) -> TestResult {
    let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT COUNT(*) FROM jobs),(SELECT COUNT(*) FROM job_input_objects),(SELECT COUNT(*) FROM provider_remote_tasks),(SELECT COUNT(*) FROM ledger_transactions)")
        .fetch_one(pool).await.map_err(debug_error)?;
    let held: i64 = sqlx::query_scalar("SELECT held_micros FROM billing_accounts LIMIT 1")
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
    require(
        counts == (0, 0, 0, 0) && held == 0 && !home.join("submit-calls").exists(),
        format!("invalid request created work/charge: {counts:?}, held={held}"),
    )
}
