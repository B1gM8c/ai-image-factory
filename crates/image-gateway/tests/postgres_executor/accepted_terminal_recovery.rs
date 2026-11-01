use super::accepted_completion_budget::{advance_clock, controlled_clock};
use super::dreamina_compliance_resolution::fixture_runner;
use super::*;
use gpt_image_2_gateway::provider_tasks::ProviderTaskDeadlineStore;
use gpt_image_2_gateway::{
    DreaminaAcceptedTerminalResolutionRequest, GatedCliCommand, GatedCliSubmitCodec,
    GatedCliSubmitDriver, ProviderExecutionContext, ProviderSubmitIntent,
    ProviderSubmitOrchestrator, ProviderSubmitOutcome, ProviderSubmitWork,
    resolve_dreamina_accepted_terminal,
};
use image_cli_runtime::WorkingDirectory;
use image_provider_dreamina_cli::parse_submit_command;
use image_provider_sdk::{PendingOperation, RemoteOperationRef};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

const REMOTE: &str = "offline-accepted-original-2053";
struct Codec {
    stdout: Vec<u8>,
    prompt: String,
    marker: PathBuf,
}
impl GatedCliSubmitCodec for Codec {
    type Payload = DreaminaSubmitPayloadV1;
    fn provider_id(&self) -> &'static str {
        DREAMINA_PROVIDER_ID
    }
    fn command(
        &self,
        _: &ProviderSubmitIntent,
        _: &ProviderExecutionContext,
        _: &SingleOutputCommand<Self::Payload>,
        workspace: &WorkingDirectory,
    ) -> Result<GatedCliCommand, ProviderFailure> {
        Ok(GatedCliCommand::new(
            "/bin/sh",
            sha256(&fs::read("/bin/sh").unwrap()),
            workspace.path(),
            vec![
                "-c".into(),
                "printf invoked >> \"$1\"; printf %s \"$2\"".into(),
                "offline-accepted".into(),
                self.marker.to_string_lossy().into_owned(),
                String::from_utf8(self.stdout.clone()).unwrap(),
                "--prompt".into(),
                self.prompt.clone(),
            ],
            BTreeMap::new(),
            Vec::new(),
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .unwrap())
    }
    fn decode_receipt(
        &self,
        intent: &ProviderSubmitIntent,
        _: &SingleOutputCommand<Self::Payload>,
        stdout: &[u8],
    ) -> Result<PendingOperation, ProviderFailure> {
        assert_eq!(stdout, self.stdout);
        Ok(PendingOperation::new(
            RemoteOperationRef::new(DREAMINA_PROVIDER_ID, intent.submission_id, REMOTE).unwrap(),
            None,
            Some(0),
        ))
    }
}
fn video_scope() -> ExecutorClaimScope {
    ExecutorClaimScope {
        execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
        provider_id: DREAMINA_PROVIDER_ID.into(),
        command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
        adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
    }
}
fn poll_scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        provider_id: DREAMINA_PROVIDER_ID.into(),
        provider_account_id: DREAMINA_ACCOUNT_ID,
        execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    evidence: PathBuf,
    original: PathBuf,
    request: DreaminaAcceptedTerminalResolutionRequest,
    marker: PathBuf,
}
fn private_json(path: &Path, value: &serde_json::Value) -> TestResult {
    fs::write(path, serde_json::to_vec(value).map_err(debug_error)?).map_err(debug_error)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(debug_error)
}
async fn fixture(database: &mut TestDatabase) -> TestResult<Fixture> {
    controlled_clock(database).await?;
    let pool = &database.pool;
    let work = seed_dreamina_video_generation_lease_for_profile(
        pool,
        "compliance-resolution-worker",
        DREAMINA_VIDEOS_API_PROFILE,
    )
    .await?;
    seed_v4_customer_quote_with_basis(
        pool,
        &work,
        V4CustomerQuoteIdentity::dreamina_video(),
        V4CustomerQuoteBasis {
            metric: "video_requested_second",
            unit: "second",
            unit_size: 1,
            unit_price_micros: 1_200_000 / 8,
            quantity_source: "request_derived",
            confidence: "exact",
            max_quantity: 8,
            max_amount_micros: 1_200_000,
        },
    )
    .await?;
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    let prepared = executor
        .prepare_and_handoff(&work, DREAMINA_VIDEO_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    require(prepared.len() == 1, "fixture must have one video output")?;
    seed_terminal_quota(pool, &work).await?;
    let lease = executor
        .claim_prepared(&video_scope(), "compliance-resolution-executor", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("fixture executor not claimable")?;
    executor.start(&lease).await.map_err(debug_error)?;
    let journal = tempfile::tempdir().map_err(debug_error)?;
    let journal_root = journal.path().join("remote-submit");
    let invocation_marker = journal.path().join("offline-invocations");
    let command_source = serde_json::to_vec(&work.command_json).map_err(debug_error)?;
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).map_err(debug_error)?,
        DreaminaSubmitPayloadV1::new(
            &lease.command_hash,
            parse_submit_command(&command_source)
                .map_err(debug_error)?
                .into_single_output(),
        )
        .map_err(debug_error)?,
    )
    .map_err(debug_error)?;

    let prompt = work.command_json["prompt"]
        .as_str()
        .ok_or("fixture prompt missing")?
        .to_owned();
    let workspace = journal.path().join("workspace");
    fs::create_dir(&workspace).map_err(debug_error)?;
    fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
    let runner = fixture_runner(journal.path())?;
    let driver = GatedCliSubmitDriver::new(
        Codec {
            stdout: serde_json::to_vec(
                &serde_json::json!({"submit_id":REMOTE,"prompt":prompt,"gen_status":"querying"}),
            )
            .unwrap(),
            prompt: prompt.clone(),
            marker: invocation_marker.clone(),
        },
        &runner,
        sha256(&fs::read(&runner).map_err(debug_error)?),
        WorkingDirectory::new_private(&workspace).map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    let outcome = ProviderSubmitOrchestrator::new(
        PostgresProviderTaskStore::new(pool.clone()),
        driver,
        60_000,
        &journal_root,
    )
    .map_err(debug_error)?
    .submit(ProviderSubmitWork::new(&lease, command).map_err(debug_error)?)
    .await
    .map_err(debug_error)?;
    require(
        matches!(outcome, ProviderSubmitOutcome::Attached(_)),
        "fixture original not accepted",
    )?;
    advance_clock(pool, 86_402_000).await?;
    let tasks = PostgresProviderTaskStore::new(pool.clone());
    require(
        tasks
            .resolve_due_remote_task_deadline(&poll_scope())
            .await
            .map_err(debug_error)?
            .is_some(),
        "fixture no deadline",
    )?;
    let reductions = PostgresExecutorTerminalStore::new(pool.clone());
    let terminal = reductions
        .claim_terminal("accepted-terminal-fixture-reducer", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("no uncertain reducer")?;
    reductions
        .complete_terminal(&terminal, None)
        .await
        .map_err(debug_error)?;
    let original = fs::canonicalize(journal_root.join(lease.submission_id.simple().to_string()))
        .map_err(debug_error)?;
    let evidence = journal.path().join("query");
    fs::create_dir(&evidence).map_err(debug_error)?;
    fs::set_permissions(&evidence, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
    let evidence = fs::canonicalize(evidence).map_err(debug_error)?;
    let original_request_bytes =
        fs::read(original.join("process-request.json")).map_err(debug_error)?;
    let original_terminal_bytes =
        fs::read(original.join("process-terminal.json")).map_err(debug_error)?;
    let original_request: serde_json::Value =
        serde_json::from_slice(&original_request_bytes).map_err(debug_error)?;
    let spec: serde_json::Value =
        serde_json::from_slice(&fs::read(original.join("spec.json")).map_err(debug_error)?)
            .map_err(debug_error)?;
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let stamp = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(now) * 1_000_000)
        .map_err(debug_error)?
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(debug_error)?;
    let stdout = serde_json::json!({"submit_id":REMOTE,"prompt":prompt,"gen_status":"fail","fail_reason":"generation failed: code=2053; OutputVideoCopyright; 生成的视频里包含受版权保护元素","result_json":null});
    private_json(&evidence.join("query-stdout.json"), &stdout)?;
    let raw = fs::read(evidence.join("query-stdout.json")).map_err(debug_error)?;
    private_json(
        &evidence.join("execution-audit.json"),
        &serde_json::json!({
        "job_id":work.job_id,"submission_id":lease.submission_id,"execution_id":lease.executor_execution_id,
        "native_id":REMOTE,"argv":[original_request["executable"],"query_result","--submit_id",REMOTE],
        "binding_sha256":spec["execution_binding_sha256"],"provider_command_sha256":spec["provider_command_sha256"],"executable_sha256":original_request["executable_sha256"],
        "prompt_sha256":sha256(prompt.as_bytes()),"query_only":true,"query_calls":1,"generation_calls":0,"downloads":0,
        "exit_code":0,"timed_out":false,"output_limit_exceeded":false,"original_evidence_unchanged":true,
        "stdout_bytes":raw.len(),"stderr_bytes":0,"stdout_sha256":sha256(&raw),"started_at":stamp,"finished_at":stamp,
        "request_sha256":sha256(&original_request_bytes),"terminal_sha256":sha256(&original_terminal_bytes)
        }),
    )?;
    let (hold_id,quote_hash,request_hash):(Uuid,String,String)=sqlx::query_as("SELECT hold.hold_id,quote.quote_hash,payload.request_hash FROM customer_billing_holds hold JOIN customer_price_quotes quote ON quote.quote_id=hold.quote_id JOIN job_payloads payload ON payload.job_id=hold.job_id WHERE hold.job_id=$1")
 .bind(work.job_id).fetch_one(pool).await.map_err(debug_error)?;
    Ok(Fixture {
        _root: journal,
        evidence: evidence.clone(),
        original,
        marker: invocation_marker,
        request: DreaminaAcceptedTerminalResolutionRequest {
            job_id: work.job_id,
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            reviewed_stdout_sha256: sha256(&raw),
            reviewed_query_audit_sha256: sha256(
                &fs::read(evidence.join("execution-audit.json")).map_err(debug_error)?,
            ),
            expected_remote_id: REMOTE.into(),
            expected_account_id: DREAMINA_ACCOUNT_ID,
            expected_hold_id: hold_id,
            expected_quote_hash: quote_hash,
            expected_request_hash: request_hash,
            expected_command_hash: lease.command_hash,
            operator_reference: "offline-reviewed-original-terminal".into(),
        },
    })
}
async fn frozen(pool: &PgPool, f: &Fixture) -> TestResult<serde_json::Value> {
    sqlx::query_scalar("SELECT jsonb_build_object('submission',to_jsonb(s),'execution',to_jsonb(e),'intent',to_jsonb(i),'quarantine',to_jsonb(q),'decision',to_jsonb(d),'task',to_jsonb(t)) FROM provider_submissions s JOIN executor_executions e USING(submission_id,executor_execution_id) JOIN provider_remote_submit_intents i USING(submission_id,executor_execution_id) JOIN provider_remote_tasks t USING(submission_id,executor_execution_id) JOIN provider_remote_task_quarantines q ON q.quarantine_id=t.deadline_quarantine_id JOIN executor_resolution_decisions d ON d.decision_id=e.resolution_decision_id WHERE s.submission_id=$1")
 .bind(f.request.submission_id).fetch_one(pool).await.map_err(debug_error)
}
async fn resolve(
    pool: &PgPool,
    f: &Fixture,
) -> Result<
    gpt_image_2_gateway::DreaminaAcceptedTerminalResolution,
    gpt_image_2_gateway::ImageGatewayError,
> {
    resolve_dreamina_accepted_terminal(pool, &f.evidence, &f.original, &f.request).await
}
#[tokio::test]
async fn accepted_terminal_exact_failure_releases_once_and_preserves_originals() -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let Some(mut db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
 let f=fixture(&mut db).await?;let before=frozen(&db.pool,&f).await?;
 let receipt=resolve(&db.pool,&f).await.map_err(debug_error)?;
 require(receipt.captured_micros==0&&receipt.released_micros==1_200_000,"wrong release")?;
 require(resolve(&db.pool,&f).await.map_err(debug_error)?==receipt,"replay changed receipt")?;
 require(frozen(&db.pool,&f).await?==before,"original authority changed")?;
 let state:(String,String,String,i64,i64,i64)=sqlx::query_as("SELECT j.state,j.last_error_code,h.state,h.captured_micros,h.released_micros,(SELECT count(*) FROM dreamina_accepted_terminal_resolutions WHERE job_id=j.job_id) FROM jobs j JOIN customer_billing_holds h USING(job_id) WHERE j.job_id=$1")
 .bind(f.request.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
 require(state==("failed".into(),"dreamina_copyright_rejected".into(),"released".into(),0,1_200_000,1),format!("wrong projection {state:?}"))?;
 let store=PostgresExecutionSettlementStore::new(db.pool.clone(),Arc::new(InMemoryArtifactBlobStore::default()));
 require(store.project_video_released_settlement("executord-process-smoke","project-test",None,f.request.job_id).await.map_err(debug_error)?.is_some(),"missing API release")?;
 let status=store.project_video_status("executord-process-smoke","project-test",None,f.request.job_id).await.map_err(debug_error)?;
 require(matches!(status,Some(gpt_image_2_gateway::settlement::VideoResultStatus::Failed{error_code:Some(ref code),..}) if code=="dreamina_copyright_rejected"),format!("wrong API terminal {status:?}"))?;
 for (tenant,project,actor) in [("another-tenant","project-test",None),("executord-process-smoke","another-project",None),("executord-process-smoke","project-test",Some(Uuid::new_v4()))] {
 require(store.project_video_released_settlement(tenant,project,actor,f.request.job_id).await.map_err(debug_error)?.is_none(),"released receipt leaked scope")?;
 }
 require(fs::read(&f.marker).map_err(debug_error)?==b"invoked","fixture dispatched twice")?;
 Ok(())}.await;
    combine(result, db.cleanup().await)
}
#[tokio::test]
async fn accepted_terminal_wrong_review_rolls_back_and_concurrent_replay_is_unique() -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let Some(mut db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let f = fixture(&mut db).await?;
        let before = frozen(&db.pool, &f).await?;
        let mut bad = f.request.clone();
        bad.expected_remote_id = "another-native-id".into();
        require(
            resolve_dreamina_accepted_terminal(&db.pool, &f.evidence, &f.original, &bad)
                .await
                .is_err(),
            "wrong native identity accepted",
        )?;
        require(
            frozen(&db.pool, &f).await? == before,
            "rejected proof changed original",
        )?;
        let (a, b) = tokio::join!(resolve(&db.pool, &f), resolve(&db.pool, &f));
        require(a.is_ok() || b.is_ok(), format!("both failed {a:?} {b:?}"))?;
        let receipt = resolve(&db.pool, &f).await.map_err(debug_error)?;
        require(
            receipt.captured_micros == 0 && receipt.released_micros == 1_200_000,
            "duplicate release",
        )?;
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM dreamina_accepted_terminal_resolutions")
                .fetch_one(&db.pool)
                .await
                .map_err(debug_error)?;
        require(count == 1, "multiple audits")?;
        require(
            sqlx::query("DELETE FROM dreamina_accepted_terminal_resolutions")
                .execute(&db.pool)
                .await
                .is_err(),
            "audit mutable",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_remote_tasks SET cancel_requested=true WHERE submission_id=$1",
            )
            .bind(f.request.submission_id)
            .execute(&db.pool)
            .await
            .is_err(),
            "late cancellation changed resolved proof",
        )?;
        Ok(())
    }
    .await;
    combine(result, db.cleanup().await)
}
#[tokio::test]
async fn accepted_terminal_rejects_bad_query_and_rolls_back_late_failure() -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let Some(mut db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
 let f=fixture(&mut db).await?;
 let stdout_bytes=fs::read(f.evidence.join("query-stdout.json")).map_err(debug_error)?;
 let audit_bytes=fs::read(f.evidence.join("execution-audit.json")).map_err(debug_error)?;
 let stdout:serde_json::Value=serde_json::from_slice(&stdout_bytes).map_err(debug_error)?;
 let audit:serde_json::Value=serde_json::from_slice(&audit_bytes).map_err(debug_error)?;
 for (field,value) in [("submit_id",serde_json::json!("another-id")),("prompt",serde_json::json!("different prompt")),("gen_status",serde_json::json!("success")),("fail_reason",serde_json::json!("unclassified provider error")),("result_json",serde_json::json!({"videos":[{"url":"https://media.example.invalid/result.mp4"}]}))] {
  let mut bad=stdout.clone();bad[field]=value;private_json(&f.evidence.join("query-stdout.json"),&bad)?;
  let raw=fs::read(f.evidence.join("query-stdout.json")).map_err(debug_error)?;
  let mut bad_audit=audit.clone();bad_audit["stdout_sha256"]=serde_json::json!(sha256(&raw));bad_audit["stdout_bytes"]=serde_json::json!(raw.len());private_json(&f.evidence.join("execution-audit.json"),&bad_audit)?;
  let mut request=f.request.clone();request.reviewed_stdout_sha256=sha256(&raw);request.reviewed_query_audit_sha256=sha256(&fs::read(f.evidence.join("execution-audit.json")).map_err(debug_error)?);
  require(resolve_dreamina_accepted_terminal(&db.pool,&f.evidence,&f.original,&request).await.is_err(),format!("bad query {field} accepted"))?;
 }
 fs::write(f.evidence.join("query-stdout.json"),stdout_bytes).map_err(debug_error)?;fs::write(f.evidence.join("execution-audit.json"),audit_bytes).map_err(debug_error)?;
 let before=frozen(&db.pool,&f).await?;
 // Inject a failure after allocation and money updates; the whole transaction must roll back.
 sqlx::raw_sql("CREATE FUNCTION reject_accepted_terminal_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.semantic_key='job.operator_dreamina_accepted_terminal' THEN RAISE EXCEPTION 'offline injected rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER accepted_terminal_fixture_fault BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_accepted_terminal_fixture()").execute(&db.pool).await.map_err(debug_error)?;
 require(resolve(&db.pool,&f).await.is_err(),"late injected failure committed")?;
 require(frozen(&db.pool,&f).await?==before,"late rollback changed frozen evidence")?;
 let unchanged:(String,String,i64,i64,i64)=sqlx::query_as("SELECT a.state,h.state,h.captured_micros,h.released_micros,(SELECT count(*) FROM dreamina_accepted_terminal_resolutions) FROM executor_capacity_allocations a JOIN provider_submissions s USING(submission_id,executor_execution_id) JOIN customer_billing_holds h ON h.job_id=s.job_id WHERE s.submission_id=$1").bind(f.request.submission_id).fetch_one(&db.pool).await.map_err(debug_error)?;
 require(unchanged==("held".into(),"held".into(),0,0,0),format!("late rollback financial drift {unchanged:?}"))?;
 sqlx::query("DROP TRIGGER accepted_terminal_fixture_fault ON job_events").execute(&db.pool).await.map_err(debug_error)?;
 resolve(&db.pool,&f).await.map_err(debug_error)?;
 Ok(())}.await;
    combine(result, db.cleanup().await)
}

#[tokio::test]
async fn accepted_terminal_video_get_exposes_only_audited_project_release() -> TestResult {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use gpt_image_2_gateway::{
        ApiKeyKeyring, ApiKeyPermissionMode, ApiKeyPermissions, ApiKeyStore, AppConfig,
        ExternalImageGatewayComponents, GenerationAdmissionContract, PostgresApiKeyStore,
        PostgresUsageStore, ProxyConfig, admission::PostgresAdmissionStore,
        build_router_with_external_execution,
    };
    use tower::ServiceExt;

    let Some(mut db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let f = fixture(&mut db).await?;
        let keys = Arc::new(PostgresApiKeyStore::new(
            db.pool.clone(),
            ApiKeyKeyring::new(1, [(1, vec![0x47; 32])]).map_err(debug_error)?,
        ));
        let reader = keys
            .create_service_account(
                "project-test",
                "Release reader",
                ApiKeyPermissionMode::ReadOnly,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        let denied = keys
            .create_service_account(
                "project-test",
                "No video permission",
                ApiKeyPermissionMode::Restricted,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        let other_project = keys
            .create_project("Other release scope")
            .await
            .map_err(debug_error)?;
        let other = keys
            .create_service_account(
                &other_project.id,
                "Other reader",
                ApiKeyPermissionMode::ReadOnly,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        let blobs = Arc::new(InMemoryArtifactBlobStore::default());
        let store = Arc::new(PostgresExecutionSettlementStore::new(
            db.pool.clone(),
            blobs.clone(),
        ));
        let app = build_router_with_external_execution(
            AppConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                auth_token: None,
                admin_token: None,
                legacy_admin_auth_enabled: false,
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
            },
            ExternalImageGatewayComponents {
                usage_store: Arc::new(PostgresUsageStore::new(db.pool.clone())),
                api_key_store: keys,
                admission_store: Arc::new(PostgresAdmissionStore::new(db.pool.clone())),
                settlement_store: store,
                input_blob_store: blobs,
                provider_readiness_store: Arc::new(PostgresProviderTaskStore::new(db.pool.clone())),
            },
        )
        .map_err(debug_error)?;
        let get = |token: String| {
            let app = app.clone();
            let uri = format!("/v1/dreamina/videos/{}", f.request.job_id);
            async move {
                let response = app
                    .oneshot(
                        Request::builder()
                            .uri(uri)
                            .header(header::AUTHORIZATION, format!("Bearer {token}"))
                            .body(Body::empty())
                            .map_err(debug_error)?,
                    )
                    .await
                    .map_err(debug_error)?;
                let status = response.status();
                let bytes = to_bytes(response.into_body(), 64 * 1024)
                    .await
                    .map_err(debug_error)?;
                let body: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(debug_error)?;
                Ok::<_, String>((status, body))
            }
        };
        // Before the audited resolution the original remains uncertain, without
        // inventing a released settlement from the error or billing hold alone.
        let (status, body) = get(reader.api_key.value.clone()).await?;
        require(
            status == StatusCode::OK
                && body["status"] == "uncertain"
                && body.get("settlement").is_none(),
            format!("unresolved original fabricated receipt: {status}/{body}"),
        )?;
        let receipt = resolve(&db.pool, &f).await.map_err(debug_error)?;
        let (status, body) = get(reader.api_key.value.clone()).await?;
        require(
            status == StatusCode::OK
                && body["status"] == "failed"
                && body["error"]["code"] == "dreamina_copyright_rejected"
                && body["error"]["message"] == "生成的视频里包含受版权保护元素",
            format!("copyright GET lost actual terminal reason: {status}/{body}"),
        )?;
        require(
            body["settlement"]
                == json!({
                    "state": "released", "resolution_audit_id": receipt.resolution_id,
                    "job_id": receipt.job_id, "request_id": receipt.request_id,
                    "hold_id": receipt.hold_id, "currency": "USD",
                    "held_micros": "1200000", "released_micros": "1200000", "captured_micros": "0",
                }),
            format!("GET omitted or changed audited settlement: {body}"),
        )?;
        for (token, expected) in [
            (denied.api_key.value, StatusCode::FORBIDDEN),
            (other.api_key.value, StatusCode::BAD_REQUEST),
            ("invalid-fixture-token".to_owned(), StatusCode::UNAUTHORIZED),
        ] {
            let (status, body) = get(token).await?;
            require(
                status == expected && body.get("settlement").is_none(),
                format!("unauthorized GET exposed settlement: {status}/{body}"),
            )?;
        }
        require(
            fs::read(&f.marker).map_err(debug_error)? == b"invoked",
            "project GET dispatched a provider operation",
        )?;
        Ok(())
    }
    .await;
    combine(result, db.cleanup().await)
}
