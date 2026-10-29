use super::*;
use gpt_image_2_gateway::{DreaminaVideoArchiveResolutionRequest, resolve_dreamina_video_archive};

fn terminal_payload_sha256(terminal: &serde_json::Value) -> TestResult<String> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"ai-image-factory/gated-cli-terminal/v1\0");
    let binding = terminal["execution_binding_sha256"]
        .as_str()
        .ok_or("binding")?;
    digest.update((binding.len() as u64).to_be_bytes());
    digest.update(binding.as_bytes());
    for key in ["launch_nonce", "helper_nonce", "child_nonce"] {
        digest.update(
            Uuid::parse_str(terminal[key].as_str().ok_or("nonce")?)
                .map_err(debug_error)?
                .as_bytes(),
        );
    }
    digest.update([1, 1, 0, 1, 0, 0, 0, 0, 0]);
    for _ in 0..2 {
        digest.update(0_u64.to_be_bytes());
        digest.update([0]);
    }
    Ok(hex::encode(digest.finalize()))
}

async fn normal_original(
    pool: &PgPool,
    model: &'static str,
    audio_count: usize,
    grant_micros: i64,
) -> TestResult<Fixture> {
    let work = seed_supplement_video(pool, model, audio_count).await?;
    let price = if model == "seedance2.0mini" {
        800_000
    } else {
        1_200_000
    };
    seed_v4_customer_quote_with_funding(pool,&work,V4CustomerQuoteIdentity{provider_model_id:model,public_model_id:model,dimensions:json!({"duration":"10","ratio":"16:9","resolution":"720p","processing_mode":"synchronous","workflow":"multimodal2video","input_image_count":"1","input_video_count":"0","input_audio_count":audio_count.to_string()}),..V4CustomerQuoteIdentity::dreamina_video()},V4CustomerQuoteBasis{metric:"video_requested_second",unit:"second",unit_size:1,unit_price_micros:price/10,quantity_source:"request_derived",confidence:"exact",max_quantity:10,max_amount_micros:price},grant_micros).await?;
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    executor
        .prepare_and_handoff(&work, DREAMINA_VIDEO_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    seed_terminal_quota(pool, &work).await?;
    let scope = ExecutorClaimScope {
        execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
        provider_id: DREAMINA_PROVIDER_ID.into(),
        command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
        adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
    };
    let lease = executor
        .claim_prepared(&scope, "supplement-original", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("missing original lease")?;
    executor.start(&lease).await.map_err(debug_error)?;
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).map_err(debug_error)?,
        DreaminaSubmitPayloadV1::new(
            &lease.command_hash,
            parse_submit_command(&serde_json::to_vec(&work.command_json).map_err(debug_error)?)
                .map_err(debug_error)?,
        )
        .map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    let reservation = RemoteTaskSubmitReservation::new(
        &lease,
        format!("original-{}", lease.submission_id.simple()),
        command.output(),
        command.identity(),
        2_000,
    );
    let tasks = PostgresProviderTaskStore::new(pool.clone());
    tasks
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    tasks
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    let binding: String = sqlx::query_scalar("SELECT execution_binding_sha256 FROM provider_remote_submit_intents WHERE submission_id=$1")
        .bind(lease.submission_id).fetch_one(pool).await.map_err(debug_error)?;
    tasks
        .record_submit_failure(
            &gpt_image_2_gateway::provider_tasks::RemoteTaskSubmitFailure {
                submission_id: lease.submission_id,
                executor_execution_id: lease.executor_execution_id,
                executor_owner: lease.executor_owner.clone(),
                executor_lease_epoch: lease.executor_lease_epoch,
                kind:
                    gpt_image_2_gateway::provider_tasks::ProviderSubmitFailureKind::OutcomeUnknown,
                event_identity: "offline-empty-normal-submit".into(),
                error_code: "dreamina_submit_receipt_invalid".into(),
                execution_binding_sha256: binding,
                recovery_fence: None,
            },
        )
        .await
        .map_err(debug_error)?;
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    tasks
        .resolve_due_submit_deadline(&ProviderTaskClaimScope {
            provider_id: DREAMINA_PROVIDER_ID.into(),
            provider_account_id: DREAMINA_ACCOUNT_ID,
            execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
        })
        .await
        .map_err(debug_error)?
        .ok_or("original deadline not due")?;
    let terminal = PostgresExecutorTerminalStore::new(pool.clone());
    let terminal_lease = terminal
        .claim_terminal("supplement-reducer", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("missing terminal reduction")?;
    terminal
        .complete_terminal(&terminal_lease, None)
        .await
        .map_err(debug_error)?;
    let quote: String =
        sqlx::query_scalar("SELECT quote_hash FROM customer_price_quotes WHERE job_id=$1")
            .bind(work.job_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let now = database_now(pool).await?;
    sqlx::query("INSERT INTO provider_account_environments (provider_account_id,provider_id,environment_kind,environment_ref,upstream_identity_sha256,display_name,state,created_at_ms,updated_at_ms) VALUES ($1,'dreamina-cli','dreamina_home_v1','/offline/supplement-home',$2,'Supplement fixture','active',$3,$3) ON CONFLICT (provider_account_id) DO NOTHING").bind(DREAMINA_ACCOUNT_ID).bind("d".repeat(64)).bind(now).execute(pool).await.map_err(debug_error)?;
    let credentials:(String,i64,String)=sqlx::query_as("SELECT credential_ref,credential_revision,credential_auth_sha256 FROM provider_accounts WHERE provider_account_id=$1").bind(DREAMINA_ACCOUNT_ID).fetch_one(pool).await.map_err(debug_error)?;
    let runtime = json!({"execution_profile_id":DREAMINA_VIDEO_PROFILE_ID,"provider_account_id":DREAMINA_ACCOUNT_ID,"upstream_identity_sha256":"d".repeat(64),"credential_revision":credentials.1,"credential_ref_sha256":sha256(credentials.0.as_bytes()),"credential_auth_sha256":credentials.2,"account_home_sha256":sha256(b"/offline/supplement-home"),"cli_path":"/offline/dreamina","cli_sha256":"e".repeat(64),"runner_path":"/offline/runner","runner_sha256":"f".repeat(64)});
    let directory = tempfile::tempdir().map_err(debug_error)?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
        .map_err(debug_error)?;
    let root = fs::canonicalize(directory.path()).map_err(debug_error)?;
    Ok(Fixture {
        request: Authorize {
            runtime_binding_json: runtime,
            job_id: work.job_id,
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            expected_command_sha256: lease.command_hash,
            expected_quote_hash: quote,
            operator_reference: "offline-regression:one-manual-supplement".into(),
        },
        root,
        _directory: directory,
    })
}
async fn recovery_evidence(
    pool: &PgPool,
    fixture: &Fixture,
) -> TestResult<DreaminaVideoArchiveResolutionRequest> {
    let (command, binding, provider_hash): (serde_json::Value, String, String) = sqlx::query_as(
        "SELECT payload.command_json,intent.execution_binding_sha256,intent.provider_command_sha256 FROM job_payloads payload JOIN provider_submissions submission USING(job_id) JOIN provider_remote_submit_intents intent USING(submission_id) WHERE payload.job_id=$1")
        .bind(fixture.request.job_id).fetch_one(pool).await.map_err(debug_error)?;
    let command_bytes = serde_json::to_vec(&command).map_err(debug_error)?;
    let native = Uuid::new_v4().to_string();
    let nonce = Uuid::new_v4();
    let source = json!({"prompt":command["prompt"],"model_version":command["model_version"],"ratio":"16:9","duration":10,"video_resolution":"720p","image_paths":["/offline/image-0.png"],"audio_paths":["/offline/audio-0.mp3"]});
    let start = format!(
        "INFO 2026-10-01 00:00:00,000 traceABC [SubmitTask] start submit generation task input={source}"
    );
    let candidate = format!("INFO 2026-10-01 00:00:00,001 traceABC upload submit_id={native}");
    let start_ms = (time::OffsetDateTime::parse(
        "2026-10-01T00:00:00+08:00",
        &time::format_description::well_known::Rfc3339,
    )
    .map_err(debug_error)?
    .unix_timestamp_nanos()
        / 1_000_000) as i64;
    let spec = json!({"provider_id":"dreamina-cli","provider_account_id":DREAMINA_ACCOUNT_ID,"command_schema":DREAMINA_SUBMIT_COMMAND_SCHEMA,"adapter_revision":DREAMINA_ADAPTER_REVISION,"output_index":0,"output_total":1,"submission_id":fixture.request.submission_id,"executor_execution_id":fixture.request.executor_execution_id,"provider_command_sha256":provider_hash,"execution_binding_sha256":binding,"command_bytes_sha256":sha256(&command_bytes),"command_byte_size":command_bytes.len()});
    let gate = json!({"environment":{"HOME":"/offline/supplement-home"},"submission_id":fixture.request.submission_id,"execution_binding_sha256":binding,"launch_nonce":nonce,"executable":"/offline/dreamina","executable_sha256":"e".repeat(64),"arguments":["multimodal2video","--prompt",command["prompt"],"--model_version",command["model_version"],"--ratio","16:9","--video_resolution","720p","--duration","10","--image","/offline/image-0.png","--audio","/offline/audio-0.mp3","--poll=0"]});
    let mut terminal = json!({"execution_binding_sha256":binding,"launch_nonce":nonce,"helper_nonce":Uuid::new_v4(),"child_nonce":Uuid::new_v4(),"released":true,"exec_started":true,"outcome":{"state":"exited","exit_code":0,"signal":null},"stdout_truncated":false,"stderr_truncated":false,"stdout_base64":"","stderr_base64":""});
    terminal["payload_sha256"] = json!(terminal_payload_sha256(&terminal)?);
    let query=serde_json::to_vec(&json!({"submit_id":native,"gen_status":"success","prompt":command["prompt"],"result_json":{"images":[],"videos":[{"video_url":URL,"duration":10}]}})).map_err(debug_error)?;
    let audit = json!({"native_id":native,"exit_code":0,"timed_out":false,"query_only":true,"generation_calls":0,"business_writes":0,"stdout_sha256":sha256(&query),"stdout_bytes":query.len(),"stderr_bytes":0,"executable_sha256":"e".repeat(64),"argv":["/offline/dreamina","query_result","--submit_id",native],"started_at":"2026-10-01T00:00:02+08:00","finished_at":"2026-10-01T00:00:03+08:00"});
    let archive = json!({"native_id":native,"query_sha256":sha256(&query),"original_sha256":sha256(video_bytes()),"original_bytes":video_bytes().len(),"public_dns_pinned":true,"tls_hostname_verified":true,"transcoded":false,"generation_calls":0,"business_writes":0,"hops":[{"host":"media.example.test","status":200}],"started_at":"2026-10-01T00:00:04+08:00","finished_at":"2026-10-01T00:00:05+08:00"});
    let mut files = std::collections::BTreeMap::new();
    for (name, bytes) in [
        ("command.bin", command_bytes),
        ("spec.json", serde_json::to_vec(&spec).map_err(debug_error)?),
        (
            "process-request.json",
            serde_json::to_vec(&gate).map_err(debug_error)?,
        ),
        (
            "process-terminal.json",
            serde_json::to_vec(&terminal).map_err(debug_error)?,
        ),
        (
            "source-account-log.bin",
            format!("{start}\n{candidate}\n").into_bytes(),
        ),
        ("query-stdout.json", query),
        (
            "execution-audit.json",
            serde_json::to_vec(&audit).map_err(debug_error)?,
        ),
        (
            "capture-query-only.py",
            b"# Offline fixture: never executed\n".to_vec(),
        ),
        ("query-stderr.bin", Vec::new()),
        ("original-video.mp4", video_bytes().to_vec()),
        (
            "archive-audit.json",
            serde_json::to_vec(&archive).map_err(debug_error)?,
        ),
    ] {
        files.insert(name.to_owned(), sha256(&bytes));
        write(&fixture.root, name, &bytes)?;
    }
    let review = json!({"provider_account_id":DREAMINA_ACCOUNT_ID,"account_home_sha256":sha256(b"/offline/supplement-home"),"schema_version":1,"evidence_mode":"normal_submit_archive_v1","job_id":fixture.request.job_id,"submission_id":fixture.request.submission_id,"executor_execution_id":fixture.request.executor_execution_id,"tenant_id":"executord-process-smoke","quote_hash":fixture.request.expected_quote_hash,"source_command_sha256":fixture.request.expected_command_sha256,"provider_command_sha256":provider_hash,"execution_binding_sha256":binding,"remote_operation_id":native,"operator_reference":"offline-original-archive","original_started_at_ms":start_ms,"original_finished_at_ms":start_ms+1000,"source_trace_id":"traceABC","source_start_line_sha256":sha256(start.as_bytes()),"source_candidate_line_sha256":sha256(candidate.as_bytes()),"files":files});
    let review = serde_json::to_vec(&review).map_err(debug_error)?;
    write(&fixture.root, "review.json", &review)?;
    Ok(DreaminaVideoArchiveResolutionRequest {
        job_id: fixture.request.job_id,
        submission_id: fixture.request.submission_id,
        executor_execution_id: fixture.request.executor_execution_id,
        reviewed_recovery_sha256: sha256(&review),
        operator_reference: "offline-original-archive".into(),
    })
}

#[tokio::test]
async fn normal_archive_binds_real_terminal_schema_without_submission_id() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = normal_original(&db.pool, "seedance2.0fast_vip", 1, 0).await?;
        let request = recovery_evidence(&db.pool, &fixture).await?;
        let original_review: serde_json::Value = serde_json::from_slice(&fs::read(fixture.root.join("review.json")).map_err(debug_error)?).map_err(debug_error)?;
        let original_gate: serde_json::Value = serde_json::from_slice(&fs::read(fixture.root.join("process-request.json")).map_err(debug_error)?).map_err(debug_error)?;
        let original_terminal: serde_json::Value = serde_json::from_slice(&fs::read(fixture.root.join("process-terminal.json")).map_err(debug_error)?).map_err(debug_error)?;
        require(original_terminal.get("submission_id").is_none(), "fixture invented a terminal submission_id")?;
        for case in ["payload", "binding", "nonce", "request_submission"] {
            let mut terminal = original_terminal.clone();
            let mut gate = original_gate.clone();
            let mut review = original_review.clone();
            match case {
                "payload" => terminal["payload_sha256"] = json!("0".repeat(64)),
                "binding" => {
                    terminal["execution_binding_sha256"] = json!("0".repeat(64));
                    terminal["payload_sha256"] = json!(terminal_payload_sha256(&terminal)?);
                }
                "nonce" => {
                    terminal["launch_nonce"] = json!(Uuid::new_v4());
                    terminal["payload_sha256"] = json!(terminal_payload_sha256(&terminal)?);
                }
                "request_submission" => gate["submission_id"] = json!(Uuid::new_v4()),
                _ => unreachable!(),
            }
            for (name, value) in [("process-terminal.json", terminal), ("process-request.json", gate)] {
                let bytes = serde_json::to_vec(&value).map_err(debug_error)?;
                write(&fixture.root, name, &bytes)?;
                review["files"][name] = json!(sha256(&bytes));
            }
            let review_bytes = serde_json::to_vec(&review).map_err(debug_error)?;
            write(&fixture.root, "review.json", &review_bytes)?;
            let mut changed = request.clone();
            changed.reviewed_recovery_sha256 = sha256(&review_bytes);
            require(resolve_dreamina_video_archive(&db.pool, &InMemoryArtifactBlobStore::default(), &fixture.root, &changed).await.is_err(), format!("terminal {case} mismatch accepted"))?;
            assert_pending(&db.pool, request.job_id).await?;
            let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_operator_video_success_resolutions WHERE job_id=$1),(SELECT count(*) FROM artifacts WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1)").bind(request.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
            require(counts == (0, 0, 0), "rejected terminal changed delivery or billing")?;
        }
        for (name, value) in [("process-terminal.json", original_terminal), ("process-request.json", original_gate), ("review.json", original_review)] {
            write(&fixture.root, name, &serde_json::to_vec(&value).map_err(debug_error)?)?;
        }
        let recovered = resolve_dreamina_video_archive(&db.pool, &InMemoryArtifactBlobStore::default(), &fixture.root, &request).await.map_err(debug_error)?;
        require(recovered.archive_sha256 == sha256(video_bytes()), "real terminal schema did not recover original bytes")
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn normal_archive_recovers_original_bytes_once_with_capacity_and_quote_conserved()
-> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixture=normal_original(&db.pool,"seedance2.0fast_vip",1,0).await?;
        let request=recovery_evidence(&db.pool,&fixture).await?;
        let before:serde_json::Value=sqlx::query_scalar("SELECT dreamina_video_supplement_original_snapshot($1)").bind(request.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        let blobs=Arc::new(InMemoryArtifactBlobStore::default());
        let (a,b)=tokio::join!(resolve_dreamina_video_archive(&db.pool,&*blobs,&fixture.root,&request),resolve_dreamina_video_archive(&db.pool,&*blobs,&fixture.root,&request));
        let a=a.map_err(debug_error)?;let b=b.map_err(debug_error)?;
        require(a==b,"concurrent identical archive recovery did not replay one result")?;
        require(a.archive_sha256==sha256(video_bytes())&&a.archive_byte_size==video_bytes().len()as i64,"archive identity changed")?;
        let service=PostgresExecutionSettlementStore::new(db.pool.clone(),blobs.clone());
        let status=service.video_status("executord-process-smoke",request.job_id).await.map_err(debug_error)?;
        require(matches!(status,Some(gpt_image_2_gateway::settlement::VideoResultStatus::Succeeded{artifact_id,..}) if artifact_id==a.artifact_id),format!("canonical API did not expose original artifact: {status:?}"))?;
        let file=service.load_video_artifact("executord-process-smoke",a.artifact_id).await.map_err(debug_error)?.ok_or("original artifact not downloadable")?;
        require(file.bytes==video_bytes() && file.media_type=="video/mp4","download API changed original bytes")?;
        let snapshot:serde_json::Value=sqlx::query_scalar("SELECT dreamina_video_supplement_original_snapshot($1)").bind(request.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        for key in ["provider_submissions","executor_executions","provider_remote_submit_intents","provider_submit_recoveries","executor_resolution_decisions","executor_terminal_reductions","provider_receipts"] {require(before[key]==snapshot[key],format!("original evidence changed: {key}"))?;}
        let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM artifacts WHERE job_id=$1),(SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),(SELECT count(*) FROM provider_remote_tasks WHERE submission_id=$2)").bind(request.job_id).bind(request.submission_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counts==(1,1,1,0),format!("archive/settlement/task counts mismatch: {counts:?}"))?;
        let capacity:(String,String,String,String)=sqlx::query_as("SELECT allocation.state,capacity.state,capacity.evidence_kind,capacity.remote_terminal_state FROM executor_capacity_allocations allocation JOIN provider_capacity_reconciliations capacity USING(submission_id) WHERE allocation.submission_id=$1").bind(request.submission_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(capacity==("released".into(),"released".into(),"remote_terminal".into(),"succeeded".into()),"capacity not closed by positive original result")?;
        let mut conflict=request.clone();conflict.operator_reference="different-authority".into();
        require(resolve_dreamina_video_archive(&db.pool,&*blobs,&fixture.root,&conflict).await.is_err(),"conflicting authority replayed")?;
        Ok(())
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn normal_archive_rejects_changed_original_bytes_before_settlement() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = normal_original(&db.pool, "seedance2.0fast_vip", 1, 0).await?;
        let request = recovery_evidence(&db.pool, &fixture).await?;
        let mut changed = video_bytes().to_vec();
        changed.push(0);
        write(&fixture.root, "original-video.mp4", &changed)?;
        require(
            resolve_dreamina_video_archive(
                &db.pool,
                &InMemoryArtifactBlobStore::default(),
                &fixture.root,
                &request,
            )
            .await
            .is_err(),
            "changed archive accepted",
        )?;
        assert_pending(&db.pool, request.job_id).await?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM dreamina_operator_video_success_resolutions WHERE job_id=$1",
        )
        .bind(request.job_id)
        .fetch_one(&db.pool)
        .await
        .map_err(debug_error)?;
        require(count == 0, "rejected archive left authority")
    }
    .await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn normal_archive_rejects_mismatched_review_and_existing_supplement_authority() -> TestResult
{
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = normal_original(&db.pool, "seedance2.0fast_vip", 1, 0).await?;
        let original_request = recovery_evidence(&db.pool, &fixture).await?;
        let original_review: serde_json::Value = serde_json::from_slice(
            &fs::read(fixture.root.join("review.json")).map_err(debug_error)?,
        )
        .map_err(debug_error)?;
        let original_query: serde_json::Value = serde_json::from_slice(
            &fs::read(fixture.root.join("query-stdout.json")).map_err(debug_error)?,
        )
        .map_err(debug_error)?;
        for case in ["tenant", "native", "prompt", "source"] {
            let mut review = original_review.clone();
            let mut query = original_query.clone();
            match case {
                "tenant" => review["tenant_id"] = json!("different-tenant"),
                "native" => review["remote_operation_id"] = json!(Uuid::new_v4()),
                "prompt" => query["prompt"] = json!("different request prompt"),
                "source" => review["source_start_line_sha256"] = json!("9".repeat(64)),
                _ => unreachable!(),
            }
            let query_bytes = serde_json::to_vec(&query).map_err(debug_error)?;
            write(&fixture.root, "query-stdout.json", &query_bytes)?;
            review["files"]["query-stdout.json"] = json!(sha256(&query_bytes));
            let review_bytes = serde_json::to_vec(&review).map_err(debug_error)?;
            write(&fixture.root, "review.json", &review_bytes)?;
            let mut request = original_request.clone();
            request.reviewed_recovery_sha256 = sha256(&review_bytes);
            require(
                resolve_dreamina_video_archive(
                    &db.pool,
                    &InMemoryArtifactBlobStore::default(),
                    &fixture.root,
                    &request,
                )
                .await
                .is_err(),
                format!("mismatched {case} accepted"),
            )?;
            assert_pending(&db.pool, request.job_id).await?;
        }
        write(
            &fixture.root,
            "review.json",
            &serde_json::to_vec(&original_review).map_err(debug_error)?,
        )?;
        write(
            &fixture.root,
            "query-stdout.json",
            &serde_json::to_vec(&original_query).map_err(debug_error)?,
        )?;
        store(&db.pool)
            .authorize_dreamina_video_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        require(
            resolve_dreamina_video_archive(
                &db.pool,
                &InMemoryArtifactBlobStore::default(),
                &fixture.root,
                &original_request,
            )
            .await
            .is_err(),
            "archive reused an original with a supplement authorization",
        )?;
        assert_pending(&db.pool, original_request.job_id).await?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM dreamina_operator_video_success_resolutions WHERE job_id=$1",
        )
        .bind(original_request.job_id)
        .fetch_one(&db.pool)
        .await
        .map_err(debug_error)?;
        require(count == 0, "rejected authority persisted")
    }
    .await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn normal_archive_cannot_steal_a_live_capacity_lease() -> TestResult {
    use gpt_image_2_gateway::provider_tasks::ProviderCapacityReconciliationStore;
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixture=normal_original(&db.pool,"seedance2.0fast_vip",1,0).await?;
        let request=recovery_evidence(&db.pool,&fixture).await?;
        let tasks=PostgresProviderTaskStore::new(db.pool.clone());
        let scope=ProviderTaskClaimScope{provider_id:DREAMINA_PROVIDER_ID.into(),provider_account_id:DREAMINA_ACCOUNT_ID,execution_profile_id:DREAMINA_VIDEO_PROFILE_ID};
        let lease=tasks.claim_due_capacity_reconciliation(&scope,"offline-other-owner","offline-exclusive-claim",60_000).await.map_err(debug_error)?.ok_or("capacity claim not due")?;
        require(resolve_dreamina_video_archive(&db.pool,&InMemoryArtifactBlobStore::default(),&fixture.root,&request).await.is_err(),"archive stole another owner's live capacity lease")?;
        assert_pending(&db.pool,request.job_id).await?;
        let owner:String=sqlx::query_scalar("SELECT reconciliation_owner FROM provider_capacity_reconciliations WHERE submission_id=$1").bind(request.submission_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(owner==lease.reconciliation_owner,"capacity owner changed after rollback")?;
        let counts:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_operator_video_success_resolutions WHERE job_id=$1),(SELECT count(*) FROM artifacts WHERE job_id=$1)").bind(request.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counts==(0,0),"conflicted lease left delivery authority or artifact")
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn normal_archive_different_native_race_commits_only_one_charge() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixture=normal_original(&db.pool,"seedance2.0fast_vip",1,0).await?;
        let first=recovery_evidence(&db.pool,&fixture).await?;
        let alternative=tempfile::tempdir().map_err(debug_error)?;
        fs::set_permissions(alternative.path(),fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
        let first_root=fs::canonicalize(alternative.path()).map_err(debug_error)?;
        for entry in fs::read_dir(&fixture.root).map_err(debug_error)? {
            let entry=entry.map_err(debug_error)?;
            write(&first_root,&entry.file_name().to_string_lossy(),&fs::read(entry.path()).map_err(debug_error)?)?;
        }
        let second=recovery_evidence(&db.pool,&fixture).await?;
        let blobs=InMemoryArtifactBlobStore::default();
        let (a,b)=tokio::join!(resolve_dreamina_video_archive(&db.pool,&blobs,&first_root,&first),resolve_dreamina_video_archive(&db.pool,&blobs,&fixture.root,&second));
        require(usize::from(a.is_ok())+usize::from(b.is_ok())==1,"different reviewed native IDs both committed or neither succeeded")?;
        let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_operator_video_success_resolutions WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),(SELECT count(*) FROM artifacts WHERE job_id=$1)").bind(first.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counts==(1,1,1),"native race duplicated delivery or charge")?;
        let mut tx=db.pool.begin().await.map_err(debug_error)?;
        let late_failure=sqlx::query("UPDATE jobs SET state='failed',last_error_code='late_old_failure' WHERE job_id=$1").bind(first.job_id).execute(&mut *tx).await;
        let rejected=if late_failure.is_err(){tx.rollback().await.map_err(debug_error)?;true}else{tx.commit().await.is_err()};
        require(rejected,"late failure overwrote recovered success")
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn normal_archive_preserves_confirmed_cancellation_and_unique_release() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = normal_original(&db.pool, "seedance2.0fast_vip", 1, 0).await?;
        let request = recovery_evidence(&db.pool, &fixture).await?;
        let recovery = tempfile::tempdir().map_err(debug_error)?;
        fs::set_permissions(recovery.path(), fs::Permissions::from_mode(0o700))
            .map_err(debug_error)?;
        for entry in fs::read_dir(&fixture.root).map_err(debug_error)? {
            let entry = entry.map_err(debug_error)?;
            write(
                recovery.path(),
                &entry.file_name().to_string_lossy(),
                &fs::read(entry.path()).map_err(debug_error)?,
            )?;
        }
        let service = store(&db.pool);
        let plan = service
            .authorize_dreamina_video_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        service
            .claim_dreamina_video_supplement_dispatch(
                plan.authorization_id,
                plan.dispatch_nonce,
                &plan.runtime_binding_json,
            )
            .await
            .map_err(debug_error)?
            .ok_or("missing dispatch claim")?;
        let (accepted, _) = evidence(&fixture, &plan)?;
        service
            .record_dreamina_video_supplement_accepted(&accepted)
            .await
            .map_err(debug_error)?;
        let plan = service
            .load_dreamina_video_supplement(plan.authorization_id)
            .await
            .map_err(debug_error)?;
        let cancelled = super::manual_cancellation::cancellation_evidence(&db.pool, &plan).await?;
        service
            .confirm_dreamina_supplement_video_cancellation(&cancelled.root, &cancelled.request)
            .await
            .map_err(debug_error)?;
        let before: serde_json::Value =
            sqlx::query_scalar("SELECT dreamina_video_supplement_original_snapshot($1)")
                .bind(request.job_id)
                .fetch_one(&db.pool)
                .await
                .map_err(debug_error)?;
        let financial: (i64,i64,i64,i64) = sqlx::query_as("SELECT captured_micros,released_micros,(SELECT count(*) FROM dreamina_operator_video_supplement_cancellations WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1) FROM customer_billing_holds WHERE job_id=$1").bind(request.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(financial==(0,1_200_000,1,0),format!("confirmed cancellation did not release the original hold once: {financial:?}"))?;
        require(
            resolve_dreamina_video_archive(
                &db.pool,
                &InMemoryArtifactBlobStore::default(),
                recovery.path(),
                &request,
            )
            .await
            .is_err(),
            "confirmed cancellation was overwritten by archive recovery",
        )?;
        let after: serde_json::Value =
            sqlx::query_scalar("SELECT dreamina_video_supplement_original_snapshot($1)")
                .bind(request.job_id)
                .fetch_one(&db.pool)
                .await
                .map_err(debug_error)?;
        require(
            before == after,
            "rejected archive changed confirmed cancellation or original financial state",
        )?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM dreamina_operator_video_success_resolutions WHERE job_id=$1",
        )
        .bind(request.job_id)
        .fetch_one(&db.pool)
        .await
        .map_err(debug_error)?;
        require(count == 0, "cancelled original acquired archive authority")
    }
    .await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

async fn normal_native_request(
    pool: &PgPool,
    native: &str,
) -> TestResult<(RemoteTaskSubmitReceipt, RemoteTaskAttach)> {
    let work = seed_dreamina_video_generation_lease_for_profile(
        pool,
        "native-identity-worker",
        DREAMINA_VIDEOS_API_PROFILE,
    )
    .await?;
    seed_v4_customer_quote_with_funding(
        pool,
        &work,
        V4CustomerQuoteIdentity::dreamina_video(),
        V4CustomerQuoteBasis {
            metric: "video_requested_second",
            unit: "second",
            unit_size: 1,
            unit_price_micros: 3,
            quantity_source: "request_derived",
            confidence: "exact",
            max_quantity: 8,
            max_amount_micros: 24,
        },
        0,
    )
    .await?;
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    executor
        .prepare_and_handoff(&work, DREAMINA_VIDEO_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    seed_terminal_quota(pool, &work).await?;
    let lease = executor
        .claim_prepared(
            &ExecutorClaimScope {
                execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
                provider_id: DREAMINA_PROVIDER_ID.into(),
                command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
                adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
            },
            "native-identity-executor",
            60_000,
        )
        .await
        .map_err(debug_error)?
        .ok_or("missing normal lease")?;
    executor.start(&lease).await.map_err(debug_error)?;
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).map_err(debug_error)?,
        DreaminaSubmitPayloadV1::new(
            &lease.command_hash,
            parse_submit_command(&serde_json::to_vec(&work.command_json).map_err(debug_error)?)
                .map_err(debug_error)?
                .into_single_output(),
        )
        .map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    let reservation = RemoteTaskSubmitReservation::new(
        &lease,
        format!("native-identity-{}", lease.submission_id),
        command.output(),
        command.identity(),
        60_000,
    );
    let tasks = PostgresProviderTaskStore::new(pool.clone());
    tasks
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    tasks
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    let binding = tasks
        .load_submit_intent(lease.submission_id)
        .await
        .map_err(debug_error)?
        .ok_or("missing normal intent")?
        .execution_binding_sha256;
    let receipt = RemoteTaskSubmitReceipt {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner.clone(),
        executor_lease_epoch: lease.executor_lease_epoch,
        remote_operation_id: native.into(),
        provider_request_id: None,
        event_identity: "normal-native-accepted".into(),
        execution_binding_sha256: binding.clone(),
    };
    let attach = RemoteTaskAttach {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner,
        executor_lease_epoch: lease.executor_lease_epoch,
        remote_operation_id: native.into(),
        provider_request_id: None,
        event_identity: "normal-native-attached".into(),
        execution_binding_sha256: binding,
        poll_after_ms: 0,
        recovery_fence: None,
    };
    Ok((receipt, attach))
}

async fn wait_native_waiters(pool: &PgPool, key: &str, expected: i64) -> TestResult {
    for _ in 0..200 {
        let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND classid=((hashtextextended($1,0)>>32)&4294967295)::oid AND objid=(hashtextextended($1,0)&4294967295)::oid AND objsubid=1")
            .bind(key).fetch_one(pool).await.map_err(debug_error)?;
        if waiting >= expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("expected {expected} writers waiting on native identity lock").into())
}

async fn native_authority_race(archive_first: bool) -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixture=normal_original(&db.pool,"seedance2.0fast_vip",1,0).await?;
        let request=recovery_evidence(&db.pool,&fixture).await?;
        let review:serde_json::Value=serde_json::from_slice(&fs::read(fixture.root.join("review.json")).map_err(debug_error)?).map_err(debug_error)?;
        let native=review["remote_operation_id"].as_str().ok_or("missing native")?.to_string();
        use gpt_image_2_gateway::billing_control::{BillingAccountControlService,BillingControlActor,PostgresBillingAccountControlService,UpdateBillingAccountLimitRequest};
        PostgresBillingAccountControlService::new(db.pool.clone()).update_limit("executord-process-smoke","USD",BillingControlActor{user_id:Uuid::new_v4(),session_id:Uuid::new_v4()},UpdateBillingAccountLimitRequest{credit_limit_micros:"2000000".into(),expected_control_version:"1".into(),reason:"Offline two-original native authority race".into()}).await.map_err(debug_error)?;
        let (receipt,attach)=normal_native_request(&db.pool,&native).await?;
        let key=format!("dreamina-video-success:{DREAMINA_ACCOUNT_ID}:{native}");
        let mut barrier=db.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))").bind(&key).execute(&mut *barrier).await.map_err(debug_error)?;
        let archive_pool=db.pool.clone();let root=fixture.root.clone();let archive_request=request.clone();
        let archive=move||tokio::spawn(async move {resolve_dreamina_video_archive(&archive_pool,&InMemoryArtifactBlobStore::default(),&root,&archive_request).await});
        let normal_pool=db.pool.clone();let normal_request=attach.clone();
        let normal=move||tokio::spawn(async move {
            let tasks=PostgresProviderTaskStore::new(normal_pool);
            tasks.record_submit_receipt(&receipt).await?;
            tasks.attach(&normal_request).await
        });
        let (archive,normal)=if archive_first {
            let a=archive();wait_native_waiters(&db.pool,&key,1).await?;
            let n=normal();wait_native_waiters(&db.pool,&key,2).await?;(a,n)
        }else{
            let n=normal();wait_native_waiters(&db.pool,&key,1).await?;
            let a=archive();wait_native_waiters(&db.pool,&key,2).await?;(a,n)
        };
        barrier.commit().await.map_err(debug_error)?;
        let a=archive.await.map_err(debug_error)?;let n=normal.await.map_err(debug_error)?;
        require(a.is_ok()==archive_first && n.is_ok()!=archive_first,format!("native authority race wrong winner: archive={a:?},normal={n:?}"))?;
        let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_operator_video_success_resolutions WHERE job_id=$1),(SELECT count(*) FROM provider_remote_tasks WHERE remote_operation_id=$2),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1)").bind(request.job_id).bind(&native).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counts==if archive_first{(1,0,1)}else{(0,1,0)},format!("race polluted loser state: {counts:?}"))?;
        let native_intents:i64=sqlx::query_scalar("SELECT count(*) FROM provider_remote_submit_intents WHERE remote_operation_id=$1").bind(&native).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(native_intents==if archive_first{0}else{1},format!("losing writer persisted accepted intent: {native_intents}"))?;
        if !archive_first {
            assert_pending(&db.pool,request.job_id).await?;
            let mut tx=db.pool.begin().await.map_err(debug_error)?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *tx).await.map_err(debug_error)?;
            let rejected=sqlx::query("UPDATE provider_remote_submit_intents SET remote_operation_id=remote_operation_id WHERE submission_id=$1").bind(attach.submission_id).execute(&mut *tx).await;
            require(rejected.as_ref().is_err_and(|error|error.to_string().contains("requires READ COMMITTED")),format!("RR accepted-intent writer not refused narrowly: {rejected:?}"))?;
            tx.rollback().await.map_err(debug_error)?;
            let mut tx=db.pool.begin().await.map_err(debug_error)?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *tx).await.map_err(debug_error)?;
            let rejected=sqlx::query("UPDATE provider_remote_tasks SET remote_operation_id=remote_operation_id WHERE submission_id=$1").bind(attach.submission_id).execute(&mut *tx).await;
            require(rejected.as_ref().is_err_and(|error|error.to_string().contains("requires READ COMMITTED")),format!("RR native writer not refused narrowly: {rejected:?}"))?;
            tx.rollback().await.map_err(debug_error)?;
        }
        let mut tx=db.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *tx).await.map_err(debug_error)?;
        let rejected=sqlx::query("SELECT guard_operator_dreamina_video_native_identity(jsonb_populate_record(NULL::dreamina_operator_video_success_resolutions,'{\"evidence_mode\":\"normal_submit_archive_v1\"}'::jsonb))").execute(&mut *tx).await;
        require(rejected.as_ref().is_err_and(|error|error.to_string().contains("requires READ COMMITTED")),format!("RR archive boundary not refused: {rejected:?}"))?;
        tx.rollback().await.map_err(debug_error)?;
        Ok(())
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}
#[tokio::test]
async fn normal_archive_wins_shared_native_race_and_blocks_other_normal_writer() -> TestResult {
    native_authority_race(true).await
}
#[tokio::test]
async fn normal_archive_loses_shared_native_race_without_delivery_or_charge() -> TestResult {
    native_authority_race(false).await
}
