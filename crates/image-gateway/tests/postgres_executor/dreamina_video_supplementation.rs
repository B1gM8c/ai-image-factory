use super::*;
use gpt_image_2_gateway::{
    DreaminaVideoSupplementAcceptedRequest as Accepted,
    DreaminaVideoSupplementAuthorizationRequest as Authorize, DreaminaVideoSupplementPlan,
    DreaminaVideoSupplementPublishRequest as Publish,
    DreaminaVideoSupplementUncertainRequest as Uncertain,
};
use image_provider_dreamina_cli::parse_submit_command;
use std::path::{Path, PathBuf};
const URL: &str = "https://media.example.test/supplement%2Fvideo.mp4?token=A%2Bb";
fn mp4(width: u16, height: u16, duration_ms: u32) -> Vec<u8> {
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
                width,
                height,
                seq_param_set: vec![0x67, 0x42, 0, 0x1e],
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
                duration: duration_ms,
                rendering_offset: 0,
                is_sync: true,
                bytes: vec![0, 0, 0, 1, 0x65].into(),
            },
        )
        .unwrap();
    writer.write_end().unwrap();
    writer.into_writer().into_inner()
}
fn video_bytes() -> &'static [u8] {
    static VIDEO: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    VIDEO.get_or_init(|| mp4(1280, 720, 10_000))
}
struct Fixture {
    request: Authorize,
    root: PathBuf,
    _directory: tempfile::TempDir,
}
fn store(pool: &PgPool) -> PostgresExecutionSettlementStore {
    PostgresExecutionSettlementStore::new(
        pool.clone(),
        Arc::new(InMemoryArtifactBlobStore::default()),
    )
}
async fn fixture(pool: &PgPool, model: &'static str) -> TestResult<Fixture> {
    fixture_with_audio(pool, model, 0).await
}
async fn fixture_with_audio(
    pool: &PgPool,
    model: &'static str,
    audio_count: usize,
) -> TestResult<Fixture> {
    let work = seed_supplement_video(pool, model, audio_count).await?;
    let price = if model == "seedance2.0mini" {
        800_000
    } else {
        1_200_000
    };
    seed_v4_customer_quote_with_basis(pool,&work,V4CustomerQuoteIdentity{provider_model_id:model,public_model_id:model,dimensions:json!({"duration":"10","ratio":"16:9","resolution":"720p","processing_mode":"synchronous","workflow":"multimodal2video","input_image_count":"1","input_video_count":"0","input_audio_count":audio_count.to_string()}),..V4CustomerQuoteIdentity::dreamina_video()},V4CustomerQuoteBasis{metric:"video_requested_second",unit:"second",unit_size:1,unit_price_micros:price/10,quantity_source:"request_derived",confidence:"exact",max_quantity:10,max_amount_micros:price}).await?;
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
        500,
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
    tokio::time::sleep(Duration::from_millis(550)).await;
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
fn write(root: &Path, name: &str, bytes: &[u8]) -> TestResult {
    fs::write(root.join(name), bytes).map_err(debug_error)?;
    fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o600)).map_err(debug_error)
}
fn evidence(
    fixture: &Fixture,
    plan: &DreaminaVideoSupplementPlan,
) -> TestResult<(Accepted, Publish)> {
    evidence_with_video(fixture, plan, video_bytes())
}
fn evidence_with_video(
    fixture: &Fixture,
    plan: &DreaminaVideoSupplementPlan,
    video: &[u8],
) -> TestResult<(Accepted, Publish)> {
    let remote = format!("new-video-{}", plan.authorization_id.simple());
    let submit = serde_json::to_vec(&json!({"submit_id":remote,"gen_status":"querying"}))
        .map_err(debug_error)?;
    let query=serde_json::to_vec(&json!({"submit_id":remote,"gen_status":"success","result_json":{"images":[],"videos":[{"video_url":URL,"duration":10}]}})).map_err(debug_error)?;
    let audit=serde_json::to_vec(&json!({"schema_version":1,"authorization_id":plan.authorization_id,"dispatch_nonce":plan.dispatch_nonce,"job_id":plan.job_id,"submission_id":plan.submission_id,"executor_execution_id":plan.executor_execution_id,"remote_operation_id":remote,"argv":["query_result","--submit_id",remote],"stdout_sha256":sha256(&query),"exit_code":0,"executed_at_ms":1})).map_err(debug_error)?;
    let review=serde_json::to_vec(&json!({"schema_version":1,"stdout_sha256":sha256(&query),"execution_audit_sha256":sha256(&audit),"video_url_sha256":sha256(URL.as_bytes()),"quote_hash":plan.quote_hash,"provider_command_sha256":plan.command_hash,"submit_stdout_sha256":sha256(&submit),"retrieved_video_sha256":sha256(video),"retrieved_video_byte_size":video.len()})).map_err(debug_error)?;
    for (name, bytes) in [
        ("submit-stdout.json", submit.clone()),
        ("reviewed-query.json", query.clone()),
        ("execution-audit.json", audit),
        ("review.json", review),
        ("retrieved-video.mp4", video.to_vec()),
    ] {
        write(&fixture.root, name, &bytes)?;
    }
    Ok((
        Accepted {
            authorization_id: plan.authorization_id,
            dispatch_nonce: plan.dispatch_nonce,
            remote_operation_id: remote,
            submit_stdout_sha256: sha256(&submit),
        },
        Publish {
            authorization_id: plan.authorization_id,
            dispatch_nonce: plan.dispatch_nonce,
            reviewed_stdout_sha256: sha256(&query),
        },
    ))
}
async fn assert_pending(pool: &PgPool, job: Uuid) -> TestResult {
    let status:(String,String,i64,i64,i32,i32)=sqlx::query_as("SELECT job.state,hold.state,hold.captured_micros,hold.released_micros,quota.committed_units,quota.released_units FROM jobs job JOIN customer_billing_holds hold USING(job_id) JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id WHERE job.job_id=$1").bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        status == ("uncertain".into(), "held".into(), 0, 0, 0, 0),
        format!("pending changed original customer status: {status:?}"),
    )
}
async fn seed_supplement_video(
    pool: &PgPool,
    model: &str,
    audio_count: usize,
) -> TestResult<WorkLease> {
    let job_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let images =
        json!([{"filename":"image-0.png","sha256":"a".repeat(64),"width":1280,"height":720}]);
    let audios: Vec<_> = (0..audio_count).map(|index| json!({
        "filename":format!("audio-{index}.mp3"),"sha256":format!("{:064x}",index+1),"duration_ms":4000,
    })).collect();
    let command = json!({"schema_version":1,"operation":"multimodal2video","images":images,
        "videos":[],"audios":audios,"prompt":"offline multimodal regression", "model_version":model,
        "ratio":"16:9","duration":10,"video_resolution":"720p","poll":0});
    parse_submit_command(&serde_json::to_vec(&command).map_err(debug_error)?)
        .map_err(debug_error)?;
    let work = seed_generation_lease(
        pool,
        "operator-video-worker",
        GenerationLeaseSeed {
            job_id,
            admission_session_id: session_id,
            owner_token: Uuid::new_v4(),
            api_profile: DREAMINA_VIDEOS_API_PROFILE.into(),
            provider_id: DREAMINA_PROVIDER_ID.into(),
            model: model.into(),
            request_id: format!("request-{}", Uuid::new_v4().simple()),
            request_hash: sha256(&serde_json::to_vec(&command).map_err(debug_error)?),
            command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
            command_json: command,
            operation: VIDEO_GENERATION_OPERATION.into(),
            work_kind: "video_single".into(),
            output_count: 1,
            billable_units: 10,
            output_billable_units: 10,
            billing_metric: "video_second".into(),
            billing_unit: "second".into(),
        },
    )
    .await?;
    let now = database_now(pool).await?;
    sqlx::query("INSERT INTO job_input_manifests (job_id,admission_session_id,manifest_schema,manifest_hash,input_count,created_at_ms) VALUES ($1,$2,'dreamina.workflow.inputs.v1',$3,$4,$5)")
        .bind(job_id).bind(session_id).bind("a".repeat(64)).bind((audio_count+1) as i16).bind(now).execute(pool).await.map_err(debug_error)?;
    for index in 0..=audio_count {
        let (role, input_index, media_type, digest) = if index == 0 {
            ("image", 0, "image/png", "a".repeat(64))
        } else {
            ("audio", index - 1, "audio/mpeg", format!("{:064x}", index))
        };
        sqlx::query("INSERT INTO job_input_objects (input_id,job_id,admission_session_id,role,input_index,media_type,storage_backend,object_key,sha256_hex,byte_size,created_at_ms) VALUES ($1,$2,$3,$4,$5,$6,'filesystem-input-v1',$7,$8,100,$9)")
            .bind(Uuid::new_v4()).bind(job_id).bind(session_id).bind(role).bind(input_index as i16).bind(media_type)
            .bind(format!("offline/{job_id}/{role}-{input_index}")).bind(digest).bind(now).execute(pool).await.map_err(debug_error)?;
    }
    Ok(work)
}

#[tokio::test]
async fn supplement_progress_tracks_only_the_accepted_attempt_without_settling() -> TestResult {
    use gpt_image_2_gateway::settlement::{VideoPendingStage, VideoResultStatus};
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture_with_audio(&db.pool, "seedance2.0fast_vip", 1).await?;
        let store = store(&db.pool);
        let plan = store.authorize_dreamina_video_supplement(&fixture.request).await.map_err(debug_error)?;
        require(matches!(store.video_status("executord-process-smoke", plan.job_id).await.map_err(debug_error)?, Some(VideoResultStatus::Uncertain { .. })), "unsubmitted supplement claimed acceptance")?;
        store.claim_dreamina_video_supplement_dispatch(plan.authorization_id, plan.dispatch_nonce, &plan.runtime_binding_json).await.map_err(debug_error)?.ok_or("missing one-shot permit")?;
        let (accepted, _) = evidence(&fixture, &plan)?;
        store.record_dreamina_video_supplement_accepted(&accepted).await.map_err(debug_error)?;
        require(matches!(store.video_status("executord-process-smoke", plan.job_id).await.map_err(debug_error)?, Some(VideoResultStatus::Pending { stage: VideoPendingStage::Processing, provider_progress: None, .. })), "accepted attempt did not project pending")?;
        let at = database_now(&db.pool).await?;
        let raw = serde_json::to_vec(&json!({"submit_id":accepted.remote_operation_id,"gen_status":"querying","queue_info":{"queue_idx":610,"queue_length":233061,"queue_status":"Queueing"},"estimated_remaining_seconds":5860})).map_err(debug_error)?;
        store.record_dreamina_video_supplement_observation(plan.authorization_id, plan.dispatch_nonce, &raw, at).await.map_err(debug_error)?;
        store.record_dreamina_video_supplement_observation(plan.authorization_id, plan.dispatch_nonce, &raw, at).await.map_err(debug_error)?;
        require(store.record_dreamina_video_supplement_observation(plan.authorization_id, Uuid::new_v4(), &raw, at).await.is_err(), "unbound nonce accepted")?;
        let statuses = [
            store.video_status("executord-process-smoke", plan.job_id).await.map_err(debug_error)?.ok_or("missing tenant read")?,
            store.project_video_status("executord-process-smoke", "project-test", None, plan.job_id).await.map_err(debug_error)?.ok_or("missing scoped read")?,
            store.project_video_status_with_metadata("executord-process-smoke", "project-test", None, plan.job_id).await.map_err(debug_error)?.ok_or("missing metadata read")?.0,
        ];
        for status in statuses {
            require(matches!(status, VideoResultStatus::Pending { stage: VideoPendingStage::Processing, provider_progress: Some(ref value), .. }
                if value.observed_at_ms == at && value.progress.provider_status == "Queueing"
                    && value.progress.queue_position == Some(610) && value.progress.queue_total == Some(233061)
                    && value.progress.estimated_remaining_seconds == Some(5860)), "queue observation was not projected exactly")?;
        }
        for (tenant, project, actor) in [
            ("wrong-tenant", "project-test", None),
            ("executord-process-smoke", "wrong-project", None),
            ("executord-process-smoke", "project-test", Some(Uuid::new_v4())),
        ] {
            require(store.project_video_status(tenant, project, actor, plan.job_id).await.map_err(debug_error)?.is_none(), "progress escaped original scope")?;
            require(store.project_video_status_with_metadata(tenant, project, actor, plan.job_id).await.map_err(debug_error)?.is_none(), "metadata progress escaped original scope")?;
        }
        for (status, expected_pending) in [("fail", false), ("success", true)] {
            let raw = serde_json::to_vec(&json!({"submit_id":accepted.remote_operation_id,"gen_status":status})).map_err(debug_error)?;
            store.record_dreamina_video_supplement_observation(plan.authorization_id, plan.dispatch_nonce, &raw, at + if expected_pending { 2 } else { 1 }).await.map_err(debug_error)?;
            let read = store.video_status("executord-process-smoke", plan.job_id).await.map_err(debug_error)?.ok_or("missing original")?;
            require(if expected_pending { matches!(read, VideoResultStatus::Pending { provider_progress: None, .. }) } else { matches!(read, VideoResultStatus::Uncertain { .. }) }, "terminal query fabricated delivery or refund")?;
            assert_pending(&db.pool, plan.job_id).await?;
        }
        require(store.claim_dreamina_video_supplement_dispatch(plan.authorization_id, plan.dispatch_nonce, &plan.runtime_binding_json).await.map_err(debug_error)?.is_none(), "progress reopened generation permit")?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dreamina_operator_video_supplement_observations WHERE authorization_id=$1").bind(plan.authorization_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(count == 3, "duplicate observation was not idempotent")?;
        require(sqlx::query("UPDATE dreamina_operator_video_supplement_observations SET query_status='success' WHERE authorization_id=$1").bind(plan.authorization_id).execute(&db.pool).await.is_err(), "observation mutation was allowed")
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supplement_concurrent_claim_has_one_permit_and_failures_hold_funds() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool, "seedance2.0fast_vip").await?;
        let store = store(&db.pool);
        let plan = store
            .authorize_dreamina_video_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        let replay = store
            .authorize_dreamina_video_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        require(
            replay.authorization_id == plan.authorization_id,
            "authorization replay changed identity",
        )?;
        let (a, b) = tokio::join!(
            store.claim_dreamina_video_supplement_dispatch(
                plan.authorization_id,
                plan.dispatch_nonce,
                &plan.runtime_binding_json
            ),
            store.claim_dreamina_video_supplement_dispatch(
                plan.authorization_id,
                plan.dispatch_nonce,
                &plan.runtime_binding_json
            )
        );
        require(
            usize::from(a.map_err(debug_error)?.is_some())
                + usize::from(b.map_err(debug_error)?.is_some())
                == 1,
            "duplicate dispatch permits",
        )?;
        store
            .record_dreamina_video_supplement_uncertain(&Uncertain {
                authorization_id: plan.authorization_id,
                dispatch_nonce: plan.dispatch_nonce,
                error_code: "provider_submit_deadline".into(),
            })
            .await
            .map_err(debug_error)?;
        require(
            store
                .claim_dreamina_video_supplement_dispatch(
                    plan.authorization_id,
                    plan.dispatch_nonce,
                    &plan.runtime_binding_json,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "uncertain was retried",
        )?;
        assert_pending(&db.pool, plan.job_id).await?;
        require(
            matches!(
                store
                    .video_status("executord-process-smoke", plan.job_id)
                    .await
                    .map_err(debug_error)?,
                Some(gpt_image_2_gateway::settlement::VideoResultStatus::Uncertain { .. })
            ),
            "pending GET not uncertain",
        )?;
        Ok(())
    }
    .await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}
#[tokio::test]
async fn supplement_four_multimodal_successes_settle_each_original_hold_once() -> TestResult {
    for (model, audio_count, price) in [
        ("seedance2.0mini", 0, 800_000),
        ("seedance2.0fast_vip", 1, 1_200_000),
        ("seedance2.0fast_vip", 2, 1_200_000),
        ("seedance2.0fast_vip", 2, 1_200_000),
    ] {
        let Some(db) = TestDatabase::new().await? else {
            return Ok(());
        };
        let result=async {
            let fixture=fixture_with_audio(&db.pool,model,audio_count).await?;
            let store=store(&db.pool);
            let plan=store.authorize_dreamina_video_supplement(&fixture.request).await.map_err(debug_error)?;
            require(plan.command_json["operation"]=="multimodal2video" && plan.command_json["images"].as_array().map(Vec::len)==Some(1)
                && plan.command_json["audios"].as_array().map(Vec::len)==Some(audio_count),"original multimodal inputs changed")?;
            store.claim_dreamina_video_supplement_dispatch(plan.authorization_id,plan.dispatch_nonce,&plan.runtime_binding_json).await.map_err(debug_error)?.ok_or("missing permit")?;
            let (accepted,publish)=evidence(&fixture,&plan)?;
            store.record_dreamina_video_supplement_accepted(&accepted).await.map_err(debug_error)?;
            let first=store.publish_dreamina_video_supplement(&fixture.root,&publish).await.map_err(debug_error)?;
            let replay=store.publish_dreamina_video_supplement(&fixture.root,&publish).await.map_err(debug_error)?;
            require(first==replay && first.captured_micros==price && first.released_micros==0 && first.committed_units==10 && first.released_units==0,"success did not settle original hold exactly once")?;
            require((first.video_width,first.video_height,first.video_duration_ms)==(1280,720,10_000),"measured media metadata drifted")?;
            let status=store.video_status("executord-process-smoke",plan.job_id).await.map_err(debug_error)?.ok_or("missing GET")?;
            require(matches!(status,gpt_image_2_gateway::settlement::VideoResultStatus::Succeeded{artifact_id,duration_ms:10_000,..} if artifact_id==first.artifact_id),format!("GET missing supplement artifact {status:?}"))?;
            let downloaded=store.load_video_artifact("executord-process-smoke",first.artifact_id).await.map_err(debug_error)?.ok_or("missing supplement bytes")?;
            require(downloaded.bytes==video_bytes(),"download bytes differ from verified media")?;
            let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM provider_submissions WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),(SELECT count(*) FROM provider_usage_facts WHERE job_id=$1),(SELECT count(*) FROM artifacts WHERE job_id=$1)")
                .bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
            require(counts==(1,1,1,0),"duplicate submission/charge/usage or canonical artifact inserted")?;
            let old:(String,String)=sqlx::query_as("SELECT submission.state,receipt.outcome FROM provider_submissions submission JOIN provider_receipts receipt USING(submission_id) WHERE submission.job_id=$1").bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
            require(old==("uncertain".into(),"uncertain".into()),"original history rewritten")
        }.await;
        let cleanup = db.cleanup().await;
        result.and(cleanup)?;
    }
    Ok(())
}
#[tokio::test]
async fn supplement_rejects_changed_identity_media_and_late_original_mutations() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async{
 let fixture=fixture(&db.pool,"seedance2.0fast_vip").await?;let store=store(&db.pool);
 for field in 0..3{let mut request=fixture.request.clone();match field{0=>request.expected_command_sha256="a".repeat(64),1=>request.expected_quote_hash="b".repeat(64),_=>request.submission_id=Uuid::new_v4()};require(store.authorize_dreamina_video_supplement(&request).await.is_err(),"mismatch authorized")?;}
 let plan=store.authorize_dreamina_video_supplement(&fixture.request).await.map_err(debug_error)?;
 for query in ["UPDATE provider_submissions SET updated_at_ms=updated_at_ms+1 WHERE job_id=$1","UPDATE executor_executions SET updated_at_ms=updated_at_ms+1 WHERE job_id=$1","UPDATE provider_remote_submit_intents SET updated_at_ms=updated_at_ms+1 WHERE job_id=$1","UPDATE provider_receipts SET evidence=evidence || '{\"late_callback\":true}'::jsonb WHERE job_id=$1","UPDATE job_input_objects SET sha256_hex=repeat('f',64) WHERE job_id=$1","UPDATE job_payloads SET command_json=command_json || '{\"prompt\":\"changed\"}'::jsonb WHERE job_id=$1","UPDATE customer_billing_holds SET state='released',released_micros=held_micros,account_released_micros=account_held_micros WHERE job_id=$1","UPDATE jobs SET state='failed' WHERE job_id=$1", "UPDATE jobs SET model='seedance2.0mini' WHERE job_id=$1", "UPDATE jobs SET request_id='changed-request' WHERE job_id=$1", "UPDATE admission_sessions SET api_profile='changed-profile' WHERE job_id=$1", "UPDATE outbox_events SET payload_json='{}'::jsonb WHERE job_id=$1"]{let mut tx=db.pool.begin().await.map_err(debug_error)?;let attempt=sqlx::query(query).bind(plan.job_id).execute(&mut *tx).await;require(if attempt.is_err(){tx.rollback().await.map_err(debug_error)?;true}else{tx.commit().await.is_err()},"late original write escaped supplement fence")?;}
 store.claim_dreamina_video_supplement_dispatch(plan.authorization_id,plan.dispatch_nonce,&plan.runtime_binding_json).await.map_err(debug_error)?;
 let(accepted,publish)=evidence(&fixture,&plan)?;store.record_dreamina_video_supplement_accepted(&accepted).await.map_err(debug_error)?;write(&fixture.root,"retrieved-video.mp4",b"\0\0\0\x18ftypisomDIFFERENT")?;
 require(store.publish_dreamina_video_supplement(&fixture.root,&publish).await.is_err(),"changed media published")?;assert_pending(&db.pool,plan.job_id).await
 }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}
#[tokio::test]
async fn supplement_audit_failure_rolls_back_delivery_and_settlement() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async{
 let fixture=fixture(&db.pool,"seedance2.0fast_vip").await?;let store=store(&db.pool);let plan=store.authorize_dreamina_video_supplement(&fixture.request).await.map_err(debug_error)?;store.claim_dreamina_video_supplement_dispatch(plan.authorization_id,plan.dispatch_nonce,&plan.runtime_binding_json).await.map_err(debug_error)?;let(accepted,publish)=evidence(&fixture,&plan)?;store.record_dreamina_video_supplement_accepted(&accepted).await.map_err(debug_error)?;
 sqlx::raw_sql("CREATE FUNCTION reject_supplement_audit() RETURNS TRIGGER AS $$ BEGIN IF NEW.semantic_key='job.operator_dreamina_video_supplement_success' THEN RAISE EXCEPTION 'offline injected audit failure'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql; CREATE TRIGGER reject_supplement_audit BEFORE INSERT ON outbox_events FOR EACH ROW EXECUTE FUNCTION reject_supplement_audit();").execute(&db.pool).await.map_err(debug_error)?;
 require(store.publish_dreamina_video_supplement(&fixture.root,&publish).await.is_err(),"audit failure committed")?;assert_pending(&db.pool,plan.job_id).await?;let count:i64=sqlx::query_scalar("SELECT count(*) FROM dreamina_operator_video_supplement_resolutions WHERE job_id=$1").bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;require(count==0,"partial resolution persisted")?;
 let counts:(i64,i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM job_response_projections WHERE job_id=$1),(SELECT count(*) FROM provider_usage_facts WHERE job_id=$1),(SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),(SELECT count(*) FROM usage_events WHERE job_id=$1)").bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
 require(counts==(0,0,0,0,0),"audit failure left delivery/usage/rating/ledger effects")?;
 require(store.load_video_artifact("executord-process-smoke",plan.dispatch_nonce).await.map_err(debug_error)?.is_none(),"rolled-back orphan blob became downloadable")?;
 sqlx::query("DROP TRIGGER reject_supplement_audit ON outbox_events").execute(&db.pool).await.map_err(debug_error)?;
 let retried=store.publish_dreamina_video_supplement(&fixture.root,&publish).await.map_err(debug_error)?;
 require(retried.captured_micros==1_200_000,"reviewed publication could not recover atomically after rollback")
 }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supplement_runtime_pin_mismatch_never_claims_and_authorization_cannot_refresh()
-> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async{
 let fixture=fixture(&db.pool,"seedance2.0fast_vip").await?;let store=store(&db.pool);let plan=store.authorize_dreamina_video_supplement(&fixture.request).await.map_err(debug_error)?;
 let mut altered=plan.runtime_binding_json.clone();altered["cli_sha256"]=json!("a".repeat(64));
 require(store.claim_dreamina_video_supplement_dispatch(plan.authorization_id,plan.dispatch_nonce,&altered).await.is_err(),"altered binary pin claimed dispatch")?;
 let mut altered_request=fixture.request.clone();altered_request.runtime_binding_json=altered;
 require(store.authorize_dreamina_video_supplement(&altered_request).await.is_err(),"authorization pins silently refreshed")?;
 sqlx::query("UPDATE provider_account_environments SET state='disabled' WHERE provider_account_id=$1").bind(plan.provider_account_id).execute(&db.pool).await.map_err(debug_error)?;
 require(store.claim_dreamina_video_supplement_dispatch(plan.authorization_id,plan.dispatch_nonce,&plan.runtime_binding_json).await.is_err(),"inactive runtime account claimed dispatch")?;
 let loaded=store.load_dreamina_video_supplement(plan.authorization_id).await.map_err(debug_error)?;require(loaded.state=="authorized","failed runtime claim consumed or reset permit")?;assert_pending(&db.pool,plan.job_id).await
 }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supplement_local_artifact_scopes_and_byte_integrity_survive_old_actor_writes() -> TestResult
{
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool, "seedance2.0fast_vip").await?;
        let artifacts = tempfile::tempdir().map_err(debug_error)?;
        let blobs = FilesystemArtifactBlobStore::new(artifacts.path()).map_err(debug_error)?;
        let store = PostgresExecutionSettlementStore::new(db.pool.clone(), Arc::new(blobs));
        let plan = store
            .authorize_dreamina_video_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        store
            .claim_dreamina_video_supplement_dispatch(
                plan.authorization_id,
                plan.dispatch_nonce,
                &plan.runtime_binding_json,
            )
            .await
            .map_err(debug_error)?;
        let (accepted, publish) = evidence(&fixture, &plan)?;
        store
            .record_dreamina_video_supplement_accepted(&accepted)
            .await
            .map_err(debug_error)?;
        let resolved = store
            .publish_dreamina_video_supplement(&fixture.root, &publish)
            .await
            .map_err(debug_error)?;
        require(
            store
                .load_video_artifact("other-tenant", resolved.artifact_id)
                .await
                .map_err(debug_error)?
                .is_none(),
            "cross tenant download",
        )?;
        for (tenant, project, actor) in [
            ("other-tenant", "project-test", None),
            ("executord-process-smoke", "other-project", None),
            (
                "executord-process-smoke",
                "project-test",
                Some(Uuid::new_v4()),
            ),
        ] {
            require(
                store
                    .load_project_video_artifact(tenant, project, actor, resolved.artifact_id)
                    .await
                    .map_err(debug_error)?
                    .is_none(),
                "cross project/actor download",
            )?;
        }
        let scoped = store
            .load_project_video_artifact(
                "executord-process-smoke",
                "project-test",
                None,
                resolved.artifact_id,
            )
            .await
            .map_err(debug_error)?
            .ok_or("missing scoped bytes")?;
        require(
            scoped.bytes == video_bytes(),
            "scoped download bytes drifted",
        )?;
        write(
            &fixture.root,
            "retrieved-video.mp4",
            b"\0\0\0\x18ftypisomdifferent-old-actor",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_submissions SET updated_at_ms=updated_at_ms+1 WHERE job_id=$1",
            )
            .bind(plan.job_id)
            .execute(&db.pool)
            .await
            .is_err(),
            "old execution mutated after publish",
        )?;
        let after = store
            .load_video_artifact("executord-process-smoke", resolved.artifact_id)
            .await
            .map_err(debug_error)?
            .ok_or("missing frozen bytes")?;
        require(
            after.bytes == video_bytes(),
            "old actor changed published bytes",
        )?;
        let object: String = sqlx::query_scalar(
            "SELECT object_key FROM dreamina_operator_video_supplement_resolutions WHERE job_id=$1",
        )
        .bind(plan.job_id)
        .fetch_one(&db.pool)
        .await
        .map_err(debug_error)?;
        require(
            resolved.artifact_id == plan.dispatch_nonce,
            "blob does not use one-shot nonce",
        )?;
        let metadata = fs::metadata(artifacts.path().join(&object)).map_err(debug_error)?;
        require(
            metadata.permissions().mode() & 0o777 == 0o600,
            "published video is not private",
        )?;
        fs::write(artifacts.path().join(object), b"tampered").map_err(debug_error)?;
        require(
            store
                .load_video_artifact("executord-process-smoke", resolved.artifact_id)
                .await
                .is_err(),
            "tampered immutable blob was served",
        )
    }
    .await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supplement_download_not_ready_or_wrong_measured_media_keeps_original_unsettled()
-> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixture=fixture_with_audio(&db.pool,"seedance2.0fast_vip",2).await?;
        let store=store(&db.pool);
        let plan=store.authorize_dreamina_video_supplement(&fixture.request).await.map_err(debug_error)?;
        store.claim_dreamina_video_supplement_dispatch(plan.authorization_id,plan.dispatch_nonce,&plan.runtime_binding_json).await.map_err(debug_error)?.ok_or("missing dispatch permit")?;
        let (accepted,publish)=evidence(&fixture,&plan)?;
        store.record_dreamina_video_supplement_accepted(&accepted).await.map_err(debug_error)?;
        fs::remove_file(fixture.root.join("retrieved-video.mp4")).map_err(debug_error)?;
        require(store.publish_dreamina_video_supplement(&fixture.root,&publish).await.is_err(),"missing download published")?;
        assert_pending(&db.pool,plan.job_id).await?;
        for bytes in [
            b"\0\0\0\x18ftypisomfake-video".to_vec(),
            mp4(1280,720,9_000),
            mp4(1280,720,11_000),
            mp4(960,720,10_000),
            mp4(1920,1080,10_000),
        ] {
            let (_,publish)=evidence_with_video(&fixture,&plan,&bytes)?;
            require(store.publish_dreamina_video_supplement(&fixture.root,&publish).await.is_err(),"wrong measured media published")?;
            assert_pending(&db.pool,plan.job_id).await?;
            require(matches!(store.video_status("executord-process-smoke",plan.job_id).await.map_err(debug_error)?,
                Some(gpt_image_2_gateway::settlement::VideoResultStatus::Pending{
                    stage:gpt_image_2_gateway::settlement::VideoPendingStage::Processing,
                    provider_progress:None,..})),"unusable download fabricated delivery or queue progress")?;
        }
        let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_operator_video_supplement_resolutions WHERE job_id=$1),(SELECT count(*) FROM provider_usage_facts WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1)")
            .bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counts==(0,0,0),"unready/wrong media settled customer hold")?;
        let padded=mp4(1280,720,10_040);
        let (_,publish)=evidence_with_video(&fixture,&plan,&padded)?;
        let resolved=store.publish_dreamina_video_supplement(&fixture.root,&publish).await.map_err(debug_error)?;
        require(resolved.video_duration_ms==10_040 && resolved.captured_micros==1_200_000 && resolved.committed_units==10,"frame padding changed frozen billing")?;
        require(matches!(store.video_status("executord-process-smoke",plan.job_id).await.map_err(debug_error)?,
            Some(gpt_image_2_gateway::settlement::VideoResultStatus::Succeeded{duration_ms:10_000,..})),"physical padding changed API requested duration")
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supplement_stale_snapshot_cannot_publish_late_history_or_release() -> TestResult {
    for isolation in ["READ COMMITTED", "REPEATABLE READ"] {
        for late_write in [
            "UPDATE outbox_events SET payload_json=payload_json || '{\"late_callback\":true}'::jsonb WHERE job_id=$1",
            "UPDATE customer_billing_holds SET state='released',released_micros=held_micros,account_released_micros=account_held_micros WHERE job_id=$1",
        ] {
            let Some(db) = TestDatabase::new().await? else {
                return Ok(());
            };
            let result = async {
                let fixture = fixture(&db.pool, "seedance2.0fast_vip").await?;
                let mut old = db.pool.begin().await.map_err(debug_error)?;
                sqlx::query(AssertSqlSafe(format!("SET TRANSACTION ISOLATION LEVEL {isolation}")))
                    .execute(&mut *old).await.map_err(debug_error)?;
                // This is the barrier: the old consumer has its snapshot before
                // the authorization commits on an independent connection.
                let _: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE job_id=$1")
                    .bind(fixture.request.job_id).fetch_one(&mut *old).await.map_err(debug_error)?;
                let plan = store(&db.pool).authorize_dreamina_video_supplement(&fixture.request)
                    .await.map_err(debug_error)?;
                let error = match sqlx::query(late_write).bind(plan.job_id).execute(&mut *old).await {
                    Err(error) => { old.rollback().await.map_err(debug_error)?; Some(error) },
                    Ok(_) => old.commit().await.err(),
                }.ok_or_else(|| format!("{isolation}: old writer escaped new authorization fence: {late_write}"))?;
                let detail = error.to_string();
                require(detail.contains("supplement") || detail.contains("concurrent update"),
                    format!("late write failed outside supplement fence: {detail}"))?;
                assert_pending(&db.pool, plan.job_id).await?;
                let changed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM outbox_events WHERE job_id=$1 AND payload_json ? 'late_callback')")
                    .bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
                require(!changed, "late history committed behind frozen snapshot")
            }.await;
            let cleanup = db.cleanup().await;
            result.and(cleanup)?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn supplement_authorization_waits_for_old_history_and_freezes_committed_bytes() -> TestResult
{
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool, "seedance2.0fast_vip").await?;
        let mut old = db.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT 1 FROM jobs WHERE job_id=$1 FOR UPDATE")
            .bind(fixture.request.job_id).execute(&mut *old).await.map_err(debug_error)?;
        sqlx::query("UPDATE outbox_events SET payload_json=payload_json || '{\"before_authorization\":true}'::jsonb WHERE job_id=$1")
            .bind(fixture.request.job_id).execute(&mut *old).await.map_err(debug_error)?;
        let store = store(&db.pool);
        let request = fixture.request.clone();
        let authorization = tokio::spawn(async move {
            store.authorize_dreamina_video_supplement(&request).await.map_err(debug_error)
        });
        let mut blocked = false;
        for _ in 0..100 {
            blocked = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query='SELECT 1 FROM jobs WHERE job_id=$1 FOR UPDATE')")
                .fetch_one(&db.pool).await.map_err(debug_error)?;
            if blocked { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if !blocked {
            old.rollback().await.map_err(debug_error)?;
            authorization.abort();
            return Err("authorization did not wait on old transaction parent lock".into());
        }
        old.commit().await.map_err(debug_error)?;
        let plan = authorization.await.map_err(debug_error)??;
        let frozen: bool = sqlx::query_scalar("SELECT original_evidence_json=dreamina_video_supplement_original_snapshot(job_id) AND original_evidence_json::TEXT LIKE '%before_authorization%' FROM dreamina_operator_video_supplement_authorizations WHERE authorization_id=$1")
            .bind(plan.authorization_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(frozen, "authorization froze a stale original snapshot")?;
        assert_pending(&db.pool, plan.job_id).await
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}
