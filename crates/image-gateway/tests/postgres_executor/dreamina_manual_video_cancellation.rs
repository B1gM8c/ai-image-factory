use super::*;
use gpt_image_2_gateway::{
    DreaminaManualVideoCancellationRequest,
    provider_tasks::{
        ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskObservationSource,
    },
};
use std::path::{Path, PathBuf};

const TENANT: &str = "executord-process-smoke";
const PROJECT: &str = "project-test";

async fn video_with_outcome(
    pool: &PgPool,
    outcome: ProviderTaskObservationOutcome,
) -> TestResult<(WorkLease, ExecutorSubmissionLease, String)> {
    let work = seed_dreamina_video_generation_lease_for_profile(
        pool,
        "manual-cancel-worker",
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
        7,
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
                provider_id: DREAMINA_PROVIDER_ID.to_owned(),
                command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.to_owned(),
                adapter_revision: DREAMINA_ADAPTER_REVISION.to_owned(),
            },
            "manual-cancel-executor",
            60_000,
        )
        .await
        .map_err(debug_error)?
        .ok_or("video executor lease missing")?;
    executor.start(&lease).await.map_err(debug_error)?;
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).map_err(debug_error)?,
        DreaminaSubmitPayloadV1::new(
            &lease.command_hash,
            image_provider_dreamina_cli::parse_submit_command(
                &serde_json::to_vec(&work.command_json).map_err(debug_error)?,
            )
            .map_err(debug_error)?
            .into_single_output(),
        )
        .map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    let tasks = PostgresProviderTaskStore::new(pool.clone());
    let reservation = RemoteTaskSubmitReservation::new(
        &lease,
        format!("manual-cancel-{}", lease.submission_id.simple()),
        command.output(),
        command.identity(),
        60_000,
    );
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
        .ok_or("submit intent missing")?
        .execution_binding_sha256;
    let operation = format!("offline-video-{}", work.job_id.simple());
    tasks
        .record_submit_receipt(&RemoteTaskSubmitReceipt {
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            executor_owner: lease.executor_owner.clone(),
            executor_lease_epoch: lease.executor_lease_epoch,
            remote_operation_id: operation.clone(),
            provider_request_id: None,
            event_identity: "offline-video-accepted".to_owned(),
            execution_binding_sha256: binding.clone(),
        })
        .await
        .map_err(debug_error)?;
    tasks
        .attach(&RemoteTaskAttach {
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            executor_owner: lease.executor_owner.clone(),
            executor_lease_epoch: lease.executor_lease_epoch,
            remote_operation_id: operation.clone(),
            provider_request_id: None,
            event_identity: "offline-video-attach".to_owned(),
            execution_binding_sha256: binding,
            poll_after_ms: 0,
            recovery_fence: None,
        })
        .await
        .map_err(debug_error)?;

    let poll = tasks
        .claim_due(
            &ProviderTaskClaimScope {
                provider_id: DREAMINA_PROVIDER_ID.into(),
                provider_account_id: DREAMINA_ACCOUNT_ID,
                execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
            },
            "manual-cancel-poller",
            60_000,
        )
        .await
        .map_err(debug_error)?
        .ok_or("poll missing")?;
    tasks
        .record_observation(
            &poll,
            &ProviderTaskObservation {
                event_identity: "manual-cancel-failed".into(),
                source: ProviderTaskObservationSource::Poll,
                outcome,
                progress: None,
            },
        )
        .await
        .map_err(debug_error)?;
    let terminals = PostgresExecutorTerminalStore::new(pool.clone());
    let terminal = terminals
        .claim_terminal("manual-cancel-reducer", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("terminal missing")?;
    terminals
        .complete_terminal(&terminal, None)
        .await
        .map_err(debug_error)?;
    Ok((work, lease, operation))
}

fn write_evidence(root: &Path, name: &str, value: &[u8]) -> TestResult {
    fs::write(root.join(name), value).map_err(debug_error)?;
    fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o600)).map_err(debug_error)
}

async fn cancellation_evidence(
    pool: &PgPool,
    work: &WorkLease,
    lease: &ExecutorSubmissionLease,
    operation: &str,
) -> TestResult<(
    tempfile::TempDir,
    PathBuf,
    DreaminaManualVideoCancellationRequest,
)> {
    let directory = tempfile::tempdir().map_err(debug_error)?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
        .map_err(debug_error)?;
    let root = fs::canonicalize(directory.path()).map_err(debug_error)?;
    sqlx::query("INSERT INTO provider_account_environments (provider_account_id,provider_id,environment_kind,environment_ref,upstream_identity_sha256,display_name,state,created_at_ms,updated_at_ms) VALUES ($1,'dreamina-cli','dreamina_home_v1','/offline/cancellation/dreamina-home',$2,'Offline cancellation','active',1,1) ON CONFLICT (provider_account_id) DO NOTHING")
        .bind(DREAMINA_ACCOUNT_ID).bind("a".repeat(64)).execute(pool).await.map_err(debug_error)?;
    let (request_id, upstream, home): (String,String,String) = sqlx::query_as("SELECT job.request_id,environment.upstream_identity_sha256,environment.environment_ref FROM jobs job CROSS JOIN provider_account_environments environment WHERE job.job_id=$1 AND environment.provider_account_id=$2")
        .bind(work.job_id).bind(DREAMINA_ACCOUNT_ID).fetch_one(pool).await.map_err(debug_error)?;
    let stdout = serde_json::to_vec(
        &json!({"submit_id":operation,"gen_status":"fail","fail_reason":"Canceled by user"}),
    )
    .map_err(debug_error)?;
    let hash = |bytes: &[u8]| {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let request = DreaminaManualVideoCancellationRequest {
        job_id: work.job_id,
        request_id,
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        provider_account_id: DREAMINA_ACCOUNT_ID,
        upstream_task_id: operation.into(),
        query_stdout_sha256: hash(&stdout),
        operator_reference: "offline:manual-cancellation".into(),
    };
    let audit=serde_json::to_vec(&json!({
        "schema_version":1,"job_id":request.job_id,"request_id":request.request_id,"submission_id":request.submission_id,
        "executor_execution_id":request.executor_execution_id,"provider_account_id":request.provider_account_id,"upstream_task_id":request.upstream_task_id,
        "upstream_identity_sha256":upstream,"account_home_sha256":hash(home.as_bytes()),
        "argv":["query_result","--submit_id",operation],"stdout_sha256":request.query_stdout_sha256,"exit_code":0,"executed_at_ms":2000
    })).map_err(debug_error)?;
    let mut png = std::io::Cursor::new(Vec::new());
    RgbaImage::new(2, 2)
        .write_to(&mut png, ImageFormat::Png)
        .map_err(debug_error)?;
    let png = png.into_inner();
    let review=serde_json::to_vec(&json!({
        "schema":"factory.manual_cancellation.review.v1","job_id":request.job_id,"request_id":request.request_id,
        "submission_id":request.submission_id,"executor_execution_id":request.executor_execution_id,"provider_account_id":request.provider_account_id,
        "upstream_task_id":operation,"operator_reference":request.operator_reference,"website_cancelled":true,
        "query_stdout_sha256":request.query_stdout_sha256,"execution_audit_sha256":hash(&audit),"website_confirmation_sha256":hash(&png),"confirmed_at_ms":1000
    })).map_err(debug_error)?;
    for (name, bytes) in [
        ("query-stdout.json", stdout),
        ("execution-audit.json", audit),
        ("review.json", review),
        ("website-confirmation.png", png),
    ] {
        write_evidence(&root, name, &bytes)?;
    }
    Ok((directory, root, request))
}

#[tokio::test]
async fn manual_cancellation_preserves_zero_settlement_and_fences_late_callbacks() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let pool=&database.pool;
        let (work,lease,operation)=video_with_outcome(pool,ProviderTaskObservationOutcome::Failed { error_code: "provider_failed".into() }).await?;
        let (_directory,root,request)=cancellation_evidence(pool,&work,&lease,&operation).await?;
        let store=PostgresExecutionSettlementStore::new(pool.clone(),Arc::new(InMemoryArtifactBlobStore::default()));
        let reader: &dyn ExecutionSettlementStore = &store;
        require(reader.project_dreamina_manual_video_cancellation(TENANT,PROJECT,None,work.job_id).await.map_err(debug_error)?.is_none(),"ordinary Failed was exposed as confirmed cancellation")?;
        let before:serde_json::Value=sqlx::query_scalar("SELECT jsonb_build_object('hold',to_jsonb(hold),'quota',to_jsonb(quota),'rating',to_jsonb(rating),'ledgers',(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1)) FROM customer_billing_holds hold JOIN jobs job USING(job_id) JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id JOIN customer_rated_usage rating ON rating.job_id=job.job_id WHERE job.job_id=$1")
          .bind(work.job_id).fetch_one(pool).await.map_err(debug_error)?;
        // An insertion failure must roll back classification without touching money.
        sqlx::query("CREATE FUNCTION cancellation_test_abort() RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'offline rollback'; END; $$ LANGUAGE plpgsql").execute(pool).await.map_err(debug_error)?;
        sqlx::query("CREATE TRIGGER cancellation_test_abort AFTER INSERT ON dreamina_manual_video_cancellations FOR EACH ROW EXECUTE FUNCTION cancellation_test_abort()").execute(pool).await.map_err(debug_error)?;
        require(store.confirm_dreamina_manual_video_cancellation(&root,&request).await.is_err(),"forced rollback succeeded")?;
        sqlx::query("DROP TRIGGER cancellation_test_abort ON dreamina_manual_video_cancellations").execute(pool).await.map_err(debug_error)?;
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM dreamina_manual_video_cancellations").fetch_one(pool).await.map_err(debug_error)?;
        require(count==0,"failed classification persisted authority")?;
        let mut stale=pool.begin().await.map_err(debug_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *stale).await.map_err(debug_error)?;
        sqlx::query("SELECT job_id FROM jobs WHERE job_id=$1").bind(work.job_id).fetch_one(&mut *stale).await.map_err(debug_error)?;
        let (first,second)=tokio::join!(store.confirm_dreamina_manual_video_cancellation(&root,&request),store.confirm_dreamina_manual_video_cancellation(&root,&request));
        let proof=first.map_err(debug_error)?;
        require(second.map_err(debug_error)?==proof,"concurrent confirmation created different proof")?;
        require(proof.schema=="factory.manual_cancellation.v1" && proof.captured_micros==0 && proof.held_micros>0 && proof.released_micros==proof.held_micros,"proof did not retain original full release")?;
        let late=sqlx::query("UPDATE jobs SET updated_at_ms=updated_at_ms WHERE job_id=$1").bind(work.job_id).execute(&mut *stale).await;
        require(late.is_err(),"old RR writer escaped classification tuple fence")?;
        stale.rollback().await.map_err(debug_error)?;
        require(store.confirm_dreamina_manual_video_cancellation(&root,&request).await.map_err(debug_error)?==proof,"repeated confirmation changed proof")?;
        for statement in ["UPDATE customer_billing_holds SET updated_at_ms=updated_at_ms WHERE job_id=$1","UPDATE provider_remote_tasks SET updated_at_ms=updated_at_ms WHERE submission_id=(SELECT submission_id FROM provider_submissions WHERE job_id=$1)","UPDATE dreamina_manual_video_cancellations SET operator_reference=operator_reference WHERE job_id=$1","DELETE FROM dreamina_manual_video_cancellations WHERE job_id=$1"] {
            require(sqlx::query(statement).bind(work.job_id).execute(pool).await.is_err(),format!("late mutation escaped: {statement}"))?;
        }
        require(sqlx::query("TRUNCATE dreamina_manual_video_cancellations").execute(pool).await.is_err(),"authority truncate allowed")?;
        for (tenant,project,actor) in [("other",PROJECT,None),(TENANT,"other",None),(TENANT,PROJECT,Some(Uuid::new_v4()))] {
            require(reader.project_dreamina_manual_video_cancellation(tenant,project,actor,work.job_id).await.map_err(debug_error)?.is_none(),"proof leaked across scope")?;
        }
        require(reader.project_dreamina_manual_video_cancellation(TENANT,PROJECT,None,work.job_id).await.map_err(debug_error)?==Some(proof),"original scope lost proof")?;
        let after:serde_json::Value=sqlx::query_scalar("SELECT jsonb_build_object('hold',to_jsonb(hold),'quota',to_jsonb(quota),'rating',to_jsonb(rating),'ledgers',(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1)) FROM customer_billing_holds hold JOIN jobs job USING(job_id) JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id JOIN customer_rated_usage rating ON rating.job_id=job.job_id WHERE job.job_id=$1")
          .bind(work.job_id).fetch_one(pool).await.map_err(debug_error)?;
        require(before==after,"classification changed money, quota or rating")?;
        let states:(String,String)=sqlx::query_as("SELECT job.state,remote.state FROM jobs job JOIN provider_submissions submission USING(job_id) JOIN provider_remote_tasks remote USING(submission_id) WHERE job.job_id=$1").bind(work.job_id).fetch_one(pool).await.map_err(debug_error)?;
        require(states==("failed".into(),"failed".into()),"classification rewrote canonical failure")?;
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn manual_cancellation_rejects_uncertain_and_succeeded_originals() -> TestResult {
    for outcome in [
        ProviderTaskObservationOutcome::Uncertain {
            error_code: "provider_query_unknown".into(),
        },
        ProviderTaskObservationOutcome::RemoteReady {
            result: image_provider_sdk::RemoteVideoResult::new(
                "https://video.example.invalid/original.mp4".into(),
                None,
            )
            .map_err(debug_error)?,
        },
    ] {
        let Some(database) = TestDatabase::new().await? else {
            return Ok(());
        };
        let result: TestResult = async {
            let pool = &database.pool;
            let (work, lease, operation) = video_with_outcome(pool, outcome).await?;
            let (_directory, root, request) =
                cancellation_evidence(pool, &work, &lease, &operation).await?;
            let store = PostgresExecutionSettlementStore::new(
                pool.clone(),
                Arc::new(InMemoryArtifactBlobStore::default()),
            );
            require(
                store
                    .confirm_dreamina_manual_video_cancellation(&root, &request)
                    .await
                    .is_err(),
                "non-Failed original received cancellation authority",
            )?;
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM dreamina_manual_video_cancellations")
                    .fetch_one(pool)
                    .await
                    .map_err(debug_error)?;
            require(count == 0, "uncertain/success classification persisted")?;
            Ok(())
        }
        .await;
        database.cleanup().await?;
        result?;
    }
    Ok(())
}
