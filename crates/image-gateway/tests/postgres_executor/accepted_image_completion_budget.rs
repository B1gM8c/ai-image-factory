use super::*;
use std::{borrow::Cow, str::FromStr};

use gpt_image_2_gateway::provider_tasks::{
    ProviderArtifactAuthority, ProviderSubmitFailureKind, ProviderTaskDeadlineStore,
    ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskObservationSource,
    RemoteTaskSubmitFailure,
};
use image_provider_sdk::{Completed, ProviderTaskProgress};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

const COMPLETION_WINDOW_MS: i64 = 86_400_000;
const ORIGINAL_TIMEOUT_MS: i64 = 10 * 60 * 1000;
const PRICE: i64 = 7;

fn scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        provider_id: DREAMINA_PROVIDER_ID.into(),
        provider_account_id: DREAMINA_ACCOUNT_ID,
        execution_profile_id: DREAMINA_PROFILE_ID,
    }
}

// Shadow clocks only in this disposable schema. Original accepted evidence is
// written through the stores and never rewritten to simulate a long queue.
async fn controlled_clock(database: &mut TestDatabase) -> TestResult {
    let schema = &database.schema;
    sqlx::query(AssertSqlSafe(format!(
        "CREATE TABLE \"{schema}\".image_completion_test_clock (offset_ms BIGINT NOT NULL)"
    )))
    .execute(&database.pool)
    .await
    .map_err(debug_error)?;
    sqlx::query("INSERT INTO image_completion_test_clock VALUES (0)")
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
    for function in [
        "clock_timestamp",
        "statement_timestamp",
        "transaction_timestamp",
    ] {
        sqlx::query(AssertSqlSafe(format!(
            "CREATE FUNCTION \"{schema}\".{function}() RETURNS TIMESTAMPTZ LANGUAGE SQL VOLATILE AS \
             'SELECT pg_catalog.{function}() + offset_ms * interval ''1 millisecond'' \
              FROM \"{schema}\".image_completion_test_clock'"
        )))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
    }
    let url = env::var("TEST_DATABASE_URL").map_err(debug_error)?;
    let options = PgConnectOptions::from_str(&url)
        .map_err(debug_error)?
        .options([("search_path", format!("{schema},pg_catalog"))]);
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .map_err(debug_error)?;
    std::mem::replace(&mut database.pool, pool).close().await;
    Ok(())
}

async fn advance_clock(pool: &PgPool, offset_ms: i64) -> TestResult {
    sqlx::query("UPDATE image_completion_test_clock SET offset_ms=$1")
        .bind(offset_ms)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    let delta: i64 = sqlx::query_scalar(
        "SELECT round(extract(epoch FROM (clock_timestamp()-pg_catalog.clock_timestamp()))*1000)::BIGINT",
    ).fetch_one(pool).await.map_err(debug_error)?;
    require(
        (delta - offset_ms).abs() < 100,
        "schema-local image completion clock did not advance",
    )
}

struct AcceptedImage {
    job_id: Uuid,
    submission_id: Uuid,
    original_deadline_at_ms: i64,
    accepted_at_ms: i64,
    remote_operation_id: String,
}

async fn accepted_image(
    pool: &PgPool,
    output_count: u32,
    timeout_ms: i64,
    v4: bool,
    accept: bool,
) -> TestResult<AcceptedImage> {
    let work = seed_dreamina_generation_lease_with_outputs_for_profile(
        pool,
        "image-completion-worker",
        DREAMINA_IMAGES_API_PROFILE,
        output_count,
    )
    .await?;
    if v4 {
        seed_v4_customer_quote_with_basis(
            pool,
            &work,
            V4CustomerQuoteIdentity::dreamina(),
            V4CustomerQuoteBasis {
                metric: "image_output",
                unit: "image",
                unit_size: 1,
                unit_price_micros: PRICE,
                quantity_source: "request_derived",
                confidence: "exact",
                max_quantity: 1,
                max_amount_micros: PRICE,
            },
        )
        .await?;
    }
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    let prepared = executor
        .prepare_and_handoff(&work, DREAMINA_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    require(
        prepared.len() == output_count as usize,
        "accepted image lost frozen outputs",
    )?;
    seed_terminal_quota(pool, &work).await?;
    let lease = executor
        .claim_prepared(
            &ExecutorClaimScope {
                execution_profile_id: DREAMINA_PROFILE_ID,
                provider_id: DREAMINA_PROVIDER_ID.into(),
                command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
                adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
            },
            "image-completion-executor",
            60_000,
        )
        .await
        .map_err(debug_error)?
        .ok_or("image leader missing")?;
    require(
        lease.output_index == 0,
        "native follower acquired launch authority",
    )?;
    executor.start(&lease).await.map_err(debug_error)?;
    let source = serde_json::to_vec(&work.command_json).map_err(debug_error)?;
    let payload = if output_count == 1 {
        DreaminaSubmitPayloadV1::new(
            &lease.command_hash,
            image_provider_dreamina_cli::parse_submit_command(&source).map_err(debug_error)?,
        )
        .map_err(debug_error)?
    } else {
        DreaminaSubmitPayloadV1::new_native_image_batch(
            &lease.command_hash,
            parse_native_image_batch_command(&source).map_err(debug_error)?,
        )
        .map_err(debug_error)?
    };
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, output_count).map_err(debug_error)?,
        payload,
    )
    .map_err(debug_error)?;
    let tasks = PostgresProviderTaskStore::new(pool.clone());
    let reservation = RemoteTaskSubmitReservation::new(
        &lease,
        format!("image-completion-{}", lease.submission_id.simple()),
        command.output(),
        command.identity(),
        timeout_ms,
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
        .ok_or("image submit intent missing")?
        .execution_binding_sha256;
    let remote_operation_id = format!("accepted-image-{}", work.job_id.simple());
    if accept {
        tasks
            .record_submit_receipt(&RemoteTaskSubmitReceipt {
                submission_id: lease.submission_id,
                executor_execution_id: lease.executor_execution_id,
                executor_owner: lease.executor_owner.clone(),
                executor_lease_epoch: lease.executor_lease_epoch,
                remote_operation_id: remote_operation_id.clone(),
                provider_request_id: None,
                event_identity: "image-completion-receipt".into(),
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
                remote_operation_id: remote_operation_id.clone(),
                provider_request_id: None,
                event_identity: "image-completion-attach".into(),
                execution_binding_sha256: binding,
                poll_after_ms: 0,
                recovery_fence: None,
            })
            .await
            .map_err(debug_error)?;
    } else {
        tasks
            .record_submit_failure(&RemoteTaskSubmitFailure {
                submission_id: lease.submission_id,
                executor_execution_id: lease.executor_execution_id,
                executor_owner: lease.executor_owner.clone(),
                executor_lease_epoch: lease.executor_lease_epoch,
                kind: ProviderSubmitFailureKind::OutcomeUnknown,
                event_identity: "image-completion-lost-receipt".into(),
                error_code: "offline_submit_receipt_lost".into(),
                execution_binding_sha256: binding,
                recovery_fence: None,
            })
            .await
            .map_err(debug_error)?;
    }
    let (original_deadline_at_ms, accepted_at_ms) = if accept {
        sqlx::query_as("SELECT provider_deadline_at_ms,created_at_ms FROM provider_remote_tasks WHERE submission_id=$1")
            .bind(lease.submission_id).fetch_one(pool).await.map_err(debug_error)?
    } else {
        sqlx::query_as("SELECT provider_deadline_at_ms,created_at_ms FROM provider_submit_recoveries WHERE submission_id=$1")
            .bind(lease.submission_id).fetch_one(pool).await.map_err(debug_error)?
    };
    Ok(AcceptedImage {
        job_id: work.job_id,
        submission_id: lease.submission_id,
        original_deadline_at_ms,
        accepted_at_ms,
        remote_operation_id,
    })
}

async fn immutable_submit_fingerprint(pool: &PgPool, submission_id: Uuid) -> TestResult<String> {
    sqlx::query_scalar("SELECT encode(sha256(convert_to(jsonb_build_object(\
        'intent',to_jsonb(intent),'recovery',to_jsonb(recovery),\
        'payload',to_jsonb(payload),'quote',to_jsonb(quote),\
        'command_hash',submission.command_hash,'request_hash',payload.request_hash)::TEXT,'UTF8')),'hex') \
        FROM provider_remote_submit_intents intent JOIN provider_submit_recoveries recovery USING(submission_id) \
        JOIN provider_submissions submission USING(submission_id) JOIN jobs job USING(job_id) \
        JOIN job_payloads payload USING(job_id) LEFT JOIN customer_price_quotes quote USING(job_id) \
        WHERE intent.submission_id=$1")
        .bind(submission_id).fetch_one(pool).await.map_err(debug_error)
}

struct SingleImageResults {
    expected_remote_id: String,
    calls: Arc<AtomicUsize>,
}
impl ProviderPollDriver for SingleImageResults {
    fn provider_id(&self) -> &'static str {
        DREAMINA_PROVIDER_ID
    }
    async fn poll<S: ArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        assert_eq!(call.operation().operation_id(), self.expected_remote_id);
        self.calls.fetch_add(1, Ordering::SeqCst);
        sink.write_chunk(&png_bytes([10, 20, 30, 255]))
            .await
            .map_err(offline_batch_artifact_failure)?;
        let manifest = sink
            .finalize(ProviderArtifactMetadata {
                media_type: "image/png",
            })
            .await
            .map_err(offline_batch_artifact_failure)?;
        Ok(PollObservation::Completed(Completed::new(manifest, None)))
    }
}

async fn settle_images(
    pool: &PgPool,
    blobs: Arc<FilesystemArtifactBlobStore>,
    count: u32,
) -> TestResult {
    let reductions = PostgresExecutorTerminalStore::new(pool.clone());
    let customer = CustomerArtifactPublisher::new(blobs);
    for index in 0..count {
        let terminal = reductions
            .claim_terminal(&format!("image-completion-reducer-{index}"), 60_000)
            .await
            .map_err(debug_error)?
            .ok_or("image terminal missing")?;
        let artifact = if matches!(&terminal.outcome, CanonicalExecutorOutcome::Succeeded(_)) {
            Some(customer.publish(&terminal).await.map_err(debug_error)?)
        } else {
            require(
                matches!(&terminal.outcome,CanonicalExecutorOutcome::Failed {error_code}
                if error_code=="dreamina_native_batch_result_missing"),
                "unexpected missing-image outcome",
            )?;
            None
        };
        let first = reductions
            .complete_terminal(&terminal, artifact.as_ref())
            .await
            .map_err(debug_error)?;
        require(
            first
                == reductions
                    .complete_terminal(&terminal, artifact.as_ref())
                    .await
                    .map_err(debug_error)?,
            "image completion replay changed settlement",
        )?;
    }
    require(
        reductions
            .claim_terminal("image-completion-after-replay", 60_000)
            .await
            .map_err(debug_error)?
            .is_none(),
        "image terminal was reduced repeatedly",
    )
}

#[tokio::test]
async fn accepted_single_image_keeps_original_id_after_ten_minutes_and_nine_hours() -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        controlled_clock(&mut database).await?;
        let pool=&database.pool;
        let image=accepted_image(pool,1,ORIGINAL_TIMEOUT_MS,true,true).await?;
        let frozen=immutable_submit_fingerprint(pool,image.submission_id).await?;
        let grant:(i64,i64,i64,String)=sqlx::query_as("SELECT original_submit_deadline_at_ms,accepted_at_ms,completion_deadline_at_ms,grant_source FROM accepted_completion_budgets WHERE submission_id=$1")
            .bind(image.submission_id).fetch_one(pool).await.map_err(debug_error)?;
        require(grant==(image.original_deadline_at_ms,image.accepted_at_ms,image.accepted_at_ms+COMPLETION_WINDOW_MS,"accepted_attach".into()),
            "image completion grant reset its original acceptance anchor")?;
        let tasks=PostgresProviderTaskStore::new(pool.clone());
        advance_clock(pool,ORIGINAL_TIMEOUT_MS+1000).await?;
        let poll=tasks.claim_due(&scope(),"image-after-ten-minutes",60_000).await.map_err(debug_error)?
            .ok_or("accepted image stopped polling at ten minutes")?;
        require(poll.task.remote_operation_id==image.remote_operation_id
            && poll.context().provider_deadline_at_ms()==image.original_deadline_at_ms
            && poll.completion_deadline_at_ms()==image.accepted_at_ms+COMPLETION_WINDOW_MS,
            "image poll lost original submit identity or separate completion authority")?;
        let poll=tasks.heartbeat(&poll,60_000).await.map_err(debug_error)?;
        tasks.record_observation(&poll,&ProviderTaskObservation {
            event_identity:"image-long-queue".into(),source:ProviderTaskObservationSource::Poll,
            outcome:ProviderTaskObservationOutcome::Waiting {poll_after_ms:0},
            progress:Some(ProviderTaskProgress {provider_status:"Queueing".into(),queue_position:Some(9),
                queue_total:Some(100),estimated_remaining_seconds:Some(3600)}),
        }).await.map_err(debug_error)?;
        advance_clock(pool,9*60*60*1000).await?;
        require(tasks.resolve_due_remote_task_deadline(&scope()).await.map_err(debug_error)?.is_none(),
            "image long queue was quarantined by the immutable submit deadline")?;
        let artifacts=tempfile::tempdir().map_err(debug_error)?;
        let blobs=Arc::new(FilesystemArtifactBlobStore::new(artifacts.path()).map_err(debug_error)?);
        let calls=Arc::new(AtomicUsize::new(0));
        let poller=ProviderPollOrchestrator::new(tasks,
            SingleImageResults {expected_remote_id:image.remote_operation_id.clone(),calls:Arc::clone(&calls)},
            gpt_image_2_gateway::FilesystemProviderArtifactStagerFactory::new(Arc::clone(&blobs),1024*1024).map_err(debug_error)?,
            ProviderPollOrchestratorConfig {scope:scope(),owner:"image-nine-hour-poller".into(),lease_ms:5000,
                heartbeat_interval:Duration::from_secs(1),max_materializations:1}).map_err(debug_error)?;
        require(matches!(poller.run_once().await.map_err(debug_error)?,ProviderPollRun::Observed(ref task)
            if task.state==ProviderTaskState::ArtifactReady),"long queued image did not archive successfully")?;
        settle_images(pool,blobs,1).await?;
        require(matches!(poller.run_once().await.map_err(debug_error)?,ProviderPollRun::Idle)
            && calls.load(Ordering::SeqCst)==1,"completed image was polled again")?;
        let state:(i64,i64,i64,i64,i64)=sqlx::query_as("SELECT \
            (SELECT count(*) FROM provider_remote_tasks WHERE submission_id=$1),\
            (SELECT invocation_attempt::BIGINT FROM provider_submit_recoveries WHERE submission_id=$1),\
            (SELECT count(*) FROM artifacts WHERE job_id=$2),\
            (SELECT count(*) FROM ledger_transactions WHERE source_job_id=$2 AND transaction_type='customer_job_charge'),\
            (SELECT count(*) FROM provider_remote_task_quarantines WHERE submission_id=$1)")
            .bind(image.submission_id).bind(image.job_id).fetch_one(pool).await.map_err(debug_error)?;
        require(state==(1,1,1,1,0),format!("long queued image generated or settled repeatedly: {state:?}"))?;
        require(immutable_submit_fingerprint(pool,image.submission_id).await?==frozen,
            "long image completion rewrote original submit, payload or quote evidence")
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn accepted_native_images_materialize_after_ten_minutes_and_settle_each_output_once()
-> TestResult {
    for (count, delivered) in [(2, 2), (3, 2), (10, 10)] {
        let Some(mut database) = TestDatabase::new().await? else {
            return Ok(());
        };
        let result=async {
            controlled_clock(&mut database).await?;
            let pool=&database.pool;
            let image=accepted_image(pool,count,ORIGINAL_TIMEOUT_MS,true,true).await?;
            let frozen=immutable_submit_fingerprint(pool,image.submission_id).await?;
            advance_clock(pool,9*60*60*1000).await?;
            let artifacts=tempfile::tempdir().map_err(debug_error)?;
            let blobs=Arc::new(FilesystemArtifactBlobStore::new(artifacts.path()).map_err(debug_error)?);
            let calls=Arc::new(AtomicUsize::new(0));
            let poller=ProviderPollOrchestrator::new(PostgresProviderTaskStore::new(pool.clone()),
                OfflineDreaminaBatchResults {delivered,poll_calls:Arc::clone(&calls)},
                gpt_image_2_gateway::FilesystemProviderArtifactStagerFactory::new(Arc::clone(&blobs),1024*1024).map_err(debug_error)?,
                ProviderPollOrchestratorConfig {scope:scope(),owner:"late-native-image-poller".into(),lease_ms:5000,
                    heartbeat_interval:Duration::from_secs(1),max_materializations:1}).map_err(debug_error)?;
            require(matches!(poller.run_once().await.map_err(debug_error)?,ProviderPollRun::Observed(ref task)
                if task.state==ProviderTaskState::ArtifactReady),"late native images failed materialization")?;
            drop(poller);
            settle_images(pool,Arc::clone(&blobs),count).await?;
            let restarted=ProviderPollOrchestrator::new(PostgresProviderTaskStore::new(pool.clone()),
                OfflineDreaminaBatchResults {delivered,poll_calls:Arc::clone(&calls)},
                gpt_image_2_gateway::FilesystemProviderArtifactStagerFactory::new(blobs,1024*1024).map_err(debug_error)?,
                ProviderPollOrchestratorConfig {scope:scope(),owner:"restarted-native-image-poller".into(),lease_ms:5000,
                    heartbeat_interval:Duration::from_secs(1),max_materializations:1}).map_err(debug_error)?;
            require(matches!(restarted.run_once().await.map_err(debug_error)?,ProviderPollRun::Idle)
                && calls.load(Ordering::SeqCst)==1,"native restart submitted or polled a finished operation again")?;
            let state:(i64,i64,i64,i64,i64,i64,i64,i64)=sqlx::query_as("SELECT \
                (SELECT count(*) FROM provider_remote_tasks task JOIN provider_submissions submission USING(submission_id) WHERE submission.job_id=$1),\
                (SELECT count(*) FROM provider_remote_submit_intents intent JOIN provider_submissions submission USING(submission_id) WHERE submission.job_id=$1),\
                (SELECT invocation_attempt::BIGINT FROM provider_submit_recoveries WHERE submission_id=$2),\
                (SELECT count(*) FROM accepted_completion_budgets budget JOIN provider_submissions submission USING(submission_id) WHERE submission.job_id=$1),\
                (SELECT count(*) FROM provider_receipts WHERE job_id=$1),\
                (SELECT count(*) FROM artifacts WHERE job_id=$1),\
                (SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1 AND transaction_type='customer_job_charge'),\
                (SELECT count(*) FROM dreamina_native_batch_results result JOIN dreamina_native_batch_members member USING(submission_id,executor_execution_id) WHERE member.leader_submission_id=$2)")
                .bind(image.job_id).bind(image.submission_id).fetch_one(pool).await.map_err(debug_error)?;
            require(state==(1,1,1,1,i64::from(count),delivered as i64,1,i64::from(count-1)),
                format!("native image authorities or per-image completion were duplicated: {state:?}"))?;
            let financial:(i32,i32,i32,i64,i64)=sqlx::query_as("SELECT job.charged_units,quota.committed_units,quota.released_units,hold.captured_micros,hold.released_micros FROM jobs job JOIN quota_reservations quota USING(job_id) JOIN customer_billing_holds hold USING(job_id) WHERE job.job_id=$1")
                .bind(image.job_id).fetch_one(pool).await.map_err(debug_error)?;
            require(financial==(delivered as i32,delivered as i32,count as i32-delivered as i32,
                delivered as i64*PRICE,(i64::from(count)-delivered as i64)*PRICE),
                format!("native image amounts did not match delivered outputs: {financial:?}"))?;
            require(immutable_submit_fingerprint(pool,image.submission_id).await?==frozen,
                "native completion rewrote original submit authority")
        }.await;
        combine(result, database.cleanup().await)?;
    }
    Ok(())
}

#[tokio::test]
async fn native_follower_partial_publication_replays_after_store_restart_with_leader_budget()
-> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        controlled_clock(&mut database).await?;
        let pool=&database.pool;
        let image=accepted_image(pool,3,ORIGINAL_TIMEOUT_MS,true,true).await?;
        advance_clock(pool,ORIGINAL_TIMEOUT_MS+1000).await?;
        let tasks=PostgresProviderTaskStore::new(pool.clone());
        let poll=tasks.claim_due(&scope(),"partial-native-image-poller",60_000).await.map_err(debug_error)?
            .ok_or("late native leader was not claimable")?;
        let members=tasks.native_batch_members(&poll).await.map_err(debug_error)?.ok_or("native members missing")?;
        let id=members[1].executor_execution_id.simple().to_string();
        let authority=ProviderArtifactAuthority::new("filesystem-v1".into(),"filesystem-v1:completion-test".into(),
            format!("executor-objects/{}/{}",&id[..2],id),"b".repeat(64),128,"image/png".into())
            .ok_or("native authority fixture invalid")?;
        let first=tasks.publish_native_batch_artifact(&poll,1,&authority).await.map_err(debug_error)?;
        drop(tasks);
        let restarted=PostgresProviderTaskStore::new(pool.clone());
        let replay=restarted.publish_native_batch_artifact(&poll,1,&authority).await.map_err(debug_error)?;
        require(first==replay,"native follower replay changed immutable result mapping")?;
        let members=restarted.native_batch_members(&poll).await.map_err(debug_error)?.ok_or("restarted members missing")?;
        require(members[1].committed_artifact==Some(first),"partial follower metadata was lost on restart")?;
        restarted.record_native_batch_failure(&poll,2,"dreamina_native_batch_result_missing").await.map_err(debug_error)?;
        let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT \
            (SELECT count(*) FROM executor_result_manifests WHERE submission_id IN (SELECT submission_id FROM dreamina_native_batch_members WHERE leader_submission_id=$1)),\
            (SELECT count(*) FROM dreamina_native_batch_results result JOIN dreamina_native_batch_members member USING(submission_id,executor_execution_id) WHERE member.leader_submission_id=$1),\
            (SELECT count(*) FROM provider_remote_tasks),\
            (SELECT invocation_attempt::BIGINT FROM provider_submit_recoveries WHERE submission_id=$1)")
            .bind(image.submission_id).fetch_one(pool).await.map_err(debug_error)?;
        require(counts==(1,2,1,1),format!("partial native result was repeated or independently submitted: {counts:?}"))
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn accepted_image_completion_expiry_keeps_unknown_hold_and_quota_without_resubmit()
-> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        controlled_clock(&mut database).await?;
        let pool=&database.pool;
        let image=accepted_image(pool,1,ORIGINAL_TIMEOUT_MS,true,true).await?;
        let frozen=immutable_submit_fingerprint(pool,image.submission_id).await?;
        let tasks=PostgresProviderTaskStore::new(pool.clone());
        let stale=tasks.claim_due(&scope(),"expiring-image-poll",60_000).await.map_err(debug_error)?.ok_or("image poll missing")?;
        advance_clock(pool,COMPLETION_WINDOW_MS+1000).await?;
        let task=tasks.resolve_due_remote_task_deadline(&scope()).await.map_err(debug_error)?.ok_or("image 24h expiry unresolved")?;
        require(task.submission_id==image.submission_id && task.state==ProviderTaskState::Uncertain,
            "image completion expiry changed identity or pretended confirmed failure")?;
        require(tasks.heartbeat(&stale,60_000).await.is_err()
            && tasks.claim_due(&scope(),"image-after-expiry",60_000).await.map_err(debug_error)?.is_none()
            && tasks.resolve_due_remote_task_deadline(&scope()).await.map_err(debug_error)?.is_none(),
            "expired image retained polling authority or repeated deadline resolution")?;
        let reductions=PostgresExecutorTerminalStore::new(pool.clone());
        let terminal=reductions.claim_terminal("image-expiry-reducer",60_000).await.map_err(debug_error)?.ok_or("image expiry terminal missing")?;
        require(matches!(&terminal.outcome,CanonicalExecutorOutcome::Uncertain {error_code} if error_code=="provider_remote_task_deadline"),
            "image expiry did not preserve unknown upstream effect")?;
        reductions.complete_terminal(&terminal,None).await.map_err(debug_error)?;
        let financial:(String,i64,i64,i64,i32,i32,i32,String)=sqlx::query_as("SELECT hold.state,hold.held_micros,hold.captured_micros,hold.released_micros,job.charged_units,quota.committed_units,quota.released_units,allocation.state FROM jobs job JOIN customer_billing_holds hold USING(job_id) JOIN quota_reservations quota USING(job_id) JOIN provider_submissions submission USING(job_id) JOIN executor_capacity_allocations allocation USING(submission_id) WHERE job.job_id=$1")
            .bind(image.job_id).fetch_one(pool).await.map_err(debug_error)?;
        require(financial==("held".into(),PRICE,0,0,0,0,0,"held".into()),
            format!("unknown image completion was refunded, charged or released: {financial:?}"))?;
        let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM provider_remote_tasks WHERE submission_id=$1),(SELECT invocation_attempt::BIGINT FROM provider_submit_recoveries WHERE submission_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$2),(SELECT count(*) FROM provider_remote_task_quarantines WHERE submission_id=$1)")
            .bind(image.submission_id).bind(image.job_id).fetch_one(pool).await.map_err(debug_error)?;
        require(counts==(1,1,0,1),format!("image expiry repeated generation or effects: {counts:?}"))?;
        require(immutable_submit_fingerprint(pool,image.submission_id).await?==frozen,
            "image expiry rewrote original immutable submit evidence")
    }.await;
    combine(result, database.cleanup().await)
}

pub(super) async fn database_before_image_completion_budget() -> TestResult<Option<TestDatabase>> {
    let Some(url) = env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        if env::var_os("CI").is_some() {
            return Err("TEST_DATABASE_URL must be set in CI".into());
        }
        return Ok(None);
    };
    let schema = format!("image_completion_upgrade_{}", Uuid::new_v4().simple());
    let pool = connect_test_pool_with_search_path(&url, 8, &schema)
        .await
        .map_err(debug_error)?;
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .map_err(debug_error)?;
    require(
        name.to_ascii_lowercase().contains("test"),
        "refusing image upgrade fixture DDL outside a test database",
    )?;
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&pool)
        .await
        .map_err(debug_error)?;
    let database = TestDatabase { schema, pool };
    let result = async {
        let mut migrations = sqlx::migrate!("./migrations");
        migrations.migrations = Cow::Owned(
            migrations
                .iter()
                .filter(|migration| migration.version <= 146)
                .cloned()
                .collect(),
        );
        migrations.run(&database.pool).await.map_err(debug_error)?;
        seed_execution_profiles(&database.pool).await
    }
    .await;
    if let Err(error) = result {
        database.cleanup().await?;
        return Err(error);
    }
    Ok(Some(database))
}

#[tokio::test]
async fn migration_147_backfills_only_original_accepted_unleased_live_image_windows() -> TestResult
{
    for scenario in [
        "single",
        "native",
        "active_lease",
        "expired_window",
        "non_v4",
        "unknown_submit",
        "old_uncertain",
        "longer_original",
    ] {
        let Some(mut database) = database_before_image_completion_budget().await? else {
            return Ok(());
        };
        let result=async {
            controlled_clock(&mut database).await?;
            let pool=&database.pool;
            let timeout=match scenario {"active_lease"=>60*60*1000,"longer_original"=>2*COMPLETION_WINDOW_MS,_=>ORIGINAL_TIMEOUT_MS};
            let image=accepted_image(pool,if scenario=="native" {3} else {1},timeout,
                scenario!="non_v4",scenario!="unknown_submit").await?;
            let frozen=immutable_submit_fingerprint(pool,image.submission_id).await?;
            let tasks=PostgresProviderTaskStore::new(pool.clone());
            let active=if scenario=="active_lease" {
                Some(tasks.claim_due(&scope(),"pre-upgrade-image-poller",60_000).await.map_err(debug_error)?.ok_or("historical image lease missing")?)
            } else {None};
            if scenario=="old_uncertain" {
                let poll=tasks.claim_due(&scope(),"pre-upgrade-uncertain-poller",60_000).await.map_err(debug_error)?.ok_or("historical uncertain image poll missing")?;
                tasks.record_observation(&poll,&ProviderTaskObservation {event_identity:"pre-upgrade-image-uncertain".into(),
                    source:ProviderTaskObservationSource::Poll,outcome:ProviderTaskObservationOutcome::Uncertain {error_code:"offline_image_result_unknown".into()},progress:None})
                    .await.map_err(debug_error)?;
            }
            if scenario=="expired_window" {advance_clock(pool,COMPLETION_WINDOW_MS+1000).await?;}
            else if scenario!="active_lease" {advance_clock(pool,9*60*60*1000).await?;}
            let before:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT max(version) FROM _sqlx_migrations),(SELECT count(*) FROM provider_task_observations),(SELECT count(*) FROM provider_remote_tasks)")
                .fetch_one(pool).await.map_err(debug_error)?;
            require(before.0==146,"image history fixture did not use real migration 146")?;
            run_migrations(pool).await.map_err(debug_error)?;
            let grants:Vec<(Uuid,i64,i64,i64,String)>=sqlx::query_as("SELECT submission_id,original_submit_deadline_at_ms,accepted_at_ms,completion_deadline_at_ms,grant_source FROM accepted_completion_budgets")
                .fetch_all(pool).await.map_err(debug_error)?;
            if matches!(scenario,"single"|"native") {
                require(grants==vec![(image.submission_id,image.original_deadline_at_ms,image.accepted_at_ms,image.accepted_at_ms+COMPLETION_WINDOW_MS,"migration_0147".into())],
                    format!("eligible historical image lost original accepted budget: {scenario} {grants:?}"))?;
                let poll=tasks.claim_due(&scope(),"post-upgrade-image-poller",60_000).await.map_err(debug_error)?.ok_or("historical image not claimable after upgrade")?;
                require(poll.task.remote_operation_id==image.remote_operation_id
                    && poll.context().provider_deadline_at_ms()==image.original_deadline_at_ms
                    && poll.completion_deadline_at_ms()==image.accepted_at_ms+COMPLETION_WINDOW_MS,
                    "historical image grant reset original remote ID, deadline or acceptance time")?;
            } else {
                require(grants.is_empty(),format!("147 extended ineligible history: {scenario} {grants:?}"))?;
            }
            if let Some(active)=active {
                tasks.heartbeat(&active,60_000).await.map_err(debug_error)?;
            }
            let after:(i64,i64,bool)=sqlx::query_as("SELECT (SELECT count(*) FROM provider_task_observations),(SELECT count(*) FROM provider_remote_tasks),EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=147 AND success)")
                .fetch_one(pool).await.map_err(debug_error)?;
            require(after==(before.1,before.2,true),"147 fabricated an observation/task or was not recorded as a real upgrade")?;
            require(immutable_submit_fingerprint(pool,image.submission_id).await?==frozen,
                "147 changed immutable historical submit, request or quote bytes")
        }.await;
        combine(result, database.cleanup().await)?;
    }
    Ok(())
}

#[tokio::test]
async fn longer_original_image_deadline_is_preserved_and_lease_seal_remains_bound() -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        controlled_clock(&mut database).await?;
        let pool = &database.pool;
        let image = accepted_image(pool, 1, 2 * COMPLETION_WINDOW_MS, true, true).await?;
        let grants: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM accepted_completion_budgets WHERE submission_id=$1",
        )
        .bind(image.submission_id)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
        require(
            grants == 0,
            "image policy shortened or replaced a longer original contract",
        )?;
        advance_clock(pool, COMPLETION_WINDOW_MS + 1000).await?;
        let tasks = PostgresProviderTaskStore::new(pool.clone());
        let poll = tasks
            .claim_due(&scope(), "longer-original-image-poller", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or("longer original image deadline was lost")?;
        require(
            poll.completion_deadline_at_ms() == image.original_deadline_at_ms
                && poll.context().provider_deadline_at_ms() == image.original_deadline_at_ms,
            "image completion deadline no longer preserves longer original authority",
        )?;
        let renewed = tasks.heartbeat(&poll, 60_000).await.map_err(debug_error)?;
        let mut forged = renewed.clone();
        forged.task.remote_operation_id.push_str("-changed");
        require(
            tasks.heartbeat(&forged, 60_000).await.is_err(),
            "lease authority seal accepted a changed original remote ID",
        )?;
        let mut forged = renewed.clone();
        forged.task.provider_account_id = Uuid::new_v4();
        require(
            tasks.heartbeat(&forged, 60_000).await.is_err(),
            "lease authority seal accepted a changed original account binding",
        )?;
        require(
            tasks
                .resolve_due_remote_task_deadline(&scope())
                .await
                .map_err(debug_error)?
                .is_none(),
            "image policy quarantined an original deadline longer than 24h",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}
