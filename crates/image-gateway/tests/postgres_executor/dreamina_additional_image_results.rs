use super::*;
use gpt_image_2_gateway::{
    provider_tasks::{
        ProviderAdditionalImageArtifactSink, ProviderArtifactAuthority, ProviderTaskLease,
        ProviderTaskStoreError, additional_image_artifact_id,
    },
    retention::{
        ArtifactRetentionStore, PostgresArtifactRetentionStore, reconcile_artifact_retention,
    },
    settlement::ImageResultStatus,
};
use image_provider_sdk::Completed;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;

fn scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        provider_id: DREAMINA_PROVIDER_ID.into(),
        provider_account_id: DREAMINA_ACCOUNT_ID,
        execution_profile_id: DREAMINA_PROFILE_ID,
    }
}

async fn clock(database: &mut TestDatabase) -> TestResult {
    sqlx::query("CREATE TABLE additional_image_test_clock(offset_ms BIGINT NOT NULL)")
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
    sqlx::query("INSERT INTO additional_image_test_clock VALUES(0)")
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
    for name in [
        "clock_timestamp",
        "statement_timestamp",
        "transaction_timestamp",
    ] {
        sqlx::query(AssertSqlSafe(format!("CREATE FUNCTION {name}() RETURNS TIMESTAMPTZ LANGUAGE SQL VOLATILE AS 'SELECT pg_catalog.{name}() + offset_ms * interval ''1 millisecond'' FROM additional_image_test_clock'")))
            .execute(&database.pool).await.map_err(debug_error)?;
    }
    let options = PgConnectOptions::from_str(&env::var("TEST_DATABASE_URL").map_err(debug_error)?)
        .map_err(debug_error)?
        .options([("search_path", format!("{},pg_catalog", database.schema))]);
    let pool = PgPoolOptions::new()
        .max_connections(12)
        .connect_with(options)
        .await
        .map_err(debug_error)?;
    std::mem::replace(&mut database.pool, pool).close().await;
    Ok(())
}

async fn advance(pool: &PgPool, offset: i64) -> TestResult {
    sqlx::query("UPDATE additional_image_test_clock SET offset_ms=$1")
        .bind(offset)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    Ok(())
}

async fn accepted(pool: &PgPool) -> TestResult<(WorkLease, Uuid)> {
    let work = seed_dreamina_generation_lease_for_profile(
        pool,
        "additional-worker",
        DREAMINA_IMAGES_API_PROFILE,
    )
    .await?;
    seed_v4_customer_quote_with_basis(
        pool,
        &work,
        V4CustomerQuoteIdentity::dreamina(),
        V4CustomerQuoteBasis {
            metric: "image_output",
            unit: "image",
            unit_size: 1,
            unit_price_micros: 7,
            quantity_source: "request_derived",
            confidence: "exact",
            max_quantity: 1,
            max_amount_micros: 7,
        },
    )
    .await?;
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    executor
        .prepare_and_handoff(&work, DREAMINA_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    seed_terminal_quota(pool, &work).await?;
    let lease = executor
        .claim_prepared(
            &ExecutorClaimScope {
                execution_profile_id: DREAMINA_PROFILE_ID,
                provider_id: DREAMINA_PROVIDER_ID.into(),
                command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
                adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
            },
            "additional-executor",
            60_000,
        )
        .await
        .map_err(debug_error)?
        .ok_or("missing executor")?;
    executor.start(&lease).await.map_err(debug_error)?;
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).map_err(debug_error)?,
        DreaminaSubmitPayloadV1::new(
            &lease.command_hash,
            image_provider_dreamina_cli::parse_submit_command(
                &serde_json::to_vec(&work.command_json).map_err(debug_error)?,
            )
            .map_err(debug_error)?,
        )
        .map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    let tasks = PostgresProviderTaskStore::new(pool.clone());
    let reservation = RemoteTaskSubmitReservation::new(
        &lease,
        format!("additional-{}", lease.submission_id.simple()),
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
        .ok_or("missing intent")?
        .execution_binding_sha256;
    let operation = format!("offline-additional-{}", work.job_id.simple());
    tasks
        .record_submit_receipt(&RemoteTaskSubmitReceipt {
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            executor_owner: lease.executor_owner.clone(),
            executor_lease_epoch: lease.executor_lease_epoch,
            remote_operation_id: operation.clone(),
            provider_request_id: None,
            event_identity: "additional-accepted".into(),
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
            remote_operation_id: operation,
            provider_request_id: None,
            event_identity: "additional-attach".into(),
            execution_binding_sha256: binding,
            poll_after_ms: 0,
            recovery_fence: None,
        })
        .await
        .map_err(debug_error)?;
    Ok((work, lease.submission_id))
}

struct FourImages {
    calls: Arc<AtomicUsize>,
}
impl ProviderPollDriver for FourImages {
    fn provider_id(&self) -> &'static str {
        DREAMINA_PROVIDER_ID
    }
    async fn poll<S: ArtifactSink>(
        &self,
        _: &ProviderPollDriverCall,
        _: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        panic!("must use physical result sink")
    }
    async fn poll_with_additional_images<S: ProviderAdditionalImageArtifactSink>(
        &self,
        _: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut primary = None;
        for index in 0..4 {
            sink.select_image(index)
                .map_err(offline_batch_artifact_failure)?;
            sink.write_chunk(&png_bytes([10 + index as u8, 20, 30, 255]))
                .await
                .map_err(offline_batch_artifact_failure)?;
            let manifest = sink
                .finalize(ProviderArtifactMetadata {
                    media_type: "image/png",
                })
                .await
                .map_err(offline_batch_artifact_failure)?;
            if index == 0 {
                primary = Some(manifest);
            }
        }
        Ok(PollObservation::Completed(Completed::new(
            primary.unwrap(),
            None,
        )))
    }
}

async fn financial(pool: &PgPool, job: Uuid) -> TestResult<(i32, i64, i64, i64, i64, i64)> {
    sqlx::query_as("SELECT job.charged_units, hold.captured_micros, hold.released_micros, (SELECT count(*) FROM provider_usage_facts WHERE job_id=$1), (SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1 AND transaction_type='customer_job_charge'), (SELECT count(*) FROM artifacts WHERE job_id=$1) FROM jobs job JOIN customer_billing_holds hold USING(job_id) WHERE job.job_id=$1")
        .bind(job).fetch_one(pool).await.map_err(debug_error)
}

#[tokio::test]
async fn additional_images_four_results_keep_one_capture_and_expire_as_a_group() -> TestResult {
    verify_additional_images_group(0).await
}

#[tokio::test]
async fn additional_images_delayed_reducer_keeps_extras_until_original_customer_retention_ends()
-> TestResult {
    verify_additional_images_group(90_000_000).await
}

async fn verify_additional_images_group(reducer_delay_ms: i64) -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        clock(&mut database).await?;
        let pool=&database.pool;
        let (work,submission)=accepted(pool).await?;
        let root=tempfile::tempdir().map_err(debug_error)?;
        let blobs=Arc::new(FilesystemArtifactBlobStore::new(root.path()).map_err(debug_error)?);
        let calls=Arc::new(AtomicUsize::new(0));
        let poller=ProviderPollOrchestrator::new(PostgresProviderTaskStore::new(pool.clone()),FourImages { calls:Arc::clone(&calls) },gpt_image_2_gateway::FilesystemProviderArtifactStagerFactory::new(Arc::clone(&blobs),1024*1024).map_err(debug_error)?,ProviderPollOrchestratorConfig { scope:scope(),owner:"additional-poller".into(),lease_ms:5000,heartbeat_interval:Duration::from_secs(1),max_materializations:1 }).map_err(debug_error)?;
        require(matches!(poller.run_once().await.map_err(debug_error)?,ProviderPollRun::Observed(ref t) if t.state==ProviderTaskState::ArtifactReady),"physical group not durable")?;
        require(matches!(poller.run_once().await.map_err(debug_error)?,ProviderPollRun::Idle) && calls.load(Ordering::SeqCst)==1,"completed group polled again")?;
        let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_image_additional_artifacts WHERE submission_id=$1),(SELECT count(*) FROM executor_artifact_authorities WHERE submission_id=$1),(SELECT count(*) FROM dreamina_image_stage_intents WHERE submission_id=$1 AND cleanup_state='deleted')")
            .bind(submission).fetch_one(pool).await.map_err(debug_error)?;
        require(counts==(3,1,1),format!("physical/billable counts drifted:{counts:?}"))?;
        if reducer_delay_ms > 0 {
            advance(pool,reducer_delay_ms).await?;
            let gc=PostgresArtifactRetentionStore::new(pool.clone());
            require(gc.claim_additional_images("delayed-reducer-cleanup",1000).await.map_err(debug_error)?.is_none(),"committed primary lost extras while customer reduction was pending")?;
        }
        let reductions=PostgresExecutorTerminalStore::new(pool.clone());
        let terminal=reductions.claim_terminal("additional-reducer",60000).await.map_err(debug_error)?.ok_or("missing terminal")?;
        let customer=CustomerArtifactPublisher::new(Arc::clone(&blobs)).publish(&terminal).await.map_err(debug_error)?;
        let first=reductions.complete_terminal(&terminal,Some(&customer)).await.map_err(debug_error)?;
        require(first==reductions.complete_terminal(&terminal,Some(&customer)).await.map_err(debug_error)? && first.parent_state==ExecutorParentTerminalState::Succeeded,"financial replay changed outcome")?;
        let before=financial(pool,work.job_id).await?;
        require(before==(1,7,0,1,1,1),format!("extras changed original bill:{before:?}"))?;
        // Fresh store instance reads the complete durable set after the poller is gone.
        drop(poller);
        let reads=PostgresExecutionSettlementStore::new(pool.clone(),blobs.clone());
        reads.validate_complete_image_result_budget("executord-process-smoke","project-test",None,work.job_id).await.map_err(debug_error)?;
        let projected=reads.project_image_status("executord-process-smoke","project-test",None,work.job_id).await.map_err(debug_error)?.ok_or("missing original result")?;
        require(matches!(projected.status,ImageResultStatus::Succeeded { ref result,ref output_counts } if result.images.len()==1 && output_counts.delivered==1 && output_counts.requested==1),"legacy result quantities changed")?;
        let extra=reads.project_additional_image_results("executord-process-smoke","project-test",None,work.job_id).await.map_err(debug_error)?;
        require(extra.len()==3 && extra.iter().enumerate().all(|(i,image)| image.source_index==i+1 && image.bytes==png_bytes([11+i as u8,20,30,255])),"restart lost original candidates")?;
        for (tenant,project,actor) in [("other-tenant","project-test",None),("executord-process-smoke","other-project",None),("executord-process-smoke","project-test",Some(Uuid::new_v4()))] {
            require(reads.project_additional_image_results(tenant,project,actor,work.job_id).await.map_err(debug_error)?.is_empty(),"cross-scope image leak")?;
        }
        let gc=PostgresArtifactRetentionStore::new(pool.clone());
        require(gc.claim_additional_images("not-due",1000).await.map_err(debug_error)?.is_none(),"unexpired candidate claimed")?;
        advance(pool,reducer_delay_ms+1800001).await?;
        require(reads.project_additional_image_results("executord-process-smoke","project-test",None,work.job_id).await.err().is_some(),"expired group silently shrank to no extras")?;
        advance(pool,reducer_delay_ms+1900000).await?;
        let outcome=reconcile_artifact_retention(&gc,&blobs,"candidate-cleanup",1000,5).await.map_err(debug_error)?;
        require(outcome.deleted==2 && outcome.failed==0,format!("original and additional TTL cleanup failed:{outcome:?}"))?;
        require(financial(pool,work.job_id).await?==before,"cleanup changed bill")?;
        let state:String=sqlx::query_scalar("SELECT cleanup_state FROM dreamina_image_result_sets WHERE submission_id=$1").bind(submission).fetch_one(pool).await.map_err(debug_error)?;
        require(state=="deleted","physical set cleanup not committed")?;
        Ok(())
    }.await;
    combine(result, database.cleanup().await)
}

fn authority(
    lease: &ProviderTaskLease,
    index: usize,
    tag: u8,
) -> (usize, ProviderArtifactAuthority) {
    let id = additional_image_artifact_id(lease.task.executor_execution_id, index)
        .unwrap()
        .simple()
        .to_string();
    let bytes = png_bytes([tag, 20, 30, 255]);
    (
        index,
        ProviderArtifactAuthority::new(
            "filesystem-v1".into(),
            "filesystem-v1:offline-stage".into(),
            format!("executor-objects/{}/{}", &id[..2], id),
            hex::encode(Sha256::digest(&bytes)),
            bytes.len() as u64,
            "image/png".into(),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn additional_images_publish_is_fenced_atomic_and_retry_immutable() -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        clock(&mut database).await?;
        let pool=&database.pool;
        let (work,submission)=accepted(pool).await?;
        let tasks=PostgresProviderTaskStore::new(pool.clone());
        let lease=tasks.claim_due(&scope(),"physical-set-publisher",60000).await.map_err(debug_error)?.ok_or("missing poll lease")?;
        tasks.prepare_additional_image_artifacts(&lease).await.map_err(debug_error)?;
        let extras=(1..=3).map(|index|authority(&lease,index,index as u8)).collect::<Vec<_>>();
        let mut forged=lease.clone();forged.poll_lease_epoch+=1;
        require(matches!(tasks.publish_additional_image_artifacts(&forged,&extras).await,Err(ProviderTaskStoreError::InvalidInput)),"forged poll epoch accepted")?;
        let stale=lease;
        advance(pool,60001).await?;
        let lease=tasks.claim_due(&scope(),"restarted-physical-publisher",60000).await.map_err(debug_error)?.ok_or("missing reclaimed poll lease")?;
        require(matches!(tasks.publish_additional_image_artifacts(&stale,&extras).await,Err(ProviderTaskStoreError::StaleLease)),"stale poll epoch published extras")?;
        require(matches!(tasks.publish_additional_image_artifacts(&lease,&[authority(&lease,2,1)]).await,Err(ProviderTaskStoreError::InvalidInput)),"noncontiguous source accepted")?;
        let (a,b)=tokio::join!(tasks.publish_additional_image_artifacts(&lease,&extras),tasks.publish_additional_image_artifacts(&lease,&extras));
        a.map_err(debug_error)?;b.map_err(debug_error)?;
        PostgresProviderTaskStore::new(pool.clone()).publish_additional_image_artifacts(&lease,&extras).await.map_err(debug_error)?;
        require(matches!(tasks.publish_additional_image_artifacts(&lease,&[]).await,Err(ProviderTaskStoreError::Conflict)),"retry silently removed existing extras")?;
        let changed=(1..=3).map(|index|authority(&lease,index,99)).collect::<Vec<_>>();
        require(matches!(tasks.publish_additional_image_artifacts(&lease,&changed).await,Err(ProviderTaskStoreError::Conflict)),"retry replaced candidate hashes")?;
        let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_image_result_sets WHERE submission_id=$1),(SELECT count(*) FROM dreamina_image_additional_artifacts WHERE submission_id=$1),(SELECT count(*) FROM executor_artifact_authorities WHERE submission_id=$1),(SELECT count(*) FROM provider_usage_facts WHERE job_id=$2)")
            .bind(submission).bind(work.job_id).fetch_one(pool).await.map_err(debug_error)?;
        require(counts==(1,3,0,0),format!("extra publication created financial/main records:{counts:?}"))?;
        require(sqlx::query("UPDATE dreamina_image_additional_artifacts SET sha256_hex=repeat('0',64) WHERE submission_id=$1").bind(submission).execute(pool).await.is_err(),"candidate metadata was mutable")?;
        require(sqlx::query("UPDATE dreamina_image_result_sets SET returned_count=1 WHERE submission_id=$1").bind(submission).execute(pool).await.is_err(),"sealed result count was mutable")?;
        let foreign_job=Uuid::new_v4();
        for statement in [
            "INSERT INTO dreamina_image_result_sets(submission_id,executor_execution_id,job_id,returned_count,created_at_ms,cleanup_after_ms) VALUES ($1,$2,$3,1,1,2)",
            "INSERT INTO dreamina_image_stage_intents(submission_id,executor_execution_id,job_id,cleanup_after_ms) VALUES ($1,$2,$3,2)",
        ] {
            let error=sqlx::query(statement).bind(submission).bind(lease.task.executor_execution_id).bind(foreign_job).execute(pool).await.expect_err("foreign lineage must fail before ordinary PK/FK checks");
            require(error.as_database_error().is_some_and(|error| error.message()=="Dreamina physical results do not match their original submission"),format!("foreign set failed for an unrelated reason:{error:?}"))?;
        }
        Ok(())
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn additional_images_unsealed_crash_intent_reclaims_only_fixed_extra_ids() -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        clock(&mut database).await?;
        let pool=&database.pool;
        let (work,submission)=accepted(pool).await?;
        let tasks=PostgresProviderTaskStore::new(pool.clone());
        let lease=tasks.claim_due(&scope(),"orphan-physical-publisher",60000).await.map_err(debug_error)?.ok_or("missing poll lease")?;
        tasks.prepare_additional_image_artifacts(&lease).await.map_err(debug_error)?;
        tasks.prepare_additional_image_artifacts(&lease).await.map_err(debug_error)?;
        let root=tempfile::tempdir().map_err(debug_error)?;
        let blobs=FilesystemArtifactBlobStore::new(root.path()).map_err(debug_error)?;
        let extra=additional_image_artifact_id(lease.task.executor_execution_id,1).unwrap().simple().to_string();
        let primary=lease.task.executor_execution_id.simple().to_string();
        let paths=[root.path().join("executor-objects").join(&extra[..2]),root.path().join("executor-objects").join(&primary[..2]),root.path().join("executor-staging").join(&extra[..2]).join(&extra)];
        for path in &paths { fs::create_dir_all(path).map_err(debug_error)?;fs::set_permissions(path,fs::Permissions::from_mode(0o700)).map_err(debug_error)?; }
        fs::set_permissions(root.path().join("executor-staging").join(&extra[..2]),fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
        let extra_file=paths[0].join(&extra);let primary_file=paths[1].join(&primary);
        let partial_file=paths[2].join(format!(".epoch-1-{}",Uuid::new_v4().simple()));
        for path in [&extra_file,&primary_file,&partial_file] { fs::write(path,b"partial preserved bytes").map_err(debug_error)?;fs::set_permissions(path,fs::Permissions::from_mode(0o600)).map_err(debug_error)?; }
        let gc=PostgresArtifactRetentionStore::new(pool.clone());
        require(gc.claim_additional_images("early-cleanup",1000).await.map_err(debug_error)?.is_none(),"active stage intent expired early")?;
        advance(pool,90000000).await?;
        let held=gc.claim_additional_images("dead-cleaner",1000).await.map_err(debug_error)?.ok_or("expired unsealed intent not claimed")?;
        require(held.stage_intent && held.staged_ids.len()==9,"orphan claim not bounded to fixed IDs")?;
        require(gc.claim_additional_images("parallel-cleaner",1000).await.map_err(debug_error)?.is_none(),"parallel GC stole live cleanup lease")?;
        let mut stale=held.clone();stale.epoch+=1;
        require(gc.finish_additional_images(&stale,true).await.is_err(),"stale cleanup epoch committed")?;
        advance(pool,90001001).await?;
        let outcome=reconcile_artifact_retention(&gc,&blobs,"restarted-cleaner",1000,3).await.map_err(debug_error)?;
        require(outcome.deleted==1 && outcome.failed==0,format!("orphan cleanup failed:{outcome:?}"))?;
        require(!extra_file.exists() && !partial_file.exists() && primary_file.exists(),"orphan cleanup removed primary or retained extra bytes")?;
        require(matches!(tasks.publish_additional_image_artifacts(&lease,&[authority(&lease,1,1)]).await,Err(ProviderTaskStoreError::StaleLease)),"expired lease republished reclaimed extra")?;
        let state:(String,i64,i64)=sqlx::query_as("SELECT cleanup_state,(SELECT count(*) FROM provider_usage_facts WHERE job_id=$2),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$2 AND transaction_type='customer_job_charge') FROM dreamina_image_stage_intents WHERE submission_id=$1")
            .bind(submission).bind(work.job_id).fetch_one(pool).await.map_err(debug_error)?;
        require(state==("deleted".into(),0,0),format!("orphan cleanup changed financial state:{state:?}"))?;
        Ok(())
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn additional_images_zero_extras_are_sealed_and_cannot_grow_on_restart() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let pool = &database.pool;
        let (work, _) = accepted(pool).await?;
        let tasks = PostgresProviderTaskStore::new(pool.clone());
        let lease = tasks
            .claim_due(&scope(), "empty-physical-publisher", 60000)
            .await
            .map_err(debug_error)?
            .ok_or("missing empty-group poll lease")?;
        tasks
            .prepare_additional_image_artifacts(&lease)
            .await
            .map_err(debug_error)?;
        tasks
            .publish_additional_image_artifacts(&lease, &[])
            .await
            .map_err(debug_error)?;
        tasks
            .publish_additional_image_artifacts(&lease, &[])
            .await
            .map_err(debug_error)?;
        require(
            matches!(
                tasks
                    .publish_additional_image_artifacts(&lease, &[authority(&lease, 1, 1)])
                    .await,
                Err(ProviderTaskStoreError::Conflict)
            ),
            "empty sealed set grew on retry",
        )?;
        let units: i32 = sqlx::query_scalar("SELECT charged_units FROM jobs WHERE job_id=$1")
            .bind(work.job_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
        require(units == 0, "empty metadata seal charged units")?;
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}
