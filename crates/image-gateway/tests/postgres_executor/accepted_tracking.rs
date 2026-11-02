use super::accepted_completion_budget::{
    accepted_video, advance_clock, controlled_clock, immutable_submit_fingerprint,
};
use super::*;
use gpt_image_2_gateway::provider_tasks::{
    ProviderTaskDeadlineStore, ProviderTaskObservation, ProviderTaskObservationOutcome,
    ProviderTaskObservationSource,
};
use image_provider_sdk::RemoteVideoResult;

fn scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        provider_id: DREAMINA_PROVIDER_ID.into(),
        provider_account_id: DREAMINA_ACCOUNT_ID,
        execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
    }
}

#[tokio::test]
async fn accepted_tracking_keeps_same_video_after_two_days_and_settles_once() -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        controlled_clock(&mut database).await?;
        let pool=&database.pool;
        let video=accepted_video(pool, 10_000, true).await?;
        let frozen=immutable_submit_fingerprint(pool, video.submission_id).await?;
        advance_clock(pool,49*60*60*1000).await?;
        let tasks=PostgresProviderTaskStore::new(pool.clone());
        require(tasks.resolve_due_remote_task_deadline(&scope()).await.map_err(debug_error)?.is_none(),"accepted tracking was quarantined after 24 hours")?;
        let poll=tasks.claim_due(&scope(),"tracking-long-poll",60_000).await.map_err(debug_error)?.ok_or("long task no longer queryable")?;
        require(poll.task.remote_operation_id==video.remote_operation_id && poll.context().provider_deadline_at_ms()==video.original_deadline_at_ms && poll.completion_deadline_at_ms()==i64::MAX,"tracking changed original identity or submit authority")?;
        let now=database_now(pool).await?;
        require(poll.remaining_budget_ms()<=10_000 && poll.poll_attempt_deadline_at_ms()<=now+10_000,"tracking sentinel escaped into attempt budget")?;
        let poll=tasks.heartbeat(&poll,60_000).await.map_err(debug_error)?;
        let waiting=ProviderTaskObservation {event_identity:"tracking-waiting".into(),source:ProviderTaskObservationSource::Poll,outcome:ProviderTaskObservationOutcome::Waiting {poll_after_ms:0},progress:None};
        tasks.record_observation(&poll,&waiting).await.map_err(debug_error)?;
        let next=tasks.claim_due(&scope(),"tracking-next-poll",60_000).await.map_err(debug_error)?.ok_or("same-ID next poll missing")?;
        require(next.task.remote_operation_id==video.remote_operation_id && next.poll_lease_epoch>poll.poll_lease_epoch,"tracking did not fence next attempt")?;
        require(tasks.heartbeat(&poll,60_000).await.is_err(),"old epoch renewed the next attempt")?;
        let delayed:i64=sqlx::query_scalar("SELECT count(*) FROM accepted_tracking_authorities WHERE submission_id=$1 AND attention_after_at_ms=accepted_at_ms+86400000 AND attention_after_at_ms <= floor(extract(epoch FROM clock_timestamp())*1000)::BIGINT").bind(video.submission_id).fetch_one(pool).await.map_err(debug_error)?;
        require(delayed==1,"24h diagnostic omitted the long waiting task")?;
        let url="https://video.example.invalid/tracked-original.mp4";
        let ready=tasks.record_observation(&next,&ProviderTaskObservation {event_identity:"tracking-final".into(),source:ProviderTaskObservationSource::Poll,outcome:ProviderTaskObservationOutcome::RemoteReady {result:RemoteVideoResult::new(url.into(),None).map_err(debug_error)?},progress:None}).await.map_err(debug_error)?;
        require(ready.state==ProviderTaskState::RemoteReady,"late result failed publication")?;
        let terminals=PostgresExecutorTerminalStore::new(pool.clone());
        let terminal=terminals.claim_terminal("tracking-reducer",60_000).await.map_err(debug_error)?.ok_or("late terminal missing")?;
        let complete=terminals.complete_terminal(&terminal,None).await.map_err(debug_error)?;
        require(complete==terminals.complete_terminal(&terminal,None).await.map_err(debug_error)?,"late settlement replay changed result")?;
        let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM provider_submissions WHERE job_id=$1),(SELECT invocation_attempt::BIGINT FROM provider_submit_recoveries WHERE submission_id=$2),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1 AND transaction_type='customer_job_charge'),(SELECT count(*) FROM provider_remote_task_quarantines WHERE submission_id=$2)").bind(video.job_id).bind(video.submission_id).fetch_one(pool).await.map_err(debug_error)?;
        require(counts==(1,1,1,0),format!("long tracking repeated submission/settlement: {counts:?}"))?;
        require(immutable_submit_fingerprint(pool,video.submission_id).await?==frozen,"tracking rewrote frozen submit authority")?;
        for statement in ["UPDATE accepted_tracking_authorities SET remote_operation_id='other'", "DELETE FROM accepted_tracking_authorities", "TRUNCATE accepted_tracking_authorities"] {
            require(sqlx::query(AssertSqlSafe(statement.to_owned())).execute(pool).await.is_err(),"tracking authority was mutable")?;
        }
        Ok(())
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn accepted_tracking_native_batch_finishes_after_two_days_without_duplicate_billing()
-> TestResult {
    super::accepted_image_completion_budget::native_images_complete(i64::MAX, 49 * 60 * 60 * 1000)
        .await
}

#[tokio::test]
async fn accepted_tracking_upgrade_refuses_held_leases_and_preserves_quarantine() -> TestResult {
    let Some(mut database) = TestDatabase::new_through(1000, 166).await? else {
        return Ok(());
    };
    let result = async {
        controlled_clock(&mut database).await?;
        let pool = &database.pool;
        sqlx::query("INSERT INTO billing_accounts(tenant_id,currency,credit_limit_micros,held_micros,captured_micros,created_at_ms,updated_at_ms) VALUES ('executord-process-smoke','USD',48,0,0,1,1)")
            .execute(pool).await.map_err(debug_error)?;
        let waiting = accepted_video(pool, 10_000, true).await?;
        let quarantined = accepted_video(pool, 10_000, true).await?;
        let tasks = PostgresProviderTaskStore::new(pool.clone());
        // Keep one legacy task waiting while the other reaches its old deadline.
        // The schema clock does not mutate evidence or host time.
        let held = tasks
            .claim_due(&scope(), "migration-held", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or("held poll missing")?;
        let mut tx = pool.begin().await.map_err(debug_error)?;
        require(
            sqlx::raw_sql(include_str!(
                "../../migrations/0167_dreamina_accepted_tracking.sql"
            ))
            .execute(&mut *tx)
            .await
            .is_err(),
            "migration accepted a held old-runtime lease",
        )?;
        tx.rollback().await.map_err(debug_error)?;
        let rolled_back: bool =
            sqlx::query_scalar("SELECT to_regclass('accepted_tracking_authorities') IS NULL")
                .fetch_one(pool)
                .await
                .map_err(debug_error)?;
        require(rolled_back, "failed migration left partial DDL")?;
        tasks
            .record_observation(
                &held,
                &ProviderTaskObservation {
                    event_identity: "release-before-upgrade".into(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                    progress: None,
                },
            )
            .await
            .map_err(debug_error)?;
        advance_clock(pool, 49 * 60 * 60 * 1000).await?;
        let unknown = tasks
            .request_cancel(quarantined.submission_id)
            .await
            .map_err(debug_error)?;
        require(
            unknown.state == ProviderTaskState::Uncertain,
            "old expired task not quarantined",
        )?;
        run_migrations(pool).await.map_err(debug_error)?;
        let grants: Vec<Uuid> =
            sqlx::query_scalar("SELECT submission_id FROM accepted_tracking_authorities")
                .fetch_all(pool)
                .await
                .map_err(debug_error)?;
        require(
            grants == vec![waiting.submission_id],
            "migration reopened old quarantine or omitted legal waiting task",
        )?;
        let next = tasks
            .claim_due(&scope(), "after-upgrade", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or("backfilled waiting task not queryable")?;
        require(
            next.task.submission_id == waiting.submission_id
                && next.completion_deadline_at_ms() == i64::MAX,
            "backfill changed task identity",
        )?;
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

struct NeverTrackedPoll;
impl ProviderPollDriver for NeverTrackedPoll {
    fn provider_id(&self) -> &'static str {
        DREAMINA_PROVIDER_ID
    }
    async fn poll<S: ArtifactSink>(
        &self,
        _: &ProviderPollDriverCall,
        _: &mut S,
    ) -> Result<image_provider_sdk::PollObservation, image_provider_sdk::ProviderFailure> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn accepted_tracking_attempt_timeout_persists_due_and_releases_real_lease() -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        controlled_clock(&mut database).await?;
        let pool=&database.pool;
        let video=accepted_video(pool,2000,true).await?;
        advance_clock(pool,49*60*60*1000).await?;
        let root=tempfile::tempdir().map_err(debug_error)?;
        let blobs=Arc::new(FilesystemArtifactBlobStore::new(root.path()).map_err(debug_error)?);
        let poller=ProviderPollOrchestrator::new(PostgresProviderTaskStore::new(pool.clone()),NeverTrackedPoll,gpt_image_2_gateway::FilesystemProviderArtifactStagerFactory::new(blobs,1024*1024).map_err(debug_error)?,ProviderPollOrchestratorConfig {scope:scope(),owner:"bounded-query".into(),lease_ms:1000,heartbeat_interval:Duration::from_millis(100),max_materializations:1}).map_err(debug_error)?;
        let result=tokio::time::timeout(Duration::from_secs(5),poller.run_once()).await.map_err(debug_error)?.map_err(debug_error)?;
        require(matches!(result,ProviderPollRun::Observed(ref t) if t.state==ProviderTaskState::ProviderWaiting),"attempt timeout was terminal")?;
        let stored:(String,Option<String>,Option<i64>,Option<i64>,String)=sqlx::query_as("SELECT state,poll_owner,poll_lease_expires_at_ms,next_poll_at_ms,remote_operation_id FROM provider_remote_tasks WHERE submission_id=$1").bind(video.submission_id).fetch_one(pool).await.map_err(debug_error)?;
        require(stored.0=="provider_waiting" && stored.1.is_none() && stored.2.is_none() && stored.3.is_some() && stored.4==video.remote_operation_id,"timeout did not durably release same-ID poll with due time")?;
        advance_clock(pool,49*60*60*1000+60_000).await?;
        let next=PostgresProviderTaskStore::new(pool.clone()).claim_due(&scope(),"after-timeout",1000).await.map_err(debug_error)?.ok_or("due poll not reclaimable after store restart")?;
        require(next.task.remote_operation_id==video.remote_operation_id,"timeout changed remote ID")?;
        Ok(())
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn accepted_tracking_migration_keeps_legacy_stage_until_terminal() -> TestResult {
    let Some(mut database) = TestDatabase::new_through(1000, 166).await? else {
        return Ok(());
    };
    let result = async {
        controlled_clock(&mut database).await?;
        let pool = &database.pool;
        let (_, submission) = super::dreamina_additional_image_results::accepted(pool).await?;
        let tasks = PostgresProviderTaskStore::new(pool.clone());
        let scope = super::dreamina_additional_image_results::scope();
        let held = tasks
            .claim_due(&scope, "old-stage", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or("old stage poll missing")?;
        tasks
            .prepare_additional_image_artifacts(&held)
            .await
            .map_err(debug_error)?;
        tasks
            .record_observation(
                &held,
                &ProviderTaskObservation {
                    event_identity: "old-stage-wait".into(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                    progress: None,
                },
            )
            .await
            .map_err(debug_error)?;
        run_migrations(pool).await.map_err(debug_error)?;
        advance_clock(pool, 49 * 60 * 60 * 1000).await?;
        use gpt_image_2_gateway::retention::{
            ArtifactRetentionStore, PostgresArtifactRetentionStore,
        };
        let gc = PostgresArtifactRetentionStore::new(pool.clone());
        require(
            gc.claim_additional_images("after-migration", 1000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "migration let GC delete ongoing stage",
        )?;
        let held = tasks
            .claim_due(&scope, "finish-stage", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or("migrated staged task stopped")?;
        tasks
            .prepare_additional_image_artifacts(&held)
            .await
            .map_err(debug_error)?;
        tasks
            .record_observation(
                &held,
                &ProviderTaskObservation {
                    event_identity: "original-failed".into(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Failed {
                        error_code: "provider_rejected".into(),
                    },
                    progress: None,
                },
            )
            .await
            .map_err(debug_error)?;
        let cleanup = gc
            .claim_additional_images("terminal-stage", 1000)
            .await
            .map_err(debug_error)?
            .ok_or("terminal orphan not reclaimable")?;
        require(
            cleanup.submission_id == submission && cleanup.stage_intent,
            "wrong stage reclaimed",
        )?;
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn accepted_tracking_native_partial_survives_restart_after_two_days() -> TestResult {
    super::accepted_image_completion_budget::native_partial(i64::MAX, 49 * 60 * 60 * 1000).await
}
