use super::*;
use std::{borrow::Cow, str::FromStr};

use gpt_image_2_gateway::provider_tasks::{
    ProviderTaskDeadlineStore, ProviderTaskObservation, ProviderTaskObservationOutcome,
    ProviderTaskObservationSource,
};
use image_provider_sdk::{ProviderTaskProgress, RemoteVideoResult};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

const COMPLETION_WINDOW_MS: i64 = 86_400_000;
const ORIGINAL_TIMEOUT_MS: i64 = 10_000;
const REMOTE_URL: &str = "https://video.example.invalid/completion%2fwait.mp4?token=A%2Bb";

fn scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        provider_id: DREAMINA_PROVIDER_ID.to_owned(),
        provider_account_id: DREAMINA_ACCOUNT_ID,
        execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
    }
}

/// Only this disposable schema shadows database clock functions. Every stored
/// timestamp and immutable submit binding is created through the normal stores;
/// advancing time never updates those records or changes the host clock.
async fn controlled_clock(database: &mut TestDatabase) -> TestResult {
    let schema = &database.schema;
    sqlx::query(AssertSqlSafe(format!(
        "CREATE TABLE \"{schema}\".completion_test_clock (offset_ms BIGINT NOT NULL)"
    )))
    .execute(&database.pool)
    .await
    .map_err(debug_error)?;
    sqlx::query("INSERT INTO completion_test_clock VALUES (0)")
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
              FROM \"{schema}\".completion_test_clock'"
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
    let previous = std::mem::replace(&mut database.pool, pool);
    previous.close().await;
    Ok(())
}

async fn advance_clock(pool: &PgPool, offset_ms: i64) -> TestResult {
    sqlx::query("UPDATE completion_test_clock SET offset_ms=$1")
        .bind(offset_ms)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    let delta: i64 = sqlx::query_scalar(
        "SELECT round(extract(epoch FROM (clock_timestamp()-pg_catalog.clock_timestamp()))*1000)::BIGINT",
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        (delta - offset_ms).abs() < 100,
        "disposable schema clock did not advance independently",
    )
}

struct AcceptedVideo {
    job_id: Uuid,
    submission_id: Uuid,
    original_deadline_at_ms: i64,
    accepted_at_ms: i64,
    remote_operation_id: String,
}

async fn accepted_video(pool: &PgPool, timeout_ms: i64, v4: bool) -> TestResult<AcceptedVideo> {
    let work = seed_dreamina_video_generation_lease_for_profile(
        pool,
        "completion-budget-worker",
        DREAMINA_VIDEOS_API_PROFILE,
    )
    .await?;
    if v4 {
        seed_v4_customer_quote_with_basis(
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
        )
        .await?;
    }
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
            "completion-budget-executor",
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
        format!("completion-{}", lease.submission_id.simple()),
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
    let intent = tasks
        .load_submit_intent(lease.submission_id)
        .await
        .map_err(debug_error)?
        .ok_or("submit intent missing")?;
    let operation = format!("offline-existing-video-{}", work.job_id.simple());
    tasks
        .record_submit_receipt(&RemoteTaskSubmitReceipt {
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            executor_owner: lease.executor_owner.clone(),
            executor_lease_epoch: lease.executor_lease_epoch,
            remote_operation_id: operation.clone(),
            provider_request_id: None,
            event_identity: "completion-video-accepted".to_owned(),
            execution_binding_sha256: intent.execution_binding_sha256.clone(),
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
            event_identity: "completion-video-attach".to_owned(),
            execution_binding_sha256: intent.execution_binding_sha256,
            poll_after_ms: 0,
            recovery_fence: None,
        })
        .await
        .map_err(debug_error)?;
    let (original_deadline_at_ms, accepted_at_ms) = sqlx::query_as(
        "SELECT provider_deadline_at_ms,created_at_ms FROM provider_remote_tasks WHERE submission_id=$1",
    )
    .bind(lease.submission_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    Ok(AcceptedVideo {
        job_id: work.job_id,
        submission_id: lease.submission_id,
        original_deadline_at_ms,
        accepted_at_ms,
        remote_operation_id: operation,
    })
}

async fn immutable_submit_fingerprint(pool: &PgPool, submission_id: Uuid) -> TestResult<String> {
    sqlx::query_scalar(
        "SELECT encode(sha256(convert_to((jsonb_build_object(\
             'intent',to_jsonb(intent),'recovery',to_jsonb(recovery)))::TEXT,'UTF8')),'hex') \
         FROM provider_remote_submit_intents intent JOIN provider_submit_recoveries recovery USING(submission_id) \
         WHERE intent.submission_id=$1",
    )
    .bind(submission_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

#[tokio::test]
async fn accepted_video_completes_after_original_submit_deadline_without_resubmitting() -> TestResult
{
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        controlled_clock(&mut database).await?;
        let pool = &database.pool;
        let video = accepted_video(pool, ORIGINAL_TIMEOUT_MS, true).await?;
        let frozen = immutable_submit_fingerprint(pool, video.submission_id).await?;
        let grant: (i64, i64, i64, String) = sqlx::query_as(
            "SELECT original_submit_deadline_at_ms,accepted_at_ms,completion_deadline_at_ms,grant_source FROM accepted_completion_budgets WHERE submission_id=$1",
        )
        .bind(video.submission_id)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
        require(
            grant
                == (
                    video.original_deadline_at_ms,
                    video.accepted_at_ms,
                    video.accepted_at_ms + COMPLETION_WINDOW_MS,
                    "accepted_attach".to_owned(),
                ),
            "new accepted task did not receive its original-acceptance-based audit",
        )?;
        let tasks = PostgresProviderTaskStore::new(pool.clone());
        advance_clock(pool, 9 * 60 * 60 * 1000).await?;
        require(database_now(pool).await? > video.original_deadline_at_ms, "submit deadline did not elapse")?;
        require(tasks.resolve_due_remote_task_deadline(&scope()).await.map_err(debug_error)?.is_none(),
            "accepted video was quarantined by its elapsed submit deadline")?;
        let poll = tasks.claim_due(&scope(), "completion-poller", 60_000).await.map_err(debug_error)?.ok_or("accepted video stopped polling after submit deadline")?;
        require(poll.task.remote_operation_id == video.remote_operation_id
                && poll.context().provider_deadline_at_ms() == video.original_deadline_at_ms
                && poll.completion_deadline_at_ms() == video.accepted_at_ms + COMPLETION_WINDOW_MS,
            "completion authority changed the original remote ID or frozen submit deadline")?;
        let poll = tasks.heartbeat(&poll, 60_000).await.map_err(debug_error)?;
        tasks.record_observation(&poll, &ProviderTaskObservation {
            event_identity: "completion-queue-after-submit-deadline".to_owned(),
            source: ProviderTaskObservationSource::Poll,
            outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
            progress: Some(ProviderTaskProgress {
                provider_status: "Queueing".to_owned(), queue_position: Some(9),
                queue_total: Some(100), estimated_remaining_seconds: Some(3600),
            }),
        }).await.map_err(debug_error)?;
        let poll = tasks.claim_due(&scope(), "completion-poller", 60_000).await.map_err(debug_error)?.ok_or("second original-ID poll missing")?;
        let ready = tasks.record_observation(&poll, &ProviderTaskObservation {
            event_identity: "completion-video-after-long-queue".to_owned(),
            source: ProviderTaskObservationSource::Poll,
            outcome: ProviderTaskObservationOutcome::RemoteReady {
                result: RemoteVideoResult::new(REMOTE_URL.to_owned(), None).map_err(debug_error)?,
            },
            progress: None,
        }).await.map_err(debug_error)?;
        require(ready.state == ProviderTaskState::RemoteReady, "late successful URL did not establish canonical remote delivery")?;
        let terminal_store = PostgresExecutorTerminalStore::new(pool.clone());
        let terminal = terminal_store.claim_terminal("completion-reducer",60_000).await.map_err(debug_error)?.ok_or("long queue produced no terminal reduction")?;
        require(matches!(&terminal.outcome,CanonicalExecutorOutcome::SucceededRemoteVideo(remote) if remote.result.video_url()==REMOTE_URL),"late result lost remote URL authority")?;
        let completion = terminal_store.complete_terminal(&terminal,None).await.map_err(debug_error)?;
        require(completion == terminal_store.complete_terminal(&terminal,None).await.map_err(debug_error)?,"completion replay changed settlement")?;
        require(immutable_submit_fingerprint(pool,video.submission_id).await?==frozen,"readback rewrote original immutable submit evidence")?;
        let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM provider_submissions WHERE job_id=$1),(SELECT invocation_attempt::BIGINT FROM provider_submit_recoveries WHERE submission_id=$2),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1 AND transaction_type='customer_job_charge'),(SELECT count(*) FROM provider_remote_task_quarantines WHERE submission_id=$2)")
            .bind(video.job_id).bind(video.submission_id).fetch_one(pool).await.map_err(debug_error)?;
        require(counts==(1,1,1,0),format!("long queue generated, charged, or quarantined more than once: {counts:?}"))?;
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn completion_budget_expires_once_without_refunding_unknown_generation() -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        controlled_clock(&mut database).await?;
        let pool = &database.pool;
        let video = accepted_video(pool, ORIGINAL_TIMEOUT_MS, true).await?;
        let frozen = immutable_submit_fingerprint(pool, video.submission_id).await?;
        for statement in [
            "UPDATE accepted_completion_budgets SET completion_deadline_at_ms=completion_deadline_at_ms+1",
            "DELETE FROM accepted_completion_budgets",
            "TRUNCATE accepted_completion_budgets",
        ] {
            require(sqlx::query(AssertSqlSafe(statement.to_owned())).execute(pool).await.is_err(),
                "completion authority was mutable")?;
        }
        let tasks = PostgresProviderTaskStore::new(pool.clone());
        let stale_poll = tasks.claim_due(&scope(), "completion-expiring-poll", 60_000)
            .await.map_err(debug_error)?.ok_or("original poll missing")?;
        advance_clock(pool, COMPLETION_WINDOW_MS + 1_000).await?;
        let task = tasks.resolve_due_remote_task_deadline(&scope()).await.map_err(debug_error)?
            .ok_or("expired accepted completion budget was not resolved")?;
        require(task.submission_id==video.submission_id && task.state==ProviderTaskState::Uncertain,
            "completion expiry changed the original task or claimed confirmed failure")?;
        require(tasks.claim_due(&scope(),"completion-after-expiry",60_000).await.map_err(debug_error)?.is_none(),
            "expired task became pollable without a new authority")?;
        require(tasks.heartbeat(&stale_poll,60_000).await.is_err(),"expired poll authority survived quarantine")?;
        require(tasks.resolve_due_remote_task_deadline(&scope()).await.map_err(debug_error)?.is_none(),
            "deadline was resolved more than once")?;
        let reductions=PostgresExecutorTerminalStore::new(pool.clone());
        let terminal=reductions.claim_terminal("completion-uncertain-reducer",60_000).await.map_err(debug_error)?
            .ok_or("expired task produced no canonical reduction")?;
        require(matches!(&terminal.outcome,CanonicalExecutorOutcome::Uncertain {error_code} if error_code=="provider_remote_task_deadline"),
            "expiry was converted into a confirmed failure")?;
        reductions.complete_terminal(&terminal,None).await.map_err(debug_error)?;
        let financial:(String,i64,i64,i64,i32,i32,i32,String)=sqlx::query_as(
            "SELECT hold.state,hold.held_micros,hold.captured_micros,hold.released_micros,job.charged_units,quota.committed_units,quota.released_units,allocation.state \
             FROM jobs job JOIN customer_billing_holds hold USING(job_id) JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id \
             JOIN provider_submissions submission ON submission.job_id=job.job_id JOIN executor_capacity_allocations allocation ON allocation.submission_id=submission.submission_id WHERE job.job_id=$1")
            .bind(video.job_id).fetch_one(pool).await.map_err(debug_error)?;
        require(financial==("held".to_owned(),24,0,0,0,0,0,"held".to_owned()),
            format!("unknown upstream effect was refunded, charged or released: {financial:?}"))?;
        let counts:(i64,i64,i64,i64)=sqlx::query_as(
            "SELECT (SELECT count(*) FROM provider_submissions WHERE job_id=$1),\
             (SELECT invocation_attempt::BIGINT FROM provider_submit_recoveries WHERE submission_id=$2),\
             (SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),\
             (SELECT count(*) FROM provider_remote_task_quarantines WHERE submission_id=$2)")
            .bind(video.job_id).bind(video.submission_id).fetch_one(pool).await.map_err(debug_error)?;
        require(counts==(1,1,0,1),format!("expiry generated, charged or quarantined repeatedly: {counts:?}"))?;
        require(immutable_submit_fingerprint(pool,video.submission_id).await?==frozen,
            "completion expiry changed frozen submit evidence")?;
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn non_v4_video_and_longer_original_budget_do_not_receive_an_extension() -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        controlled_clock(&mut database).await?;
        let pool = &database.pool;
        let old = accepted_video(pool, ORIGINAL_TIMEOUT_MS, false).await?;
        let longer = accepted_video(pool, 2 * COMPLETION_WINDOW_MS, true).await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM accepted_completion_budgets")
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
        require(
            count == 0,
            "non-v4 video or existing longer authority received an extension",
        )?;
        advance_clock(pool, 30_000).await?;
        let tasks = PostgresProviderTaskStore::new(pool.clone());
        let poll = tasks
            .claim_due(&scope(), "longer-original-poll", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or("longer original budget was lost")?;
        require(
            poll.task.submission_id == longer.submission_id
                && poll.completion_deadline_at_ms() == longer.original_deadline_at_ms
                && poll.context().provider_deadline_at_ms() == longer.original_deadline_at_ms,
            "completion policy shortened or replaced the longer original contract",
        )?;
        let expired = tasks
            .resolve_due_remote_task_deadline(&scope())
            .await
            .map_err(debug_error)?
            .ok_or("noneligible original deadline was ignored")?;
        require(
            expired.submission_id == old.submission_id
                && expired.state == ProviderTaskState::Uncertain,
            "completion extension leaked into a noneligible task",
        )?;
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn sixty_four_expired_tasks_do_not_hide_the_next_live_accepted_video() -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        controlled_clock(&mut database).await?;
        let pool=&database.pool;
        for _ in 0..64 {accepted_video(pool,ORIGINAL_TIMEOUT_MS,false).await?;}
        advance_clock(pool,60_000).await?;
        let live=accepted_video(pool,ORIGINAL_TIMEOUT_MS,true).await?;
        let tasks=PostgresProviderTaskStore::new(pool.clone());
        let poll=tasks.claim_due(&scope(),"live-after-expired-window",60_000).await.map_err(debug_error)?
            .ok_or("64 expired candidates starved the next live task")?;
        require(poll.task.submission_id==live.submission_id,"claim selected an expired task or another operation")?;
        let expired:i64=sqlx::query_scalar("SELECT count(*) FROM provider_remote_tasks WHERE provider_deadline_at_ms<floor(extract(epoch FROM clock_timestamp())*1000)::BIGINT")
            .fetch_one(pool).await.map_err(debug_error)?;
        require(expired==64,"starvation fixture did not retain all 64 expired tasks")?;
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

async fn database_before_completion_budget() -> TestResult<Option<TestDatabase>> {
    let Some(url) = env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        if env::var_os("CI").is_some() {
            return Err("TEST_DATABASE_URL must be set in CI".to_owned());
        }
        return Ok(None);
    };
    let schema = format!("completion_upgrade_{}", Uuid::new_v4().simple());
    let pool = connect_test_pool_with_search_path(&url, 8, &schema)
        .await
        .map_err(debug_error)?;
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .map_err(debug_error)?;
    require(
        name.to_ascii_lowercase().contains("test"),
        "refusing migration fixture DDL outside a test database",
    )?;
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&pool)
        .await
        .map_err(debug_error)?;
    let database = TestDatabase { schema, pool };
    let result = async {
        let mut migrations = sqlx::migrate!("./migrations");
        migrations.migrations = Cow::Owned(migrations.iter().filter(|migration| migration.version<=142).cloned().collect());
        migrations.run(&database.pool).await.map_err(debug_error)?;
        seed_execution_profiles(&database.pool).await?;
        // Current store reads select the effective deadline. This identity-only
        // compatibility function reproduces 142 behavior for legal fixture setup;
        // it has no table dependencies, grants no budget and is dropped before 143.
        sqlx::query("CREATE FUNCTION provider_task_completion_deadline(UUID,BIGINT) RETURNS BIGINT LANGUAGE SQL IMMUTABLE AS 'SELECT $2'")
            .execute(&database.pool).await.map_err(debug_error)?;
        Ok::<_,String>(())
    }.await;
    if let Err(error) = result {
        database.cleanup().await?;
        return Err(error);
    }
    Ok(Some(database))
}

#[tokio::test]
async fn historical_upgrade_grants_only_eligible_original_acceptance_windows() -> TestResult {
    for scenario in [
        "eligible",
        "active_lease",
        "expired_window",
        "longer_original",
    ] {
        let Some(mut database) = database_before_completion_budget().await? else {
            return Ok(());
        };
        let result=async {
            controlled_clock(&mut database).await?;
            let pool=&database.pool;
            let timeout=match scenario {
                "active_lease"=>60*60*1000,
                "longer_original"=>2*COMPLETION_WINDOW_MS,
                _=>ORIGINAL_TIMEOUT_MS,
            };
            let video=accepted_video(pool,timeout,true).await?;
            let legacy=accepted_video(pool,timeout,false).await?;
            let frozen=immutable_submit_fingerprint(pool,video.submission_id).await?;
            let tasks=PostgresProviderTaskStore::new(pool.clone());
            let active_poll=if scenario=="active_lease" {
                Some(tasks.claim_due(&scope(),"upgrade-existing-poller",60_000).await.map_err(debug_error)?.ok_or("historical active poll missing")?)
            } else {None};
            if scenario=="expired_window" {
                advance_clock(pool,COMPLETION_WINDOW_MS+1_000).await?;
            } else if scenario!="active_lease" {
                advance_clock(pool,9*60*60*1000).await?;
            }
            let observations_before:i64=sqlx::query_scalar("SELECT count(*) FROM provider_task_observations")
                .fetch_one(pool).await.map_err(debug_error)?;
            let before_version:i64=sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations")
                .fetch_one(pool).await.map_err(debug_error)?;
            require(before_version==142,"historical fixture did not use the real 142 schema")?;
            sqlx::query("DROP FUNCTION provider_task_completion_deadline(UUID,BIGINT)")
                .execute(pool).await.map_err(debug_error)?;
            run_migrations(pool).await.map_err(debug_error)?;
            let applied: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=143 AND success)",
            )
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
            require(applied,"completion budget migration was not recorded as a successful real upgrade")?;
            let grants:Vec<(Uuid,i64,i64,i64,String)>=sqlx::query_as(
                "SELECT submission_id,original_submit_deadline_at_ms,accepted_at_ms,completion_deadline_at_ms,grant_source FROM accepted_completion_budgets")
                .fetch_all(pool).await.map_err(debug_error)?;
            if scenario=="eligible" {
                require(grants==vec![(video.submission_id,video.original_deadline_at_ms,video.accepted_at_ms,video.accepted_at_ms+COMPLETION_WINDOW_MS,"migration_0143".to_owned())],
                    format!("historical grant reset acceptance or included a non-v4 task: {grants:?}"))?;
                let poll=tasks.claim_due(&scope(),"upgraded-existing-id",60_000).await.map_err(debug_error)?.ok_or("eligible historical video did not resume original-ID readback")?;
                require(poll.task.submission_id==video.submission_id
                    && poll.task.remote_operation_id==video.remote_operation_id
                    && poll.context().provider_deadline_at_ms()==video.original_deadline_at_ms
                    && poll.completion_deadline_at_ms()==video.accepted_at_ms+COMPLETION_WINDOW_MS,
                    "upgrade changed the original submit contract instead of adding a completion budget")?;
            } else {
                require(grants.is_empty(),format!("ineligible {scenario} received a historical extension"))?;
                if let Some(poll)=active_poll {
                    tasks.heartbeat(&poll,60_000).await.map_err(debug_error)?;
                }
                let effective:i64=sqlx::query_scalar("SELECT provider_task_completion_deadline(submission_id,provider_deadline_at_ms) FROM provider_remote_tasks WHERE submission_id=$1")
                    .bind(video.submission_id).fetch_one(pool).await.map_err(debug_error)?;
                require(effective==video.original_deadline_at_ms,"upgrade changed an ineligible or longer original budget")?;
            }
            let legacy_count:i64=sqlx::query_scalar("SELECT count(*) FROM accepted_completion_budgets WHERE submission_id=$1")
                .bind(legacy.submission_id).fetch_one(pool).await.map_err(debug_error)?;
            require(legacy_count==0,"historical non-v4 task received completion authority")?;
            require(immutable_submit_fingerprint(pool,video.submission_id).await?==frozen,"migration rewrote original submit evidence")?;
            let observed_after:i64=sqlx::query_scalar("SELECT count(*) FROM provider_task_observations")
                .fetch_one(pool).await.map_err(debug_error)?;
            require(observations_before==observed_after,"migration submitted or polled an upstream task")?;
            let charges:i64=sqlx::query_scalar("SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1")
                .bind(video.job_id).fetch_one(pool).await.map_err(debug_error)?;
            require(charges==0,"migration charged or refunded a historical task")?;
            Ok(())
        }.await;
        database.cleanup().await?;
        result.map_err(|error: String| format!("historical upgrade {scenario}: {error}"))?;
    }
    Ok(())
}
