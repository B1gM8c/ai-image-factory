use super::*;
use std::str::FromStr;

use gpt_image_2_gateway::{
    provider_tasks::{
        ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskObservationSource,
    },
    settlement::{VideoPendingStage, VideoProviderProgress, VideoResultStatus},
};
use image_provider_sdk::{ProviderTaskProgress, RemoteVideoResult};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

const TENANT: &str = "executord-process-smoke";
const PROJECT: &str = "project-test";

fn poll_scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        provider_id: DREAMINA_PROVIDER_ID.to_owned(),
        provider_account_id: DREAMINA_ACCOUNT_ID,
        execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
    }
}

fn pending_progress(
    status: Option<VideoResultStatus>,
) -> TestResult<Option<VideoProviderProgress>> {
    match status {
        Some(VideoResultStatus::Pending {
            model,
            duration_ms: 8_000,
            stage: VideoPendingStage::Processing,
            provider_progress,
        }) if model == "seedance2.0fast" => Ok(provider_progress),
        other => Err(format!(
            "accepted video lost its pending projection: {other:?}"
        )),
    }
}

#[tokio::test]
async fn accepted_video_projects_latest_durable_queue_under_the_original_scope() -> TestResult {
    verify_latest_durable_progress(true).await
}

#[tokio::test]
async fn failed_video_drops_its_last_durable_queue_progress() -> TestResult {
    verify_latest_durable_progress(false).await
}

// Freeze only this disposable schema's clock. This exercises two successful
// polls in the same millisecond without rewriting any immutable observations.
async fn progress_test_clock(database: &mut TestDatabase) -> TestResult<i64> {
    sqlx::query("CREATE TABLE progress_test_clock (now_ms BIGINT NOT NULL)")
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
    let now: i64 = sqlx::query_scalar(
        "INSERT INTO progress_test_clock SELECT floor(extract(epoch FROM pg_catalog.clock_timestamp())*1000)::BIGINT RETURNING now_ms",
    )
    .fetch_one(&database.pool)
    .await
    .map_err(debug_error)?;
    for function in [
        "clock_timestamp",
        "statement_timestamp",
        "transaction_timestamp",
    ] {
        sqlx::query(AssertSqlSafe(format!(
            "CREATE FUNCTION {function}() RETURNS TIMESTAMPTZ LANGUAGE SQL VOLATILE AS \
             'SELECT pg_catalog.to_timestamp(now_ms::DOUBLE PRECISION / 1000) FROM progress_test_clock'"
        )))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
    }
    let options = PgConnectOptions::from_str(&env::var("TEST_DATABASE_URL").map_err(debug_error)?)
        .map_err(debug_error)?
        .options([("search_path", format!("{},pg_catalog", database.schema))]);
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .map_err(debug_error)?;
    let previous = std::mem::replace(&mut database.pool, pool);
    previous.close().await;
    Ok(now)
}

async fn verify_latest_durable_progress(succeeds: bool) -> TestResult {
    let Some(mut database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let initial_time = progress_test_clock(&mut database).await?;
        let pool = &database.pool;
        let work = seed_dreamina_video_generation_lease_for_profile(
            pool,
            "queue-projection-worker",
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
                unit_price_micros: 3,
                quantity_source: "request_derived",
                confidence: "exact",
                max_quantity: 8,
                max_amount_micros: 24,
            },
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
                "queue-projection-executor",
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
            format!("queue-projection-{}", lease.submission_id.simple()),
            command.output(),
            command.identity(),
            60_000,
        );
        tasks.reserve_submit(&reservation).await.map_err(debug_error)?;
        tasks.start_submit(&reservation).await.map_err(debug_error)?;
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
                remote_operation_id: operation,
                provider_request_id: None,
                event_identity: "offline-video-attach".to_owned(),
                execution_binding_sha256: binding,
                poll_after_ms: 0,
                recovery_fence: None,
            })
            .await
            .map_err(debug_error)?;
        let reads = PostgresExecutionSettlementStore::new(
            pool.clone(),
            Arc::new(InMemoryArtifactBlobStore::default()),
        );
        require(
            pending_progress(reads.project_video_status(TENANT, PROJECT, None, work.job_id).await.map_err(debug_error)?)?.is_none(),
            "accepted task invented queue information before its first poll",
        )?;
        let mut latest_progress = None;
        for (index, (offset_ms, progress)) in [
            (0, Some(ProviderTaskProgress {
                provider_status: "Queueing".to_owned(),
                queue_position: Some(3341),
                queue_total: Some(476961),
                estimated_remaining_seconds: Some(27131),
            })),
            (1000, None),
            (2000, Some(ProviderTaskProgress {
                provider_status: "Queueing".to_owned(),
                queue_position: Some(3339),
                queue_total: Some(476960),
                estimated_remaining_seconds: None,
            })),
            (2000, Some(ProviderTaskProgress {
                provider_status: "querying".to_owned(),
                queue_position: None,
                queue_total: None,
                estimated_remaining_seconds: None,
            })),
            (3000, None),
            (4000, Some(ProviderTaskProgress {
                provider_status: "Processing".to_owned(),
                queue_position: None,
                queue_total: None,
                estimated_remaining_seconds: None,
            })),
            (5000, Some(ProviderTaskProgress {
                provider_status: "Generating".to_owned(),
                queue_position: None,
                queue_total: None,
                estimated_remaining_seconds: Some(60),
            })),
            (35000, Some(ProviderTaskProgress {
                provider_status: "Generating".to_owned(),
                queue_position: None,
                queue_total: None,
                estimated_remaining_seconds: Some(30),
            })),
            (65000, Some(ProviderTaskProgress {
                provider_status: "Generating".to_owned(),
                queue_position: None,
                queue_total: None,
                estimated_remaining_seconds: Some(120),
            })),
            (66000, Some(ProviderTaskProgress {
                provider_status: "Generating".to_owned(),
                queue_position: None,
                queue_total: None,
                estimated_remaining_seconds: Some(0),
            })),
            (67000, Some(ProviderTaskProgress {
                provider_status: "Generating".to_owned(),
                queue_position: None,
                queue_total: None,
                estimated_remaining_seconds: None,
            })),
            (68000, Some(ProviderTaskProgress {
                provider_status: "Queueing".to_owned(),
                queue_position: None,
                queue_total: None,
                estimated_remaining_seconds: Some(30),
            })),
        ]
        .into_iter()
        .enumerate()
        {
            sqlx::query("UPDATE progress_test_clock SET now_ms=$1")
                .bind(initial_time + offset_ms)
                .execute(pool)
                .await
                .map_err(debug_error)?;
            let poll = tasks
                .claim_due(&poll_scope(), "queue-projection-poller", 60_000)
                .await
                .map_err(debug_error)?
                .ok_or("video poll lease missing")?;
            tasks
                .record_observation(
                    &poll,
                    &ProviderTaskObservation {
                        event_identity: format!("offline-video-poll-{index}"),
                        source: ProviderTaskObservationSource::Poll,
                        outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                        progress: progress.clone(),
                    },
                )
                .await
                .map_err(debug_error)?;
            let durable: (Uuid, i64, i64, i64) = sqlx::query_as(
                "SELECT observation.observation_id, observation.observed_at_ms, task.updated_at_ms,
                        (SELECT count(*) FROM provider_task_observations WHERE submission_id=task.submission_id)
                 FROM provider_remote_tasks task JOIN provider_task_observations observation
                   ON observation.observation_id=task.state_observation_id
                  AND observation.submission_id=task.submission_id WHERE task.submission_id=$1",
            )
            .bind(lease.submission_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
            require(durable.1 == initial_time + offset_ms, "test poll escaped the controlled observation clock")?;
            if let Some(progress) = progress {
                latest_progress = Some(VideoProviderProgress {
                    estimated_percent: match offset_ms {
                        5000 => Some(0),
                        35000 => Some(50),
                        65000 => Some(33),
                        66000 => Some(99),
                        _ => None,
                    },
                    progress, observed_at_ms: durable.1,
                });
            } else {
                require(latest_progress.as_ref().is_some_and(|latest| latest.observed_at_ms < durable.1),
                    "query error did not occur after the last valid progress report")?;
            }
            for _ in 0..2 {
                let restarted_reads = PostgresExecutionSettlementStore::new(
                    pool.clone(),
                    Arc::new(InMemoryArtifactBlobStore::default()),
                );
                let projected = pending_progress(restarted_reads.project_video_status(TENANT, PROJECT, None, work.job_id).await.map_err(debug_error)?)?;
                require(projected == latest_progress,
                    format!("poll {index}: GET lost the last valid progress, retained old queue values, or refreshed its observation time: {projected:?}"))?;
                let unscoped = pending_progress(restarted_reads.video_status(TENANT, work.job_id).await.map_err(debug_error)?)?;
                require(unscoped == projected, "tenant video projection lost queue metadata")?;
                let with_metadata = restarted_reads.project_video_status_with_metadata(TENANT, PROJECT, None, work.job_id).await.map_err(debug_error)?.ok_or("video metadata projection missing")?;
                require(pending_progress(Some(with_metadata.0))? == projected && with_metadata.1.provider_model.as_deref()==Some("seedance2.0fast"),
                    "metadata projection lost queue data or the frozen requested model")?;
            }
            let unchanged: (Uuid, i64, i64, i64) = sqlx::query_as(
                "SELECT observation.observation_id, observation.observed_at_ms, task.updated_at_ms,
                        (SELECT count(*) FROM provider_task_observations WHERE submission_id=task.submission_id)
                 FROM provider_remote_tasks task JOIN provider_task_observations observation
                   ON observation.observation_id=task.state_observation_id
                  AND observation.submission_id=task.submission_id WHERE task.submission_id=$1",
            )
            .bind(lease.submission_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
            require(durable == unchanged, "read projection wrote an observation or changed its time")?;
            // This existing API-key/legacy fixture has no individual actor. A supplied
            // unrelated actor must not inherit its project-wide read visibility.
            for (tenant, project, actor) in [
                ("other-tenant", PROJECT, None),
                (TENANT, "other-project", None),
                (TENANT, PROJECT, Some(Uuid::new_v4())),
            ] {
                require(reads.project_video_status(tenant, project, actor, work.job_id).await.map_err(debug_error)?.is_none(),
                    "queue progress leaked across tenant, project, or actor scope")?;
                require(reads.project_video_status_with_metadata(tenant, project, actor, work.job_id).await.map_err(debug_error)?.is_none(),
                    "metadata queue progress leaked across tenant, project, or actor scope")?;
            }
        }
        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM provider_submissions WHERE job_id=$1),
                    (SELECT count(*) FROM provider_remote_submit_intents WHERE submission_id=$2),
                    (SELECT count(*) FROM usage_events WHERE job_id=$1)",
        )
        .bind(work.job_id)
        .bind(lease.submission_id)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
        require(counts == (1, 1, 0), "queue observations submitted again or charged usage")?;
        let mut plan_tx = pool.begin().await.map_err(debug_error)?;
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *plan_tx).await.map_err(debug_error)?;
        let plan: Vec<String> = sqlx::query_scalar(
            "EXPLAIN (COSTS OFF)
             SELECT progress.progress_json, progress.observed_at_ms
             FROM provider_remote_tasks task
             LEFT JOIN LATERAL (
                 SELECT observation.progress_json, observation.observed_at_ms
                 FROM provider_task_observations observation
                 WHERE observation.submission_id=task.submission_id
                   AND observation.executor_execution_id=task.executor_execution_id
                   AND observation.source='poll'
                   AND observation.observed_state='provider_waiting'
                   AND observation.progress_json IS NOT NULL
                 ORDER BY observation.observed_at_ms DESC,
                          observation.poll_lease_epoch DESC, observation.observation_id DESC
                 LIMIT 1
             ) progress ON TRUE
             WHERE task.submission_id=$1 AND task.executor_execution_id=$2",
        )
        .bind(lease.submission_id).bind(lease.executor_execution_id)
        .fetch_all(&mut *plan_tx).await.map_err(debug_error)?;
        let baseline_plan: Vec<String> = sqlx::query_scalar(
            "EXPLAIN (COSTS OFF) SELECT baseline.observed_at_ms
             FROM provider_task_observations baseline
             WHERE baseline.submission_id=$1 AND baseline.executor_execution_id=$2
               AND baseline.source='poll' AND baseline.observed_state='provider_waiting'
               AND baseline.progress_json IS NOT NULL
               AND baseline.progress_json ->> 'provider_status'='Generating'
               AND jsonb_typeof(baseline.progress_json -> 'estimated_remaining_seconds')='number'
             ORDER BY baseline.observed_at_ms ASC,
                      baseline.poll_lease_epoch ASC, baseline.observation_id ASC LIMIT 1",
        ).bind(lease.submission_id).bind(lease.executor_execution_id)
            .fetch_all(&mut *plan_tx).await.map_err(debug_error)?;
        let baseline_plan = baseline_plan.join("\n");
        require(baseline_plan.contains("provider_task_observations_progress_projection_idx")
                && baseline_plan.contains("Limit")
                && !baseline_plan.lines().any(|line| line.trim_start().starts_with("Sort")),
            format!("generation baseline lost its indexed lookup:\n{baseline_plan}"))?;
        plan_tx.rollback().await.map_err(debug_error)?;
        let plan = plan.join("\n");
        require(plan.contains("provider_task_observations_progress_projection_idx")
                && plan.contains("Limit")
                && !plan.lines().any(|line| line.trim_start().starts_with("Sort")),
            format!("latest progress lost its bounded indexed lookup:\n{plan}"))?;

        let poll = tasks.claim_due(&poll_scope(), "queue-projection-poller", 60_000)
            .await.map_err(debug_error)?.ok_or("terminal video poll lease missing")?;
        let outcome = if succeeds {
            ProviderTaskObservationOutcome::RemoteReady {
                result: RemoteVideoResult::new("https://video.example.invalid/progress.mp4".to_owned(), None)
                    .map_err(debug_error)?,
            }
        } else {
            ProviderTaskObservationOutcome::Failed { error_code: "provider_failed".to_owned() }
        };
        tasks.record_observation(&poll, &ProviderTaskObservation {
            event_identity: "offline-video-terminal".to_owned(),
            source: ProviderTaskObservationSource::Poll,
            outcome,
            progress: None,
        }).await.map_err(debug_error)?;
        let terminal_store = PostgresExecutorTerminalStore::new(pool.clone());
        let terminal = terminal_store.claim_terminal("queue-projection-reducer", 60_000)
            .await.map_err(debug_error)?.ok_or("video terminal reduction missing")?;
        terminal_store.complete_terminal(&terminal, None).await.map_err(debug_error)?;
        for status in [
            reads.video_status(TENANT, work.job_id).await.map_err(debug_error)?,
            reads.project_video_status(TENANT, PROJECT, None, work.job_id).await.map_err(debug_error)?,
            reads.project_video_status_with_metadata(TENANT, PROJECT, None, work.job_id)
                .await.map_err(debug_error)?.map(|(status, _)| status),
        ] {
            require(if succeeds {
                matches!(status, Some(VideoResultStatus::SucceededRemote { .. }))
            } else {
                matches!(status, Some(VideoResultStatus::Failed { error_code: Some(ref code), .. }) if code == "provider_failed")
            }, "terminal projection retained pending progress or lost its terminal state")?;
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
