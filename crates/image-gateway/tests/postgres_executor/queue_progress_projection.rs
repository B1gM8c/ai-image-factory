use super::*;
use gpt_image_2_gateway::{
    provider_tasks::{
        ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskObservationSource,
    },
    settlement::{VideoPendingStage, VideoProviderProgress, VideoResultStatus},
};
use image_provider_sdk::ProviderTaskProgress;

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
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
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
        for (index, progress) in [
            Some(ProviderTaskProgress {
                provider_status: "Queueing".to_owned(),
                queue_position: Some(3341),
                queue_total: Some(476961),
                estimated_remaining_seconds: Some(27131),
            }),
            Some(ProviderTaskProgress {
                provider_status: "Queueing".to_owned(),
                queue_position: Some(3339),
                queue_total: Some(476960),
                estimated_remaining_seconds: None,
            }),
            None,
        ]
        .into_iter()
        .enumerate()
        {
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
            for _ in 0..2 {
                let restarted_reads = PostgresExecutionSettlementStore::new(
                    pool.clone(),
                    Arc::new(InMemoryArtifactBlobStore::default()),
                );
                let projected = pending_progress(restarted_reads.project_video_status(TENANT, PROJECT, None, work.job_id).await.map_err(debug_error)?)?;
                require(projected == progress.clone().map(|progress| VideoProviderProgress { progress, observed_at_ms:durable.1 }),
                    "GET lost the latest queue values, retained stale values, or refreshed the observation time")?;
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
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
