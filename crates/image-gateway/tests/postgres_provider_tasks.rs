use std::{env, time::Duration};

use gpt_image_2_gateway::{
    ExecutorClaimScope, ExecutorHandoffStore, ExecutorSubmissionLease, ExecutorSubmissionOutcome,
    ExecutorSubmissionStore, PostgresExecutorSubmissionStore, PostgresProviderTaskStore,
    ProviderArtifactAuthority, ProviderCapacityEvidence, ProviderCapacityEvidenceOutcome,
    ProviderCapacityReconciliationState, ProviderCapacityReconciliationStore,
    ProviderCapacityTerminalState, ProviderSubmitFailureKind, ProviderSubmitIntentState,
    ProviderSubmitRecoveryFence, ProviderSubmitStart, ProviderTaskClaimScope,
    ProviderTaskDeadlineStore, ProviderTaskObservation, ProviderTaskObservationOutcome,
    ProviderTaskObservationSource, ProviderTaskState, ProviderTaskStore, ProviderTaskStoreError,
    RemoteTaskAttach, RemoteTaskSubmitFailure, RemoteTaskSubmitReceipt,
    RemoteTaskSubmitReservation, VerifiedCallbackWakeup,
    admission::WorkLease,
    database::{connect_test_pool_with_search_path, run_migrations},
};
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const PROFILE_ID: Uuid = Uuid::from_u128(0x1710);
const POOL_ID: Uuid = Uuid::from_u128(0x1720);
const ACCOUNT_ID: Uuid = Uuid::from_u128(0x1730);
const POLICY_ID: Uuid = Uuid::from_u128(0x1740);

#[tokio::test]
async fn remote_task_store_closes_attach_poll_callback_and_cancel_invariants() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let first = seed_running_submission(&database.pool, "remote-worker-a").await?;
        let second = seed_running_submission(&database.pool, "remote-worker-b").await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());

        let reservation = reservation_request(&first);
        let (reserved_left, reserved_right) = tokio::join!(
            store.reserve_submit(&reservation),
            store.reserve_submit(&reservation)
        );
        let reserved_left = reserved_left.map_err(debug_error)?;
        let reserved_right = reserved_right.map_err(debug_error)?;
        require(
            reserved_left == reserved_right
                && reserved_left.state == ProviderSubmitIntentState::Reserved,
            "concurrent submit reservation did not converge",
        )?;
        let (started_left, started_right) = tokio::join!(
            store.start_submit(&reservation),
            store.start_submit(&reservation)
        );
        let starts = [started_left.map_err(debug_error)?, started_right.map_err(debug_error)?];
        require(
            starts
                .iter()
                .filter(|start| matches!(start, ProviderSubmitStart::Acquired(_)))
                .count()
                == 1
                && starts.iter().all(|start| match start {
                    ProviderSubmitStart::Acquired(intent)
                    | ProviderSubmitStart::Existing(intent) =>
                        intent.intent.state == ProviderSubmitIntentState::Sending,
                }),
            "concurrent submit start did not elect exactly one sender",
        )?;
        let first_context = match &starts[0] {
            ProviderSubmitStart::Acquired(invocation)
            | ProviderSubmitStart::Existing(invocation) => invocation.context().clone(),
        };
        let receipt = submit_receipt(&first, "operation-a", "submit-receipt-a");
        let (receipt_left, receipt_right) = tokio::join!(
            store.record_submit_receipt(&receipt),
            store.record_submit_receipt(&receipt)
        );
        let receipt_left = receipt_left.map_err(debug_error)?;
        let receipt_right = receipt_right.map_err(debug_error)?;
        require(
            receipt_left == receipt_right
                && receipt_left.state == ProviderSubmitIntentState::OperationKnown,
            "concurrent submit receipt did not converge",
        )?;
        let attach = attach_request(&first, "operation-a", "submit-event-a");
        let (left, right) = tokio::join!(store.attach(&attach), store.attach(&attach));
        let left = left.map_err(debug_error)?;
        let right = right.map_err(debug_error)?;
        require(left == right, "concurrent attach did not converge on one task")?;
        require(
            left.state == ProviderTaskState::ProviderWaiting,
            "attached task was not waiting",
        )?;
        let task_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_remote_tasks WHERE submission_id = $1",
        )
        .bind(first.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(task_count == 1, "attach created more than one remote task")?;
        let executor_projection: (String, Option<String>, Option<i64>, String) = sqlx::query_as(
            r#"
            SELECT execution.state, execution.executor_owner,
                   execution.lease_expires_at_ms, submission.state
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(first.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            executor_projection
                == ("provider_waiting".to_string(), None, None, "provider_waiting".to_string()),
            format!("executor lease was retained while waiting: {executor_projection:?}"),
        )?;

        let conflicting = RemoteTaskAttach {
            remote_operation_id: "operation-conflict".to_string(),
            ..attach.clone()
        };
        require(
            store.attach(&conflicting).await == Err(ProviderTaskStoreError::Conflict),
            "same submission accepted a conflicting remote operation",
        )?;
        store
            .reserve_submit(&reservation_request(&second))
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation_request(&second))
            .await
            .map_err(debug_error)?;
        require(
            store
                .record_submit_receipt(&submit_receipt(
                    &second,
                    "operation-a",
                    "conflicting-receipt-b",
                ))
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "same account remote operation was accepted across submissions",
        )?;
        store
            .record_submit_receipt(&submit_receipt(
                &second,
                "operation-b",
                "submit-receipt-b",
            ))
            .await
            .map_err(debug_error)?;
        let cross_submission = attach_request(&second, "operation-a", "submit-event-b");
        require(
            store.attach(&cross_submission).await == Err(ProviderTaskStoreError::Conflict),
            "same account remote operation was attached across submissions",
        )?;
        let second_attach = attach_request(&second, "operation-b", "submit-event-b");

        let capacity_before_callback =
            capacity_heartbeat(&database.pool, first.executor_execution_id).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let callback = VerifiedCallbackWakeup {
            submission_id: first.submission_id,
            event_identity: "callback-event-a".to_string(),
        };
        store
            .record_verified_callback(&callback)
            .await
            .map_err(debug_error)?;
        store
            .record_verified_callback(&callback)
            .await
            .map_err(debug_error)?;
        let callback_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM provider_task_observations
            WHERE submission_id = $1 AND source = 'verified_callback'
            "#,
        )
        .bind(first.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(callback_count == 1, "duplicate callback was not deduplicated")?;
        require(
            capacity_heartbeat(&database.pool, first.executor_execution_id).await?
                == capacity_before_callback,
            "callback wakeup impersonated provider worker liveness",
        )?;
        let callback_task = store
            .load(first.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "callback task disappeared".to_string())?;
        require(
            callback_task.state == ProviderTaskState::ProviderWaiting
                && callback_task.artifact_ref.is_none(),
            "callback granted terminal or artifact authority",
        )?;

        let scope = claim_scope();
        let capacity_before_poll_claim =
            capacity_heartbeat(&database.pool, first.executor_execution_id).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let first_lease = store
            .claim_due(&scope, "poller-a", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "first task was not pollable".to_string())?;
        require(
            first_lease.context() == &first_context,
            "poll claim re-resolved the frozen provider context",
        )?;
        let capacity_after_poll_claim =
            capacity_heartbeat(&database.pool, first.executor_execution_id).await?;
        require(
            capacity_after_poll_claim > capacity_before_poll_claim,
            "poll claim did not heartbeat held provider capacity",
        )?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let first_lease = store
            .heartbeat(&first_lease, 5_000)
            .await
            .map_err(debug_error)?;
        require(
            capacity_heartbeat(&database.pool, first.executor_execution_id).await?
                > capacity_after_poll_claim,
            "poll lease renewal did not heartbeat held provider capacity",
        )?;
        store
            .request_cancel(first.submission_id)
            .await
            .map_err(debug_error)?;
        let uncertain = ProviderTaskObservation {
            event_identity: "cancel-unknown-a".to_string(),
            source: ProviderTaskObservationSource::Cancel,
            outcome: ProviderTaskObservationOutcome::Uncertain {
                error_code: "cancel_effect_unknown".to_string(),
            },
        };
        let (concurrent_heartbeat, terminal) = tokio::time::timeout(
            Duration::from_secs(2),
            async {
                tokio::join!(
                    store.heartbeat(&first_lease, 5_000),
                    store.record_observation(&first_lease, &uncertain),
                )
            },
        )
        .await
        .map_err(|_| "poll heartbeat and terminal release deadlocked".to_string())?;
        require(
            matches!(
                concurrent_heartbeat,
                Ok(_) | Err(ProviderTaskStoreError::StaleLease)
            ),
            "poll heartbeat and terminal release produced an invalid race result",
        )?;
        let terminal = terminal.map_err(debug_error)?;
        require(
            terminal.state == ProviderTaskState::Uncertain,
            "unknown cancellation was projected as canceled",
        )?;
        let canonical: (String, String, String, String, i32) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, decision.source,
                   allocation.state, policy.allocated_count
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(first.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            canonical
                == (
                    "uncertain".to_string(),
                    "uncertain".to_string(),
                    "remote_provider_observation".to_string(),
                    "released".to_string(),
                    1,
                ),
            format!("remote terminal evidence did not close canonical state: {canonical:?}"),
        )?;
        let reductions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM executor_terminal_reductions WHERE submission_id = $1 AND state = 'ready'",
        )
        .bind(first.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(reductions == 1, "remote terminal reduction was not enqueued")?;
        let replay = store
            .record_observation(&first_lease, &uncertain)
            .await
            .map_err(debug_error)?;
        require(replay == terminal, "duplicate observation was not idempotent")?;
        let observation_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_task_observations WHERE submission_id = $1 AND event_identity = $2",
        )
        .bind(first.submission_id)
        .bind(&uncertain.event_identity)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(observation_count == 1, "duplicate observation was appended twice")?;
        store
            .record_verified_callback(&VerifiedCallbackWakeup {
                submission_id: first.submission_id,
                event_identity: "terminal-callback-a".to_string(),
            })
            .await
            .map_err(debug_error)?;
        let terminal_callback_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_task_observations WHERE submission_id = $1 AND event_identity = 'terminal-callback-a'",
        )
        .bind(first.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            terminal_callback_count == 0,
            "terminal callback replay appended or heartbeated durable state",
        )?;

        store.attach(&second_attach).await.map_err(debug_error)?;
        let (attach_replay, second_claim) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                store.attach(&second_attach),
                store.claim_due(&scope, "poller-old", 200)
            )
        })
        .await
        .map_err(|_| "attach replay and poll claim deadlocked".to_string())?;
        attach_replay.map_err(debug_error)?;
        let second_lease = second_claim
            .map_err(debug_error)?
            .ok_or_else(|| "second task was not pollable".to_string())?;
        let authority_id = second.executor_execution_id.simple().to_string();
        let authority = ProviderArtifactAuthority::new(
            "filesystem-v1".to_string(),
            "filesystem-v1:provider-task-test".to_string(),
            format!("executor-objects/{}/{}", &authority_id[..2], authority_id),
            "a".repeat(64),
            128,
            "image/png".to_string(),
        )
        .ok_or_else(|| "valid provider artifact authority was rejected".to_string())?;
        let mut task_locker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT 1 FROM provider_remote_tasks WHERE submission_id = $1 FOR UPDATE")
            .bind(second.submission_id)
            .execute(&mut *task_locker)
            .await
            .map_err(debug_error)?;
        let stale_store = store.clone();
        let stale_lease = second_lease.clone();
        let stale_authority = authority.clone();
        let mut blocked_publication = tokio::spawn(async move {
            stale_store
                .publish_artifact_authority(&stale_lease, &stale_authority)
                .await
        });
        let stale_store = store.clone();
        let stale_lease = second_lease.clone();
        let mut blocked_observation = tokio::spawn(async move {
            stale_store
                .record_observation(
                    &stale_lease,
                    &ProviderTaskObservation {
                        event_identity: "expired-while-locked-b".to_string(),
                        source: ProviderTaskObservationSource::Poll,
                        outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                    },
                )
                .await
        });
        let stale_store = store.clone();
        let stale_lease = second_lease.clone();
        let mut blocked_heartbeat =
            tokio::spawn(async move { stale_store.heartbeat(&stale_lease, 5_000).await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        require(
            !blocked_publication.is_finished()
                && !blocked_observation.is_finished()
                && !blocked_heartbeat.is_finished(),
            "provider write did not wait for the task fence lock",
        )?;
        task_locker.commit().await.map_err(debug_error)?;
        let stale_publication =
            tokio::time::timeout(Duration::from_secs(2), &mut blocked_publication)
                .await
                .map_err(|_| "stale authority publication remained blocked".to_string())?
                .map_err(debug_error)?;
        require(
            stale_publication == Err(ProviderTaskStoreError::StaleLease),
            "authority publication used a database timestamp captured before its task lock",
        )?;
        let stale_observation =
            tokio::time::timeout(Duration::from_secs(2), &mut blocked_observation)
                .await
                .map_err(|_| "stale provider observation remained blocked".to_string())?
                .map_err(debug_error)?;
        require(
            stale_observation.is_err(),
            "provider observation used a database timestamp captured before its task lock",
        )?;
        let stale_heartbeat = tokio::time::timeout(Duration::from_secs(2), &mut blocked_heartbeat)
            .await
            .map_err(|_| "stale provider heartbeat remained blocked".to_string())?
            .map_err(debug_error)?;
        require(
            stale_heartbeat == Err(ProviderTaskStoreError::StaleLease),
            "provider heartbeat used a database timestamp captured before its task lock",
        )?;
        let stale_observation_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_task_observations WHERE submission_id = $1 AND event_identity = 'expired-while-locked-b'",
        )
        .bind(second.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            stale_observation_count == 0,
            "expired poll owner left append-only observation evidence",
        )?;
        let authority_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM executor_artifact_authorities WHERE authority_id = $1",
        )
        .bind(second.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            authority_count == 0,
            "expired poll owner published immutable artifact authority",
        )?;
        let reclaimed = store
            .claim_due(&scope, "poller-new", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired task was not reclaimed".to_string())?;
        require(
            reclaimed.poll_lease_epoch == second_lease.poll_lease_epoch + 1,
            "poll lease epoch did not advance on reclaim",
        )?;
        let capacity_after_reclaim =
            capacity_heartbeat(&database.pool, second.executor_execution_id).await?;
        require(
            store.heartbeat(&second_lease, 5_000).await
                == Err(ProviderTaskStoreError::StaleLease),
            "expired poll fence renewed provider capacity",
        )?;
        require(
            capacity_heartbeat(&database.pool, second.executor_execution_id).await?
                == capacity_after_reclaim,
            "stale poll heartbeat changed held provider capacity",
        )?;
        let stale_result = store
            .record_observation(
                &second_lease,
                &ProviderTaskObservation {
                    event_identity: "stale-poll-b".to_string(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                },
            )
            .await;
        require(stale_result.is_err(), "stale poll fence wrote an observation")?;
        let waiting = ProviderTaskObservation {
            event_identity: "waiting-replay-b".to_string(),
            source: ProviderTaskObservationSource::Poll,
            outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
        };
        let first_waiting = store
            .record_observation(&reclaimed, &waiting)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let replayed_waiting = store
            .record_observation(&reclaimed, &waiting)
            .await
            .map_err(debug_error)?;
        require(
            first_waiting.next_poll_at_ms == replayed_waiting.next_poll_at_ms,
            "waiting observation replay changed its absolute poll time",
        )?;
        let conflicting_waiting = ProviderTaskObservation {
            outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 1 },
            ..waiting.clone()
        };
        require(
            store
                .record_observation(&reclaimed, &conflicting_waiting)
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "waiting observation replay accepted a different relative delay",
        )?;
        let reclaimed = store
            .claim_due(&scope, "poller-after-replay", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "replayed waiting task was not claimable".to_string())?;
        let cancel_without_request = store
            .record_observation(
                &reclaimed,
                &ProviderTaskObservation {
                    event_identity: "forged-cancel-b".to_string(),
                    source: ProviderTaskObservationSource::Cancel,
                    outcome: ProviderTaskObservationOutcome::Canceled {
                        error_code: "provider_canceled".to_string(),
                    },
                },
            )
            .await;
        require(
            cancel_without_request.is_err(),
            "task became canceled without a durable cancel request",
        )?;
        require(
            store
                .load(second.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|task| task.state == ProviderTaskState::ProviderWaiting),
            "failed fenced writes changed another submission",
        )?;
        require(
            sqlx::query(
                r#"
                INSERT INTO provider_task_observations
                  (observation_id, submission_id, executor_execution_id,
                   event_identity, source, observed_state, artifact_ref,
                   result_manifest_id, artifact_sha256_hex, artifact_byte_size,
                   artifact_media_type, error_code, effect_certainty,
                   next_poll_at_ms, poll_owner, poll_lease_epoch, payload_hash,
                   observed_at_ms)
                VALUES ($1, $2, $3, 'artifact-ready-b', 'poll', 'artifact_ready',
                        'durable-object-b', $2, $4, 128, 'image/png', NULL,
                        'not_applicable', NULL, $5, $6, $7,
                        floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(second.submission_id)
            .bind(second.executor_execution_id)
            .bind("a".repeat(64))
            .bind(&reclaimed.poll_owner)
            .bind(reclaimed.poll_lease_epoch)
            .bind("b".repeat(64))
            .execute(&database.pool)
            .await
            .is_err(),
            "artifact_ready committed before its immutable authority and manifest",
        )?;
        require(
            store
                .load(second.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|task| task.state == ProviderTaskState::ProviderWaiting),
            "rejected artifact_ready changed the durable task",
        )?;
        let premature_observations: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_task_observations WHERE submission_id = $1 AND event_identity = 'artifact-ready-b'",
        )
        .bind(second.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            premature_observations == 0,
            "rejected artifact_ready left append-only evidence behind",
        )?;
        let publication = store
            .publish_artifact_authority(&reclaimed, &authority)
            .await
            .map_err(|error| format!("publish remote artifact authority: {error:?}"))?;
        let mut stale_publication_replay = reclaimed.clone();
        stale_publication_replay.poll_lease_expires_at_ms = 0;
        require(
            store
                .publish_artifact_authority(&stale_publication_replay, &authority)
                .await
                .map_err(debug_error)?
                == publication,
            "exact authority commit-ack replay required a live poll lease",
        )?;
        require(
            store
                .record_observation(
                    &reclaimed,
                    &ProviderTaskObservation {
                        event_identity: "failure-after-authority-b".to_string(),
                        source: ProviderTaskObservationSource::Poll,
                        outcome: ProviderTaskObservationOutcome::Failed {
                            error_code: "contradictory_failure".to_string(),
                        },
                    },
                )
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "contradictory failure won after immutable artifact publication",
        )?;
        let artifact_ready = ProviderTaskObservation {
            event_identity: "artifact-ready-b".to_string(),
            source: ProviderTaskObservationSource::Poll,
            outcome: ProviderTaskObservationOutcome::ArtifactReady {
                artifact_ref: "durable-object-b".to_string(),
                publication: publication.clone(),
            },
        };
        let mut split_observation = database.pool.begin().await.map_err(debug_error)?;
        let split_observed_at = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            INSERT INTO provider_task_observations
              (observation_id, submission_id, executor_execution_id,
               event_identity, source, observed_state, artifact_ref,
               result_manifest_id, artifact_sha256_hex, artifact_byte_size,
               artifact_media_type, error_code, effect_certainty,
               next_poll_at_ms, poll_owner, poll_lease_epoch, payload_hash,
               observed_at_ms)
            VALUES ($1, $2, $3, 'artifact-ready-b', 'poll', 'artifact_ready',
                    'durable-object-b', $2, $4, 128, 'image/png', NULL,
                    'not_applicable', NULL, $5, $6, $7, $8)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(second.submission_id)
        .bind(second.executor_execution_id)
        .bind("a".repeat(64))
        .bind(&reclaimed.poll_owner)
        .bind(reclaimed.poll_lease_epoch)
        .bind("e".repeat(64))
        .bind(split_observed_at)
        .execute(&mut *split_observation)
        .await
        .map_err(debug_error)?;
        require(
            split_observation.commit().await.is_err(),
            "raw artifact_ready observation committed without canonical resolution",
        )?;
        let ready = store
            .record_observation(&reclaimed, &artifact_ready)
            .await
            .map_err(|error| format!("record remote artifact ready: {error:?}"))?;
        require(
            ready.state == ProviderTaskState::ArtifactReady,
            "verified remote artifact did not become ready",
        )?;
        let replayed_ready = store
            .record_observation(&reclaimed, &artifact_ready)
            .await
            .map_err(|error| format!("replay remote artifact ready: {error:?}"))?;
        require(
            replayed_ready == ready,
            "artifact_ready commit-ack replay changed the durable task",
        )?;
        require(
            publication.manifest().manifest_id() == second.submission_id,
            "remote artifact manifest identity drifted",
        )?;
        let success_projection: (String, String, String, String) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, decision.source, allocation.state
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(second.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            success_projection
                == (
                    "succeeded".to_string(),
                    "succeeded".to_string(),
                    "remote_provider_observation".to_string(),
                    "released".to_string(),
            ),
            format!("remote artifact did not close canonical success: {success_projection:?}"),
        )?;
        let exact_counts: (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_task_observations
               WHERE submission_id = $1 AND event_identity = 'artifact-ready-b'),
              (SELECT COUNT(*) FROM executor_resolution_decisions
               WHERE submission_id = $1 AND source = 'remote_provider_observation')
            "#,
        )
        .bind(second.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            exact_counts == (1, 1),
            format!("artifact_ready replay duplicated evidence or resolution: {exact_counts:?}"),
        )?;

        let third = seed_running_submission(&database.pool, "remote-worker-c").await?;
        let mut invalid_projection = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = 'provider_waiting', executor_owner = NULL,
                lease_expires_at_ms = NULL, updated_at_ms = updated_at_ms + 1
            WHERE executor_execution_id = $1
            "#,
        )
        .bind(third.executor_execution_id)
        .execute(&mut *invalid_projection)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "UPDATE provider_submissions SET state = 'provider_waiting', updated_at_ms = updated_at_ms + 1 WHERE submission_id = $1",
        )
        .bind(third.submission_id)
        .execute(&mut *invalid_projection)
        .await
        .map_err(debug_error)?;
        require(
            invalid_projection.commit().await.is_err(),
            "provider_waiting committed without a durable remote task",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_fences_late_poll_and_quarantines_capacity() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor = seed_attached_remote_task(
            &database.pool,
            &store,
            "remote-deadline-worker",
            "remote-deadline",
            900,
            60_000,
        )
        .await?;
        let (task_deadline, recovery_deadline, next_poll): (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT task.provider_deadline_at_ms, recovery.provider_deadline_at_ms,
                   task.next_poll_at_ms
            FROM provider_remote_tasks task
            JOIN provider_submit_recoveries recovery
              ON recovery.submission_id = task.submission_id
             AND recovery.executor_execution_id = task.executor_execution_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            task_deadline == recovery_deadline && next_poll == task_deadline,
            "attached task did not retain its exact bounded recovery deadline",
        )?;
        require(
            store
                .resolve_due_remote_task_deadline(&ProviderTaskClaimScope {
                    provider_id: "provider-test".to_string(),
                    provider_account_id: Uuid::new_v4(),
                })
                .await
                .map_err(debug_error)?
                .is_none(),
            "deadline resolver escaped its provider/account scope",
        )?;
        store
            .request_cancel(executor.submission_id)
            .await
            .map_err(debug_error)?;

        let lease = store
            .claim_due(&claim_scope(), "remote-deadline-poller", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "bounded remote task was not claimable".to_string())?;
        require(
            lease.context().provider_deadline_at_ms() == task_deadline
                && lease.poll_lease_expires_at_ms <= task_deadline,
            "poll claim extended beyond the frozen provider deadline",
        )?;
        let lease = store
            .heartbeat(&lease, 60_000)
            .await
            .map_err(debug_error)?;
        require(
            lease.poll_lease_expires_at_ms == task_deadline,
            "poll heartbeat was not capped at the frozen provider deadline",
        )?;
        let heartbeat_before_deadline =
            capacity_heartbeat(&database.pool, executor.executor_execution_id).await?;

        let authority = artifact_authority(&executor, "remote-deadline")?;
        let mut task_locker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT 1 FROM provider_remote_tasks WHERE submission_id = $1 FOR UPDATE")
            .bind(executor.submission_id)
            .execute(&mut *task_locker)
            .await
            .map_err(debug_error)?;
        let heartbeat_store = store.clone();
        let heartbeat_lease = lease.clone();
        let mut blocked_heartbeat = tokio::spawn(async move {
            heartbeat_store.heartbeat(&heartbeat_lease, 60_000).await
        });
        let observation_store = store.clone();
        let observation_lease = lease.clone();
        let mut blocked_observation = tokio::spawn(async move {
            observation_store
                .record_observation(
                    &observation_lease,
                    &ProviderTaskObservation {
                        event_identity: "late-deadline-poll".to_string(),
                        source: ProviderTaskObservationSource::Poll,
                        outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                    },
                )
                .await
        });
        let publication_store = store.clone();
        let publication_lease = lease.clone();
        let publication_authority = authority.clone();
        let mut blocked_publication = tokio::spawn(async move {
            publication_store
                .publish_artifact_authority(&publication_lease, &publication_authority)
                .await
        });
        sleep_until_database_time(&database.pool, task_deadline + 20).await?;
        require(
            !blocked_heartbeat.is_finished()
                && !blocked_observation.is_finished()
                && !blocked_publication.is_finished(),
            "late provider write did not wait for the task authority lock",
        )?;
        task_locker.commit().await.map_err(debug_error)?;
        let late_heartbeat = tokio::time::timeout(Duration::from_secs(2), &mut blocked_heartbeat)
            .await
            .map_err(|_| "late heartbeat remained blocked".to_string())?
            .map_err(debug_error)?;
        let late_observation =
            tokio::time::timeout(Duration::from_secs(2), &mut blocked_observation)
                .await
                .map_err(|_| "late observation remained blocked".to_string())?
                .map_err(debug_error)?;
        let late_publication =
            tokio::time::timeout(Duration::from_secs(2), &mut blocked_publication)
                .await
                .map_err(|_| "late artifact publication remained blocked".to_string())?
                .map_err(debug_error)?;
        require(
            late_heartbeat == Err(ProviderTaskStoreError::StaleLease)
                && late_observation == Err(ProviderTaskStoreError::StaleLease)
                && late_publication == Err(ProviderTaskStoreError::StaleLease),
            "a provider write crossed the database absolute deadline",
        )?;
        let late_evidence: (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_task_observations
               WHERE submission_id = $1 AND event_identity = 'late-deadline-poll'),
              (SELECT COUNT(*) FROM executor_artifact_authorities
               WHERE executor_execution_id = $2)
            "#,
        )
        .bind(executor.submission_id)
        .bind(executor.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            late_evidence == (0, 0),
            format!("late provider write left durable evidence: {late_evidence:?}"),
        )?;

        let left_scope = claim_scope();
        let right_scope = claim_scope();
        let (left, right) = tokio::join!(
            store.resolve_due_remote_task_deadline(&left_scope),
            store.resolve_due_remote_task_deadline(&right_scope),
        );
        let results = [left.map_err(debug_error)?, right.map_err(debug_error)?];
        require(
            results.iter().filter(|result| result.is_some()).count() == 1,
            "concurrent deadline resolvers did not elect exactly one transition",
        )?;
        let resolved = results
            .into_iter()
            .flatten()
            .next()
            .ok_or_else(|| "deadline resolver returned no task".to_string())?;
        require(
            resolved.submission_id == executor.submission_id
                && resolved.state == ProviderTaskState::Uncertain,
            "deadline quarantine changed the public compatibility projection",
        )?;
        let state_projection: (
            String,
            Option<String>,
            Option<Uuid>,
            String,
            String,
            String,
            String,
        ) = sqlx::query_as(
            r#"
            SELECT task.state, task.error_code, task.deadline_quarantine_id,
                   execution.state, submission.state, decision.source,
                   allocation.state
            FROM provider_remote_tasks task
            JOIN executor_executions execution
              ON execution.executor_execution_id = task.executor_execution_id
             AND execution.submission_id = task.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = task.executor_execution_id
             AND submission.submission_id = task.submission_id
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = task.executor_execution_id
             AND allocation.submission_id = task.submission_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state_projection
                == (
                    "uncertain".to_string(),
                    Some("provider_remote_task_deadline".to_string()),
                    Some(executor.executor_execution_id),
                    "uncertain".to_string(),
                    "uncertain".to_string(),
                    "remote_task_deadline".to_string(),
                    "held".to_string(),
                ),
            format!("deadline quarantine state diverged: {state_projection:?}"),
        )?;
        let authority_projection: (i32, i64, i64) = sqlx::query_as(
            r#"
            SELECT policy.allocated_count,
                   (SELECT COUNT(*) FROM provider_remote_task_quarantines quarantine
                    WHERE quarantine.submission_id = task.submission_id),
                   (SELECT COUNT(*) FROM executor_terminal_reductions reduction
                    WHERE reduction.submission_id = task.submission_id
                      AND reduction.state = 'ready')
            FROM provider_remote_tasks task
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = task.executor_execution_id
             AND allocation.submission_id = task.submission_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            authority_projection == (1, 1, 1),
            format!("deadline quarantine authority diverged: {authority_projection:?}"),
        )?;
        require(
            capacity_heartbeat(&database.pool, executor.executor_execution_id).await?
                == heartbeat_before_deadline,
            "deadline quarantine impersonated provider liveness",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_remote_task_quarantines SET error_code = error_code WHERE submission_id = $1",
            )
            .bind(executor.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "deadline quarantine authority was mutable",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_recovers_committed_artifact_authority() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor = seed_attached_remote_task(
            &database.pool,
            &store,
            "artifact-deadline-worker",
            "artifact-deadline",
            700,
            0,
        )
        .await?;
        let lease = store
            .claim_due(&claim_scope(), "artifact-deadline-poller", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "artifact deadline task was not claimable".to_string())?;
        let deadline = lease.context().provider_deadline_at_ms();
        let reserved_event = "internal:artifact-authority-recovery-v1";
        require(
            store
                .record_observation(
                    &lease,
                    &ProviderTaskObservation {
                        event_identity: reserved_event.to_string(),
                        source: ProviderTaskObservationSource::Poll,
                        outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                    },
                )
                .await
                == Err(ProviderTaskStoreError::InvalidInput)
                && store
                    .record_verified_callback(&VerifiedCallbackWakeup {
                        submission_id: executor.submission_id,
                        event_identity: reserved_event.to_string(),
                    })
                    .await
                    == Err(ProviderTaskStoreError::InvalidInput),
            "public provider evidence entered the internal event namespace",
        )?;
        let now = database_now(&database.pool).await?;
        require(
            sqlx::query(
                r#"
                INSERT INTO provider_task_observations
                  (observation_id, submission_id, executor_execution_id,
                   event_identity, source, observed_state, effect_certainty,
                   next_poll_at_ms, poll_owner, poll_lease_epoch,
                   payload_hash, observed_at_ms)
                VALUES ($1, $2, $3, $4, 'poll', 'provider_waiting',
                        'not_applicable', $5, $6, $7, $8, $5)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(executor.submission_id)
            .bind(executor.executor_execution_id)
            .bind(reserved_event)
            .bind(now)
            .bind(&lease.poll_owner)
            .bind(lease.poll_lease_epoch)
            .bind("f".repeat(64))
            .execute(&database.pool)
            .await
            .is_err(),
            "database accepted public evidence with the internal event identity",
        )?;
        let publication = store
            .publish_artifact_authority(
                &lease,
                &artifact_authority(&executor, "artifact-deadline")?,
            )
            .await
            .map_err(debug_error)?;
        sleep_until_database_time(&database.pool, deadline + 20).await?;

        let recovered = store
            .resolve_due_remote_task_deadline(&claim_scope())
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "committed artifact authority was not recoverable".to_string())?;
        require(
            recovered.state == ProviderTaskState::ArtifactReady
                && recovered.artifact_ref.as_deref()
                    == Some(
                        format!("manifest:{}", publication.manifest().manifest_id().simple())
                            .as_str(),
                    ),
            "deadline resolver did not materialize the committed artifact authority",
        )?;
        let projection: (String, String, String, String, i64, i64) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, decision.source,
                   allocation.state,
                   (SELECT COUNT(*) FROM provider_task_observations observation
                    WHERE observation.submission_id = submission.submission_id
                      AND observation.source = 'artifact_recovery'),
                   (SELECT COUNT(*) FROM provider_remote_task_quarantines quarantine
                    WHERE quarantine.submission_id = submission.submission_id)
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
             AND allocation.submission_id = execution.submission_id
            WHERE submission.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection
                == (
                    "succeeded".to_string(),
                    "succeeded".to_string(),
                    "remote_provider_observation".to_string(),
                    "released".to_string(),
                    1,
                    0,
                ),
            format!("artifact authority recovery projection diverged: {projection:?}"),
        )?;
        require(
            store
                .resolve_due_remote_task_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .is_none(),
            "artifact authority recovery replayed a terminal deadline",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_error_code_remains_public_provider_evidence() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor = seed_attached_remote_task(
            &database.pool,
            &store,
            "deadline-error-worker",
            "deadline-error",
            60_000,
            0,
        )
        .await?;
        let lease = store
            .claim_due(&claim_scope(), "deadline-error-poller", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "deadline error task was not claimable".to_string())?;
        let resolved = store
            .record_observation(
                &lease,
                &ProviderTaskObservation {
                    event_identity: "deadline-error-observation".to_string(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Uncertain {
                        error_code: "provider_remote_task_deadline".to_string(),
                    },
                },
            )
            .await
            .map_err(debug_error)?;
        require(
            resolved.state == ProviderTaskState::Uncertain
                && resolved.error_code.as_deref() == Some("provider_remote_task_deadline"),
            "provider uncertainty error code changed its public projection",
        )?;
        let projection: (Option<Uuid>, String, String, i64) = sqlx::query_as(
            r#"
            SELECT task.deadline_quarantine_id, decision.source, allocation.state,
                   (SELECT COUNT(*) FROM provider_remote_task_quarantines quarantine
                    WHERE quarantine.submission_id = task.submission_id)
            FROM provider_remote_tasks task
            JOIN executor_executions execution
              ON execution.executor_execution_id = task.executor_execution_id
             AND execution.submission_id = task.submission_id
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = task.executor_execution_id
             AND allocation.submission_id = task.submission_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection
                == (
                    None,
                    "remote_provider_observation".to_string(),
                    "released".to_string(),
                    0,
                ),
            format!("provider uncertainty was confused with quarantine: {projection:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn poll_claim_stops_after_one_locked_candidate_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        for index in 0..65 {
            let lease =
                seed_running_submission(&database.pool, &format!("bounded-poll-worker-{index}"))
                    .await?;
            let reservation = reservation_request(&lease);
            store
                .reserve_submit(&reservation)
                .await
                .map_err(debug_error)?;
            store
                .start_submit(&reservation)
                .await
                .map_err(debug_error)?;
            let operation_id = format!("bounded-poll-operation-{index}");
            store
                .record_submit_receipt(&submit_receipt(
                    &lease,
                    &operation_id,
                    &format!("bounded-poll-receipt-{index}"),
                ))
                .await
                .map_err(debug_error)?;
            store
                .attach(&attach_request(
                    &lease,
                    &operation_id,
                    &format!("bounded-poll-attach-{index}"),
                ))
                .await
                .map_err(debug_error)?;
        }

        let first_window: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT submission_id
            FROM provider_remote_tasks
            WHERE provider_id = 'provider-test'
              AND provider_account_id = $1
              AND state = 'provider_waiting'
            ORDER BY GREATEST(
                       next_poll_at_ms,
                       COALESCE(poll_lease_expires_at_ms, next_poll_at_ms)
                     ),
                     submission_id
            LIMIT 64
            "#,
        )
        .bind(ACCOUNT_ID)
        .fetch_all(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            first_window.len() == 64,
            "poll claim fixture did not fill its window",
        )?;
        let mut locker = database.pool.begin().await.map_err(debug_error)?;
        let locked: i64 = sqlx::query_scalar(
            r#"
            WITH locked AS (
              SELECT submission_id
              FROM provider_remote_tasks
              WHERE submission_id = ANY($1)
              FOR UPDATE
            )
            SELECT COUNT(*) FROM locked
            "#,
        )
        .bind(&first_window)
        .fetch_one(&mut *locker)
        .await
        .map_err(debug_error)?;
        require(
            locked == 64,
            "poll claim fixture did not lock its first window",
        )?;

        require(
            store
                .claim_due(&claim_scope(), "bounded-poll-claimant", 5_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "poll claim scanned beyond its locked 64-row candidate window",
        )?;
        locker.commit().await.map_err(debug_error)?;
        require(
            store
                .claim_due(&claim_scope(), "bounded-poll-after-unlock", 5_000)
                .await
                .map_err(debug_error)?
                .is_some(),
            "poll claim remained empty after its candidate window unlocked",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_resolver_stops_after_one_locked_candidate_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        for index in 0..65 {
            seed_attached_remote_task(
                &database.pool,
                &store,
                &format!("bounded-deadline-worker-{index}"),
                &format!("bounded-deadline-{index}"),
                5_000,
                0,
            )
            .await?;
        }
        let latest_deadline: i64 =
            sqlx::query_scalar("SELECT MAX(provider_deadline_at_ms) FROM provider_remote_tasks")
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        sleep_until_database_time(&database.pool, latest_deadline + 20).await?;

        let mut locked_window = database.pool.begin().await.map_err(debug_error)?;
        let locked: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT submission_id
            FROM provider_remote_tasks
            WHERE provider_id = 'provider-test'
              AND provider_account_id = $1
              AND state = 'provider_waiting'
            ORDER BY provider_deadline_at_ms, submission_id
            LIMIT 64
            FOR UPDATE
            "#,
        )
        .bind(ACCOUNT_ID)
        .fetch_all(&mut *locked_window)
        .await
        .map_err(debug_error)?;
        require(
            locked.len() == 64,
            "failed to lock the first deadline candidate window",
        )?;
        let scope = claim_scope();
        let bounded = tokio::time::timeout(
            Duration::from_secs(2),
            store.resolve_due_remote_task_deadline(&scope),
        )
        .await
        .map_err(|_| "deadline resolver blocked behind its candidate window".to_string())?
        .map_err(debug_error)?;
        require(
            bounded.is_none(),
            "deadline resolver scanned past its fixed locked candidate window",
        )?;
        locked_window.commit().await.map_err(debug_error)?;
        require(
            store
                .resolve_due_remote_task_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .is_some(),
            "deadline resolver did not resume after the candidate window unlocked",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_submit_recovery_command_retries_share_one_result() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "same-recovery-command", 200)
                .await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 60_000;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(250)).await;

        let scope = claim_scope();
        let (left, right) = tokio::join!(
            store.claim_submit_recovery(&scope, "same-command-owner", "claim/retry@1", 5_000,),
            store.claim_submit_recovery(&scope, "same-command-owner", "claim/retry@1", 5_000,),
        );
        let left = left
            .map_err(debug_error)?
            .ok_or_else(|| "first concurrent command retry returned no lease".to_string())?;
        let right = right
            .map_err(debug_error)?
            .ok_or_else(|| "second concurrent command retry returned no lease".to_string())?;
        require(
            left == right,
            "concurrent retries of one command returned different authority",
        )?;
        let command_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM provider_submit_recovery_commands
            WHERE provider_id = 'provider-test' AND provider_account_id = $1
              AND command_owner = 'same-command-owner'
              AND command_id = 'claim/retry@1'
            "#,
        )
        .bind(ACCOUNT_ID)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            command_count == 1,
            "concurrent command retries wrote more than one receipt",
        )?;
        require(
            sqlx::query(
                r#"
                INSERT INTO provider_submit_recovery_commands
                SELECT provider_id, provider_account_id, command_owner,
                       'claim/retry-alias', command_kind, request_duration_ms,
                       submission_id, executor_execution_id, recovery_lease_epoch,
                       claim_claimed_at_ms, claim_lease_expires_at_ms, intent_state,
                       intent_remote_operation_id, intent_provider_request_id,
                       intent_send_started_at_ms, intent_receipt_event_identity,
                       intent_failure_event_identity, intent_failure_error_code,
                       intent_updated_at_ms, created_at_ms
                FROM provider_submit_recovery_commands
                WHERE command_id = 'claim/retry@1'
                "#,
            )
            .execute(&database.pool)
            .await
            .is_err(),
            "database accepted a second command identity for one recovery transition",
        )?;
        require(
            store
                .claim_submit_recovery(&scope, "empty-owner", "empty-claim", 5_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "live recovery unexpectedly produced a second claim",
        )?;
        let empty_command_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_submit_recovery_commands WHERE command_id = 'empty-claim'",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            empty_command_count == 0,
            "empty recovery polling wrote an unbounded command receipt",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_submit_recovery_commands SET request_duration_ms = request_duration_ms + 1 WHERE command_id = 'claim/retry@1'",
            )
            .execute(&database.pool)
            .await
            .is_err(),
            "database allowed recovery command receipt mutation",
        )?;
        require(
            sqlx::query(
                "DELETE FROM provider_submit_recovery_commands WHERE command_id = 'claim/retry@1'",
            )
            .execute(&database.pool)
            .await
            .is_err(),
            "database allowed recovery command receipt deletion",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_recovery_claim_stops_after_one_locked_candidate_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        for index in 0..65 {
            let lease = seed_running_submission_with_lease(
                &database.pool,
                &format!("bounded-recovery-worker-{index}"),
                2_000,
            )
            .await?;
            let mut reservation = reservation_request(&lease);
            reservation.provider_timeout_ms = 60_000;
            store
                .reserve_submit(&reservation)
                .await
                .map_err(debug_error)?;
            store
                .start_submit(&reservation)
                .await
                .map_err(debug_error)?;
        }
        tokio::time::sleep(Duration::from_millis(2_100)).await;

        let first_window: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT submission_id
            FROM provider_submit_recoveries
            WHERE provider_id = 'provider-test'
              AND provider_account_id = $1
              AND state = 'active'
              AND GREATEST(
                    next_recovery_at_ms,
                    COALESCE(recovery_lease_expires_at_ms, next_recovery_at_ms)
                  ) <= floor(
                    extract(epoch FROM statement_timestamp()) * 1000
                  )::BIGINT
            ORDER BY GREATEST(
                       next_recovery_at_ms,
                       COALESCE(recovery_lease_expires_at_ms, next_recovery_at_ms)
                     ),
                     provider_deadline_at_ms,
                     submission_id
            LIMIT 64
            "#,
        )
        .bind(ACCOUNT_ID)
        .fetch_all(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            first_window.len() == 64,
            "submit recovery fixture did not fill its candidate window",
        )?;
        let mut locker = database.pool.begin().await.map_err(debug_error)?;
        let locked: i64 = sqlx::query_scalar(
            r#"
            WITH locked AS (
              SELECT submission_id
              FROM executor_capacity_allocations
              WHERE submission_id = ANY($1)
              FOR UPDATE
            )
            SELECT COUNT(*) FROM locked
            "#,
        )
        .bind(&first_window)
        .fetch_one(&mut *locker)
        .await
        .map_err(debug_error)?;
        require(
            locked == 64,
            "submit recovery fixture did not lock its first candidate window",
        )?;

        require(
            store
                .claim_submit_recovery(
                    &claim_scope(),
                    "bounded-recovery-claimant",
                    "bounded-recovery-claim",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "submit recovery claim scanned beyond its locked 64-row candidate window",
        )?;
        locker.commit().await.map_err(debug_error)?;
        require(
            store
                .claim_submit_recovery(
                    &claim_scope(),
                    "bounded-recovery-after-unlock",
                    "bounded-recovery-after-unlock-claim",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                .is_some(),
            "submit recovery claim remained empty after its candidate window unlocked",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_recovery_claim_stops_after_one_expired_candidate_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        for index in 0..65 {
            let lease = seed_running_submission_with_lease(
                &database.pool,
                &format!("expired-window-recovery-{index}"),
                2_000,
            )
            .await?;
            let mut reservation = reservation_request(&lease);
            reservation.provider_timeout_ms = if index < 64 { 500 } else { 60_000 };
            store
                .reserve_submit(&reservation)
                .await
                .map_err(debug_error)?;
            store
                .start_submit(&reservation)
                .await
                .map_err(debug_error)?;
        }
        tokio::time::sleep(Duration::from_millis(2_100)).await;

        require(
            store
                .claim_submit_recovery(
                    &claim_scope(),
                    "expired-window-owner",
                    "expired-window-claim",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "submit recovery claim scanned past its first 64 expired candidates",
        )?;
        let command_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_submit_recovery_commands WHERE command_id = 'expired-window-claim'",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            command_count == 0,
            "expired recovery window persisted a no-effect command",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_intent_lifecycle_fences_ambiguous_replay_and_late_evidence() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let late_lease =
            seed_running_submission_with_lease(&database.pool, "submit-late-receipt", 1_000)
                .await?;
        let late_reservation = reservation_request(&late_lease);
        store
            .reserve_submit(&late_reservation)
            .await
            .map_err(debug_error)?;
        require(
            matches!(
                store
                    .start_submit(&late_reservation)
                    .await
                    .map_err(debug_error)?,
                ProviderSubmitStart::Acquired(ref intent)
                    if intent.intent.state == ProviderSubmitIntentState::Sending
            ),
            "first submit start did not acquire send authority",
        )?;
        require(
            matches!(
                store
                    .start_submit(&late_reservation)
                    .await
                    .map_err(debug_error)?,
                ProviderSubmitStart::Existing(ref intent)
                    if intent.intent.state == ProviderSubmitIntentState::Sending
            ),
            "submit start replay acquired a second sender",
        )?;
        let executor_store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        require(
            executor_store
                .record_outcome(
                    &late_lease,
                    &ExecutorSubmissionOutcome::Failed {
                        error_code: "wrong_terminal_path".to_string(),
                    },
                )
                .await
                .is_err(),
            "generic runner terminalized an active remote submit protocol",
        )?;
        let recoverable = store
            .load_submit_intent(late_lease.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "sending intent was not durable".to_string())?;
        require(
            recoverable.state == ProviderSubmitIntentState::Sending
                && recoverable.submit_owner == late_lease.executor_owner
                && recoverable.submit_lease_epoch == late_lease.executor_lease_epoch,
            "sending intent lost its frozen recovery identity",
        )?;

        let unknown = submit_failure(
            &late_lease,
            ProviderSubmitFailureKind::OutcomeUnknown,
            "submit-receipt-lost",
            "submit_effect_unknown",
        );
        let (concurrent_start, outcome_unknown) = tokio::join!(
            store.start_submit(&late_reservation),
            store.record_submit_failure(&unknown)
        );
        let concurrent_start = concurrent_start.map_err(debug_error)?;
        let outcome_unknown = outcome_unknown.map_err(debug_error)?;
        require(
            matches!(
                concurrent_start,
                ProviderSubmitStart::Existing(ref intent)
                    if matches!(
                        intent.intent.state,
                        ProviderSubmitIntentState::Sending
                            | ProviderSubmitIntentState::OutcomeUnknown
                    )
            ),
            "submit replay did not serialize with ambiguous failure recording",
        )?;
        let replay = store
            .record_submit_failure(&unknown)
            .await
            .map_err(debug_error)?;
        require(
            outcome_unknown == replay
                && outcome_unknown.state == ProviderSubmitIntentState::OutcomeUnknown
                && outcome_unknown.remote_operation_id.is_none()
                && outcome_unknown.failure_error_code.as_deref()
                    == Some("submit_effect_unknown"),
            "unknown submit outcome was not durable and idempotent",
        )?;
        let mismatched_failure = RemoteTaskSubmitFailure {
            kind: ProviderSubmitFailureKind::Rejected,
            ..unknown.clone()
        };
        require(
            store.record_submit_failure(&mismatched_failure).await
                == Err(ProviderTaskStoreError::Conflict),
            "failure replay accepted the same evidence under a different kind",
        )?;
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        require(
            executor_store
                .reconcile_expired(100)
                .await
                .map_err(debug_error)?
                == 0,
            "generic lease reconciliation stole submit recovery ownership",
        )?;
        let known = store
            .record_submit_receipt(&submit_receipt(
                &late_lease,
                "operation-late",
                "late-receipt",
            ))
            .await
            .map_err(debug_error)?;
        require(
            known.state == ProviderSubmitIntentState::OperationKnown
                && known.remote_operation_id.as_deref() == Some("operation-late"),
            "late receipt did not replace the unknown outcome with stable identity",
        )?;
        let failure_replay_after_receipt = store
            .record_submit_failure(&unknown)
            .await
            .map_err(debug_error)?;
        require(
            failure_replay_after_receipt == known
                && known.failure_event_identity.as_deref() == Some("submit-receipt-lost")
                && known.failure_error_code.as_deref() == Some("submit_effect_unknown"),
            "late receipt overwrote or invalidated prior ambiguity evidence",
        )?;
        let recovery = store
            .claim_submit_recovery(
                &claim_scope(),
                "submit-recovery-a",
                "claim-late-receipt",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired submit was not recoverable".to_string())?;
        require(
            recovery.intent.state == ProviderSubmitIntentState::OperationKnown
                && recovery.context().idempotency_key() == late_reservation.idempotency_key
                && recovery.context().invocation_attempt() == 1,
            "recovery claim did not return the frozen invocation context",
        )?;
        let mut recovered_attach =
            attach_request(&late_lease, "operation-late", "late-attach");
        recovered_attach.recovery_fence = Some(ProviderSubmitRecoveryFence {
            recovery_owner: recovery.recovery_owner.clone(),
            recovery_lease_epoch: recovery.recovery_lease_epoch,
        });
        let attached = store
            .attach(&recovered_attach)
            .await
            .map_err(debug_error)?;
        require(
            attached.state == ProviderTaskState::ProviderWaiting,
            "expired executor lease prevented durable receipt handoff",
        )?;

        let rejected_lease = seed_running_submission(&database.pool, "submit-rejected").await?;
        let rejected_reservation = reservation_request(&rejected_lease);
        store
            .reserve_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        let rejected = submit_failure(
            &rejected_lease,
            ProviderSubmitFailureKind::Rejected,
            "provider-rejected-event",
            "provider_rejected",
        );
        let rejected_intent = store
            .record_submit_failure(&rejected)
            .await
            .map_err(debug_error)?;
        require(
            rejected_intent.state == ProviderSubmitIntentState::Rejected
                && rejected_intent.remote_operation_id.is_none(),
            "confirmed rejection retained an ambiguous remote operation",
        )?;
        let rejected_projection: (String, String, String, String) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, allocation.state,
                   allocation.release_reason
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
             AND allocation.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(rejected_lease.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            rejected_projection
                == (
                    "failed".to_string(),
                    "failed".to_string(),
                    "released".to_string(),
                    "remote_submit_outcome".to_string(),
                ),
            "confirmed rejection did not atomically close canonical capacity",
        )?;
        let rejected_reductions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM executor_terminal_reductions WHERE submission_id = $1 AND state = 'ready'",
        )
        .bind(rejected_lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            rejected_reductions == 1,
            "confirmed rejection did not enqueue one terminal reduction",
        )?;

        let remote_task_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_remote_tasks")
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        require(
            remote_task_count == 1,
            "submit recovery created an unexpected number of remote tasks",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_intent_terminal_projections_are_deferred_and_atomic() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());

        let rejected_lease = seed_running_submission(&database.pool, "bare-rejected").await?;
        let rejected_reservation = reservation_request(&rejected_lease);
        store
            .reserve_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        let mut rejected_tx = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE provider_remote_submit_intents
            SET state = 'rejected', failure_event_identity = 'bare-rejected-event',
                failure_error_code = 'provider_rejected', updated_at_ms = updated_at_ms + 1
            WHERE submission_id = $1
            "#,
        )
        .bind(rejected_lease.submission_id)
        .execute(&mut *rejected_tx)
        .await
        .map_err(debug_error)?;
        require(
            rejected_tx.commit().await.is_err(),
            "rejected intent committed without terminal parent and capacity projection",
        )?;
        require(
            store
                .load_submit_intent(rejected_lease.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| intent.state == ProviderSubmitIntentState::Sending),
            "failed rejected projection did not roll back the intent",
        )?;

        let attached_lease = seed_running_submission(&database.pool, "bare-attached").await?;
        let attached_reservation = reservation_request(&attached_lease);
        store
            .reserve_submit(&attached_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&attached_reservation)
            .await
            .map_err(debug_error)?;
        store
            .record_submit_receipt(&submit_receipt(
                &attached_lease,
                "bare-operation",
                "bare-receipt",
            ))
            .await
            .map_err(debug_error)?;
        let mut attached_tx = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE provider_remote_submit_intents
            SET state = 'attached', updated_at_ms = updated_at_ms + 1
            WHERE submission_id = $1
            "#,
        )
        .bind(attached_lease.submission_id)
        .execute(&mut *attached_tx)
        .await
        .map_err(debug_error)?;
        require(
            attached_tx.commit().await.is_err(),
            "attached intent committed without a remote task handoff",
        )?;
        require(
            store
                .load_submit_intent(attached_lease.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| intent.state == ProviderSubmitIntentState::OperationKnown),
            "failed attached projection did not roll back the intent",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_recovery_claim_is_scoped_fenced_and_reclaimable() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "recovery-claim", 250).await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 30_000;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let invocation = match store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?
        {
            ProviderSubmitStart::Acquired(invocation) => invocation,
            ProviderSubmitStart::Existing(_) => {
                return Err("first submit start did not acquire authority".to_string());
            }
        };
        require(
            invocation.context().model() == "model-test"
                && invocation.context().command_schema() == "provider-command-v1"
                && invocation.context().execution_profile_id() == PROFILE_ID
                && invocation.context().adapter_revision() == "provider-test-adapter-v1"
                && invocation.context().credential_pool_id() == POOL_ID
                && invocation.context().credential_ref() == "test-vault.provider-task.1"
                && invocation.context().credential_revision() == 1
                && invocation.context().credential_auth_sha256() == "1".repeat(64)
                && invocation.context().resource_policy_id() == POLICY_ID
                && invocation.context().resource_policy_revision() == 1
                && invocation.context().idempotency_key() == reservation.idempotency_key
                && invocation.context().invocation_attempt() == 1
                && invocation.context().provider_timeout_ms() == 30_000
                && invocation.context().provider_deadline_at_ms()
                    - invocation.intent.send_started_at_ms.unwrap_or_default()
                    == 30_000,
            "submit start did not return its exact frozen invocation context",
        )?;
        let context_debug = format!("{:?}", invocation.context());
        require(
            !context_debug.contains("test-vault.provider-task.1")
                && !context_debug.contains(&"1".repeat(64)),
            "provider context Debug output exposed credential identity",
        )?;
        let mut conflicting_timeout = reservation.clone();
        conflicting_timeout.provider_timeout_ms += 1;
        require(
            store.start_submit(&conflicting_timeout).await
                == Err(ProviderTaskStoreError::Conflict),
            "submit replay rewrote its frozen provider timeout",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_submit_recoveries SET provider_deadline_at_ms = provider_deadline_at_ms + 1 WHERE submission_id = $1",
            )
            .bind(executor.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "raw SQL rewrote the absolute provider deadline",
        )?;

        tokio::time::sleep(Duration::from_millis(300)).await;
        let wrong_scope = ProviderTaskClaimScope {
            provider_id: "provider-test".to_string(),
            provider_account_id: Uuid::new_v4(),
        };
        require(
            store
                .claim_submit_recovery(
                    &wrong_scope,
                    "wrong-account",
                    "claim-wrong-account",
                    2_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "recovery claim crossed the provider account boundary",
        )?;

        let scope = claim_scope();
        require(
            sqlx::query(
                r#"
                UPDATE executor_capacity_allocations
                SET last_heartbeat_at_ms =
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 60_000
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(executor.executor_execution_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "database accepted a future provider capacity heartbeat",
        )?;
        let capacity_before_recovery_claim =
            capacity_heartbeat(&database.pool, executor.executor_execution_id).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let (left, right) = tokio::join!(
            store.claim_submit_recovery(&scope, "recovery-a", "claim-recovery-a", 200),
            store.claim_submit_recovery(&scope, "recovery-b", "claim-recovery-b", 200),
        );
        let mut winners = [left.map_err(debug_error)?, right.map_err(debug_error)?]
            .into_iter()
            .flatten();
        let first = winners
            .next()
            .ok_or_else(|| "due recovery had no claimant".to_string())?;
        require(
            winners.next().is_none()
                && first.intent.submission_id == executor.submission_id
                && first.context() == invocation.context(),
            "concurrent recovery claim did not elect exactly one frozen context",
        )?;
        let first_claim_command = match first.recovery_owner.as_str() {
            "recovery-a" => "claim-recovery-a",
            "recovery-b" => "claim-recovery-b",
            _ => return Err("unexpected recovery claim owner".to_string()),
        };
        let claim_replay = store
            .claim_submit_recovery(
                &scope,
                &first.recovery_owner,
                first_claim_command,
                200,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "exact recovery claim replay disappeared".to_string())?;
        require(
            claim_replay == first,
            "exact recovery claim replay minted different authority",
        )?;
        require(
            store
                .claim_submit_recovery(
                    &scope,
                    &first.recovery_owner,
                    first_claim_command,
                    201,
                )
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "claim command identity accepted different lease parameters",
        )?;
        require(
            store
                .defer_submit_recovery(&first, first_claim_command, 100)
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "claim command identity was reused for a defer command",
        )?;
        store
            .record_submit_receipt(&submit_receipt(
                &executor,
                "operation-recovered",
                "receipt-recovered",
            ))
            .await
            .map_err(debug_error)?;
        let replay_after_receipt = store
            .claim_submit_recovery(
                &scope,
                &first.recovery_owner,
                first_claim_command,
                200,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "claim replay disappeared after late receipt".to_string())?;
        require(
            replay_after_receipt == first,
            "late receipt rewrote the original recovery claim response",
        )?;
        let capacity_after_recovery_claim =
            capacity_heartbeat(&database.pool, executor.executor_execution_id).await?;
        require(
            capacity_after_recovery_claim > capacity_before_recovery_claim,
            "submit recovery claim did not heartbeat held provider capacity",
        )?;
        require(
            first.recovery_lease_expires_at_ms <= first.context().provider_deadline_at_ms(),
            "submit recovery claim crossed the absolute provider deadline",
        )?;
        let expired_epoch = first.recovery_lease_epoch;
        let mut recovery_locker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            "SELECT 1 FROM provider_submit_recoveries WHERE submission_id = $1 FOR UPDATE",
        )
        .bind(executor.submission_id)
        .execute(&mut *recovery_locker)
        .await
        .map_err(debug_error)?;
        let stale_store = store.clone();
        let stale_recovery = first.clone();
        let mut blocked_heartbeat = tokio::spawn(async move {
            stale_store
                .heartbeat_submit_recovery(&stale_recovery, 2_000)
                .await
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        require(
            !blocked_heartbeat.is_finished(),
            "submit recovery heartbeat did not wait for its fence lock",
        )?;
        recovery_locker.commit().await.map_err(debug_error)?;
        let stale_heartbeat = tokio::time::timeout(Duration::from_secs(2), &mut blocked_heartbeat)
            .await
            .map_err(|_| "stale submit recovery heartbeat remained blocked".to_string())?
            .map_err(debug_error)?;
        require(
            stale_heartbeat == Err(ProviderTaskStoreError::StaleLease),
            "submit recovery heartbeat revived an expired epoch after a lock wait",
        )?;
        let expired_claim_replay = store
            .claim_submit_recovery(
                &scope,
                &first.recovery_owner,
                first_claim_command,
                200,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired claim acknowledgement replay disappeared".to_string())?;
        require(
            expired_claim_replay == first,
            "expired claim acknowledgement replay minted new authority",
        )?;
        let first = store
            .claim_submit_recovery(
                &scope,
                "recovery-after-expired-heartbeat",
                "claim-after-expired-heartbeat",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired heartbeat recovery was not reclaimable".to_string())?;
        require(
            first.recovery_lease_epoch == expired_epoch + 1,
            "expired heartbeat reclaim did not advance the recovery epoch",
        )?;
        let historical_claim_replay = store
            .claim_submit_recovery(
                &scope,
                &expired_claim_replay.recovery_owner,
                first_claim_command,
                200,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "superseded claim acknowledgement disappeared".to_string())?;
        require(
            historical_claim_replay == expired_claim_replay,
            "superseded claim command minted or returned different authority",
        )?;
        require(
            sqlx::query(
                r#"
                UPDATE provider_submit_recoveries
                SET recovery_lease_expires_at_ms = provider_deadline_at_ms + 1,
                    updated_at_ms = updated_at_ms + 1
                WHERE submission_id = $1
                "#,
            )
            .bind(executor.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "database accepted a recovery lease beyond the provider deadline",
        )?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let renewed = store
            .heartbeat_submit_recovery(&first, 3_000)
            .await
            .map_err(debug_error)?;
        require(
            renewed.recovery_lease_expires_at_ms > first.recovery_lease_expires_at_ms,
            "recovery heartbeat did not advance monotonically",
        )?;
        require(
            renewed.recovery_lease_expires_at_ms <= renewed.context().provider_deadline_at_ms(),
            "submit recovery heartbeat crossed the absolute provider deadline",
        )?;
        let replay_after_heartbeat = store
            .claim_submit_recovery(
                &scope,
                &first.recovery_owner,
                "claim-after-expired-heartbeat",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "claim replay disappeared after heartbeat".to_string())?;
        require(
            replay_after_heartbeat == first,
            "heartbeat rewrote the original claim command result",
        )?;
        require(
            capacity_heartbeat(&database.pool, executor.executor_execution_id).await?
                > capacity_after_recovery_claim,
            "submit recovery renewal did not heartbeat held provider capacity",
        )?;
        require(
            store
                .claim_submit_recovery(
                    &claim_scope(),
                    "recovery-c",
                    "claim-live-recovery-check",
                    2_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "live recovery lease was stolen",
        )?;

        let forged_defer_at = database_now(&database.pool).await?;
        let mut forged_defer = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_submit_recovery_commands (
                provider_id, provider_account_id, command_owner, command_id,
                command_kind, request_duration_ms, submission_id,
                executor_execution_id, recovery_lease_epoch, created_at_ms
            ) VALUES ($1, $2, $3, 'forged-defer', 'defer', 100, $4, $5, $6, $7)
            "#,
        )
        .bind(&renewed.intent.provider_id)
        .bind(renewed.intent.provider_account_id)
        .bind(&renewed.recovery_owner)
        .bind(renewed.intent.submission_id)
        .bind(renewed.intent.executor_execution_id)
        .bind(renewed.recovery_lease_epoch)
        .bind(forged_defer_at)
        .execute(&mut *forged_defer)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE provider_submit_recoveries
            SET recovery_owner = NULL, recovery_lease_expires_at_ms = NULL,
                recovery_claimed_at_ms = NULL,
                next_recovery_at_ms = LEAST(provider_deadline_at_ms, $3 + 101),
                updated_at_ms = $3
            WHERE submission_id = $1 AND executor_execution_id = $2
            "#,
        )
        .bind(renewed.intent.submission_id)
        .bind(renewed.intent.executor_execution_id)
        .bind(forged_defer_at)
        .execute(&mut *forged_defer)
        .await
        .map_err(debug_error)?;
        require(
            forged_defer.commit().await.is_err(),
            "database committed a defer receipt with a different retry result",
        )?;
        store
            .defer_submit_recovery(&renewed, "defer-recovery-c", 100)
            .await
            .map_err(debug_error)?;
        require(
            store
                .defer_submit_recovery(&renewed, "defer-recovery-c", 101)
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "defer command identity accepted different retry parameters",
        )?;
        store
            .defer_submit_recovery(&renewed, "defer-recovery-c", 100)
            .await
            .map_err(debug_error)?;
        require(
            store
                .claim_submit_recovery(
                    &claim_scope(),
                    "recovery-c",
                    "claim-before-defer-due",
                    2_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "deferred recovery became immediately claimable",
        )?;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let reclaimed = store
            .claim_submit_recovery(
                &claim_scope(),
                "recovery-c",
                "claim-after-defer/due",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "deferred recovery was not reclaimed".to_string())?;
        require(
            reclaimed.recovery_lease_epoch == first.recovery_lease_epoch + 1,
            "recovery reclaim did not advance the fence epoch",
        )?;
        store
            .defer_submit_recovery(&renewed, "defer-recovery-c", 100)
            .await
            .map_err(debug_error)?;

        store
            .record_submit_receipt(&submit_receipt(
                &executor,
                "operation-recovered",
                "receipt-recovered",
            ))
            .await
            .map_err(debug_error)?;
        let direct = attach_request(&executor, "operation-recovered", "direct-attach");
        require(
            store.attach(&direct).await == Err(ProviderTaskStoreError::StaleLease),
            "expired executor attached without the recovery fence",
        )?;
        let mut stale = direct.clone();
        stale.recovery_fence = Some(ProviderSubmitRecoveryFence {
            recovery_owner: first.recovery_owner,
            recovery_lease_epoch: first.recovery_lease_epoch,
        });
        require(
            store.attach(&stale).await == Err(ProviderTaskStoreError::StaleLease),
            "stale recovery epoch attached the remote operation",
        )?;
        let mut recovered = direct.clone();
        recovered.recovery_fence = Some(ProviderSubmitRecoveryFence {
            recovery_owner: reclaimed.recovery_owner.clone(),
            recovery_lease_epoch: reclaimed.recovery_lease_epoch,
        });
        let task = store.attach(&recovered).await.map_err(debug_error)?;
        require(
            task.state == ProviderTaskState::ProviderWaiting,
            "live recovery fence did not attach the known operation",
        )?;
        require(
            store.attach(&stale).await == Err(ProviderTaskStoreError::StaleLease)
                && store.attach(&direct).await == Err(ProviderTaskStoreError::StaleLease),
            "completed recovered attach acknowledged stale authority",
        )?;
        require(
            store.attach(&recovered).await.map_err(debug_error)? == task,
            "current recovery fence could not replay its completed attach",
        )?;
        require(
            store
                .heartbeat_submit_recovery(&reclaimed, 2_000)
                .await
                == Err(ProviderTaskStoreError::StaleLease),
            "closed recovery lease remained writable after attach",
        )?;
        let replay_after_close = store
            .claim_submit_recovery(
                &scope,
                &reclaimed.recovery_owner,
                "claim-after-defer/due",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "claim acknowledgement disappeared after close".to_string())?;
        require(
            replay_after_close == reclaimed,
            "closed recovery changed its historical claim acknowledgement",
        )?;
        store
            .defer_submit_recovery(&renewed, "defer-recovery-c", 100)
            .await
            .map_err(debug_error)?;

        let deadline_executor =
            seed_running_submission_with_lease(&database.pool, "recovery-deadline", 20).await?;
        let mut deadline_reservation = reservation_request(&deadline_executor);
        deadline_reservation.provider_timeout_ms = 120;
        store
            .reserve_submit(&deadline_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&deadline_reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(30)).await;
        let deadline_lease = store
            .claim_submit_recovery(
                &claim_scope(),
                "deadline-recovery",
                "claim-deadline-recovery",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "pre-deadline recovery was not claimable".to_string())?;
        store
            .record_submit_receipt(&submit_receipt(
                &deadline_executor,
                "operation-after-deadline",
                "receipt-before-deadline",
            ))
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let mut deadline_attach = attach_request(
            &deadline_executor,
            "operation-after-deadline",
            "attach-after-deadline",
        );
        deadline_attach.recovery_fence = Some(ProviderSubmitRecoveryFence {
            recovery_owner: deadline_lease.recovery_owner,
            recovery_lease_epoch: deadline_lease.recovery_lease_epoch,
        });
        require(
            store.attach(&deadline_attach).await == Err(ProviderTaskStoreError::StaleLease),
            "recovery fence attached a remote operation after the provider deadline",
        )?;

        let rejected_executor =
            seed_running_submission_with_lease(&database.pool, "recovery-reject", 200).await?;
        let rejected_reservation = reservation_request(&rejected_executor);
        store
            .reserve_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let rejection_lease = store
            .claim_submit_recovery(
                &claim_scope(),
                "recovery-rejector",
                "claim-recovery-rejector",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "confirmed rejection recovery was not claimable".to_string())?;
        let mut rejection = submit_failure(
            &rejected_executor,
            ProviderSubmitFailureKind::Rejected,
            "recovered-rejection",
            "provider_rejected",
        );
        rejection.recovery_fence = Some(ProviderSubmitRecoveryFence {
            recovery_owner: rejection_lease.recovery_owner.clone(),
            recovery_lease_epoch: rejection_lease.recovery_lease_epoch,
        });
        let (concurrent_heartbeat, rejected) = tokio::time::timeout(
            Duration::from_secs(2),
            async {
                tokio::join!(
                    store.heartbeat_submit_recovery(&rejection_lease, 2_000),
                    store.record_submit_failure(&rejection),
                )
            },
        )
        .await
        .map_err(|_| "recovery heartbeat and terminal release deadlocked".to_string())?;
        require(
            rejected.map_err(debug_error)?.state == ProviderSubmitIntentState::Rejected,
            "live recovery owner could not atomically commit confirmed rejection",
        )?;
        require(
            matches!(
                concurrent_heartbeat,
                Ok(_) | Err(ProviderTaskStoreError::StaleLease)
            ),
            "heartbeat and terminal release produced an invalid race result",
        )?;
        require(
            store
                .heartbeat_submit_recovery(&rejection_lease, 2_000)
                .await
                == Err(ProviderTaskStoreError::StaleLease),
            "confirmed recovered rejection did not close its recovery lease",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_heartbeat_migration_fails_closed_on_future_legacy_state() -> TestResult {
    let Some(database) = TestDatabase::new_before_capacity_heartbeats().await? else {
        return Ok(());
    };
    let result = async {
        let executor = seed_running_submission(&database.pool, "capacity-upgrade").await?;
        let mut invalid = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_capacity_allocations
            SET last_heartbeat_at_ms =
                floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 60_000
            WHERE executor_execution_id = $1
            "#,
        )
        .bind(executor.executor_execution_id)
        .execute(&mut *invalid)
        .await
        .map_err(debug_error)?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0020_provider_capacity_heartbeats.sql"
            ))
            .execute(&mut *invalid)
            .await
            .is_err(),
            "0020 accepted a future legacy capacity heartbeat",
        )?;
        invalid.rollback().await.map_err(debug_error)?;

        let mut valid = database.pool.begin().await.map_err(debug_error)?;
        sqlx::raw_sql(include_str!(
            "../migrations/0020_provider_capacity_heartbeats.sql"
        ))
        .execute(&mut *valid)
        .await
        .map_err(|error| format!("0020 failed after legacy rollback: {error}"))?;
        valid.commit().await.map_err(debug_error)?;
        let constraint_exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
              SELECT 1 FROM pg_constraint
              WHERE conrelid = 'provider_submit_recoveries'::regclass
                AND conname = 'provider_submit_recoveries_lease_deadline_check'
            )
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            constraint_exists,
            "successful 19 -> 20 upgrade omitted its deadline constraint",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn deadline_quarantine_migration_accepts_due_active_recovery() -> TestResult {
    let Some(database) = TestDatabase::new_before_deadline_quarantine().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "deadline-upgrade", 5_000).await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 60;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(90)).await;

        let mut business = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            SELECT submission_id
            FROM provider_remote_submit_intents
            WHERE submission_id = $1
            FOR UPDATE
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&mut *business)
        .await
        .map_err(debug_error)?;
        let mut migration_connection = database.pool.acquire().await.map_err(debug_error)?;
        let migration_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *migration_connection)
            .await
            .map_err(debug_error)?;
        let intent_relation_oid: i64 =
            sqlx::query_scalar("SELECT 'provider_remote_submit_intents'::regclass::oid::bigint")
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        let mut migration = tokio::spawn(async move {
            sqlx::raw_sql(include_str!(
                "../migrations/0021_provider_submit_deadline_quarantine.sql"
            ))
            .execute(&mut *migration_connection)
            .await
        });
        let lock_observation = match tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let locks: Vec<(String, bool)> = sqlx::query_as(
                    r#"
                    SELECT mode, granted
                    FROM pg_locks
                    WHERE pid = $1 AND relation::bigint = $2
                    ORDER BY mode, granted
                    "#,
                )
                .bind(migration_pid)
                .bind(intent_relation_oid)
                .fetch_all(&database.pool)
                .await
                .map_err(debug_error)?;
                if locks
                    .iter()
                    .any(|(mode, granted)| mode == "AccessExclusiveLock" && !granted)
                {
                    return Ok(locks);
                }
                if migration.is_finished() {
                    return Err("0021 completed before its blocked lock was observable".to_string());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        {
            Ok(observation) => observation,
            Err(_) => Err("0021 did not request its blocking table lock in time".to_string()),
        };
        business.commit().await.map_err(debug_error)?;
        let migration_result =
            match tokio::time::timeout(Duration::from_secs(5), &mut migration).await {
                Ok(Ok(Ok(_))) => Ok(()),
                Ok(Ok(Err(error))) => Err(format!(
                    "20 -> 21 migration rejected due active recovery: {error}"
                )),
                Ok(Err(error)) => Err(format!("20 -> 21 migration task failed: {error}")),
                Err(_) => {
                    migration.abort();
                    let _ = migration.await;
                    Err("20 -> 21 migration remained blocked after business commit".to_string())
                }
            };
        migration_result?;
        sqlx::raw_sql(include_str!(
            "../migrations/0022_provider_capacity_reconciliation.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("21 -> 22 migration failed after lock test: {error}"))?;
        let locks = lock_observation?;
        require(
            !locks
                .iter()
                .any(|(mode, granted)| mode == "ShareRowExclusiveLock" && *granted),
            format!("0021 held a weaker lock before ACCESS EXCLUSIVE: {locks:?}"),
        )?;
        let resolved = store
            .resolve_due_submit_deadline(&claim_scope())
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "migrated due recovery was not resolvable".to_string())?;
        require(
            resolved.state == ProviderSubmitIntentState::DeadlineQuarantined,
            "20 -> 21 migration changed deadline resolver semantics",
        )?;
        let index_definition: String = sqlx::query_scalar(
            r#"
            SELECT lower(indexdef) FROM pg_indexes
            WHERE schemaname = current_schema()
              AND indexname = 'provider_submit_recoveries_deadline_idx'
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            index_definition
                .contains("provider_account_id, provider_deadline_at_ms, submission_id")
                && !index_definition.contains("provider_id, provider_account_id")
                && index_definition.contains("where (state = 'active'::text)"),
            format!("deadline migration created the wrong queue index: {index_definition}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_reconciliation_migration_backfills_deadline_quarantine() -> TestResult {
    let Some(database) = TestDatabase::new_before_capacity_reconciliation().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "capacity-upgrade", 5_000).await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 40;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        force_deadline_quarantine_v21(&database.pool, &executor).await?;

        sqlx::raw_sql(include_str!(
            "../migrations/0022_provider_capacity_reconciliation.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("21 -> 22 capacity migration failed: {error}"))?;
        let backfill: (String, i64, String, i32) = sqlx::query_as(
            r#"
            SELECT reconciliation.state, reconciliation.evidence_revision,
                   allocation.state, policy.allocated_count
            FROM provider_capacity_reconciliations reconciliation
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = reconciliation.executor_execution_id
             AND allocation.submission_id = reconciliation.submission_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE reconciliation.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            backfill == ("active".to_string(), 0, "held".to_string(), 1),
            format!("21 -> 22 backfill diverged: {backfill:?}"),
        )?;
        let lease = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "capacity-upgrade-owner",
                "capacity-upgrade-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "backfilled reconciliation was not claimable".to_string())?;
        store
            .record_capacity_evidence(
                &lease,
                &ProviderCapacityEvidence {
                    event_identity: "capacity-upgrade-no-effect".to_string(),
                    outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
                },
            )
            .await
            .map_err(debug_error)?;
        require(
            sqlx::query_scalar::<_, i32>(
                "SELECT allocated_count FROM executor_resource_policies WHERE resource_policy_id = $1 AND revision = 1",
            )
            .bind(POLICY_ID)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?
                == 0,
            "backfilled reconciliation did not release capacity exactly once",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_reconciliation_migration_rejects_incomplete_quarantine() -> TestResult {
    let Some(database) = TestDatabase::new_before_capacity_reconciliation().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "capacity-drift", 5_000).await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 40;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        force_deadline_quarantine_v21(&database.pool, &executor).await?;

        sqlx::query(
            "ALTER TABLE provider_submit_recoveries DISABLE TRIGGER provider_submit_recoveries_reject_delete",
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query("DELETE FROM provider_submit_recoveries WHERE submission_id = $1")
            .bind(executor.submission_id)
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        sqlx::query(
            "ALTER TABLE provider_submit_recoveries ENABLE TRIGGER provider_submit_recoveries_reject_delete",
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0022_provider_capacity_reconciliation.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0022 silently skipped an incomplete deadline quarantine",
        )?;
        require(
            !sqlx::query_scalar::<_, bool>(
                "SELECT to_regclass('provider_capacity_reconciliations') IS NOT NULL",
            )
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?,
            "failed 0022 migration did not roll back atomically",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn atomic_artifact_migration_rejects_unresolved_ready_task() -> TestResult {
    let Some(database) = TestDatabase::new_before_atomic_artifact_resolution().await? else {
        return Ok(());
    };
    let result = async {
        let legacy =
            seed_v22_artifact_ready(&database.pool, "artifact-upgrade", "artifact-upgrade").await?;

        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0023_atomic_provider_artifact_resolution.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0023 accepted an unresolved artifact_ready task",
        )?;
        let migration_rolled_back: bool = sqlx::query_scalar(
            "SELECT to_regprocedure('enforce_provider_terminal_observation_projection()') IS NULL",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            migration_rolled_back,
            "failed 0023 migration did not roll back atomically",
        )?;
        let unresolved_projection: (String, String) = sqlx::query_as(
            r#"
            SELECT task.state, execution.state
            FROM provider_remote_tasks task
            JOIN executor_executions execution
              ON execution.executor_execution_id = task.executor_execution_id
             AND execution.submission_id = task.submission_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(legacy.executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            unresolved_projection == ("artifact_ready".to_string(), "provider_waiting".to_string()),
            format!("failed 0023 changed legacy evidence: {unresolved_projection:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn atomic_artifact_migration_backfills_canonical_ready_task() -> TestResult {
    let Some(database) = TestDatabase::new_before_atomic_artifact_resolution().await? else {
        return Ok(());
    };
    let result = async {
        let legacy = seed_v22_artifact_ready(
            &database.pool,
            "artifact-upgrade-canonical",
            "artifact-upgrade-canonical",
        )
        .await?;
        let mut canonical = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO executor_resolution_decisions
              (decision_id, executor_execution_id, submission_id, source,
               observation_id, provider_task_observation_id, resolved_state,
               result_manifest_id, error_code, decided_at_ms)
            VALUES ($1, $1, $2, 'remote_provider_observation',
                    NULL, $3, 'succeeded', $2, NULL, $4)
            "#,
        )
        .bind(legacy.executor.executor_execution_id)
        .bind(legacy.executor.submission_id)
        .bind(legacy.observation_id)
        .bind(legacy.observed_at_ms)
        .execute(&mut *canonical)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = 'succeeded', resolution_decision_id = $1,
                finished_at_ms = $3, updated_at_ms = $3, error_code = NULL
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'provider_waiting'
            "#,
        )
        .bind(legacy.executor.executor_execution_id)
        .bind(legacy.executor.submission_id)
        .bind(legacy.observed_at_ms)
        .execute(&mut *canonical)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = 'succeeded', result_manifest_id = $1,
                resolution_decision_id = $2, finished_at_ms = $3,
                updated_at_ms = $3, error_code = NULL
            WHERE executor_execution_id = $2 AND submission_id = $1
              AND state = 'provider_waiting'
            "#,
        )
        .bind(legacy.executor.submission_id)
        .bind(legacy.executor.executor_execution_id)
        .bind(legacy.observed_at_ms)
        .execute(&mut *canonical)
        .await
        .map_err(debug_error)?;
        let policy: (Uuid, i64) = sqlx::query_as(
            r#"
            SELECT resource_policy_id, resource_policy_revision
            FROM executor_capacity_allocations
            WHERE executor_execution_id = $1 AND submission_id = $2
            FOR UPDATE
            "#,
        )
        .bind(legacy.executor.executor_execution_id)
        .bind(legacy.executor.submission_id)
        .fetch_one(&mut *canonical)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_capacity_allocations
            SET state = 'released', released_at_ms = $3,
                release_decision_id = $1, released_state = 'succeeded',
                release_reason = 'remote_provider_observation',
                last_heartbeat_at_ms = GREATEST(last_heartbeat_at_ms, $3)
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'held'
            "#,
        )
        .bind(legacy.executor.executor_execution_id)
        .bind(legacy.executor.submission_id)
        .bind(legacy.observed_at_ms)
        .execute(&mut *canonical)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_resource_policies
            SET allocated_count = allocated_count - 1
            WHERE resource_policy_id = $1 AND revision = $2
              AND allocated_count > 0
            "#,
        )
        .bind(policy.0)
        .bind(policy.1)
        .execute(&mut *canonical)
        .await
        .map_err(debug_error)?;
        canonical.commit().await.map_err(debug_error)?;

        sqlx::raw_sql(include_str!(
            "../migrations/0023_atomic_provider_artifact_resolution.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let fingerprint: (Option<Uuid>, Option<String>, Option<i64>, Option<String>) =
            sqlx::query_as(
                r#"
                SELECT result_manifest_id, artifact_sha256_hex,
                       artifact_byte_size, artifact_media_type
                FROM provider_task_observations
                WHERE observation_id = $1
                "#,
            )
            .bind(legacy.observation_id)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        require(
            fingerprint
                == (
                    Some(legacy.executor.submission_id),
                    Some("c".repeat(64)),
                    Some(128),
                    Some("image/png".to_string()),
                ),
            format!("0023 did not backfill exact artifact evidence: {fingerprint:?}"),
        )?;
        require(
            sqlx::query(
                "UPDATE provider_task_observations SET payload_hash = payload_hash WHERE observation_id = $1",
            )
            .bind(legacy.observation_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "0023 did not restore append-only observation protection",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn atomic_artifact_migration_rolls_back_after_late_failure() -> TestResult {
    let Some(database) = TestDatabase::new_before_atomic_artifact_resolution().await? else {
        return Ok(());
    };
    let result = async {
        let script = format!(
            "{}\nDO $$ BEGIN RAISE EXCEPTION 'forced late migration failure'; END $$;",
            include_str!("../migrations/0023_atomic_provider_artifact_resolution.sql")
        );
        require(
            sqlx::raw_sql(AssertSqlSafe(script))
                .execute(&database.pool)
                .await
                .is_err(),
            "forced late 0023 failure unexpectedly committed",
        )?;
        let residue: (i64, bool, bool, bool) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*)
               FROM information_schema.columns
               WHERE table_schema = current_schema()
                 AND table_name = 'provider_task_observations'
                 AND column_name = 'result_manifest_id'),
              to_regclass('provider_task_observations_manifest_uidx') IS NOT NULL,
              to_regprocedure('enforce_provider_terminal_observation_projection()') IS NOT NULL,
              EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgrelid = 'provider_task_observations'::regclass
                  AND tgname = 'provider_task_observations_reject_mutation'
                  AND NOT tgisinternal
              )
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            residue == (0, false, false, true),
            format!("late 0023 failure left schema residue: {residue:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn recovery_command_migration_requires_drained_claimants() -> TestResult {
    let Some(database) = TestDatabase::new_before_replayable_recovery_commands().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor =
            seed_running_submission_with_lease(&database.pool, "recovery-command-upgrade", 5_000)
                .await?;
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 60_000;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let now = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            UPDATE provider_submit_recoveries
            SET recovery_owner = 'legacy-recovery-worker',
                recovery_lease_epoch = recovery_lease_epoch + 1,
                recovery_lease_expires_at_ms = $2 + 5_000,
                recovery_claimed_at_ms = $2, updated_at_ms = $2
            WHERE submission_id = $1 AND state = 'active'
            "#,
        )
        .bind(executor.submission_id)
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0024_replayable_provider_submit_recovery_commands.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0024 accepted a live legacy recovery claimant",
        )?;
        let command_table_exists: bool = sqlx::query_scalar(
            "SELECT to_regclass('provider_submit_recovery_commands') IS NOT NULL",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            !command_table_exists,
            "failed 0024 migration left command schema residue",
        )?;

        let now = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            UPDATE provider_submit_recoveries
            SET recovery_owner = NULL,
                recovery_lease_expires_at_ms = NULL,
                recovery_claimed_at_ms = NULL,
                next_recovery_at_ms = GREATEST(next_recovery_at_ms, $2 + 100),
                updated_at_ms = $2
            WHERE submission_id = $1 AND state = 'active'
            "#,
        )
        .bind(executor.submission_id)
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::raw_sql(include_str!(
            "../migrations/0024_replayable_provider_submit_recovery_commands.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("0024 rejected a drained recovery queue: {error}"))?;
        let migrated: (bool, bool) = sqlx::query_as(
            r#"
            SELECT to_regclass('provider_submit_recovery_commands') IS NOT NULL,
                   to_regclass('provider_submit_recovery_commands_pkey') IS NOT NULL
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(migrated == (true, true), "0024 schema is incomplete")?;
        let now = database_now(&database.pool).await?;
        let mut malformed_claim = database.pool.begin().await.map_err(debug_error)?;
        let malformed_epoch: i64 = sqlx::query_scalar(
            r#"
            UPDATE provider_submit_recoveries
            SET recovery_owner = 'malformed-claim-writer',
                recovery_lease_epoch = recovery_lease_epoch + 1,
                recovery_lease_expires_at_ms = $2 + 1,
                recovery_claimed_at_ms = $2, updated_at_ms = $2
            WHERE submission_id = $1 AND state = 'active'
            RETURNING recovery_lease_epoch
            "#,
        )
        .bind(executor.submission_id)
        .bind(now)
        .fetch_one(&mut *malformed_claim)
        .await
        .map_err(debug_error)?;
        let malformed_rejected = sqlx::query(
            r#"
            INSERT INTO provider_submit_recovery_commands (
                provider_id, provider_account_id, command_owner, command_id,
                command_kind, request_duration_ms, submission_id,
                executor_execution_id, recovery_lease_epoch,
                claim_claimed_at_ms, claim_lease_expires_at_ms,
                intent_state, intent_remote_operation_id,
                intent_provider_request_id, intent_send_started_at_ms,
                intent_receipt_event_identity, intent_failure_event_identity,
                intent_failure_error_code, intent_updated_at_ms, created_at_ms
            )
            SELECT recovery.provider_id, recovery.provider_account_id,
                   'malformed-claim-writer', 'malformed-claim', 'claim', 5000,
                   recovery.submission_id, recovery.executor_execution_id,
                   $2, $3, $3 + 1, intent.state, intent.remote_operation_id,
                   intent.provider_request_id, intent.send_started_at_ms,
                   intent.receipt_event_identity, intent.failure_event_identity,
                   intent.failure_error_code, intent.updated_at_ms, $3
            FROM provider_submit_recoveries recovery
            JOIN provider_remote_submit_intents intent
              ON intent.submission_id = recovery.submission_id
             AND intent.executor_execution_id = recovery.executor_execution_id
            WHERE recovery.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .bind(malformed_epoch)
        .bind(now)
        .execute(&mut *malformed_claim)
        .await
        .is_err();
        malformed_claim.rollback().await.map_err(debug_error)?;
        require(
            malformed_rejected,
            "0024 accepted a claim receipt whose duration did not match its lease",
        )?;
        let now = database_now(&database.pool).await?;
        require(
            sqlx::query(
                r#"
                UPDATE provider_submit_recoveries
                SET recovery_owner = 'legacy-after-migration',
                    recovery_lease_epoch = recovery_lease_epoch + 1,
                    recovery_lease_expires_at_ms = $2 + 5_000,
                    recovery_claimed_at_ms = $2, updated_at_ms = $2
                WHERE submission_id = $1 AND state = 'active'
                "#,
            )
            .bind(executor.submission_id)
            .bind(now)
            .execute(&database.pool)
            .await
            .is_err(),
            "0024 allowed an old writer to claim without command evidence",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn recovery_command_migration_rolls_back_after_late_failure() -> TestResult {
    let Some(database) = TestDatabase::new_before_replayable_recovery_commands().await? else {
        return Ok(());
    };
    let result = async {
        let script = format!(
            "{}\nDO $$ BEGIN RAISE EXCEPTION 'forced late migration failure'; END $$;",
            include_str!("../migrations/0024_replayable_provider_submit_recovery_commands.sql")
        );
        require(
            sqlx::raw_sql(AssertSqlSafe(script))
                .execute(&database.pool)
                .await
                .is_err(),
            "forced late 0024 failure unexpectedly committed",
        )?;
        let residue: (bool, bool) = sqlx::query_as(
            r#"
            SELECT to_regclass('provider_submit_recovery_commands') IS NOT NULL,
                   EXISTS (
                     SELECT 1 FROM pg_trigger
                     WHERE tgrelid = 'provider_submit_recoveries'::regclass
                       AND tgname =
                           'provider_submit_recovery_command_projection_check'
                   )
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            residue == (false, false),
            format!("late 0024 failure left schema residue: {residue:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_migration_backfills_under_first_lock_and_rolls_back() -> TestResult {
    let Some(database) = TestDatabase::new_before_remote_task_deadline().await? else {
        return Ok(());
    };
    let result = async {
        let (executor, expected_deadline) = seed_v24_remote_task(
            &database.pool,
            "remote-deadline-upgrade",
            "remote-deadline-upgrade",
            60_000,
            false,
        )
        .await?;
        let forced = format!(
            "{}\nDO $$ BEGIN RAISE EXCEPTION 'forced late migration failure'; END $$;",
            include_str!("../migrations/0025_provider_remote_task_deadline_quarantine.sql")
        );
        require(
            sqlx::raw_sql(AssertSqlSafe(forced))
                .execute(&database.pool)
                .await
                .is_err(),
            "forced late 0025 failure unexpectedly committed",
        )?;
        let residue: (i64, bool, bool) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = current_schema()
                 AND table_name = 'provider_remote_tasks'
                 AND column_name IN (
                    'provider_deadline_at_ms', 'deadline_quarantine_id'
                 )),
              to_regclass('provider_remote_task_quarantines') IS NOT NULL,
              EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgrelid = 'provider_remote_tasks'::regclass
                  AND tgname = 'provider_remote_task_update_guard'
                  AND NOT tgisinternal
              )
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            residue == (0, false, true),
            format!("late 0025 failure left schema residue: {residue:?}"),
        )?;

        let mut business = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT 1 FROM provider_remote_tasks WHERE submission_id = $1 FOR UPDATE")
            .bind(executor.submission_id)
            .execute(&mut *business)
            .await
            .map_err(debug_error)?;
        let mut migration_connection = database.pool.acquire().await.map_err(debug_error)?;
        let migration_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *migration_connection)
            .await
            .map_err(debug_error)?;
        let task_relation_oid: i64 =
            sqlx::query_scalar("SELECT 'provider_remote_tasks'::regclass::oid::bigint")
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        let mut migration = tokio::spawn(async move {
            sqlx::raw_sql(include_str!(
                "../migrations/0025_provider_remote_task_deadline_quarantine.sql"
            ))
            .execute(&mut *migration_connection)
            .await
        });
        let lock_observation = match tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let locks: Vec<(String, bool)> = sqlx::query_as(
                    r#"
                    SELECT mode, granted
                    FROM pg_locks
                    WHERE pid = $1 AND relation::bigint = $2
                    ORDER BY mode, granted
                    "#,
                )
                .bind(migration_pid)
                .bind(task_relation_oid)
                .fetch_all(&database.pool)
                .await
                .map_err(debug_error)?;
                if locks
                    .iter()
                    .any(|(mode, granted)| mode == "AccessExclusiveLock" && !granted)
                {
                    return Ok(locks);
                }
                if migration.is_finished() {
                    return Err("0025 completed before its first lock was observable".to_string());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        {
            Ok(observation) => observation,
            Err(_) => Err("0025 did not request its first table lock in time".to_string()),
        };
        business.commit().await.map_err(debug_error)?;
        match tokio::time::timeout(Duration::from_secs(5), &mut migration).await {
            Ok(Ok(Ok(_))) => {}
            Ok(Ok(Err(error))) => return Err(format!("24 -> 25 migration failed: {error}")),
            Ok(Err(error)) => return Err(format!("24 -> 25 migration task failed: {error}")),
            Err(_) => {
                migration.abort();
                let _ = migration.await;
                return Err("24 -> 25 migration remained blocked after business commit".to_string());
            }
        }
        let locks = lock_observation?;
        require(
            !locks
                .iter()
                .any(|(mode, granted)| mode == "ShareRowExclusiveLock" && *granted),
            format!("0025 acquired a weaker task lock before ACCESS EXCLUSIVE: {locks:?}"),
        )?;
        let migrated: (i64, i64, bool, String) = sqlx::query_as(
            r#"
            SELECT task.provider_deadline_at_ms, recovery.provider_deadline_at_ms,
                   task.deadline_quarantine_id IS NULL,
                   task.state
            FROM provider_remote_tasks task
            JOIN provider_submit_recoveries recovery
              ON recovery.submission_id = task.submission_id
             AND recovery.executor_execution_id = task.executor_execution_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            migrated
                == (
                    expected_deadline,
                    expected_deadline,
                    true,
                    "provider_waiting".to_string(),
                ),
            format!("24 -> 25 deadline backfill diverged: {migrated:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_migration_rejects_active_poll_owner() -> TestResult {
    let Some(database) = TestDatabase::new_before_remote_task_deadline().await? else {
        return Ok(());
    };
    let result = async {
        seed_v24_remote_task(
            &database.pool,
            "remote-deadline-active-upgrade",
            "remote-deadline-active-upgrade",
            60_000,
            true,
        )
        .await?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0025_provider_remote_task_deadline_quarantine.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0025 accepted an active legacy poll owner",
        )?;
        assert_remote_task_deadline_migration_rolled_back(&database.pool).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_migration_rejects_due_waiting_task() -> TestResult {
    let Some(database) = TestDatabase::new_before_remote_task_deadline().await? else {
        return Ok(());
    };
    let result = async {
        let (_, deadline) = seed_v24_remote_task(
            &database.pool,
            "remote-deadline-due-upgrade",
            "remote-deadline-due-upgrade",
            150,
            false,
        )
        .await?;
        sleep_until_database_time(&database.pool, deadline + 20).await?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0025_provider_remote_task_deadline_quarantine.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0025 silently migrated a waiting task already past its deadline",
        )?;
        assert_remote_task_deadline_migration_rolled_back(&database.pool).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_migration_rejects_reserved_legacy_event() -> TestResult {
    let Some(database) = TestDatabase::new_before_remote_task_deadline().await? else {
        return Ok(());
    };
    let result = async {
        let (executor, _) = seed_v24_remote_task(
            &database.pool,
            "remote-deadline-event-upgrade",
            "remote-deadline-event-upgrade",
            60_000,
            false,
        )
        .await?;
        let now = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            INSERT INTO provider_task_observations
              (observation_id, submission_id, executor_execution_id,
               event_identity, source, observed_state, effect_certainty,
               next_poll_at_ms, payload_hash, observed_at_ms)
            VALUES ($1, $2, $3, 'internal:artifact-authority-recovery-v1',
                    'verified_callback', 'provider_waiting', 'not_applicable',
                    $4, $5, $4)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(executor.submission_id)
        .bind(executor.executor_execution_id)
        .bind(now)
        .bind("a".repeat(64))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0025_provider_remote_task_deadline_quarantine.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0025 accepted a legacy event occupying the internal recovery identity",
        )?;
        assert_remote_task_deadline_migration_rolled_back(&database.pool).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn provider_submit_commit_classifies_deferred_projection_conflicts() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let receipt_executor =
            seed_running_submission_with_lease(&database.pool, "deferred-receipt", 5_000).await?;
        let mut receipt_reservation = reservation_request(&receipt_executor);
        receipt_reservation.provider_timeout_ms = 60_000;
        store
            .reserve_submit(&receipt_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&receipt_reservation)
            .await
            .map_err(debug_error)?;
        sqlx::raw_sql(
            r#"
            CREATE FUNCTION test_deferred_submit_conflict() RETURNS TRIGGER AS $$
            BEGIN
                RAISE EXCEPTION USING
                    ERRCODE = 'P0001',
                    MESSAGE = 'injected deferred provider submit conflict';
            END;
            $$ LANGUAGE plpgsql;

            CREATE CONSTRAINT TRIGGER test_deferred_submit_receipt_conflict
            AFTER UPDATE ON provider_remote_submit_intents
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW
            WHEN (NEW.receipt_event_identity = 'receipt-deferred-conflict')
            EXECUTE FUNCTION test_deferred_submit_conflict();
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            store
                .record_submit_receipt(&submit_receipt(
                    &receipt_executor,
                    "operation-deferred-conflict",
                    "receipt-deferred-conflict",
                ))
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "deferred receipt projection failure was not classified as conflict",
        )?;
        require(
            store
                .load_submit_intent(receipt_executor.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| {
                    intent.state == ProviderSubmitIntentState::Sending
                        && intent.remote_operation_id.is_none()
                }),
            "failed receipt commit did not roll back its intent update",
        )?;

        let deadline_executor =
            seed_running_submission_with_lease(&database.pool, "deferred-deadline", 5_000).await?;
        let mut deadline_reservation = reservation_request(&deadline_executor);
        deadline_reservation.provider_timeout_ms = 40;
        store
            .reserve_submit(&deadline_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&deadline_reservation)
            .await
            .map_err(debug_error)?;
        sqlx::raw_sql(
            r#"
            CREATE CONSTRAINT TRIGGER test_deferred_submit_deadline_conflict
            AFTER UPDATE ON provider_remote_submit_intents
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW
            WHEN (NEW.state = 'deadline_quarantined')
            EXECUTE FUNCTION test_deferred_submit_conflict();
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(70)).await;
        require(
            store.resolve_due_submit_deadline(&claim_scope()).await
                == Err(ProviderTaskStoreError::Conflict),
            "deferred deadline projection failure was not classified as conflict",
        )?;
        require(
            store
                .load_submit_intent(deadline_executor.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| intent.state == ProviderSubmitIntentState::Sending),
            "failed deadline commit did not roll back quarantine",
        )?;
        sqlx::raw_sql(
            r#"
            DROP TRIGGER test_deferred_submit_deadline_conflict
                ON provider_remote_submit_intents;
            DROP TRIGGER test_deferred_submit_receipt_conflict
                ON provider_remote_submit_intents;
            DROP FUNCTION test_deferred_submit_conflict();
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            store
                .resolve_due_submit_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| {
                    intent.state == ProviderSubmitIntentState::DeadlineQuarantined
                }),
            "deadline did not resolve after removing the injected conflict",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_deadline_quarantines_capacity_and_preserves_late_receipts() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "deadline-quarantine", 5_000)
                .await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 200;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let ambiguity = submit_failure(
            &executor,
            ProviderSubmitFailureKind::OutcomeUnknown,
            "deadline-ambiguity",
            "provider_submit_ambiguous",
        );
        store
            .record_submit_failure(&ambiguity)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut plan_tx = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *plan_tx)
            .await
            .map_err(debug_error)?;
        let plan: Vec<String> = sqlx::query_scalar(
            r#"
            EXPLAIN (COSTS OFF)
            SELECT recovery.submission_id, recovery.executor_execution_id
            FROM provider_submit_recoveries recovery
            JOIN provider_remote_submit_intents intent
              ON intent.submission_id = recovery.submission_id
             AND intent.executor_execution_id = recovery.executor_execution_id
            JOIN executor_executions execution
              ON execution.executor_execution_id = recovery.executor_execution_id
             AND execution.submission_id = recovery.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = recovery.executor_execution_id
             AND submission.submission_id = recovery.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = recovery.executor_execution_id
             AND allocation.submission_id = recovery.submission_id
            WHERE recovery.provider_id = $1
              AND recovery.provider_account_id = $2
              AND recovery.state = 'active'
              AND recovery.provider_deadline_at_ms <= floor(
                    extract(epoch FROM statement_timestamp()) * 1000
                  )::BIGINT
              AND intent.state IN ('sending', 'outcome_unknown', 'operation_known')
              AND execution.state = 'running'
              AND submission.state = 'running'
              AND allocation.state = 'held'
            ORDER BY recovery.provider_deadline_at_ms, recovery.submission_id
            FOR UPDATE OF execution, submission, allocation SKIP LOCKED
            LIMIT 1
            "#,
        )
        .bind("provider-test")
        .bind(ACCOUNT_ID)
        .fetch_all(&mut *plan_tx)
        .await
        .map_err(debug_error)?;
        plan_tx.rollback().await.map_err(debug_error)?;
        let plan = plan.join("\n");
        require(
            plan.contains("provider_submit_recoveries_deadline_idx")
                && !plan
                    .lines()
                    .any(|line| line.trim_start().starts_with("Sort")),
            format!("deadline resolver lost its bounded queue plan:\n{plan}"),
        )?;

        let mut resolvers = tokio::task::JoinSet::new();
        for _ in 0..64 {
            let store = store.clone();
            resolvers.spawn(async move { store.resolve_due_submit_deadline(&claim_scope()).await });
        }
        let mut winners = 0;
        while let Some(result) = resolvers.join_next().await {
            if result.map_err(debug_error)?.map_err(debug_error)?.is_some() {
                winners += 1;
            }
        }
        require(
            winners == 1,
            "concurrent deadline resolvers did not elect one winner",
        )?;

        let intent = store
            .load_submit_intent(executor.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "deadline resolver lost its submit intent".to_string())?;
        require(
            intent.state == ProviderSubmitIntentState::DeadlineQuarantined
                && intent.remote_operation_id.is_none(),
            "deadline resolver did not preserve unknown remote effect",
        )?;
        let projection: (String, String, String, String, String, String, i32, String) =
            sqlx::query_as(
                r#"
                SELECT execution.state, submission.state, decision.source,
                       decision.resolved_state, decision.error_code,
                       allocation.state, policy.allocated_count, recovery.state
                FROM executor_executions execution
                JOIN provider_submissions submission
                  ON submission.executor_execution_id = execution.executor_execution_id
                 AND submission.submission_id = execution.submission_id
                JOIN executor_resolution_decisions decision
                  ON decision.decision_id = execution.resolution_decision_id
                JOIN executor_capacity_allocations allocation
                  ON allocation.executor_execution_id = execution.executor_execution_id
                 AND allocation.submission_id = execution.submission_id
                JOIN executor_resource_policies policy
                  ON policy.resource_policy_id = allocation.resource_policy_id
                 AND policy.revision = allocation.resource_policy_revision
                JOIN provider_submit_recoveries recovery
                  ON recovery.executor_execution_id = execution.executor_execution_id
                 AND recovery.submission_id = execution.submission_id
                WHERE execution.executor_execution_id = $1
                "#,
            )
            .bind(executor.executor_execution_id)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        require(
            projection
                == (
                    "uncertain".to_string(),
                    "uncertain".to_string(),
                    "remote_submit_deadline".to_string(),
                    "uncertain".to_string(),
                    "provider_submit_deadline".to_string(),
                    "held".to_string(),
                    1,
                    "closed".to_string(),
                ),
            format!("deadline quarantine projection diverged: {projection:?}"),
        )?;
        let durable_counts: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM executor_resolution_decisions
               WHERE executor_execution_id = $1),
              (SELECT COUNT(*) FROM executor_terminal_reductions
               WHERE executor_execution_id = $1),
              (SELECT COUNT(*) FROM provider_remote_tasks
               WHERE executor_execution_id = $1)
            "#,
        )
        .bind(executor.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            durable_counts == (1, 1, 0),
            format!("deadline resolver duplicated durable evidence: {durable_counts:?}"),
        )?;
        require(
            store
                .resolve_due_submit_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .is_none(),
            "resolved deadline remained claimable",
        )?;
        require(
            store
                .record_submit_failure(&ambiguity)
                .await
                .map_err(debug_error)?
                .state
                == ProviderSubmitIntentState::DeadlineQuarantined,
            "exact ambiguity replay lost its terminal evidence",
        )?;
        require(
            sqlx::query(
                r#"
                UPDATE executor_capacity_allocations
                SET state = 'released', released_at_ms = last_heartbeat_at_ms,
                    release_decision_id = $1, released_state = 'uncertain',
                    release_reason = 'terminal_evidence'
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(executor.executor_execution_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "raw SQL released deadline-quarantined provider capacity",
        )?;

        let receipt = submit_receipt(
            &executor,
            "operation-late-after-deadline",
            "receipt-late-after-deadline",
        );
        let late = store
            .record_submit_receipt(&receipt)
            .await
            .map_err(debug_error)?;
        require(
            late.state == ProviderSubmitIntentState::DeadlineQuarantined
                && late.remote_operation_id.as_deref() == Some("operation-late-after-deadline"),
            "late receipt changed the customer terminal result or lost provider identity",
        )?;
        require(
            store
                .record_submit_receipt(&receipt)
                .await
                .map_err(debug_error)?
                == late,
            "exact late receipt replay did not converge",
        )?;
        let conflicting = submit_receipt(
            &executor,
            "operation-conflicting-after-deadline",
            "receipt-conflicting-after-deadline",
        );
        require(
            store.record_submit_receipt(&conflicting).await
                == Err(ProviderTaskStoreError::Conflict),
            "conflicting late receipt rewrote provider identity",
        )?;
        require(
            store
                .attach(&attach_request(
                    &executor,
                    "operation-late-after-deadline",
                    "attach-late-after-deadline",
                ))
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "deadline-quarantined receipt reopened provider attachment",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_deadline_races_converge_without_deadlock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "deadline-receipt-race", 5_000)
                .await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 80;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let receipt = submit_receipt(
            &executor,
            "operation-deadline-race",
            "receipt-deadline-race",
        );
        let scope = claim_scope();
        let (receipt_result, deadline_result) =
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    store.record_submit_receipt(&receipt),
                    store.resolve_due_submit_deadline(&scope)
                )
            })
            .await
            .map_err(|_| "deadline and late receipt deadlocked".to_string())?;
        receipt_result.map_err(|error| format!("deadline race receipt: {error:?}"))?;
        if deadline_result
            .map_err(|error| format!("deadline race resolver: {error:?}"))?
            .is_none()
        {
            store
                .resolve_due_submit_deadline(&claim_scope())
                .await
                .map_err(|error| format!("deadline race retry: {error:?}"))?
                .ok_or_else(|| "skipped deadline did not become claimable".to_string())?;
        }
        let intent = store
            .load_submit_intent(executor.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "deadline race lost its intent".to_string())?;
        require(
            intent.state == ProviderSubmitIntentState::DeadlineQuarantined
                && intent.remote_operation_id.as_deref() == Some("operation-deadline-race")
                && intent.receipt_event_identity.as_deref() == Some("receipt-deadline-race"),
            "deadline and receipt race did not preserve both terminal and provider evidence",
        )?;
        let evidence: (i64, i64, String) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM executor_resolution_decisions
               WHERE executor_execution_id = $1),
              (SELECT COUNT(*) FROM provider_remote_tasks
               WHERE executor_execution_id = $1),
              (SELECT state FROM executor_capacity_allocations
               WHERE executor_execution_id = $1)
            "#,
        )
        .bind(executor.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            evidence == (1, 0, "held".to_string()),
            format!("deadline race produced conflicting evidence: {evidence:?}"),
        )?;

        let attach_executor =
            seed_running_submission_with_lease(&database.pool, "deadline-attach-race", 5_000)
                .await?;
        let mut attach_reservation = reservation_request(&attach_executor);
        attach_reservation.provider_timeout_ms = 200;
        store
            .reserve_submit(&attach_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&attach_reservation)
            .await
            .map_err(debug_error)?;
        store
            .record_submit_receipt(&submit_receipt(
                &attach_executor,
                "operation-deadline-attach-race",
                "receipt-deadline-attach-race",
            ))
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let attach = attach_request(
            &attach_executor,
            "operation-deadline-attach-race",
            "attach-deadline-race",
        );
        let attach_scope = claim_scope();
        let (attach_result, deadline_result) =
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    store.attach(&attach),
                    store.resolve_due_submit_deadline(&attach_scope)
                )
            })
            .await
            .map_err(|_| "deadline and attach deadlocked".to_string())?;
        require(
            matches!(
                attach_result,
                Err(ProviderTaskStoreError::Conflict | ProviderTaskStoreError::StaleLease)
            ),
            "deadline-due attach retained authority",
        )?;
        if deadline_result.map_err(debug_error)?.is_none() {
            store
                .resolve_due_submit_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .ok_or_else(|| "attach-skipped deadline did not become claimable".to_string())?;
        }
        let attach_projection: (String, i64) = sqlx::query_as(
            r#"
            SELECT intent.state,
                   (SELECT COUNT(*) FROM provider_remote_tasks task
                    WHERE task.submission_id = intent.submission_id)
            FROM provider_remote_submit_intents intent
            WHERE intent.submission_id = $1
            "#,
        )
        .bind(attach_executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            attach_projection == ("deadline_quarantined".to_string(), 0),
            format!("attach race escaped quarantine: {attach_projection:?}"),
        )?;

        let heartbeat_executor =
            seed_running_submission_with_lease(&database.pool, "deadline-heartbeat-race", 100)
                .await?;
        let mut heartbeat_reservation = reservation_request(&heartbeat_executor);
        heartbeat_reservation.provider_timeout_ms = 1_000;
        store
            .reserve_submit(&heartbeat_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&heartbeat_reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let recovery = store
            .claim_submit_recovery(
                &claim_scope(),
                "deadline-heartbeat-owner",
                "claim-deadline-heartbeat",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "pre-deadline recovery was not claimable".to_string())?;
        tokio::time::sleep(Duration::from_millis(900)).await;
        let heartbeat_scope = claim_scope();
        let (heartbeat_result, deadline_result) =
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    store.heartbeat_submit_recovery(&recovery, 2_000),
                    store.resolve_due_submit_deadline(&heartbeat_scope)
                )
            })
            .await
            .map_err(|_| "deadline and recovery heartbeat deadlocked".to_string())?;
        require(
            heartbeat_result == Err(ProviderTaskStoreError::StaleLease),
            "post-deadline recovery heartbeat retained authority",
        )?;
        if deadline_result.map_err(debug_error)?.is_none() {
            store
                .resolve_due_submit_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .ok_or_else(|| "heartbeat-skipped deadline did not become claimable".to_string())?;
        }
        require(
            store
                .load_submit_intent(heartbeat_executor.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| {
                    intent.state == ProviderSubmitIntentState::DeadlineQuarantined
                }),
            "heartbeat race did not converge to deadline quarantine",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_reconciliation_is_scoped_fenced_and_exactly_replayable() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let first = seed_deadline_quarantine(&database.pool, &store, "capacity-release").await?;
        let raw_release_at = database_now(&database.pool).await?;
        require(
            sqlx::query(
                r#"
                UPDATE executor_capacity_allocations
                SET state = 'released', released_at_ms = $2,
                    release_decision_id = $1, released_state = 'uncertain',
                    release_reason = 'provider_capacity_reconciliation',
                    release_reconciliation_id = $1,
                    last_heartbeat_at_ms = GREATEST(last_heartbeat_at_ms, $2)
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(first.executor_execution_id)
            .bind(raw_release_at)
            .execute(&database.pool)
            .await
            .is_err(),
            "raw SQL released quarantined capacity without strong evidence",
        )?;

        let mut plan_tx = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *plan_tx)
            .await
            .map_err(debug_error)?;
        let plan: Vec<String> = sqlx::query_scalar(
            r#"
            EXPLAIN (COSTS OFF)
            WITH queue_candidates AS MATERIALIZED (
              SELECT reconciliation.submission_id,
                     reconciliation.executor_execution_id,
                     reconciliation.available_at_ms,
                     reconciliation.provider_deadline_at_ms
              FROM provider_capacity_reconciliations reconciliation
              WHERE reconciliation.provider_id = $1
                AND reconciliation.provider_account_id = $2
                AND reconciliation.state = 'active'
                AND reconciliation.available_at_ms <= floor(
                      extract(epoch FROM statement_timestamp()) * 1000
                    )::BIGINT
              ORDER BY reconciliation.available_at_ms,
                       reconciliation.provider_deadline_at_ms,
                       reconciliation.submission_id
              LIMIT 64
            )
            SELECT candidate.submission_id
            FROM queue_candidates candidate
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = candidate.executor_execution_id
             AND allocation.submission_id = candidate.submission_id
             AND allocation.state = 'held'
            ORDER BY candidate.available_at_ms,
                     candidate.provider_deadline_at_ms,
                     candidate.submission_id
            FOR UPDATE OF allocation SKIP LOCKED
            LIMIT 1
            "#,
        )
        .bind("provider-test")
        .bind(ACCOUNT_ID)
        .fetch_all(&mut *plan_tx)
        .await
        .map_err(debug_error)?;
        plan_tx.rollback().await.map_err(debug_error)?;
        let plan = plan.join("\n");
        require(
            plan.contains("provider_capacity_reconciliations_claim_idx")
                && plan.matches("Limit").count() >= 2,
            format!("capacity reconciliation lost its bounded queue plan:\n{plan}"),
        )?;

        let wrong_scope = ProviderTaskClaimScope {
            provider_id: "provider-test".to_string(),
            provider_account_id: Uuid::new_v4(),
        };
        require(
            store
                .claim_due_capacity_reconciliation(
                    &wrong_scope,
                    "wrong-capacity-account",
                    "wrong-capacity-claim",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "capacity reconciliation crossed its account scope",
        )?;

        let mut claims = tokio::task::JoinSet::new();
        for index in 0..64 {
            let store = store.clone();
            claims.spawn(async move {
                let command = format!("capacity-claim-{index}");
                let result = store
                    .claim_due_capacity_reconciliation(
                        &claim_scope(),
                        "capacity-reconciler",
                        &command,
                        5_000,
                    )
                    .await;
                (command, result)
            });
        }
        let mut winner = None;
        while let Some(result) = claims.join_next().await {
            let (command, claimed) = result.map_err(debug_error)?;
            if let Some(lease) = claimed.map_err(debug_error)? {
                require(winner.is_none(), "more than one capacity claimant won")?;
                winner = Some((command, lease));
            }
        }
        let (claim_command, lease) = winner.ok_or_else(|| "no capacity claimant won".to_string())?;
        let replay_index: String = sqlx::query_scalar(
            r#"
            SELECT lower(indexdef)
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND indexname =
                  'provider_capacity_reconciliations_claim_command_idx'
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            replay_index.contains("unique index")
                && replay_index.contains(
                    "(provider_id, provider_account_id, last_command_owner, last_command_id)"
                )
                && replay_index.contains("where")
                && replay_index.contains("last_command_kind = 'claim'"),
            format!("claim acknowledgement replay index diverged: {replay_index}"),
        )?;
        let replay = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "capacity-reconciler",
                &claim_command,
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "exact claim replay lost its lease".to_string())?;
        require(replay == lease, "exact claim replay changed the lease epoch")?;
        let mut replay_blocker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            "SELECT TRUE FROM executor_capacity_allocations WHERE executor_execution_id = $1 FOR UPDATE",
        )
        .bind(first.executor_execution_id)
        .fetch_one(&mut *replay_blocker)
        .await
        .map_err(debug_error)?;
        let replay_store = store.clone();
        let blocked_command = claim_command.clone();
        let mut blocked_replay = tokio::spawn(async move {
            replay_store
                .claim_due_capacity_reconciliation(
                    &claim_scope(),
                    "capacity-reconciler",
                    &blocked_command,
                    5_000,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        require(
            !blocked_replay.is_finished(),
            "exact claim replay skipped a temporarily locked allocation",
        )?;
        replay_blocker.commit().await.map_err(debug_error)?;
        let blocked_result = tokio::time::timeout(Duration::from_secs(3), &mut blocked_replay)
            .await
            .map_err(|_| "exact claim replay remained blocked".to_string())?
            .map_err(debug_error)?
            .map_err(debug_error)?;
        require(
            blocked_result == Some(lease.clone()),
            "lock-delayed claim replay created different authority",
        )?;
        require(
            lease.context().provider_deadline_at_ms()
                == lease.reconciliation.provider_deadline_at_ms,
            "capacity claim re-resolved its frozen provider context",
        )?;

        let evidence = ProviderCapacityEvidence {
            event_identity: "confirmed-no-effect-1".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
        };
        let released = store
            .record_capacity_evidence(&lease, &evidence)
            .await
            .map_err(debug_error)?;
        require(
            released.state == ProviderCapacityReconciliationState::Released
                && released.evidence.as_ref() == Some(&evidence),
            "strong no-effect evidence was not frozen",
        )?;
        require(
            store
                .record_capacity_evidence(&lease, &evidence)
                .await
                .map_err(debug_error)?
                == released,
            "release acknowledgement loss was not exactly replayable",
        )?;
        require(
            store
                .record_capacity_evidence(
                    &lease,
                    &ProviderCapacityEvidence {
                        event_identity: "conflicting-no-effect".to_string(),
                        outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
                    },
                )
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "conflicting capacity evidence rewrote a release",
        )?;
        let projection: (String, String, String, i32) = sqlx::query_as(
            r#"
            SELECT execution.state, allocation.state, allocation.release_reason,
                   policy.allocated_count
            FROM executor_executions execution
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(first.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection
                == (
                    "uncertain".to_string(),
                    "released".to_string(),
                    "provider_capacity_reconciliation".to_string(),
                    0,
                ),
            format!("capacity evidence release diverged: {projection:?}"),
        )?;
        let late_after_release = store
            .record_submit_receipt(&submit_receipt(
                &first,
                "operation-after-no-effect",
                "receipt-after-no-effect",
            ))
            .await
            .map_err(debug_error)?;
        require(
            late_after_release.state == ProviderSubmitIntentState::DeadlineQuarantined,
            "late receipt reopened the customer result after release",
        )?;

        let second = seed_deadline_quarantine(&database.pool, &store, "capacity-revision").await?;
        let stale = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "revision-owner-a",
                "revision-claim-a",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "revision test was not claimable".to_string())?;
        let raw_receipt_at = database_now(&database.pool).await?;
        require(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET remote_operation_id = 'raw-receipt-operation',
                    provider_request_id = 'raw-receipt-request',
                    receipt_event_identity = 'raw-receipt-event',
                    updated_at_ms = $2
                WHERE submission_id = $1
                  AND state = 'deadline_quarantined'
                  AND remote_operation_id IS NULL
                "#,
            )
            .bind(second.submission_id)
            .bind(raw_receipt_at)
            .execute(&database.pool)
            .await
            .is_err(),
            "raw receipt bypassed the reconciliation evidence revision",
        )?;
        store
            .record_submit_receipt(&submit_receipt(
                &second,
                "operation-before-release",
                "receipt-before-release",
            ))
            .await
            .map_err(debug_error)?;
        require(
            store
                .claim_due_capacity_reconciliation(
                    &claim_scope(),
                    "revision-owner-a",
                    "revision-claim-a",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                == Some(stale.clone()),
            "receipt wake changed the exact claim response snapshot",
        )?;
        require(
            store
                .heartbeat_capacity_reconciliation(&stale, 5_000)
                .await
                == Err(ProviderTaskStoreError::StaleLease),
            "receipt evidence did not fence a stale heartbeat",
        )?;
        store
            .defer_capacity_reconciliation(&stale, "revision-defer-a", 60_000)
            .await
            .map_err(debug_error)?;
        store
            .defer_capacity_reconciliation(&stale, "revision-defer-a", 60_000)
            .await
            .map_err(debug_error)?;
        let fresh = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "revision-owner-b",
                "revision-claim-b",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "fresh receipt evidence was deferred away".to_string())?;
        require(
            fresh.reconciliation_lease_epoch > stale.reconciliation_lease_epoch
                && fresh.claimed_evidence_revision == 1,
            "receipt wake did not advance both lease and evidence fences",
        )?;
        require(
            store
                .record_capacity_evidence(
                    &fresh,
                    &ProviderCapacityEvidence {
                        event_identity: "wrong-terminal-operation".to_string(),
                        outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                            remote_operation_id: "operation-conflict".to_string(),
                            terminal_state: ProviderCapacityTerminalState::Failed,
                        },
                    },
                )
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "terminal evidence changed the durable remote operation",
        )?;
        let terminal = ProviderCapacityEvidence {
            event_identity: "terminal-operation-before-release".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                remote_operation_id: "operation-before-release".to_string(),
                terminal_state: ProviderCapacityTerminalState::Succeeded,
            },
        };
        store
            .record_capacity_evidence(&fresh, &terminal)
            .await
            .map_err(debug_error)?;

        let third = seed_deadline_quarantine(&database.pool, &store, "capacity-terminal").await?;
        let third_lease = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "terminal-owner",
                "terminal-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "terminal evidence test was not claimable".to_string())?;
        let third_evidence = ProviderCapacityEvidence {
            event_identity: "terminal-with-receipt".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                remote_operation_id: "operation-terminal-authority".to_string(),
                terminal_state: ProviderCapacityTerminalState::Canceled,
            },
        };
        require(
            store
                .record_capacity_evidence(&third_lease, &third_evidence)
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "remote terminal evidence established an unowned operation identity",
        )?;
        store
            .record_submit_receipt(&submit_receipt(
                &third,
                "operation-terminal-authority",
                "receipt-terminal-authority",
            ))
            .await
            .map_err(debug_error)?;
        store
            .defer_capacity_reconciliation(&third_lease, "terminal-defer", 60_000)
            .await
            .map_err(debug_error)?;
        let third_fresh = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "terminal-finisher",
                "terminal-reclaim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "terminal receipt wake was not claimable".to_string())?;
        store
            .record_capacity_evidence(&third_fresh, &third_evidence)
            .await
            .map_err(debug_error)?;
        require(
            store
                .record_submit_receipt(&submit_receipt(
                    &third,
                    "operation-terminal-conflict",
                    "receipt-terminal-conflict",
                ))
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "late receipt contradicted remote terminal evidence",
        )?;
        store
            .record_submit_receipt(&submit_receipt(
                &third,
                "operation-terminal-authority",
                "receipt-terminal-authority",
            ))
            .await
            .map_err(debug_error)?;

        let fourth = seed_deadline_quarantine(&database.pool, &store, "capacity-stale-epoch").await?;
        let expired = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "expired-owner",
                "expired-claim",
                40,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "stale epoch test was not claimable".to_string())?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        require(
            store
                .claim_due_capacity_reconciliation(
                    &claim_scope(),
                    "expired-owner",
                    "expired-claim",
                    40,
                )
                .await
                .map_err(debug_error)?
                == Some(expired.clone()),
            "expired claim acknowledgement replay created a new epoch",
        )?;
        let reclaimed = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "reclaimed-owner",
                "reclaimed-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired capacity lease was not reclaimable".to_string())?;
        let stale_evidence = ProviderCapacityEvidence {
            event_identity: "expired-owner-no-effect".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
        };
        require(
            store
                .record_capacity_evidence(&expired, &stale_evidence)
                .await
                == Err(ProviderTaskStoreError::StaleLease),
            "expired reconciliation epoch released capacity",
        )?;
        store
            .record_capacity_evidence(
                &reclaimed,
                &ProviderCapacityEvidence {
                    event_identity: "reclaimed-owner-no-effect".to_string(),
                    outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
                },
            )
            .await
            .map_err(debug_error)?;
        require(
            store
                .load_submit_intent(fourth.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| {
                    intent.state == ProviderSubmitIntentState::DeadlineQuarantined
                }),
            "capacity reconciliation changed the customer deadline decision",
        )?;

        require(
            sqlx::query_scalar::<_, i32>(
                "SELECT allocated_count FROM executor_resource_policies WHERE resource_policy_id = $1 AND revision = 1",
            )
            .bind(POLICY_ID)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?
                == 0,
            "capacity releases did not balance the shared policy counter",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_evidence_and_late_receipt_race_converges_without_deadlock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor =
            seed_deadline_quarantine(&database.pool, &store, "capacity-receipt-race").await?;
        let lease = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "capacity-race-owner",
                "capacity-race-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "capacity race was not claimable".to_string())?;
        let receipt = submit_receipt(
            &executor,
            "operation-capacity-race",
            "receipt-capacity-race",
        );
        let evidence = ProviderCapacityEvidence {
            event_identity: "terminal-capacity-race".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                remote_operation_id: "operation-capacity-race".to_string(),
                terminal_state: ProviderCapacityTerminalState::Succeeded,
            },
        };
        let (receipt_result, evidence_result) =
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    store.record_submit_receipt(&receipt),
                    store.record_capacity_evidence(&lease, &evidence)
                )
            })
            .await
            .map_err(|_| "capacity evidence and late receipt deadlocked".to_string())?;
        receipt_result.map_err(debug_error)?;
        match evidence_result {
            Ok(_) => {}
            Err(ProviderTaskStoreError::StaleLease) => {
                store
                    .defer_capacity_reconciliation(&lease, "capacity-race-defer", 60_000)
                    .await
                    .map_err(debug_error)?;
                let fresh = store
                    .claim_due_capacity_reconciliation(
                        &claim_scope(),
                        "capacity-race-finisher",
                        "capacity-race-reclaim",
                        5_000,
                    )
                    .await
                    .map_err(debug_error)?
                    .ok_or_else(|| "receipt wake was lost during race recovery".to_string())?;
                store
                    .record_capacity_evidence(&fresh, &evidence)
                    .await
                    .map_err(debug_error)?;
            }
            Err(error) => return Err(format!("capacity evidence race failed: {error:?}")),
        }
        let projection: (String, String, String, i32) = sqlx::query_as(
            r#"
            SELECT intent.state, allocation.state, reconciliation.state,
                   policy.allocated_count
            FROM provider_remote_submit_intents intent
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = intent.executor_execution_id
             AND allocation.submission_id = intent.submission_id
            JOIN provider_capacity_reconciliations reconciliation
              ON reconciliation.executor_execution_id = intent.executor_execution_id
             AND reconciliation.submission_id = intent.submission_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE intent.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection
                == (
                    "deadline_quarantined".to_string(),
                    "released".to_string(),
                    "released".to_string(),
                    0,
                ),
            format!("receipt/evidence race did not converge: {projection:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_lifecycle_migration_backfills_attached_receipts() -> TestResult {
    let Some(database) = TestDatabase::new_before_submit_lifecycle().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "submit-upgrade").await?;
        seed_legacy_attached_task(&database.pool, &lease).await?;
        sqlx::raw_sql(include_str!(
            "../migrations/0018_provider_submit_lifecycle.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("0018 attached receipt migration failed: {error}"))?;
        let migrated: (String, String, Option<String>, Option<i64>, Option<String>) =
            sqlx::query_as(
                r#"
                SELECT state, remote_operation_id, provider_request_id,
                       send_started_at_ms, receipt_event_identity
                FROM provider_remote_submit_intents
                WHERE submission_id = $1
                "#,
            )
            .bind(lease.submission_id)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        require(
            migrated.0 == "attached"
                && migrated.1 == "legacy-operation"
                && migrated.2.as_deref() == Some("legacy-request")
                && migrated.3.is_some()
                && migrated.4.as_deref() == Some("legacy-attach-event"),
            format!("0018 did not preserve the attached receipt: {migrated:?}"),
        )?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0019_provider_submit_recovery_leases.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0019 fabricated a provider deadline for legacy remote activity",
        )?;
        let recovery_table_exists: bool = sqlx::query_scalar(
            "SELECT to_regclass(current_schema() || '.provider_submit_recoveries') IS NOT NULL",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            !recovery_table_exists,
            "failed 0019 recovery migration did not roll back atomically",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

async fn seed_legacy_attached_task(pool: &PgPool, lease: &ExecutorSubmissionLease) -> TestResult {
    let now = database_now(pool).await?;
    let observation_id = Uuid::new_v4();
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_remote_submit_intents
          (submission_id, executor_execution_id, provider_id, provider_account_id,
           submit_owner, submit_lease_epoch, idempotency_key, state,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-test', $3, $4, $5, $6, 'reserved', $7, $7)
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(ACCOUNT_ID)
    .bind(&lease.executor_owner)
    .bind(lease.executor_lease_epoch)
    .bind(format!("legacy-submit-{}", lease.submission_id.simple()))
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "UPDATE provider_remote_submit_intents SET state = 'attached', remote_operation_id = 'legacy-operation', updated_at_ms = $2 WHERE submission_id = $1",
    )
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_remote_tasks
          (submission_id, executor_execution_id, provider_id, provider_account_id,
           remote_operation_id, provider_request_id, submit_owner, submit_lease_epoch,
           state, effect_certainty, next_poll_at_ms, state_observation_id,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-test', $3, 'legacy-operation', 'legacy-request',
                $4, $5, 'provider_waiting', 'not_applicable', $6, $7, $6, $6)
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(ACCOUNT_ID)
    .bind(&lease.executor_owner)
    .bind(lease.executor_lease_epoch)
    .bind(now)
    .bind(observation_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_task_observations
          (observation_id, submission_id, executor_execution_id, event_identity,
           source, observed_state, effect_certainty, next_poll_at_ms,
           payload_hash, observed_at_ms)
        VALUES ($1, $2, $3, 'legacy-attach-event', 'submit_attach',
                'provider_waiting', 'not_applicable', $4, $5, $4)
        "#,
    )
    .bind(observation_id)
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .bind("a".repeat(64))
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_executions
        SET state = 'provider_waiting', executor_owner = NULL,
            lease_expires_at_ms = NULL, updated_at_ms = $3
        WHERE executor_execution_id = $1 AND submission_id = $2
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "UPDATE provider_submissions SET state = 'provider_waiting', updated_at_ms = $2 WHERE submission_id = $1",
    )
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)
}

struct LegacyArtifactReady {
    executor: ExecutorSubmissionLease,
    observation_id: Uuid,
    observed_at_ms: i64,
}

async fn seed_v22_artifact_ready(
    pool: &PgPool,
    worker: &str,
    identity: &str,
) -> TestResult<LegacyArtifactReady> {
    // This fixture exercises migration 0023 with the current store. Add only the
    // later task columns required by that store; migration 0023 owns all behavior
    // under test here.
    sqlx::raw_sql(
        r#"
        ALTER TABLE provider_remote_tasks
          ADD COLUMN IF NOT EXISTS provider_deadline_at_ms BIGINT,
          ADD COLUMN IF NOT EXISTS deadline_quarantine_id UUID
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let executor = seed_running_submission(pool, worker).await?;
    let store = PostgresProviderTaskStore::new(pool.clone());
    let reservation = reservation_request(&executor);
    store
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    store
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    let operation_id = format!("{identity}-operation");
    store
        .record_submit_receipt(&submit_receipt(
            &executor,
            &operation_id,
            &format!("{identity}-receipt"),
        ))
        .await
        .map_err(debug_error)?;
    store
        .attach(&attach_request(
            &executor,
            &operation_id,
            &format!("{identity}-attach"),
        ))
        .await
        .map_err(debug_error)?;
    let lease = store
        .claim_due(&claim_scope(), &format!("{identity}-poller"), 60_000)
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "v22 artifact task was not claimable".to_string())?;
    let authority_id = executor.executor_execution_id.simple().to_string();
    let authority = ProviderArtifactAuthority::new(
        "filesystem-v1".to_string(),
        format!("filesystem-v1:{identity}"),
        format!("executor-objects/{}/{}", &authority_id[..2], authority_id),
        "c".repeat(64),
        128,
        "image/png".to_string(),
    )
    .ok_or_else(|| "v22 artifact authority was invalid".to_string())?;
    store
        .publish_artifact_authority(&lease, &authority)
        .await
        .map_err(debug_error)?;

    let observation_id = Uuid::new_v4();
    let observed_at_ms = database_now(pool).await?;
    let event_identity = format!("{identity}-ready");
    let artifact_ref = format!("{identity}-object");
    let mut legacy = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_task_observations
          (observation_id, submission_id, executor_execution_id,
           event_identity, source, observed_state, artifact_ref,
           error_code, effect_certainty, next_poll_at_ms, poll_owner,
           poll_lease_epoch, payload_hash, observed_at_ms)
        VALUES ($1, $2, $3, $4, 'poll', 'artifact_ready', $5, NULL,
                'not_applicable', NULL, $6, $7, $8, $9)
        "#,
    )
    .bind(observation_id)
    .bind(executor.submission_id)
    .bind(executor.executor_execution_id)
    .bind(event_identity)
    .bind(&artifact_ref)
    .bind(&lease.poll_owner)
    .bind(lease.poll_lease_epoch)
    .bind("d".repeat(64))
    .bind(observed_at_ms)
    .execute(&mut *legacy)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_remote_tasks
        SET state = 'artifact_ready', artifact_ref = $2,
            next_poll_at_ms = NULL, poll_owner = NULL,
            poll_lease_expires_at_ms = NULL, poll_claimed_at_ms = NULL,
            state_observation_id = $3, updated_at_ms = $4, terminal_at_ms = $4
        WHERE submission_id = $1
        "#,
    )
    .bind(executor.submission_id)
    .bind(artifact_ref)
    .bind(observation_id)
    .bind(observed_at_ms)
    .execute(&mut *legacy)
    .await
    .map_err(debug_error)?;
    legacy.commit().await.map_err(debug_error)?;
    Ok(LegacyArtifactReady {
        executor,
        observation_id,
        observed_at_ms,
    })
}

fn reservation_request(lease: &ExecutorSubmissionLease) -> RemoteTaskSubmitReservation {
    RemoteTaskSubmitReservation {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner.clone(),
        executor_lease_epoch: lease.executor_lease_epoch,
        idempotency_key: format!("provider-submit-{}", lease.submission_id.simple()),
        provider_timeout_ms: 60_000,
    }
}

fn submit_failure(
    lease: &ExecutorSubmissionLease,
    kind: ProviderSubmitFailureKind,
    event_identity: &str,
    error_code: &str,
) -> RemoteTaskSubmitFailure {
    RemoteTaskSubmitFailure {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner.clone(),
        executor_lease_epoch: lease.executor_lease_epoch,
        kind,
        event_identity: event_identity.to_string(),
        error_code: error_code.to_string(),
        recovery_fence: None,
    }
}

fn submit_receipt(
    lease: &ExecutorSubmissionLease,
    operation: &str,
    event: &str,
) -> RemoteTaskSubmitReceipt {
    RemoteTaskSubmitReceipt {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner.clone(),
        executor_lease_epoch: lease.executor_lease_epoch,
        remote_operation_id: operation.to_string(),
        provider_request_id: Some(format!("request-{operation}")),
        event_identity: event.to_string(),
    }
}

fn attach_request(
    lease: &ExecutorSubmissionLease,
    operation: &str,
    event: &str,
) -> RemoteTaskAttach {
    RemoteTaskAttach {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner.clone(),
        executor_lease_epoch: lease.executor_lease_epoch,
        remote_operation_id: operation.to_string(),
        provider_request_id: Some(format!("request-{operation}")),
        event_identity: event.to_string(),
        poll_after_ms: 0,
        recovery_fence: None,
    }
}

fn claim_scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        provider_id: "provider-test".to_string(),
        provider_account_id: ACCOUNT_ID,
    }
}

async fn seed_attached_remote_task(
    pool: &PgPool,
    store: &PostgresProviderTaskStore,
    worker: &str,
    identity: &str,
    provider_timeout_ms: i64,
    poll_after_ms: i64,
) -> TestResult<ExecutorSubmissionLease> {
    let lease = seed_running_submission_with_lease(pool, worker, 5_000).await?;
    let mut reservation = reservation_request(&lease);
    reservation.provider_timeout_ms = provider_timeout_ms;
    store
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    store
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    let operation = format!("{identity}-operation");
    store
        .record_submit_receipt(&submit_receipt(
            &lease,
            &operation,
            &format!("{identity}-receipt"),
        ))
        .await
        .map_err(debug_error)?;
    let mut attach = attach_request(&lease, &operation, &format!("{identity}-attach"));
    attach.poll_after_ms = poll_after_ms;
    store.attach(&attach).await.map_err(debug_error)?;
    Ok(lease)
}

async fn seed_v24_remote_task(
    pool: &PgPool,
    worker: &str,
    identity: &str,
    provider_timeout_ms: i64,
    leave_claimed: bool,
) -> TestResult<(ExecutorSubmissionLease, i64)> {
    sqlx::raw_sql(
        r#"
        ALTER TABLE provider_remote_tasks
          ADD COLUMN provider_deadline_at_ms BIGINT,
          ADD COLUMN deadline_quarantine_id UUID
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let store = PostgresProviderTaskStore::new(pool.clone());
    let executor =
        seed_attached_remote_task(pool, &store, worker, identity, provider_timeout_ms, 0).await?;
    let deadline: i64 = sqlx::query_scalar(
        "SELECT provider_deadline_at_ms FROM provider_submit_recoveries WHERE submission_id = $1",
    )
    .bind(executor.submission_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    if leave_claimed {
        store
            .claim_due(&claim_scope(), &format!("{identity}-poller"), 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "v24 remote task was not claimable".to_string())?;
    }
    sqlx::raw_sql(
        r#"
        ALTER TABLE provider_remote_tasks
          DROP COLUMN deadline_quarantine_id,
          DROP COLUMN provider_deadline_at_ms
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok((executor, deadline))
}

async fn assert_remote_task_deadline_migration_rolled_back(pool: &PgPool) -> TestResult {
    let residue: (i64, bool, bool) = sqlx::query_as(
        r#"
        SELECT
          (SELECT COUNT(*) FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'provider_remote_tasks'
             AND column_name IN (
                'provider_deadline_at_ms', 'deadline_quarantine_id'
             )),
          to_regclass('provider_remote_task_quarantines') IS NOT NULL,
          EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid = 'provider_remote_tasks'::regclass
              AND tgname = 'provider_remote_task_update_guard'
              AND NOT tgisinternal
          )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        residue == (0, false, true),
        format!("failed 0025 migration left schema residue: {residue:?}"),
    )
}

fn artifact_authority(
    lease: &ExecutorSubmissionLease,
    identity: &str,
) -> TestResult<ProviderArtifactAuthority> {
    let authority_id = lease.executor_execution_id.simple().to_string();
    ProviderArtifactAuthority::new(
        "filesystem-v1".to_string(),
        format!("filesystem-v1:{identity}"),
        format!("executor-objects/{}/{}", &authority_id[..2], authority_id),
        "a".repeat(64),
        128,
        "image/png".to_string(),
    )
    .ok_or_else(|| "valid provider artifact authority was rejected".to_string())
}

async fn seed_deadline_quarantine(
    pool: &PgPool,
    store: &PostgresProviderTaskStore,
    worker: &str,
) -> TestResult<ExecutorSubmissionLease> {
    let lease = seed_running_submission_with_lease(pool, worker, 5_000).await?;
    let mut reservation = reservation_request(&lease);
    reservation.provider_timeout_ms = 40;
    store
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    store
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    store
        .record_submit_failure(&submit_failure(
            &lease,
            ProviderSubmitFailureKind::OutcomeUnknown,
            &format!("{worker}-ambiguous"),
            "provider_submit_ambiguous",
        ))
        .await
        .map_err(debug_error)?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    let resolved = store
        .resolve_due_submit_deadline(&claim_scope())
        .await
        .map_err(debug_error)?
        .ok_or_else(|| format!("{worker} deadline was not resolvable"))?;
    require(
        resolved.submission_id == lease.submission_id
            && resolved.state == ProviderSubmitIntentState::DeadlineQuarantined,
        format!("{worker} resolved the wrong deadline"),
    )?;
    Ok(lease)
}

async fn force_deadline_quarantine_v21(
    pool: &PgPool,
    lease: &ExecutorSubmissionLease,
) -> TestResult {
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_remote_submit_intents
        SET state = 'deadline_quarantined', updated_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2
          AND state IN ('sending', 'outcome_unknown', 'operation_known')
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_submit_recoveries
        SET state = 'closed', next_recovery_at_ms = NULL,
            recovery_owner = NULL, recovery_lease_expires_at_ms = NULL,
            recovery_claimed_at_ms = NULL,
            updated_at_ms = $3, closed_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2
          AND state = 'active' AND provider_deadline_at_ms <= $3
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_resolution_decisions
          (decision_id, executor_execution_id, submission_id, source,
           observation_id, provider_task_observation_id, provider_submit_intent_id,
           resolved_state, result_manifest_id, error_code, decided_at_ms)
        VALUES ($1, $1, $2, 'remote_submit_deadline', NULL, NULL, $2,
                'uncertain', NULL, 'provider_submit_deadline', $3)
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_executions
        SET state = 'uncertain', executor_owner = NULL,
            lease_expires_at_ms = NULL, resolution_decision_id = $1,
            finished_at_ms = $3, updated_at_ms = $3,
            error_code = 'provider_submit_deadline'
        WHERE executor_execution_id = $1 AND submission_id = $2
          AND state = 'running'
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_submissions
        SET state = 'uncertain', resolution_decision_id = $1,
            finished_at_ms = $3, updated_at_ms = $3,
            error_code = 'provider_submit_deadline'
        WHERE executor_execution_id = $1 AND submission_id = $2
          AND state = 'running'
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)
}

async fn seed_running_submission(
    pool: &PgPool,
    worker: &str,
) -> TestResult<ExecutorSubmissionLease> {
    seed_running_submission_with_lease(pool, worker, 60_000).await
}

async fn seed_running_submission_with_lease(
    pool: &PgPool,
    worker: &str,
    lease_ms: i64,
) -> TestResult<ExecutorSubmissionLease> {
    let work = seed_work_lease(pool, worker).await?;
    let store = PostgresExecutorSubmissionStore::new(pool.clone());
    let prepared = store
        .prepare_and_handoff(&work, PROFILE_ID)
        .await
        .map_err(debug_error)?;
    require(prepared.len() == 1, "expected one provider submission")?;
    let lease = store
        .claim_prepared(
            &ExecutorClaimScope {
                execution_profile_id: PROFILE_ID,
                provider_id: "provider-test".to_string(),
                command_schema: "provider-command-v1".to_string(),
                adapter_revision: "provider-test-adapter-v1".to_string(),
            },
            &format!("executor-{worker}"),
            lease_ms,
        )
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "prepared submission was not claimable".to_string())?;
    store.start(&lease).await.map_err(debug_error)?;
    Ok(lease)
}

async fn seed_work_lease(pool: &PgPool, worker: &str) -> TestResult<WorkLease> {
    let job_id = Uuid::new_v4();
    let work_item_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let now = database_now(pool).await?;
    let request_id = format!("request-{}", Uuid::new_v4().simple());
    let command =
        json!({"schema_version": 1, "operation": "generation", "n": 1, "prompt": "remote task"});
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model, state,
           requested_units, economics_contract_version, created_at_ms, updated_at_ms)
        VALUES ($1, 'provider-task-test', $2, 'generation', 'provider-test',
                'model-test', 'reserved', 1, 2, $3, $3)
        "#,
    )
    .bind(job_id)
    .bind(&request_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO job_outputs (output_id, job_id, output_index, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 0, 'pending', $3, $3)",
    )
    .bind(Uuid::new_v4())
    .bind(job_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO admission_sessions
          (session_id, owner_token, tenant_id, project_id, api_profile, operation,
           request_id, request_hash, state, job_id, deadline_at_ms, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-task-test', 'project-test', 'openai-images-v1',
                'generation', $3, $4, 'attached', $5, $6, $7, $7)
        "#,
    )
    .bind(session_id)
    .bind(Uuid::new_v4())
    .bind(&request_id)
    .bind("d".repeat(64))
    .bind(job_id)
    .bind(now + 300_000)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO job_payloads (job_id, admission_session_id, command_schema, command_json, request_hash, created_at_ms) VALUES ($1, $2, 'provider-command-v1', $3, $4, $5)",
    )
    .bind(job_id)
    .bind(session_id)
    .bind(&command)
    .bind("d".repeat(64))
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms, lease_epoch,
           lease_owner, lease_expires_at_ms, execution_id, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'generation', 'leased', $4, 1, $3, $5, $6, $4, $4)
        "#,
    )
    .bind(work_item_id)
    .bind(job_id)
    .bind(worker)
    .bind(now)
    .bind(now + 300_000)
    .bind(execution_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts
          (attempt_id, execution_id, work_item_id, lease_epoch, worker_id,
           state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 1, $4, 'claimed', $5, $5)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(execution_id)
    .bind(work_item_id)
    .bind(worker)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(WorkLease {
        work_item_id,
        job_id,
        execution_id,
        lease_epoch: 1,
        worker_id: worker.to_string(),
        command_schema: "provider-command-v1".to_string(),
        command_json: command,
    })
}

async fn seed_execution_profile(pool: &PgPool) -> TestResult {
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query("INSERT INTO provider_credential_pools (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms) VALUES ($1, 'provider-task-pool', 'provider-test', 'enabled', $2, $2)")
        .bind(POOL_ID).bind(now).execute(&mut *tx).await.map_err(debug_error)?;
    sqlx::query("INSERT INTO provider_accounts (provider_account_id, credential_pool_id, provider_id, account_key, credential_ref, credential_revision, credential_auth_sha256, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 'provider-test', 'provider-task-account', 'test-vault.provider-task.1', 1, $3, 'enabled', $4, $4)")
        .bind(ACCOUNT_ID).bind(POOL_ID).bind("1".repeat(64)).bind(now)
        .execute(&mut *tx).await.map_err(debug_error)?;
    sqlx::query("INSERT INTO executor_resource_policies (resource_policy_id, revision, credential_pool_id, provider_account_id, provider_id, execution_class, max_concurrency, state, created_at_ms) VALUES ($1, 1, $2, $3, 'provider-test', 'remote-task', 100, 'enabled', $4)")
        .bind(POLICY_ID).bind(POOL_ID).bind(ACCOUNT_ID).bind(now)
        .execute(&mut *tx).await.map_err(debug_error)?;
    sqlx::query("INSERT INTO provider_execution_profiles (execution_profile_id, profile_key, provider_id, command_schema, adapter_revision, credential_pool_id, provider_account_id, credential_ref, credential_revision, resource_policy_id, resource_policy_revision, state, created_at_ms, updated_at_ms) VALUES ($1, 'provider-task-profile', 'provider-test', 'provider-command-v1', 'provider-test-adapter-v1', $2, $3, 'test-vault.provider-task.1', 1, $4, 1, 'enabled', $5, $5)")
        .bind(PROFILE_ID).bind(POOL_ID).bind(ACCOUNT_ID).bind(POLICY_ID).bind(now)
        .execute(&mut *tx).await.map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)
}

struct TestDatabase {
    schema: String,
    pool: PgPool,
}

impl TestDatabase {
    async fn new() -> TestResult<Option<Self>> {
        let Some(url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping PostgreSQL provider task test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_provider_tasks_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 8, &schema)
            .await
            .map_err(debug_error)?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        if let Err(error) = run_migrations(&pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return Err(format!("migration failed: {error:?}"));
        }
        seed_execution_profile(&pool).await?;
        Ok(Some(Self { schema, pool }))
    }

    async fn new_before_submit_lifecycle() -> TestResult<Option<Self>> {
        let Some(url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping PostgreSQL migration test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_provider_upgrade_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 8, &schema)
            .await
            .map_err(debug_error)?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        for migration in [
            include_str!("../migrations/0000_legacy_reconciliation.sql"),
            include_str!("../migrations/0001_usage.sql"),
            include_str!("../migrations/0002_durable_admission.sql"),
            include_str!("../migrations/0003_durable_scheduling.sql"),
            include_str!("../migrations/0004_api_key_hmac.sql"),
            include_str!("../migrations/0005_artifact_replay.sql"),
            include_str!("../migrations/0006_execution_context.sql"),
            include_str!("../migrations/0007_edit_inputs.sql"),
            include_str!("../migrations/0008_provider_submissions.sql"),
            include_str!("../migrations/0009_economic_kernel.sql"),
            include_str!("../migrations/0010_executor_artifact_authority.sql"),
            include_str!("../migrations/0011_executor_observation_resolution.sql"),
            include_str!("../migrations/0012_executor_pending_evidence_index.sql"),
            include_str!("../migrations/0013_executor_execution_profiles.sql"),
            include_str!("../migrations/0014_executor_handoff.sql"),
            include_str!("../migrations/0015_executor_terminal_reductions.sql"),
            include_str!("../migrations/0016_terminal_reduction_completion.sql"),
            include_str!("../migrations/0017_provider_remote_tasks.sql"),
        ] {
            if let Err(error) = sqlx::raw_sql(migration).execute(&pool).await {
                let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                    .execute(&pool)
                    .await;
                pool.close().await;
                return Err(format!("pre-0018 migration failed: {error}"));
            }
        }
        seed_execution_profile(&pool).await?;
        Ok(Some(Self { schema, pool }))
    }

    async fn new_before_capacity_heartbeats() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_submit_lifecycle().await? else {
            return Ok(None);
        };
        for migration in [
            include_str!("../migrations/0018_provider_submit_lifecycle.sql"),
            include_str!("../migrations/0019_provider_submit_recovery_leases.sql"),
        ] {
            if let Err(error) = sqlx::raw_sql(migration).execute(&database.pool).await {
                let cleanup = database.cleanup().await;
                return match cleanup {
                    Ok(()) => Err(format!("pre-0020 migration failed: {error}")),
                    Err(cleanup) => Err(format!(
                        "pre-0020 migration failed: {error}; cleanup failed: {cleanup}"
                    )),
                };
            }
        }
        Ok(Some(database))
    }

    async fn new_before_deadline_quarantine() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_capacity_heartbeats().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0020_provider_capacity_heartbeats.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0021 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0021 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn new_before_capacity_reconciliation() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_deadline_quarantine().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0021_provider_submit_deadline_quarantine.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0022 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0022 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn new_before_atomic_artifact_resolution() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_capacity_reconciliation().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0022_provider_capacity_reconciliation.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0023 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0023 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn new_before_replayable_recovery_commands() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_atomic_artifact_resolution().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0023_atomic_provider_artifact_resolution.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0024 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0024 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn new_before_remote_task_deadline() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_replayable_recovery_commands().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0024_replayable_provider_submit_recovery_commands.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0025 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0025 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(debug_error);
        self.pool.close().await;
        result.map(|_| ())
    }
}

async fn database_now(pool: &PgPool) -> TestResult<i64> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(debug_error)
}

async fn sleep_until_database_time(pool: &PgPool, target_ms: i64) -> TestResult {
    let now = database_now(pool).await?;
    if target_ms > now {
        tokio::time::sleep(Duration::from_millis(
            u64::try_from(target_ms - now).map_err(debug_error)?,
        ))
        .await;
    }
    Ok(())
}

async fn capacity_heartbeat(pool: &PgPool, executor_execution_id: Uuid) -> TestResult<i64> {
    sqlx::query_scalar(
        r#"
        SELECT last_heartbeat_at_ms
        FROM executor_capacity_allocations
        WHERE executor_execution_id = $1 AND state = 'held'
        "#,
    )
    .bind(executor_execution_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

fn combine(result: TestResult, cleanup: TestResult) -> TestResult {
    match (result, cleanup) {
        (Err(error), Err(cleanup)) => Err(format!("{error}; cleanup failed: {cleanup}")),
        (Err(error), _) => Err(error),
        (_, Err(error)) => Err(error),
        _ => Ok(()),
    }
}
