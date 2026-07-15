use std::{env, time::Duration};

use gpt_image_2_gateway::{
    ExecutorClaimScope, ExecutorHandoffStore, ExecutorSubmissionLease, ExecutorSubmissionOutcome,
    ExecutorSubmissionStore, PostgresExecutorSubmissionStore, PostgresProviderTaskStore,
    ProviderArtifactAuthority, ProviderSubmitFailureKind, ProviderSubmitIntentState,
    ProviderSubmitRecoveryFence, ProviderSubmitStart, ProviderTaskClaimScope,
    ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskObservationSource,
    ProviderTaskState, ProviderTaskStore, ProviderTaskStoreError, RemoteTaskAttach,
    RemoteTaskSubmitFailure, RemoteTaskSubmitReceipt, RemoteTaskSubmitReservation,
    VerifiedCallbackWakeup,
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
        let first_lease = store
            .claim_due(&scope, "poller-a", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "first task was not pollable".to_string())?;
        require(
            first_lease.context() == &first_context,
            "poll claim re-resolved the frozen provider context",
        )?;
        let first_lease = store
            .heartbeat(&first_lease, 5_000)
            .await
            .map_err(debug_error)?;
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
        let terminal = store
            .record_observation(&first_lease, &uncertain)
            .await
            .map_err(debug_error)?;
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
        let second_lease = store
            .claim_due(&scope, "poller-old", 5)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "second task was not pollable".to_string())?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let reclaimed = store
            .claim_due(&scope, "poller-new", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired task was not reclaimed".to_string())?;
        require(
            reclaimed.poll_lease_epoch == second_lease.poll_lease_epoch + 1,
            "poll lease epoch did not advance on reclaim",
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
        let reclaimed = store
            .claim_due(&scope, "poller-after-replay", 5_000)
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
        let manifest = store
            .publish_artifact_authority(&reclaimed, &authority)
            .await
            .map_err(|error| format!("publish remote artifact authority: {error:?}"))?;
        let ready = store
            .record_observation(
                &reclaimed,
                &ProviderTaskObservation {
                    event_identity: "artifact-ready-b".to_string(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::ArtifactReady {
                        artifact_ref: "durable-object-b".to_string(),
                    },
                },
            )
            .await
            .map_err(|error| format!("record remote artifact ready: {error:?}"))?;
        require(
            ready.state == ProviderTaskState::ArtifactReady,
            "verified remote artifact did not become ready",
        )?;
        store
            .resolve_artifact(second.submission_id, &manifest)
            .await
            .map_err(|error| format!("resolve remote artifact: {error:?}"))?;
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
            .claim_submit_recovery(&claim_scope(), "submit-recovery-a", 5_000)
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
                .claim_submit_recovery(&wrong_scope, "wrong-account", 2_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "recovery claim crossed the provider account boundary",
        )?;

        let scope = claim_scope();
        let (left, right) = tokio::join!(
            store.claim_submit_recovery(&scope, "recovery-a", 2_000),
            store.claim_submit_recovery(&scope, "recovery-b", 2_000),
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
        let renewed = store
            .heartbeat_submit_recovery(&first, 3_000)
            .await
            .map_err(debug_error)?;
        require(
            renewed.recovery_lease_expires_at_ms > first.recovery_lease_expires_at_ms,
            "recovery heartbeat did not advance monotonically",
        )?;
        require(
            store
                .claim_submit_recovery(&claim_scope(), "recovery-c", 2_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "live recovery lease was stolen",
        )?;

        store
            .defer_submit_recovery(&renewed, 100)
            .await
            .map_err(debug_error)?;
        require(
            store
                .claim_submit_recovery(&claim_scope(), "recovery-c", 2_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "deferred recovery became immediately claimable",
        )?;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let reclaimed = store
            .claim_submit_recovery(&claim_scope(), "recovery-c", 2_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "deferred recovery was not reclaimed".to_string())?;
        require(
            reclaimed.recovery_lease_epoch == first.recovery_lease_epoch + 1,
            "recovery reclaim did not advance the fence epoch",
        )?;

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
            .claim_submit_recovery(&claim_scope(), "recovery-rejector", 2_000)
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
        require(
            store
                .record_submit_failure(&rejection)
                .await
                .map_err(debug_error)?
                .state
                == ProviderSubmitIntentState::Rejected,
            "live recovery owner could not atomically commit confirmed rejection",
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
