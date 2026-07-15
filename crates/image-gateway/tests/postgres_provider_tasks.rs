use std::{env, time::Duration};

use gpt_image_2_gateway::{
    ExecutorClaimScope, ExecutorHandoffStore, ExecutorSubmissionLease, ExecutorSubmissionStore,
    PostgresExecutorSubmissionStore, PostgresProviderTaskStore, ProviderArtifactAuthority,
    ProviderTaskClaimScope, ProviderTaskObservation, ProviderTaskObservationOutcome,
    ProviderTaskObservationSource, ProviderTaskState, ProviderTaskStore, ProviderTaskStoreError,
    RemoteTaskAttach, RemoteTaskSubmitReservation, VerifiedCallbackWakeup,
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
            reserved_left == reserved_right && !reserved_left.attached,
            "concurrent submit reservation did not converge",
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

fn reservation_request(lease: &ExecutorSubmissionLease) -> RemoteTaskSubmitReservation {
    RemoteTaskSubmitReservation {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner.clone(),
        executor_lease_epoch: lease.executor_lease_epoch,
        idempotency_key: format!("provider-submit-{}", lease.submission_id.simple()),
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
            60_000,
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
