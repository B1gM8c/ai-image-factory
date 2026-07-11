use std::{collections::HashSet, env, sync::Arc, time::Duration};

use gpt_image_2_gateway::database::{connect_test_pool_with_search_path, run_migrations};
use gpt_image_2_gateway::{
    admission::WorkLease,
    executor::{
        ExecutorClaimScope, ExecutorResultManifest, ExecutorSubmissionError,
        ExecutorSubmissionLease, ExecutorSubmissionOutcome, ExecutorSubmissionStore,
        PostgresExecutorSubmissionStore,
    },
};
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn concurrent_prepare_returns_one_stable_identity_set() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "prepare-worker", 3).await?;
        let store = Arc::new(PostgresExecutorSubmissionStore::new(database.pool.clone()));
        let tasks = (0..24)
            .map(|_| {
                let store = store.clone();
                let lease = lease.clone();
                tokio::spawn(async move { store.prepare_for_lease(&lease).await })
            })
            .collect::<Vec<_>>();

        let mut expected = None;
        for task in tasks {
            let prepared = task
                .await
                .map_err(|error| format!("prepare task failed: {error}"))?
                .map_err(|error| format!("prepare failed: {error:?}"))?;
            require(prepared.len() == 3, "prepare did not create every output")?;
            require(
                prepared.iter().all(|item| {
                    item.executor_execution_id != lease.execution_id
                        && item.command_hash.len() == 64
                        && item
                            .command_hash
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
                }),
                "executor identity or canonical command hash is invalid",
            )?;
            if let Some(expected) = &expected {
                require(
                    &prepared == expected,
                    "concurrent prepare changed stable IDs",
                )?;
            } else {
                expected = Some(prepared);
            }
        }

        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM job_outputs),
                   (SELECT COUNT(*) FROM provider_submissions),
                   (SELECT COUNT(*) FROM executor_executions),
                   (SELECT COUNT(*) FROM provider_submission_attachments)
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("identity count query failed: {error}"))?;
        require(
            counts == (3, 3, 3, 3),
            format!("unexpected counts: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn prepare_binds_the_persisted_command_and_output_count() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "command-worker", 2).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());

        let mut forged = lease.clone();
        forged.command_json["prompt"] = json!("forged command");
        require(
            store.prepare_for_lease(&forged).await == Err(ExecutorSubmissionError::Conflict),
            "caller-supplied command replaced the durable payload",
        )?;

        sqlx::query("UPDATE jobs SET requested_units = 3 WHERE job_id = $1")
            .bind(lease.job_id)
            .execute(&database.pool)
            .await
            .map_err(|error| format!("requested unit tamper failed: {error}"))?;
        require(
            store.prepare_for_lease(&lease).await == Err(ExecutorSubmissionError::Conflict),
            "quota units and durable command output count diverged",
        )?;

        let oversized = seed_lease(&database.pool, "oversized-worker", 11).await?;
        require(
            store.prepare_for_lease(&oversized).await == Err(ExecutorSubmissionError::InvalidInput),
            "executor accepted an image output count outside the API contract",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn unstarted_worker_requeue_keeps_provider_identities_and_appends_attachment() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let original = seed_lease(&database.pool, "original-worker", 2).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let first = store
            .prepare_for_lease(&original)
            .await
            .map_err(|error| format!("original prepare failed: {error:?}"))?;

        let replacement =
            requeue_and_reclaim(&database.pool, &original, "replacement-worker").await?;
        let replay = store
            .prepare_for_lease(&replacement)
            .await
            .map_err(|error| format!("replacement prepare failed: {error:?}"))?;

        require(
            first == replay,
            "worker requeue changed provider identities",
        )?;
        require(
            replacement.execution_id != original.execution_id
                && replay
                    .iter()
                    .all(|item| item.executor_execution_id != replacement.execution_id),
            "attempt and executor execution identities were conflated",
        )?;
        let attachment_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_submission_attachments WHERE work_item_id = $1",
        )
        .bind(original.work_item_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("attachment count failed: {error}"))?;
        require(
            attachment_count == 4,
            "attempt attachment history was not retained",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn claim_requires_live_running_work_and_has_one_winner_per_submission() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "claim-worker", 4).await?;
        let store = Arc::new(PostgresExecutorSubmissionStore::new(database.pool.clone()));
        store
            .prepare_for_lease(&lease)
            .await
            .map_err(|error| format!("prepare failed: {error:?}"))?;
        require(
            store
                .claim_prepared(&claim_scope(), "too-early", 60_000)
                .await
                .map_err(|error| format!("early claim failed: {error:?}"))?
                .is_none(),
            "leased work granted provider launch authority",
        )?;
        activate_work(&database.pool, &lease).await?;
        expire_worker_lease(&database.pool, &lease).await?;
        require(
            store
                .claim_prepared(&claim_scope(), "expired-worker", 60_000)
                .await
                .map_err(|error| format!("expired claim failed: {error:?}"))?
                .is_none(),
            "expired worker lease granted provider launch authority",
        )?;
        extend_worker_lease(&database.pool, &lease).await?;

        let tasks = (0..20)
            .map(|index| {
                let store = store.clone();
                tokio::spawn(async move {
                    store
                        .claim_prepared(&claim_scope(), &format!("executor-{index}"), 60_000)
                        .await
                })
            })
            .collect::<Vec<_>>();
        let mut claims = Vec::new();
        for task in tasks {
            if let Some(claim) = task
                .await
                .map_err(|error| format!("claim task failed: {error}"))?
                .map_err(|error| format!("claim failed: {error:?}"))?
            {
                claims.push(claim);
            }
        }
        require(
            claims.len() == 4,
            format!("expected four winners: {claims:?}"),
        )?;
        let unique = claims
            .iter()
            .map(|claim| claim.submission_id)
            .collect::<HashSet<_>>();
        require(
            unique.len() == 4,
            "one submission had multiple claim winners",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn stale_executor_lease_is_fenced_after_unstarted_reclaim() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "fence-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&lease).await.map_err(debug_error)?;
        activate_work(&database.pool, &lease).await?;
        let stale = claim_required(&store, "executor-old").await?;
        expire_executor_lease(&database.pool, stale.submission_id).await?;
        let current = claim_required(&store, "executor-new").await?;

        require(
            stale.submission_id == current.submission_id,
            "submission ID changed",
        )?;
        require(
            stale.executor_execution_id == current.executor_execution_id,
            "executor execution ID changed",
        )?;
        require(
            current.executor_lease_epoch == stale.executor_lease_epoch + 1,
            "executor lease epoch did not advance",
        )?;
        require(
            store.start(&stale).await == Err(ExecutorSubmissionError::StaleLease),
            "stale start was accepted",
        )?;
        require(
            store.heartbeat(&stale, 60_000).await == Err(ExecutorSubmissionError::StaleLease),
            "stale heartbeat was accepted",
        )?;
        store.start(&current).await.map_err(debug_error)
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn record_outcome_persists_evidence_without_settling_customer_output() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "outcome-worker", 3).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&lease).await.map_err(debug_error)?;
        activate_work(&database.pool, &lease).await?;
        let mut claims = Vec::new();
        for _ in 0..3 {
            let claim = claim_required(&store, "outcome-executor").await?;
            store.start(&claim).await.map_err(debug_error)?;
            claims.push(claim);
        }

        deactivate_work(&database.pool, &lease, "uncertain").await?;
        let outcomes = [
            ExecutorSubmissionOutcome::Succeeded(result_manifest(&claims[0])),
            ExecutorSubmissionOutcome::Failed {
                error_code: "provider_rejected".to_string(),
            },
            ExecutorSubmissionOutcome::Uncertain {
                error_code: "runner_evidence_lost".to_string(),
            },
        ];
        for (claim, outcome) in claims.iter().zip(&outcomes) {
            store
                .record_outcome(claim, outcome)
                .await
                .map_err(debug_error)?;
            require(
                store.record_outcome(claim, outcome).await.is_ok(),
                "identical terminal outcome did not recover a lost COMMIT response",
            )?;
        }
        require(
            store
                .record_outcome(
                    &claims[0],
                    &ExecutorSubmissionOutcome::Failed {
                        error_code: "conflicting_result".to_string(),
                    },
                )
                .await
                == Err(ExecutorSubmissionError::Conflict),
            "conflicting terminal outcome was accepted",
        )?;

        type OutcomeRow = (String, String, String, Option<String>, Option<Uuid>);
        let rows: Vec<OutcomeRow> = sqlx::query_as(
            r#"
            SELECT s.state, e.state, o.state, s.error_code, s.result_manifest_id
            FROM provider_submissions s
            JOIN executor_executions e
              ON e.executor_execution_id = s.executor_execution_id
             AND e.submission_id = s.submission_id
            JOIN job_outputs o ON o.output_id = s.output_id
            ORDER BY o.output_index
            "#,
        )
        .fetch_all(&database.pool)
        .await
        .map_err(|error| format!("outcome query failed: {error}"))?;
        require(
            rows[0].0 == "succeeded"
                && rows[0].1 == "succeeded"
                && rows[0].2 == "pending"
                && rows[0].3.is_none()
                && rows[0].4.is_some(),
            format!("success evidence bypassed output settlement: {rows:?}"),
        )?;
        require(
            rows[1].0 == "failed"
                && rows[1].1 == "failed"
                && rows[1].2 == "pending"
                && rows[1].3.as_deref() == Some("provider_rejected")
                && rows[1].4.is_none(),
            format!("failed evidence diverged: {rows:?}"),
        )?;
        require(
            rows[2].0 == "uncertain"
                && rows[2].1 == "uncertain"
                && rows[2].2 == "pending"
                && rows[2].3.as_deref() == Some("runner_evidence_lost")
                && rows[2].4.is_none(),
            format!("uncertain evidence diverged: {rows:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn expired_unstarted_execution_is_canceled_after_work_becomes_terminal() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "abandoned-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&lease).await.map_err(debug_error)?;
        activate_work(&database.pool, &lease).await?;
        let claim = claim_required(&store, "abandoned-executor").await?;
        deactivate_work(&database.pool, &lease, "uncertain").await?;
        expire_executor_lease(&database.pool, claim.submission_id).await?;

        require(
            store.reconcile_expired(100).await.map_err(debug_error)? == 1,
            "abandoned executor lease was not reconciled",
        )?;
        let states: (String, String, String, Option<String>) = sqlx::query_as(
            r#"
            SELECT s.state, e.state, o.state, s.error_code
            FROM provider_submissions s
            JOIN executor_executions e USING (executor_execution_id)
            JOIN job_outputs o ON o.output_id = s.output_id
            WHERE s.submission_id = $1
            "#,
        )
        .bind(claim.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            states
                == (
                    "canceled".into(),
                    "canceled".into(),
                    "pending".into(),
                    Some("executor_start_abandoned".into()),
                ),
            format!("unexpected abandoned states: {states:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn expired_running_execution_reconciles_to_uncertain_without_retry() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "reconcile-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&lease).await.map_err(debug_error)?;
        activate_work(&database.pool, &lease).await?;
        let claim = claim_required(&store, "reconcile-executor").await?;
        store.start(&claim).await.map_err(debug_error)?;
        expire_executor_lease(&database.pool, claim.submission_id).await?;

        require(
            store
                .claim_prepared(&claim_scope(), "challenger", 60_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "running execution was reclaimed",
        )?;
        require(
            store.reconcile_expired(100).await.map_err(debug_error)? == 1,
            "expired running execution was not reconciled",
        )?;
        let states: (String, String, String, Option<String>) = sqlx::query_as(
            r#"
            SELECT s.state, e.state, o.state, s.error_code
            FROM provider_submissions s
            JOIN executor_executions e USING (executor_execution_id)
            JOIN job_outputs o ON o.output_id = s.output_id
            WHERE s.submission_id = $1
            "#,
        )
        .bind(claim.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("reconciled state query failed: {error}"))?;
        require(
            states
                == (
                    "uncertain".into(),
                    "uncertain".into(),
                    "pending".into(),
                    Some("executor_lease_expired".into()),
                ),
            format!("unexpected reconciled states: {states:?}"),
        )?;
        require(
            store
                .record_outcome(
                    &claim,
                    &ExecutorSubmissionOutcome::Failed {
                        error_code: "late_result".to_string(),
                    },
                )
                .await
                == Err(ExecutorSubmissionError::Conflict),
            "late outcome overwrote uncertain evidence",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn executor_boundary_rejects_unbounded_identity_and_lease_inputs() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "validation-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&lease).await.map_err(debug_error)?;
        activate_work(&database.pool, &lease).await?;
        require(
            store
                .claim_prepared(&claim_scope(), "owner with spaces", 60_000)
                .await
                == Err(ExecutorSubmissionError::InvalidInput),
            "unsafe executor owner was accepted",
        )?;
        let provider_rewrite =
            sqlx::query("UPDATE jobs SET provider_id = 'other-provider' WHERE job_id = $1")
                .bind(lease.job_id)
                .execute(&database.pool)
                .await;
        require(
            provider_rewrite.is_err(),
            "durable provider identity was mutable after submission preparation",
        )?;
        let wrong_scope = ExecutorClaimScope {
            provider_id: "other-provider".to_string(),
            command_schema: "provider-command-v1".to_string(),
        };
        require(
            store
                .claim_prepared(&wrong_scope, "executor", 60_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "executor claimed a submission outside its provider scope",
        )?;
        require(
            store
                .claim_prepared(&claim_scope(), "executor", i64::MAX)
                .await
                == Err(ExecutorSubmissionError::InvalidInput),
            "unbounded executor lease was accepted",
        )?;
        let claim = claim_required(&store, "executor").await?;
        store.start(&claim).await.map_err(debug_error)?;
        require(
            store
                .record_outcome(
                    &claim,
                    &ExecutorSubmissionOutcome::Failed {
                        error_code: "raw provider error with spaces".to_string(),
                    },
                )
                .await
                == Err(ExecutorSubmissionError::InvalidInput),
            "unbounded provider error detail was accepted",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn prepare_uses_job_then_work_lock_order_without_deadlock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "lock-order-worker", 1).await?;
        let mut blocker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT 1 FROM jobs WHERE job_id = $1 FOR UPDATE")
            .bind(lease.job_id)
            .execute(&mut *blocker)
            .await
            .map_err(debug_error)?;

        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepare_lease = lease.clone();
        let prepare = tokio::spawn(async move { store.prepare_for_lease(&prepare_lease).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::time::timeout(
            Duration::from_secs(1),
            sqlx::query("SELECT 1 FROM work_items WHERE work_item_id = $1 FOR UPDATE")
                .bind(lease.work_item_id)
                .execute(&mut *blocker),
        )
        .await
        .map_err(|_| "prepare locked work before the job and formed a deadlock edge".to_string())?
        .map_err(debug_error)?;
        blocker.commit().await.map_err(debug_error)?;
        prepare.await.map_err(debug_error)?.map_err(debug_error)?;
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn lock_wait_cannot_resurrect_an_expired_executor_lease() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "deadline-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&lease).await.map_err(debug_error)?;
        activate_work(&database.pool, &lease).await?;
        let claim = claim_required(&store, "deadline-executor").await?;
        store.start(&claim).await.map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET lease_expires_at_ms =
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 150
            WHERE executor_execution_id = $1
            "#,
        )
        .bind(claim.executor_execution_id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        let mut blocker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            "SELECT 1 FROM executor_executions WHERE executor_execution_id = $1 FOR UPDATE",
        )
        .bind(claim.executor_execution_id)
        .execute(&mut *blocker)
        .await
        .map_err(debug_error)?;
        let store_for_outcome = store.clone();
        let claim_for_outcome = claim.clone();
        let outcome = tokio::spawn(async move {
            store_for_outcome
                .record_outcome(
                    &claim_for_outcome,
                    &ExecutorSubmissionOutcome::Failed {
                        error_code: "provider_rejected".to_string(),
                    },
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(250)).await;
        blocker.commit().await.map_err(debug_error)?;
        require(
            outcome.await.map_err(debug_error)? == Err(ExecutorSubmissionError::StaleLease),
            "lock wait used a timestamp captured before the executor lease expired",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn attachment_foreign_keys_reject_cross_work_audit_history() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let first = seed_lease(&database.pool, "attachment-a", 1).await?;
        let second = seed_lease(&database.pool, "attachment-b", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = store.prepare_for_lease(&first).await.map_err(debug_error)?;
        let result = sqlx::query(
            r#"
            INSERT INTO provider_submission_attachments
              (submission_id, job_id, attempt_execution_id, work_item_id, lease_epoch,
               attached_at_ms)
            VALUES ($1, $2, $3, $4, $5,
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT)
            "#,
        )
        .bind(prepared[0].submission_id)
        .bind(second.job_id)
        .bind(second.execution_id)
        .bind(second.work_item_id)
        .bind(second.lease_epoch)
        .execute(&database.pool)
        .await;
        require(
            result.is_err(),
            "cross-work submission attachment passed database FKs",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

fn claim_scope() -> ExecutorClaimScope {
    ExecutorClaimScope {
        provider_id: "provider-test".to_string(),
        command_schema: "provider-command-v1".to_string(),
    }
}

fn result_manifest(claim: &ExecutorSubmissionLease) -> ExecutorResultManifest {
    ExecutorResultManifest {
        manifest_id: Uuid::new_v4(),
        storage_backend: "filesystem-v1".to_string(),
        object_key: format!("executor/{}/result", claim.executor_execution_id),
        sha256_hex: "a".repeat(64),
        byte_size: 128,
        media_type: "image/png".to_string(),
    }
}

async fn claim_required(
    store: &PostgresExecutorSubmissionStore,
    owner: &str,
) -> TestResult<ExecutorSubmissionLease> {
    store
        .claim_prepared(&claim_scope(), owner, 60_000)
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "executor claim returned none".to_string())
}

async fn seed_lease(pool: &PgPool, worker_id: &str, requested_units: i32) -> TestResult<WorkLease> {
    let job_id = Uuid::new_v4();
    let work_item_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let admission_session_id = Uuid::new_v4();
    let command_json = json!({
        "schema_version": 1,
        "operation": "generation",
        "n": requested_units,
        "prompt": "stable provider identity"
    });
    let now = database_now(pool).await?;
    let request_id = format!("request-{}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model, state,
           requested_units, created_at_ms, updated_at_ms)
        VALUES ($1, 'executor-test', $2, 'generation', 'provider-test', 'model-test',
                'reserved', $3, $4, $4)
        "#,
    )
    .bind(job_id)
    .bind(&request_id)
    .bind(requested_units)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("job seed failed: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO admission_sessions
          (session_id, owner_token, tenant_id, project_id, api_profile, operation,
           request_id, request_hash, state, job_id, deadline_at_ms, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'executor-test', 'project-test', 'openai-images-v1', 'generation',
                $3, $4, 'attached', $5, $6, $7, $7)
        "#,
    )
    .bind(admission_session_id)
    .bind(Uuid::new_v4())
    .bind(&request_id)
    .bind("c".repeat(64))
    .bind(job_id)
    .bind(now + 300_000)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("admission seed failed: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO job_payloads
          (job_id, admission_session_id, command_schema, command_json, request_hash, created_at_ms)
        VALUES ($1, $2, 'provider-command-v1', $3, $4, $5)
        "#,
    )
    .bind(job_id)
    .bind(admission_session_id)
    .bind(&command_json)
    .bind("c".repeat(64))
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("payload seed failed: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms, lease_epoch,
           lease_owner, lease_expires_at_ms, execution_id, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'generation', 'leased', $4, 7, $3, $5, $6, $4, $4)
        "#,
    )
    .bind(work_item_id)
    .bind(job_id)
    .bind(worker_id)
    .bind(now)
    .bind(now + 300_000)
    .bind(execution_id)
    .execute(pool)
    .await
    .map_err(|error| format!("work seed failed: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts
          (attempt_id, execution_id, work_item_id, lease_epoch, worker_id, state,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 7, $4, 'claimed', $5, $5)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(execution_id)
    .bind(work_item_id)
    .bind(worker_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("attempt seed failed: {error}"))?;
    Ok(WorkLease {
        work_item_id,
        job_id,
        execution_id,
        lease_epoch: 7,
        worker_id: worker_id.to_string(),
        command_schema: "provider-command-v1".to_string(),
        command_json,
    })
}

async fn activate_work(pool: &PgPool, lease: &WorkLease) -> TestResult {
    let mut tx = pool.begin().await.map_err(debug_error)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE work_items SET state = 'running'
            WHERE work_item_id = $1 AND job_id = $2 AND execution_id = $3
              AND lease_epoch = $4 AND lease_owner = $5 AND state = 'leased'
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.job_id)
        .bind(lease.execution_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?,
        "work activation",
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE job_attempts SET state = 'running'
            WHERE execution_id = $1 AND work_item_id = $2 AND lease_epoch = $3
              AND worker_id = $4 AND state = 'claimed'
            "#,
        )
        .bind(lease.execution_id)
        .bind(lease.work_item_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?,
        "attempt activation",
    )?;
    tx.commit().await.map_err(debug_error)
}

async fn deactivate_work(pool: &PgPool, lease: &WorkLease, state: &str) -> TestResult {
    require_one(
        sqlx::query(
            r#"
            UPDATE work_items
            SET state = $2, lease_owner = NULL, lease_expires_at_ms = NULL, execution_id = NULL
            WHERE work_item_id = $1
            "#,
        )
        .bind(lease.work_item_id)
        .bind(state)
        .execute(pool)
        .await
        .map_err(debug_error)?,
        "work deactivation",
    )
}

async fn requeue_and_reclaim(
    pool: &PgPool,
    lease: &WorkLease,
    replacement_worker: &str,
) -> TestResult<WorkLease> {
    let execution_id = Uuid::new_v4();
    let now = database_now(pool).await?;
    let next_epoch = lease.lease_epoch + 1;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE job_attempts
            SET state = 'failed', finished_at_ms = $3,
                error_code = 'lease_expired_before_start', updated_at_ms = $3
            WHERE execution_id = $1 AND lease_epoch = $2 AND state = 'claimed'
            "#,
        )
        .bind(lease.execution_id)
        .bind(lease.lease_epoch)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?,
        "old attempt requeue",
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE work_items
            SET state = 'leased', lease_epoch = $2, lease_owner = $3,
                lease_expires_at_ms = $4, execution_id = $5, updated_at_ms = $6
            WHERE work_item_id = $1 AND job_id = $7 AND lease_epoch = $8 AND state = 'leased'
            "#,
        )
        .bind(lease.work_item_id)
        .bind(next_epoch)
        .bind(replacement_worker)
        .bind(now + 300_000)
        .bind(execution_id)
        .bind(now)
        .bind(lease.job_id)
        .bind(lease.lease_epoch)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?,
        "work requeue",
    )?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts
          (attempt_id, execution_id, work_item_id, lease_epoch, worker_id, state,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, $5, 'claimed', $6, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(execution_id)
    .bind(lease.work_item_id)
    .bind(next_epoch)
    .bind(replacement_worker)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)?;
    Ok(WorkLease {
        work_item_id: lease.work_item_id,
        job_id: lease.job_id,
        execution_id,
        lease_epoch: next_epoch,
        worker_id: replacement_worker.to_string(),
        command_schema: lease.command_schema.clone(),
        command_json: lease.command_json.clone(),
    })
}

async fn expire_worker_lease(pool: &PgPool, lease: &WorkLease) -> TestResult {
    sqlx::query(
        r#"
        UPDATE work_items
        SET lease_expires_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT - 1
        WHERE work_item_id = $1 AND lease_epoch = $2
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.lease_epoch)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn extend_worker_lease(pool: &PgPool, lease: &WorkLease) -> TestResult {
    sqlx::query(
        r#"
        UPDATE work_items
        SET lease_expires_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 300000
        WHERE work_item_id = $1 AND lease_epoch = $2
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.lease_epoch)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn expire_executor_lease(pool: &PgPool, submission_id: Uuid) -> TestResult {
    sqlx::query(
        r#"
        UPDATE executor_executions
        SET lease_expires_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT - 1
        WHERE submission_id = $1
        "#,
    )
    .bind(submission_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn database_now(pool: &PgPool) -> TestResult<i64> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(debug_error)
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
            eprintln!("skipping PostgreSQL executor test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_executor_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 24, &schema)
            .await
            .map_err(|error| format!("test database connection failed: {error:?}"))?;
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

fn require_one(result: sqlx::postgres::PgQueryResult, operation: &str) -> TestResult {
    require(
        result.rows_affected() == 1,
        format!("{operation} changed {} rows", result.rows_affected()),
    )
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn combine(primary: TestResult, cleanup: TestResult) -> TestResult {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(format!("{primary}; cleanup also failed: {cleanup}")),
    }
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}
