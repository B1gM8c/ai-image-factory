use std::{
    collections::HashSet, env, fs, os::unix::fs::PermissionsExt, process::Stdio, sync::Arc,
    time::Duration,
};

use gpt_image_2_gateway::database::{connect_test_pool_with_search_path, run_migrations};
use gpt_image_2_gateway::{
    GenerationJob,
    admission::{GENERATION_COMMAND_SCHEMA, GenerationCommandV1, WorkLease},
    artifacts::{ExecutorArtifactPublisher, FilesystemArtifactBlobStore},
    economics::{
        EconomicReceipt, EconomicReceiptOutcome, EconomicSettlementStore,
        PostgresEconomicSettlementStore,
    },
    executor::{
        ExecutorClaimScope, ExecutorEvidenceStore, ExecutorLaunchContextStore,
        ExecutorOwnerGuardError, ExecutorResultManifest, ExecutorSubmissionError,
        ExecutorSubmissionLease, ExecutorSubmissionOutcome, ExecutorSubmissionStore,
        PostgresExecutorOwnerGuard, PostgresExecutorSubmissionStore,
    },
};
use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn real_executord_process_runs_one_output_through_durable_helper_and_artifact_authority()
-> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_codex_generation_lease(&database.pool, "executord-smoke-workerd").await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = store.prepare_for_lease(&lease).await.map_err(debug_error)?;
        require(prepared.len() == 1, "expected one prepared output")?;
        activate_work(&database.pool, &lease).await?;

        let files = ExecutordFixture::new(Duration::ZERO)?;
        let mut child = files
            .command(&database, "executord-process-smoke")?
            .spawn()
            .map_err(|error| format!("failed to spawn executord: {error}"))?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let terminal = loop {
            let row: Option<(String, String, i64, i64, i64)> = sqlx::query_as(
                r#"
                    SELECT e.state, s.state,
                           (SELECT COUNT(*) FROM executor_artifact_authorities
                            WHERE executor_execution_id = e.executor_execution_id),
                           (SELECT COUNT(*) FROM executor_runner_observations
                            WHERE executor_execution_id = e.executor_execution_id),
                           (SELECT COUNT(*) FROM executor_resolution_decisions
                            WHERE executor_execution_id = e.executor_execution_id)
                    FROM executor_executions e
                    JOIN provider_submissions s ON s.submission_id = e.submission_id
                    WHERE s.job_id = $1
                    "#,
            )
            .bind(lease.job_id)
            .fetch_optional(&database.pool)
            .await
            .map_err(debug_error)?;
            if row.as_ref().is_some_and(|row| {
                row.0 == "succeeded"
                    && row.1 == "succeeded"
                    && row.2 == 1
                    && row.3 == 1
                    && row.4 == 1
            }) {
                break row.unwrap();
            }
            if let Some(status) = child.try_wait().map_err(debug_error)? {
                return Err(format!("executord exited early with {status}; row={row:?}"));
            }
            if tokio::time::Instant::now() >= deadline {
                if let Some(pid) = child.id() {
                    unsafe {
                        libc::kill(pid as libc::pid_t, libc::SIGTERM);
                    }
                }
                let _ = tokio::time::timeout(Duration::from_secs(3), child.wait()).await;
                return Err(format!("executord result timed out; row={row:?}"));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        require(
            terminal == ("succeeded".to_string(), "succeeded".to_string(), 1, 1, 1),
            format!("unexpected executor terminal projection: {terminal:?}"),
        )?;
        let pid = child
            .id()
            .ok_or_else(|| "executord PID unavailable".to_string())?;
        if unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) } != 0 {
            return Err("failed to signal executord".to_string());
        }
        let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
            .await
            .map_err(|_| "executord did not exit after SIGTERM".to_string())?
            .map_err(debug_error)?;
        require(
            output.status.success(),
            format!(
                "executord exit failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        )?;
        require(
            fs::read_to_string(&files.invocations).map_err(debug_error)? == "1\n",
            "Codex helper launched the provider more than once",
        )?;
        let artifact_files = walk_regular_files(&files.artifact_root)?;
        require(
            artifact_files.len() == 1,
            format!("unexpected artifact file count: {artifact_files:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn restarted_executord_attaches_running_helper_without_relaunching_provider() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease =
            seed_codex_generation_lease(&database.pool, "executord-restart-workerd").await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        require(
            store
                .prepare_for_lease(&lease)
                .await
                .map_err(debug_error)?
                .len()
                == 1,
            "expected one prepared output",
        )?;
        activate_work(&database.pool, &lease).await?;
        let files = ExecutordFixture::new(Duration::from_secs(2))?;
        let owner = "executord-restart-smoke";
        let mut first = files
            .command(&database, owner)?
            .spawn()
            .map_err(debug_error)?;
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let state: Option<String> = sqlx::query_scalar(
                    r#"
                    SELECT e.state
                    FROM executor_executions e
                    JOIN provider_submissions s ON s.submission_id = e.submission_id
                    WHERE s.job_id = $1
                    "#,
                )
                .bind(lease.job_id)
                .fetch_optional(&database.pool)
                .await
                .map_err(debug_error)?;
                if state.as_deref() == Some("running")
                    && fs::read_to_string(&files.invocations).is_ok_and(|value| value == "1\n")
                {
                    break Ok::<_, String>(());
                }
                if let Some(status) = first.try_wait().map_err(debug_error)? {
                    break Err(format!("first executord exited early with {status}"));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "provider helper did not start before restart test deadline".to_string())??;
        let first_pid = first
            .id()
            .ok_or_else(|| "first executord PID unavailable".to_string())?;
        if unsafe { libc::kill(first_pid as libc::pid_t, libc::SIGKILL) } != 0 {
            return Err("failed to SIGKILL first executord".to_string());
        }
        let first_output = tokio::time::timeout(Duration::from_secs(3), first.wait_with_output())
            .await
            .map_err(|_| "first executord did not exit after SIGKILL".to_string())?
            .map_err(debug_error)?;
        require(
            !first_output.status.success(),
            "SIGKILLed executord unexpectedly exited successfully",
        )?;

        let mut second = files
            .command(&database, owner)?
            .spawn()
            .map_err(debug_error)?;
        let terminal = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let row: Option<(String, String, i64)> = sqlx::query_as(
                    r#"
                    SELECT e.state, s.state,
                           (SELECT COUNT(*) FROM executor_runner_observations
                            WHERE executor_execution_id = e.executor_execution_id)
                    FROM executor_executions e
                    JOIN provider_submissions s ON s.submission_id = e.submission_id
                    WHERE s.job_id = $1
                    "#,
                )
                .bind(lease.job_id)
                .fetch_optional(&database.pool)
                .await
                .map_err(debug_error)?;
                if row
                    .as_ref()
                    .is_some_and(|row| row.0 == "succeeded" && row.1 == "succeeded" && row.2 == 1)
                {
                    break Ok::<_, String>(row.unwrap());
                }
                if let Some(status) = second.try_wait().map_err(debug_error)? {
                    break Err(format!("second executord exited early with {status}"));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "restarted executord did not attach before deadline".to_string())??;
        require(
            terminal == ("succeeded".to_string(), "succeeded".to_string(), 1),
            format!("unexpected attached terminal state: {terminal:?}"),
        )?;
        let second_pid = second
            .id()
            .ok_or_else(|| "second executord PID unavailable".to_string())?;
        if unsafe { libc::kill(second_pid as libc::pid_t, libc::SIGTERM) } != 0 {
            return Err("failed to terminate second executord".to_string());
        }
        let second_output = tokio::time::timeout(Duration::from_secs(5), second.wait_with_output())
            .await
            .map_err(|_| "second executord did not drain after SIGTERM".to_string())?
            .map_err(debug_error)?;
        require(
            second_output.status.success(),
            format!(
                "second executord failed: {}",
                String::from_utf8_lossy(&second_output.stderr)
            ),
        )?;
        require(
            fs::read_to_string(&files.invocations).map_err(debug_error)? == "1\n",
            "restart launched the provider more than once",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn expired_execution_recovers_late_success_evidence_without_relaunching_provider()
-> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_codex_generation_lease(&database.pool, "late-evidence-workerd").await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = store.prepare_for_lease(&work).await.map_err(debug_error)?;
        require(prepared.len() == 1, "expected one prepared output")?;
        activate_work(&database.pool, &work).await?;

        let files = ExecutordFixture::new(Duration::from_millis(1_200))?;
        let owner = "late-evidence-executord";
        let mut first = files
            .command_with_lease(&database, owner, 800, 100)?
            .spawn()
            .map_err(debug_error)?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let running: Option<(String, i64)> = sqlx::query_as(
                    r#"
                    SELECT e.state, e.lease_epoch
                    FROM executor_executions e
                    JOIN provider_submissions s ON s.submission_id = e.submission_id
                    WHERE s.job_id = $1
                    "#,
                )
                .bind(work.job_id)
                .fetch_optional(&database.pool)
                .await
                .map_err(debug_error)?;
                if running.as_ref().is_some_and(|row| row.0 == "running")
                    && fs::read_to_string(&files.invocations).is_ok_and(|value| value == "1\n")
                {
                    break Ok::<_, String>(());
                }
                if let Some(status) = first.try_wait().map_err(debug_error)? {
                    break Err(format!("first executord exited early with {status}"));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "late-evidence helper did not start".to_string())??;
        let first_pid = first
            .id()
            .ok_or_else(|| "first executord PID unavailable".to_string())?;
        if unsafe { libc::kill(first_pid as libc::pid_t, libc::SIGKILL) } != 0 {
            return Err("failed to SIGKILL first executord".to_string());
        }
        let first_output = tokio::time::timeout(Duration::from_secs(3), first.wait_with_output())
            .await
            .map_err(|_| "first executord did not exit after SIGKILL".to_string())?
            .map_err(debug_error)?;
        require(
            !first_output.status.success(),
            "SIGKILLed executord unexpectedly exited successfully",
        )?;

        let scope = ExecutorClaimScope {
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
        };
        require(
            store
                .load_pending_evidence(&scope, owner)
                .await
                .map_err(debug_error)?
                .is_none(),
            "live lease was exposed as late evidence",
        )?;
        let process_terminal = files
            .runner_root
            .join(prepared[0].executor_execution_id.simple().to_string())
            .join("result.json");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !process_terminal.is_file() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "detached helper did not persist terminal evidence".to_string())?;
        let evidence = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(evidence) = store
                    .load_pending_evidence(&scope, owner)
                    .await
                    .map_err(debug_error)?
                {
                    break Ok::<_, String>(evidence);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "expired launch fence was not recoverable".to_string())??;
        require(
            evidence.executor_execution_id == prepared[0].executor_execution_id
                && evidence.submission_id == prepared[0].submission_id
                && evidence.executor_owner == owner
                && evidence.executor_lease_epoch > 0,
            "recovered evidence did not preserve the immutable launch fence",
        )?;

        let mut second = files
            .command_with_lease(&database, owner, 800, 100)?
            .spawn()
            .map_err(debug_error)?;
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let row: (i64, i64, Option<String>) = sqlx::query_as(
                    r#"
                    SELECT
                      (SELECT COUNT(*) FROM executor_artifact_authorities
                       WHERE executor_execution_id = $1),
                      (SELECT COUNT(*) FROM executor_runner_observations
                       WHERE executor_execution_id = $1),
                      (SELECT observed_state FROM executor_runner_observations
                       WHERE executor_execution_id = $1)
                    "#,
                )
                .bind(prepared[0].executor_execution_id)
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
                if row == (1, 1, Some("succeeded".to_string())) {
                    break Ok::<_, String>(());
                }
                if let Some(status) = second.try_wait().map_err(debug_error)? {
                    break Err(format!("recovery executord exited early with {status}"));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "late success evidence was not imported".to_string())??;
        require(
            store.reconcile_expired(100).await.map_err(debug_error)? == 1,
            "expired execution was not canonically reconciled",
        )?;
        let canonical: (String, String, String, i64) = sqlx::query_as(
            r#"
            SELECT e.state, s.state, d.source,
                   (SELECT COUNT(*) FROM executor_runner_observations
                    WHERE executor_execution_id = e.executor_execution_id)
            FROM executor_executions e
            JOIN provider_submissions s ON s.submission_id = e.submission_id
            JOIN executor_resolution_decisions d
              ON d.decision_id = e.resolution_decision_id
            WHERE s.job_id = $1
            "#,
        )
        .bind(work.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            canonical
                == (
                    "uncertain".to_string(),
                    "uncertain".to_string(),
                    "executor_lease_expired".to_string(),
                    1,
                ),
            format!("unexpected late evidence canonical projection: {canonical:?}"),
        )?;
        let second_pid = second
            .id()
            .ok_or_else(|| "recovery executord PID unavailable".to_string())?;
        if unsafe { libc::kill(second_pid as libc::pid_t, libc::SIGTERM) } != 0 {
            return Err("failed to terminate recovery executord".to_string());
        }
        let second_output = tokio::time::timeout(Duration::from_secs(5), second.wait_with_output())
            .await
            .map_err(|_| "recovery executord did not terminate".to_string())?
            .map_err(debug_error)?;
        require(
            second_output.status.success(),
            format!(
                "recovery executord failed: {}",
                String::from_utf8_lossy(&second_output.stderr)
            ),
        )?;
        require(
            fs::read_to_string(&files.invocations).map_err(debug_error)? == "1\n",
            "late evidence recovery relaunched the provider",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn executord_sigterm_drains_running_helper_through_database_resolution() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_codex_generation_lease(&database.pool, "executord-drain-workerd").await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        require(
            store
                .prepare_for_lease(&lease)
                .await
                .map_err(debug_error)?
                .len()
                == 1,
            "expected one prepared output",
        )?;
        activate_work(&database.pool, &lease).await?;
        let files = ExecutordFixture::new(Duration::from_secs(1))?;
        let mut child = files
            .command(&database, "executord-drain-smoke")?
            .spawn()
            .map_err(debug_error)?;
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let state: Option<String> = sqlx::query_scalar(
                    r#"
                    SELECT e.state
                    FROM executor_executions e
                    JOIN provider_submissions s ON s.submission_id = e.submission_id
                    WHERE s.job_id = $1
                    "#,
                )
                .bind(lease.job_id)
                .fetch_optional(&database.pool)
                .await
                .map_err(debug_error)?;
                if state.as_deref() == Some("running")
                    && fs::read_to_string(&files.invocations).is_ok_and(|value| value == "1\n")
                {
                    break Ok::<_, String>(());
                }
                if let Some(status) = child.try_wait().map_err(debug_error)? {
                    break Err(format!(
                        "executord exited before drain signal with {status}"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "provider helper did not enter running state".to_string())??;
        let pid = child
            .id()
            .ok_or_else(|| "executord PID unavailable".to_string())?;
        if unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) } != 0 {
            return Err("failed to SIGTERM executord".to_string());
        }
        let output = tokio::time::timeout(Duration::from_secs(8), child.wait_with_output())
            .await
            .map_err(|_| "executord did not finish drain".to_string())?
            .map_err(debug_error)?;
        require(
            output.status.success(),
            format!(
                "executord drain failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        )?;
        let terminal: (String, String, i64, i64) = sqlx::query_as(
            r#"
            SELECT e.state, s.state,
                   (SELECT COUNT(*) FROM executor_artifact_authorities
                    WHERE executor_execution_id = e.executor_execution_id),
                   (SELECT COUNT(*) FROM executor_resolution_decisions
                    WHERE executor_execution_id = e.executor_execution_id)
            FROM executor_executions e
            JOIN provider_submissions s ON s.submission_id = e.submission_id
            WHERE s.job_id = $1
            "#,
        )
        .bind(lease.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            terminal == ("succeeded".to_string(), "succeeded".to_string(), 1, 1),
            format!("SIGTERM drain left incomplete state: {terminal:?}"),
        )?;
        require(
            fs::read_to_string(&files.invocations).map_err(debug_error)? == "1\n",
            "SIGTERM drain relaunched the provider",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn executor_owner_guard_allows_only_one_live_owner_scope_session() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let scope = ExecutorClaimScope {
            provider_id: "openai-codex".to_string(),
            command_schema: "openai.images.generation.v1".to_string(),
        };
        let mut first = PostgresExecutorOwnerGuard::acquire(
            &database.pool,
            "owner-guard-test",
            &scope,
            Duration::from_secs(2),
        )
        .await
        .map_err(debug_error)?;
        first.verify().await.map_err(debug_error)?;
        let second = PostgresExecutorOwnerGuard::acquire(
            &database.pool,
            "owner-guard-test",
            &scope,
            Duration::from_secs(2),
        )
        .await;
        require(
            matches!(second, Err(ExecutorOwnerGuardError::AlreadyActive)),
            "second owner guard was not rejected",
        )?;
        drop(first);
        let reacquired = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match PostgresExecutorOwnerGuard::acquire(
                    &database.pool,
                    "owner-guard-test",
                    &scope,
                    Duration::from_secs(1),
                )
                .await
                {
                    Ok(guard) => break Ok(guard),
                    Err(ExecutorOwnerGuardError::AlreadyActive) => {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Err(error) => break Err(error),
                }
            }
        })
        .await
        .map_err(|_| "owner guard was not released after connection close".to_string())?
        .map_err(debug_error)?;
        drop(reacquired);
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn executor_owner_guard_fails_closed_after_its_database_session_is_terminated() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let scope = ExecutorClaimScope {
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
        };
        let mut guard = PostgresExecutorOwnerGuard::acquire(
            &database.pool,
            "owner-guard-session-loss",
            &scope,
            Duration::from_secs(2),
        )
        .await
        .map_err(debug_error)?;
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
            .bind(guard.backend_pid())
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        require(terminated, "owner guard backend was not terminated")?;
        require(
            matches!(
                guard.verify().await,
                Err(ExecutorOwnerGuardError::Unavailable)
            ),
            "owner guard did not fail closed after session loss",
        )?;
        drop(guard);
        let mut reacquired = PostgresExecutorOwnerGuard::acquire(
            &database.pool,
            "owner-guard-session-loss",
            &scope,
            Duration::from_secs(2),
        )
        .await
        .map_err(debug_error)?;
        reacquired.verify().await.map_err(debug_error)
    }
    .await;
    combine(result, database.cleanup().await)
}

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
async fn prepare_attaches_submissions_to_admission_owned_outputs() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "admission-output-worker", 2).await?;
        let output_ids = [Uuid::new_v4(), Uuid::new_v4()];
        let now = database_now(&database.pool).await?;
        let mut tx = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("UPDATE jobs SET economics_contract_version = 2 WHERE job_id = $1")
            .bind(lease.job_id)
            .execute(&mut *tx)
            .await
            .map_err(debug_error)?;
        for (output_index, output_id) in output_ids.into_iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO job_outputs
                  (output_id, job_id, output_index, state, created_at_ms, updated_at_ms)
                VALUES ($1, $2, $3, 'pending', $4, $4)
                "#,
            )
            .bind(output_id)
            .bind(lease.job_id)
            .bind(output_index as i32)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(debug_error)?;
        }
        tx.commit().await.map_err(debug_error)?;

        let prepared = PostgresExecutorSubmissionStore::new(database.pool.clone())
            .prepare_for_lease(&lease)
            .await
            .map_err(debug_error)?;
        require(
            prepared
                .iter()
                .map(|item| item.output_id)
                .collect::<Vec<_>>()
                == output_ids.to_vec(),
            "executor replaced admission-owned output identities",
        )?;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_outputs WHERE job_id = $1")
            .bind(lease.job_id)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        require(count == 2, "executor created duplicate customer outputs")
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn successful_provider_output_is_rated_exactly_once_from_frozen_quote() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "economic-worker", 1).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (artifacts, _artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_for_lease(&work)
            .await
            .map_err(debug_error)?;
        seed_price_hold(&database.pool, &prepared[0], 0).await?;
        activate_work(&database.pool, &work).await?;
        let lease = claim_required(&executor, "economic-executor").await?;
        executor.start(&lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&artifacts, &lease).await?;
        executor
            .record_outcome(&lease, &ExecutorSubmissionOutcome::Succeeded(manifest))
            .await
            .map_err(debug_error)?;

        let economics = PostgresEconomicSettlementStore::new(database.pool.clone());
        let receipt = EconomicReceipt::new(
            lease.submission_id,
            EconomicReceiptOutcome::Succeeded,
            "provider.receipt.v1",
            json!({"provider_request_id": "provider-1"}),
        )
        .map_err(debug_error)?;
        let first = economics.settle(&receipt).await.map_err(debug_error)?;
        let replay = economics.settle(&receipt).await.map_err(debug_error)?;
        require(
            first == replay,
            "economic replay changed the settled identity",
        )?;

        let state: (String, String, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT o.state, h.state,
                   (SELECT COUNT(*) FROM provider_receipts WHERE submission_id = $1),
                   (SELECT COUNT(*) FROM economic_metering_events WHERE output_id = $2),
                   (SELECT COUNT(*) FROM rated_usage WHERE output_id = $2),
                   (SELECT COUNT(*) FROM ledger_transactions WHERE source_output_id = $2)
            FROM job_outputs o
            JOIN output_holds h ON h.output_id = o.output_id
            WHERE o.output_id = $2
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.output_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state == ("succeeded".to_string(), "settled".to_string(), 1, 1, 1, 0),
            format!("unexpected economic settlement state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn nonzero_rating_posts_one_balanced_customer_transaction() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "ledger-worker", 1).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (artifacts, _artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_for_lease(&work)
            .await
            .map_err(debug_error)?;
        seed_price_hold(&database.pool, &prepared[0], 7).await?;
        activate_work(&database.pool, &work).await?;
        let lease = claim_required(&executor, "ledger-executor").await?;
        executor.start(&lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&artifacts, &lease).await?;
        executor
            .record_outcome(&lease, &ExecutorSubmissionOutcome::Succeeded(manifest))
            .await
            .map_err(debug_error)?;

        let settlement = PostgresEconomicSettlementStore::new(database.pool.clone())
            .settle(
                &EconomicReceipt::new(
                    lease.submission_id,
                    EconomicReceiptOutcome::Succeeded,
                    "provider.receipt.v1",
                    json!({"provider_request_id": "provider-paid"}),
                )
                .map_err(debug_error)?,
            )
            .await
            .map_err(debug_error)?;
        require(
            settlement.customer_ledger_transaction_id.is_some(),
            "nonzero customer charge omitted the ledger transaction",
        )?;
        let state: (i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT a.held_micros, a.captured_micros, r.amount_micros,
                   COUNT(p.posting_no)::BIGINT,
                   COALESCE(SUM(p.amount_micros::NUMERIC), 0)::BIGINT
            FROM billing_accounts a
            JOIN rated_usage r ON r.output_id = $1
            JOIN ledger_transactions t ON t.source_output_id = $1
              AND t.transaction_type = 'customer_charge'
            JOIN ledger_postings p ON p.transaction_id = t.transaction_id
            WHERE a.tenant_id = $2 AND a.currency = 'USD'
            GROUP BY a.held_micros, a.captured_micros, r.amount_micros
            "#,
        )
        .bind(lease.output_id)
        .bind(&lease.tenant_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state == (0, 7, 7, 2, 0),
            format!("customer ledger is not exactly balanced: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn uncertain_provider_outcome_keeps_the_full_monetary_hold() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "uncertain-economic-worker", 1).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = executor
            .prepare_for_lease(&work)
            .await
            .map_err(debug_error)?;
        seed_price_hold(&database.pool, &prepared[0], 7).await?;
        activate_work(&database.pool, &work).await?;
        let lease = claim_required(&executor, "uncertain-economic-executor").await?;
        executor.start(&lease).await.map_err(debug_error)?;
        executor
            .record_outcome(
                &lease,
                &ExecutorSubmissionOutcome::Uncertain {
                    error_code: "provider_result_unknown".to_string(),
                },
            )
            .await
            .map_err(debug_error)?;

        let settlement = PostgresEconomicSettlementStore::new(database.pool.clone())
            .settle(
                &EconomicReceipt::new(
                    lease.submission_id,
                    EconomicReceiptOutcome::Uncertain,
                    "provider.receipt.v1",
                    json!({"reason": "provider_result_unknown"}),
                )
                .map_err(debug_error)?,
            )
            .await
            .map_err(debug_error)?;
        require(
            settlement.rated_usage_id.is_none()
                && settlement.customer_ledger_transaction_id.is_none(),
            "uncertain output was charged",
        )?;
        let state: (String, String, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT o.state, h.state, a.held_micros, a.captured_micros,
                   (SELECT COUNT(*) FROM rated_usage WHERE output_id = $1),
                   (SELECT COUNT(*) FROM ledger_transactions WHERE source_output_id = $1)
            FROM job_outputs o
            JOIN output_holds h ON h.output_id = o.output_id
            JOIN billing_accounts a ON a.tenant_id = h.tenant_id AND a.currency = h.currency
            WHERE o.output_id = $1
            "#,
        )
        .bind(lease.output_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state == ("uncertain".to_string(), "held".to_string(), 7, 0, 0, 0),
            format!("uncertain output changed its monetary hold: {state:?}"),
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
        let stale = claim_required_for(&store, "executor-old", 25).await?;
        require(
            sqlx::query(
                r#"
                UPDATE executor_executions
                SET state = 'prepared', executor_owner = NULL, lease_epoch = 0,
                    lease_expires_at_ms = NULL, leased_at_ms = NULL
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(stale.executor_execution_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "leased executor moved backward to prepared",
        )?;
        tokio::time::sleep(Duration::from_millis(40)).await;
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
async fn start_replays_same_running_lease_without_changing_fence_or_timestamp() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "start-replay-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&work).await.map_err(debug_error)?;
        activate_work(&database.pool, &work).await?;
        let lease = claim_required(&store, "stable-executor").await?;
        require(
            sqlx::query(
                r#"
                UPDATE executor_executions
                SET launch_owner = executor_owner,
                    launch_lease_epoch = lease_epoch
                WHERE executor_execution_id = $1 AND submission_id = $2
                "#,
            )
            .bind(lease.executor_execution_id)
            .bind(lease.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "launch fence was set before the leased-to-running transition",
        )?;
        store.start(&lease).await.map_err(debug_error)?;
        require(
            sqlx::query(
                r#"
                UPDATE executor_executions
                SET lease_expires_at_ms =
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT - 1
                WHERE executor_execution_id = $1 AND submission_id = $2
                "#,
            )
            .bind(lease.executor_execution_id)
            .bind(lease.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "running executor lease expiry moved backward",
        )?;
        let before: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT started_at_ms, lease_epoch, lease_expires_at_ms
            FROM executor_executions
            WHERE executor_execution_id = $1 AND submission_id = $2
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;

        store.start(&lease).await.map_err(debug_error)?;

        let after: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT started_at_ms, lease_epoch, lease_expires_at_ms
            FROM executor_executions
            WHERE executor_execution_id = $1 AND submission_id = $2
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            before == after,
            "start replay changed timestamp or lease fence",
        )?;

        sqlx::query(
            r#"
            UPDATE work_items
            SET lease_expires_at_ms =
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT - 1
            WHERE work_item_id = $1
            "#,
        )
        .bind(lease.work_item_id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        store.start(&lease).await.map_err(|error| {
            format!("committed start replay depended on parent lease: {error:?}")
        })?;
        sqlx::query(
            r#"
            UPDATE work_items
            SET lease_expires_at_ms =
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 60000
            WHERE work_item_id = $1
            "#,
        )
        .bind(lease.work_item_id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        let wrong_owner = ExecutorSubmissionLease {
            executor_owner: "other-executor".to_string(),
            ..lease.clone()
        };
        require(
            store.start(&wrong_owner).await == Err(ExecutorSubmissionError::StaleLease),
            "different owner replay was accepted",
        )?;
        let wrong_epoch = ExecutorSubmissionLease {
            executor_lease_epoch: lease.executor_lease_epoch + 1,
            ..lease.clone()
        };
        require(
            store.start(&wrong_epoch).await == Err(ExecutorSubmissionError::StaleLease),
            "different epoch replay was accepted",
        )?;

        require(
            sqlx::query(
                r#"
                UPDATE executor_executions
                SET launch_owner = 'replacement-executor'
                WHERE executor_execution_id = $1 AND submission_id = $2
                "#,
            )
            .bind(lease.executor_execution_id)
            .bind(lease.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "immutable launch owner was replaced",
        )?;
        require(
            sqlx::query(
                r#"
                UPDATE executor_executions
                SET state = 'leased', started_at_ms = NULL,
                    launch_owner = NULL, launch_lease_epoch = NULL
                WHERE executor_execution_id = $1 AND submission_id = $2
                "#,
            )
            .bind(lease.executor_execution_id)
            .bind(lease.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "immutable launch fence was cleared",
        )?;
        require(
            sqlx::query("DELETE FROM executor_executions WHERE executor_execution_id = $1")
                .bind(lease.executor_execution_id)
                .execute(&database.pool)
                .await
                .is_err(),
            "durable executor execution was deleted",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn executor_execution_insert_requires_prepared_state_without_launch_fence() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "forged-insert-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = store
            .prepare_for_lease(&work)
            .await
            .map_err(debug_error)?
            .remove(0);
        let now = database_now(&database.pool).await?;
        let invalid_identity = Uuid::new_v4();
        let identity_output_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO job_outputs
              (output_id, job_id, output_index, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 98, 'pending', $3, $3)
            "#,
        )
        .bind(identity_output_id)
        .bind(prepared.job_id)
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            sqlx::query(
                r#"
                INSERT INTO provider_submissions
                  (submission_id, executor_execution_id, output_id, job_id,
                   tenant_id, provider_id, model, work_item_id,
                   created_by_execution_id, created_by_lease_epoch,
                   command_schema, command_hash, state,
                   prepared_at_ms, updated_at_ms)
                SELECT $1, $1, $2, job_id, tenant_id, provider_id, model,
                       work_item_id, created_by_execution_id,
                       created_by_lease_epoch, command_schema, command_hash,
                       'prepared', $3, $3
                FROM provider_submissions
                WHERE submission_id = $4
                "#,
            )
            .bind(invalid_identity)
            .bind(identity_output_id)
            .bind(now)
            .bind(prepared.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "submission and executor identities were allowed to alias",
        )?;
        let output_id = Uuid::new_v4();
        let submission_id = Uuid::new_v4();
        let executor_execution_id = Uuid::new_v4();
        let mut forged = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO job_outputs
              (output_id, job_id, output_index, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 99, 'pending', $3, $3)
            "#,
        )
        .bind(output_id)
        .bind(prepared.job_id)
        .bind(now)
        .execute(&mut *forged)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_submissions
              (submission_id, executor_execution_id, output_id, job_id,
               tenant_id, provider_id, model, work_item_id,
               created_by_execution_id, created_by_lease_epoch,
               command_schema, command_hash, state,
               prepared_at_ms, started_at_ms, updated_at_ms)
            SELECT $1, $2, $3, job_id, tenant_id, provider_id, model,
                   work_item_id, created_by_execution_id, created_by_lease_epoch,
                   command_schema, command_hash, 'running', $4, $4, $4
            FROM provider_submissions
            WHERE submission_id = $5
            "#,
        )
        .bind(submission_id)
        .bind(executor_execution_id)
        .bind(output_id)
        .bind(now)
        .bind(prepared.submission_id)
        .execute(&mut *forged)
        .await
        .map_err(debug_error)?;
        let error = sqlx::query(
            r#"
            INSERT INTO executor_executions
              (executor_execution_id, submission_id, state, executor_owner,
               lease_epoch, lease_expires_at_ms, created_at_ms, leased_at_ms,
               started_at_ms, updated_at_ms, launch_owner, launch_lease_epoch)
            VALUES ($1, $2, 'running', 'forged-executor', 1, $3,
                    $4, $4, $4, $4, 'forged-executor', 1)
            "#,
        )
        .bind(executor_execution_id)
        .bind(submission_id)
        .bind(now + 60_000)
        .bind(now)
        .execute(&mut *forged)
        .await
        .expect_err("running executor insert must fail");
        require(
            error
                .to_string()
                .contains("must be inserted prepared without a launch fence"),
            "running executor insert failed outside the launch-fence trigger",
        )?;
        forged.rollback().await.map_err(debug_error)
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn success_requires_exact_immutable_artifact_authority() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "authority-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (artifacts, _artifact_root) = artifact_publisher(&store)?;
        store.prepare_for_lease(&work).await.map_err(debug_error)?;
        activate_work(&database.pool, &work).await?;
        let lease = claim_required(&store, "authority-executor").await?;
        store.start(&lease).await.map_err(debug_error)?;

        require(
            sqlx::query(
                r#"
                INSERT INTO executor_artifact_authorities
                  (authority_id, executor_execution_id, submission_id, output_id,
                   job_id, storage_backend, storage_namespace, object_key,
                   sha256_hex, byte_size, media_type, created_at_ms)
                VALUES ($1, $1, $2, $3, $4, 'memory-v1', 'memory-v1:test',
                        'executor-objects/00/forged', $5, 1, 'image/png', 1)
                "#,
            )
            .bind(lease.executor_execution_id)
            .bind(lease.submission_id)
            .bind(lease.output_id)
            .bind(lease.job_id)
            .bind("a".repeat(64))
            .execute(&database.pool)
            .await
            .is_err(),
            "volatile memory backend received a permanent artifact authority",
        )?;
        require(
            sqlx::query(
                r#"
                INSERT INTO executor_result_manifests
                  (manifest_id, artifact_authority_id, executor_execution_id,
                   submission_id, created_at_ms)
                VALUES ($1, $2, $3, $4, 1)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(Uuid::new_v4())
            .bind(lease.executor_execution_id)
            .bind(lease.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "result manifest bypassed the artifact authority foreign key",
        )?;

        let bytes = png_bytes([10, 20, 30, 255]);
        let authority_id = lease.executor_execution_id;
        let manifest = artifacts.publish(&lease, &bytes).await.map_err(debug_error)?;
        require(
            artifacts.publish(&lease, &bytes).await.is_ok(),
            "verified artifact publication did not replay",
        )?;
        require(
            manifest.manifest_id() == lease.submission_id
                && manifest.artifact_authority_id() == lease.executor_execution_id,
            "artifact identities were not derived from durable execution identities",
        )?;

        store
            .record_outcome(
                &lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest.clone()),
            )
            .await
            .map_err(debug_error)?;
        let stored: (Uuid, Uuid, String, i64, String) = sqlx::query_as(
            r#"
            SELECT m.artifact_authority_id, a.authority_id, a.sha256_hex,
                   a.byte_size, a.media_type
            FROM executor_result_manifests m
            JOIN executor_artifact_authorities a
              ON a.authority_id = m.artifact_authority_id
             AND a.executor_execution_id = m.executor_execution_id
             AND a.submission_id = m.submission_id
            WHERE m.manifest_id = $1
            "#,
        )
        .bind(manifest.manifest_id())
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            stored
                == (
                    authority_id,
                    authority_id,
                    sha256(&bytes),
                    bytes.len() as i64,
                    "image/png".to_string(),
                ),
            "authority metadata was not derived from durable object bytes",
        )?;
        require(
            sqlx::query(
                "UPDATE executor_artifact_authorities SET object_key = 'forged' WHERE authority_id = $1",
            )
            .bind(authority_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "artifact authority was mutable after publication",
        )?;
        require(
            sqlx::query(
                "UPDATE executor_result_manifests SET artifact_authority_id = artifact_authority_id WHERE manifest_id = $1",
            )
            .bind(manifest.manifest_id())
            .execute(&database.pool)
            .await
            .is_err(),
            "executor result manifest was mutable after publication",
        )
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
        let (artifacts, _artifact_root) = artifact_publisher(&store)?;
        store.prepare_for_lease(&lease).await.map_err(debug_error)?;
        activate_work(&database.pool, &lease).await?;
        let mut claims = Vec::new();
        for _ in 0..3 {
            let claim = claim_required(&store, "outcome-executor").await?;
            store.start(&claim).await.map_err(debug_error)?;
            claims.push(claim);
        }

        require(
            sqlx::query(
                r#"
                INSERT INTO executor_resolution_decisions
                  (decision_id, executor_execution_id, submission_id, source,
                   observation_id, resolved_state, result_manifest_id,
                   error_code, decided_at_ms)
                VALUES ($1, $1, $2, 'executor_lease_expired', NULL,
                        'uncertain', NULL, 'executor_lease_expired', $3)
                "#,
            )
            .bind(claims[0].executor_execution_id)
            .bind(claims[0].submission_id)
            .bind(database_now(&database.pool).await?)
            .execute(&database.pool)
            .await
            .is_err(),
            "an unexpired running execution accepted a forged expiry decision",
        )?;

        let successful_manifest = publish_result_authority(&artifacts, &claims[0]).await?;
        deactivate_work(&database.pool, &lease, "uncertain").await?;
        let outcomes = [
            ExecutorSubmissionOutcome::Succeeded(successful_manifest),
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
        let mut wrong_owner = claims[1].clone();
        wrong_owner.executor_owner = "different-executor".to_string();
        require(
            store.record_outcome(&wrong_owner, &outcomes[1]).await
                == Err(ExecutorSubmissionError::StaleLease),
            "terminal replay ignored the immutable launch owner",
        )?;
        let mut wrong_epoch = claims[1].clone();
        wrong_epoch.executor_lease_epoch += 1;
        require(
            store.record_outcome(&wrong_epoch, &outcomes[1]).await
                == Err(ExecutorSubmissionError::StaleLease),
            "terminal replay ignored the immutable launch epoch",
        )?;

        require(
            sqlx::query(
                "UPDATE executor_executions SET error_code = 'forged_error' WHERE executor_execution_id = $1",
            )
            .bind(claims[1].executor_execution_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "terminal executor payload remained mutable",
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
        let claim = claim_required_for(&store, "abandoned-executor", 25).await?;
        deactivate_work(&database.pool, &lease, "uncertain").await?;
        tokio::time::sleep(Duration::from_millis(40)).await;

        require(
            sqlx::query(
                r#"
                UPDATE executor_executions
                SET lease_expires_at_ms =
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 60000
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(claim.executor_execution_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "expired leased executor fence was revived",
        )?;

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
        let claim = claim_required_for(&store, "reconcile-executor", 25).await?;
        store.start(&claim).await.map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(40)).await;

        require(
            sqlx::query(
                r#"
                UPDATE executor_executions
                SET lease_expires_at_ms =
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 60000
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(claim.executor_execution_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "expired running executor fence was revived",
        )?;

        require(
            sqlx::query(
                r#"
                INSERT INTO executor_resolution_decisions
                  (decision_id, executor_execution_id, submission_id, source,
                   observation_id, resolved_state, result_manifest_id,
                   error_code, decided_at_ms)
                VALUES ($1, $1, $2, 'executor_lease_expired', NULL,
                        'uncertain', NULL, 'executor_lease_expired', $3)
                "#,
            )
            .bind(claim.executor_execution_id)
            .bind(claim.submission_id)
            .bind(database_now(&database.pool).await?)
            .execute(&database.pool)
            .await
            .is_err(),
            "an unprojected expiry decision poisoned the execution",
        )?;

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
                    &ExecutorSubmissionOutcome::Uncertain {
                        error_code: "executor_lease_expired".to_string(),
                    },
                )
                .await
                == Err(ExecutorSubmissionError::Conflict),
            "late outcome overwrote uncertain evidence",
        )?;
        let late_observation: (String, Option<String>, String) = sqlx::query_as(
            r#"
            SELECT observed_state, error_code, payload_hash
            FROM executor_runner_observations
            WHERE executor_execution_id = $1 AND submission_id = $2
            "#,
        )
        .bind(claim.executor_execution_id)
        .bind(claim.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            late_observation.0 == "uncertain"
                && late_observation.1.as_deref() == Some("executor_lease_expired")
                && late_observation.2.len() == 64,
            "matching late runner evidence was not retained after expiry resolution",
        )?;
        require(
            sqlx::query(
                "UPDATE executor_runner_observations SET payload_hash = payload_hash WHERE executor_execution_id = $1",
            )
            .bind(claim.executor_execution_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "runner observation was mutable",
        )?;
        require(
            sqlx::query(
                "UPDATE executor_resolution_decisions SET source = source WHERE executor_execution_id = $1",
            )
            .bind(claim.executor_execution_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "resolution decision was mutable",
        )?;
        require(
            sqlx::query(
                r#"
                UPDATE provider_submissions
                SET state = 'running', result_manifest_id = NULL,
                    finished_at_ms = NULL, error_code = NULL,
                    resolution_decision_id = NULL
                WHERE submission_id = $1
                "#,
            )
            .bind(claim.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "terminal submission was reopened without its decision",
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
            sqlx::query(
                r#"
                UPDATE executor_executions
                SET state = 'failed', executor_owner = NULL,
                    lease_expires_at_ms = NULL,
                    started_at_ms = started_at_ms + 1,
                    finished_at_ms = $2, updated_at_ms = $2,
                    error_code = 'forged_error'
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(claim.executor_execution_id)
            .bind(database_now(&database.pool).await?)
            .execute(&database.pool)
            .await
            .is_err(),
            "terminal executor transition changed immutable start history",
        )?;
        require(
            sqlx::query(
                r#"
                UPDATE provider_submissions
                SET state = 'failed', command_hash = $2,
                    finished_at_ms = $3, updated_at_ms = $3,
                    error_code = 'forged_error'
                WHERE submission_id = $1
                "#,
            )
            .bind(claim.submission_id)
            .bind("b".repeat(64))
            .bind(database_now(&database.pool).await?)
            .execute(&database.pool)
            .await
            .is_err(),
            "terminal submission transition changed its immutable command",
        )?;
        let valid_outcome = ExecutorSubmissionOutcome::Failed {
            error_code: "provider_rejected".to_string(),
        };
        let mut aliased = claim.clone();
        aliased.executor_execution_id = aliased.submission_id;
        require(
            store.record_outcome(&aliased, &valid_outcome).await
                == Err(ExecutorSubmissionError::InvalidInput),
            "aliased executor identity reached runner observation construction",
        )?;
        let mut nil_identity = claim.clone();
        nil_identity.executor_execution_id = Uuid::nil();
        require(
            store.record_outcome(&nil_identity, &valid_outcome).await
                == Err(ExecutorSubmissionError::InvalidInput),
            "nil executor identity reached runner observation construction",
        )?;
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
async fn start_locks_parent_before_executor_without_deadlock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "start-lock-order-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&work).await.map_err(debug_error)?;
        activate_work(&database.pool, &work).await?;
        let lease = claim_required(&store, "start-lock-order-executor").await?;

        let mut blocker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT 1 FROM work_items WHERE work_item_id = $1 FOR UPDATE")
            .bind(work.work_item_id)
            .execute(&mut *blocker)
            .await
            .map_err(debug_error)?;

        let start_store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let start_lease = lease.clone();
        let start = tokio::spawn(async move { start_store.start(&start_lease).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::time::timeout(
            Duration::from_secs(1),
            sqlx::query(
                "SELECT 1 FROM executor_executions WHERE executor_execution_id = $1 FOR UPDATE",
            )
            .bind(lease.executor_execution_id)
            .execute(&mut *blocker),
        )
        .await
        .map_err(|_| "start locked executor before parent and formed a deadlock edge".to_string())?
        .map_err(debug_error)?;
        blocker.commit().await.map_err(debug_error)?;
        start.await.map_err(debug_error)?.map_err(debug_error)
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn expired_parent_does_not_start_a_still_leased_execution() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "expired-parent-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&work).await.map_err(debug_error)?;
        activate_work(&database.pool, &work).await?;
        let lease = claim_required(&store, "expired-parent-executor").await?;
        sqlx::query(
            r#"
            UPDATE work_items
            SET lease_expires_at_ms =
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT - 1
            WHERE work_item_id = $1
            "#,
        )
        .bind(work.work_item_id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        require(
            store.start(&lease).await == Err(ExecutorSubmissionError::StaleLease),
            "expired parent started a leased execution",
        )?;
        let states: (String, String) = sqlx::query_as(
            r#"
            SELECT e.state, s.state
            FROM executor_executions e
            JOIN provider_submissions s ON s.submission_id = e.submission_id
            WHERE e.executor_execution_id = $1 AND e.submission_id = $2
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            states == ("leased".to_string(), "prepared".to_string()),
            format!("expired parent changed executor states: {states:?}"),
        )
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
        let claim = claim_required_for(&store, "deadline-executor", 150).await?;
        store.start(&claim).await.map_err(debug_error)?;

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

fn artifact_publisher(
    store: &PostgresExecutorSubmissionStore,
) -> TestResult<(ExecutorArtifactPublisher, tempfile::TempDir)> {
    let root = tempfile::tempdir().map_err(debug_error)?;
    let blobs = FilesystemArtifactBlobStore::new(root.path()).map_err(debug_error)?;
    Ok((
        ExecutorArtifactPublisher::with_filesystem_store(Arc::new(blobs), store.clone()),
        root,
    ))
}

async fn publish_result_authority(
    publisher: &ExecutorArtifactPublisher,
    claim: &ExecutorSubmissionLease,
) -> TestResult<ExecutorResultManifest> {
    publisher
        .publish(claim, &png_bytes([10, 20, 30, 255]))
        .await
        .map_err(debug_error)
}

fn png_bytes(pixel: [u8; 4]) -> Vec<u8> {
    let image = RgbaImage::from_pixel(1, 1, Rgba(pixel));
    let mut cursor = std::io::Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(image)
        .write_to(&mut cursor, ImageFormat::Png)
        .unwrap();
    cursor.into_inner()
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

async fn claim_required(
    store: &PostgresExecutorSubmissionStore,
    owner: &str,
) -> TestResult<ExecutorSubmissionLease> {
    claim_required_for(store, owner, 60_000).await
}

async fn claim_required_for(
    store: &PostgresExecutorSubmissionStore,
    owner: &str,
    lease_ms: i64,
) -> TestResult<ExecutorSubmissionLease> {
    store
        .claim_prepared(&claim_scope(), owner, lease_ms)
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "executor claim returned none".to_string())
}

#[tokio::test]
async fn resume_running_fences_owner_scope_state_and_database_expiry() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "resume-worker", 2).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&work).await.map_err(debug_error)?;
        activate_work(&database.pool, &work).await?;

        let leased = claim_required_for(&store, "stable-executor", 200).await?;
        require(
            store
                .resume_running(&claim_scope(), "stable-executor")
                .await
                .map_err(debug_error)?
                .is_none(),
            "leased execution was resumable",
        )?;
        store.start(&leased).await.map_err(debug_error)?;

        let resumed = store
            .resume_running(&claim_scope(), "stable-executor")
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "running execution did not resume".to_string())?;
        require(resumed == leased, "resume changed durable lease identity")?;
        require(
            store
                .resume_running(&claim_scope(), "other-executor")
                .await
                .map_err(debug_error)?
                .is_none(),
            "other owner resumed running execution",
        )?;
        let wrong_scope = ExecutorClaimScope {
            provider_id: claim_scope().provider_id,
            command_schema: "other-command-v1".to_string(),
        };
        require(
            store
                .resume_running(&wrong_scope, "stable-executor")
                .await
                .map_err(debug_error)?
                .is_none(),
            "other scope resumed running execution",
        )?;

        tokio::time::sleep(Duration::from_millis(220)).await;
        require(
            store
                .resume_running(&claim_scope(), "stable-executor")
                .await
                .map_err(debug_error)?
                .is_none(),
            "expired running execution was resumable",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn launch_context_is_loaded_only_for_the_exact_running_lease() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "launch-context-worker", 2).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&work).await.map_err(debug_error)?;
        activate_work(&database.pool, &work).await?;
        let lease = claim_required(&store, "launch-context-executor").await?;

        require(
            store.load_launch_context(&lease).await == Err(ExecutorSubmissionError::StaleLease),
            "leased execution exposed launch context before start",
        )?;
        store.start(&lease).await.map_err(debug_error)?;

        let context = store
            .load_launch_context(&lease)
            .await
            .map_err(debug_error)?;
        require(
            context.request_id().starts_with("request-"),
            "launch context lost request identity",
        )?;
        require(
            context.api_profile() == "openai-images-v1",
            "launch context lost API profile",
        )?;
        require(
            context.output_index() == lease.output_index,
            "launch context changed output index",
        )?;
        require(
            context.command_schema() == lease.command_schema
                && context.command_hash() == lease.command_hash
                && context.command_json() == &work.command_json,
            "launch context changed the immutable command",
        )?;

        let mut forged = lease.clone();
        forged.executor_owner = "other-executor".to_string();
        require(
            store.load_launch_context(&forged).await == Err(ExecutorSubmissionError::StaleLease),
            "forged owner loaded launch context",
        )?;
        forged = lease.clone();
        forged.executor_lease_epoch += 1;
        require(
            store.load_launch_context(&forged).await == Err(ExecutorSubmissionError::StaleLease),
            "forged epoch loaded launch context",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn launch_context_rejects_command_tampering_and_expired_lease() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "launch-integrity-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_for_lease(&work).await.map_err(debug_error)?;
        activate_work(&database.pool, &work).await?;
        let lease = claim_required_for(&store, "launch-integrity-executor", 100).await?;
        store.start(&lease).await.map_err(debug_error)?;

        sqlx::query(
            "UPDATE job_payloads SET command_json = jsonb_set(command_json, '{prompt}', '\"tampered\"') WHERE job_id = $1",
        )
        .bind(lease.job_id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            store.load_launch_context(&lease).await
                == Err(ExecutorSubmissionError::Conflict),
            "tampered command passed canonical hash validation",
        )?;

        sqlx::query("UPDATE job_payloads SET command_json = $2 WHERE job_id = $1")
            .bind(lease.job_id)
            .bind(&work.command_json)
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(120)).await;
        require(
            store.load_launch_context(&lease).await
                == Err(ExecutorSubmissionError::StaleLease),
            "expired lease loaded launch context",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

async fn seed_price_hold(
    pool: &PgPool,
    submission: &gpt_image_2_gateway::PreparedExecutorSubmission,
    success_micros: i64,
) -> TestResult {
    let now = database_now(pool).await?;
    let quote_id = Uuid::new_v4();
    let price_version_id = Uuid::new_v4();
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO price_versions
          (price_version_id, price_key, version, api_profile, operation, provider_id, model,
           currency, success_micros, failed_micros, no_effect_micros, state,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 1, 'test', 'generation', $3, $4,
                'USD', $5, 0, 0, 'retired', $6, $6)
        "#,
    )
    .bind(price_version_id)
    .bind(format!("test-price-{price_version_id}"))
    .bind(&submission.provider_id)
    .bind(&submission.model)
    .bind(success_micros)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts
          (tenant_id, currency, credit_limit_micros, held_micros, captured_micros,
           created_at_ms, updated_at_ms)
        VALUES ($1, 'USD', $2, $2, 0, $3, $3)
        "#,
    )
    .bind(&submission.tenant_id)
    .bind(success_micros)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO price_quotes
          (quote_id, job_id, price_version_id, currency, output_count,
           success_micros, failed_micros, no_effect_micros, max_total_micros,
           quote_hash, created_at_ms)
        VALUES ($1, $2, $3, 'USD', 1, $4, 0, 0, $4, $5, $6)
        "#,
    )
    .bind(quote_id)
    .bind(submission.job_id)
    .bind(price_version_id)
    .bind(success_micros)
    .bind("e".repeat(64))
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO output_holds
          (output_id, job_id, quote_id, tenant_id, currency, held_micros,
           state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, 'USD', $5, 'held', $6, $6)
        "#,
    )
    .bind(submission.output_id)
    .bind(submission.job_id)
    .bind(quote_id)
    .bind(&submission.tenant_id)
    .bind(success_micros)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query("UPDATE jobs SET economics_contract_version = 2 WHERE job_id = $1")
        .bind(submission.job_id)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)?;
    Ok(())
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

async fn seed_codex_generation_lease(pool: &PgPool, worker_id: &str) -> TestResult<WorkLease> {
    let job_id = Uuid::new_v4();
    let work_item_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let admission_session_id = Uuid::new_v4();
    let request_id = format!("request-{}", Uuid::new_v4().simple());
    let job = GenerationJob {
        request_id: request_id.clone(),
        model: "gpt-image-2".to_string(),
        prompt: "draw a process-smoke lighthouse".to_string(),
        moderation: "auto".to_string(),
        n: 1,
        size: "1024x1024".to_string(),
        quality: "high".to_string(),
        output_format: "png".to_string(),
        output_compression: None,
        background: "opaque".to_string(),
        stream: false,
        partial_images: 0,
    };
    let command =
        GenerationCommandV1::from_generation_job(&job, "openai-images-v1", "openai-codex");
    let command_json = serde_json::to_value(&command).map_err(debug_error)?;
    let request_hash = command.request_hash_hex();
    let now = database_now(pool).await?;
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model, state,
           requested_units, created_at_ms, updated_at_ms)
        VALUES ($1, 'executord-process-smoke', $2, 'generation', 'openai-codex',
                'gpt-image-2', 'reserved', 1, $3, $3)
        "#,
    )
    .bind(job_id)
    .bind(&request_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO admission_sessions
          (session_id, owner_token, tenant_id, project_id, api_profile, operation,
           request_id, request_hash, state, job_id, deadline_at_ms, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'executord-process-smoke', 'project-test', 'openai-images-v1',
                'generation', $3, $4, 'attached', $5, $6, $7, $7)
        "#,
    )
    .bind(admission_session_id)
    .bind(Uuid::new_v4())
    .bind(&request_id)
    .bind(&request_hash)
    .bind(job_id)
    .bind(now + 300_000)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_payloads
          (job_id, admission_session_id, command_schema, command_json, request_hash, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(job_id)
    .bind(admission_session_id)
    .bind(GENERATION_COMMAND_SCHEMA)
    .bind(&command_json)
    .bind(&request_hash)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
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
    .map_err(debug_error)?;
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
    .map_err(debug_error)?;
    Ok(WorkLease {
        work_item_id,
        job_id,
        execution_id,
        lease_epoch: 7,
        worker_id: worker_id.to_string(),
        command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
        command_json,
    })
}

struct ExecutordFixture {
    _temp: tempfile::TempDir,
    artifact_root: std::path::PathBuf,
    runner_root: std::path::PathBuf,
    credentials: std::path::PathBuf,
    fake_codex: std::path::PathBuf,
    invocations: std::path::PathBuf,
}

impl ExecutordFixture {
    fn new(delay: Duration) -> TestResult<Self> {
        let temp = tempfile::TempDir::new().map_err(debug_error)?;
        let artifact_root = temp.path().join("artifacts");
        let runner_root = temp.path().join("runner");
        let credentials = temp.path().join("credentials");
        fs::create_dir(&artifact_root).map_err(debug_error)?;
        fs::create_dir(&credentials).map_err(debug_error)?;
        fs::set_permissions(&artifact_root, fs::Permissions::from_mode(0o700))
            .map_err(debug_error)?;
        fs::set_permissions(&credentials, fs::Permissions::from_mode(0o700))
            .map_err(debug_error)?;
        let auth = credentials.join("auth.json");
        fs::write(&auth, b"{}\n").map_err(debug_error)?;
        fs::set_permissions(&auth, fs::Permissions::from_mode(0o600)).map_err(debug_error)?;
        let source = temp.path().join("source.png");
        let mut bytes = std::io::Cursor::new(Vec::new());
        DynamicImage::new_rgba8(1, 1)
            .write_to(&mut bytes, ImageFormat::Png)
            .map_err(debug_error)?;
        fs::write(&source, bytes.into_inner()).map_err(debug_error)?;
        let invocations = temp.path().join("invocations");
        let fake_codex = temp.path().join("fake-codex");
        let delay = if delay.is_zero() {
            String::new()
        } else {
            format!("/bin/sleep {:.3}\n", delay.as_secs_f64())
        };
        fs::write(
            &fake_codex,
            format!(
                "#!/bin/sh\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\n{delay}/bin/cp '{}' final.png\n",
                invocations.display(),
                source.display()
            ),
        )
        .map_err(debug_error)?;
        fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o755)).map_err(debug_error)?;
        Ok(Self {
            _temp: temp,
            artifact_root,
            runner_root,
            credentials,
            fake_codex,
            invocations,
        })
    }

    fn command(&self, database: &TestDatabase, owner: &str) -> TestResult<tokio::process::Command> {
        self.command_with_lease(database, owner, 10_000, 250)
    }

    fn command_with_lease(
        &self,
        database: &TestDatabase,
        owner: &str,
        lease_ms: u64,
        heartbeat_ms: u64,
    ) -> TestResult<tokio::process::Command> {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_executord"));
        command
            .env_clear()
            .env(
                "DATABASE_URL",
                env::var("TEST_DATABASE_URL").map_err(debug_error)?,
            )
            .env("GATEWAY_DATABASE_SCHEMA", &database.schema)
            .env("GATEWAY_ARTIFACT_ROOT", &self.artifact_root)
            .env("EXECUTOR_RUNNER_ROOT", &self.runner_root)
            .env(
                "EXECUTOR_HELPER_EXECUTABLE",
                env!("CARGO_BIN_EXE_codex-runner"),
            )
            .env("EXECUTOR_CODEX_EXECUTABLE", &self.fake_codex)
            .env("EXECUTOR_CODEX_CREDENTIAL_HOME", &self.credentials)
            .env("EXECUTOR_OWNER", owner)
            .env("EXECUTOR_PROVIDER_ID", "openai-codex")
            .env("EXECUTOR_COMMAND_SCHEMA", GENERATION_COMMAND_SCHEMA)
            .env("EXECUTOR_LEASE_MS", lease_ms.to_string())
            .env("EXECUTOR_HEARTBEAT_INTERVAL_MS", heartbeat_ms.to_string())
            .env("EXECUTOR_POLL_INTERVAL_MS", "20")
            .env("EXECUTOR_PROCESS_POLL_INTERVAL_MS", "10")
            .env("EXECUTOR_PROCESS_STARTUP_GRACE_MS", "1000")
            .env("EXECUTOR_REQUEST_TIMEOUT_MS", "5000")
            .env("EXECUTOR_OWNER_GUARD_TIMEOUT_MS", "1000")
            .env("RUST_LOG", "executord=info")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        Ok(command)
    }
}

fn walk_regular_files(root: &std::path::Path) -> TestResult<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(root).map_err(debug_error)? {
        let path = entry.map_err(debug_error)?.path();
        let metadata = fs::symlink_metadata(&path).map_err(debug_error)?;
        if metadata.is_dir() {
            files.extend(walk_regular_files(&path)?);
        } else if metadata.is_file() {
            files.push(path);
        } else {
            return Err(format!("unexpected artifact entry: {}", path.display()));
        }
    }
    Ok(files)
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
