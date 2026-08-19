use std::{
    collections::HashSet, env, fs, os::unix::fs::PermissionsExt, process::Stdio, sync::Arc,
    time::Duration,
};

use gpt_image_2_gateway::database::{connect_test_pool_with_search_path, run_migrations};
use gpt_image_2_gateway::{
    CODEX_GENERATION_ADAPTER_REVISION, CanonicalExecutorOutcome, CustomerArtifactPublisher,
    ExecutionSettlementStore, ExecutorParentTerminalState, ExecutorTerminalBlockReason,
    ExecutorTerminalError, ExecutorTerminalStore, GenerationJob, PostgresExecutionSettlementStore,
    PostgresExecutorTerminalStore, PostgresReconciliationStore, ReconciliationOutcome,
    ReconciliationStore, UsageCharge, UsageLimits, UsageReservation, UsageSnapshot,
    admission::{
        AdmissionTicket, DreaminaImageAdmissionPlan, DreaminaVideoAdmissionPlan,
        GENERATION_COMMAND_SCHEMA, GenerationCommandV1, VIDEO_GENERATION_OPERATION, WorkLease,
    },
    artifacts::{
        ArtifactBlobStore, ArtifactIdentity, ExecutorArtifactPublisher,
        FilesystemArtifactBlobStore, GENERATION_RESPONSE_SCHEMA, GenerationResponseProjection,
        GenerationResultManifest, InMemoryArtifactBlobStore,
    },
    economics::{
        EconomicReceipt, EconomicReceiptOutcome, EconomicSettlementStore,
        PostgresEconomicSettlementStore,
    },
    executor::{
        ExecutorClaimScope, ExecutorEvidenceStore, ExecutorExecutionProfileStore,
        ExecutorHandoffStore, ExecutorLaunchContextStore, ExecutorOwnerGuardError,
        ExecutorResultManifest, ExecutorSubmissionError, ExecutorSubmissionLease,
        ExecutorSubmissionOutcome, ExecutorSubmissionStore, PostgresExecutorOwnerGuard,
        PostgresExecutorSubmissionStore,
    },
    reconcile_inline_customer_settlement,
};
use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
use image_api_contracts::dreamina::{
    DREAMINA_IMAGES_API_PROFILE, DREAMINA_VIDEOS_API_PROFILE, DreaminaImageGenerationRequest,
    DreaminaVideoGenerationRequest,
};
use image_provider_contracts::{
    BillingMetric, ProviderCostEvidenceScope, ProviderReportedCostEvidenceV1,
};
use image_provider_dreamina_cli::{
    ADAPTER_REVISION as DREAMINA_ADAPTER_REVISION, DREAMINA_IMAGE_GENERATION_OPERATION_V1,
    DREAMINA_SUBMIT_COMMAND_SCHEMA, DREAMINA_VIDEO_GENERATION_OPERATION_V1,
    PROVIDER_ID as DREAMINA_PROVIDER_ID,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const TEST_PROFILE_ID: Uuid = Uuid::from_u128(0x100);
const CODEX_PROFILE_ID: Uuid = Uuid::from_u128(0x200);
const TEST_POLICY_ID: Uuid = Uuid::from_u128(0x300);
const CODEX_POLICY_ID: Uuid = Uuid::from_u128(0x400);
const TEST_POOL_ID: Uuid = Uuid::from_u128(0x500);
const CODEX_POOL_ID: Uuid = Uuid::from_u128(0x600);
const TEST_ACCOUNT_ID: Uuid = Uuid::from_u128(0x700);
const CODEX_ACCOUNT_ID: Uuid = Uuid::from_u128(0x800);
const DREAMINA_PROFILE_ID: Uuid = Uuid::from_u128(0x900);
const DREAMINA_VIDEO_PROFILE_ID: Uuid = Uuid::from_u128(0xd00);
const DREAMINA_POLICY_ID: Uuid = Uuid::from_u128(0xa00);
const DREAMINA_POOL_ID: Uuid = Uuid::from_u128(0xb00);
const DREAMINA_ACCOUNT_ID: Uuid = Uuid::from_u128(0xc00);
const TEST_AUTH_SHA256: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const CODEX_AUTH_SHA256: &str = "ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356";
const DREAMINA_AUTH_SHA256: &str =
    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const PROCESS_STATE_TIMEOUT: Duration = Duration::from_secs(30);
static EXECUTORD_PROCESS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct AtomicCompletionState {
    reduction_state: String,
    output_state: String,
    work_state: String,
    attempt_state: String,
    committed_units: i32,
    released_units: i32,
    charged_units: i32,
    quota_state: String,
    receipt_count: i64,
    provider_usage_fact_count: i64,
    economic_meter_count: i64,
    rating_count: i64,
    artifact_count: i64,
    projection_count: i64,
    usage_count: i64,
    job_event_count: i64,
    outbox_count: i64,
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct TerminalParentSnapshot {
    work_state: String,
    attempt_state: String,
    job_state: String,
    quota_state: String,
    committed_units: i32,
    released_units: i32,
    charged_units: i32,
    receipt_count: i64,
    economic_meter_count: i64,
    rating_count: i64,
    artifact_count: i64,
    projection_count: i64,
    held_hold_count: i64,
    terminal_job_event_count: i64,
    terminal_outbox_count: i64,
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct V4TerminalEconomicState {
    job_state: String,
    output_state: String,
    receipt_count: i64,
    provider_usage_fact_count: i64,
    legacy_economic_meter_count: i64,
    legacy_rating_count: i64,
    legacy_hold_count: i64,
    legacy_customer_charge_count: i64,
    customer_rating_count: i64,
    customer_rating_line_count: i64,
    customer_fact_link_count: i64,
    customer_job_charge_count: i64,
    hold_state: String,
    hold_captured_micros: i64,
    hold_released_micros: i64,
    account_held_micros: i64,
    account_captured_micros: i64,
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct InlineCustomerSettlementState {
    job_state: String,
    charged_units: i32,
    quota_state: String,
    committed_units: i32,
    released_units: i32,
    usage_fact_count: i64,
    customer_rating_count: i64,
    customer_charge_count: i64,
    hold_state: String,
    captured_micros: i64,
    released_micros: i64,
    account_held_micros: i64,
    account_captured_micros: i64,
}

fn profile_id_for_lease(lease: &WorkLease) -> Uuid {
    match lease.command_schema.as_str() {
        GENERATION_COMMAND_SCHEMA => CODEX_PROFILE_ID,
        DREAMINA_SUBMIT_COMMAND_SCHEMA => DREAMINA_PROFILE_ID,
        _ => TEST_PROFILE_ID,
    }
}

#[tokio::test]
async fn profile_bound_claim_hands_off_to_the_same_execution_profile() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease =
            seed_codex_generation_lease(&database.pool, "profile-bound-handoff-worker").await?;
        sqlx::query("UPDATE work_items SET execution_profile_id = $2 WHERE work_item_id = $1")
            .bind(lease.work_item_id)
            .bind(CODEX_PROFILE_ID)
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;

        let prepared = PostgresExecutorSubmissionStore::new(database.pool.clone())
            .prepare_and_handoff(&lease, CODEX_PROFILE_ID)
            .await
            .map_err(debug_error)?;
        require(
            prepared.len() == 1,
            "profile-bound claim did not hand off its output",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn real_executord_process_runs_one_output_through_durable_helper_and_artifact_authority()
-> TestResult {
    let _process_guard = EXECUTORD_PROCESS_TEST_LOCK.lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_codex_generation_lease(&database.pool, "executord-smoke-workerd").await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = store
            .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
            .await
            .map_err(debug_error)?;
        require(prepared.len() == 1, "expected one prepared output")?;

        let files = ExecutordFixture::new(Duration::ZERO)?;
        let mut child = files
            .command(&database, "executord-process-smoke")
            .await?
            .spawn()
            .map_err(|error| format!("failed to spawn executord: {error}"))?;
        let deadline = tokio::time::Instant::now() + PROCESS_STATE_TIMEOUT;
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
                let output = child.wait_with_output().await.map_err(debug_error)?;
                return Err(format!(
                    "executord exited early with {status}; row={row:?}; stdout={}; stderr={}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ));
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
async fn executord_rejects_auth_home_that_does_not_match_database_credential() -> TestResult {
    let _process_guard = EXECUTORD_PROCESS_TEST_LOCK.lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let files = ExecutordFixture::new(Duration::ZERO)?;
        let wrong_credentials = files._temp.path().join("wrong-credentials");
        fs::create_dir(&wrong_credentials).map_err(debug_error)?;
        fs::set_permissions(&wrong_credentials, fs::Permissions::from_mode(0o700))
            .map_err(debug_error)?;
        let wrong_auth = wrong_credentials.join("auth.json");
        fs::write(&wrong_auth, b"{\"account\":\"other\"}\n").map_err(debug_error)?;
        fs::set_permissions(&wrong_auth, fs::Permissions::from_mode(0o600)).map_err(debug_error)?;

        let mut command = files.command(&database, "executord-wrong-auth").await?;
        command.env("EXECUTOR_CODEX_CREDENTIAL_HOME", &wrong_credentials);
        let output = tokio::time::timeout(Duration::from_secs(5), command.output())
            .await
            .map_err(|_| "executord did not reject mismatched credentials".to_string())?
            .map_err(debug_error)?;
        require(
            !output.status.success(),
            "executord accepted auth material from a different provider account",
        )?;
        require(
            !files.invocations.exists(),
            "mismatched credentials launched the provider process",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn restarted_executord_attaches_running_helper_without_relaunching_provider() -> TestResult {
    let _process_guard = EXECUTORD_PROCESS_TEST_LOCK.lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease =
            seed_codex_generation_lease(&database.pool, "executord-restart-workerd").await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        require(
            store
                .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
                .await
                .map_err(debug_error)?
                .len()
                == 1,
            "expected one prepared output",
        )?;
        let files = ExecutordFixture::new(Duration::from_secs(2))?;
        let owner = "executord-restart-smoke";
        let mut first = files
            .command(&database, owner)
            .await?
            .spawn()
            .map_err(debug_error)?;
        tokio::time::timeout(PROCESS_STATE_TIMEOUT, async {
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

        let now = database_now(&database.pool).await?;
        let mut disable = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            "UPDATE provider_execution_profiles SET state = 'disabled', updated_at_ms = $2 WHERE execution_profile_id = $1",
        )
        .bind(CODEX_PROFILE_ID)
        .bind(now)
        .execute(&mut *disable)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "UPDATE provider_credential_pools SET state = 'disabled', updated_at_ms = $2 WHERE credential_pool_id = $1",
        )
        .bind(CODEX_POOL_ID)
        .bind(now)
        .execute(&mut *disable)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "UPDATE executor_resource_policies SET state = 'disabled' WHERE resource_policy_id = $1 AND revision = 1",
        )
        .bind(CODEX_POLICY_ID)
        .execute(&mut *disable)
        .await
        .map_err(debug_error)?;
        disable.commit().await.map_err(debug_error)?;

        let mut second = files
            .command(&database, owner)
            .await?
            .spawn()
            .map_err(debug_error)?;
        let terminal = tokio::time::timeout(PROCESS_STATE_TIMEOUT, async {
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
                    let mut stderr = String::new();
                    if let Some(mut stream) = second.stderr.take() {
                        tokio::io::AsyncReadExt::read_to_string(&mut stream, &mut stderr)
                            .await
                            .map_err(debug_error)?;
                    }
                    break Err(format!(
                        "second executord exited early with {status}: {stderr}"
                    ));
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
    let _process_guard = EXECUTORD_PROCESS_TEST_LOCK.lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_codex_generation_lease(&database.pool, "late-evidence-workerd").await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = store
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        require(prepared.len() == 1, "expected one prepared output")?;

        let files = ExecutordFixture::new(Duration::from_millis(1_200))?;
        let owner = "late-evidence-executord";
        let mut first = files
            .command_with_lease(&database, owner, 800, 100)
            .await?
            .spawn()
            .map_err(debug_error)?;
        tokio::time::timeout(PROCESS_STATE_TIMEOUT, async {
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
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
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
        tokio::time::timeout(PROCESS_STATE_TIMEOUT, async {
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
            .command_with_lease(&database, owner, 800, 100)
            .await?
            .spawn()
            .map_err(debug_error)?;
        tokio::time::timeout(PROCESS_STATE_TIMEOUT, async {
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
        let allocation: (String, i32) = sqlx::query_as(
            r#"
            SELECT allocation.state, policy.allocated_count
            FROM executor_capacity_allocations allocation
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE allocation.executor_execution_id = $1
            "#,
        )
        .bind(prepared[0].executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            allocation == ("released".to_string(), 0),
            format!("late terminal evidence did not release capacity: {allocation:?}"),
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
    let _process_guard = EXECUTORD_PROCESS_TEST_LOCK.lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_codex_generation_lease(&database.pool, "executord-drain-workerd").await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        require(
            store
                .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
                .await
                .map_err(debug_error)?
                .len()
                == 1,
            "expected one prepared output",
        )?;
        let files = ExecutordFixture::new(Duration::from_secs(1))?;
        let mut child = files
            .command(&database, "executord-drain-smoke")
            .await?
            .spawn()
            .map_err(debug_error)?;
        tokio::time::timeout(PROCESS_STATE_TIMEOUT, async {
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
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: "openai.images.generation.v1".to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
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
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
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
                tokio::spawn(async move {
                    store
                        .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
                        .await
                })
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
async fn handoff_commit_failure_rolls_back_every_identity_and_parent_projection() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "rollback-handoff-worker", 2).await?;
        sqlx::query(
            r#"
            CREATE FUNCTION fail_test_executor_handoff() RETURNS TRIGGER AS $$
            BEGIN
                RAISE EXCEPTION 'injected handoff commit failure';
            END;
            $$ LANGUAGE plpgsql
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            CREATE CONSTRAINT TRIGGER fail_test_executor_handoff_commit
                AFTER UPDATE ON work_items
                DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW
                WHEN (NEW.state = 'awaiting_executor')
                EXECUTE FUNCTION fail_test_executor_handoff()
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        require(
            store
                .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
                .await
                == Err(ExecutorSubmissionError::Unavailable),
            "injected commit failure was not surfaced",
        )?;
        let rolled_back: (String, String, Option<Uuid>, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT work.state, attempt.state, work.execution_profile_id,
                   (SELECT COUNT(*) FROM provider_submissions WHERE job_id = work.job_id),
                   (SELECT COUNT(*) FROM executor_executions execution
                    JOIN provider_submissions submission
                      ON submission.submission_id = execution.submission_id
                    WHERE submission.job_id = work.job_id),
                   (SELECT COUNT(*) FROM provider_submission_attachments
                    WHERE job_id = work.job_id)
            FROM work_items work
            JOIN job_attempts attempt
              ON attempt.execution_id = work.execution_id
             AND attempt.work_item_id = work.work_item_id
             AND attempt.lease_epoch = work.lease_epoch
            WHERE work.work_item_id = $1
            "#,
        )
        .bind(lease.work_item_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            rolled_back == ("leased".to_string(), "claimed".to_string(), None, 0, 0, 0),
            format!("failed handoff leaked durable state: {rolled_back:?}"),
        )?;

        sqlx::query("DROP TRIGGER fail_test_executor_handoff_commit ON work_items")
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        sqlx::query("DROP FUNCTION fail_test_executor_handoff()")
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        let prepared = store
            .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
            .await
            .map_err(debug_error)?;
        require(
            prepared.len() == 2,
            "exact retry did not commit the handoff",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn committed_handoff_replays_after_profile_disable_and_fences_other_identity() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "disabled-replay-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let first = store
            .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
            .await
            .map_err(debug_error)?;
        let (other_profile_id, _) = seed_limited_test_profile(&database.pool, 1).await?;
        sqlx::query(
            "UPDATE provider_execution_profiles SET state = 'disabled' WHERE execution_profile_id = $1",
        )
        .bind(profile_id_for_lease(&lease))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        let replay = store
            .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
            .await
            .map_err(debug_error)?;
        require(first == replay, "disabled bound profile changed replay identity")?;
        require(
            store.prepare_and_handoff(&lease, other_profile_id).await
                == Err(ExecutorSubmissionError::Conflict),
            "committed handoff accepted a different profile",
        )?;
        let mut forged_epoch = lease.clone();
        forged_epoch.lease_epoch += 1;
        require(
            store
                .prepare_and_handoff(&forged_epoch, profile_id_for_lease(&lease))
                .await
                == Err(ExecutorSubmissionError::StaleLease),
            "committed handoff accepted a different worker epoch",
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
        let output_ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT output_id FROM job_outputs WHERE job_id = $1 ORDER BY output_index",
        )
        .bind(lease.job_id)
        .fetch_all(&database.pool)
        .await
        .map_err(debug_error)?;

        let prepared = PostgresExecutorSubmissionStore::new(database.pool.clone())
            .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
            .await
            .map_err(debug_error)?;
        require(
            prepared
                .iter()
                .map(|item| item.output_id)
                .collect::<Vec<_>>()
                == output_ids,
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
async fn standalone_v2_economics_cannot_bypass_terminal_reducer() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "economic-bypass-worker", 1).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (artifacts, _artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(|error| format!("v4 prepare_and_handoff failed: {error:?}"))?;
        seed_price_hold(&database.pool, &prepared[0], 7).await?;
        let lease = claim_required(&executor, "economic-bypass-executor").await?;
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
        require(
            economics.settle(&receipt).await
                == Err(gpt_image_2_gateway::economics::EconomicSettlementError::Unavailable),
            "standalone V2 economics bypassed canonical terminal reduction",
        )?;

        let state: (String, String, String, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT reduction.state, o.state, h.state,
                   (SELECT COUNT(*) FROM provider_receipts WHERE submission_id = $1),
                   (SELECT COUNT(*) FROM economic_metering_events WHERE output_id = $2),
                   (SELECT COUNT(*) FROM rated_usage WHERE output_id = $2),
                   account.held_micros, account.captured_micros
            FROM executor_terminal_reductions reduction
            JOIN provider_submissions submission
              ON submission.submission_id = reduction.submission_id
            JOIN job_outputs o ON o.output_id = submission.output_id
            JOIN output_holds h ON h.output_id = o.output_id
            JOIN billing_accounts account
              ON account.tenant_id = h.tenant_id AND account.currency = h.currency
            WHERE reduction.submission_id = $1 AND o.output_id = $2
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.output_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state
                == (
                    "ready".to_string(),
                    "pending".to_string(),
                    "held".to_string(),
                    0,
                    0,
                    0,
                    7,
                    0,
                ),
            format!("standalone V2 economics leaked effects before reducer: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn terminal_reduction_claim_reads_only_canonical_success_authority() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "terminal-reader-worker", 1).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (artifacts, artifact_root) = artifact_publisher(&executor)?;
        executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        let executor_lease = claim_required(&executor, "terminal-reader-executor").await?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&artifacts, &executor_lease).await?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest.clone()),
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("terminal-reader", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "canonical terminal reduction was not queued".to_string())?;
        require(
            terminal.submission_id == executor_lease.submission_id
                && terminal.executor_execution_id == executor_lease.executor_execution_id
                && terminal.resolution_decision_id == executor_lease.executor_execution_id
                && terminal.output_id == executor_lease.output_id
                && terminal.job_id == executor_lease.job_id
                && terminal.work_item_id == executor_lease.work_item_id
                && terminal.attempt_execution_id == work.execution_id
                && terminal.attempt_lease_epoch == work.lease_epoch,
            "terminal read model changed a canonical execution identity",
        )?;
        let CanonicalExecutorOutcome::Succeeded(authority) = &terminal.outcome else {
            return Err(format!(
                "successful executor decision mapped to {:?}",
                terminal.outcome
            ));
        };
        require(
            authority.authority_id == executor_lease.executor_execution_id
                && manifest.manifest_id() == executor_lease.submission_id
                && manifest.artifact_authority_id() == authority.authority_id
                && authority.storage_backend == "filesystem-v1"
                && authority.storage_namespace.starts_with("filesystem-v1:")
                && !authority.object_key.is_empty()
                && authority.sha256_hex.len() == 64
                && authority.byte_size > 0
                && authority.media_type == "image/png",
            "terminal success omitted or changed artifact authority",
        )?;
        let customer_blobs = Arc::new(
            FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?,
        );
        let customer_publisher = CustomerArtifactPublisher::new(customer_blobs.clone());
        let first_customer = customer_publisher
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        let replay_customer = customer_publisher
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        require(
            first_customer == replay_customer
                && first_customer.identity.artifact_id == terminal.output_id
                && first_customer.identity.execution_id == terminal.attempt_execution_id
                && first_customer.identity.lease_epoch == terminal.attempt_lease_epoch
                && first_customer.identity.output_index
                    == u32::try_from(terminal.output_index).map_err(debug_error)?
                && first_customer.sha256_hex == authority.sha256_hex
                && first_customer.byte_size == authority.byte_size
                && first_customer.identity.media_type == authority.media_type
                && first_customer.object_key.starts_with("objects/"),
            "customer artifact publication was not deterministic",
        )?;
        require(
            customer_blobs
                .get(&first_customer)
                .await
                .map_err(debug_error)?
                == png_bytes([10, 20, 30, 255]),
            "customer artifact bytes differ from canonical executor bytes",
        )?;
        let renewed = reductions
            .heartbeat_terminal(&terminal, 90_000)
            .await
            .map_err(debug_error)?;
        require(
            renewed.reducer_lease_epoch == terminal.reducer_lease_epoch
                && renewed.reducer_lease_expires_at_ms >= terminal.reducer_lease_expires_at_ms,
            "terminal reduction heartbeat changed its fence",
        )?;
        require(
            reductions
                .claim_terminal("another-reader", 60_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "a live terminal reduction lease was claimed twice",
        )?;
        require(
            sqlx::query(
                "UPDATE executor_terminal_reductions SET resolution_decision_id = $2 WHERE submission_id = $1",
            )
            .bind(terminal.submission_id)
            .bind(Uuid::new_v4())
            .execute(&database.pool)
            .await
            .is_err(),
            "terminal reduction decision identity was rewritten",
        )?;
        require(
            sqlx::query("DELETE FROM executor_terminal_reductions WHERE submission_id = $1")
                .bind(terminal.submission_id)
                .execute(&database.pool)
                .await
                .is_err(),
            "terminal reduction queue item was deleted",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn inline_v4_success_captures_customer_hold_once() -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_codex_generation_lease(&database.pool, "inline-v4-success-worker").await?;
        bind_inline_profile(&database.pool, &work).await?;
        seed_v4_customer_quote(&database.pool, &work, 7).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let reservation = inline_usage_reservation(&database.pool, &work).await?;
        let artifacts = Arc::new(InMemoryArtifactBlobStore::default());
        let manifest = inline_generation_manifest(artifacts.as_ref(), &work, &reservation).await?;
        let settlement = PostgresExecutionSettlementStore::new(database.pool.clone(), artifacts);

        settlement
            .succeed(&work, &reservation, &manifest)
            .await
            .map_err(debug_error)?;
        settlement
            .succeed(&work, &reservation, &manifest)
            .await
            .map_err(debug_error)?;

        let state = inline_customer_settlement_state(&database.pool, &work).await?;
        require(
            state
                == InlineCustomerSettlementState {
                    job_state: "succeeded".to_string(),
                    charged_units: 1,
                    quota_state: "committed".to_string(),
                    committed_units: 1,
                    released_units: 0,
                    usage_fact_count: 1,
                    customer_rating_count: 1,
                    customer_charge_count: 1,
                    hold_state: "settled".to_string(),
                    captured_micros: 7,
                    released_micros: 0,
                    account_held_micros: 0,
                    account_captured_micros: 7,
                },
            format!("inline success settlement was not exact or idempotent: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn inline_v4_failure_releases_customer_hold_once() -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_codex_generation_lease(&database.pool, "inline-v4-failure-worker").await?;
        bind_inline_profile(&database.pool, &work).await?;
        seed_v4_customer_quote(&database.pool, &work, 7).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let reservation = inline_usage_reservation(&database.pool, &work).await?;
        let settlement = PostgresExecutionSettlementStore::new(
            database.pool.clone(),
            Arc::new(InMemoryArtifactBlobStore::default()),
        );

        settlement
            .fail(&work, &reservation, "provider_rejected")
            .await
            .map_err(debug_error)?;
        settlement
            .fail(&work, &reservation, "provider_rejected")
            .await
            .map_err(debug_error)?;

        let state = inline_customer_settlement_state(&database.pool, &work).await?;
        require(
            state
                == InlineCustomerSettlementState {
                    job_state: "failed".to_string(),
                    charged_units: 0,
                    quota_state: "released".to_string(),
                    committed_units: 0,
                    released_units: 1,
                    usage_fact_count: 1,
                    customer_rating_count: 1,
                    customer_charge_count: 0,
                    hold_state: "settled".to_string(),
                    captured_micros: 0,
                    released_micros: 7,
                    account_held_micros: 0,
                    account_captured_micros: 0,
                },
            format!("inline failure settlement was not exact or idempotent: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn inline_v4_reconcile_is_idempotent_for_an_existing_terminal_job() -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work =
            seed_codex_generation_lease(&database.pool, "inline-v4-reconcile-worker").await?;
        bind_inline_profile(&database.pool, &work).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let reservation = inline_usage_reservation(&database.pool, &work).await?;
        let artifacts = Arc::new(InMemoryArtifactBlobStore::default());
        let manifest = inline_generation_manifest(artifacts.as_ref(), &work, &reservation).await?;
        PostgresExecutionSettlementStore::new(database.pool.clone(), artifacts)
            .succeed(&work, &reservation, &manifest)
            .await
            .map_err(debug_error)?;
        seed_v4_customer_quote(&database.pool, &work, 7).await?;

        reconcile_inline_customer_settlement(&database.pool, work.job_id)
            .await
            .map_err(debug_error)?;
        reconcile_inline_customer_settlement(&database.pool, work.job_id)
            .await
            .map_err(debug_error)?;

        let state = inline_customer_settlement_state(&database.pool, &work).await?;
        require(
            state
                == InlineCustomerSettlementState {
                    job_state: "succeeded".to_string(),
                    charged_units: 1,
                    quota_state: "committed".to_string(),
                    committed_units: 1,
                    released_units: 0,
                    usage_fact_count: 1,
                    customer_rating_count: 1,
                    customer_charge_count: 1,
                    hold_state: "settled".to_string(),
                    captured_micros: 7,
                    released_micros: 0,
                    account_held_micros: 0,
                    account_captured_micros: 7,
                },
            format!("inline reconcile was not exact or idempotent: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn terminal_success_completion_commits_every_effect_atomically() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work =
            seed_codex_generation_lease(&database.pool, "terminal-completion-worker").await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        seed_price_hold(&database.pool, &prepared[0], 7).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(&scope, "terminal-completion-executor", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "codex executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&executor_artifacts, &executor_lease).await?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("terminal-completion-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "terminal completion reduction was not queued".to_string())?;
        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer = CustomerArtifactPublisher::new(customer_blobs.clone())
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        let first = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        let replay = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        require(
            first == replay
                && first.parent_state == ExecutorParentTerminalState::Succeeded
                && first.customer_artifact_id == Some(terminal.output_id),
            format!("terminal completion replay changed identity: {first:?} {replay:?}"),
        )?;

        let state: AtomicCompletionState = sqlx::query_as(
            r#"
            SELECT reduction.state AS reduction_state, output.state AS output_state,
                   work.state AS work_state, attempt.state AS attempt_state,
                   quota.committed_units, quota.released_units, job.charged_units,
                   quota.state AS quota_state,
                   (SELECT COUNT(*) FROM provider_receipts WHERE submission_id = $1)
                     AS receipt_count,
                   (SELECT COUNT(*) FROM provider_usage_facts
                      WHERE submission_id = $1
                        AND receipt_id = reduction.provider_receipt_id
                        AND output_id = $2
                        AND job_id = $3
                        AND provider_id = 'openai-codex'
                        AND provider_account_id = $6
                        AND execution_surface = 'provider_cli'
                        AND metric = 'image_output'
                        AND quantity = 1
                        AND unit = 'image'
                        AND quantity_source = 'request_derived'
                        AND confidence = 'exact')
                     AS provider_usage_fact_count,
                   (SELECT COUNT(*) FROM economic_metering_events WHERE output_id = $2)
                     AS economic_meter_count,
                   (SELECT COUNT(*) FROM rated_usage WHERE output_id = $2) AS rating_count,
                   (SELECT COUNT(*) FROM artifacts WHERE artifact_id = $2) AS artifact_count,
                   (SELECT COUNT(*) FROM job_response_projections WHERE job_id = $3)
                     AS projection_count,
                   (SELECT COUNT(*) FROM usage_events WHERE request_id = job.request_id
                      AND outcome = 'charged') AS usage_count,
                   (SELECT COUNT(*) FROM job_events WHERE job_id = $3
                      AND event_type = 'job.succeeded') AS job_event_count,
                   (SELECT COUNT(*) FROM outbox_events WHERE job_id = $3
                      AND event_type = 'job.succeeded') AS outbox_count
            FROM executor_terminal_reductions reduction
            JOIN job_outputs output ON output.output_id = $2
            JOIN work_items work ON work.work_item_id = $4
            JOIN job_attempts attempt ON attempt.execution_id = $5
            JOIN jobs job ON job.job_id = $3
            JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
            WHERE reduction.submission_id = $1
            "#,
        )
        .bind(terminal.submission_id)
        .bind(terminal.output_id)
        .bind(terminal.job_id)
        .bind(terminal.work_item_id)
        .bind(terminal.attempt_execution_id)
        .bind(CODEX_ACCOUNT_ID)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state
                == AtomicCompletionState {
                    reduction_state: "completed".to_string(),
                    output_state: "succeeded".to_string(),
                    work_state: "succeeded".to_string(),
                    attempt_state: "succeeded".to_string(),
                    committed_units: 1,
                    released_units: 0,
                    charged_units: 1,
                    quota_state: "committed".to_string(),
                    receipt_count: 1,
                    provider_usage_fact_count: 1,
                    economic_meter_count: 1,
                    rating_count: 1,
                    artifact_count: 1,
                    projection_count: 1,
                    usage_count: 1,
                    job_event_count: 1,
                    outbox_count: 1,
                },
            format!("terminal completion state is not atomic or exact: {state:?}"),
        )?;
        require(
            sqlx::query(
                r#"
                INSERT INTO artifacts
                  (artifact_id, tenant_id, job_id, work_item_id, execution_id,
                   lease_epoch, output_index, state, storage_backend, object_key,
                   sha256_hex, byte_size, media_type, created_at_ms)
                VALUES ($1, $2, $3, $4, $5, $6, 1, 'ready', 'filesystem-v1',
                        $7, $8, 1, 'image/png', $9)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&terminal.tenant_id)
            .bind(terminal.job_id)
            .bind(terminal.work_item_id)
            .bind(terminal.attempt_execution_id)
            .bind(terminal.attempt_lease_epoch)
            .bind(format!("objects/forged/{}", Uuid::new_v4().simple()))
            .bind("f".repeat(64))
            .bind(database_now(&database.pool).await?)
            .execute(&database.pool)
            .await
            .is_err(),
            "completed parent accepted an extra unlinked customer artifact",
        )?;
        let idempotency_state: String = sqlx::query_scalar(
            "SELECT state FROM idempotency_requests WHERE job_id = $1",
        )
        .bind(terminal.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            idempotency_state == "succeeded",
            format!("terminal completion did not project idempotency: {idempotency_state}"),
        )?;
        require(
            sqlx::query("DELETE FROM idempotency_requests WHERE job_id = $1")
                .bind(terminal.job_id)
                .execute(&database.pool)
                .await
                .is_err(),
            "completed V2 job lost its idempotency binding",
        )?;
        let unrelated = seed_lease(&database.pool, "cross-parent-event-worker", 1).await?;
        require(
            sqlx::query(
                "UPDATE job_events SET job_id = $2 WHERE job_id = $1 AND event_type = 'job.succeeded'",
            )
            .bind(terminal.job_id)
            .bind(unrelated.job_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "canonical terminal event moved away from its completed parent",
        )?;
        require(
            customer_blobs.get(&customer).await.map_err(debug_error)?
                == png_bytes([10, 20, 30, 255]),
            "committed customer artifact bytes changed",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn v4_terminal_success_settles_customer_and_provider_cost_once() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work =
            seed_codex_generation_lease(&database.pool, "v4-terminal-completion-worker").await?;
        seed_v4_customer_quote(&database.pool, &work, 7).await?;
        let provider_actual_version_id = seed_provider_reported_actual_price(
            &database.pool,
            &work,
            V4CustomerQuoteIdentity::openai(),
        )
        .await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        require(prepared.len() == 1, "expected one v4 prepared output")?;
        seed_terminal_quota(&database.pool, &work).await?;

        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(&scope, "v4-terminal-completion-executor", 60_000)
            .await
            .map_err(|error| format!("v4 claim_prepared failed: {error:?}"))?
            .ok_or_else(|| "v4 codex executor claim returned none".to_string())?;
        executor
            .start(&executor_lease)
            .await
            .map_err(|error| format!("v4 executor start failed: {error:?}"))?;
        let provider_operation_id = format!("provider-operation-{}", Uuid::new_v4().simple());
        let provider_cost = ProviderReportedCostEvidenceV1::usd_ticks(
            ProviderCostEvidenceScope::CliInvocation,
            "openai-codex",
            "provider_cli",
            &provider_operation_id,
            200_000_000,
            br#"{"total_cost_usd_ticks":200000000}"#,
            "end.total_cost_usd_ticks",
        )
        .map_err(debug_error)?;
        let manifest = publish_result_authority(&executor_artifacts, &executor_lease)
            .await?
            .with_provider_reported_cost(Some(provider_cost))
            .ok_or("provider cost evidence was rejected")?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(|error| format!("v4 record_outcome failed: {error:?}"))?;
        let evidence_at_ms: i64 = sqlx::query_scalar(
            "SELECT created_at_ms FROM executor_provider_cost_evidence WHERE submission_id = $1",
        )
        .bind(executor_lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require_one(
            sqlx::query(
                r#"
                UPDATE price_book_versions
                SET state = 'retired', effective_until_ms = $2,
                    control_version = control_version + 1, updated_at_ms = $2
                WHERE price_book_version_id = $1
                "#,
            )
            .bind(provider_actual_version_id)
            .bind(evidence_at_ms + 1)
            .execute(&database.pool)
            .await
            .map_err(debug_error)?,
            "provider actual price retirement",
        )?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("v4-terminal-completion-reducer", 60_000)
            .await
            .map_err(|error| format!("v4 claim_terminal failed: {error:?}"))?
            .ok_or_else(|| "v4 terminal completion reduction was not queued".to_string())?;
        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer = CustomerArtifactPublisher::new(customer_blobs)
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        let first = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(|error| format!("v4 complete_terminal failed: {error:?}"))?;
        let replay = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(|error| format!("v4 complete_terminal replay failed: {error:?}"))?;
        require(
            first == replay && first.parent_state == ExecutorParentTerminalState::Succeeded,
            format!("v4 terminal replay changed identity: {first:?} {replay:?}"),
        )?;

        let state = v4_terminal_economic_state(
            &database.pool,
            terminal.submission_id,
            terminal.job_id,
            terminal.output_id,
            "succeeded",
            "image_output",
            1,
            &json!({"quality": "high", "size": "1024x1024"}),
        )
        .await?;
        require(
            state
                == V4TerminalEconomicState {
                    job_state: "succeeded".to_string(),
                    output_state: "succeeded".to_string(),
                    receipt_count: 1,
                    provider_usage_fact_count: 1,
                    legacy_economic_meter_count: 0,
                    legacy_rating_count: 0,
                    legacy_hold_count: 0,
                    legacy_customer_charge_count: 0,
                    customer_rating_count: 1,
                    customer_rating_line_count: 1,
                    customer_fact_link_count: 1,
                    customer_job_charge_count: 1,
                    hold_state: "settled".to_string(),
                    hold_captured_micros: 7,
                    hold_released_micros: 0,
                    account_held_micros: 0,
                    account_captured_micros: 7,
                },
            format!("v4 terminal customer settlement was not exact: {state:?}"),
        )?;
        let (posting_count, posting_sum): (i64, i64) = sqlx::query_as(
            r#"
            SELECT COUNT(posting.posting_no)::BIGINT,
                   COALESCE(SUM(posting.amount_micros), 0)::BIGINT
            FROM ledger_transactions transaction
            JOIN ledger_postings posting
              ON posting.transaction_id = transaction.transaction_id
            WHERE transaction.source_job_id = $1
              AND transaction.transaction_type = 'customer_job_charge'
            "#,
        )
        .bind(terminal.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            posting_count == 2 && posting_sum == 0,
            format!("v4 customer ledger was not balanced: {posting_count}/{posting_sum}"),
        )?;
        let provider_cost_state: (String, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT observation.native_quantity::TEXT,
                   observation.amount_micros,
                   (SELECT COUNT(*)::BIGINT
                    FROM provider_cost_observation_fact_links fact_link
                    WHERE fact_link.provider_cost_observation_id =
                        observation.provider_cost_observation_id),
                   (SELECT COUNT(*)::BIGINT
                    FROM provider_cost_observation_receipts receipt_link
                    WHERE receipt_link.provider_cost_observation_id =
                        observation.provider_cost_observation_id),
                   (SELECT COUNT(*)::BIGINT
                    FROM ledger_transactions ledger
                    WHERE ledger.source_provider_cost_observation_id =
                        observation.provider_cost_observation_id
                      AND ledger.transaction_type = 'provider_cost'),
                   (SELECT COALESCE(SUM(posting.amount_micros), 0)::BIGINT
                    FROM ledger_transactions ledger
                    JOIN ledger_postings posting
                      ON posting.transaction_id = ledger.transaction_id
                    WHERE ledger.source_provider_cost_observation_id =
                        observation.provider_cost_observation_id)
            FROM provider_cost_observations observation
            WHERE observation.provider_id = 'openai-codex'
              AND observation.provider_operation_id = $1
            "#,
        )
        .bind(&provider_operation_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            provider_cost_state == ("200000000".to_string(), 20_000, 1, 1, 1, 0),
            format!("provider-reported cost did not settle exactly once: {provider_cost_state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn v4_terminal_retries_when_provider_actual_price_arrives_after_evidence() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work =
            seed_codex_generation_lease(&database.pool, "v4-provider-price-retry-worker").await?;
        seed_v4_customer_quote(&database.pool, &work, 7).await?;
        let initial_provider_price = seed_provider_reported_actual_price_at(
            &database.pool,
            &work,
            V4CustomerQuoteIdentity::openai(),
            database_now(&database.pool).await? - 1_000,
        )
        .await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        require(prepared.len() == 1, "expected one v4 prepared output")?;
        seed_terminal_quota(&database.pool, &work).await?;

        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(&scope, "v4-provider-price-retry-executor", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "v4 retry executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let provider_cost = ProviderReportedCostEvidenceV1::usd_ticks(
            ProviderCostEvidenceScope::CliInvocation,
            "openai-codex",
            "provider_cli",
            &format!("provider-operation-{}", Uuid::new_v4().simple()),
            200_000_000,
            br#"{"total_cost_usd_ticks":200000000}"#,
            "end.total_cost_usd_ticks",
        )
        .map_err(debug_error)?;
        let manifest = publish_result_authority(&executor_artifacts, &executor_lease)
            .await?
            .with_provider_reported_cost(Some(provider_cost))
            .ok_or("provider cost evidence was rejected")?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(debug_error)?;
        let evidence_at_ms: i64 = sqlx::query_scalar(
            "SELECT created_at_ms FROM executor_provider_cost_evidence WHERE submission_id = $1",
        )
        .bind(executor_lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require_one(
            sqlx::query(
                r#"
                UPDATE price_book_versions
                SET state = 'retired', effective_until_ms = $2,
                    control_version = control_version + 1, updated_at_ms = $2
                WHERE price_book_version_id = $1
                "#,
            )
            .bind(initial_provider_price)
            .bind(evidence_at_ms)
            .execute(&database.pool)
            .await
            .map_err(debug_error)?,
            "provider actual price gap setup",
        )?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("v4-provider-price-retry-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "v4 retry terminal reduction was not queued".to_string())?;
        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer = CustomerArtifactPublisher::new(customer_blobs)
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        let missing_price_result = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await;
        require(
            missing_price_result == Err(ExecutorTerminalError::Unavailable),
            format!(
                "missing provider actual price returned {missing_price_result:?} instead of a retryable error"
            ),
        )?;
        let before_retry: (String, i64, i64) = sqlx::query_as(
            r#"
            SELECT reduction.state,
                   (SELECT COUNT(*) FROM provider_receipts WHERE submission_id = $1),
                   (SELECT COUNT(*) FROM provider_cost_observations
                    WHERE provider_id = 'openai-codex')
            FROM executor_terminal_reductions reduction
            WHERE reduction.submission_id = $1
            "#,
        )
        .bind(terminal.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            before_retry == ("leased".to_string(), 0, 0),
            format!("retryable provider price gap leaked effects: {before_retry:?}"),
        )?;

        seed_provider_reported_actual_price_at(
            &database.pool,
            &work,
            V4CustomerQuoteIdentity::openai(),
            evidence_at_ms,
        )
        .await?;
        let completion = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        require(
            completion.parent_state == ExecutorParentTerminalState::Succeeded,
            "terminal reduction did not complete after provider price publication",
        )?;
        let after_retry: (String, i64, i64) = sqlx::query_as(
            r#"
            SELECT reduction.state,
                   (SELECT COUNT(*) FROM provider_receipts WHERE submission_id = $1),
                   (SELECT COUNT(*) FROM provider_cost_observations
                    WHERE provider_id = 'openai-codex')
            FROM executor_terminal_reductions reduction
            WHERE reduction.submission_id = $1
            "#,
        )
        .bind(terminal.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            after_retry == ("completed".to_string(), 1, 1),
            format!("provider price retry was not exactly once: {after_retry:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn v4_dreamina_terminal_success_settles_the_native_quote_once() -> TestResult {
    verify_v4_dreamina_terminal_success(
        DREAMINA_IMAGES_API_PROFILE,
        V4CustomerQuoteIdentity::dreamina(),
        "dreamina",
    )
    .await
}

#[tokio::test]
async fn v4_ark_terminal_success_preserves_ark_identity_and_dreamina_price() -> TestResult {
    verify_v4_dreamina_terminal_success(
        image_api_contracts::ark::ARK_IMAGES_API_PROFILE,
        V4CustomerQuoteIdentity::ark(),
        "ark",
    )
    .await
}

async fn verify_v4_dreamina_terminal_success(
    api_profile: &str,
    quote_identity: V4CustomerQuoteIdentity,
    owner_suffix: &str,
) -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_dreamina_generation_lease_for_profile(
            &database.pool,
            &format!("v4-{owner_suffix}-terminal-worker"),
            api_profile,
        )
        .await?;
        seed_v4_customer_quote_with_basis(
            &database.pool,
            &work,
            quote_identity.clone(),
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
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, DREAMINA_PROFILE_ID)
            .await
            .map_err(debug_error)?;
        require(prepared.len() == 1, "expected one Dreamina prepared output")?;
        seed_terminal_quota(&database.pool, &work).await?;

        let scope = ExecutorClaimScope {
            execution_profile_id: DREAMINA_PROFILE_ID,
            provider_id: DREAMINA_PROVIDER_ID.to_string(),
            command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.to_string(),
            adapter_revision: DREAMINA_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(
                &scope,
                &format!("v4-{owner_suffix}-terminal-executor"),
                60_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "Dreamina terminal executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&executor_artifacts, &executor_lease).await?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal(&format!("v4-{owner_suffix}-terminal-reducer"), 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "Dreamina terminal reduction was not queued".to_string())?;
        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer = CustomerArtifactPublisher::new(customer_blobs)
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        let first = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        let replay = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        require(
            first == replay && first.parent_state == ExecutorParentTerminalState::Succeeded,
            format!("Dreamina terminal replay changed identity: {first:?} {replay:?}"),
        )?;

        let state = v4_terminal_economic_state(
            &database.pool,
            terminal.submission_id,
            terminal.job_id,
            terminal.output_id,
            "succeeded",
            "image_output",
            1,
            &json!({"resolution_type": "2k", "ratio": "1:1"}),
        )
        .await?;
        require(
            state
                == V4TerminalEconomicState {
                    job_state: "succeeded".to_string(),
                    output_state: "succeeded".to_string(),
                    receipt_count: 1,
                    provider_usage_fact_count: 1,
                    legacy_economic_meter_count: 0,
                    legacy_rating_count: 0,
                    legacy_hold_count: 0,
                    legacy_customer_charge_count: 0,
                    customer_rating_count: 1,
                    customer_rating_line_count: 1,
                    customer_fact_link_count: 1,
                    customer_job_charge_count: 1,
                    hold_state: "settled".to_string(),
                    hold_captured_micros: 7,
                    hold_released_micros: 0,
                    account_held_micros: 0,
                    account_captured_micros: 7,
                },
            format!("Dreamina customer settlement was not exact: {state:?}"),
        )?;
        let identity: (
            String,
            String,
            String,
            String,
            serde_json::Value,
            String,
            Uuid,
            String,
            String,
        ) = sqlx::query_as(
            r#"
            SELECT quote.api_profile, version.api_profile,
                   quote.provider_model_id, quote.public_model_id,
                   quote.request_dimensions_json,
                   fact.provider_id, fact.provider_account_id,
                   projection.api_profile, projection.size
            FROM customer_price_quotes quote
            JOIN price_book_versions version
              ON version.price_book_version_id = quote.price_book_version_id
            JOIN provider_usage_facts fact ON fact.job_id = quote.job_id
            JOIN job_response_projections projection ON projection.job_id = quote.job_id
            WHERE quote.job_id = $1
              AND fact.submission_id = $2
              AND fact.metric = 'image_output'
            "#,
        )
        .bind(terminal.job_id)
        .bind(terminal.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            identity.0 == quote_identity.quote_api_profile
                && identity.1 == quote_identity.price_api_profile
                && identity.2 == quote_identity.provider_model_id
                && identity.3 == quote_identity.public_model_id
                && identity.4 == quote_identity.dimensions
                && identity.5 == DREAMINA_PROVIDER_ID
                && identity.6 == DREAMINA_ACCOUNT_ID
                && identity.7 == api_profile
                && identity.8 == "2k:1:1",
            format!("{owner_suffix} terminal identity drifted: {identity:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn v4_dreamina_video_terminal_success_settles_output_seconds_once() -> TestResult {
    verify_v4_dreamina_video_terminal_success(
        DREAMINA_VIDEOS_API_PROFILE,
        V4CustomerQuoteIdentity::dreamina_video(),
        "dreamina-video",
    )
    .await
}

#[tokio::test]
async fn v4_ark_video_terminal_success_preserves_ark_identity_and_dreamina_rate() -> TestResult {
    verify_v4_dreamina_video_terminal_success(
        image_api_contracts::ark::ARK_CONTENT_GENERATION_API_PROFILE,
        V4CustomerQuoteIdentity::ark_video(),
        "ark-video",
    )
    .await
}

async fn verify_v4_dreamina_video_terminal_success(
    api_profile: &str,
    quote_identity: V4CustomerQuoteIdentity,
    owner_suffix: &str,
) -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_dreamina_video_generation_lease_for_profile(
            &database.pool,
            &format!("v4-{owner_suffix}-terminal-worker"),
            api_profile,
        )
        .await?;
        seed_v4_customer_quote_with_basis(
            &database.pool,
            &work,
            quote_identity.clone(),
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
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, DREAMINA_VIDEO_PROFILE_ID)
            .await
            .map_err(debug_error)?;
        require(
            prepared.len() == 1,
            "expected one Dreamina video prepared output",
        )?;
        seed_terminal_quota(&database.pool, &work).await?;

        let scope = ExecutorClaimScope {
            execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
            provider_id: DREAMINA_PROVIDER_ID.to_string(),
            command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.to_string(),
            adapter_revision: DREAMINA_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(
                &scope,
                &format!("v4-{owner_suffix}-terminal-executor"),
                60_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "Dreamina video terminal executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let manifest = publish_video_result_authority(&executor_artifacts, &executor_lease).await?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal(&format!("v4-{owner_suffix}-terminal-reducer"), 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "Dreamina video terminal reduction was not queued".to_string())?;
        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer = CustomerArtifactPublisher::new(customer_blobs)
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        let first = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        let replay = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        require(
            first == replay && first.parent_state == ExecutorParentTerminalState::Succeeded,
            format!("Dreamina video terminal replay changed identity: {first:?} {replay:?}"),
        )?;

        let state = v4_terminal_economic_state(
            &database.pool,
            terminal.submission_id,
            terminal.job_id,
            terminal.output_id,
            "succeeded",
            "video_requested_second",
            8,
            &quote_identity.dimensions,
        )
        .await?;
        require(
            state
                == V4TerminalEconomicState {
                    job_state: "succeeded".to_string(),
                    output_state: "succeeded".to_string(),
                    receipt_count: 1,
                    provider_usage_fact_count: 1,
                    legacy_economic_meter_count: 0,
                    legacy_rating_count: 0,
                    legacy_hold_count: 0,
                    legacy_customer_charge_count: 0,
                    customer_rating_count: 1,
                    customer_rating_line_count: 1,
                    customer_fact_link_count: 1,
                    customer_job_charge_count: 1,
                    hold_state: "settled".to_string(),
                    hold_captured_micros: 24,
                    hold_released_micros: 0,
                    account_held_micros: 0,
                    account_captured_micros: 24,
                },
            format!("Dreamina video customer settlement was not exact: {state:?}"),
        )?;

        let identity: serde_json::Value = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'quote_api_profile', quote.api_profile,
                'price_api_profile', version.api_profile,
                'provider_model_id', quote.provider_model_id,
                'public_model_id', quote.public_model_id,
                'dimensions', quote.request_dimensions_json,
                'source_kind', version.source_kind,
                'source_url', version.source_url,
                'provider_id', fact.provider_id,
                'provider_account_id', fact.provider_account_id::TEXT,
                'metric', fact.metric,
                'unit', fact.unit,
                'quantity', fact.quantity,
                'projection_api_profile', projection.api_profile,
                'projection_operation', projection.operation,
                'output_format', projection.output_format,
                'size', projection.size,
                'media_type', authority.media_type,
                'media_duration_ms', authority.media_duration_ms
            )
            FROM customer_price_quotes quote
            JOIN price_book_versions version
              ON version.price_book_version_id = quote.price_book_version_id
            JOIN provider_usage_facts fact ON fact.job_id = quote.job_id
            JOIN job_response_projections projection ON projection.job_id = quote.job_id
            JOIN executor_artifact_authorities authority
              ON authority.submission_id = fact.submission_id
            WHERE quote.job_id = $1
              AND fact.submission_id = $2
              AND fact.metric = 'video_requested_second'
            "#,
        )
        .bind(terminal.job_id)
        .bind(terminal.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let expected_identity = json!({
            "quote_api_profile": quote_identity.quote_api_profile,
            "price_api_profile": quote_identity.price_api_profile,
            "provider_model_id": quote_identity.provider_model_id,
            "public_model_id": quote_identity.public_model_id,
            "dimensions": quote_identity.dimensions,
            "source_kind": "manual",
            "source_url": null,
            "provider_id": DREAMINA_PROVIDER_ID,
            "provider_account_id": DREAMINA_ACCOUNT_ID.to_string(),
            "metric": "video_requested_second",
            "unit": "second",
            "quantity": 8,
            "projection_api_profile": api_profile,
            "projection_operation": VIDEO_GENERATION_OPERATION,
            "output_format": "mp4",
            "size": "720p",
            "media_type": "video/mp4",
            "media_duration_ms": 8000,
        });
        require(
            identity == expected_identity,
            format!("{owner_suffix} video terminal identity drifted: {identity}"),
        )?;

        let inspected_usage: serde_json::Value = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'quantity', fact.quantity,
                'unit', fact.unit,
                'quantity_source', fact.quantity_source,
                'confidence', fact.confidence,
                'evidence_path', fact.evidence_path,
                'partition_key', fact.billing_partition_key,
                'media_duration_ms', fact.metadata_json -> 'media_duration_ms',
                'duration_rounding', fact.metadata_json -> 'duration_rounding'
            )
            FROM provider_usage_facts fact
            WHERE fact.job_id = $1
              AND fact.submission_id = $2
              AND fact.metric = 'video_output_second'
              AND fact.quantity_source = 'media_inspected'
            "#,
        )
        .bind(terminal.job_id)
        .bind(terminal.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            inspected_usage
                == json!({
                    "quantity": 8,
                    "unit": "second",
                    "quantity_source": "media_inspected",
                    "confidence": "exact",
                    "evidence_path": "executor_artifact_authorities.media_duration_ms",
                    "partition_key": format!("provider-output:{}", terminal.output_id),
                    "media_duration_ms": 8000,
                    "duration_rounding": "ceil_to_second",
                }),
            format!("video media evidence did not produce exact usage: {inspected_usage}"),
        )?;

        let (posting_count, posting_sum): (i64, i64) = sqlx::query_as(
            r#"
            SELECT COUNT(posting.posting_no)::BIGINT,
                   COALESCE(SUM(posting.amount_micros), 0)::BIGINT
            FROM ledger_transactions transaction
            JOIN ledger_postings posting
              ON posting.transaction_id = transaction.transaction_id
            WHERE transaction.source_job_id = $1
              AND transaction.transaction_type = 'customer_job_charge'
            "#,
        )
        .bind(terminal.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            posting_count == 2 && posting_sum == 0,
            format!(
                "Dreamina video customer ledger was not balanced: {posting_count}/{posting_sum}"
            ),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn v4_terminal_success_rates_the_frozen_official_token_quantity() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work =
            seed_codex_generation_lease(&database.pool, "v4-token-completion-worker").await?;
        seed_v4_customer_token_quote(&database.pool, &work).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        require(prepared.len() == 1, "expected one token-priced output")?;
        seed_terminal_quota(&database.pool, &work).await?;

        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(&scope, "v4-token-completion-executor", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "token-priced executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&executor_artifacts, &executor_lease).await?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("v4-token-completion-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "token-priced terminal reduction was not queued".to_string())?;
        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer = CustomerArtifactPublisher::new(customer_blobs)
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        let first = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        let replay = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        require(
            first == replay && first.parent_state == ExecutorParentTerminalState::Succeeded,
            format!("token-priced terminal replay changed identity: {first:?} {replay:?}"),
        )?;

        let state: (
            i64,
            String,
            String,
            String,
            i64,
            i64,
            i64,
            i64,
            String,
        ) = sqlx::query_as(
            r#"
            SELECT fact.quantity, fact.quantity_source, fact.confidence,
                   fact.evidence_path,
                   line.actual_quantity, line.amount_micros,
                   hold.captured_micros, account.captured_micros,
                   hold.state
            FROM provider_usage_facts fact
            JOIN customer_rated_usage_fact_links link
              ON link.usage_fact_id = fact.usage_fact_id
            JOIN customer_rated_usage_lines line
              ON line.rated_usage_line_id = link.rated_usage_line_id
            JOIN customer_rated_usage rating
              ON rating.rated_usage_id = line.rated_usage_id
            JOIN customer_billing_holds hold
              ON hold.job_id = rating.job_id
            JOIN billing_accounts account
              ON account.tenant_id = hold.tenant_id
             AND account.currency = hold.currency
            WHERE fact.submission_id = $1
              AND fact.metric = 'image_output_token'
              AND fact.unit = 'token'
              AND fact.terminal_outcome = 'succeeded'
            "#,
        )
        .bind(terminal.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state
                == (
                    7_024,
                    "official_lookup".to_string(),
                    "estimated".to_string(),
                    "https://developers.openai.com/api/docs/guides/image-generation#gpt-image-2-output-tokens".to_string(),
                    7_024,
                    210_720,
                    210_720,
                    210_720,
                    "settled".to_string(),
                ),
            format!("official token quantity did not settle exactly: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn v4_terminal_rejects_a_conflicting_usage_fact_without_partial_settlement() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_codex_generation_lease(&database.pool, "v4-fact-conflict-worker").await?;
        seed_v4_customer_quote(&database.pool, &work, 7).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        seed_terminal_quota(&database.pool, &work).await?;

        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(&scope, "v4-fact-conflict-executor", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "fact-conflict executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&executor_artifacts, &executor_lease).await?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("v4-fact-conflict-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "fact-conflict terminal reduction was not queued".to_string())?;
        sqlx::query(
            r#"
            CREATE FUNCTION inject_conflicting_v4_usage_fact() RETURNS TRIGGER AS $$
            DECLARE
                account_id UUID;
            BEGIN
                SELECT provider_account_id INTO account_id
                FROM provider_submissions
                WHERE submission_id = NEW.submission_id;

                INSERT INTO provider_usage_facts (
                    usage_fact_id, semantic_key, job_id, output_id, submission_id,
                    receipt_id, provider_id, provider_account_id, execution_surface,
                    fact_domain, metric, quantity, unit, quantity_source, confidence, evidence_path,
                    metadata_json, billing_partition_key, terminal_outcome, created_at_ms
                )
                VALUES (
                    NEW.receipt_id,
                    NEW.receipt_id::TEXT || ':image_output:image:request_derived:v1',
                    NEW.job_id, NEW.output_id, NEW.submission_id, NEW.receipt_id,
                    NEW.provider_id, account_id, 'provider_cli',
                    'customer_billable', 'image_output', 2, 'image',
                    'request_derived', 'exact',
                    'job_outputs.billable_units',
                    '{"quality":"high","size":"1024x1024","basis":"forged"}'::JSONB,
                    'output:' || NEW.output_id::TEXT, NEW.outcome, NEW.created_at_ms
                );
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            CREATE TRIGGER inject_conflicting_v4_usage_fact
            AFTER INSERT ON provider_receipts
            FOR EACH ROW EXECUTE FUNCTION inject_conflicting_v4_usage_fact()
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer = CustomerArtifactPublisher::new(customer_blobs)
            .publish(&terminal)
            .await
            .map_err(debug_error)?;
        require(
            matches!(
                reductions
                    .complete_terminal(&terminal, Some(&customer))
                    .await,
                Err(ExecutorTerminalError::Conflict)
            ),
            "terminal settlement accepted a conflicting immutable usage fact",
        )?;

        let state: (String, String, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT reduction.state, output.state,
                   (SELECT COUNT(*) FROM provider_receipts WHERE submission_id = $1),
                   (SELECT COUNT(*) FROM provider_usage_facts WHERE submission_id = $1),
                   (SELECT COUNT(*) FROM customer_rated_usage WHERE job_id = $2),
                   (SELECT COUNT(*) FROM ledger_transactions
                      WHERE source_job_id = $2
                        AND transaction_type = 'customer_job_charge'),
                   hold.captured_micros, hold.released_micros
            FROM executor_terminal_reductions reduction
            JOIN provider_submissions submission
              ON submission.submission_id = reduction.submission_id
            JOIN job_outputs output ON output.output_id = submission.output_id
            JOIN customer_billing_holds hold ON hold.job_id = submission.job_id
            WHERE reduction.submission_id = $1
            "#,
        )
        .bind(terminal.submission_id)
        .bind(terminal.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state
                == (
                    "leased".to_string(),
                    "pending".to_string(),
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ),
            format!("conflicting usage fact left partial settlement state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn v4_terminal_failure_releases_the_official_token_hold_without_a_charge() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_codex_generation_lease(&database.pool, "v4-token-failure-worker").await?;
        seed_v4_customer_token_quote(&database.pool, &work).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        require(prepared.len() == 1, "expected one token-priced output")?;
        seed_terminal_quota(&database.pool, &work).await?;

        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(&scope, "v4-token-failure-executor", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "token-priced failed executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Failed {
                    error_code: "provider_failed".to_string(),
                },
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("v4-token-failure-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "token-priced failed reduction was not queued".to_string())?;
        let first = reductions
            .complete_terminal(&terminal, None)
            .await
            .map_err(debug_error)?;
        let replay = reductions
            .complete_terminal(&terminal, None)
            .await
            .map_err(debug_error)?;
        require(
            first == replay && first.parent_state == ExecutorParentTerminalState::Failed,
            format!("token-priced failed replay changed identity: {first:?} {replay:?}"),
        )?;

        let state: (i64, i64, i64, i64, i64, String) = sqlx::query_as(
            r#"
            SELECT fact.quantity, line.actual_quantity, line.amount_micros,
                   hold.captured_micros, hold.released_micros, hold.state
            FROM provider_usage_facts fact
            JOIN customer_rated_usage_fact_links link
              ON link.usage_fact_id = fact.usage_fact_id
            JOIN customer_rated_usage_lines line
              ON line.rated_usage_line_id = link.rated_usage_line_id
            JOIN customer_rated_usage rating
              ON rating.rated_usage_id = line.rated_usage_id
            JOIN customer_billing_holds hold
              ON hold.job_id = rating.job_id
            WHERE fact.submission_id = $1
              AND fact.metric = 'image_output_token'
              AND fact.unit = 'token'
              AND fact.terminal_outcome = 'failed'
              AND fact.quantity_source = 'official_lookup'
              AND fact.confidence = 'estimated'
            "#,
        )
        .bind(terminal.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state == (0, 0, 0, 0, 210_720, "settled".to_string()),
            format!("failed token-priced output was charged or remained held: {state:?}"),
        )?;
        let charge_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM ledger_transactions
             WHERE source_job_id = $1 AND transaction_type = 'customer_job_charge'",
        )
        .bind(terminal.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            charge_count == 0,
            format!("failed token-priced output created {charge_count} customer charges"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn v4_terminal_multi_output_rates_every_token_partition_once() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work =
            seed_codex_generation_lease_with_outputs(&database.pool, "v4-token-multi-worker", 2)
                .await?;
        seed_v4_customer_token_quote(&database.pool, &work).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        require(prepared.len() == 2, "expected two token-priced outputs")?;
        seed_terminal_quota(&database.pool, &work).await?;

        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer_publisher = CustomerArtifactPublisher::new(customer_blobs);
        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        for output_index in 0..2 {
            let executor_lease = executor
                .claim_prepared(
                    &scope,
                    &format!("v4-token-multi-executor-{output_index}"),
                    60_000,
                )
                .await
                .map_err(debug_error)?
                .ok_or_else(|| format!("token output {output_index} was not claimable"))?;
            executor.start(&executor_lease).await.map_err(debug_error)?;
            let manifest = publish_result_authority(&executor_artifacts, &executor_lease).await?;
            executor
                .record_outcome(
                    &executor_lease,
                    &ExecutorSubmissionOutcome::Succeeded(manifest),
                )
                .await
                .map_err(debug_error)?;
            let terminal = reductions
                .claim_terminal(&format!("v4-token-multi-reducer-{output_index}"), 60_000)
                .await
                .map_err(debug_error)?
                .ok_or_else(|| format!("token output {output_index} reduction was not queued"))?;
            let customer = customer_publisher
                .publish(&terminal)
                .await
                .map_err(debug_error)?;
            let completion = reductions
                .complete_terminal(&terminal, Some(&customer))
                .await
                .map_err(debug_error)?;
            let expected_parent = if output_index == 0 {
                ExecutorParentTerminalState::Pending
            } else {
                ExecutorParentTerminalState::Succeeded
            };
            require(
                completion.parent_state == expected_parent,
                format!("token output {output_index} settled parent too early: {completion:?}"),
            )?;
        }

        let state: (i64, i64, i64, i64, i64, i64, i64, String) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_usage_facts
                 WHERE job_id = $1
                   AND metric = 'image_output_token'
                   AND quantity = 7024
                   AND quantity_source = 'official_lookup'
                   AND confidence = 'estimated'),
              (SELECT COUNT(DISTINCT billing_partition_key)
                 FROM provider_usage_facts
                 WHERE job_id = $1 AND metric = 'image_output_token'),
              (SELECT COUNT(*) FROM customer_rated_usage WHERE job_id = $1),
              (SELECT COUNT(*) FROM customer_rated_usage_lines line
                 JOIN customer_rated_usage rating
                   ON rating.rated_usage_id = line.rated_usage_id
                 WHERE rating.job_id = $1),
              (SELECT COUNT(*) FROM customer_rated_usage_fact_links link
                 JOIN customer_rated_usage_lines line
                   ON line.rated_usage_line_id = link.rated_usage_line_id
                 JOIN customer_rated_usage rating
                   ON rating.rated_usage_id = line.rated_usage_id
                 WHERE rating.job_id = $1),
              (SELECT COUNT(*) FROM ledger_transactions
                 WHERE source_job_id = $1
                   AND transaction_type = 'customer_job_charge'),
              hold.captured_micros,
              hold.state
            FROM customer_billing_holds hold
            WHERE hold.job_id = $1
            "#,
        )
        .bind(work.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state == (2, 2, 1, 2, 2, 1, 421_440, "settled".to_string()),
            format!("multi-output token settlement lost partition identity: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn v4_terminal_failure_releases_the_customer_hold_without_a_charge() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work =
            seed_codex_generation_lease(&database.pool, "v4-terminal-failure-worker").await?;
        seed_v4_customer_quote(&database.pool, &work, 7).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        require(
            prepared.len() == 1,
            "expected one failed v4 prepared output",
        )?;
        seed_terminal_quota(&database.pool, &work).await?;

        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(&scope, "v4-terminal-failure-executor", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "failed v4 codex executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Failed {
                    error_code: "provider_failed".to_string(),
                },
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("v4-terminal-failure-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "failed v4 terminal reduction was not queued".to_string())?;
        let first = reductions
            .complete_terminal(&terminal, None)
            .await
            .map_err(debug_error)?;
        let replay = reductions
            .complete_terminal(&terminal, None)
            .await
            .map_err(debug_error)?;
        require(
            first == replay && first.parent_state == ExecutorParentTerminalState::Failed,
            format!("failed v4 terminal replay changed identity: {first:?} {replay:?}"),
        )?;

        let state = v4_terminal_economic_state(
            &database.pool,
            terminal.submission_id,
            terminal.job_id,
            terminal.output_id,
            "failed",
            "image_output",
            1,
            &json!({"quality": "high", "size": "1024x1024"}),
        )
        .await?;
        require(
            state
                == V4TerminalEconomicState {
                    job_state: "failed".to_string(),
                    output_state: "failed".to_string(),
                    receipt_count: 1,
                    provider_usage_fact_count: 1,
                    legacy_economic_meter_count: 0,
                    legacy_rating_count: 0,
                    legacy_hold_count: 0,
                    legacy_customer_charge_count: 0,
                    customer_rating_count: 1,
                    customer_rating_line_count: 1,
                    customer_fact_link_count: 1,
                    customer_job_charge_count: 0,
                    hold_state: "settled".to_string(),
                    hold_captured_micros: 0,
                    hold_released_micros: 7,
                    account_held_micros: 0,
                    account_captured_micros: 0,
                },
            format!("failed v4 terminal customer settlement was not exact: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_duplicate_completion_has_one_durable_result() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_codex_generation_lease(&database.pool, "duplicate-terminal-worker").await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        seed_price_hold(&database.pool, &prepared[0], 7).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(&scope, "duplicate-terminal-executor", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "duplicate terminal executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&executor_artifacts, &executor_lease).await?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("duplicate-terminal-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "duplicate terminal reduction was not queued".to_string())?;
        let customer = CustomerArtifactPublisher::new(Arc::new(
            FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?,
        ))
        .publish(&terminal)
        .await
        .map_err(debug_error)?;
        let barrier = Arc::new(tokio::sync::Barrier::new(12));
        let mut tasks = Vec::new();
        for _ in 0..12 {
            let store = reductions.clone();
            let lease = terminal.clone();
            let artifact = customer.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                store.complete_terminal(&lease, Some(&artifact)).await
            }));
        }
        let completions = tokio::time::timeout(Duration::from_secs(10), async {
            let mut completions = Vec::new();
            for task in tasks {
                completions.push(task.await.map_err(debug_error)?.map_err(debug_error)?);
            }
            Ok::<_, String>(completions)
        })
        .await
        .map_err(|_| "duplicate terminal completions deadlocked".to_string())??;
        let expected = completions
            .first()
            .ok_or_else(|| "duplicate terminal completion returned no result".to_string())?;
        require(
            completions.iter().all(|completion| completion == expected)
                && expected.parent_state == ExecutorParentTerminalState::Succeeded,
            format!("duplicate completion results diverged: {completions:?}"),
        )?;
        let counts: (i64, i64, i64, i64, i64, i64, i32) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_receipts WHERE submission_id = $1),
              (SELECT COUNT(*) FROM economic_metering_events WHERE output_id = $2),
              (SELECT COUNT(*) FROM rated_usage WHERE output_id = $2),
              (SELECT COUNT(*) FROM artifacts WHERE artifact_id = $2),
              (SELECT COUNT(*) FROM job_response_projections WHERE job_id = $3),
              (SELECT COUNT(*) FROM outbox_events WHERE job_id = $3
                 AND event_type = 'job.succeeded'),
              (SELECT committed_units FROM quota_reservations WHERE job_id = $3)
            "#,
        )
        .bind(terminal.submission_id)
        .bind(terminal.output_id)
        .bind(terminal.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1, 1, 1, 1, 1, 1),
            format!("duplicate completion persisted duplicate effects: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn deferred_commit_failure_rolls_back_every_terminal_effect() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_codex_generation_lease(&database.pool, "terminal-rollback-worker").await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        seed_price_hold(&database.pool, &prepared[0], 7).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(&scope, "terminal-rollback-executor", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "rollback executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&executor_artifacts, &executor_lease).await?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(debug_error)?;
        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let terminal = reductions
            .claim_terminal("terminal-rollback-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "rollback terminal reduction was not queued".to_string())?;
        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer = CustomerArtifactPublisher::new(customer_blobs.clone())
            .publish(&terminal)
            .await
            .map_err(debug_error)?;

        sqlx::query(
            r#"
            CREATE FUNCTION reject_terminal_completion_commit() RETURNS TRIGGER AS $$
            BEGIN
                RAISE EXCEPTION 'injected terminal completion commit failure';
            END;
            $$ LANGUAGE plpgsql
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            CREATE CONSTRAINT TRIGGER reject_terminal_completion_at_commit
            AFTER UPDATE ON executor_terminal_reductions
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW WHEN (NEW.state = 'completed')
            EXECUTE FUNCTION reject_terminal_completion_commit()
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            CREATE FUNCTION erase_terminal_receipt_evidence() RETURNS TRIGGER AS $$
            BEGIN
                NEW.evidence = '{}'::jsonb;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            CREATE TRIGGER erase_terminal_receipt_evidence
            BEFORE INSERT ON provider_receipts
            FOR EACH ROW EXECUTE FUNCTION erase_terminal_receipt_evidence()
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        require(
            matches!(
                reductions
                    .complete_terminal(&terminal, Some(&customer))
                    .await,
                Err(ExecutorTerminalError::Unavailable)
            ),
            "deferred commit injection did not fail the outer completion transaction",
        )?;
        let rolled_back: (String, String, String, i32, i32, i64, i64, i64, i64) = sqlx::query_as(
            r#"
                SELECT reduction.state, output.state, work.state,
                       quota.committed_units, quota.released_units,
                       (SELECT COUNT(*) FROM provider_receipts WHERE submission_id = $1),
                       (SELECT COUNT(*) FROM rated_usage WHERE output_id = $2),
                       (SELECT COUNT(*) FROM artifacts WHERE artifact_id = $2),
                       (SELECT COUNT(*) FROM job_response_projections WHERE job_id = $3)
                FROM executor_terminal_reductions reduction
                JOIN job_outputs output ON output.output_id = $2
                JOIN work_items work ON work.work_item_id = $4
                JOIN jobs job ON job.job_id = $3
                JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
                WHERE reduction.submission_id = $1
                "#,
        )
        .bind(terminal.submission_id)
        .bind(terminal.output_id)
        .bind(terminal.job_id)
        .bind(terminal.work_item_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            rolled_back
                == (
                    "leased".to_string(),
                    "pending".to_string(),
                    "awaiting_executor".to_string(),
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ),
            format!("deferred failure leaked terminal effects: {rolled_back:?}"),
        )?;
        require(
            customer_blobs.get(&customer).await.map_err(debug_error)?
                == png_bytes([10, 20, 30, 255]),
            "rollback removed the deterministic customer blob",
        )?;

        sqlx::query(
            "DROP TRIGGER reject_terminal_completion_at_commit ON executor_terminal_reductions",
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query("DROP FUNCTION reject_terminal_completion_commit()")
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        require(
            matches!(
                reductions
                    .complete_terminal(&terminal, Some(&customer))
                    .await,
                Err(ExecutorTerminalError::Unavailable)
            ),
            "missing canonical receipt evidence bypassed terminal completion constraints",
        )?;
        let receipt_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_receipts WHERE submission_id = $1")
                .bind(terminal.submission_id)
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        require(
            receipt_count == 0,
            format!("invalid receipt evidence leaked {receipt_count} receipt rows"),
        )?;

        sqlx::query("DROP TRIGGER erase_terminal_receipt_evidence ON provider_receipts")
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        sqlx::query("DROP FUNCTION erase_terminal_receipt_evidence()")
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        let retried = reductions
            .complete_terminal(&terminal, Some(&customer))
            .await
            .map_err(debug_error)?;
        require(
            retried.parent_state == ExecutorParentTerminalState::Succeeded,
            "exact retry did not complete after the injected commit failure",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn multi_output_completion_does_not_finalize_parent_early() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work =
            seed_codex_generation_lease_with_outputs(&database.pool, "multi-terminal-worker", 2)
                .await?;

        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        seed_terminal_economics(&database.pool, &prepared, 7, 3, 0).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        for owner in ["multi-executor-0", "multi-executor-1"] {
            let executor_lease = executor
                .claim_prepared(&scope, owner, 60_000)
                .await
                .map_err(debug_error)?
                .ok_or_else(|| format!("{owner} claim returned none"))?;
            executor.start(&executor_lease).await.map_err(debug_error)?;
            let manifest = publish_result_authority(&executor_artifacts, &executor_lease).await?;
            executor
                .record_outcome(
                    &executor_lease,
                    &ExecutorSubmissionOutcome::Succeeded(manifest),
                )
                .await
                .map_err(debug_error)?;
        }

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer_publisher = CustomerArtifactPublisher::new(customer_blobs);
        let first = reductions
            .claim_terminal("multi-reducer-0", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "first multi-output reduction was not queued".to_string())?;
        let first_artifact = customer_publisher
            .publish(&first)
            .await
            .map_err(debug_error)?;
        let first_completion = reductions
            .complete_terminal(&first, Some(&first_artifact))
            .await
            .map_err(debug_error)?;
        require(
            first_completion.parent_state == ExecutorParentTerminalState::Pending,
            "first output finalized the parent early",
        )?;
        let intermediate: (String, String, i32, i32, i64) = sqlx::query_as(
            r#"
            SELECT work.state, quota.state, quota.committed_units, quota.released_units,
                   (SELECT COUNT(*) FROM job_response_projections WHERE job_id = $1)
            FROM work_items work
            JOIN jobs job ON job.job_id = work.job_id
            JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
            WHERE work.job_id = $1
            "#,
        )
        .bind(work.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            intermediate
                == (
                    "awaiting_executor".to_string(),
                    "reserved".to_string(),
                    1,
                    0,
                    0,
                ),
            format!("first output leaked parent completion: {intermediate:?}"),
        )?;

        let second = reductions
            .claim_terminal("multi-reducer-1", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "second multi-output reduction was not queued".to_string())?;
        let second_artifact = customer_publisher
            .publish(&second)
            .await
            .map_err(debug_error)?;
        let second_completion = reductions
            .complete_terminal(&second, Some(&second_artifact))
            .await
            .map_err(debug_error)?;
        require(
            second_completion.parent_state == ExecutorParentTerminalState::Succeeded,
            "last output did not finalize the successful parent",
        )?;
        let final_state: (String, String, i32, i32, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT work.state, quota.state, quota.committed_units, quota.released_units,
                   (SELECT COUNT(*) FROM artifacts WHERE job_id = $1),
                   (SELECT COUNT(*) FROM provider_receipts receipt
                      JOIN provider_submissions submission
                        ON submission.submission_id = receipt.submission_id
                      WHERE submission.job_id = $1),
                   (SELECT COUNT(*) FROM job_response_projections WHERE job_id = $1)
            FROM work_items work
            JOIN jobs job ON job.job_id = work.job_id
            JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
            WHERE work.job_id = $1
            "#,
        )
        .bind(work.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            final_state
                == (
                    "succeeded".to_string(),
                    "committed".to_string(),
                    2,
                    0,
                    2,
                    2,
                    1,
                ),
            format!("multi-output final state is incomplete: {final_state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_multi_output_completion_finalizes_parent_once() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work =
            seed_codex_generation_lease_with_outputs(&database.pool, "concurrent-parent-worker", 3)
                .await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        seed_terminal_economics(&database.pool, &prepared, 7, 3, 0).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        for index in 0..3 {
            let lease = executor
                .claim_prepared(
                    &scope,
                    &format!("concurrent-parent-executor-{index}"),
                    60_000,
                )
                .await
                .map_err(debug_error)?
                .ok_or_else(|| format!("concurrent parent output {index} was not claimable"))?;
            executor.start(&lease).await.map_err(debug_error)?;
            let manifest = publish_result_authority(&executor_artifacts, &lease).await?;
            executor
                .record_outcome(&lease, &ExecutorSubmissionOutcome::Succeeded(manifest))
                .await
                .map_err(debug_error)?;
        }

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let customer_publisher = CustomerArtifactPublisher::new(Arc::new(
            FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?,
        ));
        let mut terminal_inputs = Vec::new();
        for index in 0..3 {
            let terminal = reductions
                .claim_terminal(&format!("concurrent-parent-reducer-{index}"), 60_000)
                .await
                .map_err(debug_error)?
                .ok_or_else(|| format!("concurrent parent reduction {index} was not queued"))?;
            let artifact = customer_publisher
                .publish(&terminal)
                .await
                .map_err(debug_error)?;
            terminal_inputs.push((terminal, artifact));
        }
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();
        for (terminal, artifact) in terminal_inputs {
            let store = reductions.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                store.complete_terminal(&terminal, Some(&artifact)).await
            }));
        }
        let completions = tokio::time::timeout(Duration::from_secs(10), async {
            let mut completions = Vec::new();
            for task in tasks {
                completions.push(task.await.map_err(debug_error)?.map_err(debug_error)?);
            }
            Ok::<_, String>(completions)
        })
        .await
        .map_err(|_| "concurrent output completion deadlocked".to_string())??;
        require(
            completions
                .iter()
                .filter(|completion| {
                    completion.parent_state == ExecutorParentTerminalState::Succeeded
                })
                .count()
                == 1
                && completions
                    .iter()
                    .filter(|completion| {
                        completion.parent_state == ExecutorParentTerminalState::Pending
                    })
                    .count()
                    == 2,
            format!("concurrent parent completion ownership diverged: {completions:?}"),
        )?;
        let state: (String, String, i32, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT work.state, quota.state, quota.committed_units,
                   (SELECT COUNT(*) FROM provider_receipts receipt
                      JOIN provider_submissions submission
                        ON submission.submission_id = receipt.submission_id
                      WHERE submission.job_id = $1),
                   (SELECT COUNT(*) FROM rated_usage WHERE job_id = $1),
                   (SELECT COUNT(*) FROM artifacts WHERE job_id = $1),
                   (SELECT COUNT(*) FROM job_response_projections WHERE job_id = $1),
                   (SELECT COUNT(*) FROM outbox_events WHERE job_id = $1
                      AND event_type = 'job.succeeded')
            FROM work_items work
            JOIN jobs job ON job.job_id = work.job_id
            JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
            WHERE work.job_id = $1
            "#,
        )
        .bind(work.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state
                == (
                    "succeeded".to_string(),
                    "committed".to_string(),
                    3,
                    3,
                    3,
                    3,
                    1,
                    1,
                ),
            format!("concurrent parent completion was not exactly once: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn partial_failure_settles_outputs_and_fails_parent() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work =
            seed_codex_generation_lease_with_outputs(&database.pool, "partial-failure-worker", 3)
                .await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        seed_terminal_economics(&database.pool, &prepared, 7, 3, 0).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        for index in 0..3 {
            let lease = executor
                .claim_prepared(&scope, &format!("partial-executor-{index}"), 60_000)
                .await
                .map_err(debug_error)?
                .ok_or_else(|| format!("partial output {index} was not claimable"))?;
            executor.start(&lease).await.map_err(debug_error)?;
            let outcome = if lease.output_index == 1 {
                ExecutorSubmissionOutcome::Failed {
                    error_code: "provider_failed".to_string(),
                }
            } else {
                ExecutorSubmissionOutcome::Succeeded(
                    publish_result_authority(&executor_artifacts, &lease).await?,
                )
            };
            executor
                .record_outcome(&lease, &outcome)
                .await
                .map_err(debug_error)?;
        }

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let customer_publisher = CustomerArtifactPublisher::new(Arc::new(
            FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?,
        ));
        for index in 0..3 {
            let terminal = reductions
                .claim_terminal(&format!("partial-reducer-{index}"), 60_000)
                .await
                .map_err(debug_error)?
                .ok_or_else(|| format!("partial reduction {index} was not queued"))?;
            let artifact = if matches!(terminal.outcome, CanonicalExecutorOutcome::Succeeded(_)) {
                Some(
                    customer_publisher
                        .publish(&terminal)
                        .await
                        .map_err(debug_error)?,
                )
            } else {
                None
            };
            let completion = reductions
                .complete_terminal(&terminal, artifact.as_ref())
                .await
                .map_err(debug_error)?;
            let expected = if index == 2 {
                ExecutorParentTerminalState::Failed
            } else {
                ExecutorParentTerminalState::Pending
            };
            require(
                completion.parent_state == expected,
                format!("partial completion {index} returned {completion:?}"),
            )?;
        }

        let state: TerminalParentSnapshot = sqlx::query_as(
            r#"
            SELECT work.state AS work_state, attempt.state AS attempt_state,
                   job.state AS job_state, quota.state AS quota_state,
                   quota.committed_units, quota.released_units, job.charged_units,
                   (SELECT COUNT(*) FROM provider_receipts receipt
                      JOIN provider_submissions submission
                        ON submission.submission_id = receipt.submission_id
                      WHERE submission.job_id = $1) AS receipt_count,
                   (SELECT COUNT(*) FROM economic_metering_events WHERE job_id = $1)
                     AS economic_meter_count,
                   (SELECT COUNT(*) FROM rated_usage WHERE job_id = $1) AS rating_count,
                   (SELECT COUNT(*) FROM artifacts WHERE job_id = $1) AS artifact_count,
                   (SELECT COUNT(*) FROM job_response_projections WHERE job_id = $1)
                     AS projection_count,
                   (SELECT COUNT(*) FROM output_holds WHERE job_id = $1 AND state = 'held')
                     AS held_hold_count,
                   (SELECT COUNT(*) FROM job_events WHERE job_id = $1
                      AND event_type = 'job.failed') AS terminal_job_event_count,
                   (SELECT COUNT(*) FROM outbox_events WHERE job_id = $1
                      AND event_type = 'job.failed') AS terminal_outbox_count
            FROM work_items work
            JOIN job_attempts attempt ON attempt.execution_id = work.execution_id
            JOIN jobs job ON job.job_id = work.job_id
            JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
            WHERE work.job_id = $1
            "#,
        )
        .bind(work.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state
                == TerminalParentSnapshot {
                    work_state: "failed".to_string(),
                    attempt_state: "failed".to_string(),
                    job_state: "failed".to_string(),
                    quota_state: "committed".to_string(),
                    committed_units: 2,
                    released_units: 1,
                    charged_units: 2,
                    receipt_count: 3,
                    economic_meter_count: 3,
                    rating_count: 3,
                    artifact_count: 2,
                    projection_count: 0,
                    held_hold_count: 0,
                    terminal_job_event_count: 1,
                    terminal_outbox_count: 1,
                },
            format!("partial failure did not close every output exactly once: {state:?}"),
        )?;
        let account: (i64, i64) = sqlx::query_as(
            "SELECT held_micros, captured_micros FROM billing_accounts WHERE tenant_id = $1",
        )
        .bind("executord-process-smoke")
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            account == (0, 17),
            format!("partial failure monetary account is not exact: {account:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn uncertain_output_keeps_hold_and_parent_unresolved() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_codex_generation_lease_with_outputs(
            &database.pool,
            "uncertain-terminal-worker",
            2,
        )
        .await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        seed_terminal_economics(&database.pool, &prepared, 7, 3, 0).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        for index in 0..2 {
            let lease = executor
                .claim_prepared(&scope, &format!("uncertain-executor-{index}"), 60_000)
                .await
                .map_err(debug_error)?
                .ok_or_else(|| format!("uncertain output {index} was not claimable"))?;
            executor.start(&lease).await.map_err(debug_error)?;
            let outcome = if lease.output_index == 1 {
                ExecutorSubmissionOutcome::Uncertain {
                    error_code: "provider_result_unknown".to_string(),
                }
            } else {
                ExecutorSubmissionOutcome::Succeeded(
                    publish_result_authority(&executor_artifacts, &lease).await?,
                )
            };
            executor
                .record_outcome(&lease, &outcome)
                .await
                .map_err(debug_error)?;
        }

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let customer_publisher = CustomerArtifactPublisher::new(Arc::new(
            FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?,
        ));
        for index in 0..2 {
            let terminal = reductions
                .claim_terminal(&format!("uncertain-reducer-{index}"), 60_000)
                .await
                .map_err(debug_error)?
                .ok_or_else(|| format!("uncertain reduction {index} was not queued"))?;
            let artifact = if matches!(terminal.outcome, CanonicalExecutorOutcome::Succeeded(_)) {
                Some(
                    customer_publisher
                        .publish(&terminal)
                        .await
                        .map_err(debug_error)?,
                )
            } else {
                None
            };
            let completion = reductions
                .complete_terminal(&terminal, artifact.as_ref())
                .await
                .map_err(debug_error)?;
            let expected = if index == 1 {
                ExecutorParentTerminalState::Uncertain
            } else {
                ExecutorParentTerminalState::Pending
            };
            require(
                completion.parent_state == expected,
                format!("uncertain completion {index} returned {completion:?}"),
            )?;
        }

        let state: TerminalParentSnapshot = sqlx::query_as(
            r#"
            SELECT work.state AS work_state, attempt.state AS attempt_state,
                   job.state AS job_state, quota.state AS quota_state,
                   quota.committed_units, quota.released_units, job.charged_units,
                   (SELECT COUNT(*) FROM provider_receipts receipt
                      JOIN provider_submissions submission
                        ON submission.submission_id = receipt.submission_id
                      WHERE submission.job_id = $1) AS receipt_count,
                   (SELECT COUNT(*) FROM economic_metering_events WHERE job_id = $1)
                     AS economic_meter_count,
                   (SELECT COUNT(*) FROM rated_usage WHERE job_id = $1) AS rating_count,
                   (SELECT COUNT(*) FROM artifacts WHERE job_id = $1) AS artifact_count,
                   (SELECT COUNT(*) FROM job_response_projections WHERE job_id = $1)
                     AS projection_count,
                   (SELECT COUNT(*) FROM output_holds WHERE job_id = $1 AND state = 'held')
                     AS held_hold_count,
                   (SELECT COUNT(*) FROM job_events WHERE job_id = $1
                      AND event_type = 'job.uncertain') AS terminal_job_event_count,
                   (SELECT COUNT(*) FROM outbox_events WHERE job_id = $1
                      AND event_type = 'job.uncertain') AS terminal_outbox_count
            FROM work_items work
            JOIN job_attempts attempt ON attempt.execution_id = work.execution_id
            JOIN jobs job ON job.job_id = work.job_id
            JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
            WHERE work.job_id = $1
            "#,
        )
        .bind(work.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state
                == TerminalParentSnapshot {
                    work_state: "uncertain".to_string(),
                    attempt_state: "uncertain".to_string(),
                    job_state: "uncertain".to_string(),
                    quota_state: "reserved".to_string(),
                    committed_units: 1,
                    released_units: 0,
                    charged_units: 1,
                    receipt_count: 2,
                    economic_meter_count: 2,
                    rating_count: 1,
                    artifact_count: 1,
                    projection_count: 0,
                    held_hold_count: 1,
                    terminal_job_event_count: 1,
                    terminal_outbox_count: 1,
                },
            format!("uncertain terminal state lost its unresolved hold: {state:?}"),
        )?;
        let account: (i64, i64) = sqlx::query_as(
            "SELECT held_micros, captured_micros FROM billing_accounts WHERE tenant_id = $1",
        )
        .bind("executord-process-smoke")
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            account == (7, 7),
            format!("uncertain monetary account changed its unresolved hold: {account:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn customer_artifact_publication_rejects_tampered_private_authority() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "tampered-artifact-worker", 1).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (artifacts, artifact_root) = artifact_publisher(&executor)?;
        executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        let executor_lease = claim_required(&executor, "tampered-artifact-executor").await?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&artifacts, &executor_lease).await?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(debug_error)?;
        let terminal = PostgresExecutorTerminalStore::new(database.pool.clone())
            .claim_terminal("tampered-artifact-reader", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "tampered artifact terminal was not queued".to_string())?;

        let authority_id = executor_lease.executor_execution_id.simple().to_string();
        let private_object = artifact_root
            .path()
            .join("executor-objects")
            .join(&authority_id[..2])
            .join(&authority_id);
        fs::write(&private_object, b"tampered").map_err(debug_error)?;
        let customer_blobs =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let customer_object = artifact_root
            .path()
            .join("objects")
            .join(&terminal.output_id.simple().to_string()[..2])
            .join(terminal.output_id.simple().to_string());
        require(
            matches!(
                CustomerArtifactPublisher::new(customer_blobs)
                    .publish(&terminal)
                    .await,
                Err(gpt_image_2_gateway::CustomerArtifactPublishError::Integrity)
            ) && !customer_object.exists(),
            "tampered private authority was published to the customer namespace",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn expired_terminal_reduction_has_one_reclaim_winner_and_fences_old_epoch() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "terminal-reclaim-worker", 1).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        let executor_lease = claim_required(&executor, "terminal-reclaim-executor").await?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Failed {
                    error_code: "provider_failed".to_string(),
                },
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let expired = reductions
            .claim_terminal("expired-reader", 25)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "failed terminal reduction was not queued".to_string())?;
        require(
            matches!(
                &expired.outcome,
                CanonicalExecutorOutcome::Failed { error_code }
                    if error_code == "provider_failed"
            ),
            "failed decision was not reconstructed from canonical evidence",
        )?;
        tokio::time::sleep(Duration::from_millis(40)).await;

        let mut tasks = Vec::new();
        for index in 0..20 {
            let store = reductions.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .claim_terminal(&format!("reclaim-reader-{index}"), 60_000)
                    .await
            }));
        }
        let mut winners = Vec::new();
        for task in tasks {
            if let Some(lease) = task.await.map_err(debug_error)?.map_err(debug_error)? {
                winners.push(lease);
            }
        }
        require(
            winners.len() == 1 && winners[0].reducer_lease_epoch == 2,
            format!("terminal reduction reclaim winners were not fenced: {winners:?}"),
        )?;
        require(
            matches!(
                reductions.heartbeat_terminal(&expired, 60_000).await,
                Err(ExecutorTerminalError::StaleLease)
            ),
            "expired terminal reduction epoch retained heartbeat authority",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn blocked_terminal_reduction_is_durable_and_fences_its_lease() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "blocked-terminal-worker", 1).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        let executor_lease = claim_required(&executor, "blocked-terminal-executor").await?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Failed {
                    error_code: "provider_failed".to_string(),
                },
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let lease = reductions
            .claim_terminal("blocked-terminal-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "failed terminal reduction was not queued".to_string())?;
        reductions
            .block_terminal(&lease, ExecutorTerminalBlockReason::CanonicalConflict)
            .await
            .map_err(debug_error)?;

        let blocked: (
            String,
            Option<String>,
            Option<i64>,
            String,
            String,
            i64,
            Option<String>,
            Option<Uuid>,
            Option<Uuid>,
            Option<Uuid>,
        ) = sqlx::query_as(
            r#"
            SELECT state, lease_owner, lease_expires_at_ms,
                   blocked_error_code, blocked_by, blocked_at_ms,
                   completion_owner, provider_receipt_id,
                   customer_artifact_id, quota_reservation_id
            FROM executor_terminal_reductions
            WHERE submission_id = $1
            "#,
        )
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            blocked.0 == "blocked"
                && blocked.1.is_none()
                && blocked.2.is_none()
                && blocked.3 == "canonical_conflict"
                && blocked.4 == lease.reducer_owner
                && blocked.5 > 0
                && blocked.6.is_none()
                && blocked.7.is_none()
                && blocked.8.is_none()
                && blocked.9.is_none(),
            format!("blocked terminal state was not durable and clean: {blocked:?}"),
        )?;
        require(
            reductions
                .claim_terminal("blocked-terminal-reclaimer", 60_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "blocked terminal reduction was reclaimed",
        )?;
        require(
            reductions.heartbeat_terminal(&lease, 60_000).await
                == Err(ExecutorTerminalError::StaleLease),
            "blocked terminal lease retained heartbeat authority",
        )?;
        let completion = reductions.complete_terminal(&lease, None).await;
        require(
            completion == Err(ExecutorTerminalError::StaleLease),
            format!("blocked terminal lease retained completion authority: {completion:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn expired_reduction_lease_cannot_complete_after_reclaim() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_codex_generation_lease(&database.pool, "stale-terminal-worker").await?;
        let executor = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let (executor_artifacts, artifact_root) = artifact_publisher(&executor)?;
        let prepared = executor
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        seed_price_hold(&database.pool, &prepared[0], 7).await?;
        seed_terminal_quota(&database.pool, &work).await?;
        let scope = ExecutorClaimScope {
            execution_profile_id: CODEX_PROFILE_ID,
            provider_id: "openai-codex".to_string(),
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
        };
        let executor_lease = executor
            .claim_prepared(&scope, "stale-terminal-executor", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "stale terminal executor claim returned none".to_string())?;
        executor.start(&executor_lease).await.map_err(debug_error)?;
        let manifest = publish_result_authority(&executor_artifacts, &executor_lease).await?;
        executor
            .record_outcome(
                &executor_lease,
                &ExecutorSubmissionOutcome::Succeeded(manifest),
            )
            .await
            .map_err(debug_error)?;

        let reductions = PostgresExecutorTerminalStore::new(database.pool.clone());
        let expired = reductions
            .claim_terminal("stale-terminal-reducer", 25)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "stale terminal reduction was not queued".to_string())?;
        let publisher = CustomerArtifactPublisher::new(Arc::new(
            FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?,
        ));
        let first_artifact = publisher.publish(&expired).await.map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(40)).await;
        let reclaimed = reductions
            .claim_terminal("reclaimed-terminal-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired terminal reduction was not reclaimed".to_string())?;
        require(
            reclaimed.reducer_lease_epoch == expired.reducer_lease_epoch + 1,
            "terminal reclaim did not advance its epoch",
        )?;
        require(
            reductions
                .complete_terminal(&expired, Some(&first_artifact))
                .await
                == Err(ExecutorTerminalError::StaleLease),
            "expired terminal lease completed after reclaim",
        )?;
        let untouched: (String, i64, i64, i64, i32) = sqlx::query_as(
            r#"
            SELECT reduction.state,
                   (SELECT COUNT(*) FROM provider_receipts WHERE submission_id = $1),
                   (SELECT COUNT(*) FROM artifacts WHERE artifact_id = $2),
                   (SELECT COUNT(*) FROM rated_usage WHERE output_id = $2),
                   quota.committed_units
            FROM executor_terminal_reductions reduction
            JOIN jobs job ON job.job_id = $3
            JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
            WHERE reduction.submission_id = $1
            "#,
        )
        .bind(expired.submission_id)
        .bind(expired.output_id)
        .bind(expired.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            untouched == ("leased".to_string(), 0, 0, 0, 0),
            format!("stale completion leaked durable effects: {untouched:?}"),
        )?;
        let reclaimed_artifact = publisher.publish(&reclaimed).await.map_err(debug_error)?;
        require(
            reclaimed_artifact == first_artifact,
            "reclaimed reducer did not reuse the deterministic customer artifact",
        )?;
        let completion = reductions
            .complete_terminal(&reclaimed, Some(&reclaimed_artifact))
            .await
            .map_err(debug_error)?;
        require(
            completion.parent_state == ExecutorParentTerminalState::Succeeded,
            "reclaimed terminal lease did not complete",
        )?;
        let mut forged = reclaimed.clone();
        forged.reducer_owner = "forged-terminal-reducer".to_string();
        require(
            reductions
                .complete_terminal(&forged, Some(&reclaimed_artifact))
                .await
                == Err(ExecutorTerminalError::StaleLease),
            "completed terminal replay accepted a forged reducer owner",
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
            store
                .prepare_and_handoff(&forged, profile_id_for_lease(&forged))
                .await
                == Err(ExecutorSubmissionError::Conflict),
            "caller-supplied command replaced the durable payload",
        )?;

        sqlx::query(
            "UPDATE jobs SET requested_units = 3, output_count = 3, billable_units = 3 WHERE job_id = $1",
        )
            .bind(lease.job_id)
            .execute(&database.pool)
            .await
            .map_err(|error| format!("requested unit tamper failed: {error}"))?;
        require(
            store
                .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
                .await
                == Err(ExecutorSubmissionError::Conflict),
            "quota units and durable command output count diverged",
        )?;

        let oversized = seed_lease(&database.pool, "oversized-worker", 11).await?;
        require(
            store
                .prepare_and_handoff(&oversized, profile_id_for_lease(&oversized))
                .await
                == Err(ExecutorSubmissionError::InvalidInput),
            "executor accepted an image output count outside the API contract",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn committed_handoff_replays_the_same_provider_identities() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let original = seed_lease(&database.pool, "original-worker", 2).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let first = store
            .prepare_and_handoff(&original, profile_id_for_lease(&original))
            .await
            .map_err(|error| format!("original prepare failed: {error:?}"))?;

        let replay = store
            .prepare_and_handoff(&original, profile_id_for_lease(&original))
            .await
            .map_err(|error| format!("handoff replay failed: {error:?}"))?;

        require(
            first == replay,
            "handoff replay changed provider identities",
        )?;
        require(
            replay
                .iter()
                .all(|item| item.executor_execution_id != original.execution_id),
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
            attachment_count == 2,
            "handoff replay duplicated attempt attachments",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_limit_is_global_and_terminal_release_admits_exactly_one_waiter() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let (profile_id, policy_id) = seed_limited_test_profile(&database.pool, 2).await?;
        let nonzero_policy_id = Uuid::new_v4();
        require(
            sqlx::query(
                r#"
                INSERT INTO executor_resource_policies
                  (resource_policy_id, revision, credential_pool_id, provider_account_id,
                   provider_id, execution_class, max_concurrency, allocated_count,
                   state, created_at_ms)
                VALUES ($1, 1, $2, $3, 'provider-test', 'invalid-nonzero',
                        2, 1, 'disabled', $4)
                "#,
            )
            .bind(nonzero_policy_id)
            .bind(TEST_POOL_ID)
            .bind(TEST_ACCOUNT_ID)
            .bind(database_now(&database.pool).await?)
            .execute(&database.pool)
            .await
            .is_err(),
            "resource policy was created with a forged allocation counter",
        )?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        for index in 0..5 {
            let lease = seed_lease(
                &database.pool,
                &format!("capacity-worker-{index}"),
                1,
            )
            .await?;
            store
                .prepare_and_handoff(&lease, profile_id)
                .await
                .map_err(debug_error)?;
        }
        let scope = test_scope(profile_id);
        let mut tasks = Vec::new();
        for index in 0..20 {
            let store = store.clone();
            let scope = scope.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .claim_prepared(&scope, &format!("capacity-executor-{index}"), 60_000)
                    .await
            }));
        }
        let mut winners = Vec::new();
        for task in tasks {
            if let Some(lease) = task.await.map_err(debug_error)?.map_err(debug_error)? {
                winners.push(lease);
            }
        }
        require(
            winners.len() == 2,
            format!("capacity limit produced {} winners", winners.len()),
        )?;
        let counts: (i32, i64) = sqlx::query_as(
            r#"
            SELECT policy.allocated_count,
                   (SELECT COUNT(*) FROM executor_capacity_allocations allocation
                    WHERE allocation.resource_policy_id = policy.resource_policy_id
                      AND allocation.resource_policy_revision = policy.revision
                      AND allocation.state = 'held')
            FROM executor_resource_policies policy
            WHERE policy.resource_policy_id = $1 AND policy.revision = 1
            "#,
        )
        .bind(policy_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(counts == (2, 2), format!("capacity counter drifted: {counts:?}"))?;
        let replacement_policy_id = Uuid::new_v4();
        let mut replacement = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            "UPDATE executor_resource_policies SET state = 'disabled' WHERE resource_policy_id = $1 AND revision = 1",
        )
        .bind(policy_id)
        .execute(&mut *replacement)
        .await
        .map_err(debug_error)?;
        require(
            sqlx::query(
                r#"
                INSERT INTO executor_resource_policies
                  (resource_policy_id, revision, credential_pool_id, provider_account_id,
                   provider_id, execution_class, max_concurrency, state, created_at_ms)
                VALUES ($1, 1, $2, $3, 'provider-test', 'replacement',
                        2, 'enabled', $4)
                "#,
            )
            .bind(replacement_policy_id)
            .bind(TEST_POOL_ID)
            .bind(TEST_ACCOUNT_ID)
            .bind(database_now(&database.pool).await?)
            .execute(&mut *replacement)
            .await
            .is_err(),
            "provider account enabled a replacement policy while capacity remained held",
        )?;
        replacement.rollback().await.map_err(debug_error)?;
        require(
            sqlx::query(
                r#"
                UPDATE executor_capacity_allocations
                SET state = 'released', released_at_ms = $2,
                    release_decision_id = executor_execution_id,
                    released_state = 'failed', release_reason = 'terminal_evidence'
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(winners[0].executor_execution_id)
            .bind(database_now(&database.pool).await?)
            .execute(&database.pool)
            .await
            .is_err(),
            "capacity allocation was released without a durable resolution decision",
        )?;
        require(
            sqlx::query(
                "UPDATE executor_resource_policies SET allocated_count = allocated_count - 1 WHERE resource_policy_id = $1 AND revision = 1",
            )
            .bind(policy_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "resource policy counter diverged from held allocations",
        )?;

        store.start(&winners[0]).await.map_err(debug_error)?;
        store
            .record_outcome(
                &winners[0],
                &ExecutorSubmissionOutcome::Failed {
                    error_code: "provider_rejected".to_string(),
                },
            )
            .await
            .map_err(debug_error)?;
        let next = store
            .claim_prepared(&scope, "capacity-executor-next", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "released capacity did not admit one waiter".to_string())?;
        require(
            next.executor_execution_id != winners[0].executor_execution_id,
            "released terminal execution was reclaimed",
        )?;
        let counts: (i32, i64, i64) = sqlx::query_as(
            r#"
            SELECT policy.allocated_count,
                   COUNT(*) FILTER (WHERE allocation.state = 'held'),
                   COUNT(*) FILTER (WHERE allocation.state = 'released')
            FROM executor_resource_policies policy
            JOIN executor_capacity_allocations allocation
              ON allocation.resource_policy_id = policy.resource_policy_id
             AND allocation.resource_policy_revision = policy.revision
            WHERE policy.resource_policy_id = $1 AND policy.revision = 1
            GROUP BY policy.allocated_count
            "#,
        )
        .bind(policy_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (2, 2, 1),
            format!("terminal release was not exactly once: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn expired_lease_reuses_allocation_and_disabled_profile_preserves_running_attach()
-> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let (profile_id, policy_id) = seed_limited_test_profile(&database.pool, 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let first = seed_lease(&database.pool, "reclaim-profile-worker", 1).await?;
        let first_prepared = store
            .prepare_and_handoff(&first, profile_id)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let second = seed_lease(&database.pool, "disabled-profile-worker", 1).await?;
        let second_prepared = store
            .prepare_and_handoff(&second, profile_id)
            .await
            .map_err(debug_error)?;
        let first_execution_id = first_prepared
            .first()
            .ok_or_else(|| "first prepared submission missing".to_string())?
            .executor_execution_id;
        let second_execution_id = second_prepared
            .first()
            .ok_or_else(|| "second prepared submission missing".to_string())?
            .executor_execution_id;
        let mut skipped_fresh = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT 1 FROM executor_executions WHERE executor_execution_id = $1 FOR UPDATE")
            .bind(first_execution_id)
            .fetch_one(&mut *skipped_fresh)
            .await
            .map_err(debug_error)?;
        let scope = test_scope(profile_id);
        let claimed = store
            .claim_prepared(&scope, "reclaim-owner-a", 25)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "initial capacity claim was empty".to_string())?;
        require(
            claimed.executor_execution_id == second_execution_id,
            "SKIP LOCKED fixture did not place the newer submission under lease",
        )?;
        skipped_fresh.commit().await.map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(40)).await;
        let reclaimed = store
            .claim_prepared(&scope, "reclaim-owner-b", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired leased execution was not reclaimable".to_string())?;
        require(
            reclaimed.executor_execution_id == claimed.executor_execution_id
                && reclaimed.executor_lease_epoch == claimed.executor_lease_epoch + 1,
            "expired lease did not preserve its durable allocation identity",
        )?;
        let counts: (i32, i64) = sqlx::query_as(
            r#"
            SELECT allocated_count,
                   (SELECT COUNT(*) FROM executor_capacity_allocations
                    WHERE resource_policy_id = $1 AND state = 'held')
            FROM executor_resource_policies
            WHERE resource_policy_id = $1 AND revision = 1
            "#,
        )
        .bind(policy_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(counts == (1, 1), format!("reclaim double-counted capacity: {counts:?}"))?;
        store.start(&reclaimed).await.map_err(debug_error)?;

        let now = database_now(&database.pool).await?;
        sqlx::query(
            "UPDATE provider_execution_profiles SET state = 'disabled', updated_at_ms = $2 WHERE execution_profile_id = $1",
        )
        .bind(profile_id)
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let profile_key: String = sqlx::query_scalar(
            "SELECT profile_key FROM provider_execution_profiles WHERE execution_profile_id = $1",
        )
        .bind(profile_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let loaded = store
            .load_execution_profile(&profile_key)
            .await
            .map_err(debug_error)?;
        require(
            loaded.execution_profile_id == profile_id,
            "disabled profile could not be loaded for in-flight recovery",
        )?;
        let resumed = store
            .resume_owned(&scope, "reclaim-owner-b")
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "disabled profile interrupted an in-flight attach".to_string())?
            .into_lease();
        require(resumed == reclaimed, "running attach changed after profile disable")?;
        require(
            store
                .claim_prepared(&scope, "disabled-profile-executor", 60_000)
                .await
                == Err(ExecutorSubmissionError::Conflict),
            "disabled profile admitted a new capacity allocation",
        )?;
        store
            .record_outcome(
                &resumed,
                &ExecutorSubmissionOutcome::Failed {
                    error_code: "provider_rejected".to_string(),
                },
            )
            .await
            .map_err(debug_error)?;
        let allocated: i32 = sqlx::query_scalar(
            "SELECT allocated_count FROM executor_resource_policies WHERE resource_policy_id = $1 AND revision = 1",
        )
        .bind(policy_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(allocated == 0, "terminal outcome did not release disabled profile capacity")
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn claim_requires_committed_handoff_and_has_one_winner_per_submission() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "claim-worker", 4).await?;
        let store = Arc::new(PostgresExecutorSubmissionStore::new(database.pool.clone()));
        require(
            store
                .claim_prepared(&claim_scope(), "too-early", 60_000)
                .await
                .map_err(|error| format!("early claim failed: {error:?}"))?
                .is_none(),
            "unhanded work granted provider launch authority",
        )?;
        sqlx::query(
            r#"
            UPDATE work_items
            SET lease_expires_at_ms =
                floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 100
            WHERE work_item_id = $1
            "#,
        )
        .bind(lease.work_item_id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        store
            .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
            .await
            .map_err(|error| format!("handoff failed: {error:?}"))?;
        sqlx::query(
            r#"
            UPDATE provider_account_execution_controls control
            SET lifecycle_state = 'draining', control_version = control_version + 1,
                drain_started_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
                updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            FROM provider_execution_profiles profile
            WHERE profile.execution_profile_id = $1
              AND control.provider_account_id = profile.provider_account_id
            "#,
        )
        .bind(profile_id_for_lease(&lease))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(130)).await;
        let reconciled = PostgresReconciliationStore::new(database.pool.clone())
            .reconcile_expired_work(10)
            .await
            .map_err(debug_error)?;
        require(
            reconciled == ReconciliationOutcome::default(),
            format!("worker reconciler reclaimed executor-owned work: {reconciled:?}"),
        )?;

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
        store
            .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
            .await
            .map_err(debug_error)?;
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
        store
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
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

        let parent: (String, Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT state, lease_owner, lease_expires_at_ms FROM work_items WHERE work_item_id = $1",
        )
        .bind(lease.work_item_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            parent == ("awaiting_executor".to_string(), None, None),
            format!("executor start still retained a worker lease: {parent:?}"),
        )?;
        store.start(&lease).await.map_err(|error| {
            format!("committed start replay depended on parent lease: {error:?}")
        })?;

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
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
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
                   command_schema, command_hash, execution_profile_id,
                   credential_pool_id, provider_account_id, credential_ref,
                   credential_revision, adapter_revision, resource_policy_id,
                   resource_policy_revision, operation_id,
                   operation_descriptor_revision, operation_descriptor_sha256_v1,
                   completion_mode, idempotency_mode, operation_binding_version, state,
                   prepared_at_ms, updated_at_ms)
                SELECT $1, $1, $2, job_id, tenant_id, provider_id, model,
                       work_item_id, created_by_execution_id,
                       created_by_lease_epoch, command_schema, command_hash,
                       execution_profile_id, credential_pool_id, provider_account_id,
                       credential_ref, credential_revision, adapter_revision,
                       resource_policy_id, resource_policy_revision, operation_id,
                       operation_descriptor_revision, operation_descriptor_sha256_v1,
                       completion_mode, idempotency_mode, operation_binding_version,
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
               command_schema, command_hash, execution_profile_id,
               credential_pool_id, provider_account_id, credential_ref,
               credential_revision, adapter_revision, resource_policy_id,
               resource_policy_revision, operation_id,
               operation_descriptor_revision, operation_descriptor_sha256_v1,
               completion_mode, idempotency_mode, operation_binding_version, state,
               prepared_at_ms, started_at_ms, updated_at_ms)
            SELECT $1, $2, $3, job_id, tenant_id, provider_id, model,
                   work_item_id, created_by_execution_id, created_by_lease_epoch,
                   command_schema, command_hash, execution_profile_id,
                   credential_pool_id, provider_account_id, credential_ref,
                   credential_revision, adapter_revision, resource_policy_id,
                   resource_policy_revision, operation_id,
                   operation_descriptor_revision, operation_descriptor_sha256_v1,
                   completion_mode, idempotency_mode, operation_binding_version,
                   'running', $4, $4, $4
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
        store.prepare_and_handoff(&work, profile_id_for_lease(&work)).await.map_err(debug_error)?;
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
        store.prepare_and_handoff(&lease, profile_id_for_lease(&lease)).await.map_err(debug_error)?;
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

        let provider_cost = ProviderReportedCostEvidenceV1::usd_ticks(
            ProviderCostEvidenceScope::CliInvocation,
            "provider-test",
            "provider_cli",
            "provider-operation-1",
            200_000_000,
            br#"{"total_cost_usd_ticks":200000000}"#,
            "end.total_cost_usd_ticks",
        )
        .map_err(debug_error)?;
        let successful_manifest = publish_result_authority(&artifacts, &claims[0])
            .await?
            .with_provider_reported_cost(Some(provider_cost.clone()))
            .ok_or("provider cost evidence was rejected")?;
        let successful_manifest_id = successful_manifest.manifest_id();
        let successful_authority_id = successful_manifest.artifact_authority_id();
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
        let conflicting_cost = ProviderReportedCostEvidenceV1::usd_ticks(
            ProviderCostEvidenceScope::CliInvocation,
            "provider-test",
            "provider_cli",
            "provider-operation-1",
            300_000_000,
            br#"{"total_cost_usd_ticks":300000000}"#,
            "end.total_cost_usd_ticks",
        )
        .map_err(debug_error)?;
        let conflicting_manifest =
            ExecutorResultManifest::new(successful_manifest_id, successful_authority_id)
                .and_then(|manifest| {
                    manifest.with_provider_reported_cost(Some(conflicting_cost))
                })
                .ok_or("conflicting provider cost manifest was rejected locally")?;
        require(
            store
                .record_outcome(
                    &claims[0],
                    &ExecutorSubmissionOutcome::Succeeded(conflicting_manifest),
                )
                .await
                == Err(ExecutorSubmissionError::Conflict),
            "terminal replay accepted different provider cost evidence",
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
        let stored_cost: (String, String, String, String) = sqlx::query_as(
            r#"
            SELECT scope, provider_id, native_quantity::TEXT, evidence_path
            FROM executor_provider_cost_evidence
            WHERE manifest_id = $1
            "#,
        )
        .bind(successful_manifest_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            stored_cost
                == (
                    "cli_invocation".to_string(),
                    "provider-test".to_string(),
                    "200000000".to_string(),
                    "end.total_cost_usd_ticks".to_string(),
                ),
            format!("provider cost evidence drifted in storage: {stored_cost:?}"),
        )?;
        require(
            sqlx::query(
                "UPDATE executor_provider_cost_evidence SET native_quantity = 1 WHERE manifest_id = $1",
            )
            .bind(successful_manifest_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "provider cost evidence remained mutable",
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
async fn record_outcome_rolls_back_evidence_projection_and_capacity_on_projection_failure()
-> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "atomic-outcome-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        let claim = claim_required(&store, "atomic-outcome-executor").await?;
        store.start(&claim).await.map_err(debug_error)?;
        sqlx::raw_sql(
            r#"
            CREATE FUNCTION reject_test_terminal_projection() RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.state IN ('succeeded', 'failed', 'uncertain') THEN
                    RAISE EXCEPTION 'injected terminal projection failure';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER reject_test_terminal_projection
                BEFORE UPDATE ON provider_submissions
                FOR EACH ROW EXECUTE FUNCTION reject_test_terminal_projection();
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let outcome = ExecutorSubmissionOutcome::Failed {
            error_code: "provider_rejected".to_string(),
        };
        require(
            store.record_outcome(&claim, &outcome).await
                == Err(ExecutorSubmissionError::Unavailable),
            "injected projection failure did not abort terminal recording",
        )?;
        let rolled_back: (String, String, i64, i64, String, i32) = sqlx::query_as(
            r#"
            SELECT e.state, s.state,
                   (SELECT COUNT(*) FROM executor_runner_observations observation
                    WHERE observation.executor_execution_id = e.executor_execution_id),
                   (SELECT COUNT(*) FROM executor_resolution_decisions decision
                    WHERE decision.executor_execution_id = e.executor_execution_id),
                   allocation.state, policy.allocated_count
            FROM executor_executions e
            JOIN provider_submissions s ON s.submission_id = e.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = e.executor_execution_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE e.executor_execution_id = $1
            "#,
        )
        .bind(claim.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            rolled_back
                == (
                    "running".to_string(),
                    "running".to_string(),
                    0,
                    0,
                    "held".to_string(),
                    1,
                ),
            format!("terminal transaction partially committed: {rolled_back:?}"),
        )?;
        sqlx::raw_sql(
            r#"
            DROP TRIGGER reject_test_terminal_projection ON provider_submissions;
            DROP FUNCTION reject_test_terminal_projection();
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        store
            .record_outcome(&claim, &outcome)
            .await
            .map_err(debug_error)?;
        let committed: (String, String, i64, i64, String, i32) = sqlx::query_as(
            r#"
            SELECT e.state, s.state,
                   (SELECT COUNT(*) FROM executor_runner_observations observation
                    WHERE observation.executor_execution_id = e.executor_execution_id),
                   (SELECT COUNT(*) FROM executor_resolution_decisions decision
                    WHERE decision.executor_execution_id = e.executor_execution_id),
                   allocation.state, policy.allocated_count
            FROM executor_executions e
            JOIN provider_submissions s ON s.submission_id = e.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = e.executor_execution_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE e.executor_execution_id = $1
            "#,
        )
        .bind(claim.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            committed
                == (
                    "failed".to_string(),
                    "failed".to_string(),
                    1,
                    1,
                    "released".to_string(),
                    0,
                ),
            format!("terminal retry did not commit exactly once: {committed:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn v2_parent_cannot_terminal_before_output_reductions() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "early-terminal-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store
            .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
            .await
            .map_err(debug_error)?;
        let claim = claim_required_for(&store, "early-terminal-executor", 25).await?;
        require(
            deactivate_work(&database.pool, &lease, "uncertain")
                .await
                .is_err(),
            "V2 parent terminalized before its output reductions",
        )?;
        tokio::time::sleep(Duration::from_millis(40)).await;
        let reclaimed =
            claim_required_for(&store, "reclaimed-early-terminal-executor", 60_000).await?;
        require(
            reclaimed.executor_execution_id == claim.executor_execution_id
                && reclaimed.executor_lease_epoch == claim.executor_lease_epoch + 1,
            "rejected early parent terminal prevented normal executor reclaim",
        )?;
        let states: (String, String, String, String, String, i64) = sqlx::query_as(
            r#"
            SELECT s.state, e.state, o.state, work.state, attempt.state,
                   (SELECT COUNT(*) FROM executor_terminal_reductions
                    WHERE submission_id = s.submission_id)
            FROM provider_submissions s
            JOIN executor_executions e USING (executor_execution_id)
            JOIN job_outputs o ON o.output_id = s.output_id
            JOIN work_items work ON work.work_item_id = s.work_item_id
            JOIN job_attempts attempt ON attempt.execution_id = work.execution_id
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
                    "prepared".into(),
                    "leased".into(),
                    "pending".into(),
                    "awaiting_executor".into(),
                    "handed_off".into(),
                    0,
                ),
            format!("rejected early terminal changed canonical states: {states:?}"),
        )?;
        let allocation: (String, i32) = sqlx::query_as(
            r#"
            SELECT allocation.state, policy.allocated_count
            FROM executor_capacity_allocations allocation
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE allocation.executor_execution_id = $1
            "#,
        )
        .bind(claim.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            allocation == ("held".to_string(), 1),
            format!("executor reclaim changed capacity ownership: {allocation:?}"),
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
        store.prepare_and_handoff(&lease, profile_id_for_lease(&lease)).await.map_err(debug_error)?;
        let claim = claim_required_for(&store, "reconcile-executor", 200).await?;
        store.start(&claim).await.map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(250)).await;

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
        let held: (String, i32) = sqlx::query_as(
            r#"
            SELECT allocation.state, policy.allocated_count
            FROM executor_capacity_allocations allocation
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE allocation.executor_execution_id = $1
            "#,
        )
        .bind(claim.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            held == ("held".to_string(), 1),
            format!("lease expiry released capacity without terminal evidence: {held:?}"),
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
                .is_ok(),
            "matching late outcome was not retained as evidence",
        )?;
        let canonical: (String, String, String) = sqlx::query_as(
            r#"
            SELECT e.state, s.state, d.source
            FROM executor_executions e
            JOIN provider_submissions s ON s.submission_id = e.submission_id
            JOIN executor_resolution_decisions d
              ON d.decision_id = e.resolution_decision_id
            WHERE e.executor_execution_id = $1 AND e.submission_id = $2
            "#,
        )
        .bind(claim.executor_execution_id)
        .bind(claim.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            canonical
                == (
                    "uncertain".to_string(),
                    "uncertain".to_string(),
                    "executor_lease_expired".to_string(),
                ),
            format!("late evidence rewrote the canonical expiry decision: {canonical:?}"),
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
        let released: (String, i32) = sqlx::query_as(
            r#"
            SELECT allocation.state, policy.allocated_count
            FROM executor_capacity_allocations allocation
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE allocation.executor_execution_id = $1
            "#,
        )
        .bind(claim.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            released == ("released".to_string(), 0),
            format!("late evidence did not release held capacity: {released:?}"),
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
        store
            .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
            .await
            .map_err(debug_error)?;
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
            execution_profile_id: TEST_PROFILE_ID,
            provider_id: "other-provider".to_string(),
            command_schema: "provider-command-v1".to_string(),
            adapter_revision: "provider-test-adapter-v1".to_string(),
        };
        require(
            store.claim_prepared(&wrong_scope, "executor", 60_000).await
                == Err(ExecutorSubmissionError::Conflict),
            "executor accepted a scope that conflicts with its database profile",
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
        let prepare = tokio::spawn(async move {
            store
                .prepare_and_handoff(&prepare_lease, profile_id_for_lease(&prepare_lease))
                .await
        });
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
        store
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
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
async fn handed_off_parent_cannot_regain_a_worker_lease_and_allows_start() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "expired-parent-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        let lease = claim_required(&store, "expired-parent-executor").await?;
        require(
            sqlx::query(
                r#"
            UPDATE work_items
            SET lease_owner = 'forged-worker',
                lease_expires_at_ms =
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 60000
            WHERE work_item_id = $1
            "#,
            )
            .bind(work.work_item_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "executor-owned work regained a worker lease",
        )?;

        store.start(&lease).await.map_err(debug_error)?;
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
            states == ("running".to_string(), "running".to_string()),
            format!("handed-off parent did not allow start: {states:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn lock_wait_records_late_evidence_without_resurrecting_expired_executor_lease() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_lease(&database.pool, "deadline-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store
            .prepare_and_handoff(&lease, profile_id_for_lease(&lease))
            .await
            .map_err(debug_error)?;
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
            outcome.await.map_err(debug_error)?.is_ok(),
            "late terminal evidence was discarded after lock wait",
        )?;
        let after_wait: (String, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT e.state, e.lease_expires_at_ms,
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
                   (SELECT COUNT(*) FROM executor_runner_observations observation
                    WHERE observation.executor_execution_id = e.executor_execution_id)
            FROM executor_executions e
            WHERE e.executor_execution_id = $1
            "#,
        )
        .bind(claim.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            after_wait.0 == "running" && after_wait.1 <= after_wait.2 && after_wait.3 == 1,
            format!("lock wait revived or lost the expired execution: {after_wait:?}"),
        )?;
        require(
            store.reconcile_expired(1).await.map_err(debug_error)? == 1,
            "expired execution with late evidence was not reconciled",
        )?;
        let canonical: (String, String) = sqlx::query_as(
            r#"
            SELECT e.state, d.source
            FROM executor_executions e
            JOIN executor_resolution_decisions d
              ON d.decision_id = e.resolution_decision_id
            WHERE e.executor_execution_id = $1
            "#,
        )
        .bind(claim.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            canonical
                == (
                    "uncertain".to_string(),
                    "executor_lease_expired".to_string(),
                ),
            format!("late evidence bypassed conservative expiry resolution: {canonical:?}"),
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
        let prepared = store
            .prepare_and_handoff(&first, profile_id_for_lease(&first))
            .await
            .map_err(debug_error)?;
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
        execution_profile_id: TEST_PROFILE_ID,
        provider_id: "provider-test".to_string(),
        command_schema: "provider-command-v1".to_string(),
        adapter_revision: "provider-test-adapter-v1".to_string(),
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

async fn publish_video_result_authority(
    publisher: &ExecutorArtifactPublisher,
    claim: &ExecutorSubmissionLease,
) -> TestResult<ExecutorResultManifest> {
    publisher
        .publish(claim, &minimal_mp4())
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

fn minimal_mp4() -> Vec<u8> {
    let config = mp4::Mp4Config {
        major_brand: "isom".parse().unwrap(),
        minor_version: 512,
        compatible_brands: vec!["isom".parse().unwrap(), "avc1".parse().unwrap()],
        timescale: 1_000,
    };
    let mut writer =
        mp4::Mp4Writer::write_start(std::io::Cursor::new(Vec::new()), &config).unwrap();
    writer
        .add_track(
            &mp4::AvcConfig {
                width: 16,
                height: 16,
                seq_param_set: vec![0x67, 0x42, 0x00, 0x1e],
                pic_param_set: vec![0x68, 0xce, 0x3c, 0x80],
            }
            .into(),
        )
        .unwrap();
    writer
        .write_sample(
            1,
            &mp4::Mp4Sample {
                start_time: 0,
                duration: 8_000,
                rendering_offset: 0,
                is_sync: true,
                bytes: vec![0, 0, 0, 1, 0x65].into(),
            },
        )
        .unwrap();
    writer.write_end().unwrap();
    writer.into_writer().into_inner()
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
async fn resume_owned_fences_owner_scope_state_and_database_expiry() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "resume-worker", 2).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;

        let leased = claim_required_for(&store, "stable-executor", 200).await?;
        let resumed_lease = store
            .resume_owned(&claim_scope(), "stable-executor")
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "active leased execution did not resume".to_string())?;
        require(
            resumed_lease.needs_start() && resumed_lease.into_lease() == leased,
            "leased resume changed durable lease identity",
        )?;
        require(
            store
                .resume_owned(&claim_scope(), "other-executor")
                .await
                .map_err(debug_error)?
                .is_none(),
            "other owner resumed leased execution",
        )?;
        store.start(&leased).await.map_err(debug_error)?;

        let resumed = store
            .resume_owned(&claim_scope(), "stable-executor")
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "running execution did not resume".to_string())?;
        require(
            !resumed.needs_start() && resumed.into_lease() == leased,
            "resume changed durable lease identity",
        )?;
        require(
            store
                .resume_owned(&claim_scope(), "other-executor")
                .await
                .map_err(debug_error)?
                .is_none(),
            "other owner resumed running execution",
        )?;
        let wrong_scope = ExecutorClaimScope {
            command_schema: "other-command-v1".to_string(),
            ..claim_scope()
        };
        require(
            store
                .resume_owned(&wrong_scope, "stable-executor")
                .await
                .map_err(debug_error)?
                .is_none(),
            "other scope resumed running execution",
        )?;

        tokio::time::sleep(Duration::from_millis(220)).await;
        require(
            store
                .resume_owned(&claim_scope(), "stable-executor")
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
async fn resume_owned_rejects_expired_prelaunch_lease() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "resume-expired-lease-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        claim_required_for(&store, "expired-prelaunch-owner", 25).await?;
        tokio::time::sleep(Duration::from_millis(40)).await;

        require(
            store
                .resume_owned(&claim_scope(), "expired-prelaunch-owner")
                .await
                .map_err(debug_error)?
                .is_none(),
            "expired prelaunch lease was resumable",
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
        store
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
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
async fn launch_context_restores_digest_bound_input_metadata() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "launch-input-worker", 1).await?;
        let admission_session_id: Uuid = sqlx::query_scalar(
            "SELECT admission_session_id FROM job_payloads WHERE job_id = $1",
        )
        .bind(work.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let now = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            INSERT INTO job_input_manifests
              (job_id, admission_session_id, manifest_schema, manifest_hash, input_count, created_at_ms)
            VALUES ($1, $2, 'openai.images.edit.inputs.v1', $3, 3, $4)
            "#,
        )
        .bind(work.job_id)
        .bind(admission_session_id)
        .bind("e".repeat(64))
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let input_specs = [
            ("mask", 0_i16, "image/png", "f".repeat(64), 45_i64),
            ("image", 1_i16, "image/png", "e".repeat(64), 124_i64),
            ("image", 0_i16, "image/jpeg", "d".repeat(64), 123_i64),
        ];
        let mut expected = Vec::new();
        for (role, index, media_type, digest, byte_size) in input_specs {
            let input_id = Uuid::new_v4();
            let object_key = format!("sealed/{role}-{index}-{input_id}");
            sqlx::query(
                r#"
                INSERT INTO job_input_objects
                  (input_id, job_id, admission_session_id, role, input_index, media_type,
                   storage_backend, object_key, sha256_hex, byte_size, created_at_ms)
                VALUES ($1, $2, $3, $4, $5, $6,
                        'filesystem-input-v1', $7, $8, $9, $10)
                "#,
            )
            .bind(input_id)
            .bind(work.job_id)
            .bind(admission_session_id)
            .bind(role)
            .bind(index)
            .bind(media_type)
            .bind(&object_key)
            .bind(&digest)
            .bind(byte_size)
            .bind(now)
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
            expected.push((role, index, media_type, input_id, object_key, digest, byte_size));
        }

        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store
            .prepare_and_handoff(&work, profile_id_for_lease(&work))
            .await
            .map_err(debug_error)?;
        let lease = claim_required(&store, "launch-input-executor").await?;
        store.start(&lease).await.map_err(debug_error)?;
        let context = store
            .load_launch_context(&lease)
            .await
            .map_err(debug_error)?;
        let [source, reference, mask] = context.inputs() else {
            return Err(format!(
                "launch context did not restore the semantic-mask inputs: {:?}",
                context.inputs()
            ));
        };
        let source_expected = &expected[2];
        let reference_expected = &expected[1];
        let mask_expected = &expected[0];
        require(
            restored_input_matches(source, admission_session_id, source_expected)
                && restored_input_matches(reference, admission_session_id, reference_expected)
                && restored_input_matches(mask, admission_session_id, mask_expected),
            format!(
                "launch context changed sealed input authority or ordering: {:?}",
                context.inputs()
            ),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

fn restored_input_matches(
    input: &gpt_image_2_gateway::executor::ExecutorInputObject,
    admission_session_id: Uuid,
    expected: &(&str, i16, &str, Uuid, String, String, i64),
) -> bool {
    input.role() == expected.0
        && i16::try_from(input.index()).ok() == Some(expected.1)
        && input.media_type() == expected.2
        && input.blob().key.admission_session_id == admission_session_id
        && input.blob().key.input_id == expected.3
        && input.blob().storage_backend == "filesystem-input-v1"
        && input.blob().object_key == expected.4
        && input.blob().sha256_hex == expected.5
        && i64::try_from(input.blob().byte_size).ok() == Some(expected.6)
}

#[tokio::test]
async fn launch_context_rejects_command_tampering_and_expired_lease() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_lease(&database.pool, "launch-integrity-worker", 1).await?;
        let store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        store.prepare_and_handoff(&work, profile_id_for_lease(&work)).await.map_err(debug_error)?;
        let lease = claim_required_for(&store, "launch-integrity-executor", 5_000).await?;
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
        tokio::time::sleep(Duration::from_millis(5_100)).await;
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
          (quote_id, job_id, price_version_id, currency, output_count, billable_units,
           billing_metric, billing_unit,
           success_micros, failed_micros, no_effect_micros, max_total_micros,
           quote_hash, created_at_ms)
        VALUES ($1, $2, $3, 'USD', 1, 1, 'output', 'output',
                $4, 0, 0, $4, $5, $6)
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

async fn seed_terminal_economics(
    pool: &PgPool,
    submissions: &[gpt_image_2_gateway::PreparedExecutorSubmission],
    success_micros: i64,
    failed_micros: i64,
    no_effect_micros: i64,
) -> TestResult {
    let first = submissions
        .first()
        .ok_or_else(|| "terminal economics requires at least one output".to_string())?;
    require(
        submissions.iter().all(|submission| {
            submission.job_id == first.job_id
                && submission.tenant_id == first.tenant_id
                && submission.provider_id == first.provider_id
                && submission.model == first.model
        }),
        "terminal economics outputs do not share one immutable job identity",
    )?;
    let output_count = i32::try_from(submissions.len())
        .map_err(|_| "terminal economics output count exceeds i32".to_string())?;
    let held_per_output = success_micros.max(failed_micros).max(no_effect_micros);
    let max_total_micros = held_per_output
        .checked_mul(i64::from(output_count))
        .ok_or_else(|| "terminal economics hold overflowed".to_string())?;
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
                'USD', $5, $6, $7, 'retired', $8, $8)
        "#,
    )
    .bind(price_version_id)
    .bind(format!("test-price-{price_version_id}"))
    .bind(&first.provider_id)
    .bind(&first.model)
    .bind(success_micros)
    .bind(failed_micros)
    .bind(no_effect_micros)
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
    .bind(&first.tenant_id)
    .bind(max_total_micros)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO price_quotes
          (quote_id, job_id, price_version_id, currency, output_count, billable_units,
           billing_metric, billing_unit,
           success_micros, failed_micros, no_effect_micros, max_total_micros,
           quote_hash, created_at_ms)
        VALUES ($1, $2, $3, 'USD', $4, $4, 'output', 'output',
                $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(quote_id)
    .bind(first.job_id)
    .bind(price_version_id)
    .bind(output_count)
    .bind(success_micros)
    .bind(failed_micros)
    .bind(no_effect_micros)
    .bind(max_total_micros)
    .bind(hex::encode(Sha256::digest(quote_id.as_bytes())))
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    for submission in submissions {
        sqlx::query(
            r#"
            INSERT INTO output_holds
              (output_id, job_id, quote_id, tenant_id, currency, held_micros,
               state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, 'USD', $5, 'held', $6, $6)
            "#,
        )
        .bind(submission.output_id)
        .bind(first.job_id)
        .bind(quote_id)
        .bind(&first.tenant_id)
        .bind(held_per_output)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    }
    tx.commit().await.map_err(debug_error)
}

async fn v4_terminal_economic_state(
    pool: &PgPool,
    submission_id: Uuid,
    job_id: Uuid,
    output_id: Uuid,
    terminal_outcome: &str,
    expected_metric: &str,
    expected_quantity: i64,
    expected_dimensions: &serde_json::Value,
) -> TestResult<V4TerminalEconomicState> {
    sqlx::query_as(
        r#"
        SELECT job.state AS job_state, output.state AS output_state,
               (SELECT COUNT(*) FROM provider_receipts
                WHERE submission_id = $1) AS receipt_count,
               (SELECT COUNT(*) FROM provider_usage_facts
                WHERE submission_id = $1
                  AND job_id = $2
                  AND output_id = $3
                  AND billing_partition_key = 'output:' || $3::TEXT
                  AND terminal_outcome = $4
                  AND metric = $5
                  AND quantity = $6
                  AND metadata_json @> $7)
                 AS provider_usage_fact_count,
               (SELECT COUNT(*) FROM economic_metering_events
                WHERE output_id = $3) AS legacy_economic_meter_count,
               (SELECT COUNT(*) FROM rated_usage
                WHERE output_id = $3) AS legacy_rating_count,
               (SELECT COUNT(*) FROM output_holds
                WHERE output_id = $3) AS legacy_hold_count,
               (SELECT COUNT(*) FROM ledger_transactions
                WHERE source_job_id = $2
                  AND transaction_type = 'customer_charge')
                 AS legacy_customer_charge_count,
               (SELECT COUNT(*) FROM customer_rated_usage
                WHERE job_id = $2) AS customer_rating_count,
               (SELECT COUNT(*) FROM customer_rated_usage_lines line
                JOIN customer_rated_usage rating
                  ON rating.rated_usage_id = line.rated_usage_id
                WHERE rating.job_id = $2) AS customer_rating_line_count,
               (SELECT COUNT(*) FROM customer_rated_usage_fact_links link
                JOIN customer_rated_usage_lines line
                  ON line.rated_usage_line_id = link.rated_usage_line_id
                JOIN customer_rated_usage rating
                  ON rating.rated_usage_id = line.rated_usage_id
                WHERE rating.job_id = $2) AS customer_fact_link_count,
               (SELECT COUNT(*) FROM ledger_transactions
                WHERE source_job_id = $2
                  AND transaction_type = 'customer_job_charge')
                 AS customer_job_charge_count,
               hold.state AS hold_state,
               hold.captured_micros AS hold_captured_micros,
               hold.released_micros AS hold_released_micros,
               account.held_micros AS account_held_micros,
               account.captured_micros AS account_captured_micros
        FROM jobs job
        JOIN job_outputs output ON output.job_id = job.job_id
        JOIN customer_billing_holds hold ON hold.job_id = job.job_id
        JOIN billing_accounts account
          ON account.tenant_id = job.tenant_id
         AND account.currency = hold.currency
        WHERE job.job_id = $2 AND output.output_id = $3
        "#,
    )
    .bind(submission_id)
    .bind(job_id)
    .bind(output_id)
    .bind(terminal_outcome)
    .bind(expected_metric)
    .bind(expected_quantity)
    .bind(expected_dimensions)
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

async fn seed_v4_customer_quote(
    pool: &PgPool,
    work: &WorkLease,
    success_micros: i64,
) -> TestResult {
    seed_v4_customer_quote_with_basis(
        pool,
        work,
        V4CustomerQuoteIdentity::openai(),
        V4CustomerQuoteBasis {
            metric: "image_output",
            unit: "image",
            unit_size: 1,
            unit_price_micros: success_micros,
            quantity_source: "request_derived",
            confidence: "exact",
            max_quantity: 1,
            max_amount_micros: success_micros,
        },
    )
    .await
}

async fn seed_v4_customer_token_quote(pool: &PgPool, work: &WorkLease) -> TestResult {
    seed_v4_customer_quote_with_basis(
        pool,
        work,
        V4CustomerQuoteIdentity::openai(),
        V4CustomerQuoteBasis {
            metric: "image_output_token",
            unit: "token",
            unit_size: 1_000_000,
            unit_price_micros: 30_000_000,
            quantity_source: "official_lookup",
            confidence: "estimated",
            max_quantity: 7_024,
            max_amount_micros: 210_720,
        },
    )
    .await
}

async fn seed_provider_reported_actual_price(
    pool: &PgPool,
    work: &WorkLease,
    identity: V4CustomerQuoteIdentity,
) -> TestResult<Uuid> {
    let effective_from_ms = database_now(pool).await?;
    seed_provider_reported_actual_price_at(pool, work, identity, effective_from_ms).await
}

async fn seed_provider_reported_actual_price_at(
    pool: &PgPool,
    work: &WorkLease,
    identity: V4CustomerQuoteIdentity,
    effective_from_ms: i64,
) -> TestResult<Uuid> {
    let provider_id: String = sqlx::query_scalar("SELECT provider_id FROM jobs WHERE job_id = $1")
        .bind(work.job_id)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
    let now = database_now(pool).await?;
    let price_book_id = Uuid::new_v4();
    let price_book_version_id = Uuid::new_v4();
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO price_books (
            price_book_id, price_book_key, display_name, purpose,
            scope_type, provider_id, currency, state,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'Executor provider actual cost', 'provider_actual',
                'platform', $3, 'USD', 'active', $4, $4)
        "#,
    )
    .bind(price_book_id)
    .bind(format!(
        "executor-provider-actual-{}",
        price_book_id.simple()
    ))
    .bind(&provider_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier,
            execution_surface, billing_mode, is_free, state,
            effective_from_ms, source_kind,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8, 'standard',
                'provider_cli', 'provider_reported', FALSE, 'draft',
                $9, 'manual', $10, $10)
        "#,
    )
    .bind(price_book_version_id)
    .bind(price_book_id)
    .bind(identity.price_api_profile)
    .bind(identity.operation)
    .bind(&provider_id)
    .bind(identity.provider_model_id)
    .bind(identity.public_model_id)
    .bind(identity.media_kind)
    .bind(effective_from_ms)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE price_book_versions
            SET state = 'active', control_version = 2, updated_at_ms = $2
            WHERE price_book_version_id = $1
            "#,
        )
        .bind(price_book_version_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?,
        "provider actual price version activation",
    )?;
    tx.commit().await.map_err(debug_error)?;
    Ok(price_book_version_id)
}

#[derive(Clone)]
struct V4CustomerQuoteIdentity {
    quote_api_profile: &'static str,
    price_api_profile: &'static str,
    operation: &'static str,
    provider_model_id: &'static str,
    public_model_id: &'static str,
    media_kind: &'static str,
    dimensions: serde_json::Value,
    source_kind: &'static str,
    source_url: Option<&'static str>,
}

impl V4CustomerQuoteIdentity {
    fn openai() -> Self {
        Self {
            quote_api_profile: "openai-images-v1",
            price_api_profile: "openai-images-v1",
            operation: "generation",
            provider_model_id: "gpt-image-2",
            public_model_id: "gpt-image-2",
            media_kind: "image",
            dimensions: json!({"quality": "high", "size": "1024x1024"}),
            source_kind: "official_document",
            source_url: Some("https://developers.openai.com/api/docs/pricing"),
        }
    }

    fn dreamina() -> Self {
        Self {
            quote_api_profile: DREAMINA_IMAGES_API_PROFILE,
            price_api_profile: DREAMINA_IMAGES_API_PROFILE,
            operation: "generation",
            provider_model_id: "5.0",
            public_model_id: "5.0",
            media_kind: "image",
            dimensions: json!({"resolution_type": "2k", "ratio": "1:1"}),
            source_kind: "official_document",
            source_url: Some("https://bytedance.larkoffice.com/wiki/FVTwwm0bGiishxkKOoScdHR2nsg"),
        }
    }

    fn ark() -> Self {
        Self {
            quote_api_profile: image_api_contracts::ark::ARK_IMAGES_API_PROFILE,
            price_api_profile: DREAMINA_IMAGES_API_PROFILE,
            operation: "generation",
            provider_model_id: "5.0",
            public_model_id: "doubao-seedream-5-0-lite",
            media_kind: "image",
            dimensions: json!({"resolution_type": "2k", "ratio": "1:1"}),
            source_kind: "official_document",
            source_url: Some("https://bytedance.larkoffice.com/wiki/FVTwwm0bGiishxkKOoScdHR2nsg"),
        }
    }

    fn dreamina_video() -> Self {
        Self {
            quote_api_profile: DREAMINA_VIDEOS_API_PROFILE,
            price_api_profile: DREAMINA_VIDEOS_API_PROFILE,
            operation: VIDEO_GENERATION_OPERATION,
            provider_model_id: "seedance2.0fast",
            public_model_id: "seedance2.0fast",
            media_kind: "video",
            dimensions: json!({"duration": "8", "ratio": "9:16", "resolution": "720p"}),
            source_kind: "manual",
            source_url: None,
        }
    }

    fn ark_video() -> Self {
        Self {
            quote_api_profile: image_api_contracts::ark::ARK_CONTENT_GENERATION_API_PROFILE,
            price_api_profile: DREAMINA_VIDEOS_API_PROFILE,
            operation: VIDEO_GENERATION_OPERATION,
            provider_model_id: "seedance2.0fast",
            public_model_id: "doubao-seedance-2-0-fast-260128",
            media_kind: "video",
            dimensions: json!({"duration": "8", "ratio": "9:16", "resolution": "720p"}),
            source_kind: "manual",
            source_url: None,
        }
    }
}

#[derive(Clone, Copy)]
struct V4CustomerQuoteBasis {
    metric: &'static str,
    unit: &'static str,
    unit_size: i64,
    unit_price_micros: i64,
    quantity_source: &'static str,
    confidence: &'static str,
    max_quantity: i64,
    max_amount_micros: i64,
}

async fn seed_v4_customer_quote_with_basis(
    pool: &PgPool,
    work: &WorkLease,
    identity: V4CustomerQuoteIdentity,
    basis: V4CustomerQuoteBasis,
) -> TestResult {
    let (tenant_id, provider_id): (String, String) =
        sqlx::query_as("SELECT tenant_id, provider_id FROM jobs WHERE job_id = $1")
            .bind(work.job_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let output_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT output_id FROM job_outputs WHERE job_id = $1 ORDER BY output_index",
    )
    .bind(work.job_id)
    .fetch_all(pool)
    .await
    .map_err(debug_error)?;
    require(
        !output_ids.is_empty(),
        "v4 customer quote requires at least one output",
    )?;
    let max_total_micros = basis
        .max_amount_micros
        .checked_mul(
            i64::try_from(output_ids.len())
                .map_err(|_| "v4 output count exceeds i64".to_string())?,
        )
        .ok_or_else(|| "v4 quote maximum overflowed".to_string())?;
    let now = database_now(pool).await?;
    let price_book_id = Uuid::new_v4();
    let price_book_version_id = Uuid::new_v4();
    let success_price_component_id = Uuid::new_v4();
    let failed_price_component_id = Uuid::new_v4();
    let quote_id = Uuid::new_v4();
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO identity_organizations (
            organization_id, display_name, organization_kind,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, 'Executor v4 test', 'system', $2, $2)
        "#,
    )
    .bind(&tenant_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO gateway_projects (id, tenant_id, name, created_at)
        VALUES ('project-test', $1, 'Executor test project', $2)
        "#,
    )
    .bind(&tenant_id)
    .bind(now / 1_000)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions (
            job_id, tenant_id, project_id, auth_kind, admitted_at_ms
        )
        VALUES ($1, $2, 'project-test', 'legacy', $3)
        "#,
    )
    .bind(work.job_id)
    .bind(&tenant_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    require_one(
        sqlx::query(
            "UPDATE jobs SET economics_contract_version = 4, updated_at_ms = $2 WHERE job_id = $1",
        )
        .bind(work.job_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?,
        "v4 economics contract activation",
    )?;
    sqlx::query(
        r#"
        INSERT INTO price_books (
            price_book_id, price_book_key, display_name, purpose,
            scope_type, currency, state, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'Executor v4 customer price', 'customer_sale',
                'platform', 'USD', 'active', $3, $3)
        "#,
    )
    .bind(price_book_id)
    .bind(format!("executor-v4-{}", price_book_id.simple()))
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier,
            execution_surface, billing_mode, is_free, state,
            effective_from_ms, source_kind, source_url,
            source_checked_at_ms, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 1, $3, $4,
                $5, $6, $7, $8, 'standard', 'provider_cli',
                'customer_rate', FALSE, 'draft', $9,
                $10, $11, $9, $9, $9)
        "#,
    )
    .bind(price_book_version_id)
    .bind(price_book_id)
    .bind(identity.price_api_profile)
    .bind(identity.operation)
    .bind(&provider_id)
    .bind(identity.provider_model_id)
    .bind(identity.public_model_id)
    .bind(identity.media_kind)
    .bind(now)
    .bind(identity.source_kind)
    .bind(identity.source_url)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    for (component_id, suffix, outcome, unit_price_micros) in [
        (
            success_price_component_id,
            "succeeded",
            "succeeded",
            basis.unit_price_micros,
        ),
        (failed_price_component_id, "failed", "failed", 0),
    ] {
        sqlx::query(
            r#"
            INSERT INTO price_components (
                price_component_id, price_book_version_id, component_key,
                metric, unit, unit_size, unit_price_micros, outcome,
                quantity_source, required_confidence, rounding_mode,
                dimensions_json, created_at_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    'exact', $11, $12)
            "#,
        )
        .bind(component_id)
        .bind(price_book_version_id)
        .bind(format!("{}.{}", basis.metric, suffix))
        .bind(basis.metric)
        .bind(basis.unit)
        .bind(basis.unit_size)
        .bind(unit_price_micros)
        .bind(outcome)
        .bind(basis.quantity_source)
        .bind(basis.confidence)
        .bind(&identity.dimensions)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    }
    let contract_key = format!("test.executor-surface.{}", price_book_version_id.simple());
    let contract_hash = hex::encode(Sha256::digest(contract_key.as_bytes()));
    sqlx::query(
        r#"
        INSERT INTO pricing_surface_contract_revisions (
            contract_key, revision, contract_hash, contract_schema_version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier, execution_surface,
            normalizer_key, normalizer_revision, contract_json, created_at_ms
        )
        VALUES (
            $1, 1, $2, 1, $3, $4, $5, $6, $7, $8,
            'standard', 'provider_cli',
            'test.executor-surface', 1, '{}'::JSONB, $9
        )
        "#,
    )
    .bind(&contract_key)
    .bind(&contract_hash)
    .bind(identity.quote_api_profile)
    .bind(identity.operation)
    .bind(&provider_id)
    .bind(identity.provider_model_id)
    .bind(identity.public_model_id)
    .bind(identity.media_kind)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO price_book_version_surface_contract_bindings (
            price_book_version_id, contract_key, contract_revision,
            contract_hash, bound_at_ms
        )
        VALUES ($1, $2, 1, $3, $4)
        "#,
    )
    .bind(price_book_version_id)
    .bind(&contract_key)
    .bind(&contract_hash)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE price_book_versions
            SET state = 'active', control_version = 2, updated_at_ms = $2
            WHERE price_book_version_id = $1
            "#,
        )
        .bind(price_book_version_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?,
        "v4 price version activation",
    )?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts (
            tenant_id, currency, credit_limit_micros, held_micros,
            captured_micros, created_at_ms, updated_at_ms
        )
        VALUES ($1, 'USD', $2, $2, 0, $3, $3)
        "#,
    )
    .bind(&tenant_id)
    .bind(max_total_micros)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO customer_price_quotes (
            quote_id, job_id, tenant_id, project_id,
            price_book_id, price_book_version_id,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier,
            execution_surface, request_dimensions_json,
            billing_mode, is_free, currency,
            max_total_micros, quote_hash, created_at_ms
        )
        VALUES ($1, $2, $3, 'project-test', $4, $5,
                $6, $7, $8, $9, $10,
                $11, 'standard', 'provider_cli', $12,
                'customer_rate', FALSE, 'USD', $13, $14, $15)
        "#,
    )
    .bind(quote_id)
    .bind(work.job_id)
    .bind(&tenant_id)
    .bind(price_book_id)
    .bind(price_book_version_id)
    .bind(identity.quote_api_profile)
    .bind(identity.operation)
    .bind(&provider_id)
    .bind(identity.provider_model_id)
    .bind(identity.public_model_id)
    .bind(identity.media_kind)
    .bind(&identity.dimensions)
    .bind(max_total_micros)
    .bind(hex::encode(Sha256::digest(quote_id.as_bytes())))
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    for output_id in output_ids {
        for (component_id, suffix, outcome, unit_price_micros, max_amount_micros) in [
            (
                success_price_component_id,
                "succeeded",
                "succeeded",
                basis.unit_price_micros,
                basis.max_amount_micros,
            ),
            (failed_price_component_id, "failed", "failed", 0, 0),
        ] {
            sqlx::query(
                r#"
                INSERT INTO customer_price_quote_lines (
                    quote_line_id, quote_id, job_id, price_component_id,
                    component_key, partition_key, terminal_outcome,
                    metric, unit, unit_size, unit_price_micros,
                    quantity_source, required_confidence, rounding_mode,
                    reservation_quantity_source, reservation_confidence,
                    dimensions_json, max_quantity, max_amount_micros, created_at_ms
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13, 'exact',
                    $12, $13, $14, $15, $16, $17
                )
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(quote_id)
            .bind(work.job_id)
            .bind(component_id)
            .bind(format!("{}.{}", basis.metric, suffix))
            .bind(format!("output:{output_id}"))
            .bind(outcome)
            .bind(basis.metric)
            .bind(basis.unit)
            .bind(basis.unit_size)
            .bind(unit_price_micros)
            .bind(basis.quantity_source)
            .bind(basis.confidence)
            .bind(&identity.dimensions)
            .bind(basis.max_quantity)
            .bind(max_amount_micros)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(debug_error)?;
        }
    }
    sqlx::query(
        r#"
        INSERT INTO customer_billing_holds (
            hold_id, quote_id, job_id, tenant_id, currency,
            held_micros, account_held_micros,
            state, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, $4, 'USD', $5, $5, 'held', $6, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(quote_id)
    .bind(work.job_id)
    .bind(&tenant_id)
    .bind(max_total_micros)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)
}

async fn seed_terminal_quota(pool: &PgPool, work: &WorkLease) -> TestResult {
    let (tenant_id, request_id, requested_units, billing_metric, billing_unit): (
        String,
        String,
        i32,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT tenant_id, request_id, requested_units, billing_metric, billing_unit
         FROM jobs WHERE job_id = $1",
    )
    .bind(work.job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let reservation_id = Uuid::new_v4();
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO quota_reservations
          (reservation_id, tenant_id, request_id, job_id, requested_units,
           committed_units, started_units, released_units, state,
           created_at_ms, updated_at_ms, expires_at_ms,
           limit_5h, remaining_5h, limit_7d, remaining_7d,
           billing_metric, billing_unit)
        VALUES ($1, $2, $3, $4, $5, 0, 0, 0, 'reserved',
                $6, $6, $7, 100, 99, 1000, 999, $8, $9)
        "#,
    )
    .bind(reservation_id)
    .bind(&tenant_id)
    .bind(&request_id)
    .bind(work.job_id)
    .bind(requested_units)
    .bind(now)
    .bind(now + 300_000)
    .bind(billing_metric)
    .bind(billing_unit)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    let changed = sqlx::query(
        "UPDATE jobs SET reservation_id = $2, state = 'running', updated_at_ms = $3 WHERE job_id = $1",
    )
    .bind(work.job_id)
    .bind(reservation_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?
    .rows_affected();
    require(changed == 1, "terminal quota did not bind its job")?;
    tx.commit().await.map_err(debug_error)
}

async fn bind_inline_profile(pool: &PgPool, work: &WorkLease) -> TestResult {
    let mut tx = pool.begin().await.map_err(debug_error)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE work_items
            SET execution_profile_id = $2,
                state = 'running',
                updated_at_ms = updated_at_ms + 1
            WHERE work_item_id = $1
              AND state = 'leased'
              AND execution_profile_id IS NULL
            "#,
        )
        .bind(work.work_item_id)
        .bind(CODEX_PROFILE_ID)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?,
        "inline profile binding",
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE job_attempts
            SET state = 'running',
                started_at_ms = updated_at_ms,
                updated_at_ms = updated_at_ms + 1
            WHERE execution_id = $1
              AND state = 'claimed'
            "#,
        )
        .bind(work.execution_id)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?,
        "inline attempt start",
    )?;
    tx.commit().await.map_err(debug_error)
}

async fn inline_usage_reservation(pool: &PgPool, work: &WorkLease) -> TestResult<UsageReservation> {
    let (
        reservation_id,
        tenant_id,
        request_id,
        provider_id,
        model,
        output_count,
        billable_units,
        billing_metric,
        limit_5h,
        remaining_5h,
        limit_7d,
        remaining_7d,
    ): (
        Uuid,
        String,
        String,
        String,
        String,
        i32,
        i32,
        String,
        i32,
        i32,
        i32,
        i32,
    ) = sqlx::query_as(
        r#"
        SELECT quota.reservation_id, job.tenant_id, job.request_id,
               job.provider_id, job.model, job.output_count, job.billable_units,
               job.billing_metric, quota.limit_5h, quota.remaining_5h,
               quota.limit_7d, quota.remaining_7d
        FROM jobs job
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
        WHERE job.job_id = $1
        "#,
    )
    .bind(work.job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let billing_metric = match billing_metric.as_str() {
        "output" => BillingMetric::Output,
        "request" => BillingMetric::Request,
        "video_second" => BillingMetric::VideoSecond,
        _ => {
            return Err(format!(
                "unsupported inline billing metric {billing_metric}"
            ));
        }
    };
    Ok(UsageReservation {
        reservation_id,
        job_id: work.job_id,
        charge: UsageCharge {
            tenant_id,
            attribution: None,
            request_id,
            admission_session_id: None,
            operation: "generation",
            provider_id,
            model,
            output_count: u32::try_from(output_count).map_err(debug_error)?,
            billable_units: u32::try_from(billable_units).map_err(debug_error)?,
            billing_metric,
            limits: UsageLimits {
                five_hour_image_limit: u32::try_from(limit_5h).map_err(debug_error)?,
                seven_day_image_limit: u32::try_from(limit_7d).map_err(debug_error)?,
            },
        },
        snapshot: UsageSnapshot {
            limit_5h: u32::try_from(limit_5h).map_err(debug_error)?,
            remaining_5h: u32::try_from(remaining_5h).map_err(debug_error)?,
            limit_7d: u32::try_from(limit_7d).map_err(debug_error)?,
            remaining_7d: u32::try_from(remaining_7d).map_err(debug_error)?,
        },
    })
}

async fn inline_generation_manifest(
    artifacts: &dyn ArtifactBlobStore,
    work: &WorkLease,
    reservation: &UsageReservation,
) -> TestResult<GenerationResultManifest> {
    let artifact = artifacts
        .put(
            ArtifactIdentity {
                artifact_id: Uuid::new_v4(),
                tenant_id: reservation.charge.tenant_id.clone(),
                job_id: work.job_id,
                work_item_id: work.work_item_id,
                execution_id: work.execution_id,
                lease_epoch: work.lease_epoch,
                output_index: 0,
                media_type: "image/png".to_string(),
            },
            &png_bytes([10, 20, 30, 255]),
        )
        .await
        .map_err(debug_error)?;
    Ok(GenerationResultManifest {
        job_id: work.job_id,
        tenant_id: reservation.charge.tenant_id.clone(),
        projection: GenerationResponseProjection {
            api_profile: "openai-images-v1".to_string(),
            operation: "generation".to_string(),
            response_schema: GENERATION_RESPONSE_SCHEMA.to_string(),
            created_at_seconds: 1_800_000_000,
            output_format: "png".to_string(),
            quality: "high".to_string(),
            size: "1024x1024".to_string(),
            background: "opaque".to_string(),
            stream: false,
            usage: reservation.snapshot.clone(),
        },
        artifacts: vec![artifact],
    })
}

async fn inline_customer_settlement_state(
    pool: &PgPool,
    work: &WorkLease,
) -> TestResult<InlineCustomerSettlementState> {
    sqlx::query_as(
        r#"
        SELECT job.state AS job_state, job.charged_units,
               quota.state AS quota_state, quota.committed_units,
               quota.released_units,
               (SELECT COUNT(*) FROM provider_usage_facts fact
                WHERE fact.job_id = job.job_id
                  AND fact.attempt_execution_id = $2
                  AND fact.submission_id IS NULL
                  AND fact.receipt_id IS NULL
                  AND fact.fact_domain = 'customer_billable'
                  AND fact.metric = 'image_output'
                  AND fact.quantity = 1) AS usage_fact_count,
               (SELECT COUNT(*) FROM customer_rated_usage rating
                WHERE rating.job_id = job.job_id) AS customer_rating_count,
               (SELECT COUNT(*) FROM ledger_transactions ledger
                WHERE ledger.source_job_id = job.job_id
                  AND ledger.transaction_type = 'customer_job_charge')
                 AS customer_charge_count,
               hold.state AS hold_state, hold.captured_micros,
               hold.released_micros, account.held_micros AS account_held_micros,
               account.captured_micros AS account_captured_micros
        FROM jobs job
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
        JOIN customer_billing_holds hold ON hold.job_id = job.job_id
        JOIN billing_accounts account
          ON account.tenant_id = job.tenant_id
         AND account.currency = hold.currency
        WHERE job.job_id = $1
        "#,
    )
    .bind(work.job_id)
    .bind(work.execution_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)
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
           requested_units, output_count, billable_units, billing_metric, billing_unit,
           economics_contract_version, created_at_ms, updated_at_ms)
        VALUES ($1, 'executor-test', $2, 'generation', 'provider-test', 'model-test',
                'reserved', $3, $3, $3, 'output', 'output', 2, $4, $4)
        "#,
    )
    .bind(job_id)
    .bind(&request_id)
    .bind(requested_units)
    .bind(now)
    .execute(pool)
    .await
    .map_err(|error| format!("job seed failed: {error}"))?;
    for output_index in 0..requested_units {
        sqlx::query(
            r#"
            INSERT INTO job_outputs
              (output_id, job_id, output_index, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, 'pending', $4, $4)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(job_id)
        .bind(output_index)
        .bind(now)
        .execute(pool)
        .await
        .map_err(|error| format!("output seed failed: {error}"))?;
    }
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
    seed_codex_generation_lease_with_outputs(pool, worker_id, 1).await
}

async fn seed_codex_generation_lease_with_outputs(
    pool: &PgPool,
    worker_id: &str,
    output_count: u32,
) -> TestResult<WorkLease> {
    let job_id = Uuid::new_v4();
    let request_id = format!("request-{}", Uuid::new_v4().simple());
    let job = GenerationJob {
        request_id: request_id.clone(),
        model: "gpt-image-2".to_string(),
        prompt: "draw a process-smoke lighthouse".to_string(),
        moderation: "auto".to_string(),
        n: output_count,
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
    seed_generation_lease(
        pool,
        worker_id,
        GenerationLeaseSeed {
            job_id,
            admission_session_id: Uuid::new_v4(),
            owner_token: Uuid::new_v4(),
            api_profile: "openai-images-v1".to_string(),
            provider_id: "openai-codex".to_string(),
            model: "gpt-image-2".to_string(),
            request_id,
            request_hash,
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            command_json,
            operation: "generation".to_string(),
            work_kind: "image_batch".to_string(),
            output_count,
            billable_units: output_count,
            output_billable_units: 1,
            billing_metric: "output".to_string(),
            billing_unit: "output".to_string(),
        },
    )
    .await
}

async fn seed_dreamina_generation_lease_for_profile(
    pool: &PgPool,
    worker_id: &str,
    api_profile: &str,
) -> TestResult<WorkLease> {
    let job_id = Uuid::new_v4();
    let owner_token = Uuid::new_v4();
    let request_id = format!("request-{}", Uuid::new_v4().simple());
    let plan = DreaminaImageAdmissionPlan::new(DreaminaImageGenerationRequest {
        prompt: "draw a provider-native pricing lighthouse".to_string(),
        model_version: Some("5.0".to_string()),
        ratio: Some("1:1".to_string()),
        resolution_type: "2k".to_string(),
        width: None,
        height: None,
        generate_num: Some(1),
    })
    .map_err(debug_error)?;
    let claim = plan.claim_for_profile(
        api_profile,
        owner_token,
        "executord-process-smoke",
        "project-test",
        request_id.clone(),
        None,
        i64::MAX,
    );
    let attach = plan.attach(
        AdmissionTicket {
            session_id: Uuid::new_v4(),
            owner_token,
            request_hash: claim.request_hash.clone(),
        },
        job_id,
        "dreamina-terminal-test",
    );
    seed_generation_lease(
        pool,
        worker_id,
        GenerationLeaseSeed {
            job_id,
            admission_session_id: attach.ticket.session_id,
            owner_token,
            api_profile: claim.api_profile,
            provider_id: plan.provider_id().to_string(),
            model: plan.provider_model().to_string(),
            request_id,
            request_hash: claim.request_hash,
            command_schema: attach.command_schema,
            command_json: attach.command_json,
            operation: "generation".to_string(),
            work_kind: "image_batch".to_string(),
            output_count: plan.output_count(),
            billable_units: plan.output_count(),
            output_billable_units: 1,
            billing_metric: "output".to_string(),
            billing_unit: "output".to_string(),
        },
    )
    .await
}

async fn seed_dreamina_video_generation_lease_for_profile(
    pool: &PgPool,
    worker_id: &str,
    api_profile: &str,
) -> TestResult<WorkLease> {
    let job_id = Uuid::new_v4();
    let owner_token = Uuid::new_v4();
    let request_id = format!("request-{}", Uuid::new_v4().simple());
    let plan = DreaminaVideoAdmissionPlan::new(DreaminaVideoGenerationRequest {
        prompt: "animate a provider-native pricing lighthouse".to_string(),
        model_version: Some("seedance2.0fast".to_string()),
        ratio: Some("9:16".to_string()),
        duration: Some(8),
        video_resolution: "720p".to_string(),
    })
    .map_err(debug_error)?;
    let claim = plan.claim_for_profile(
        api_profile,
        owner_token,
        "executord-process-smoke",
        "project-test",
        request_id.clone(),
        None,
        i64::MAX,
    );
    let attach = plan.attach(
        AdmissionTicket {
            session_id: Uuid::new_v4(),
            owner_token,
            request_hash: claim.request_hash.clone(),
        },
        job_id,
        "dreamina-video-terminal-test",
    );
    seed_generation_lease(
        pool,
        worker_id,
        GenerationLeaseSeed {
            job_id,
            admission_session_id: attach.ticket.session_id,
            owner_token,
            api_profile: claim.api_profile,
            provider_id: plan.provider_id().to_string(),
            model: plan.provider_model().to_string(),
            request_id,
            request_hash: claim.request_hash,
            command_schema: attach.command_schema,
            command_json: attach.command_json,
            operation: VIDEO_GENERATION_OPERATION.to_string(),
            work_kind: "video_single".to_string(),
            output_count: 1,
            billable_units: u32::from(plan.duration()),
            output_billable_units: u32::from(plan.duration()),
            billing_metric: "video_second".to_string(),
            billing_unit: "second".to_string(),
        },
    )
    .await
}

struct GenerationLeaseSeed {
    job_id: Uuid,
    admission_session_id: Uuid,
    owner_token: Uuid,
    api_profile: String,
    provider_id: String,
    model: String,
    request_id: String,
    request_hash: String,
    command_schema: String,
    command_json: serde_json::Value,
    operation: String,
    work_kind: String,
    output_count: u32,
    billable_units: u32,
    output_billable_units: u32,
    billing_metric: String,
    billing_unit: String,
}

async fn seed_generation_lease(
    pool: &PgPool,
    worker_id: &str,
    seed: GenerationLeaseSeed,
) -> TestResult<WorkLease> {
    let output_count = i32::try_from(seed.output_count)
        .map_err(|_| "generation output count exceeds i32".to_string())?;
    let billable_units = i32::try_from(seed.billable_units)
        .map_err(|_| "generation billable units exceed i32".to_string())?;
    let output_billable_units = i32::try_from(seed.output_billable_units)
        .map_err(|_| "output billable units exceed i32".to_string())?;
    require(
        output_count > 0
            && billable_units > 0
            && output_billable_units > 0
            && billable_units == output_count * output_billable_units,
        "generation output count must be positive",
    )?;
    let work_item_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let now = database_now(pool).await?;
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model, state,
           requested_units, output_count, billable_units, billing_metric, billing_unit,
           economics_contract_version, created_at_ms, updated_at_ms)
        VALUES ($1, 'executord-process-smoke', $2, $3, $4,
                $5, 'reserved', $6, $7, $8, $9, $10, 2, $11, $11)
        "#,
    )
    .bind(seed.job_id)
    .bind(&seed.request_id)
    .bind(&seed.operation)
    .bind(&seed.provider_id)
    .bind(&seed.model)
    .bind(billable_units)
    .bind(output_count)
    .bind(billable_units)
    .bind(&seed.billing_metric)
    .bind(&seed.billing_unit)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    for output_index in 0..output_count {
        sqlx::query(
            r#"
            INSERT INTO job_outputs
              (output_id, job_id, output_index, billable_units,
               state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, 'pending', $5, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(seed.job_id)
        .bind(output_index)
        .bind(output_billable_units)
        .bind(now)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    }
    sqlx::query(
        r#"
        INSERT INTO admission_sessions
          (session_id, owner_token, tenant_id, project_id, api_profile, operation,
           request_id, request_hash, state, job_id, deadline_at_ms, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'executord-process-smoke', 'project-test', $3,
                $4, $5, $6, 'attached', $7, $8, $9, $9)
        "#,
    )
    .bind(seed.admission_session_id)
    .bind(seed.owner_token)
    .bind(&seed.api_profile)
    .bind(&seed.operation)
    .bind(&seed.request_id)
    .bind(&seed.request_hash)
    .bind(seed.job_id)
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
    .bind(seed.job_id)
    .bind(seed.admission_session_id)
    .bind(&seed.command_schema)
    .bind(&seed.command_json)
    .bind(&seed.request_hash)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO idempotency_requests
          (project_id, api_profile, operation, key_digest, tenant_id,
           request_hash, session_id, job_id, state, created_at_ms, updated_at_ms)
        VALUES ('project-test', $1, $2, $3,
                'executord-process-smoke', $4, $5, $6, 'accepted', $7, $7)
        "#,
    )
    .bind(&seed.api_profile)
    .bind(&seed.operation)
    .bind(sha256(seed.request_id.as_bytes()))
    .bind(&seed.request_hash)
    .bind(seed.admission_session_id)
    .bind(seed.job_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms, lease_epoch,
           lease_owner, lease_expires_at_ms, execution_id, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 'leased', $5, 7, $4, $6, $7, $5, $5)
        "#,
    )
    .bind(work_item_id)
    .bind(seed.job_id)
    .bind(&seed.work_kind)
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
        job_id: seed.job_id,
        execution_id,
        lease_epoch: 7,
        worker_id: worker_id.to_string(),
        command_schema: seed.command_schema,
        command_json: seed.command_json,
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
                "#!/bin/sh\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\n{delay}/bin/cp '{}' sealed-output.bin\n",
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

    async fn command(
        &self,
        database: &TestDatabase,
        owner: &str,
    ) -> TestResult<tokio::process::Command> {
        self.command_with_lease(database, owner, 10_000, 250).await
    }

    async fn command_with_lease(
        &self,
        database: &TestDatabase,
        owner: &str,
        lease_ms: u64,
        heartbeat_ms: u64,
    ) -> TestResult<tokio::process::Command> {
        let now = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_environments (
                provider_account_id, provider_id, environment_kind, environment_ref,
                upstream_identity_sha256, display_name, account_email, state,
                created_at_ms, updated_at_ms
            )
            VALUES ($1, 'openai-codex', 'codex_home_v1', $2, $3,
                    'Executor process test', NULL, 'active', $4, $4)
            ON CONFLICT (provider_account_id) DO UPDATE
            SET environment_ref = EXCLUDED.environment_ref,
                upstream_identity_sha256 = EXCLUDED.upstream_identity_sha256,
                state = 'active',
                updated_at_ms = EXCLUDED.updated_at_ms
            "#,
        )
        .bind(CODEX_ACCOUNT_ID)
        .bind(self.credentials.to_string_lossy().as_ref())
        .bind(CODEX_AUTH_SHA256)
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
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
            .env("EXECUTOR_PROFILE_KEY", "openai-codex-generation-v1")
            .env("EXECUTOR_CREDENTIAL_REF", "test-vault.openai-codex.1")
            .env("EXECUTOR_CREDENTIAL_REVISION", "1")
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
            if path.file_name() != Some(std::ffi::OsStr::new(".storage-namespace-id")) {
                files.push(path);
            }
        } else {
            return Err(format!("unexpected artifact entry: {}", path.display()));
        }
    }
    Ok(files)
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

async fn database_now(pool: &PgPool) -> TestResult<i64> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(debug_error)
}

async fn seed_execution_profiles(pool: &PgPool) -> TestResult {
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    for (pool_id, pool_key, provider_id) in [
        (TEST_POOL_ID, "provider-test-pool", "provider-test"),
        (CODEX_POOL_ID, "openai-codex-pool", "openai-codex"),
        (
            DREAMINA_POOL_ID,
            "dreamina-image-pool",
            DREAMINA_PROVIDER_ID,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO provider_credential_pools
              (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, 'enabled', $4, $4)
            "#,
        )
        .bind(pool_id)
        .bind(pool_key)
        .bind(provider_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    }
    for (account_id, pool_id, provider_id, account_key, credential_ref, auth_sha256) in [
        (
            TEST_ACCOUNT_ID,
            TEST_POOL_ID,
            "provider-test",
            "provider-test-account",
            "test-vault.provider-test.1",
            TEST_AUTH_SHA256,
        ),
        (
            CODEX_ACCOUNT_ID,
            CODEX_POOL_ID,
            "openai-codex",
            "openai-codex-account",
            "test-vault.openai-codex.1",
            CODEX_AUTH_SHA256,
        ),
        (
            DREAMINA_ACCOUNT_ID,
            DREAMINA_POOL_ID,
            DREAMINA_PROVIDER_ID,
            "dreamina-image-account",
            "test-vault.dreamina.1",
            DREAMINA_AUTH_SHA256,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO provider_accounts
              (provider_account_id, credential_pool_id, provider_id, account_key,
               credential_ref, credential_revision, credential_auth_sha256,
               state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, 1, $6, 'enabled', $7, $7)
            "#,
        )
        .bind(account_id)
        .bind(pool_id)
        .bind(provider_id)
        .bind(account_key)
        .bind(credential_ref)
        .bind(auth_sha256)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    }
    for (policy_id, pool_id, account_id, provider_id, execution_class) in [
        (
            TEST_POLICY_ID,
            TEST_POOL_ID,
            TEST_ACCOUNT_ID,
            "provider-test",
            "provider-test",
        ),
        (
            CODEX_POLICY_ID,
            CODEX_POOL_ID,
            CODEX_ACCOUNT_ID,
            "openai-codex",
            "agentic-cli",
        ),
        (
            DREAMINA_POLICY_ID,
            DREAMINA_POOL_ID,
            DREAMINA_ACCOUNT_ID,
            DREAMINA_PROVIDER_ID,
            "remote-task",
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO executor_resource_policies
              (resource_policy_id, revision, credential_pool_id,
               provider_account_id, provider_id, execution_class,
               max_concurrency, state, created_at_ms)
            VALUES ($1, 1, $2, $3, $4, $5, 1000, 'enabled', $6)
            "#,
        )
        .bind(policy_id)
        .bind(pool_id)
        .bind(account_id)
        .bind(provider_id)
        .bind(execution_class)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    }
    for (
        profile_id,
        profile_key,
        provider_id,
        command_schema,
        operation_id,
        operation_descriptor_revision,
        operation_descriptor_sha256_v1,
        completion_mode,
        idempotency_mode,
        adapter_revision,
        pool_id,
        account_id,
        credential_ref,
        policy_id,
    ) in [
        (
            TEST_PROFILE_ID,
            "provider-test-generation-v1",
            "provider-test",
            "provider-command-v1",
            "images.generations",
            "provider-test/images.generations/v1",
            "2".repeat(64),
            "inline",
            "submission_bound",
            "provider-test-adapter-v1",
            TEST_POOL_ID,
            TEST_ACCOUNT_ID,
            "test-vault.provider-test.1",
            TEST_POLICY_ID,
        ),
        (
            CODEX_PROFILE_ID,
            "openai-codex-generation-v1",
            "openai-codex",
            GENERATION_COMMAND_SCHEMA,
            "images.generations",
            "openai-codex/images.generations/v1",
            "f7f3e84594bfda2312d9420aa22108e76b10b3b22c52535ccf768f944d9b7aaa".to_string(),
            "inline",
            "submission_bound",
            CODEX_GENERATION_ADAPTER_REVISION,
            CODEX_POOL_ID,
            CODEX_ACCOUNT_ID,
            "test-vault.openai-codex.1",
            CODEX_POLICY_ID,
        ),
        (
            DREAMINA_PROFILE_ID,
            "dreamina-image-generation-v1",
            DREAMINA_PROVIDER_ID,
            DREAMINA_SUBMIT_COMMAND_SCHEMA,
            DREAMINA_IMAGE_GENERATION_OPERATION_V1.id,
            DREAMINA_IMAGE_GENERATION_OPERATION_V1.descriptor_revision,
            DREAMINA_IMAGE_GENERATION_OPERATION_V1.canonical_sha256_v1_hex(),
            DREAMINA_IMAGE_GENERATION_OPERATION_V1.completion.as_str(),
            DREAMINA_IMAGE_GENERATION_OPERATION_V1.idempotency.as_str(),
            DREAMINA_ADAPTER_REVISION,
            DREAMINA_POOL_ID,
            DREAMINA_ACCOUNT_ID,
            "test-vault.dreamina.1",
            DREAMINA_POLICY_ID,
        ),
        (
            DREAMINA_VIDEO_PROFILE_ID,
            "dreamina-video-generation-v1",
            DREAMINA_PROVIDER_ID,
            DREAMINA_SUBMIT_COMMAND_SCHEMA,
            DREAMINA_VIDEO_GENERATION_OPERATION_V1.id,
            DREAMINA_VIDEO_GENERATION_OPERATION_V1.descriptor_revision,
            DREAMINA_VIDEO_GENERATION_OPERATION_V1.canonical_sha256_v1_hex(),
            DREAMINA_VIDEO_GENERATION_OPERATION_V1.completion.as_str(),
            DREAMINA_VIDEO_GENERATION_OPERATION_V1.idempotency.as_str(),
            DREAMINA_ADAPTER_REVISION,
            DREAMINA_POOL_ID,
            DREAMINA_ACCOUNT_ID,
            "test-vault.dreamina.1",
            DREAMINA_POLICY_ID,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO provider_execution_profiles
              (execution_profile_id, profile_key, provider_id, command_schema,
               operation_id, operation_descriptor_revision,
               operation_descriptor_sha256_v1, completion_mode, idempotency_mode,
               adapter_revision, credential_pool_id, provider_account_id,
               credential_ref, credential_revision, resource_policy_id,
               resource_policy_revision, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                    $10, $11, $12, $13, 1, $14, 1, 'enabled', $15, $15)
            "#,
        )
        .bind(profile_id)
        .bind(profile_key)
        .bind(provider_id)
        .bind(command_schema)
        .bind(operation_id)
        .bind(operation_descriptor_revision)
        .bind(operation_descriptor_sha256_v1)
        .bind(completion_mode)
        .bind(idempotency_mode)
        .bind(adapter_revision)
        .bind(pool_id)
        .bind(account_id)
        .bind(credential_ref)
        .bind(policy_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    }
    tx.commit().await.map_err(debug_error)?;
    Ok(())
}

async fn seed_limited_test_profile(
    pool: &PgPool,
    max_concurrency: i32,
) -> TestResult<(Uuid, Uuid)> {
    let profile_id = Uuid::new_v4();
    let policy_id = Uuid::new_v4();
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        "UPDATE executor_resource_policies SET state = 'disabled' WHERE resource_policy_id = $1 AND revision = 1",
    )
    .bind(TEST_POLICY_ID)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_resource_policies
          (resource_policy_id, revision, credential_pool_id, provider_account_id,
           provider_id, execution_class, max_concurrency, state, created_at_ms)
        VALUES ($1, 1, $2, $3, 'provider-test', 'provider-test-limited',
                $4, 'enabled', $5)
        "#,
    )
    .bind(policy_id)
    .bind(TEST_POOL_ID)
    .bind(TEST_ACCOUNT_ID)
    .bind(max_concurrency)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_execution_profiles
          (execution_profile_id, profile_key, provider_id, command_schema,
           operation_id, operation_descriptor_revision,
           operation_descriptor_sha256_v1, completion_mode, idempotency_mode,
           adapter_revision, credential_pool_id, provider_account_id,
           credential_ref, credential_revision, resource_policy_id,
           resource_policy_revision, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-test', 'provider-command-v1',
                'images.generations', 'provider-test/images.generations/v1',
                $3, 'inline', 'submission_bound',
                'provider-test-adapter-v1', $4, $5,
                'test-vault.provider-test.1', 1, $6, 1,
                'enabled', $7, $7)
        "#,
    )
    .bind(profile_id)
    .bind(format!("provider-test-limited-{}", profile_id.simple()))
    .bind("2".repeat(64))
    .bind(TEST_POOL_ID)
    .bind(TEST_ACCOUNT_ID)
    .bind(policy_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)?;
    Ok((profile_id, policy_id))
}

fn test_scope(execution_profile_id: Uuid) -> ExecutorClaimScope {
    ExecutorClaimScope {
        execution_profile_id,
        provider_id: "provider-test".to_string(),
        command_schema: "provider-command-v1".to_string(),
        adapter_revision: "provider-test-adapter-v1".to_string(),
    }
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
        if let Err(error) = seed_execution_profiles(&pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return Err(format!("execution profile seed failed: {error}"));
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
