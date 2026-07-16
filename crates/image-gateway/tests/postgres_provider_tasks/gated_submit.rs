use std::{
    collections::BTreeMap,
    fs,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use gpt_image_2_gateway::{
    GatedCliBinding, GatedCliCommand, GatedCliObservation, GatedCliSubmission, GatedCliSubmitCodec,
    GatedCliSubmitDriver,
};

use super::*;

#[tokio::test]
async fn orchestrator_runs_one_cli_for_concurrent_callers_and_replays() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "gated-orchestrator-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let side_effect = journal.path().join("provider-invoked");
        let codec = FakeGatedSubmitCodec::new(journal.path(), &side_effect, "gated-operation-1")?;
        let workspace_root = codec.workspace_root.clone();
        let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
        let runner_sha256 = file_sha256(runner)?;
        let orchestrator = Arc::new(
            ProviderSubmitOrchestrator::new(
                PostgresProviderTaskStore::new(database.pool.clone()),
                gated_submit_driver(codec.clone(), runner, &runner_sha256)?,
                60_000,
                &journal_root,
            )
            .map_err(debug_error)?,
        );

        let mut tasks = Vec::new();
        for _ in 0..32 {
            let orchestrator = Arc::clone(&orchestrator);
            let work = gated_orchestrator_work(&lease)?;
            tasks.push(tokio::spawn(async move { orchestrator.submit(work).await }));
        }
        let mut attached = 0;
        for task in tasks {
            if matches!(
                task.await.map_err(debug_error)?.map_err(debug_error)?,
                ProviderSubmitOutcome::Attached(_)
            ) {
                attached += 1;
            }
        }
        require(
            attached >= 1,
            "concurrent gated submit did not attach a provider task",
        )?;
        let restarted = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            gated_submit_driver(codec, runner, &runner_sha256)?,
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?;
        let replay = restarted
            .submit(gated_orchestrator_work(&lease)?)
            .await
            .map_err(debug_error)?;
        require(
            matches!(replay, ProviderSubmitOutcome::Attached(ref task)
                if task.remote_operation_id == "gated-operation-1")
                && fs::read(&side_effect).map_err(debug_error)? == b"invoked"
                && workspace_attempt_count(&workspace_root)? == 0,
            format!("gated submit relaunched or failed to replay: {replay:?}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn recovery_releases_the_same_ready_process_after_journal_release() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease =
            seed_running_submission(&database.pool, "gated-release-recovery-worker").await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let command = orchestrator_command(&lease);
        let reservation = RemoteTaskSubmitReservation::new(
            &lease,
            format!("provider-submit-{}", lease.submission_id.simple()),
            command.output(),
            command.identity(),
            60_000,
        );
        let acquired = store
            .acquire_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let ProviderSubmitAcquire::Dispatch(authority) = acquired else {
            return Err(format!(
                "initial gated acquire did not dispatch: {acquired:?}"
            ));
        };
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let side_effect = journal.path().join("provider-invoked");
        let codec =
            FakeGatedSubmitCodec::new(journal.path(), &side_effect, "recovered-gated-operation")?;
        let launch_nonce = seed_remote_submit_launch_prefix(
            &journal_root,
            authority.intent(),
            authority.context(),
            &command,
        )?;
        let binding = GatedCliBinding::new(
            authority.context().execution_binding_sha256(),
            launch_nonce,
            u64::try_from(authority.context().provider_deadline_at_ms()).map_err(debug_error)?,
        )
        .map_err(debug_error)?;
        let submission =
            GatedCliSubmission::new(&journal_root, lease.submission_id).map_err(debug_error)?;
        let process_workspace = gated_attempt_workspace(&codec, lease.submission_id, launch_nonce)?;
        let process_workspace_path = process_workspace.path().to_owned();
        let process_command = codec
            .gated_command(&process_workspace)
            .map_err(debug_error)?;
        submission
            .prepare(&binding, &process_command)
            .map_err(debug_error)?;
        let runner_path = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
        let mut runner_command = tokio::process::Command::new(runner_path);
        runner_command
            .arg(&journal_root)
            .arg(lease.submission_id.to_string())
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut runner = runner_command.spawn().map_err(debug_error)?;
        let _ready = wait_gated_ready(&submission, &binding).await?;
        require(
            !side_effect.exists() && process_workspace_path.is_dir(),
            "ready gated process invoked provider or lost its private workspace",
        )?;
        seed_remote_submit_dispatch_release(
            &journal_root,
            lease.submission_id,
            authority.context().execution_binding_sha256(),
            launch_nonce,
        )?;
        drop(authority);

        let recovered = ProviderSubmitOrchestrator::new(
            store,
            gated_submit_driver(codec, runner_path, file_sha256(runner_path)?)?,
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(
            ProviderSubmitWork::<GatedCliSubmitDriver<FakeGatedSubmitCodec>>::new(&lease, command)
                .map_err(debug_error)?,
        )
        .await
        .map_err(debug_error)?;
        let runner_status = runner.wait().await.map_err(debug_error)?;
        require(
            runner_status.success()
                && matches!(recovered, ProviderSubmitOutcome::Attached(ref task)
                    if task.remote_operation_id == "recovered-gated-operation")
                && fs::read(&side_effect).map_err(debug_error)? == b"invoked",
            format!("released gated process was not recovered exactly once: {recovered:?}"),
        )?;
        require(
            !process_workspace_path.exists(),
            "terminal recovery left the submit attempt workspace behind",
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn replaced_submit_workspace_root_fails_before_process_or_provider() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease =
            seed_running_submission(&database.pool, "gated-workspace-replaced-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let side_effect = journal.path().join("provider-invoked");
        let codec =
            FakeGatedSubmitCodec::new(journal.path(), &side_effect, "workspace-must-not-run")?;
        let workspace_root = codec.workspace_root.clone();
        let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
        let driver = gated_submit_driver(codec, runner, file_sha256(runner)?)?;

        let moved_workspace = journal.path().join("moved-provider-submit-workspace");
        fs::rename(&workspace_root, &moved_workspace).map_err(debug_error)?;
        fs::create_dir(&workspace_root).map_err(debug_error)?;
        fs::set_permissions(&workspace_root, fs::Permissions::from_mode(0o700))
            .map_err(debug_error)?;

        let outcome = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            driver,
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(gated_orchestrator_work(&lease)?)
        .await
        .map_err(debug_error)?;
        let durable: (String, String, Option<String>) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, submission.error_code
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(lease.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;

        require(
            matches!(outcome, ProviderSubmitOutcome::Terminal(_))
                && durable
                    == (
                        "failed".to_owned(),
                        "failed".to_owned(),
                        Some("provider_submit_workspace_invalid".to_owned()),
                    )
                && !side_effect.exists()
                && workspace_attempt_count(&workspace_root)? == 0
                && workspace_attempt_count(&moved_workspace)? == 0,
            format!(
                "replaced submit workspace escaped fail-closed handling: {outcome:?}/{durable:?}"
            ),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn codec_cannot_replace_the_platform_allocated_submit_workspace() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease =
            seed_running_submission(&database.pool, "gated-workspace-binding-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let side_effect = journal.path().join("provider-invoked");
        let codec =
            FakeGatedSubmitCodec::new(journal.path(), &side_effect, "workspace-must-not-run")?
                .with_workspace_mismatch();
        let workspace_root = codec.workspace_root.clone();
        let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));

        let outcome = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            gated_submit_driver(codec, runner, file_sha256(runner)?)?,
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(gated_orchestrator_work(&lease)?)
        .await
        .map_err(debug_error)?;
        let durable: (String, String, Option<String>) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, submission.error_code
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(lease.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;

        require(
            matches!(outcome, ProviderSubmitOutcome::Terminal(_))
                && durable
                    == (
                        "failed".to_owned(),
                        "failed".to_owned(),
                        Some("provider_submit_workspace_binding_mismatch".to_owned()),
                    )
                && !side_effect.exists()
                && workspace_attempt_count(&workspace_root)? == 0,
            format!("codec escaped the platform submit workspace: {outcome:?}/{durable:?}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn replays_process_receipt_after_database_transaction_failure() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease =
            seed_running_submission(&database.pool, "gated-receipt-recovery-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let side_effect = journal.path().join("provider-invoked");
        let codec =
            FakeGatedSubmitCodec::new(journal.path(), &side_effect, "gated-durable-operation")?;
        let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
        let runner_sha256 = file_sha256(runner)?;
        sqlx::query(
            r#"
            CREATE FUNCTION reject_gated_provider_submit_receipt()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF NEW.state = 'operation_known' THEN
                RAISE EXCEPTION 'injected gated receipt transaction failure';
              END IF;
              RETURN NEW;
            END
            $$
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            CREATE TRIGGER reject_gated_provider_submit_receipt
            BEFORE UPDATE ON provider_remote_submit_intents
            FOR EACH ROW EXECUTE FUNCTION reject_gated_provider_submit_receipt()
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        let first = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            gated_submit_driver(codec.clone(), runner, &runner_sha256)?,
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(gated_orchestrator_work(&lease)?)
        .await;
        require(
            matches!(first, Err(ProviderSubmitOrchestratorError::Store(_)))
                && fs::read(&side_effect).map_err(debug_error)? == b"invoked",
            format!("gated receipt fault was not injected after one CLI call: {first:?}"),
        )?;
        let state_after_failure: String = sqlx::query_scalar(
            "SELECT state FROM provider_remote_submit_intents WHERE submission_id = $1",
        )
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state_after_failure == "sending",
            format!("failed gated receipt transaction partially committed: {state_after_failure}"),
        )?;

        sqlx::query(
            "DROP TRIGGER reject_gated_provider_submit_receipt ON provider_remote_submit_intents",
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query("DROP FUNCTION reject_gated_provider_submit_receipt()")
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        let recovered = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            gated_submit_driver(codec, runner, &runner_sha256)?,
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(gated_orchestrator_work(&lease)?)
        .await
        .map_err(debug_error)?;
        require(
            matches!(recovered, ProviderSubmitOutcome::Attached(ref task)
                if task.remote_operation_id == "gated-durable-operation")
                && fs::read(&side_effect).map_err(debug_error)? == b"invoked",
            format!("durable gated receipt was not replayed without a second CLI: {recovered:?}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn expired_pre_release_budget_never_invokes_the_cli() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "gated-expired-budget-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let side_effect = journal.path().join("provider-invoked");
        let projection_delay = Duration::from_millis(750);
        let codec =
            FakeGatedSubmitCodec::new(journal.path(), &side_effect, "expired-budget-operation")?
                .with_projection_delay(projection_delay);
        let workspace_root = codec.workspace_root.clone();
        let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
        let started = Instant::now();
        let outcome = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            gated_submit_driver(codec, runner, file_sha256(runner)?)?,
            500,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(gated_orchestrator_work(&lease)?)
        .await
        .map_err(debug_error)?;
        require(
            started.elapsed() >= projection_delay
                && matches!(outcome, ProviderSubmitOutcome::Terminal(_))
                && !side_effect.exists()
                && workspace_attempt_count(&workspace_root)? == 0,
            format!(
                "expired pre-release budget invoked the CLI or skipped projection: {outcome:?}"
            ),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn prepared_gate_expiring_before_dispatch_reaches_terminal_and_cleans_workspace() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease =
            seed_running_submission(&database.pool, "gated-expired-prepared-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let side_effect = journal.path().join("provider-invoked");
        let codec = FakeGatedSubmitCodec::new(journal.path(), &side_effect, "must-not-dispatch")?;
        let workspace_root = codec.workspace_root.clone();
        let real_runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
        let delayed_runner = journal.path().join("delayed-remote-submit-runner");
        fs::write(
            &delayed_runner,
            format!(
                "#!/bin/sh\n/bin/sleep 0.15\nexec '{}' \"$@\"\n",
                real_runner.display()
            ),
        )
        .map_err(debug_error)?;
        fs::set_permissions(&delayed_runner, fs::Permissions::from_mode(0o500))
            .map_err(debug_error)?;

        let outcome = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            gated_submit_driver(codec, &delayed_runner, file_sha256(&delayed_runner)?)?,
            75,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(gated_orchestrator_work(&lease)?)
        .await
        .map_err(debug_error)?;
        let durable: (String, String, Option<String>) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, submission.error_code
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(lease.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let process_terminal = journal_root
            .join(lease.submission_id.simple().to_string())
            .join("process-terminal.json");

        require(
            matches!(outcome, ProviderSubmitOutcome::Terminal(_))
                && durable
                    == (
                        "failed".to_owned(),
                        "failed".to_owned(),
                        Some("provider_submit_deadline_elapsed".to_owned()),
                    )
                && process_terminal.is_file()
                && !side_effect.exists()
                && workspace_attempt_count(&workspace_root)? == 0,
            format!(
                "expired prepared gate leaked work or skipped terminal cleanup: \
                 {outcome:?}/{durable:?}"
            ),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn recovery_claim_completes_the_elected_unlaunched_attempt_once() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission_with_lease(
            &database.pool,
            "gated-unlaunched-recovery-worker",
            200,
        )
        .await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let command = orchestrator_command(&lease);
        let reservation = RemoteTaskSubmitReservation::new(
            &lease,
            format!("provider-submit-{}", lease.submission_id.simple()),
            command.output(),
            command.identity(),
            60_000,
        );
        let acquired = store
            .acquire_submit(&reservation)
            .await
            .map_err(debug_error)?;
        require(
            matches!(acquired, ProviderSubmitAcquire::Dispatch(_)),
            format!("fresh submit did not elect one dispatch: {acquired:?}"),
        )?;
        drop(acquired);

        tokio::time::sleep(Duration::from_millis(250)).await;
        let recovery = store
            .claim_submit_recovery(
                &claim_scope(),
                "gated-unlaunched-recovery",
                "claim-unlaunched-recovery",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "unlaunched submit was not recovery-claimable".to_owned())?;
        let expected_command_json: serde_json::Value =
            sqlx::query_scalar("SELECT command_json FROM job_payloads WHERE job_id = $1")
                .bind(lease.job_id)
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        require(
            recovery.command_json() == &expected_command_json
                && (1..=60_000).contains(&recovery.remaining_budget_ms()),
            "recovery claim did not freeze the durable command and database budget",
        )?;
        let recovery_debug = format!("{recovery:?}");
        require(
            recovery_debug.contains("[redacted]")
                && !recovery_debug.contains(&expected_command_json.to_string()),
            "recovery debug output exposed the frozen source command",
        )?;

        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let side_effect = journal.path().join("provider-invoked");
        let codec =
            FakeGatedSubmitCodec::new(journal.path(), &side_effect, "recovered-unlaunched")?;
        let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
        let recovered = ProviderSubmitOrchestrator::new(
            store,
            gated_submit_driver(codec, runner, file_sha256(runner)?)?,
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?
        .recover(ProviderSubmitRecoveryWork::new(&recovery, command).map_err(debug_error)?)
        .await
        .map_err(debug_error)?;
        let attached_fence: (Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT attach_recovery_owner, attach_recovery_lease_epoch \
             FROM provider_remote_tasks WHERE submission_id = $1",
        )
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            matches!(recovered, ProviderSubmitOutcome::Attached(ref task)
                if task.remote_operation_id == "recovered-unlaunched")
                && fs::read(&side_effect).map_err(debug_error)? == b"invoked"
                && attached_fence.0.as_deref() == Some(recovery.recovery_owner.as_str())
                && attached_fence.1 == Some(recovery.recovery_lease_epoch),
            format!("recovery did not complete the elected attempt exactly once: {recovered:?}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn outcome_unknown_recovery_observes_without_relaunching_cli() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission_with_lease(
            &database.pool,
            "gated-unknown-recovery-worker",
            200,
        )
        .await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let side_effect = journal.path().join("provider-invoked");
        let codec = FakeGatedSubmitCodec::new(journal.path(), &side_effect, "unused-operation")?
            .with_receipt("invalid-receipt");
        let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
        let runner_sha256 = file_sha256(runner)?;
        let first = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            gated_submit_driver(codec.clone(), runner, &runner_sha256)?,
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(gated_orchestrator_work(&lease)?)
        .await
        .map_err(debug_error)?;
        require(
            matches!(first, ProviderSubmitOutcome::AwaitingEvidence(ref intent)
                if intent.state == ProviderSubmitIntentState::OutcomeUnknown)
                && fs::read(&side_effect).map_err(debug_error)? == b"invoked",
            format!("invalid receipt did not create one unknown attempt: {first:?}"),
        )?;

        tokio::time::sleep(Duration::from_millis(250)).await;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let recovery = store
            .claim_submit_recovery(
                &claim_scope(),
                "gated-unknown-recovery",
                "claim-unknown-recovery",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "unknown submit was not recovery-claimable".to_owned())?;
        let orchestrator = ProviderSubmitOrchestrator::new(
            store,
            gated_submit_driver(codec, runner, &runner_sha256)?,
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?;
        for _ in 0..2 {
            let observed = orchestrator
                .recover(
                    ProviderSubmitRecoveryWork::new(&recovery, orchestrator_command(&lease))
                        .map_err(debug_error)?,
                )
                .await
                .map_err(debug_error)?;
            require(
                matches!(observed, ProviderSubmitOutcome::AwaitingEvidence(ref intent)
                    if intent.state == ProviderSubmitIntentState::OutcomeUnknown),
                format!("unknown recovery changed the submit outcome: {observed:?}"),
            )?;
        }
        require(
            fs::read(&side_effect).map_err(debug_error)? == b"invoked",
            "outcome_unknown recovery launched a second CLI side effect",
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[derive(Clone)]
struct FakeGatedSubmitCodec {
    workspace_root: PathBuf,
    side_effect: PathBuf,
    shell_sha256: String,
    operation_id: String,
    projection_delay: Duration,
    receipt: String,
    workspace_mismatch: bool,
}

impl FakeGatedSubmitCodec {
    fn new(
        working_directory: impl AsRef<Path>,
        side_effect: impl AsRef<Path>,
        operation_id: impl Into<String>,
    ) -> TestResult<Self> {
        let workspace_root = working_directory.as_ref().join("provider-submit-workspace");
        match fs::create_dir(&workspace_root) {
            Ok(()) => fs::set_permissions(&workspace_root, fs::Permissions::from_mode(0o700))
                .map_err(debug_error)?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(debug_error(error)),
        }
        Ok(Self {
            workspace_root,
            side_effect: side_effect.as_ref().to_owned(),
            shell_sha256: file_sha256(Path::new("/bin/sh"))?,
            operation_id: operation_id.into(),
            projection_delay: Duration::ZERO,
            receipt: "gated-receipt".to_owned(),
            workspace_mismatch: false,
        })
    }

    fn with_projection_delay(mut self, projection_delay: Duration) -> Self {
        self.projection_delay = projection_delay;
        self
    }

    fn with_receipt(mut self, receipt: impl Into<String>) -> Self {
        self.receipt = receipt.into();
        self
    }

    fn with_workspace_mismatch(mut self) -> Self {
        self.workspace_mismatch = true;
        self
    }

    fn workspace_root(&self) -> TestResult<WorkingDirectory> {
        WorkingDirectory::new_private(&self.workspace_root).map_err(debug_error)
    }

    fn gated_command(
        &self,
        workspace: &WorkingDirectory,
    ) -> Result<GatedCliCommand, ProviderFailure> {
        GatedCliCommand::new(
            "/bin/sh",
            &self.shell_sha256,
            workspace.path(),
            vec![
                "-c".to_owned(),
                "printf invoked >> \"$1\"; printf %s \"$2\"".to_owned(),
                "gated-provider-test".to_owned(),
                self.side_effect.to_string_lossy().into_owned(),
                self.receipt.clone(),
            ],
            BTreeMap::new(),
            Vec::new(),
            Duration::from_secs(30),
            Duration::from_millis(100),
        )
        .map_err(|_| {
            fake_provider_failure("gated_command_invalid", EffectCertainty::NoRemoteEffect)
        })
    }
}

impl GatedCliSubmitCodec for FakeGatedSubmitCodec {
    type Payload = TestPayload;

    fn provider_id(&self) -> &'static str {
        "provider-test"
    }

    fn command(
        &self,
        _intent: &ProviderSubmitIntent,
        _context: &ProviderExecutionContext,
        _command: &SingleOutputCommand<Self::Payload>,
        workspace: &WorkingDirectory,
    ) -> Result<GatedCliCommand, ProviderFailure> {
        std::thread::sleep(self.projection_delay);
        if self.workspace_mismatch {
            let workspace = self.workspace_root().map_err(|_| {
                fake_provider_failure("gated_workspace_invalid", EffectCertainty::NoRemoteEffect)
            })?;
            self.gated_command(&workspace)
        } else {
            self.gated_command(workspace)
        }
    }

    fn decode_receipt(
        &self,
        intent: &ProviderSubmitIntent,
        _command: &SingleOutputCommand<Self::Payload>,
        stdout: &[u8],
    ) -> Result<PendingOperation, ProviderFailure> {
        if stdout != b"gated-receipt" {
            return Err(fake_provider_failure(
                "gated_receipt_invalid",
                EffectCertainty::UnknownRemoteEffect,
            ));
        }
        Ok(PendingOperation::new(
            RemoteOperationRef::new(
                self.provider_id(),
                intent.submission_id.to_string(),
                self.operation_id.clone(),
            )
            .map_err(|_| {
                fake_provider_failure(
                    "gated_receipt_identity_invalid",
                    EffectCertainty::UnknownRemoteEffect,
                )
            })?,
            None,
            Some(25),
        ))
    }
}

fn gated_submit_driver(
    codec: FakeGatedSubmitCodec,
    runner: impl AsRef<Path>,
    runner_sha256: impl AsRef<str>,
) -> TestResult<GatedCliSubmitDriver<FakeGatedSubmitCodec>> {
    let workspace_root = codec.workspace_root()?;
    GatedCliSubmitDriver::new(codec, runner, runner_sha256, workspace_root).map_err(debug_error)
}

fn gated_attempt_workspace(
    codec: &FakeGatedSubmitCodec,
    submission_id: Uuid,
    launch_nonce: Uuid,
) -> TestResult<WorkingDirectory> {
    let workspace = RecoverableAttemptWorkspace::new(&codec.workspace_root()?, ".provider-submit-")
        .map_err(debug_error)?;
    workspace
        .open_or_create(&format!(
            "{}-{}",
            submission_id.simple(),
            launch_nonce.simple()
        ))
        .and_then(|attempt| attempt.working_directory())
        .map_err(debug_error)
}

fn workspace_attempt_count(path: &Path) -> TestResult<usize> {
    fs::read_dir(path)
        .map_err(debug_error)?
        .try_fold(0_usize, |count, entry| {
            let entry = entry.map_err(debug_error)?;
            Ok(count
                + usize::from(
                    entry
                        .file_name()
                        .as_bytes()
                        .starts_with(b".provider-submit-"),
                ))
        })
}

fn gated_orchestrator_work(
    lease: &ExecutorSubmissionLease,
) -> TestResult<ProviderSubmitWork<GatedCliSubmitDriver<FakeGatedSubmitCodec>>> {
    ProviderSubmitWork::new(lease, orchestrator_command(lease)).map_err(debug_error)
}

fn file_sha256(path: &Path) -> TestResult<String> {
    fs::read(path)
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
        .map_err(debug_error)
}

fn fake_provider_failure(code: &str, effect: EffectCertainty) -> ProviderFailure {
    ProviderFailure::new(
        if effect == EffectCertainty::NoRemoteEffect {
            ProviderFailureClass::Permanent
        } else {
            ProviderFailureClass::Ambiguous
        },
        code,
        effect,
        RetryDirective::Never,
    )
    .expect("fake provider failure must be valid")
}

async fn wait_gated_ready(
    submission: &GatedCliSubmission,
    binding: &GatedCliBinding,
) -> TestResult<gpt_image_2_gateway::GatedCliReady> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match submission.observe(binding).map_err(debug_error)? {
                GatedCliObservation::Ready(ready) => return Ok(ready),
                GatedCliObservation::AwaitingHelper | GatedCliObservation::Starting => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                observation => {
                    return Err(format!(
                        "unexpected gated recovery observation before ready: {observation:?}"
                    ));
                }
            }
        }
    })
    .await
    .map_err(|_| "gated runner did not become ready".to_owned())?
}

fn seed_remote_submit_dispatch_release(
    root: &Path,
    submission_id: Uuid,
    execution_binding_sha256: &str,
    launch_nonce: Uuid,
) -> TestResult {
    use std::{
        fs::{File, OpenOptions},
        io::Write,
        os::unix::fs::OpenOptionsExt,
    };

    let entry = root.join(submission_id.simple().to_string());
    let release = serde_json::to_vec(&json!({
        "execution_binding_sha256": execution_binding_sha256,
        "launch_nonce": launch_nonce,
    }))
    .map_err(debug_error)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(entry.join("dispatch-released.json"))
        .map_err(debug_error)?;
    file.write_all(&release).map_err(debug_error)?;
    file.sync_all().map_err(debug_error)?;
    File::open(&entry)
        .and_then(|directory| directory.sync_all())
        .map_err(debug_error)
}
