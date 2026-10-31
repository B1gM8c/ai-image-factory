use super::*;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use gpt_image_2_gateway::{
    DreaminaComplianceRejectionResolutionRequest, GatedCliCommand, GatedCliSubmitCodec,
    GatedCliSubmitDriver, ProviderExecutionContext, ProviderSubmitIntent,
    ProviderSubmitIntentState, ProviderSubmitOrchestrator, ProviderSubmitOutcome,
    ProviderSubmitWork, resolve_dreamina_compliance_rejection,
};
use image_cli_runtime::WorkingDirectory;
use image_provider_dreamina_cli::parse_submit_command;
use image_provider_sdk::PendingOperation;

const REFUSAL: &[u8] = br#"{"gen_status":"fail","fail_reason":"api error: ret=4010, message=aigc compliance confirmation required"}"#;
const CONCURRENCY_REFUSAL: &[u8] = br#"{"submit_id":"fixture-local-id","gen_status":"fail","fail_reason":"api error: ret=1310, message=ExceedConcurrencyLimit, logid=fixture1310"}"#;
const HELD_MICROS: i64 = 1_200_000;

pub(super) fn fixture_runner(root: &Path) -> TestResult<PathBuf> {
    let source = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
    #[cfg(target_os = "linux")]
    {
        // ELF embeds DWARF in the executable. Rehashing that debug payload can
        // consume this fixture's five-second submit budget before runner start.
        // Keep the real launch-time pin check and short deadline on a private
        // debug-free copy; never modify Cargo's binary or the production runner.
        let runner = root.join("remote-submit-runner");
        fs::copy(source, &runner).map_err(debug_error)?;
        fs::set_permissions(&runner, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
        let status = std::process::Command::new("strip")
            .arg("--strip-debug")
            .arg(&runner)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|_| "offline fixture runner debug stripping unavailable".to_string())?;
        require(
            status.success(),
            "offline fixture runner debug stripping failed",
        )?;
        Ok(runner)
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Mach-O keeps its debug payload separately; preserve its code signature.
        let _ = root;
        Ok(source.to_path_buf())
    }
}

/// Reproduce the old decoder's conservative classification while publishing
/// genuine runner/journal evidence. This shell has no network or media ability.
struct HistoricalRefusalCodec {
    invocation_marker: PathBuf,
    shell_sha256: String,
    refusal: &'static [u8],
    historical_code: &'static str,
}

impl GatedCliSubmitCodec for HistoricalRefusalCodec {
    type Payload = DreaminaSubmitPayloadV1;

    fn provider_id(&self) -> &'static str {
        DREAMINA_PROVIDER_ID
    }

    fn command(
        &self,
        _: &ProviderSubmitIntent,
        _: &ProviderExecutionContext,
        _: &SingleOutputCommand<Self::Payload>,
        workspace: &WorkingDirectory,
    ) -> Result<GatedCliCommand, ProviderFailure> {
        GatedCliCommand::new(
            "/bin/sh",
            &self.shell_sha256,
            workspace.path(),
            vec![
                "-c".into(),
                "printf invoked >> \"$1\"; printf %s \"$2\"".into(),
                "offline-compliance-fixture".into(),
                self.invocation_marker.to_string_lossy().into_owned(),
                String::from_utf8(self.refusal.to_vec()).expect("ASCII fixture"),
            ],
            BTreeMap::new(),
            Vec::new(),
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .map_err(|_| historical_unknown())
    }

    fn decode_receipt(
        &self,
        _: &ProviderSubmitIntent,
        _: &SingleOutputCommand<Self::Payload>,
        stdout: &[u8],
    ) -> Result<PendingOperation, ProviderFailure> {
        assert_eq!(stdout, self.refusal);
        Err(ProviderFailure::new(
            ProviderFailureClass::Ambiguous,
            self.historical_code,
            EffectCertainty::UnknownRemoteEffect,
            RetryDirective::Never,
        )
        .unwrap())
    }
}

fn historical_unknown() -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Ambiguous,
        "dreamina_submit_rejected",
        EffectCertainty::UnknownRemoteEffect,
        RetryDirective::Never,
    )
    .expect("valid fixture failure")
}

struct RefusalFixture {
    journal: tempfile::TempDir,
    journal_root: PathBuf,
    invocation_marker: PathBuf,
    work: WorkLease,
    lease: ExecutorSubmissionLease,
    refusal: &'static [u8],
}

impl RefusalFixture {
    fn request(&self) -> DreaminaComplianceRejectionResolutionRequest {
        DreaminaComplianceRejectionResolutionRequest {
            job_id: self.work.job_id,
            submission_id: self.lease.submission_id,
            executor_execution_id: self.lease.executor_execution_id,
            reviewed_stdout_sha256: sha256(self.refusal),
            operator_reference: "offline-regression:reviewed-4010".into(),
        }
    }

    fn entry(&self) -> PathBuf {
        self.journal_root
            .join(self.lease.submission_id.simple().to_string())
    }

    fn assert_one_fixture_invocation(&self) -> TestResult {
        require(
            fs::read(&self.invocation_marker).map_err(debug_error)? == b"invoked",
            "resolution or replay launched the offline fixture again",
        )
    }
}

fn video_scope() -> ExecutorClaimScope {
    ExecutorClaimScope {
        execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
        provider_id: DREAMINA_PROVIDER_ID.into(),
        command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
        adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
    }
}

fn video_poll_scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        provider_id: DREAMINA_PROVIDER_ID.into(),
        provider_account_id: DREAMINA_ACCOUNT_ID,
        execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
    }
}

async fn refusal_fixture(pool: &PgPool, terminalize: bool) -> TestResult<RefusalFixture> {
    refusal_fixture_with_receipt(pool, terminalize, REFUSAL, "dreamina_submit_rejected").await
}

async fn refusal_fixture_with_receipt(
    pool: &PgPool,
    terminalize: bool,
    refusal: &'static [u8],
    historical_code: &'static str,
) -> TestResult<RefusalFixture> {
    let work = seed_dreamina_video_generation_lease_for_profile(
        pool,
        "compliance-resolution-worker",
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
            unit_price_micros: HELD_MICROS / 8,
            quantity_source: "request_derived",
            confidence: "exact",
            max_quantity: 8,
            max_amount_micros: HELD_MICROS,
        },
    )
    .await?;
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    let prepared = executor
        .prepare_and_handoff(&work, DREAMINA_VIDEO_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    require(prepared.len() == 1, "fixture must have one video output")?;
    seed_terminal_quota(pool, &work).await?;
    let lease = executor
        .claim_prepared(&video_scope(), "compliance-resolution-executor", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("fixture executor not claimable")?;
    executor.start(&lease).await.map_err(debug_error)?;
    let journal = tempfile::tempdir().map_err(debug_error)?;
    let journal_root = journal.path().join("remote-submit");
    let invocation_marker = journal.path().join("offline-invocations");
    let command_source = serde_json::to_vec(&work.command_json).map_err(debug_error)?;
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).map_err(debug_error)?,
        DreaminaSubmitPayloadV1::new(
            &lease.command_hash,
            parse_submit_command(&command_source)
                .map_err(debug_error)?
                .into_single_output(),
        )
        .map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    if !terminalize {
        let tasks = PostgresProviderTaskStore::new(pool.clone());
        let reservation = RemoteTaskSubmitReservation::new(
            &lease,
            format!("provider-submit-{}", lease.submission_id.simple()),
            command.output(),
            command.identity(),
            60_000,
        );
        tasks
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        tasks
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        return Ok(RefusalFixture {
            journal,
            journal_root,
            invocation_marker,
            work,
            lease,
            refusal,
        });
    }
    let workspace = journal.path().join("workspace");
    fs::create_dir(&workspace).map_err(debug_error)?;
    fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
    let runner = fixture_runner(journal.path())?;
    let runner_bytes = fs::metadata(&runner).map_err(debug_error)?.len();
    let codec = HistoricalRefusalCodec {
        invocation_marker: invocation_marker.clone(),
        shell_sha256: sha256(&fs::read("/bin/sh").map_err(debug_error)?),
        refusal,
        historical_code,
    };
    let driver = GatedCliSubmitDriver::new(
        codec,
        &runner,
        sha256(&fs::read(&runner).map_err(debug_error)?),
        WorkingDirectory::new_private(&workspace).map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    let preparation_started = std::time::Instant::now();
    let outcome = ProviderSubmitOrchestrator::new(
        PostgresProviderTaskStore::new(pool.clone()),
        driver,
        5_000,
        &journal_root,
    )
    .map_err(debug_error)?
    .submit(ProviderSubmitWork::new(&lease, command).map_err(debug_error)?)
    .await
    .map_err(debug_error)?;
    let expected = matches!(outcome, ProviderSubmitOutcome::AwaitingEvidence(ref intent)
        if intent.state == ProviderSubmitIntentState::OutcomeUnknown
            && intent.failure_error_code.as_deref() == Some(historical_code));
    let error_code = match &outcome {
        ProviderSubmitOutcome::AwaitingEvidence(intent)
        | ProviderSubmitOutcome::Terminal(intent) => intent
            .failure_error_code
            .as_deref()
            .unwrap_or("missing_failure_code"),
        ProviderSubmitOutcome::Attached(_) => "unexpected_accepted_operation",
    };
    require(
        expected,
        format!(
            "offline fixture runner_bytes={runner_bytes} preparation_ms={} error_code={error_code}",
            preparation_started.elapsed().as_millis()
        ),
    )?;
    let fixture = RefusalFixture {
        journal,
        journal_root,
        invocation_marker,
        work,
        lease,
        refusal,
    };
    fixture.assert_one_fixture_invocation()?;
    if terminalize {
        tokio::time::sleep(Duration::from_millis(5_100)).await;
        let intent = PostgresProviderTaskStore::new(pool.clone())
            .resolve_due_submit_deadline(&video_poll_scope())
            .await
            .map_err(debug_error)?
            .ok_or("fixture deadline did not resolve")?;
        require(
            intent.state == ProviderSubmitIntentState::DeadlineQuarantined,
            "fixture deadline was not quarantined",
        )?;
        let reductions = PostgresExecutorTerminalStore::new(pool.clone());
        let terminal = reductions
            .claim_terminal("compliance-resolution-reducer", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or("uncertain reduction missing")?;
        let completed = reductions
            .complete_terminal(&terminal, None)
            .await
            .map_err(debug_error)?;
        require(
            completed.parent_state == ExecutorParentTerminalState::Uncertain,
            "fixture canonical parent must remain uncertain",
        )?;
    }
    Ok(fixture)
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct ResolutionSnapshot {
    job_state: String,
    output_state: String,
    submission_state: String,
    execution_state: String,
    decision_source: String,
    decision_state: String,
    reduction_state: String,
    reduction_outcome: String,
    hold_state: String,
    held_micros: i64,
    captured_micros: i64,
    released_micros: i64,
    account_held_micros: i64,
    account_captured_micros: i64,
    quota_state: String,
    quota_committed: i32,
    quota_released: i32,
    resolution_count: i64,
    ledger_count: i64,
    usage_count: i64,
    remote_count: i64,
    submission_count: i64,
    resolution_event_count: i64,
    resolution_outbox_count: i64,
}

async fn snapshot(pool: &PgPool, fixture: &RefusalFixture) -> TestResult<ResolutionSnapshot> {
    sqlx::query_as(r#"
        SELECT job.state AS job_state, output.state AS output_state,
          submission.state AS submission_state, execution.state AS execution_state,
          decision.source AS decision_source, decision.resolved_state AS decision_state,
          reduction.state AS reduction_state, reduction.resolved_state AS reduction_outcome,
          hold.state AS hold_state, hold.held_micros, hold.captured_micros, hold.released_micros,
          account.held_micros AS account_held_micros, account.captured_micros AS account_captured_micros,
          quota.state AS quota_state, quota.committed_units AS quota_committed,
          quota.released_units AS quota_released,
          (SELECT COUNT(*) FROM dreamina_operator_rejection_resolutions WHERE job_id = $1) AS resolution_count,
          (SELECT COUNT(*) FROM ledger_transactions WHERE source_job_id = $1) AS ledger_count,
          (SELECT COUNT(*) FROM provider_usage_facts WHERE job_id = $1) AS usage_count,
          (SELECT COUNT(*) FROM provider_remote_tasks WHERE submission_id = $2) AS remote_count,
          (SELECT COUNT(*) FROM provider_submissions WHERE job_id = $1) AS submission_count,
          (SELECT COUNT(*) FROM job_events WHERE job_id = $1 AND semantic_key = 'job.operator_dreamina_compliance_rejection') AS resolution_event_count,
          (SELECT COUNT(*) FROM outbox_events WHERE job_id = $1 AND semantic_key = 'job.operator_dreamina_compliance_rejection') AS resolution_outbox_count
        FROM jobs job JOIN job_outputs output ON output.job_id = job.job_id
        JOIN provider_submissions submission ON submission.job_id = job.job_id AND submission.output_id = output.output_id
        JOIN executor_executions execution ON execution.executor_execution_id = submission.executor_execution_id AND execution.submission_id = submission.submission_id
        JOIN executor_resolution_decisions decision ON decision.decision_id = execution.resolution_decision_id
        JOIN executor_terminal_reductions reduction ON reduction.executor_execution_id = execution.executor_execution_id AND reduction.submission_id = submission.submission_id
        JOIN customer_billing_holds hold ON hold.job_id = job.job_id AND hold.tenant_id = job.tenant_id
        JOIN billing_accounts account ON account.tenant_id = job.tenant_id AND account.currency = hold.currency
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
        WHERE job.job_id = $1 AND submission.submission_id = $2
    "#).bind(fixture.work.job_id).bind(fixture.lease.submission_id)
        .fetch_one(pool).await.map_err(debug_error)
}

async fn canonical_evidence(
    pool: &PgPool,
    fixture: &RefusalFixture,
) -> TestResult<serde_json::Value> {
    sqlx::query_scalar(r#"
        SELECT jsonb_build_object('decision', to_jsonb(decision),
          'execution', to_jsonb(execution), 'submission', to_jsonb(submission),
          'reduction', to_jsonb(reduction))
        FROM provider_submissions submission
        JOIN executor_executions execution USING(executor_execution_id, submission_id)
        JOIN executor_resolution_decisions decision ON decision.decision_id = execution.resolution_decision_id
        JOIN executor_terminal_reductions reduction ON reduction.executor_execution_id = execution.executor_execution_id AND reduction.submission_id = submission.submission_id
        WHERE submission.submission_id = $1
    "#).bind(fixture.lease.submission_id).fetch_one(pool).await.map_err(debug_error)
}

#[tokio::test]
async fn dreamina_compliance_resolution_releases_exactly_once_preserving_canonical_uncertainty()
-> TestResult {
    rejection_resolution_preserves_originals(
        REFUSAL,
        "dreamina_submit_rejected",
        "dreamina_compliance_confirmation_required",
    )
    .await
}

#[tokio::test]
async fn dreamina_concurrency_resolution_releases_exactly_once_preserving_canonical_uncertainty()
-> TestResult {
    rejection_resolution_preserves_originals(
        CONCURRENCY_REFUSAL,
        "dreamina_submit_recovery_evidence_invalid",
        "dreamina_concurrency_limit_exceeded",
    )
    .await
}

async fn rejection_resolution_preserves_originals(
    refusal: &'static [u8],
    historical_code: &'static str,
    reason: &'static str,
) -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = refusal_fixture_with_receipt(&database.pool, true, refusal, historical_code).await?;
        let before = snapshot(&database.pool, &fixture).await?;
        let canonical_before = canonical_evidence(&database.pool, &fixture).await?;
        let read_store = PostgresExecutionSettlementStore::new(database.pool.clone(),
            Arc::new(InMemoryArtifactBlobStore::default()));
        require(read_store.project_video_released_settlement(
            "executord-process-smoke", "project-test", None, fixture.work.job_id)
            .await.map_err(debug_error)?.is_none(),
            "an unresolved held order exposed a released-settlement receipt")?;
        let original_cost: (Option<i64>, Option<String>) = sqlx::query_as(
            "SELECT provider_cost_micros, provider_cost_currency FROM provider_receipts WHERE submission_id = $1")
            .bind(fixture.lease.submission_id).fetch_one(&database.pool).await.map_err(debug_error)?;
        require(original_cost == (None, None), "fixture must preserve the absence of provider cost evidence")?;
        require(
            before.job_state == "uncertain"
                && before.hold_state == "held"
                && before.held_micros == HELD_MICROS
                && before.account_held_micros == HELD_MICROS
                && before.released_micros == 0
                && before.captured_micros == 0,
            format!("fixture lost the original USD1.20 hold: {before:?}"),
        )?;
        let request = fixture.request();
        let (first_attempt, competing_attempt) = tokio::join!(
            resolve_dreamina_compliance_rejection(&database.pool, &fixture.journal_root, &request),
            resolve_dreamina_compliance_rejection(&database.pool, &fixture.journal_root, &request),
        );
        // Serializable contention may reject one caller. At least one must commit,
        // and the subsequent exact replay must return that same immutable audit.
        let first = match (first_attempt, competing_attempt) {
            (Ok(first), Ok(second)) => { require(first == second, "concurrent resolutions differed")?; first }
            (Ok(first), Err(_)) | (Err(_), Ok(first)) => first,
            (Err(first), Err(second)) => return Err(format!("both resolutions failed: {first:?}/{second:?}")),
        };
        let replay =
            resolve_dreamina_compliance_rejection(&database.pool, &fixture.journal_root, &request)
                .await
                .map_err(debug_error)?;
        require(
            first == replay
                && first.job_id == fixture.work.job_id
                && first.submission_id == fixture.lease.submission_id
                && first.executor_execution_id == fixture.lease.executor_execution_id
                && first.released_micros == HELD_MICROS
                && first.captured_micros == 0
                && first.released_units == 8
                && first.currency == "USD",
            format!("resolution receipt or replay drifted: {first:?}/{replay:?}"),
        )?;
        let public_receipt = read_store.project_video_released_settlement(
            "executord-process-smoke", "project-test", None, fixture.work.job_id)
            .await.map_err(debug_error)?.ok_or("released audit receipt was not project-readable")?;
        require(public_receipt == image_api_contracts::dreamina::DreaminaReleasedSettlement {
            state: "released".into(), resolution_audit_id: first.resolution_id.to_string(),
            job_id: fixture.work.job_id.to_string(), request_id: first.request_id.clone(),
            hold_id: first.hold_id.to_string(), currency: "USD".into(),
            held_micros: HELD_MICROS.to_string(), released_micros: HELD_MICROS.to_string(),
            captured_micros: "0".into(),
        }, format!("public settlement receipt lost exact audit or monetary identity: {public_receipt:?}"))?;
        let public_status = read_store.project_video_status(
            "executord-process-smoke", "project-test", None, fixture.work.job_id)
            .await.map_err(debug_error)?;
        require(matches!(public_status,
            Some(gpt_image_2_gateway::settlement::VideoResultStatus::Failed { error_code: Some(ref code), .. })
            if code == reason),
            format!("resolved video API projection lost its truthful terminal status: {public_status:?}"))?;
        for (tenant, project, actor) in [
            ("other-tenant", "project-test", None),
            ("executord-process-smoke", "other-project", None),
            ("executord-process-smoke", "project-test", Some(Uuid::new_v4())),
        ] {
            require(read_store.project_video_released_settlement(tenant, project, actor, fixture.work.job_id)
                .await.map_err(debug_error)?.is_none(),
                "released settlement leaked across tenant, project or actor scope")?;
        }
        let after = snapshot(&database.pool, &fixture).await?;
        require(canonical_evidence(&database.pool, &fixture).await? == canonical_before,
            "operator settlement rewrote original immutable executor evidence")?;
        require(
            after
                == ResolutionSnapshot {
                    job_state: "failed".into(),
                    output_state: "failed".into(),
                    submission_state: "uncertain".into(),
                    execution_state: "uncertain".into(),
                    decision_source: "remote_submit_deadline".into(),
                    decision_state: "uncertain".into(),
                    reduction_state: "completed".into(),
                    reduction_outcome: "uncertain".into(),
                    hold_state: "released".into(),
                    held_micros: HELD_MICROS,
                    captured_micros: 0,
                    released_micros: HELD_MICROS,
                    account_held_micros: 0,
                    account_captured_micros: 0,
                    quota_state: "released".into(),
                    quota_committed: 0,
                    quota_released: 8,
                    resolution_count: 1,
                    ledger_count: 0,
                    usage_count: 0,
                    remote_count: 0,
                    submission_count: 1,
                    resolution_event_count: 1,
                    resolution_outbox_count: 1,
                },
            format!(
                "resolution was not full, zero-charge, append-only and exactly once: {after:?}"
            ),
        )?;
        require(
            PostgresExecutorSubmissionStore::new(database.pool.clone())
                .claim_prepared(&video_scope(), "must-not-retry", 60_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "resolved order became claimable again",
        )?;
        require(
            PostgresProviderTaskStore::new(database.pool.clone())
                .resolve_due_submit_deadline(&video_poll_scope())
                .await
                .map_err(debug_error)?
                .is_none(),
            "resolved old deadline became actionable again",
        )?;
        let capacity: (String, String, String, String) = sqlx::query_as(r#"
            SELECT allocation.state, reconciliation.state, reconciliation.evidence_kind,
                   reconciliation.event_identity
            FROM executor_capacity_allocations allocation
            JOIN provider_capacity_reconciliations reconciliation USING(submission_id, executor_execution_id)
            WHERE allocation.submission_id = $1
        "#).bind(fixture.lease.submission_id).fetch_one(&database.pool).await.map_err(debug_error)?;
        require(capacity == ("released".into(), "released".into(), "confirmed_no_effect".into(),
            format!("operator:{}", first.resolution_id)),
            format!("capacity was not released under the audited no-effect identity: {capacity:?}"))?;
        require(sqlx::query("UPDATE executor_resolution_decisions SET resolved_state = 'failed' WHERE submission_id = $1")
            .bind(fixture.lease.submission_id).execute(&database.pool).await.is_err(),
            "operator overlay made the canonical decision mutable")?;
        require(sqlx::query("DELETE FROM dreamina_operator_rejection_resolutions WHERE resolution_id = $1")
            .bind(first.resolution_id).execute(&database.pool).await.is_err(),
            "operator audit record was deletable")?;
        let tasks = PostgresProviderTaskStore::new(database.pool.clone());
        let intent = tasks.load_submit_intent(fixture.lease.submission_id).await.map_err(debug_error)?
            .ok_or("resolved fixture intent missing")?;
        require(tasks.record_submit_receipt(&RemoteTaskSubmitReceipt {
            submission_id: fixture.lease.submission_id,
            executor_execution_id: fixture.lease.executor_execution_id,
            executor_owner: fixture.lease.executor_owner.clone(),
            executor_lease_epoch: fixture.lease.executor_lease_epoch,
            remote_operation_id: "offline-receipt-after-resolution".into(),
            provider_request_id: None,
            event_identity: "offline-receipt-after-resolution".into(),
            execution_binding_sha256: intent.execution_binding_sha256,
        }).await.is_err(), "a late acceptance overwrote the reviewed zero-effect settlement")?;
        require(snapshot(&database.pool, &fixture).await? == after
            && canonical_evidence(&database.pool, &fixture).await? == canonical_before,
            "late acceptance rejection changed resolved state, economics or canonical evidence")?;
        fixture.assert_one_fixture_invocation()
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn dreamina_compliance_resolution_rejects_mismatched_review_and_missing_evidence_unchanged()
-> TestResult {
    rejection_resolution_rejects_bad_evidence(REFUSAL, "dreamina_submit_rejected").await
}

#[tokio::test]
async fn dreamina_concurrency_resolution_rejects_mismatched_review_and_missing_evidence_unchanged()
-> TestResult {
    rejection_resolution_rejects_bad_evidence(
        CONCURRENCY_REFUSAL,
        "dreamina_submit_recovery_evidence_invalid",
    )
    .await
}

async fn rejection_resolution_rejects_bad_evidence(
    refusal: &'static [u8],
    historical_code: &'static str,
) -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture =
            refusal_fixture_with_receipt(&database.pool, true, refusal, historical_code).await?;
        let before = snapshot(&database.pool, &fixture).await?;
        let mut wrong_sha = fixture.request();
        wrong_sha.reviewed_stdout_sha256 = "a".repeat(64);
        require(
            resolve_dreamina_compliance_rejection(
                &database.pool,
                &fixture.journal_root,
                &wrong_sha,
            )
            .await
            .is_err(),
            "mismatched reviewed stdout SHA released a hold",
        )?;
        let mut wrong_execution = fixture.request();
        wrong_execution.executor_execution_id = Uuid::new_v4();
        require(
            resolve_dreamina_compliance_rejection(
                &database.pool,
                &fixture.journal_root,
                &wrong_execution,
            )
            .await
            .is_err(),
            "mismatched execution identity released a hold",
        )?;
        let missing = fixture.journal.path().join("absent-journal");
        require(
            resolve_dreamina_compliance_rejection(&database.pool, &missing, &fixture.request())
                .await
                .is_err()
                && !missing.exists(),
            "missing evidence was accepted or created",
        )?;
        let terminal = fixture.entry().join("process-terminal.json");
        let original = fs::read(&terminal).map_err(debug_error)?;
        let outside = fixture.journal.path().join("untrusted-terminal.json");
        fs::write(&outside, &original).map_err(debug_error)?;
        fs::remove_file(&terminal).map_err(debug_error)?;
        std::os::unix::fs::symlink(&outside, &terminal).map_err(debug_error)?;
        require(
            resolve_dreamina_compliance_rejection(
                &database.pool,
                &fixture.journal_root,
                &fixture.request(),
            )
            .await
            .is_err(),
            "symlinked process evidence released a hold",
        )?;
        require(
            snapshot(&database.pool, &fixture).await? == before,
            "rejected evidence changed canonical state, economics, or audit counts",
        )?;
        fs::remove_file(&terminal).map_err(debug_error)?;
        fs::write(&terminal, original).map_err(debug_error)?;
        fs::set_permissions(&terminal, fs::Permissions::from_mode(0o600)).map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO usage_events (event_id, tenant_id, request_id, job_id, operation,
                units, outcome, billing_metric, billing_unit, created_at_ms)
            SELECT $2, tenant_id, request_id, job_id, operation, 1, 'succeeded',
                billing_metric, billing_unit, created_at_ms FROM jobs WHERE job_id = $1
        "#,
        )
        .bind(fixture.work.job_id)
        .bind(Uuid::new_v4())
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            resolve_dreamina_compliance_rejection(
                &database.pool,
                &fixture.journal_root,
                &fixture.request(),
            )
            .await
            .is_err(),
            "contradictory legacy usage allowed a no-effect hold release",
        )?;
        require(
            snapshot(&database.pool, &fixture).await? == before,
            "contradictory usage rejection changed state or economics",
        )?;
        fixture.assert_one_fixture_invocation()
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn dreamina_compliance_resolution_rejects_active_execution_unchanged() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = refusal_fixture(&database.pool, false).await?;
        let active_snapshot = |pool: PgPool, job_id: Uuid| async move {
            sqlx::query_as::<_, (String, String, i64, i64, i64, i64)>(
                r#"
                SELECT job.state, hold.state, hold.released_micros, hold.captured_micros,
                  (SELECT COUNT(*) FROM dreamina_operator_rejection_resolutions WHERE job_id=$1),
                  (SELECT COUNT(*) FROM ledger_transactions WHERE source_job_id=$1)
                FROM jobs job JOIN customer_billing_holds hold USING(job_id) WHERE job.job_id=$1
            "#,
            )
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .map_err(debug_error)
        };
        let before = active_snapshot(database.pool.clone(), fixture.work.job_id).await?;
        require(
            resolve_dreamina_compliance_rejection(
                &database.pool,
                &fixture.journal_root,
                &fixture.request(),
            )
            .await
            .is_err(),
            "active execution allowed an operator hold release",
        )?;
        require(
            active_snapshot(database.pool.clone(), fixture.work.job_id).await? == before,
            "active rejection changed state or economics",
        )?;
        require(
            !fixture.invocation_marker.exists(),
            "active-order check launched the offline provider",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn dreamina_compliance_resolution_rejects_late_accepted_remote_unchanged() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = refusal_fixture(&database.pool, true).await?;
        let tasks = PostgresProviderTaskStore::new(database.pool.clone());
        let binding = tasks
            .load_submit_intent(fixture.lease.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or("fixture intent missing")?
            .execution_binding_sha256;
        tasks
            .record_submit_receipt(&RemoteTaskSubmitReceipt {
                submission_id: fixture.lease.submission_id,
                executor_execution_id: fixture.lease.executor_execution_id,
                executor_owner: fixture.lease.executor_owner.clone(),
                executor_lease_epoch: fixture.lease.executor_lease_epoch,
                remote_operation_id: "offline-late-accepted-operation".into(),
                provider_request_id: None,
                event_identity: "offline-late-accepted-evidence".into(),
                execution_binding_sha256: binding,
            })
            .await
            .map_err(debug_error)?;
        let before = snapshot(&database.pool, &fixture).await?;
        require(
            resolve_dreamina_compliance_rejection(
                &database.pool,
                &fixture.journal_root,
                &fixture.request(),
            )
            .await
            .is_err(),
            "accepted remote operation allowed a no-effect refund",
        )?;
        require(
            snapshot(&database.pool, &fixture).await? == before,
            "accepted remote rejection changed state or economics",
        )?;
        fixture.assert_one_fixture_invocation()
    }
    .await;
    combine(result, database.cleanup().await)
}
