use super::*;
use gpt_image_2_gateway::{
    ProviderSubmitDriver, ProviderSubmitDriverCall, ProviderSubmitDriverRecovery,
};
use tokio::sync::Semaphore;

struct FrozenPayload(String);

impl CanonicalCommandPayload for FrozenPayload {
    const SCHEMA_ID: &'static str = DREAMINA_SUBMIT_COMMAND_SCHEMA;
    const ADAPTER_REVISION: &'static str = DREAMINA_ADAPTER_REVISION;

    fn source_command_sha256(&self) -> &str {
        &self.0
    }
    fn into_canonical_bytes(self, _: OutputSlot) -> Vec<u8> {
        b"offline-dreamina-command".to_vec()
    }
}

#[derive(Clone, Default)]
struct OfflineDriver {
    dispatches: Arc<AtomicUsize>,
    preparing: Option<Arc<Semaphore>>,
    prepared: Option<Arc<Semaphore>>,
    dispatching: Option<Arc<Semaphore>>,
    dispatched: Option<Arc<Semaphore>>,
}

fn pending(call: &ProviderSubmitDriverCall<FrozenPayload>) -> PendingOperation {
    PendingOperation::new(
        RemoteOperationRef::new(
            DREAMINA_PROVIDER_ID,
            &call.intent().submission_id.to_string(),
            "offline-existing-task",
        )
        .unwrap(),
        None,
        Some(0),
    )
}

impl ProviderSubmitDriver for OfflineDriver {
    type Payload = FrozenPayload;
    type Prepared = ();
    fn provider_id(&self) -> &'static str {
        DREAMINA_PROVIDER_ID
    }
    async fn prepare(
        &self,
        _: &ProviderSubmitDriverCall<FrozenPayload>,
    ) -> Result<(), ProviderFailure> {
        if let Some(signal) = &self.preparing {
            signal.add_permits(1);
        }
        if let Some(wait) = &self.prepared {
            wait.acquire().await.unwrap().forget();
        }
        Ok(())
    }
    async fn dispatch(
        &self,
        _: (),
        call: &ProviderSubmitDriverCall<FrozenPayload>,
    ) -> Result<PendingOperation, ProviderFailure> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        if let Some(signal) = &self.dispatching {
            signal.add_permits(1);
        }
        if let Some(wait) = &self.dispatched {
            wait.acquire().await.unwrap().forget();
        }
        Ok(pending(call))
    }
    async fn recover_released(
        &self,
        call: &ProviderSubmitDriverCall<FrozenPayload>,
    ) -> ProviderSubmitDriverRecovery {
        ProviderSubmitDriverRecovery::Accepted(pending(call))
    }
}

fn scope() -> ExecutorClaimScope {
    ExecutorClaimScope {
        execution_profile_id: DREAMINA_PROFILE_ID,
        provider_id: DREAMINA_PROVIDER_ID.into(),
        command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
        adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
    }
}

async fn prepare(pool: &PgPool) -> TestResult {
    seed_dreamina_execution_profile(pool).await?;
    let work = seed_work_lease_for_runtime_profile_with_command(
        pool,
        "fresh-credential-work",
        DREAMINA_PROVIDER_ID,
        "dreamina-image-5.0",
        DREAMINA_SUBMIT_COMMAND_SCHEMA,
        json!({"operation":"text2image","schema_version":1,"prompt":"offline fixture",
            "model_version":"5.0","ratio":"1:1","resolution_type":"2k","generate_num":1,"poll":0}),
    )
    .await?;
    PostgresExecutorSubmissionStore::new(pool.clone())
        .prepare_and_handoff(&work, DREAMINA_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    Ok(())
}

async fn claim(pool: &PgPool) -> TestResult<ExecutorSubmissionLease> {
    let store = PostgresExecutorSubmissionStore::new(pool.clone());
    let lease = store
        .claim_prepared(&scope(), "fresh-credential-executor", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("fresh fixture not claimable")?;
    store.start(&lease).await.map_err(debug_error)?;
    Ok(lease)
}

fn command(lease: &ExecutorSubmissionLease) -> SingleOutputCommand<FrozenPayload> {
    SingleOutputCommand::new(
        OutputSlot::new(0, 1).unwrap(),
        FrozenPayload(lease.command_hash.clone()),
    )
    .unwrap()
}

fn work(lease: &ExecutorSubmissionLease) -> ProviderSubmitWork<OfflineDriver> {
    ProviderSubmitWork::new(lease, command(lease)).unwrap()
}

async fn reauth(pool: &PgPool) -> TestResult {
    sqlx::query("UPDATE provider_account_credential_heads SET lifecycle_state='reauth_required' WHERE provider_account_id=$1")
        .bind(DREAMINA_ACCOUNT_ID).execute(pool).await.map_err(debug_error)?;
    Ok(())
}

#[tokio::test]
async fn fresh_credential_blocks_new_claim_and_acquire_after_reauth() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        prepare(&database.pool).await?;
        reauth(&database.pool).await?;
        require(matches!(PostgresExecutorSubmissionStore::new(database.pool.clone())
            .claim_prepared(&scope(), "blocked", 60_000).await,
            Err(gpt_image_2_gateway::ExecutorSubmissionError::Conflict)), "reauth head allowed new executor claim")?;
        sqlx::query("UPDATE provider_account_credential_heads SET lifecycle_state='active' WHERE provider_account_id=$1")
            .bind(DREAMINA_ACCOUNT_ID).execute(&database.pool).await.map_err(debug_error)?;
        let lease = claim(&database.pool).await?;
        reauth(&database.pool).await?;
        let command = command(&lease);
        let reservation = RemoteTaskSubmitReservation::new(&lease, "offline-acquire".into(), command.output(), command.identity(), 60_000);
        require(matches!(PostgresProviderTaskStore::new(database.pool.clone()).acquire_submit(&reservation).await,
            Err(ProviderTaskStoreError::Conflict)), "revocation after executor claim allowed submit acquire")?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM provider_remote_submit_intents")
            .fetch_one(&database.pool).await.map_err(debug_error)?;
        require(count == 0, "blocked acquire mutated submit intents")
    }.await;
    combine(result, database.cleanup().await)
}

async fn changed_after_prepare(revision: bool, disabled: bool) -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        prepare(&database.pool).await?;
        let lease = claim(&database.pool).await?;
        let driver = OfflineDriver {
            preparing: Some(Arc::new(Semaphore::new(0))), prepared: Some(Arc::new(Semaphore::new(0))),
            ..OfflineDriver::default()
        };
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let root = journal.path().join("journal");
        let orchestrator = Arc::new(ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()), driver.clone(), 60_000, &root,
        ).map_err(debug_error)?);
        let submit = { let orchestrator = orchestrator.clone(); let work = work(&lease);
            tokio::spawn(async move { orchestrator.submit(work).await }) };
        tokio::time::timeout(Duration::from_secs(5), driver.preparing.as_ref().unwrap().acquire())
            .await.map_err(debug_error)?.map_err(debug_error)?.forget();
        if revision {
            sqlx::query("INSERT INTO provider_account_credential_revisions(provider_account_id,revision,material_kind,material_fingerprint_sha256,created_at_ms) VALUES($1,2,'auth_file',$2,1)")
                .bind(DREAMINA_ACCOUNT_ID).bind("d".repeat(64)).execute(&database.pool).await.map_err(debug_error)?;
            sqlx::query("UPDATE provider_account_credential_heads SET active_revision=2 WHERE provider_account_id=$1")
                .bind(DREAMINA_ACCOUNT_ID).execute(&database.pool).await.map_err(debug_error)?;
        } else if disabled {
            sqlx::query("UPDATE provider_accounts SET state='disabled' WHERE provider_account_id=$1")
                .bind(DREAMINA_ACCOUNT_ID).execute(&database.pool).await.map_err(debug_error)?;
        } else { reauth(&database.pool).await?; }
        driver.prepared.as_ref().unwrap().add_permits(1);
        let outcome = submit.await.map_err(debug_error)?;
        require(matches!(outcome, Err(ProviderSubmitOrchestratorError::Store(ProviderTaskStoreError::Conflict))),
            format!("changed head was not denied: {outcome:?}"))?;
        require(driver.dispatches.load(Ordering::SeqCst) == 0 && !root.join(lease.submission_id.simple().to_string()).join("dispatch-released.json").exists(),
            "changed credential released a new dispatch")?;
        let (state, receipts): (String, i64) = sqlx::query_as("SELECT intent.state,(SELECT count(*) FROM provider_receipts) FROM provider_remote_submit_intents intent WHERE submission_id=$1")
            .bind(lease.submission_id).fetch_one(&database.pool).await.map_err(debug_error)?;
        require(state == "sending" && receipts == 0, "credential denial changed task certainty or created a receipt")
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn fresh_credential_reauth_between_claim_and_release_never_dispatches() -> TestResult {
    changed_after_prepare(false, false).await
}
#[tokio::test]
async fn fresh_credential_revision_drift_between_claim_and_release_never_dispatches() -> TestResult
{
    changed_after_prepare(true, false).await
}
#[tokio::test]
async fn fresh_credential_disabled_between_claim_and_release_never_dispatches() -> TestResult {
    changed_after_prepare(false, true).await
}

#[tokio::test]
async fn fresh_credential_active_dispatch_then_reauth_keeps_attached_poll_and_replay() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        prepare(&database.pool).await?;
        let lease = claim(&database.pool).await?;
        let driver = OfflineDriver {
            dispatching: Some(Arc::new(Semaphore::new(0))),
            dispatched: Some(Arc::new(Semaphore::new(0))),
            ..OfflineDriver::default()
        };
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let orchestrator = Arc::new(
            ProviderSubmitOrchestrator::new(
                store.clone(),
                driver.clone(),
                60_000,
                journal.path().join("journal"),
            )
            .map_err(debug_error)?,
        );
        let submit = {
            let orchestrator = orchestrator.clone();
            let work = work(&lease);
            tokio::spawn(async move { orchestrator.submit(work).await })
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            driver.dispatching.as_ref().unwrap().acquire(),
        )
        .await
        .map_err(debug_error)?
        .map_err(debug_error)?
        .forget();
        // Dispatch may wait for a long CLI receipt: credential updates must not be locked for that wait.
        tokio::time::timeout(Duration::from_secs(2), reauth(&database.pool))
            .await
            .map_err(debug_error)??;
        driver.dispatched.as_ref().unwrap().add_permits(1);
        require(
            matches!(
                submit.await.map_err(debug_error)?.map_err(debug_error)?,
                ProviderSubmitOutcome::Attached(_)
            ),
            "active dispatch failed",
        )?;
        require(
            matches!(
                orchestrator
                    .submit(work(&lease))
                    .await
                    .map_err(debug_error)?,
                ProviderSubmitOutcome::Attached(_)
            ),
            "reauth blocked existing attachment",
        )?;
        let polled = store
            .claim_due(
                &ProviderTaskClaimScope {
                    execution_profile_id: DREAMINA_PROFILE_ID,
                    provider_id: DREAMINA_PROVIDER_ID.into(),
                    provider_account_id: DREAMINA_ACCOUNT_ID,
                },
                "offline-poll",
                60_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or("reauth blocked existing task poll")?;
        require(
            polled.task.remote_operation_id == "offline-existing-task"
                && driver.dispatches.load(Ordering::SeqCst) == 1,
            "existing task was changed or resent",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn fresh_credential_guard_serializes_head_mutation_until_release() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        prepare(&database.pool).await?;
        let lease = claim(&database.pool).await?;
        let command = command(&lease);
        let reservation = RemoteTaskSubmitReservation::new(&lease, "offline-lock".into(), command.output(), command.identity(), 60_000);
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let ProviderSubmitAcquire::Dispatch(authority) = store.acquire_submit(&reservation).await.map_err(debug_error)? else { return Err("active acquire did not dispatch".into()); };
        let guard = store.guard_submit_credentials(authority.intent(), authority.context()).await.map_err(debug_error)?;
        let mut connection = database.pool.acquire().await.map_err(debug_error)?;
        sqlx::query("SET lock_timeout='50ms'").execute(&mut *connection).await.map_err(debug_error)?;
        let blocked = sqlx::query("UPDATE provider_account_credential_heads SET lifecycle_state='reauth_required' WHERE provider_account_id=$1")
            .bind(DREAMINA_ACCOUNT_ID).execute(&mut *connection).await;
        require(blocked.as_ref().err().and_then(sqlx::Error::as_database_error).and_then(|e| e.code()).as_deref() == Some("55P03"), "head mutation was not fenced by dispatch guard")?;
        drop(guard);
        reauth(&database.pool).await?;
        require(matches!(store.guard_submit_credentials(authority.intent(), authority.context()).await, Err(ProviderTaskStoreError::Conflict)), "fresh guard reused stale head")
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn fresh_credential_admission_claim_skips_reauth_and_resumes_when_active() -> TestResult {
    use gpt_image_2_gateway::admission::{
        AdmissionContract, AdmissionStore, PostgresAdmissionStore,
    };
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&database.pool).await?;
        let work = seed_work_lease_for_runtime_profile(&database.pool, "offline-ready", DREAMINA_PROVIDER_ID, "dreamina-image-5.0", DREAMINA_SUBMIT_COMMAND_SCHEMA).await?;
        sqlx::query("UPDATE work_items SET state='ready',lease_owner=NULL,lease_expires_at_ms=NULL,execution_profile_id=$2 WHERE work_item_id=$1")
            .bind(work.work_item_id).bind(DREAMINA_PROFILE_ID).execute(&database.pool).await.map_err(debug_error)?;
        reauth(&database.pool).await?;
        let store = PostgresAdmissionStore::new(database.pool.clone());
        require(store.claim_ready_for_profile("offline-claim", 60_000, AdmissionContract::OutputEconomicsV2, DREAMINA_SUBMIT_COMMAND_SCHEMA, DREAMINA_PROFILE_ID)
            .await.map_err(debug_error)?.is_none(), "reauth head allowed new admission work claim")?;
        sqlx::query("UPDATE provider_account_credential_heads SET lifecycle_state='active' WHERE provider_account_id=$1")
            .bind(DREAMINA_ACCOUNT_ID).execute(&database.pool).await.map_err(debug_error)?;
        require(store.claim_ready_for_profile("offline-claim", 60_000, AdmissionContract::OutputEconomicsV2, DREAMINA_SUBMIT_COMMAND_SCHEMA, DREAMINA_PROFILE_ID)
            .await.map_err(debug_error)?.is_some(), "active head did not resume admission claim")
    }.await;
    combine(result, database.cleanup().await)
}

#[cfg(unix)]
mod process_release {
    use super::*;
    use gpt_image_2_gateway::{
        GatedCliCommand, GatedCliSubmitCodec, GatedCliSubmitDriver, ProviderSubmitCredentialGuard,
        ProviderSubmitOrchestrationStore, RemoteTaskQuarantinedReceipt,
    };
    use std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
    };

    #[derive(Clone)]
    struct RevokeAtProcessReleaseStore {
        inner: PostgresProviderTaskStore,
        pool: PgPool,
        calls: Arc<AtomicUsize>,
        revoke: Arc<AtomicBool>,
        guard_ready: Option<Arc<Semaphore>>,
        guard_proceed: Option<Arc<Semaphore>>,
    }

    impl ProviderSubmitOrchestrationStore for RevokeAtProcessReleaseStore {
        async fn guard_submit_credentials(
            &self,
            intent: &ProviderSubmitIntent,
            context: &ProviderExecutionContext,
        ) -> Result<ProviderSubmitCredentialGuard, ProviderTaskStoreError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            // The first guard protects the outer journal. Revoke only after that
            // permission, at the actual process release (including recovery).
            if call >= 2 && self.revoke.load(Ordering::SeqCst) {
                reauth(&self.pool)
                    .await
                    .map_err(|_| ProviderTaskStoreError::Unavailable)?;
            }
            let guard =
                ProviderTaskStore::guard_submit_credentials(&self.inner, intent, context).await?;
            if call == 2 {
                if let (Some(ready), Some(proceed)) = (&self.guard_ready, &self.guard_proceed) {
                    ready.add_permits(1);
                    proceed.acquire().await.unwrap().forget();
                    // With one async worker, the caller resumes only after the
                    // synchronous return has queued the marker-writing worker.
                    ready.add_permits(1);
                }
            }
            Ok(guard)
        }
        async fn acquire_submit(
            &self,
            request: &RemoteTaskSubmitReservation,
        ) -> Result<ProviderSubmitAcquire, ProviderTaskStoreError> {
            ProviderTaskStore::acquire_submit(&self.inner, request).await
        }
        async fn record_submit_failure(
            &self,
            request: &RemoteTaskSubmitFailure,
        ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
            ProviderTaskStore::record_submit_failure(&self.inner, request).await
        }
        async fn record_submit_receipt(
            &self,
            request: &RemoteTaskSubmitReceipt,
        ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
            ProviderTaskStore::record_submit_receipt(&self.inner, request).await
        }
        async fn quarantine_submit_receipt(
            &self,
            request: &RemoteTaskQuarantinedReceipt,
        ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
            ProviderTaskStore::quarantine_submit_receipt(&self.inner, request).await
        }
        async fn attach(
            &self,
            request: &RemoteTaskAttach,
        ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
            ProviderTaskStore::attach(&self.inner, request).await
        }
        async fn load(
            &self,
            submission_id: Uuid,
        ) -> Result<Option<ProviderRemoteTask>, ProviderTaskStoreError> {
            ProviderTaskStore::load(&self.inner, submission_id).await
        }
    }

    struct LocalCodec {
        side_effect: PathBuf,
        shell_sha256: String,
        finish: Option<PathBuf>,
    }
    impl GatedCliSubmitCodec for LocalCodec {
        type Payload = FrozenPayload;
        fn provider_id(&self) -> &'static str {
            DREAMINA_PROVIDER_ID
        }
        fn command(
            &self,
            _: &ProviderSubmitIntent,
            _: &ProviderExecutionContext,
            _: &SingleOutputCommand<FrozenPayload>,
            workspace: &WorkingDirectory,
        ) -> Result<GatedCliCommand, ProviderFailure> {
            let script = if self.finish.is_some() {
                "printf invoked >> \"$1\"; while [ ! -f \"$2\" ]; do /bin/sleep 0.01; done; printf receipt"
            } else {
                "printf invoked >> \"$1\"; printf receipt"
            };
            GatedCliCommand::new(
                "/bin/sh",
                &self.shell_sha256,
                workspace.path(),
                vec![
                    "-c".into(),
                    script.into(),
                    "offline-cli".into(),
                    self.side_effect.to_string_lossy().into_owned(),
                    self.finish
                        .as_ref()
                        .map(|path| path.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                ],
                BTreeMap::new(),
                Vec::new(),
                Duration::from_secs(5),
                Duration::from_millis(100),
            )
            .map_err(|_| {
                ProviderFailure::new(
                    ProviderFailureClass::Permanent,
                    "offline_command_invalid",
                    EffectCertainty::NoRemoteEffect,
                    RetryDirective::Never,
                )
                .unwrap()
            })
        }
        fn decode_receipt(
            &self,
            intent: &ProviderSubmitIntent,
            _: &SingleOutputCommand<FrozenPayload>,
            stdout: &[u8],
        ) -> Result<PendingOperation, ProviderFailure> {
            assert_eq!(stdout, b"receipt");
            Ok(PendingOperation::new(
                RemoteOperationRef::new(
                    DREAMINA_PROVIDER_ID,
                    intent.submission_id.to_string(),
                    "offline-gated-task",
                )
                .unwrap(),
                None,
                Some(0),
            ))
        }
    }

    #[tokio::test]
    async fn fresh_credential_real_process_release_and_ready_recovery_both_fail_closed()
    -> TestResult {
        let Some(database) = TestDatabase::new().await? else {
            return Ok(());
        };
        let result = async {
            prepare(&database.pool).await?;
            let lease = claim(&database.pool).await?;
            let root = tempfile::tempdir().map_err(debug_error)?;
            let side_effect = root.path().join("fake-provider-invoked");
            let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
            let workspace = root.path().join("workspace");
            fs::create_dir(&workspace).map_err(debug_error)?;
            fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
            let calls = Arc::new(AtomicUsize::new(0));
            let revoke = Arc::new(AtomicBool::new(true));
            let store = RevokeAtProcessReleaseStore {
                inner: PostgresProviderTaskStore::new(database.pool.clone()),
                pool: database.pool.clone(), calls: calls.clone(), revoke: revoke.clone(), guard_ready: None, guard_proceed: None,
            };
            let driver = GatedCliSubmitDriver::new(LocalCodec { side_effect: side_effect.clone(), shell_sha256: hex::encode(Sha256::digest(fs::read("/bin/sh").map_err(debug_error)?)), finish: None }, runner,
                hex::encode(Sha256::digest(fs::read(runner).map_err(debug_error)?)), WorkingDirectory::new_private(&workspace).map_err(debug_error)?).map_err(debug_error)?;
            let journal = root.path().join("journal");
            let orchestrator = ProviderSubmitOrchestrator::new(store, driver, 30_000, &journal).map_err(debug_error)?;
            let make_work = || ProviderSubmitWork::new(&lease, command(&lease)).map_err(debug_error);
            let entry = journal.join(lease.submission_id.simple().to_string());
            let first = orchestrator.submit(make_work()?).await.map_err(debug_error)?;
            require(matches!(first, ProviderSubmitOutcome::AwaitingEvidence(_)) && calls.load(Ordering::SeqCst) == 2,
                format!("prepared process was not checked at its first release: {first:?}"))?;
            require(entry.join("dispatch-released.json").exists() && !entry.join("process-dispatch-released.json").exists() && !side_effect.exists(), "outer journal permission leaked into provider invocation")?;
            let replay = orchestrator.submit(make_work()?).await.map_err(debug_error)?;
            require(matches!(replay, ProviderSubmitOutcome::AwaitingEvidence(_)) && calls.load(Ordering::SeqCst) == 3
                && !entry.join("process-dispatch-released.json").exists() && !side_effect.exists(),
                "Ready recovery bypassed fresh process credential gate")?;
            let durable: (String, i64) = sqlx::query_as("SELECT state,(SELECT count(*) FROM provider_receipts) FROM provider_remote_submit_intents WHERE submission_id=$1")
                .bind(lease.submission_id).fetch_one(&database.pool).await.map_err(debug_error)?;
            require(durable == ("sending".into(), 0), "credential denial created terminal financial evidence")?;
            // The same ready process may proceed only after explicit credential restoration.
            revoke.store(false, Ordering::SeqCst);
            sqlx::query("UPDATE provider_account_credential_heads SET lifecycle_state='active' WHERE provider_account_id=$1")
                .bind(DREAMINA_ACCOUNT_ID).execute(&database.pool).await.map_err(debug_error)?;
            let accepted = orchestrator.submit(make_work()?).await.map_err(debug_error)?;
            require(matches!(accepted, ProviderSubmitOutcome::Attached(_)) && fs::read(&side_effect).map_err(debug_error)? == b"invoked", "restored credential did not release exactly one existing process")?;
            reauth(&database.pool).await?;
            let calls_before = calls.load(Ordering::SeqCst);
            require(matches!(orchestrator.submit(make_work()?).await.map_err(debug_error)?, ProviderSubmitOutcome::Attached(_))
                && calls.load(Ordering::SeqCst) == calls_before && fs::read(&side_effect).map_err(debug_error)? == b"invoked",
                "reauth blocked accepted task replay or resent it")
        }.await;
        combine(result, database.cleanup().await)
    }
    #[tokio::test]
    async fn fresh_credential_running_observation_survives_reauth_without_release_or_resend()
    -> TestResult {
        let Some(database) = TestDatabase::new().await? else {
            return Ok(());
        };
        let result = async {
            prepare(&database.pool).await?;
            let lease = claim(&database.pool).await?;
            let root = tempfile::tempdir().map_err(debug_error)?;
            let side_effect = root.path().join("fake-provider-invoked");
            let finish = root.path().join("finish");
            let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
            let workspace = root.path().join("workspace");
            fs::create_dir(&workspace).map_err(debug_error)?;
            fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700))
                .map_err(debug_error)?;
            let calls = Arc::new(AtomicUsize::new(0));
            let store = RevokeAtProcessReleaseStore {
                inner: PostgresProviderTaskStore::new(database.pool.clone()),
                pool: database.pool.clone(),
                calls: calls.clone(),
                revoke: Arc::new(AtomicBool::new(false)),
                guard_ready: None,
                guard_proceed: None,
            };
            let driver = GatedCliSubmitDriver::new(
                LocalCodec {
                    side_effect: side_effect.clone(),
                    shell_sha256: hex::encode(Sha256::digest(
                        fs::read("/bin/sh").map_err(debug_error)?,
                    )),
                    finish: Some(finish.clone()),
                },
                runner,
                hex::encode(Sha256::digest(fs::read(runner).map_err(debug_error)?)),
                WorkingDirectory::new_private(&workspace).map_err(debug_error)?,
            )
            .map_err(debug_error)?;
            let journal = root.path().join("journal");
            let orchestrator = Arc::new(
                ProviderSubmitOrchestrator::new(store, driver, 30_000, &journal)
                    .map_err(debug_error)?,
            );
            let submit = {
                let orchestrator = orchestrator.clone();
                let work = ProviderSubmitWork::new(&lease, command(&lease)).map_err(debug_error)?;
                tokio::spawn(async move { orchestrator.submit(work).await })
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                while !side_effect.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .map_err(debug_error)?;
            require(
                calls.load(Ordering::SeqCst) == 2,
                "active process did not pass both release checks",
            )?;
            // Keep the original command running while another caller observes it.
            tokio::time::timeout(Duration::from_secs(2), reauth(&database.pool))
                .await
                .map_err(debug_error)??;
            let mut replay = {
                let orchestrator = orchestrator.clone();
                let work = ProviderSubmitWork::new(&lease, command(&lease)).map_err(debug_error)?;
                tokio::spawn(async move { orchestrator.submit(work).await })
            };
            require(
                tokio::time::timeout(Duration::from_millis(100), &mut replay)
                    .await
                    .is_err(),
                "Running observation did not wait for original command",
            )?;
            require(
                calls.load(Ordering::SeqCst) == 2,
                "Running observation requested a new credential release",
            )?;
            fs::write(&finish, b"done").map_err(debug_error)?;
            require(
                matches!(
                    submit.await.map_err(debug_error)?.map_err(debug_error)?,
                    ProviderSubmitOutcome::Attached(_)
                ) && matches!(
                    replay.await.map_err(debug_error)?.map_err(debug_error)?,
                    ProviderSubmitOutcome::Attached(_)
                ) && fs::read(&side_effect).map_err(debug_error)? == b"invoked"
                    && calls.load(Ordering::SeqCst) == 2,
                "Running task was blocked, released again, or resent after reauth",
            )
        }
        .await;
        combine(result, database.cleanup().await)
    }
    #[test]
    fn fresh_credential_cancelled_release_retains_lock_in_the_marker_worker() -> TestResult {
        tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1)
            .enable_all().build().map_err(debug_error)?.block_on(async {
                let Some(database) = TestDatabase::new().await? else { return Ok(()); };
                let result = async {
                    prepare(&database.pool).await?;
                    let lease = claim(&database.pool).await?;
                    let root = tempfile::tempdir().map_err(debug_error)?;
                    let side_effect = root.path().join("fake-provider-invoked");
                    let runner = Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
                    let workspace = root.path().join("workspace");
                    fs::create_dir(&workspace).map_err(debug_error)?;
                    fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
                    let ready = Arc::new(Semaphore::new(0));
                    let proceed = Arc::new(Semaphore::new(0));
                    let calls = Arc::new(AtomicUsize::new(0));
                    let store = RevokeAtProcessReleaseStore {
                        inner: PostgresProviderTaskStore::new(database.pool.clone()), pool: database.pool.clone(),
                        calls: calls.clone(), revoke: Arc::new(AtomicBool::new(false)),
                        guard_ready: Some(ready.clone()), guard_proceed: Some(proceed.clone()),
                    };
                    let driver = GatedCliSubmitDriver::new(LocalCodec {
                        side_effect: side_effect.clone(), finish: None,
                        shell_sha256: hex::encode(Sha256::digest(fs::read("/bin/sh").map_err(debug_error)?)),
                    }, runner, hex::encode(Sha256::digest(fs::read(runner).map_err(debug_error)?)), WorkingDirectory::new_private(&workspace).map_err(debug_error)?).map_err(debug_error)?;
                    let journal = root.path().join("journal");
                    let orchestrator = Arc::new(ProviderSubmitOrchestrator::new(store, driver, 30_000, &journal).map_err(debug_error)?);
                    let submit = { let orchestrator = orchestrator.clone(); let work = ProviderSubmitWork::new(&lease, command(&lease)).map_err(debug_error)?;
                        tokio::spawn(async move { orchestrator.submit(work).await }) };
                    tokio::time::timeout(Duration::from_secs(5), ready.acquire()).await.map_err(debug_error)?.map_err(debug_error)?.forget();
                    let occupied = Arc::new(Semaphore::new(0));
                    let occupied_signal = occupied.clone();
                    let (unblock, blocked) = std::sync::mpsc::channel::<()>();
                    let blocker = tokio::task::spawn_blocking(move || {
                        occupied_signal.add_permits(1);
                        // A bounded wait also guarantees cleanup on a failed assertion.
                        let _ = blocked.recv_timeout(Duration::from_secs(5));
                    });
                    occupied.acquire().await.map_err(debug_error)?.forget();
                    proceed.add_permits(1);
                    ready.acquire().await.map_err(debug_error)?.forget();
                    submit.abort();
                    require(submit.await.is_err_and(|error| error.is_cancelled()), "submit was not cancelled while marker worker queued")?;
                    let mut connection = database.pool.acquire().await.map_err(debug_error)?;
                    sqlx::query("SET lock_timeout='50ms'").execute(&mut *connection).await.map_err(debug_error)?;
                    let blocked_update = sqlx::query("UPDATE provider_account_credential_heads SET lifecycle_state='reauth_required' WHERE provider_account_id=$1")
                        .bind(DREAMINA_ACCOUNT_ID).execute(&mut *connection).await;
                    require(blocked_update.as_ref().err().and_then(sqlx::Error::as_database_error).and_then(|error| error.code()).as_deref() == Some("55P03"),
                        "cancelling the caller released credential locks before process permission was durable")?;
                    unblock.send(()).map_err(debug_error)?;
                    blocker.await.map_err(debug_error)?;
                    let marker = journal.join(lease.submission_id.simple().to_string()).join("process-dispatch-released.json");
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while !marker.exists() { tokio::time::sleep(Duration::from_millis(10)).await; }
                    }).await.map_err(debug_error)?;
                    reauth(&database.pool).await?;
                    require(matches!(orchestrator.submit(ProviderSubmitWork::new(&lease, command(&lease)).map_err(debug_error)?).await.map_err(debug_error)?, ProviderSubmitOutcome::Attached(_))
                        && fs::read(&side_effect).map_err(debug_error)? == b"invoked" && calls.load(Ordering::SeqCst) == 2,
                        "cancelled authorized process could not be observed exactly once after reauth")
                }.await;
                combine(result, database.cleanup().await)
            })
    }
}
