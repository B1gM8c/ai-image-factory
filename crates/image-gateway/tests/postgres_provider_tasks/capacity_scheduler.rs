use super::*;
use gpt_image_2_gateway::{
    AdminReadStore, PostgresAdminReadStore, ProviderCapacityQueryDriver,
    ProviderCapacityQueryError, ProviderCapacityQueryObservation,
    ProviderCapacityReconciliationLease, ProviderCapacityReconciliationRun,
    ProviderCapacityReconciliationWorker,
};
use image_provider_sdk::ProviderTaskProgress;

/// This fake has only a query interface: no submission, cancellation or download.
#[derive(Clone)]
struct CapacityQueryFake {
    expected_id: &'static str,
    calls: Arc<AtomicUsize>,
    response: Result<ProviderCapacityQueryObservation, ProviderCapacityQueryError>,
    started: Option<Arc<tokio::sync::Semaphore>>,
    release: Option<Arc<tokio::sync::Semaphore>>,
}

impl CapacityQueryFake {
    fn new(
        expected_id: &'static str,
        response: Result<ProviderCapacityQueryObservation, ProviderCapacityQueryError>,
    ) -> Self {
        Self {
            expected_id,
            calls: Arc::new(AtomicUsize::new(0)),
            response,
            started: None,
            release: None,
        }
    }

    fn succeeded(expected_id: &'static str) -> Self {
        Self::new(
            expected_id,
            Ok(ProviderCapacityQueryObservation::Terminal {
                terminal_state: ProviderCapacityTerminalState::Succeeded,
                event_identity: format!("capacity-fixture:{expected_id}"),
            }),
        )
    }
}

impl ProviderCapacityQueryDriver for CapacityQueryFake {
    async fn query(
        &self,
        lease: &ProviderCapacityReconciliationLease,
    ) -> Result<ProviderCapacityQueryObservation, ProviderCapacityQueryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(lease.remote_operation_id(), Some(self.expected_id));
        assert_eq!(lease.reconciliation.provider_account_id, ACCOUNT_ID);
        assert_eq!(lease.context().execution_profile_id(), PROFILE_ID);
        if let Some(started) = &self.started {
            started.add_permits(1);
        }
        if let Some(release) = &self.release {
            release.acquire().await.unwrap().forget();
        }
        self.response.clone()
    }
}

fn capacity_worker(
    store: &PostgresProviderTaskStore,
    driver: CapacityQueryFake,
    owner: &str,
) -> TestResult<ProviderCapacityReconciliationWorker<CapacityQueryFake>> {
    ProviderCapacityReconciliationWorker::new(
        store.clone(),
        driver,
        claim_scope(),
        owner.to_owned(),
        30_000,
        Duration::from_secs(1),
    )
    .map_err(debug_error)
}

async fn original_state(pool: &PgPool, submission_id: Uuid) -> TestResult<serde_json::Value> {
    sqlx::query_scalar(
        r#"SELECT jsonb_build_object(
          'submission', to_jsonb(submission), 'execution', to_jsonb(execution),
          'intent', to_jsonb(intent),
          'submission_count', (SELECT count(*) FROM provider_submissions),
          'intent_count', (SELECT count(*) FROM provider_remote_submit_intents),
          'remote_task_count', (SELECT count(*) FROM provider_remote_tasks))
        FROM provider_submissions submission
        JOIN executor_executions execution USING (submission_id, executor_execution_id)
        JOIN provider_remote_submit_intents intent USING (submission_id, executor_execution_id)
        WHERE submission.submission_id = $1"#,
    )
    .bind(submission_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

async fn allocation_state(pool: &PgPool, execution_id: Uuid) -> TestResult<serde_json::Value> {
    sqlx::query_scalar(
        "SELECT to_jsonb(allocation) FROM executor_capacity_allocations allocation WHERE executor_execution_id=$1",
    )
    .bind(execution_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

async fn assert_manual_snapshots(pool: &PgPool, held: &str, manual: &str) -> TestResult {
    let reader = PostgresAdminReadStore::new(pool.clone());
    let account_id = ACCOUNT_ID.to_string();
    let provider = reader.provider_accounts().await.map_err(debug_error)?;
    let provider = provider
        .accounts
        .iter()
        .find(|row| row.provider_account_id == account_id)
        .ok_or("provider account snapshot missing")?;
    require(
        provider.allocated_count == held
            && provider.reconciliation_count == held
            && provider.manual_reconciliation_count == manual
            && provider.execution_count == "0",
        "provider snapshot misreported held/manual capacity as active CLI execution",
    )?;
    let runtime = reader
        .provider_account_concurrency(None)
        .await
        .map_err(debug_error)?;
    let runtime = runtime
        .accounts
        .iter()
        .find(|row| row.provider_account_id == account_id)
        .ok_or("runtime account snapshot missing")?;
    require(
        runtime.allocated_count == held
            && runtime.reconciliation_count == held
            && runtime.manual_reconciliation_count == manual
            && runtime.execution_count == "0",
        "runtime snapshot disagreed with capacity disposition",
    )?;
    let scheduler = reader.scheduler(60_000).await.map_err(debug_error)?;
    let scheduler = scheduler
        .capacity
        .iter()
        .find(|row| row.provider_account_id == account_id)
        .ok_or("scheduler account snapshot missing")?;
    require(
        scheduler.allocated_count == held
            && scheduler.reconciliation_count == held
            && scheduler.manual_reconciliation_count == manual
            && scheduler.execution_count == "0",
        "scheduler snapshot disagreed with capacity disposition",
    )
}

#[tokio::test]
async fn capacity_scheduler_missing_id_is_manual_without_query_release_or_repeat_due() -> TestResult
{
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(db.pool.clone());
        let original = seed_deadline_quarantine(&db.pool, &store, "capacity-missing-id").await?;
        let before = original_state(&db.pool, original.submission_id).await?;
        let allocation_before = allocation_state(&db.pool, original.executor_execution_id).await?;
        let driver = CapacityQueryFake::succeeded("must-not-be-queried");
        let worker = capacity_worker(&store, driver.clone(), "capacity-missing-worker")?;
        require(
            worker.run_once().await.map_err(debug_error)?
                == ProviderCapacityReconciliationRun::Classified(1),
            "missing original ID was not classified",
        )?;
        for _ in 0..3 {
            require(
                worker.run_once().await.map_err(debug_error)?
                    == ProviderCapacityReconciliationRun::Idle,
                "manual original was repeatedly scheduled",
            )?;
        }
        let row = store
            .load_capacity_reconciliation(original.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or("capacity row missing")?;
        require(
            row.state == ProviderCapacityReconciliationState::Active
                && row.review_status == "manual_required"
                && row.manual_reason.as_deref() == Some("missing_original_id")
                && row.reconciliation_owner.is_none()
                && row.query_failures == 0,
            "missing-ID review changed capacity authority",
        )?;
        require(
            driver.calls.load(Ordering::SeqCst) == 0,
            "missing ID reached query driver",
        )?;
        require(
            original_state(&db.pool, original.submission_id).await? == before,
            "classification mutated original execution/receipt or created another submission",
        )?;
        require(
            allocation_state(&db.pool, original.executor_execution_id).await? == allocation_before,
            "classification mutated held allocation",
        )?;
        assert_manual_snapshots(&db.pool, "1", "1").await
    }
    .await;
    combine(result, db.cleanup().await)
}

#[tokio::test]
async fn capacity_scheduler_late_original_receipt_rearms_and_releases_only_capacity() -> TestResult
{
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(db.pool.clone());
        let original = seed_deadline_quarantine(&db.pool, &store, "capacity-late-id").await?;
        let driver = CapacityQueryFake::succeeded("original-late-id");
        let worker = capacity_worker(&store, driver.clone(), "capacity-late-worker")?;
        require(worker.run_once().await.map_err(debug_error)? == ProviderCapacityReconciliationRun::Classified(1),
            "missing original was not classified first")?;
        require(sqlx::query("UPDATE provider_capacity_reconciliations SET review_status='auto', manual_reason=NULL WHERE submission_id=$1")
            .bind(original.submission_id).execute(&db.pool).await.is_err(),
            "manual state rearmed without new original receipt")?;
        store.record_submit_receipt(&submit_receipt!(&store, &original,
            "original-late-id", "capacity-late-id-receipt")).await.map_err(debug_error)?;
        let rearmed = store.load_capacity_reconciliation(original.submission_id).await.map_err(debug_error)?
            .ok_or("rearmed row missing")?;
        require(rearmed.review_status == "auto" && rearmed.manual_reason.is_none()
            && rearmed.evidence_revision > 0, "actual late receipt did not rearm capacity query")?;
        for wrong_scope in [
            ProviderTaskClaimScope { provider_account_id: Uuid::new_v4(), ..claim_scope() },
            ProviderTaskClaimScope { execution_profile_id: Uuid::new_v4(), ..claim_scope() },
        ] {
            let wrong_worker = ProviderCapacityReconciliationWorker::new(
                store.clone(), driver.clone(), wrong_scope, "capacity-wrong-scope".into(),
                30_000, Duration::from_secs(1),
            ).map_err(debug_error)?;
            require(wrong_worker.run_once().await.map_err(debug_error)? == ProviderCapacityReconciliationRun::Idle,
                "capacity worker claimed another account or profile")?;
        }
        require(driver.calls.load(Ordering::SeqCst) == 0, "wrong scope reached query driver")?;
        let before = original_state(&db.pool, original.submission_id).await?;
        require(worker.run_once().await.map_err(debug_error)? == ProviderCapacityReconciliationRun::Released,
            "same-ID terminal observation did not release capacity")?;
        require(worker.run_once().await.map_err(debug_error)? == ProviderCapacityReconciliationRun::Idle,
            "released original was queried twice")?;
        require(driver.calls.load(Ordering::SeqCst) == 1, "late receipt query was duplicated")?;
        let row = store.load_capacity_reconciliation(original.submission_id).await.map_err(debug_error)?
            .ok_or("released row missing")?;
        require(row.state == ProviderCapacityReconciliationState::Released
            && row.evidence.as_ref().is_some_and(|evidence| evidence.outcome == ProviderCapacityEvidenceOutcome::RemoteTerminal {
                remote_operation_id: "original-late-id".into(), terminal_state: ProviderCapacityTerminalState::Succeeded,
            }), "released evidence lost original remote identity")?;
        require(allocation_state(&db.pool, original.executor_execution_id).await?["state"] == "released",
            "capacity allocation remains held after authoritative terminal")?;
        require(original_state(&db.pool, original.submission_id).await? == before,
            "capacity-only release changed original job, receipt or submission cardinality")?;
        assert_manual_snapshots(&db.pool, "0", "0").await
    }.await;
    combine(result, db.cleanup().await)
}

#[tokio::test]
async fn capacity_scheduler_three_query_failures_stop_without_release_or_resubmit() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(db.pool.clone());
        let original = seed_deadline_quarantine(&db.pool, &store, "capacity-query-errors").await?;
        store
            .record_submit_receipt(&submit_receipt!(
                &store,
                &original,
                "original-unavailable",
                "capacity-unavailable-receipt"
            ))
            .await
            .map_err(debug_error)?;
        let before = original_state(&db.pool, original.submission_id).await?;
        let driver = CapacityQueryFake::new(
            "original-unavailable",
            Err(ProviderCapacityQueryError::QueryUnavailable),
        );
        let worker = capacity_worker(&store, driver.clone(), "capacity-error-worker")?;
        for attempt in 1..=3 {
            require(
                worker.run_once().await.map_err(debug_error)?
                    == ProviderCapacityReconciliationRun::Deferred,
                "unavailable query did not defer",
            )?;
            let row = store
                .load_capacity_reconciliation(original.submission_id)
                .await
                .map_err(debug_error)?
                .ok_or("failed-query row missing")?;
            require(
                row.query_failures == attempt
                    && row.state == ProviderCapacityReconciliationState::Active,
                "query budget was not durable or released unknown capacity",
            )?;
            require(
                worker.run_once().await.map_err(debug_error)?
                    == ProviderCapacityReconciliationRun::Idle,
                "query backoff was bypassed",
            )?;
            if attempt < 3 {
                sleep_until_database_time(&db.pool, row.available_at_ms).await?;
            } else {
                require(
                    row.review_status == "manual_required"
                        && row.manual_reason.as_deref() == Some("query_unavailable"),
                    "exhausted queries stayed infinitely automatic",
                )?;
            }
        }
        let restarted = capacity_worker(
            &PostgresProviderTaskStore::new(db.pool.clone()),
            driver.clone(),
            "capacity-error-restart",
        )?;
        require(
            restarted.run_once().await.map_err(debug_error)?
                == ProviderCapacityReconciliationRun::Idle,
            "restart reset exhausted query budget",
        )?;
        require(
            driver.calls.load(Ordering::SeqCst) == 3,
            "query retry budget changed",
        )?;
        require(
            allocation_state(&db.pool, original.executor_execution_id).await?["state"] == "held",
            "query transport errors released capacity",
        )?;
        require(
            original_state(&db.pool, original.submission_id).await? == before,
            "query failures altered original task or created another submission",
        )?;
        assert_manual_snapshots(&db.pool, "1", "1").await
    }
    .await;
    combine(result, db.cleanup().await)
}

#[tokio::test]
async fn capacity_scheduler_binding_mismatch_requires_manual_review_without_retry() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(db.pool.clone());
        let original =
            seed_deadline_quarantine(&db.pool, &store, "capacity-mismatched-binding").await?;
        store
            .record_submit_receipt(&submit_receipt!(
                &store,
                &original,
                "original-mismatched-binding",
                "capacity-mismatched-receipt"
            ))
            .await
            .map_err(debug_error)?;
        let before = original_state(&db.pool, original.submission_id).await?;
        let driver = CapacityQueryFake::new(
            "original-mismatched-binding",
            Err(ProviderCapacityQueryError::BindingMismatch),
        );
        let worker = capacity_worker(&store, driver.clone(), "capacity-binding-worker")?;
        require(
            worker.run_once().await.map_err(debug_error)?
                == ProviderCapacityReconciliationRun::Deferred,
            "binding mismatch was not recorded",
        )?;
        let row = store
            .load_capacity_reconciliation(original.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or("binding-mismatch row missing")?;
        require(
            row.state == ProviderCapacityReconciliationState::Active
                && row.review_status == "manual_required"
                && row.query_failures == 3,
            "permanent binding mismatch remained automatic or released capacity",
        )?;
        require(
            worker.run_once().await.map_err(debug_error)?
                == ProviderCapacityReconciliationRun::Idle
                && driver.calls.load(Ordering::SeqCst) == 1,
            "permanent binding error was retried",
        )?;
        require(
            original_state(&db.pool, original.submission_id).await? == before,
            "binding mismatch mutated original task or submitted another task",
        )?;
        assert_manual_snapshots(&db.pool, "1", "1").await
    }
    .await;
    combine(result, db.cleanup().await)
}

#[tokio::test]
async fn capacity_scheduler_concurrent_workers_query_original_once() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(db.pool.clone());
        let original = seed_deadline_quarantine(&db.pool, &store, "capacity-concurrent").await?;
        store
            .record_submit_receipt(&submit_receipt!(
                &store,
                &original,
                "original-concurrent",
                "capacity-concurrent-receipt"
            ))
            .await
            .map_err(debug_error)?;
        let before = original_state(&db.pool, original.submission_id).await?;
        let started = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let mut driver = CapacityQueryFake::succeeded("original-concurrent");
        driver.started = Some(started.clone());
        driver.release = Some(release.clone());
        let first = capacity_worker(&store, driver.clone(), "capacity-concurrent-a")?;
        let second = capacity_worker(&store, driver.clone(), "capacity-concurrent-b")?;
        let query = tokio::spawn(async move { first.run_once().await });
        tokio::time::timeout(Duration::from_secs(5), started.acquire())
            .await
            .map_err(|_| "capacity query never started")?
            .map_err(debug_error)?
            .forget();
        let second_result = second.run_once().await;
        release.add_permits(1);
        let first_result = query.await.map_err(debug_error)?.map_err(debug_error)?;
        require(
            second_result.map_err(debug_error)? == ProviderCapacityReconciliationRun::Idle
                && first_result == ProviderCapacityReconciliationRun::Released,
            "concurrent workers both claimed the original",
        )?;
        require(
            driver.calls.load(Ordering::SeqCst) == 1,
            "concurrent reconciliation duplicated query",
        )?;
        require(
            original_state(&db.pool, original.submission_id).await? == before,
            "concurrent release altered original task or resubmitted",
        )?;
        Ok(())
    }
    .await;
    combine(result, db.cleanup().await)
}

#[tokio::test]
async fn capacity_scheduler_pending_keeps_capacity_and_original_identity() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(db.pool.clone());
        let original = seed_deadline_quarantine(&db.pool, &store, "capacity-pending").await?;
        store
            .record_submit_receipt(&submit_receipt!(
                &store,
                &original,
                "original-pending",
                "capacity-pending-receipt"
            ))
            .await
            .map_err(debug_error)?;
        let before = original_state(&db.pool, original.submission_id).await?;
        let driver = CapacityQueryFake::new(
            "original-pending",
            Ok(ProviderCapacityQueryObservation::Pending {
                next_poll_after_ms: 30_000,
                progress: ProviderTaskProgress {
                    provider_status: "Queueing".into(),
                    queue_position: Some(5),
                    queue_total: Some(50),
                    estimated_remaining_seconds: Some(60),
                },
            }),
        );
        let worker = capacity_worker(&store, driver.clone(), "capacity-pending-worker")?;
        require(
            worker.run_once().await.map_err(debug_error)?
                == ProviderCapacityReconciliationRun::Pending,
            "upstream waiting observation was not pending",
        )?;
        require(
            worker.run_once().await.map_err(debug_error)?
                == ProviderCapacityReconciliationRun::Idle,
            "pending query ignored its cadence",
        )?;
        require(
            driver.calls.load(Ordering::SeqCst) == 1,
            "pending original was queried twice",
        )?;
        let row = store
            .load_capacity_reconciliation(original.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or("pending row missing")?;
        require(
            row.state == ProviderCapacityReconciliationState::Active
                && row.review_status == "auto"
                && row.query_failures == 0
                && row.evidence.is_none(),
            "pending query invented terminal authority",
        )?;
        require(
            original_state(&db.pool, original.submission_id).await? == before,
            "pending query changed original identity or submitted another task",
        )?;
        assert_manual_snapshots(&db.pool, "1", "0").await
    }
    .await;
    combine(result, db.cleanup().await)
}

#[tokio::test]
async fn capacity_scheduler_shutdown_bounds_blocked_claim_without_query_or_release() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(db.pool.clone());
        let original = seed_deadline_quarantine(&db.pool, &store, "capacity-shutdown").await?;
        store
            .record_submit_receipt(&submit_receipt!(
                &store,
                &original,
                "original-shutdown",
                "capacity-shutdown-receipt"
            ))
            .await
            .map_err(debug_error)?;
        let before = original_state(&db.pool, original.submission_id).await?;
        let allocation_before = allocation_state(&db.pool, original.executor_execution_id).await?;
        let mut blocker = db.pool.begin().await.map_err(debug_error)?;
        let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *blocker)
            .await
            .map_err(debug_error)?;
        // Allocation locks are skipped by claim. Lock the reconciliation row so
        // the claim's UPDATE actually waits after it has acquired allocation.
        sqlx::query(
            "SELECT submission_id FROM provider_capacity_reconciliations WHERE submission_id=$1 FOR UPDATE",
        )
        .bind(original.submission_id)
        .fetch_one(&mut *blocker)
        .await
        .map_err(debug_error)?;
        let driver = CapacityQueryFake::succeeded("original-shutdown");
        let worker = capacity_worker(&store, driver.clone(), "capacity-shutdown-worker")?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let mut running = tokio::spawn(async move {
            worker
                .run_until_shutdown(
                    async {
                        let _ = shutdown_rx.await;
                    },
                    Duration::from_millis(100),
                )
                .await;
        });
        let blocked = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))",
                )
                .bind(blocker_pid)
                .fetch_one(&db.pool)
                .await
                .map_err(debug_error)?;
                if waiting {
                    return Ok::<(), String>(());
                }
                if running.is_finished() {
                    return Err("capacity worker exited before blocked claim was observed".into());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "capacity claim never entered a database lock wait".to_string())
        .and_then(|observed| observed);
        let shutdown_started = tokio::time::Instant::now();
        let _ = shutdown_tx.send(());
        let stopped = tokio::time::timeout(Duration::from_secs(2), &mut running).await;
        let bounded = stopped.is_ok() && shutdown_started.elapsed() < Duration::from_secs(2);
        if !bounded {
            running.abort();
            let _ = running.await;
        }
        // Always release the synthetic lock, including an assertion failure.
        blocker.rollback().await.map_err(debug_error)?;
        blocked?;
        require(bounded, "shutdown did not bound its blocked claim drain")?;
        stopped
            .map_err(|_| "capacity worker did not stop".to_string())?
            .map_err(debug_error)?;
        require(
            driver.calls.load(Ordering::SeqCst) == 0,
            "blocked claim reached the provider query",
        )?;
        require(
            allocation_state(&db.pool, original.executor_execution_id).await? == allocation_before,
            "shutdown mutated or released original capacity",
        )?;
        require(
            original_state(&db.pool, original.submission_id).await? == before,
            "shutdown mutated original task or created another submission",
        )?;
        let row = store
            .load_capacity_reconciliation(original.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or("shutdown reconciliation row missing")?;
        require(
            row.state == ProviderCapacityReconciliationState::Active
                && row.review_status == "auto"
                && row.reconciliation_owner.is_none()
                && row.evidence.is_none(),
            "timed-out claim committed a lease or invented terminal evidence",
        )?;
        assert_manual_snapshots(&db.pool, "1", "0").await
    }
    .await;
    combine(result, db.cleanup().await)
}
