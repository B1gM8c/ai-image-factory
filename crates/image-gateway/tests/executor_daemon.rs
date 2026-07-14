use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use gpt_image_2_gateway::executor::{
    DurableRunner, ExecutorClaimScope, ExecutorDaemon, ExecutorDaemonError, ExecutorDaemonRun,
    ExecutorResultManifest, ExecutorSubmissionError, ExecutorSubmissionLease,
    ExecutorSubmissionOutcome, ExecutorSubmissionStore, PreparedExecutorSubmission, RunnerError,
    RunnerOutcome,
};
use uuid::Uuid;

#[derive(Clone)]
struct FakeStore {
    state: Arc<Mutex<FakeStoreState>>,
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[derive(Clone)]
struct FakeStoreState {
    resumed: Option<ExecutorSubmissionLease>,
    claimed: Option<ExecutorSubmissionLease>,
    heartbeat_failure: Option<HeartbeatFailure>,
    heartbeat_calls: usize,
    recorded: Vec<ExecutorSubmissionOutcome>,
}

#[derive(Clone, Copy)]
struct HeartbeatFailure {
    call: usize,
    error: HeartbeatError,
}

#[derive(Clone, Copy)]
enum HeartbeatError {
    Stale,
    Unavailable,
}

impl FakeStore {
    fn running(lease: ExecutorSubmissionLease) -> Self {
        Self::new(Some(lease), None, None)
    }

    fn prepared(lease: ExecutorSubmissionLease) -> Self {
        Self::new(None, Some(lease), None)
    }

    fn with_heartbeat_failure(
        lease: ExecutorSubmissionLease,
        call: usize,
        error: HeartbeatError,
    ) -> Self {
        Self::new(None, Some(lease), Some(HeartbeatFailure { call, error }))
    }

    fn new(
        resumed: Option<ExecutorSubmissionLease>,
        claimed: Option<ExecutorSubmissionLease>,
        heartbeat_failure: Option<HeartbeatFailure>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeStoreState {
                resumed,
                claimed,
                heartbeat_failure,
                heartbeat_calls: 0,
                recorded: Vec::new(),
            })),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn snapshot(&self) -> FakeStoreState {
        self.state.lock().expect("fake store lock").clone()
    }

    fn runner(&self, delay: Duration) -> FakeRunner {
        FakeRunner {
            calls: Arc::new(Mutex::new(Vec::new())),
            events: self.events.clone(),
            outcome: succeeded_outcome(),
            delay,
        }
    }

    fn events(&self) -> Vec<&'static str> {
        self.events.lock().expect("fake event lock").clone()
    }

    fn push_event(&self, event: &'static str) {
        self.events.lock().expect("fake event lock").push(event);
    }
}

#[async_trait]
impl ExecutorSubmissionStore for FakeStore {
    async fn prepare_for_lease(
        &self,
        _lease: &gpt_image_2_gateway::admission::WorkLease,
    ) -> Result<Vec<PreparedExecutorSubmission>, ExecutorSubmissionError> {
        unreachable!("daemon does not prepare submissions")
    }

    async fn resume_running(
        &self,
        _scope: &ExecutorClaimScope,
        _owner: &str,
    ) -> Result<Option<ExecutorSubmissionLease>, ExecutorSubmissionError> {
        self.push_event("resume");
        Ok(self.snapshot().resumed)
    }

    async fn claim_prepared(
        &self,
        _scope: &ExecutorClaimScope,
        _owner: &str,
        _lease_ms: i64,
    ) -> Result<Option<ExecutorSubmissionLease>, ExecutorSubmissionError> {
        self.push_event("claim");
        Ok(self.snapshot().claimed)
    }

    async fn start(&self, _lease: &ExecutorSubmissionLease) -> Result<(), ExecutorSubmissionError> {
        self.push_event("start");
        Ok(())
    }

    async fn heartbeat(
        &self,
        _lease: &ExecutorSubmissionLease,
        _lease_ms: i64,
    ) -> Result<ExecutorSubmissionLease, ExecutorSubmissionError> {
        self.push_event("heartbeat");
        let failure = {
            let mut state = self.state.lock().expect("fake store lock");
            state.heartbeat_calls += 1;
            state
                .heartbeat_failure
                .filter(|failure| failure.call == state.heartbeat_calls)
        };
        match failure.map(|failure| failure.error) {
            Some(HeartbeatError::Stale) => Err(ExecutorSubmissionError::StaleLease),
            Some(HeartbeatError::Unavailable) => Err(ExecutorSubmissionError::Unavailable),
            None => Ok(_lease.clone()),
        }
    }

    async fn record_outcome(
        &self,
        _lease: &ExecutorSubmissionLease,
        outcome: &ExecutorSubmissionOutcome,
    ) -> Result<(), ExecutorSubmissionError> {
        self.push_event("record");
        let mut state = self.state.lock().expect("fake store lock");
        state.recorded = state
            .recorded
            .iter()
            .cloned()
            .chain(std::iter::once(outcome.clone()))
            .collect();
        Ok(())
    }

    async fn reconcile_expired(&self, _limit: u32) -> Result<u64, ExecutorSubmissionError> {
        unreachable!("daemon does not reconcile")
    }
}

#[derive(Clone)]
struct FakeRunner {
    calls: Arc<Mutex<Vec<ExecutorSubmissionLease>>>,
    events: Arc<Mutex<Vec<&'static str>>>,
    outcome: RunnerOutcome,
    delay: Duration,
}

#[async_trait]
impl DurableRunner for FakeRunner {
    async fn start_or_attach(&self, lease: ExecutorSubmissionLease) -> RunnerOutcome {
        self.events.lock().expect("fake event lock").push("runner");
        {
            let mut calls = self.calls.lock().expect("fake runner lock");
            *calls = calls
                .iter()
                .cloned()
                .chain(std::iter::once(lease))
                .collect();
        }
        tokio::time::sleep(self.delay).await;
        self.outcome.clone()
    }
}

#[tokio::test]
async fn restart_resumes_running_before_claim_and_attaches_once() {
    let lease = executor_lease();
    let store = FakeStore::running(lease.clone());
    let runner = store.runner(Duration::ZERO);
    let daemon = daemon(store.clone(), runner.clone());

    let result = daemon.run_once().await.expect("daemon run");

    assert_eq!(result, ExecutorDaemonRun::Recorded);
    assert_eq!(
        store.events(),
        vec!["resume", "heartbeat", "runner", "heartbeat", "record"]
    );
    assert_eq!(
        runner.calls.lock().expect("runner calls").as_slice(),
        &[lease]
    );
}

#[tokio::test]
async fn prepared_submission_is_claimed_started_and_recorded() {
    let lease = executor_lease();
    let store = FakeStore::prepared(lease.clone());
    let runner = store.runner(Duration::ZERO);
    let daemon = daemon(store.clone(), runner.clone());

    let result = daemon.run_once().await.expect("daemon run");

    assert_eq!(result, ExecutorDaemonRun::Recorded);
    assert_eq!(
        store.events(),
        vec![
            "resume",
            "claim",
            "start",
            "heartbeat",
            "runner",
            "heartbeat",
            "record"
        ]
    );
    assert_eq!(
        runner.calls.lock().expect("runner calls").as_slice(),
        &[lease]
    );
}

#[tokio::test]
async fn prelaunch_heartbeat_failure_never_calls_runner_or_records() {
    for (heartbeat_error, expected) in [
        (HeartbeatError::Stale, ExecutorSubmissionError::StaleLease),
        (
            HeartbeatError::Unavailable,
            ExecutorSubmissionError::Unavailable,
        ),
    ] {
        let lease = executor_lease();
        let store = FakeStore::with_heartbeat_failure(lease, 1, heartbeat_error);
        let runner = store.runner(Duration::ZERO);
        let daemon = daemon(store.clone(), runner.clone());

        let error = daemon
            .run_once()
            .await
            .expect_err("prelaunch heartbeat failure must fail run");

        assert_eq!(error.store_error(), Some(&expected));
        assert_eq!(
            store.events(),
            vec!["resume", "claim", "start", "heartbeat"]
        );
        assert!(runner.calls.lock().expect("runner calls").is_empty());
        assert!(store.snapshot().recorded.is_empty());
    }
}

#[tokio::test]
async fn in_flight_heartbeat_loss_never_records_runner_outcome() {
    let lease = executor_lease();
    let store = FakeStore::with_heartbeat_failure(lease, 2, HeartbeatError::Stale);
    let runner = store.runner(Duration::from_millis(50));
    let daemon = daemon(store.clone(), runner.clone());

    let error = daemon
        .run_once()
        .await
        .expect_err("lease loss must fail run");

    assert_eq!(
        error.store_error(),
        Some(&ExecutorSubmissionError::StaleLease)
    );
    let snapshot = store.snapshot();
    assert_eq!(
        store.events(),
        vec![
            "resume",
            "claim",
            "start",
            "heartbeat",
            "runner",
            "heartbeat"
        ]
    );
    assert_eq!(runner.calls.lock().expect("runner calls").len(), 1);
    assert!(snapshot.recorded.is_empty());
}

#[tokio::test]
async fn unsafe_heartbeat_intervals_fail_before_store_or_runner() {
    for (lease_ms, interval) in [
        (1_000, Duration::ZERO),
        (1_000, Duration::from_nanos(1)),
        (1_000, Duration::from_millis(334)),
        (i64::MAX, Duration::from_secs(u64::MAX)),
    ] {
        let store = FakeStore::prepared(executor_lease());
        let runner = store.runner(Duration::ZERO);
        let daemon = daemon_with_timing(store.clone(), runner.clone(), lease_ms, interval);

        assert_eq!(
            daemon.run_once().await,
            Err(ExecutorDaemonError::InvalidConfiguration)
        );
        assert!(store.events().is_empty());
        assert!(runner.calls.lock().expect("runner calls").is_empty());
    }
}

#[test]
fn runner_error_mapping_is_fail_closed() {
    assert_eq!(
        RunnerOutcome::from_error(RunnerError::Definite {
            error_code: "request_rejected".to_string(),
        }),
        RunnerOutcome::Failed {
            error_code: "request_rejected".to_string(),
        }
    );
    for (error, error_code) in [
        (RunnerError::Internal, "runner_internal"),
        (RunnerError::Unavailable, "runner_unavailable"),
        (
            RunnerError::Unknown {
                error_code: "provider_mystery".to_string(),
            },
            "provider_mystery",
        ),
    ] {
        assert_eq!(
            RunnerOutcome::from_error(error),
            RunnerOutcome::Uncertain {
                error_code: error_code.to_string(),
            }
        );
    }
}

fn daemon(store: FakeStore, runner: FakeRunner) -> ExecutorDaemon<FakeStore, FakeRunner> {
    daemon_with_timing(store, runner, 60_000, Duration::from_millis(5))
}

fn daemon_with_timing(
    store: FakeStore,
    runner: FakeRunner,
    lease_ms: i64,
    heartbeat_interval: Duration,
) -> ExecutorDaemon<FakeStore, FakeRunner> {
    ExecutorDaemon::new(
        store,
        runner,
        claim_scope(),
        "stable-executor".to_string(),
        lease_ms,
        heartbeat_interval,
    )
}

fn succeeded_outcome() -> RunnerOutcome {
    RunnerOutcome::Succeeded(ExecutorResultManifest {
        manifest_id: Uuid::new_v4(),
        storage_backend: "filesystem-v1".to_string(),
        object_key: "executor/result".to_string(),
        sha256_hex: "a".repeat(64),
        byte_size: 128,
        media_type: "image/png".to_string(),
    })
}

fn claim_scope() -> ExecutorClaimScope {
    ExecutorClaimScope {
        provider_id: "provider-test".to_string(),
        command_schema: "provider-command-v1".to_string(),
    }
}

fn executor_lease() -> ExecutorSubmissionLease {
    ExecutorSubmissionLease {
        submission_id: Uuid::new_v4(),
        executor_execution_id: Uuid::new_v4(),
        output_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        tenant_id: "tenant-test".to_string(),
        provider_id: "provider-test".to_string(),
        model: "model-test".to_string(),
        work_item_id: Uuid::new_v4(),
        output_index: 0,
        command_schema: "provider-command-v1".to_string(),
        command_hash: "a".repeat(64),
        executor_owner: "stable-executor".to_string(),
        executor_lease_epoch: 7,
        executor_lease_expires_at_ms: i64::MAX,
    }
}
