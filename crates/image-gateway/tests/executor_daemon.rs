use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use gpt_image_2_gateway::executor::{
    DurableRunner, DurableRunnerResult, ExecutorClaimScope, ExecutorDaemon, ExecutorDaemonError,
    ExecutorDaemonRun, ExecutorSubmissionError, ExecutorSubmissionLease, ExecutorSubmissionOutcome,
    ExecutorSubmissionStore, RunnerError, RunnerLaunchAuthority, RunnerOutcome,
};
use tokio::sync::Barrier;
use uuid::Uuid;

#[derive(Clone)]
struct FakeStore {
    state: Arc<Mutex<FakeStoreState>>,
    events: Arc<Mutex<Vec<&'static str>>>,
    heartbeat_barrier: Option<(usize, Arc<Barrier>)>,
}

#[derive(Clone)]
struct FakeStoreState {
    resumed: Option<ExecutorSubmissionLease>,
    claimed: Option<ExecutorSubmissionLease>,
    heartbeat_failure: Option<HeartbeatFailure>,
    heartbeat_calls: usize,
    observed: Vec<ExecutorSubmissionOutcome>,
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
                observed: Vec::new(),
                recorded: Vec::new(),
            })),
            events: Arc::new(Mutex::new(Vec::new())),
            heartbeat_barrier: None,
        }
    }

    fn with_simultaneous_heartbeat_failure(
        lease: ExecutorSubmissionLease,
        call: usize,
        error: HeartbeatError,
        barrier: Arc<Barrier>,
    ) -> Self {
        let mut store = Self::with_heartbeat_failure(lease, call, error);
        store.heartbeat_barrier = Some((call, barrier));
        store
    }

    fn snapshot(&self) -> FakeStoreState {
        self.state.lock().expect("fake store lock").clone()
    }

    fn runner(&self, delay: Duration) -> FakeRunner {
        FakeRunner {
            calls: Arc::new(Mutex::new(Vec::new())),
            events: self.events.clone(),
            outcome: definite_outcome().into(),
            delay,
            ready_barrier: None,
        }
    }

    fn runner_with_barrier(&self, barrier: Arc<Barrier>) -> FakeRunner {
        FakeRunner {
            calls: Arc::new(Mutex::new(Vec::new())),
            events: self.events.clone(),
            outcome: definite_outcome().into(),
            delay: Duration::ZERO,
            ready_barrier: Some(barrier),
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
        let (failure, call) = {
            let mut state = self.state.lock().expect("fake store lock");
            state.heartbeat_calls += 1;
            (
                state
                    .heartbeat_failure
                    .filter(|failure| failure.call == state.heartbeat_calls),
                state.heartbeat_calls,
            )
        };
        if let Some((barrier_call, barrier)) = &self.heartbeat_barrier
            && *barrier_call == call
        {
            barrier.wait().await;
        }
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
        state.observed = state
            .observed
            .iter()
            .cloned()
            .chain(std::iter::once(outcome.clone()))
            .collect();
        state.recorded.push(outcome.clone());
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
    outcome: DurableRunnerResult,
    delay: Duration,
    ready_barrier: Option<Arc<Barrier>>,
}

#[async_trait]
impl DurableRunner for FakeRunner {
    async fn start_or_attach(
        &self,
        lease: ExecutorSubmissionLease,
        _authority: RunnerLaunchAuthority,
    ) -> DurableRunnerResult {
        self.events.lock().expect("fake event lock").push("runner");
        {
            let mut calls = self.calls.lock().expect("fake runner lock");
            *calls = calls
                .iter()
                .cloned()
                .chain(std::iter::once(lease))
                .collect();
        }
        if let Some(barrier) = &self.ready_barrier {
            barrier.wait().await;
        } else {
            tokio::time::sleep(self.delay).await;
        }
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
    assert_eq!(store.events(), vec!["resume", "runner", "record"]);
    assert_eq!(
        runner.calls.lock().expect("runner calls").as_slice(),
        &[lease]
    );
}

#[tokio::test]
async fn resumed_retryable_runner_neither_renews_nor_records() {
    let lease = executor_lease();
    let store = FakeStore::running(lease);
    let mut runner = store.runner(Duration::ZERO);
    runner.outcome = DurableRunnerResult::Retryable {
        error_code: "runner_launch_evidence_missing".to_string(),
    };
    let daemon = daemon(store.clone(), runner);

    assert_eq!(
        daemon.run_once().await,
        Err(ExecutorDaemonError::RunnerRetryable {
            error_code: "runner_launch_evidence_missing".to_string(),
        })
    );
    assert_eq!(store.events(), vec!["resume", "runner"]);
    let snapshot = store.snapshot();
    assert_eq!(snapshot.heartbeat_calls, 0);
    assert!(snapshot.observed.is_empty());
    assert!(snapshot.recorded.is_empty());
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
        vec!["resume", "claim", "start", "heartbeat", "runner", "record"]
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
async fn terminal_recording_does_not_require_a_post_runner_heartbeat() {
    let lease = executor_lease();
    let store = FakeStore::with_heartbeat_failure(lease, 2, HeartbeatError::Stale);
    let runner = store.runner(Duration::ZERO);
    let daemon = daemon(store.clone(), runner.clone());

    assert_eq!(daemon.run_once().await, Ok(ExecutorDaemonRun::Recorded));
    let snapshot = store.snapshot();
    assert_eq!(
        store.events(),
        vec!["resume", "claim", "start", "heartbeat", "runner", "record"]
    );
    assert_eq!(runner.calls.lock().expect("runner calls").len(), 1);
    assert_eq!(
        snapshot.observed,
        vec![ExecutorSubmissionOutcome::from(definite_outcome())]
    );
    assert_eq!(snapshot.recorded, snapshot.observed);
}

#[tokio::test]
async fn simultaneous_runner_and_failed_renewal_records_runner_outcome() {
    let barrier = Arc::new(Barrier::new(2));
    let store = FakeStore::with_simultaneous_heartbeat_failure(
        executor_lease(),
        2,
        HeartbeatError::Stale,
        barrier.clone(),
    );
    let runner = store.runner_with_barrier(barrier);
    let daemon = daemon_with_timing(store.clone(), runner, 60_000, Duration::from_millis(1));

    assert_eq!(daemon.run_once().await, Ok(ExecutorDaemonRun::Recorded));
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
        store.snapshot().recorded,
        vec![ExecutorSubmissionOutcome::from(definite_outcome())]
    );
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

fn definite_outcome() -> RunnerOutcome {
    RunnerOutcome::Failed {
        error_code: "provider_rejected".to_string(),
    }
}

fn claim_scope() -> ExecutorClaimScope {
    ExecutorClaimScope {
        execution_profile_id: Uuid::from_u128(1),
        provider_id: "provider-test".to_string(),
        command_schema: "provider-command-v1".to_string(),
        adapter_revision: "provider-adapter-v1".to_string(),
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
        execution_profile_id: Uuid::from_u128(1),
        adapter_revision: "provider-adapter-v1".to_string(),
        executor_owner: "stable-executor".to_string(),
        executor_lease_epoch: 7,
        executor_lease_expires_at_ms: i64::MAX,
    }
}
