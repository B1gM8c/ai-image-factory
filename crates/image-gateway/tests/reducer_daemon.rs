use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use gpt_image_2_gateway::{
    artifacts::{ArtifactIdentity, ArtifactMetadata, FILESYSTEM_BACKEND},
    reduction::{
        CanonicalExecutorOutcome, CustomerArtifactPublishError, ExecutorParentTerminalState,
        ExecutorTerminalArtifact, ExecutorTerminalCompletion, ExecutorTerminalError,
        ExecutorTerminalLease, ExecutorTerminalStore, ReducerDaemon, ReducerDaemonError,
        ReducerDaemonRun, TerminalArtifactPublisher,
    },
};
use tokio::sync::{Semaphore, oneshot};
use uuid::Uuid;

#[derive(Clone)]
struct FakeStore {
    state: Arc<Mutex<FakeStoreState>>,
    events: Arc<Mutex<Vec<&'static str>>>,
    completed: Arc<Semaphore>,
}

#[derive(Clone)]
struct FakeStoreState {
    claimed: Option<ExecutorTerminalLease>,
    claim_error_once: Option<ExecutorTerminalError>,
    heartbeat_error: Option<ExecutorTerminalError>,
    heartbeat_calls: usize,
    completions: Vec<Option<ArtifactMetadata>>,
}

impl FakeStore {
    fn new(lease: ExecutorTerminalLease) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeStoreState {
                claimed: Some(lease),
                claim_error_once: None,
                heartbeat_error: None,
                heartbeat_calls: 0,
                completions: Vec::new(),
            })),
            events: Arc::new(Mutex::new(Vec::new())),
            completed: Arc::new(Semaphore::new(0)),
        }
    }

    fn with_claim_error_once(lease: ExecutorTerminalLease, error: ExecutorTerminalError) -> Self {
        let store = Self::new(lease);
        store
            .state
            .lock()
            .expect("fake store lock")
            .claim_error_once = Some(error);
        store
    }

    fn with_heartbeat_error(lease: ExecutorTerminalLease, error: ExecutorTerminalError) -> Self {
        let store = Self::new(lease);
        store.state.lock().expect("fake store lock").heartbeat_error = Some(error);
        store
    }

    fn snapshot(&self) -> FakeStoreState {
        self.state.lock().expect("fake store lock").clone()
    }

    fn events(&self) -> Vec<&'static str> {
        self.events.lock().expect("fake event lock").clone()
    }

    fn push_event(&self, event: &'static str) {
        self.events.lock().expect("fake event lock").push(event);
    }
}

#[async_trait]
impl ExecutorTerminalStore for FakeStore {
    async fn claim_terminal(
        &self,
        _owner: &str,
        _lease_ms: i64,
    ) -> Result<Option<ExecutorTerminalLease>, ExecutorTerminalError> {
        self.push_event("claim");
        let mut state = self.state.lock().expect("fake store lock");
        if let Some(error) = state.claim_error_once.take() {
            return Err(error);
        }
        Ok(state.claimed.take())
    }

    async fn heartbeat_terminal(
        &self,
        lease: &ExecutorTerminalLease,
        lease_ms: i64,
    ) -> Result<ExecutorTerminalLease, ExecutorTerminalError> {
        self.push_event("heartbeat");
        let mut state = self.state.lock().expect("fake store lock");
        state.heartbeat_calls += 1;
        if let Some(error) = state.heartbeat_error {
            return Err(error);
        }
        Ok(ExecutorTerminalLease {
            reducer_lease_expires_at_ms: lease.reducer_lease_expires_at_ms + lease_ms,
            ..lease.clone()
        })
    }

    async fn complete_terminal(
        &self,
        _lease: &ExecutorTerminalLease,
        customer_artifact: Option<&ArtifactMetadata>,
    ) -> Result<ExecutorTerminalCompletion, ExecutorTerminalError> {
        self.push_event("complete");
        self.state
            .lock()
            .expect("fake store lock")
            .completions
            .push(customer_artifact.cloned());
        self.completed.add_permits(1);
        Ok(ExecutorTerminalCompletion {
            receipt_id: Uuid::from_u128(90),
            customer_artifact_id: customer_artifact.map(|artifact| artifact.identity.artifact_id),
            parent_state: ExecutorParentTerminalState::Succeeded,
        })
    }
}

#[derive(Clone)]
struct FakePublisher {
    artifact: ArtifactMetadata,
    events: Arc<Mutex<Vec<&'static str>>>,
    calls: Arc<Mutex<Vec<ExecutorTerminalLease>>>,
    started: Arc<Semaphore>,
    release: Option<Arc<Semaphore>>,
    canceled: Arc<AtomicUsize>,
}

impl FakePublisher {
    fn immediate(store: &FakeStore, artifact: ArtifactMetadata) -> Self {
        Self::new(store, artifact, None)
    }

    fn blocked(
        store: &FakeStore,
        artifact: ArtifactMetadata,
    ) -> (Self, Arc<Semaphore>, Arc<Semaphore>) {
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        (
            Self {
                artifact,
                events: store.events.clone(),
                calls: Arc::new(Mutex::new(Vec::new())),
                started: started.clone(),
                release: Some(release.clone()),
                canceled: Arc::new(AtomicUsize::new(0)),
            },
            started,
            release,
        )
    }

    fn new(store: &FakeStore, artifact: ArtifactMetadata, release: Option<Arc<Semaphore>>) -> Self {
        Self {
            artifact,
            events: store.events.clone(),
            calls: Arc::new(Mutex::new(Vec::new())),
            started: Arc::new(Semaphore::new(0)),
            release,
            canceled: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.lock().expect("fake publisher lock").len()
    }

    fn cancellation_count(&self) -> usize {
        self.canceled.load(Ordering::SeqCst)
    }
}

struct PublishCancellationGuard {
    canceled: Arc<AtomicUsize>,
    completed: bool,
}

impl Drop for PublishCancellationGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.canceled.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl TerminalArtifactPublisher for FakePublisher {
    async fn publish(
        &self,
        lease: &ExecutorTerminalLease,
    ) -> Result<ArtifactMetadata, CustomerArtifactPublishError> {
        let mut cancellation = PublishCancellationGuard {
            canceled: self.canceled.clone(),
            completed: false,
        };
        self.events.lock().expect("fake event lock").push("publish");
        self.calls
            .lock()
            .expect("fake publisher lock")
            .push(lease.clone());
        self.started.add_permits(1);
        if let Some(release) = &self.release {
            let permit = release
                .acquire()
                .await
                .expect("fake publisher release semaphore");
            permit.forget();
        }
        cancellation.completed = true;
        Ok(self.artifact.clone())
    }
}

#[tokio::test]
async fn success_publishes_customer_artifact_before_completion() {
    let lease = terminal_lease(CanonicalExecutorOutcome::Succeeded(terminal_artifact()));
    let artifact = customer_artifact(&lease);
    let store = FakeStore::new(lease);
    let publisher = FakePublisher::immediate(&store, artifact.clone());
    let daemon = daemon(store.clone(), publisher.clone());

    assert_eq!(daemon.run_once().await, Ok(ReducerDaemonRun::Completed));
    assert_eq!(store.events(), vec!["claim", "publish", "complete"]);
    assert_eq!(publisher.call_count(), 1);
    assert_eq!(store.snapshot().completions, vec![Some(artifact)]);
}

#[tokio::test]
async fn failure_completes_without_publishing_an_artifact() {
    let lease = terminal_lease(CanonicalExecutorOutcome::Failed {
        error_code: "provider_rejected".to_string(),
    });
    let store = FakeStore::new(lease.clone());
    let publisher = FakePublisher::immediate(&store, customer_artifact(&lease));
    let daemon = daemon(store.clone(), publisher.clone());

    assert_eq!(daemon.run_once().await, Ok(ReducerDaemonRun::Completed));
    assert_eq!(store.events(), vec!["claim", "complete"]);
    assert_eq!(publisher.call_count(), 0);
    assert_eq!(store.snapshot().completions, vec![None]);
}

#[tokio::test]
async fn heartbeat_lease_loss_cancels_publication_and_never_completes() {
    let lease = terminal_lease(CanonicalExecutorOutcome::Succeeded(terminal_artifact()));
    let artifact = customer_artifact(&lease);
    let store = FakeStore::with_heartbeat_error(lease, ExecutorTerminalError::StaleLease);
    let (publisher, started, _release) = FakePublisher::blocked(&store, artifact);
    let publisher_probe = publisher.clone();
    let daemon = daemon_with_timing(store.clone(), publisher, 60_000, Duration::from_millis(5));
    let run = tokio::spawn(async move { daemon.run_once().await });
    started.acquire().await.expect("publisher started").forget();

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .expect("heartbeat result timeout")
            .expect("daemon task"),
        Err(ReducerDaemonError::Store(ExecutorTerminalError::StaleLease))
    );
    assert_eq!(store.events(), vec!["claim", "publish", "heartbeat"]);
    assert_eq!(publisher_probe.cancellation_count(), 1);
    assert!(store.snapshot().completions.is_empty());
}

#[tokio::test]
async fn transient_iteration_error_does_not_stop_polling() {
    let lease = terminal_lease(CanonicalExecutorOutcome::Succeeded(terminal_artifact()));
    let artifact = customer_artifact(&lease);
    let store = FakeStore::with_claim_error_once(lease, ExecutorTerminalError::Unavailable);
    let publisher = FakePublisher::immediate(&store, artifact.clone());
    let daemon = daemon(store.clone(), publisher.clone());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let run = tokio::spawn(async move {
        daemon
            .run_until_shutdown(
                async {
                    let _ = shutdown_rx.await;
                },
                Duration::from_millis(5),
                Duration::from_secs(1),
            )
            .await
    });

    store
        .completed
        .acquire()
        .await
        .expect("reduction completed after retry")
        .forget();
    shutdown_tx.send(()).expect("send shutdown");

    assert_eq!(run.await.expect("daemon task"), Ok(()));
    assert!(
        store
            .events()
            .starts_with(&["claim", "claim", "publish", "complete"])
    );
    assert_eq!(publisher.call_count(), 1);
    assert_eq!(store.snapshot().completions, vec![Some(artifact)]);
}

#[tokio::test]
async fn shutdown_drains_an_in_flight_reduction() {
    let lease = terminal_lease(CanonicalExecutorOutcome::Succeeded(terminal_artifact()));
    let artifact = customer_artifact(&lease);
    let store = FakeStore::new(lease);
    let (publisher, started, release) = FakePublisher::blocked(&store, artifact.clone());
    let daemon = daemon(store.clone(), publisher);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let run = tokio::spawn(async move {
        daemon
            .run_until_shutdown(
                async {
                    let _ = shutdown_rx.await;
                },
                Duration::from_millis(5),
                Duration::from_secs(1),
            )
            .await
    });
    started.acquire().await.expect("publisher started").forget();

    shutdown_tx.send(()).expect("send shutdown");
    tokio::task::yield_now().await;
    release.add_permits(1);

    assert_eq!(run.await.expect("daemon task"), Ok(()));
    assert_eq!(store.events(), vec!["claim", "publish", "complete"]);
    assert_eq!(store.snapshot().completions, vec![Some(artifact)]);
}

#[tokio::test]
async fn shutdown_drain_timeout_is_fail_closed() {
    let lease = terminal_lease(CanonicalExecutorOutcome::Succeeded(terminal_artifact()));
    let artifact = customer_artifact(&lease);
    let store = FakeStore::new(lease);
    let (publisher, started, _release) = FakePublisher::blocked(&store, artifact);
    let daemon = daemon(store.clone(), publisher);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let run = tokio::spawn(async move {
        daemon
            .run_until_shutdown(
                async {
                    let _ = shutdown_rx.await;
                },
                Duration::from_millis(5),
                Duration::from_millis(20),
            )
            .await
    });
    started.acquire().await.expect("publisher started").forget();

    shutdown_tx.send(()).expect("send shutdown");

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .expect("shutdown timeout result")
            .expect("daemon task"),
        Err(ReducerDaemonError::ShutdownDrainTimedOut)
    );
    let events = store.events();
    assert!(events.starts_with(&["claim", "publish"]));
    assert!(events[2..].iter().all(|event| *event == "heartbeat"));
    let snapshot = store.snapshot();
    assert!(snapshot.heartbeat_calls > 0);
    assert!(snapshot.completions.is_empty());
}

fn daemon(store: FakeStore, publisher: FakePublisher) -> ReducerDaemon<FakeStore, FakePublisher> {
    daemon_with_timing(store, publisher, 60_000, Duration::from_millis(10))
}

fn daemon_with_timing(
    store: FakeStore,
    publisher: FakePublisher,
    lease_ms: i64,
    heartbeat_interval: Duration,
) -> ReducerDaemon<FakeStore, FakePublisher> {
    ReducerDaemon::new(
        store,
        publisher,
        "stable-reducer".to_string(),
        lease_ms,
        heartbeat_interval,
    )
}

fn terminal_lease(outcome: CanonicalExecutorOutcome) -> ExecutorTerminalLease {
    ExecutorTerminalLease {
        submission_id: Uuid::from_u128(1),
        executor_execution_id: Uuid::from_u128(2),
        resolution_decision_id: Uuid::from_u128(3),
        output_id: Uuid::from_u128(4),
        output_index: 0,
        job_id: Uuid::from_u128(5),
        tenant_id: "tenant-test".to_string(),
        work_item_id: Uuid::from_u128(6),
        attempt_execution_id: Uuid::from_u128(7),
        attempt_lease_epoch: 8,
        reducer_owner: "stable-reducer".to_string(),
        reducer_lease_epoch: 9,
        reducer_lease_expires_at_ms: 100_000,
        outcome,
    }
}

fn terminal_artifact() -> ExecutorTerminalArtifact {
    ExecutorTerminalArtifact {
        authority_id: Uuid::from_u128(2),
        storage_backend: FILESYSTEM_BACKEND.to_string(),
        storage_namespace: "executor".to_string(),
        object_key: "executor-objects/00/source".to_string(),
        sha256_hex: "a".repeat(64),
        byte_size: 8,
        media_type: "image/png".to_string(),
    }
}

fn customer_artifact(lease: &ExecutorTerminalLease) -> ArtifactMetadata {
    ArtifactMetadata {
        identity: ArtifactIdentity {
            artifact_id: lease.output_id,
            tenant_id: lease.tenant_id.clone(),
            job_id: lease.job_id,
            work_item_id: lease.work_item_id,
            execution_id: lease.attempt_execution_id,
            lease_epoch: lease.attempt_lease_epoch,
            output_index: u32::try_from(lease.output_index).expect("output index"),
            media_type: "image/png".to_string(),
        },
        storage_backend: FILESYSTEM_BACKEND.to_string(),
        object_key: "objects/00/customer".to_string(),
        sha256_hex: "a".repeat(64),
        byte_size: 8,
    }
}
