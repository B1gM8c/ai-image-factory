use std::{
    future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::{Semaphore, oneshot};

use super::*;

#[derive(Debug, thiserror::Error)]
#[error("test submit iteration error")]
struct TestIterationError;

struct IdentityIteration {
    commands: Mutex<Vec<ProviderSubmitIterationCommand>>,
    completed: Semaphore,
}

impl ProviderSubmitIteration for IdentityIteration {
    type Error = TestIterationError;

    async fn run_once(
        &self,
        command: &ProviderSubmitIterationCommand,
    ) -> Result<ProviderSubmitRun, Self::Error> {
        let mut commands = self.commands.lock().unwrap();
        commands.push(command.clone());
        let call = commands.len();
        drop(commands);
        match call {
            1 => Err(TestIterationError),
            2 => Ok(ProviderSubmitRun::RecoveryCompleted),
            _ => {
                self.completed.add_permits(1);
                Ok(ProviderSubmitRun::Idle)
            }
        }
    }
}

struct BlockingIteration {
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

impl ProviderSubmitIteration for BlockingIteration {
    type Error = TestIterationError;

    async fn run_once(
        &self,
        _command: &ProviderSubmitIterationCommand,
    ) -> Result<ProviderSubmitRun, Self::Error> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(current, Ordering::SeqCst);
        let _guard = InFlightGuard(&self.in_flight);
        self.started.add_permits(1);
        self.release
            .acquire()
            .await
            .expect("blocking submit iteration release")
            .forget();
        Ok(ProviderSubmitRun::Idle)
    }
}

struct InFlightGuard<'a>(&'a AtomicUsize);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct PendingIteration {
    started: Arc<Semaphore>,
    dropped: Arc<AtomicBool>,
}

struct PanicIteration;

impl ProviderSubmitIteration for PanicIteration {
    type Error = TestIterationError;

    async fn run_once(
        &self,
        _command: &ProviderSubmitIterationCommand,
    ) -> Result<ProviderSubmitRun, Self::Error> {
        panic!("intentional provider submit lane panic")
    }
}

impl ProviderSubmitIteration for PendingIteration {
    type Error = TestIterationError;

    async fn run_once(
        &self,
        _command: &ProviderSubmitIterationCommand,
    ) -> Result<ProviderSubmitRun, Self::Error> {
        let _guard = DropFlag(Arc::clone(&self.dropped));
        self.started.add_permits(1);
        future::pending().await
    }
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn invalid_configuration_is_rejected() {
    let iteration = Arc::new(IdentityIteration {
        commands: Mutex::new(Vec::new()),
        completed: Semaphore::new(0),
    });
    for invalid in [
        ProviderSubmitDaemonConfig {
            max_in_flight: 0,
            ..config()
        },
        ProviderSubmitDaemonConfig {
            owner_prefix: "x".repeat(MAX_OWNER_PREFIX_BYTES + 1),
            ..config()
        },
        ProviderSubmitDaemonConfig {
            idle_delay: Duration::ZERO,
            ..config()
        },
        ProviderSubmitDaemonConfig {
            error_base_delay: Duration::from_secs(2),
            error_max_delay: Duration::from_secs(1),
            ..config()
        },
    ] {
        assert!(matches!(
            ProviderSubmitDaemon::new(Arc::clone(&iteration), invalid),
            Err(ProviderSubmitDaemonError::InvalidConfiguration)
        ));
    }
}

#[tokio::test]
async fn errors_replay_the_same_identity_and_success_advances_it() {
    let iteration = Arc::new(IdentityIteration {
        commands: Mutex::new(Vec::new()),
        completed: Semaphore::new(0),
    });
    let daemon = ProviderSubmitDaemon::with_jitter_seed(
        Arc::clone(&iteration),
        ProviderSubmitDaemonConfig {
            error_base_delay: Duration::from_millis(1),
            error_max_delay: Duration::from_millis(2),
            ..config()
        },
        [3_u8; 16],
    )
    .unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let run = tokio::spawn(async move {
        daemon
            .run_until_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    iteration
        .completed
        .acquire()
        .await
        .expect("third submit iteration")
        .forget();
    shutdown_tx.send(()).expect("send shutdown");
    let report = run.await.expect("daemon task").unwrap();
    let commands = iteration.commands.lock().unwrap();

    assert_eq!(commands.len(), 3);
    assert_eq!(commands[0], commands[1]);
    assert_ne!(commands[1], commands[2]);
    assert_eq!(report.errors, 1);
    assert_eq!(report.recovery_completed, 1);
}

#[tokio::test]
async fn lanes_bound_in_flight_iterations_and_shutdown_drains() {
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let iteration = Arc::new(BlockingIteration {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        in_flight: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let daemon = ProviderSubmitDaemon::new(
        Arc::clone(&iteration),
        ProviderSubmitDaemonConfig {
            max_in_flight: 3,
            ..config()
        },
    )
    .unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let run = tokio::spawn(async move {
        daemon
            .run_until_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    started
        .acquire_many(3)
        .await
        .expect("all submit lanes started")
        .forget();
    assert_eq!(iteration.peak.load(Ordering::SeqCst), 3);
    shutdown_tx.send(()).expect("send shutdown");
    release.add_permits(3);

    let report = run.await.expect("daemon task").unwrap();
    assert_eq!(report.idle, 3);
    assert_eq!(iteration.in_flight.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shutdown_timeout_aborts_pending_iteration() {
    let started = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let daemon = ProviderSubmitDaemon::new(
        Arc::new(PendingIteration {
            started: Arc::clone(&started),
            dropped: Arc::clone(&dropped),
        }),
        ProviderSubmitDaemonConfig {
            shutdown_drain_timeout: Duration::from_millis(20),
            ..config()
        },
    )
    .unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let run = tokio::spawn(async move {
        daemon
            .run_until_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    started.acquire().await.expect("submit started").forget();

    shutdown_tx.send(()).expect("send shutdown");
    assert_eq!(
        run.await.expect("daemon task"),
        Err(ProviderSubmitDaemonError::ShutdownDrainTimedOut)
    );
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn lane_panic_fails_closed_and_stops_the_daemon() {
    let daemon = ProviderSubmitDaemon::new(Arc::new(PanicIteration), config()).unwrap();

    assert_eq!(
        daemon.run_until_shutdown(future::pending()).await,
        Err(ProviderSubmitDaemonError::LaneTerminated)
    );
}

fn config() -> ProviderSubmitDaemonConfig {
    ProviderSubmitDaemonConfig {
        max_in_flight: 1,
        owner_prefix: "provider-submitd-test".to_owned(),
        idle_delay: Duration::from_millis(10),
        error_base_delay: Duration::from_millis(5),
        error_max_delay: Duration::from_millis(50),
        shutdown_drain_timeout: Duration::from_secs(1),
    }
}
