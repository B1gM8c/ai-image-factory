use std::{
    future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::{Semaphore, oneshot};

use super::*;

#[derive(Debug, thiserror::Error)]
#[error("test iteration error")]
struct TestIterationError;

struct IdleIteration {
    calls: AtomicUsize,
}

impl ProviderPollIteration for IdleIteration {
    type Error = TestIterationError;

    async fn run_once(&self) -> Result<ProviderPollRun, Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ProviderPollRun::Idle)
    }
}

struct BlockingIteration {
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

impl ProviderPollIteration for BlockingIteration {
    type Error = TestIterationError;

    async fn run_once(&self) -> Result<ProviderPollRun, Self::Error> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(current, Ordering::SeqCst);
        let _guard = InFlightGuard(&self.in_flight);
        self.started.add_permits(1);
        self.release
            .acquire()
            .await
            .expect("blocking iteration release")
            .forget();
        Ok(ProviderPollRun::Idle)
    }
}

struct InFlightGuard<'a>(&'a AtomicUsize);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct RecoveringIteration {
    calls: AtomicUsize,
    second_call: Arc<Semaphore>,
}

impl ProviderPollIteration for RecoveringIteration {
    type Error = TestIterationError;

    async fn run_once(&self) -> Result<ProviderPollRun, Self::Error> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            Err(TestIterationError)
        } else {
            self.second_call.add_permits(1);
            Ok(ProviderPollRun::Idle)
        }
    }
}

struct PendingIteration {
    started: Arc<Semaphore>,
    dropped: Arc<AtomicBool>,
}

impl ProviderPollIteration for PendingIteration {
    type Error = TestIterationError;

    async fn run_once(&self) -> Result<ProviderPollRun, Self::Error> {
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

struct PanicIteration;

impl ProviderPollIteration for PanicIteration {
    type Error = TestIterationError;

    async fn run_once(&self) -> Result<ProviderPollRun, Self::Error> {
        panic!("intentional provider poll lane panic")
    }
}

#[test]
fn invalid_configuration_is_rejected() {
    let iteration = Arc::new(IdleIteration {
        calls: AtomicUsize::new(0),
    });
    for invalid in [
        ProviderPollDaemonConfig {
            max_in_flight: 0,
            ..config()
        },
        ProviderPollDaemonConfig {
            max_in_flight: MAX_LANES + 1,
            ..config()
        },
        ProviderPollDaemonConfig {
            idle_delay: Duration::ZERO,
            ..config()
        },
        ProviderPollDaemonConfig {
            error_base_delay: Duration::from_secs(2),
            error_max_delay: Duration::from_secs(1),
            ..config()
        },
    ] {
        assert!(matches!(
            ProviderPollDaemon::new(Arc::clone(&iteration), invalid),
            Err(ProviderPollDaemonError::InvalidConfiguration)
        ));
    }
}

#[test]
fn jitter_is_bounded_and_reproducible() {
    let seed = [7_u8; 16];
    let idle = idle_jitter(Duration::from_millis(100), seed, 3, 4);
    assert!((Duration::from_millis(50)..=Duration::from_millis(100)).contains(&idle));
    assert_eq!(idle, idle_jitter(Duration::from_millis(100), seed, 3, 4));

    let first = error_jitter(
        Duration::from_millis(10),
        Duration::from_millis(25),
        1,
        seed,
        3,
        5,
    );
    let capped = error_jitter(
        Duration::from_millis(10),
        Duration::from_millis(25),
        20,
        seed,
        3,
        6,
    );
    assert!((Duration::from_nanos(1)..=Duration::from_millis(10)).contains(&first));
    assert!((Duration::from_nanos(1)..=Duration::from_millis(25)).contains(&capped));
}

#[tokio::test]
async fn idle_iterations_are_paced_instead_of_spinning() {
    let iteration = Arc::new(IdleIteration {
        calls: AtomicUsize::new(0),
    });
    let daemon = ProviderPollDaemon::with_jitter_seed(
        Arc::clone(&iteration),
        ProviderPollDaemonConfig {
            max_in_flight: 1,
            idle_delay: Duration::from_millis(20),
            ..config()
        },
        [1_u8; 16],
    )
    .unwrap();

    let report = daemon
        .run_until_shutdown(tokio::time::sleep(Duration::from_millis(45)))
        .await
        .unwrap();

    let calls = iteration.calls.load(Ordering::SeqCst);
    assert!(
        (2..=5).contains(&calls),
        "unexpected idle call count: {calls}"
    );
    assert_eq!(report.idle as usize, calls);
}

#[tokio::test]
async fn lanes_enforce_the_configured_in_flight_limit() {
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let iteration = Arc::new(BlockingIteration {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        in_flight: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let daemon = ProviderPollDaemon::new(
        Arc::clone(&iteration),
        ProviderPollDaemonConfig {
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
        .expect("all poll lanes started")
        .forget();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(iteration.peak.load(Ordering::SeqCst), 3);
    assert_eq!(iteration.in_flight.load(Ordering::SeqCst), 3);

    shutdown_tx.send(()).expect("send shutdown");
    release.add_permits(3);
    let report = run.await.expect("daemon task").unwrap();
    assert_eq!(report.idle, 3);
    assert_eq!(iteration.in_flight.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn transient_iteration_errors_back_off_and_recover() {
    let second_call = Arc::new(Semaphore::new(0));
    let iteration = Arc::new(RecoveringIteration {
        calls: AtomicUsize::new(0),
        second_call: Arc::clone(&second_call),
    });
    let daemon = ProviderPollDaemon::with_jitter_seed(
        iteration,
        ProviderPollDaemonConfig {
            max_in_flight: 1,
            error_base_delay: Duration::from_millis(2),
            error_max_delay: Duration::from_millis(4),
            ..config()
        },
        [2_u8; 16],
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

    second_call
        .acquire()
        .await
        .expect("iteration recovered")
        .forget();
    shutdown_tx.send(()).expect("send shutdown");

    let report = run.await.expect("daemon task").unwrap();
    assert_eq!(report.errors, 1);
    assert!(report.idle >= 1);
}

#[tokio::test]
async fn shutdown_drains_in_flight_iterations() {
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let iteration = Arc::new(BlockingIteration {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        in_flight: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let daemon = ProviderPollDaemon::new(iteration, config()).unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut run = tokio::spawn(async move {
        daemon
            .run_until_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    started.acquire().await.expect("poll started").forget();

    shutdown_tx.send(()).expect("send shutdown");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut run)
            .await
            .is_err()
    );
    release.add_permits(1);

    assert_eq!(
        run.await.expect("daemon task").unwrap(),
        ProviderPollDaemonReport {
            observed: 0,
            idle: 1,
            errors: 0,
        }
    );
}

#[tokio::test]
async fn shutdown_timeout_aborts_and_drops_pending_iteration() {
    let started = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let iteration = Arc::new(PendingIteration {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    });
    let daemon = ProviderPollDaemon::new(
        iteration,
        ProviderPollDaemonConfig {
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
    started.acquire().await.expect("poll started").forget();

    shutdown_tx.send(()).expect("send shutdown");
    assert_eq!(
        run.await.expect("daemon task"),
        Err(ProviderPollDaemonError::ShutdownDrainTimedOut)
    );
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn lane_panic_fails_closed_and_stops_the_daemon() {
    let daemon = ProviderPollDaemon::new(Arc::new(PanicIteration), config()).unwrap();

    assert_eq!(
        daemon.run_until_shutdown(future::pending()).await,
        Err(ProviderPollDaemonError::LaneTerminated)
    );
}

fn config() -> ProviderPollDaemonConfig {
    ProviderPollDaemonConfig {
        max_in_flight: 1,
        idle_delay: Duration::from_millis(10),
        error_base_delay: Duration::from_millis(5),
        error_max_delay: Duration::from_millis(50),
        shutdown_drain_timeout: Duration::from_secs(1),
    }
}
