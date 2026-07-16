use std::{error::Error, future::Future, sync::Arc, time::Duration};

use sha2::{Digest, Sha256};
use tokio::{sync::watch, task::JoinSet};

use super::{
    ProviderArtifactStagerFactory, ProviderPollDriver, ProviderPollOrchestrator,
    ProviderPollOrchestratorError, ProviderPollRun, ProviderPollStore,
};

pub(crate) const MAX_PROVIDER_POLL_LANES: usize = 1_024;
const MAX_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

pub trait ProviderPollIteration: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    fn run_once(&self) -> impl Future<Output = Result<ProviderPollRun, Self::Error>> + Send;
}

impl<S, D, F> ProviderPollIteration for ProviderPollOrchestrator<S, D, F>
where
    S: ProviderPollStore,
    D: ProviderPollDriver,
    F: ProviderArtifactStagerFactory,
{
    type Error = ProviderPollOrchestratorError;

    async fn run_once(&self) -> Result<ProviderPollRun, Self::Error> {
        ProviderPollOrchestrator::run_once(self).await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderPollDaemonConfig {
    pub max_in_flight: usize,
    pub idle_delay: Duration,
    pub error_base_delay: Duration,
    pub error_max_delay: Duration,
    pub shutdown_drain_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProviderPollDaemonReport {
    pub observed: u64,
    pub idle: u64,
    pub errors: u64,
}

impl ProviderPollDaemonReport {
    fn merge(&mut self, other: Self) {
        self.observed = self.observed.saturating_add(other.observed);
        self.idle = self.idle.saturating_add(other.idle);
        self.errors = self.errors.saturating_add(other.errors);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderPollDaemonError {
    #[error("provider poll daemon configuration is invalid")]
    InvalidConfiguration,
    #[error("provider poll daemon lane terminated unexpectedly")]
    LaneTerminated,
    #[error("provider poll daemon shutdown drain timed out")]
    ShutdownDrainTimedOut,
}

pub struct ProviderPollDaemon<I> {
    iteration: Arc<I>,
    config: ProviderPollDaemonConfig,
    jitter_seed: [u8; 16],
}

impl<I> ProviderPollDaemon<I>
where
    I: ProviderPollIteration,
{
    pub fn new(
        iteration: Arc<I>,
        config: ProviderPollDaemonConfig,
    ) -> Result<Self, ProviderPollDaemonError> {
        Self::with_jitter_seed(iteration, config, *uuid::Uuid::new_v4().as_bytes())
    }

    fn with_jitter_seed(
        iteration: Arc<I>,
        config: ProviderPollDaemonConfig,
        jitter_seed: [u8; 16],
    ) -> Result<Self, ProviderPollDaemonError> {
        validate_config(config)?;
        Ok(Self {
            iteration,
            config,
            jitter_seed,
        })
    }

    pub async fn run_until_shutdown<S>(
        &self,
        shutdown: S,
    ) -> Result<ProviderPollDaemonReport, ProviderPollDaemonError>
    where
        S: Future<Output = ()>,
    {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut lanes = JoinSet::new();
        for lane in 0..self.config.max_in_flight {
            lanes.spawn(run_lane(
                Arc::clone(&self.iteration),
                self.config,
                self.jitter_seed,
                lane,
                shutdown_rx.clone(),
            ));
        }
        drop(shutdown_rx);
        tokio::pin!(shutdown);

        tokio::select! {
            biased;
            _ = &mut shutdown => {}
            result = lanes.join_next() => {
                trace_lane_termination(result);
                let _ = shutdown_tx.send(true);
                lanes.abort_all();
                drain_aborted_lanes(&mut lanes).await;
                return Err(ProviderPollDaemonError::LaneTerminated);
            }
        }

        let _ = shutdown_tx.send(true);
        match tokio::time::timeout(self.config.shutdown_drain_timeout, drain_lanes(&mut lanes))
            .await
        {
            Ok(Ok(report)) => Ok(report),
            Ok(Err(error)) => {
                lanes.abort_all();
                drain_aborted_lanes(&mut lanes).await;
                Err(error)
            }
            Err(_) => {
                lanes.abort_all();
                drain_aborted_lanes(&mut lanes).await;
                Err(ProviderPollDaemonError::ShutdownDrainTimedOut)
            }
        }
    }
}

async fn run_lane<I>(
    iteration: Arc<I>,
    config: ProviderPollDaemonConfig,
    jitter_seed: [u8; 16],
    lane: usize,
    mut shutdown: watch::Receiver<bool>,
) -> ProviderPollDaemonReport
where
    I: ProviderPollIteration,
{
    let mut report = ProviderPollDaemonReport::default();
    let mut consecutive_errors = 0_u32;
    let mut delay_sequence = 0_u64;

    loop {
        if *shutdown.borrow_and_update() {
            return report;
        }

        let delay = match iteration.run_once().await {
            Ok(ProviderPollRun::Observed(_)) => {
                report.observed = report.observed.saturating_add(1);
                consecutive_errors = 0;
                continue;
            }
            Ok(ProviderPollRun::Idle) => {
                report.idle = report.idle.saturating_add(1);
                consecutive_errors = 0;
                idle_jitter(config.idle_delay, jitter_seed, lane, delay_sequence)
            }
            Err(_) => {
                report.errors = report.errors.saturating_add(1);
                consecutive_errors = consecutive_errors.saturating_add(1);
                tracing::error!(
                    lane,
                    consecutive_errors,
                    error_type = std::any::type_name::<I::Error>(),
                    "provider poll iteration failed"
                );
                error_jitter(
                    config.error_base_delay,
                    config.error_max_delay,
                    consecutive_errors,
                    jitter_seed,
                    lane,
                    delay_sequence,
                )
            }
        };
        delay_sequence = delay_sequence.wrapping_add(1);
        if sleep_or_shutdown(delay, &mut shutdown).await {
            return report;
        }
    }
}

async fn sleep_or_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    tokio::select! {
        biased;
        changed = shutdown.changed() => {
            changed.is_err() || *shutdown.borrow()
        }
        _ = tokio::time::sleep(delay) => false,
    }
}

async fn drain_lanes(
    lanes: &mut JoinSet<ProviderPollDaemonReport>,
) -> Result<ProviderPollDaemonReport, ProviderPollDaemonError> {
    let mut report = ProviderPollDaemonReport::default();
    while let Some(result) = lanes.join_next().await {
        match result {
            Ok(lane_report) => report.merge(lane_report),
            Err(error) => {
                tracing::error!(
                    task.id = ?error.id(),
                    task.cancelled = error.is_cancelled(),
                    task.panicked = error.is_panic(),
                    "provider poll daemon lane failed while draining"
                );
                return Err(ProviderPollDaemonError::LaneTerminated);
            }
        }
    }
    Ok(report)
}

async fn drain_aborted_lanes(lanes: &mut JoinSet<ProviderPollDaemonReport>) {
    while lanes.join_next().await.is_some() {}
}

fn trace_lane_termination(
    result: Option<Result<ProviderPollDaemonReport, tokio::task::JoinError>>,
) {
    match result {
        Some(Ok(report)) => {
            tracing::error!(?report, "provider poll daemon lane exited before shutdown")
        }
        Some(Err(error)) => tracing::error!(
            task.id = ?error.id(),
            task.cancelled = error.is_cancelled(),
            task.panicked = error.is_panic(),
            "provider poll daemon lane failed"
        ),
        None => tracing::error!("provider poll daemon lost all lanes before shutdown"),
    }
}

fn validate_config(config: ProviderPollDaemonConfig) -> Result<(), ProviderPollDaemonError> {
    let delays = [
        config.idle_delay,
        config.error_base_delay,
        config.error_max_delay,
        config.shutdown_drain_timeout,
    ];
    if !(1..=MAX_PROVIDER_POLL_LANES).contains(&config.max_in_flight)
        || delays
            .iter()
            .any(|delay| delay.is_zero() || *delay > MAX_DELAY)
        || config.error_base_delay > config.error_max_delay
    {
        return Err(ProviderPollDaemonError::InvalidConfiguration);
    }
    Ok(())
}

fn idle_jitter(maximum: Duration, seed: [u8; 16], lane: usize, sequence: u64) -> Duration {
    let maximum = duration_nanos(maximum);
    let minimum = maximum.div_ceil(2);
    Duration::from_nanos(minimum + sample_below(seed, lane, sequence, maximum - minimum + 1))
}

fn error_jitter(
    base: Duration,
    maximum: Duration,
    consecutive_errors: u32,
    seed: [u8; 16],
    lane: usize,
    sequence: u64,
) -> Duration {
    let exponent = consecutive_errors.saturating_sub(1).min(63);
    let cap = base
        .as_nanos()
        .saturating_mul(1_u128 << exponent)
        .min(maximum.as_nanos());
    let cap = u64::try_from(cap).expect("validated daemon delay fits u64 nanoseconds");
    Duration::from_nanos(1 + sample_below(seed, lane, sequence, cap))
}

fn sample_below(seed: [u8; 16], lane: usize, sequence: u64, upper: u64) -> u64 {
    debug_assert!(upper > 0);
    let mut hasher = Sha256::new();
    hasher.update(seed);
    hasher.update(lane.to_le_bytes());
    hasher.update(sequence.to_le_bytes());
    let digest = hasher.finalize();
    u64::from_le_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    ) % upper
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("validated daemon delay fits u64 nanoseconds")
}

#[cfg(test)]
mod tests;
