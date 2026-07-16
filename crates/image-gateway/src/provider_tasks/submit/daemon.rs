use std::{future::Future, sync::Arc, time::Duration};

use sha2::{Digest, Sha256};
use tokio::{sync::watch, task::JoinSet};

use super::{ProviderSubmitIteration, ProviderSubmitIterationCommand, ProviderSubmitRun};

const MAX_PROVIDER_SUBMIT_LANES: usize = 1_024;
const MAX_DELAY: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_OWNER_PREFIX_BYTES: usize = 80;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSubmitDaemonConfig {
    pub max_in_flight: usize,
    pub owner_prefix: String,
    pub idle_delay: Duration,
    pub error_base_delay: Duration,
    pub error_max_delay: Duration,
    pub shutdown_drain_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProviderSubmitDaemonReport {
    pub deadline_resolved: u64,
    pub fresh_submitted: u64,
    pub fresh_projection_rejected: u64,
    pub recovery_completed: u64,
    pub recovery_deferred: u64,
    pub idle: u64,
    pub errors: u64,
}

impl ProviderSubmitDaemonReport {
    fn record(&mut self, run: ProviderSubmitRun) {
        let counter = match run {
            ProviderSubmitRun::Idle => &mut self.idle,
            ProviderSubmitRun::DeadlineResolved => &mut self.deadline_resolved,
            ProviderSubmitRun::FreshSubmitted => &mut self.fresh_submitted,
            ProviderSubmitRun::FreshProjectionRejected => &mut self.fresh_projection_rejected,
            ProviderSubmitRun::RecoveryCompleted => &mut self.recovery_completed,
            ProviderSubmitRun::RecoveryDeferred => &mut self.recovery_deferred,
        };
        *counter = counter.saturating_add(1);
    }

    fn merge(&mut self, other: Self) {
        self.deadline_resolved = self
            .deadline_resolved
            .saturating_add(other.deadline_resolved);
        self.fresh_submitted = self.fresh_submitted.saturating_add(other.fresh_submitted);
        self.fresh_projection_rejected = self
            .fresh_projection_rejected
            .saturating_add(other.fresh_projection_rejected);
        self.recovery_completed = self
            .recovery_completed
            .saturating_add(other.recovery_completed);
        self.recovery_deferred = self
            .recovery_deferred
            .saturating_add(other.recovery_deferred);
        self.idle = self.idle.saturating_add(other.idle);
        self.errors = self.errors.saturating_add(other.errors);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderSubmitDaemonError {
    #[error("provider submit daemon configuration is invalid")]
    InvalidConfiguration,
    #[error("provider submit daemon lane terminated unexpectedly")]
    LaneTerminated,
    #[error("provider submit daemon shutdown drain timed out")]
    ShutdownDrainTimedOut,
}

pub struct ProviderSubmitDaemon<I> {
    iteration: Arc<I>,
    config: ProviderSubmitDaemonConfig,
    jitter_seed: [u8; 16],
}

impl<I> ProviderSubmitDaemon<I>
where
    I: ProviderSubmitIteration,
{
    pub fn new(
        iteration: Arc<I>,
        config: ProviderSubmitDaemonConfig,
    ) -> Result<Self, ProviderSubmitDaemonError> {
        Self::with_jitter_seed(iteration, config, *uuid::Uuid::new_v4().as_bytes())
    }

    fn with_jitter_seed(
        iteration: Arc<I>,
        config: ProviderSubmitDaemonConfig,
        jitter_seed: [u8; 16],
    ) -> Result<Self, ProviderSubmitDaemonError> {
        validate_config(&config)?;
        Ok(Self {
            iteration,
            config,
            jitter_seed,
        })
    }

    pub async fn run_until_shutdown<S>(
        &self,
        shutdown: S,
    ) -> Result<ProviderSubmitDaemonReport, ProviderSubmitDaemonError>
    where
        S: Future<Output = ()>,
    {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut lanes = JoinSet::new();
        for lane in 0..self.config.max_in_flight {
            lanes.spawn(run_lane(
                Arc::clone(&self.iteration),
                self.config.clone(),
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
                return Err(ProviderSubmitDaemonError::LaneTerminated);
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
                Err(ProviderSubmitDaemonError::ShutdownDrainTimedOut)
            }
        }
    }
}

async fn run_lane<I>(
    iteration: Arc<I>,
    config: ProviderSubmitDaemonConfig,
    jitter_seed: [u8; 16],
    lane: usize,
    mut shutdown: watch::Receiver<bool>,
) -> ProviderSubmitDaemonReport
where
    I: ProviderSubmitIteration,
{
    let mut report = ProviderSubmitDaemonReport::default();
    let mut sequence = 0_u64;
    let mut consecutive_errors = 0_u32;
    let mut delay_sequence = 0_u64;

    loop {
        if *shutdown.borrow_and_update() {
            return report;
        }
        let command = iteration_command(&config.owner_prefix, lane, sequence);
        let delay = match iteration.run_once(&command).await {
            Ok(run) => {
                report.record(run);
                consecutive_errors = 0;
                if run != ProviderSubmitRun::Idle {
                    sequence = sequence.wrapping_add(1);
                    continue;
                }
                idle_jitter(config.idle_delay, jitter_seed, lane, delay_sequence)
            }
            Err(_) => {
                report.errors = report.errors.saturating_add(1);
                consecutive_errors = consecutive_errors.saturating_add(1);
                tracing::error!(
                    lane,
                    sequence,
                    consecutive_errors,
                    error_type = std::any::type_name::<I::Error>(),
                    "provider submit iteration failed"
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

fn iteration_command(
    owner_prefix: &str,
    lane: usize,
    sequence: u64,
) -> ProviderSubmitIterationCommand {
    let owner = format!("{owner_prefix}-l{lane:04x}-s{sequence:016x}");
    ProviderSubmitIterationCommand::new(&owner, format!("{owner}-claim"), format!("{owner}-defer"))
        .expect("validated submit daemon identity must produce valid commands")
}

async fn sleep_or_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    tokio::select! {
        biased;
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        _ = tokio::time::sleep(delay) => false,
    }
}

async fn drain_lanes(
    lanes: &mut JoinSet<ProviderSubmitDaemonReport>,
) -> Result<ProviderSubmitDaemonReport, ProviderSubmitDaemonError> {
    let mut report = ProviderSubmitDaemonReport::default();
    while let Some(result) = lanes.join_next().await {
        match result {
            Ok(lane_report) => report.merge(lane_report),
            Err(error) => {
                tracing::error!(
                    task.id = ?error.id(),
                    task.cancelled = error.is_cancelled(),
                    task.panicked = error.is_panic(),
                    "provider submit daemon lane failed while draining"
                );
                return Err(ProviderSubmitDaemonError::LaneTerminated);
            }
        }
    }
    Ok(report)
}

async fn drain_aborted_lanes(lanes: &mut JoinSet<ProviderSubmitDaemonReport>) {
    while lanes.join_next().await.is_some() {}
}

fn trace_lane_termination(
    result: Option<Result<ProviderSubmitDaemonReport, tokio::task::JoinError>>,
) {
    match result {
        Some(Ok(report)) => {
            tracing::error!(
                ?report,
                "provider submit daemon lane exited before shutdown"
            )
        }
        Some(Err(error)) => tracing::error!(
            task.id = ?error.id(),
            task.cancelled = error.is_cancelled(),
            task.panicked = error.is_panic(),
            "provider submit daemon lane failed"
        ),
        None => tracing::error!("provider submit daemon lost all lanes before shutdown"),
    }
}

fn validate_config(config: &ProviderSubmitDaemonConfig) -> Result<(), ProviderSubmitDaemonError> {
    let delays = [
        config.idle_delay,
        config.error_base_delay,
        config.error_max_delay,
        config.shutdown_drain_timeout,
    ];
    if !(1..=MAX_PROVIDER_SUBMIT_LANES).contains(&config.max_in_flight)
        || config.owner_prefix.is_empty()
        || config.owner_prefix.len() > MAX_OWNER_PREFIX_BYTES
        || !config
            .owner_prefix
            .bytes()
            .all(|byte| byte.is_ascii_graphic())
        || delays
            .iter()
            .any(|delay| delay.is_zero() || *delay > MAX_DELAY)
        || config.error_base_delay > config.error_max_delay
    {
        return Err(ProviderSubmitDaemonError::InvalidConfiguration);
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

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("validated daemon delay fits u64 nanoseconds")
}

fn sample_below(seed: [u8; 16], lane: usize, sequence: u64, upper_bound: u64) -> u64 {
    if upper_bound <= 1 {
        return 0;
    }
    let mut hasher = Sha256::new();
    hasher.update(seed);
    hasher.update((lane as u64).to_be_bytes());
    hasher.update(sequence.to_be_bytes());
    let digest = hasher.finalize();
    let sample = u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 has eight bytes"));
    sample % upper_bound
}

#[cfg(test)]
mod tests;
