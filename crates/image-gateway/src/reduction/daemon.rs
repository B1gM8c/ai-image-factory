use std::{
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
};

use async_trait::async_trait;
use tokio::time::{Instant, MissedTickBehavior};

use super::{
    CanonicalExecutorOutcome, CustomerArtifactPublishError, CustomerArtifactPublisher,
    ExecutorTerminalError, ExecutorTerminalLease, ExecutorTerminalStore,
};
use crate::artifacts::ArtifactMetadata;

const MAX_LEASE_MS: i64 = 10 * 60 * 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReducerDaemonRun {
    Idle,
    Completed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReducerDaemonError {
    #[error("reducer daemon configuration is invalid")]
    InvalidConfiguration,
    #[error(transparent)]
    Store(ExecutorTerminalError),
    #[error(transparent)]
    Publish(CustomerArtifactPublishError),
    #[error("reducer daemon shutdown drain timed out")]
    ShutdownDrainTimedOut,
}

impl From<ExecutorTerminalError> for ReducerDaemonError {
    fn from(error: ExecutorTerminalError) -> Self {
        Self::Store(error)
    }
}

impl From<CustomerArtifactPublishError> for ReducerDaemonError {
    fn from(error: CustomerArtifactPublishError) -> Self {
        Self::Publish(error)
    }
}

#[async_trait]
pub trait TerminalArtifactPublisher: Send + Sync + 'static {
    async fn publish(
        &self,
        lease: &ExecutorTerminalLease,
    ) -> Result<ArtifactMetadata, CustomerArtifactPublishError>;
}

#[async_trait]
impl TerminalArtifactPublisher for CustomerArtifactPublisher {
    async fn publish(
        &self,
        lease: &ExecutorTerminalLease,
    ) -> Result<ArtifactMetadata, CustomerArtifactPublishError> {
        CustomerArtifactPublisher::publish(self, lease).await
    }
}

pub struct ReducerDaemon<S, P> {
    store: S,
    publisher: P,
    owner: String,
    lease_ms: i64,
    heartbeat_interval: Duration,
}

impl<S, P> ReducerDaemon<S, P>
where
    S: ExecutorTerminalStore,
    P: TerminalArtifactPublisher,
{
    pub fn new(
        store: S,
        publisher: P,
        owner: String,
        lease_ms: i64,
        heartbeat_interval: Duration,
    ) -> Self {
        Self {
            store,
            publisher,
            owner,
            lease_ms,
            heartbeat_interval,
        }
    }

    pub async fn run_once(&self) -> Result<ReducerDaemonRun, ReducerDaemonError> {
        self.validate_configuration()?;
        let Some(lease) = self
            .store
            .claim_terminal(&self.owner, self.lease_ms)
            .await?
        else {
            return Ok(ReducerDaemonRun::Idle);
        };

        let (lease, customer_artifact) =
            if matches!(lease.outcome, CanonicalExecutorOutcome::Succeeded(_)) {
                let publication_lease = lease.clone();
                let (lease, artifact) = self
                    .run_with_heartbeat(lease, self.publisher.publish(&publication_lease))
                    .await?;
                (lease, Some(artifact?))
            } else {
                (lease, None)
            };

        let completion_lease = lease.clone();
        let (_, completion) = self
            .run_with_heartbeat(
                lease,
                self.store
                    .complete_terminal(&completion_lease, customer_artifact.as_ref()),
            )
            .await?;
        completion?;
        Ok(ReducerDaemonRun::Completed)
    }

    pub async fn run_until_shutdown<F>(
        &self,
        shutdown: F,
        poll_interval: Duration,
        drain_timeout: Duration,
    ) -> Result<(), ReducerDaemonError>
    where
        F: Future<Output = ()>,
    {
        self.validate_configuration()?;
        if poll_interval.is_zero() || drain_timeout.is_zero() {
            return Err(ReducerDaemonError::InvalidConfiguration);
        }
        tokio::pin!(shutdown);

        loop {
            let run = self.run_once();
            tokio::pin!(run);
            let (result, shutting_down) = tokio::select! {
                _ = &mut shutdown => {
                    let result = tokio::time::timeout(drain_timeout, &mut run)
                        .await
                        .map_err(|_| ReducerDaemonError::ShutdownDrainTimedOut)?;
                    (result, true)
                }
                result = &mut run => (result, false),
            };
            match result {
                Ok(_) => {}
                Err(error @ ReducerDaemonError::Store(_))
                | Err(error @ ReducerDaemonError::Publish(_)) => {
                    tracing::error!(error = ?error, "terminal reduction iteration failed");
                }
                Err(error) => return Err(error),
            }
            if shutting_down {
                return Ok(());
            }
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = tokio::time::sleep(poll_interval) => {}
            }
        }
    }

    fn validate_configuration(&self) -> Result<(), ReducerDaemonError> {
        let interval_ms = i64::try_from(self.heartbeat_interval.as_millis())
            .map_err(|_| ReducerDaemonError::InvalidConfiguration)?;
        let heartbeat_budget = interval_ms
            .checked_mul(3)
            .ok_or(ReducerDaemonError::InvalidConfiguration)?;
        if self.owner.is_empty()
            || self.owner.len() > 255
            || self.owner.bytes().any(|byte| byte.is_ascii_control())
            || !(1..=MAX_LEASE_MS).contains(&self.lease_ms)
            || interval_ms <= 0
            || heartbeat_budget > self.lease_ms
        {
            return Err(ReducerDaemonError::InvalidConfiguration);
        }
        Ok(())
    }

    async fn run_with_heartbeat<T>(
        &self,
        mut lease: ExecutorTerminalLease,
        operation: impl Future<Output = T>,
    ) -> Result<(ExecutorTerminalLease, T), ReducerDaemonError> {
        tokio::pin!(operation);
        let first_tick = Instant::now() + self.heartbeat_interval;
        let mut heartbeat = tokio::time::interval_at(first_tick, self.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                result = &mut operation => return Ok((lease, result)),
                _ = heartbeat.tick() => {
                    let renewal_result = {
                        let heartbeat_lease = lease.clone();
                        let renewal = self
                            .store
                            .heartbeat_terminal(&heartbeat_lease, self.lease_ms);
                        tokio::pin!(renewal);
                        tokio::select! {
                            biased;
                            result = &mut operation => return Ok((lease, result)),
                            renewed = &mut renewal => renewed,
                        }
                    };
                    if let Some(result) = poll_fn(|cx| match operation.as_mut().poll(cx) {
                        Poll::Ready(result) => Poll::Ready(Some(result)),
                        Poll::Pending => Poll::Ready(None),
                    })
                    .await
                    {
                        return Ok((lease, result));
                    }
                    lease = renewal_result?;
                }
            }
        }
    }
}
