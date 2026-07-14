use std::time::Duration;

use tokio::time::{Instant, MissedTickBehavior};

use super::{
    DurableRunner, ExecutorClaimScope, ExecutorSubmissionError, ExecutorSubmissionLease,
    ExecutorSubmissionOutcome, ExecutorSubmissionStore,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutorDaemonRun {
    Idle,
    Recorded,
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExecutorDaemonError {
    #[error("executor daemon configuration is invalid")]
    InvalidConfiguration,
    #[error(transparent)]
    Store(ExecutorSubmissionError),
}

impl ExecutorDaemonError {
    pub fn store_error(&self) -> Option<&ExecutorSubmissionError> {
        match self {
            Self::Store(error) => Some(error),
            Self::InvalidConfiguration => None,
        }
    }
}

impl From<ExecutorSubmissionError> for ExecutorDaemonError {
    fn from(error: ExecutorSubmissionError) -> Self {
        Self::Store(error)
    }
}

pub struct ExecutorDaemon<S, R> {
    store: S,
    runner: R,
    scope: ExecutorClaimScope,
    owner: String,
    lease_ms: i64,
    heartbeat_interval: Duration,
}

impl<S, R> ExecutorDaemon<S, R>
where
    S: ExecutorSubmissionStore,
    R: DurableRunner,
{
    pub fn new(
        store: S,
        runner: R,
        scope: ExecutorClaimScope,
        owner: String,
        lease_ms: i64,
        heartbeat_interval: Duration,
    ) -> Self {
        Self {
            store,
            runner,
            scope,
            owner,
            lease_ms,
            heartbeat_interval,
        }
    }

    pub async fn run_once(&self) -> Result<ExecutorDaemonRun, ExecutorDaemonError> {
        self.validate_configuration()?;
        let Some((lease, claimed)) = self.acquire().await? else {
            return Ok(ExecutorDaemonRun::Idle);
        };
        if claimed {
            self.store.start(&lease).await?;
        }
        let lease = self.store.heartbeat(&lease, self.lease_ms).await?;
        self.run_with_heartbeat(lease).await
    }

    fn validate_configuration(&self) -> Result<(), ExecutorDaemonError> {
        let interval_ms = i64::try_from(self.heartbeat_interval.as_millis())
            .map_err(|_| ExecutorDaemonError::InvalidConfiguration)?;
        let heartbeat_budget = interval_ms
            .checked_mul(3)
            .ok_or(ExecutorDaemonError::InvalidConfiguration)?;
        if self.owner.is_empty()
            || self.lease_ms <= 0
            || interval_ms <= 0
            || heartbeat_budget > self.lease_ms
        {
            return Err(ExecutorDaemonError::InvalidConfiguration);
        }
        Ok(())
    }

    async fn acquire(
        &self,
    ) -> Result<Option<(ExecutorSubmissionLease, bool)>, ExecutorDaemonError> {
        if let Some(lease) = self.store.resume_running(&self.scope, &self.owner).await? {
            return Ok(Some((lease, false)));
        }
        Ok(self
            .store
            .claim_prepared(&self.scope, &self.owner, self.lease_ms)
            .await?
            .map(|lease| (lease, true)))
    }

    async fn run_with_heartbeat(
        &self,
        mut lease: ExecutorSubmissionLease,
    ) -> Result<ExecutorDaemonRun, ExecutorDaemonError> {
        let runner = self.runner.start_or_attach(lease.clone());
        tokio::pin!(runner);
        let first_tick = Instant::now() + self.heartbeat_interval;
        let mut heartbeat = tokio::time::interval_at(first_tick, self.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let outcome = loop {
            tokio::select! {
                biased;
                _ = heartbeat.tick() => {
                    lease = self.store.heartbeat(&lease, self.lease_ms).await?;
                }
                outcome = &mut runner => break outcome,
            }
        };
        lease = self.store.heartbeat(&lease, self.lease_ms).await?;
        let outcome = ExecutorSubmissionOutcome::from(outcome);
        self.store.record_outcome(&lease, &outcome).await?;
        Ok(ExecutorDaemonRun::Recorded)
    }
}
