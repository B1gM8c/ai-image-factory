use std::{
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
};

use tokio::time::{Instant, MissedTickBehavior};

use super::{
    DurableEvidenceRecovery, DurableRunner, DurableRunnerResult, ExecutorClaimScope,
    ExecutorEvidenceStore, ExecutorSubmissionError, ExecutorSubmissionLease,
    ExecutorSubmissionOutcome, ExecutorSubmissionStore, RunnerLaunchAuthority,
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
    #[error("executor runner evidence is temporarily unavailable: {error_code}")]
    RunnerRetryable { error_code: String },
}

impl ExecutorDaemonError {
    pub fn store_error(&self) -> Option<&ExecutorSubmissionError> {
        match self {
            Self::Store(error) => Some(error),
            Self::InvalidConfiguration | Self::RunnerRetryable { .. } => None,
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
        let lease = if claimed {
            self.store.heartbeat(&lease, self.lease_ms).await?
        } else {
            lease
        };
        let authority = if claimed {
            RunnerLaunchAuthority::AllowLaunch
        } else {
            RunnerLaunchAuthority::AttachOnly
        };
        self.run_with_heartbeat(lease, authority).await
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
        if let Some(resume) = self.store.resume_owned(&self.scope, &self.owner).await? {
            let needs_start = resume.needs_start();
            return Ok(Some((resume.into_lease(), needs_start)));
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
        authority: RunnerLaunchAuthority,
    ) -> Result<ExecutorDaemonRun, ExecutorDaemonError> {
        let runner = self.runner.start_or_attach(lease.clone(), authority);
        tokio::pin!(runner);
        let first_tick = Instant::now() + self.heartbeat_interval;
        let mut heartbeat = tokio::time::interval_at(first_tick, self.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let outcome = 'running: loop {
            tokio::select! {
                biased;
                outcome = &mut runner => break outcome,
                _ = heartbeat.tick() => {
                    let renewal_result = {
                        let renewal = self.store.heartbeat(&lease, self.lease_ms);
                        tokio::pin!(renewal);
                        tokio::select! {
                            biased;
                            outcome = &mut runner => break 'running outcome,
                            renewed = &mut renewal => renewed,
                        }
                    };
                    if let Some(outcome) = poll_fn(|cx| match runner.as_mut().poll(cx) {
                        Poll::Ready(outcome) => Poll::Ready(Some(outcome)),
                        Poll::Pending => Poll::Ready(None),
                    })
                    .await
                    {
                        break 'running outcome;
                    }
                    lease = renewal_result?;
                }
            }
        };
        let outcome = terminal_outcome(outcome)?;
        self.store.record_outcome(&lease, &outcome).await?;
        Ok(ExecutorDaemonRun::Recorded)
    }
}

impl<S, R> ExecutorDaemon<S, R>
where
    S: ExecutorSubmissionStore + ExecutorEvidenceStore,
    R: DurableRunner + DurableEvidenceRecovery,
{
    pub async fn recover_evidence_once(&self) -> Result<ExecutorDaemonRun, ExecutorDaemonError> {
        self.validate_configuration()?;
        let Some(lease) = self
            .store
            .load_pending_evidence(&self.scope, &self.owner)
            .await?
        else {
            return Ok(ExecutorDaemonRun::Idle);
        };
        let outcome = match self.runner.recover_evidence(lease.clone()).await {
            DurableRunnerResult::Terminal(outcome) => ExecutorSubmissionOutcome::from(outcome),
            DurableRunnerResult::Retryable { error_code }
                if error_code == "runner_launch_evidence_missing" =>
            {
                ExecutorSubmissionOutcome::Uncertain { error_code }
            }
            DurableRunnerResult::Retryable { error_code } => {
                return Err(ExecutorDaemonError::RunnerRetryable { error_code });
            }
        };
        self.store.record_outcome(&lease, &outcome).await?;
        Ok(ExecutorDaemonRun::Recorded)
    }
}

fn terminal_outcome(
    result: DurableRunnerResult,
) -> Result<ExecutorSubmissionOutcome, ExecutorDaemonError> {
    match result {
        DurableRunnerResult::Terminal(outcome) => Ok(ExecutorSubmissionOutcome::from(outcome)),
        DurableRunnerResult::Retryable { error_code } => {
            Err(ExecutorDaemonError::RunnerRetryable { error_code })
        }
    }
}
