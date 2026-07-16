use std::{error::Error, future::Future, future::poll_fn, path::Path, task::Poll, time::Duration};

use image_provider_sdk::SingleOutputCommand;
use tokio::time::{Instant, MissedTickBehavior};

use crate::{
    executor::{
        ExecutorClaimScope, ExecutorLaunchContext, ExecutorLaunchContextStore,
        ExecutorSubmissionError, ExecutorSubmissionLease, ExecutorSubmissionOutcome,
        ExecutorSubmissionStore,
    },
    provider_tasks::{
        ProviderSubmitDriver, ProviderSubmitOrchestrationStore, ProviderSubmitOrchestrator,
        ProviderSubmitOrchestratorError, ProviderSubmitOutcome, ProviderSubmitRecoveryLease,
        ProviderSubmitRecoveryWork, ProviderSubmitSchedulingStore, ProviderSubmitWork,
        ProviderTaskClaimScope, ProviderTaskStoreError,
    },
};

const MAX_RETRY_AFTER_MS: i64 = 24 * 60 * 60 * 1_000;

pub trait ProviderSubmitProjector<D>: Send + Sync + 'static
where
    D: ProviderSubmitDriver,
{
    fn project_fresh(
        &self,
        lease: &ExecutorSubmissionLease,
        context: &ExecutorLaunchContext,
    ) -> Result<SingleOutputCommand<D::Payload>, ProviderSubmitProjectionError>;

    fn project_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
    ) -> Result<SingleOutputCommand<D::Payload>, ProviderSubmitProjectionError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderSubmitProjectionError {
    #[error("provider submit source command is invalid")]
    InvalidSourceCommand,
    #[error("provider submit source command does not match the frozen execution contract")]
    ContractMismatch,
    #[error("provider submit output is outside the supported provider command range")]
    OutputOutOfRange,
}

impl ProviderSubmitProjectionError {
    pub fn error_code(self) -> &'static str {
        match self {
            Self::InvalidSourceCommand => "provider_submit_projection_invalid",
            Self::ContractMismatch => "provider_submit_projection_mismatch",
            Self::OutputOutOfRange => "provider_submit_projection_output_invalid",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSubmitIterationCommand {
    owner: String,
    recovery_claim_command_id: String,
    recovery_defer_command_id: String,
}

impl ProviderSubmitIterationCommand {
    pub fn new(
        owner: impl Into<String>,
        recovery_claim_command_id: impl Into<String>,
        recovery_defer_command_id: impl Into<String>,
    ) -> Result<Self, ProviderSubmitIterationCommandError> {
        let command = Self {
            owner: owner.into(),
            recovery_claim_command_id: recovery_claim_command_id.into(),
            recovery_defer_command_id: recovery_defer_command_id.into(),
        };
        if !valid_identity(&command.owner, 128)
            || !valid_identity(&command.recovery_claim_command_id, 255)
            || !valid_identity(&command.recovery_defer_command_id, 255)
            || command.recovery_claim_command_id == command.recovery_defer_command_id
        {
            return Err(ProviderSubmitIterationCommandError);
        }
        Ok(command)
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn recovery_claim_command_id(&self) -> &str {
        &self.recovery_claim_command_id
    }

    pub fn recovery_defer_command_id(&self) -> &str {
        &self.recovery_defer_command_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("provider submit iteration command identity is invalid")]
pub struct ProviderSubmitIterationCommandError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSubmitServiceConfig {
    pub executor_scope: ExecutorClaimScope,
    pub provider_scope: ProviderTaskClaimScope,
    pub provider_timeout_ms: i64,
    pub executor_lease_ms: i64,
    pub recovery_lease_ms: i64,
    pub heartbeat_interval: Duration,
    pub recovery_retry_after_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderSubmitRun {
    Idle,
    DeadlineResolved,
    FreshSubmitted,
    FreshProjectionRejected,
    RecoveryCompleted,
    RecoveryDeferred,
}

pub trait ProviderSubmitIteration: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    fn run_once(
        &self,
        command: &ProviderSubmitIterationCommand,
    ) -> impl Future<Output = Result<ProviderSubmitRun, Self::Error>> + Send;
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderSubmitServiceError {
    #[error("provider submit service configuration is invalid")]
    InvalidConfiguration,
    #[error(transparent)]
    Executor(ExecutorSubmissionError),
    #[error(transparent)]
    Provider(ProviderTaskStoreError),
    #[error(transparent)]
    Orchestrator(ProviderSubmitOrchestratorError),
}

impl From<ExecutorSubmissionError> for ProviderSubmitServiceError {
    fn from(error: ExecutorSubmissionError) -> Self {
        Self::Executor(error)
    }
}

impl From<ProviderTaskStoreError> for ProviderSubmitServiceError {
    fn from(error: ProviderTaskStoreError) -> Self {
        Self::Provider(error)
    }
}

impl From<ProviderSubmitOrchestratorError> for ProviderSubmitServiceError {
    fn from(error: ProviderSubmitOrchestratorError) -> Self {
        Self::Orchestrator(error)
    }
}

pub struct ProviderSubmitService<E, S, D, P> {
    executor_store: E,
    provider_store: S,
    orchestrator: ProviderSubmitOrchestrator<S, D>,
    projector: P,
    config: ProviderSubmitServiceConfig,
}

impl<E, S, D, P> ProviderSubmitService<E, S, D, P>
where
    E: ExecutorSubmissionStore + ExecutorLaunchContextStore,
    S: ProviderSubmitOrchestrationStore + ProviderSubmitSchedulingStore + Clone,
    D: ProviderSubmitDriver,
    P: ProviderSubmitProjector<D>,
{
    pub fn new(
        executor_store: E,
        provider_store: S,
        driver: D,
        projector: P,
        config: ProviderSubmitServiceConfig,
        journal_root: impl AsRef<Path>,
    ) -> Result<Self, ProviderSubmitServiceError> {
        validate_config(&config, driver.provider_id())?;
        let orchestrator = ProviderSubmitOrchestrator::new(
            provider_store.clone(),
            driver,
            config.provider_timeout_ms,
            journal_root,
        )?;
        Ok(Self {
            executor_store,
            provider_store,
            orchestrator,
            projector,
            config,
        })
    }

    pub async fn run_once(
        &self,
        command: &ProviderSubmitIterationCommand,
    ) -> Result<ProviderSubmitRun, ProviderSubmitServiceError> {
        if self
            .provider_store
            .resolve_due_submit_deadline(&self.config.provider_scope)
            .await?
            .is_some()
        {
            return Ok(ProviderSubmitRun::DeadlineResolved);
        }

        if let Some(recovery) = self
            .provider_store
            .claim_submit_recovery(
                &self.config.provider_scope,
                command.owner(),
                command.recovery_claim_command_id(),
                self.config.recovery_lease_ms,
            )
            .await?
        {
            return self.run_recovery(command, recovery).await;
        }

        let Some((lease, needs_start)) = self.acquire_fresh(command.owner()).await? else {
            return Ok(ProviderSubmitRun::Idle);
        };
        self.run_fresh(lease, needs_start).await
    }

    async fn acquire_fresh(
        &self,
        owner: &str,
    ) -> Result<Option<(ExecutorSubmissionLease, bool)>, ProviderSubmitServiceError> {
        if let Some(resume) = self
            .executor_store
            .resume_owned(&self.config.executor_scope, owner)
            .await?
        {
            let needs_start = resume.needs_start();
            return Ok(Some((resume.into_lease(), needs_start)));
        }
        Ok(self
            .executor_store
            .claim_prepared(
                &self.config.executor_scope,
                owner,
                self.config.executor_lease_ms,
            )
            .await?
            .map(|lease| (lease, true)))
    }

    async fn run_fresh(
        &self,
        lease: ExecutorSubmissionLease,
        needs_start: bool,
    ) -> Result<ProviderSubmitRun, ProviderSubmitServiceError> {
        if needs_start {
            self.executor_store.start(&lease).await?;
        }
        let lease = self
            .executor_store
            .heartbeat(&lease, self.config.executor_lease_ms)
            .await?;
        let context = self.executor_store.load_launch_context(&lease).await?;
        let projected = match self.projector.project_fresh(&lease, &context) {
            Ok(projected) => projected,
            Err(error) => {
                self.executor_store
                    .record_outcome(
                        &lease,
                        &ExecutorSubmissionOutcome::Failed {
                            error_code: error.error_code().to_owned(),
                        },
                    )
                    .await?;
                return Ok(ProviderSubmitRun::FreshProjectionRejected);
            }
        };
        let work = ProviderSubmitWork::<D>::new(&lease, projected)?;
        let operation = self.orchestrator.submit(work);
        let (_, outcome) = self.run_with_executor_heartbeat(lease, operation).await?;
        outcome?;
        Ok(ProviderSubmitRun::FreshSubmitted)
    }

    async fn run_recovery(
        &self,
        command: &ProviderSubmitIterationCommand,
        lease: ProviderSubmitRecoveryLease,
    ) -> Result<ProviderSubmitRun, ProviderSubmitServiceError> {
        let projected = match self.projector.project_recovery(&lease) {
            Ok(projected) => projected,
            Err(_) => {
                self.provider_store
                    .defer_submit_recovery(
                        &lease,
                        command.recovery_defer_command_id(),
                        self.config.recovery_retry_after_ms,
                    )
                    .await?;
                return Ok(ProviderSubmitRun::RecoveryDeferred);
            }
        };
        let work = ProviderSubmitRecoveryWork::<D>::new(&lease, projected)?;
        let operation = self.orchestrator.recover(work);
        let (lease, outcome) = self.run_with_recovery_heartbeat(lease, operation).await?;
        let outcome = outcome?;
        match outcome {
            ProviderSubmitOutcome::AwaitingEvidence(_) => {
                self.provider_store
                    .defer_submit_recovery(
                        &lease,
                        command.recovery_defer_command_id(),
                        self.config.recovery_retry_after_ms,
                    )
                    .await?;
                Ok(ProviderSubmitRun::RecoveryDeferred)
            }
            ProviderSubmitOutcome::Attached(_) | ProviderSubmitOutcome::Terminal(_) => {
                Ok(ProviderSubmitRun::RecoveryCompleted)
            }
        }
    }

    async fn run_with_executor_heartbeat<T>(
        &self,
        mut lease: ExecutorSubmissionLease,
        operation: impl Future<Output = T>,
    ) -> Result<(ExecutorSubmissionLease, T), ProviderSubmitServiceError> {
        tokio::pin!(operation);
        let first_tick = Instant::now() + self.config.heartbeat_interval;
        let mut heartbeat = tokio::time::interval_at(first_tick, self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                result = &mut operation => return Ok((lease, result)),
                _ = heartbeat.tick() => {
                    let renewal_result = {
                        let heartbeat_lease = lease.clone();
                        let renewal = self
                            .executor_store
                            .heartbeat(&heartbeat_lease, self.config.executor_lease_ms);
                        tokio::pin!(renewal);
                        tokio::select! {
                            biased;
                            result = &mut operation => return Ok((lease, result)),
                            renewed = &mut renewal => renewed,
                        }
                    };
                    if let Some(result) = poll_ready(&mut operation).await {
                        return Ok((lease, result));
                    }
                    lease = renewal_result?;
                }
            }
        }
    }

    async fn run_with_recovery_heartbeat<T>(
        &self,
        mut lease: ProviderSubmitRecoveryLease,
        operation: impl Future<Output = T>,
    ) -> Result<(ProviderSubmitRecoveryLease, T), ProviderSubmitServiceError> {
        tokio::pin!(operation);
        let first_tick = Instant::now() + self.config.heartbeat_interval;
        let mut heartbeat = tokio::time::interval_at(first_tick, self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                result = &mut operation => return Ok((lease, result)),
                _ = heartbeat.tick() => {
                    let renewal_result = {
                        let heartbeat_lease = lease.clone();
                        let renewal = self
                            .provider_store
                            .heartbeat_submit_recovery(
                                &heartbeat_lease,
                                self.config.recovery_lease_ms,
                            );
                        tokio::pin!(renewal);
                        tokio::select! {
                            biased;
                            result = &mut operation => return Ok((lease, result)),
                            renewed = &mut renewal => renewed,
                        }
                    };
                    if let Some(result) = poll_ready(&mut operation).await {
                        return Ok((lease, result));
                    }
                    lease = renewal_result?;
                }
            }
        }
    }
}

impl<E, S, D, P> ProviderSubmitIteration for ProviderSubmitService<E, S, D, P>
where
    E: ExecutorSubmissionStore + ExecutorLaunchContextStore,
    S: ProviderSubmitOrchestrationStore + ProviderSubmitSchedulingStore + Clone,
    D: ProviderSubmitDriver,
    P: ProviderSubmitProjector<D>,
{
    type Error = ProviderSubmitServiceError;

    async fn run_once(
        &self,
        command: &ProviderSubmitIterationCommand,
    ) -> Result<ProviderSubmitRun, Self::Error> {
        ProviderSubmitService::run_once(self, command).await
    }
}

async fn poll_ready<T>(future: &mut std::pin::Pin<&mut impl Future<Output = T>>) -> Option<T> {
    poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Ready(result) => Poll::Ready(Some(result)),
        Poll::Pending => Poll::Ready(None),
    })
    .await
}

fn validate_config(
    config: &ProviderSubmitServiceConfig,
    driver_provider_id: &str,
) -> Result<(), ProviderSubmitServiceError> {
    let heartbeat_ms = i64::try_from(config.heartbeat_interval.as_millis())
        .map_err(|_| ProviderSubmitServiceError::InvalidConfiguration)?;
    let heartbeat_budget = heartbeat_ms
        .checked_mul(3)
        .ok_or(ProviderSubmitServiceError::InvalidConfiguration)?;
    if config.executor_scope.execution_profile_id.is_nil()
        || config.executor_scope.provider_id != config.provider_scope.provider_id
        || config.executor_scope.provider_id != driver_provider_id
        || config.executor_scope.command_schema.is_empty()
        || config.executor_scope.adapter_revision.is_empty()
        || config.provider_scope.provider_account_id.is_nil()
        || config.provider_timeout_ms <= 0
        || config.executor_lease_ms <= 0
        || config.recovery_lease_ms <= 0
        || heartbeat_ms <= 0
        || heartbeat_budget > config.executor_lease_ms
        || heartbeat_budget > config.recovery_lease_ms
        || !(1..=MAX_RETRY_AFTER_MS).contains(&config.recovery_retry_after_ms)
    {
        return Err(ProviderSubmitServiceError::InvalidConfiguration);
    }
    Ok(())
}

fn valid_identity(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && value.bytes().all(|byte| byte.is_ascii_graphic())
}
