use std::{
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
};

use image_provider_sdk::{
    ArtifactSinkError, EffectCertainty, PollObservation, ProviderFailure, ProviderFailureClass,
    RetryDirective,
};
use tokio::{
    sync::Semaphore,
    time::{Instant, MissedTickBehavior},
};

use super::{
    ControlledProviderArtifactSink, ProviderArtifactSinkContractError,
    ProviderArtifactStageContext, ProviderArtifactStagerFactory, ProviderPollDriver,
    ProviderPollDriverCall,
};
use crate::provider_tasks::{
    ProviderArtifactAuthority, ProviderArtifactPublication, ProviderRemoteTask,
    ProviderTaskClaimScope, ProviderTaskLease, ProviderTaskObservation,
    ProviderTaskObservationOutcome, ProviderTaskObservationSource, ProviderTaskStore,
    ProviderTaskStoreError,
};

const DEFAULT_PENDING_POLL_AFTER_MS: u64 = 1_000;
const DEFAULT_BACKOFF_POLL_AFTER_MS: u64 = 1_000;
const MAX_POLL_AFTER_MS: u64 = 24 * 60 * 60 * 1_000;

pub trait ProviderPollStore: Send + Sync + 'static {
    fn claim_poll(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        lease_ms: i64,
    ) -> impl Future<Output = Result<Option<ProviderTaskLease>, ProviderTaskStoreError>> + Send;

    fn heartbeat_poll(
        &self,
        lease: &ProviderTaskLease,
        lease_ms: i64,
    ) -> impl Future<Output = Result<ProviderTaskLease, ProviderTaskStoreError>> + Send;

    fn publish_poll_artifact(
        &self,
        lease: &ProviderTaskLease,
        authority: &ProviderArtifactAuthority,
    ) -> impl Future<Output = Result<ProviderArtifactPublication, ProviderTaskStoreError>> + Send;

    fn record_poll_observation(
        &self,
        lease: &ProviderTaskLease,
        observation: &ProviderTaskObservation,
    ) -> impl Future<Output = Result<ProviderRemoteTask, ProviderTaskStoreError>> + Send;
}

impl<S> ProviderPollStore for S
where
    S: ProviderTaskStore,
{
    async fn claim_poll(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ProviderTaskLease>, ProviderTaskStoreError> {
        ProviderTaskStore::claim_due(self, scope, owner, lease_ms).await
    }

    async fn heartbeat_poll(
        &self,
        lease: &ProviderTaskLease,
        lease_ms: i64,
    ) -> Result<ProviderTaskLease, ProviderTaskStoreError> {
        ProviderTaskStore::heartbeat(self, lease, lease_ms).await
    }

    async fn publish_poll_artifact(
        &self,
        lease: &ProviderTaskLease,
        authority: &ProviderArtifactAuthority,
    ) -> Result<ProviderArtifactPublication, ProviderTaskStoreError> {
        ProviderTaskStore::publish_artifact_authority(self, lease, authority).await
    }

    async fn record_poll_observation(
        &self,
        lease: &ProviderTaskLease,
        observation: &ProviderTaskObservation,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        ProviderTaskStore::record_observation(self, lease, observation).await
    }
}

pub struct ProviderPollOrchestrator<S, D, F> {
    store: S,
    driver: D,
    stagers: F,
    scope: ProviderTaskClaimScope,
    owner: String,
    lease_ms: i64,
    heartbeat_interval: Duration,
    max_materializations: usize,
    materialization_limit: std::sync::Arc<Semaphore>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderPollOrchestratorConfig {
    pub scope: ProviderTaskClaimScope,
    pub owner: String,
    pub lease_ms: i64,
    pub heartbeat_interval: Duration,
    pub max_materializations: usize,
}

impl<S, D, F> ProviderPollOrchestrator<S, D, F>
where
    S: ProviderPollStore,
    D: ProviderPollDriver,
    F: ProviderArtifactStagerFactory,
{
    pub fn new(
        store: S,
        driver: D,
        stagers: F,
        config: ProviderPollOrchestratorConfig,
    ) -> Result<Self, ProviderPollOrchestratorError> {
        let ProviderPollOrchestratorConfig {
            scope,
            owner,
            lease_ms,
            heartbeat_interval,
            max_materializations,
        } = config;
        if max_materializations > Semaphore::MAX_PERMITS {
            return Err(ProviderPollOrchestratorError::InvalidConfiguration);
        }
        let orchestrator = Self {
            store,
            driver,
            stagers,
            scope,
            owner,
            lease_ms,
            heartbeat_interval,
            max_materializations,
            materialization_limit: std::sync::Arc::new(Semaphore::new(max_materializations)),
        };
        orchestrator.validate_configuration()?;
        Ok(orchestrator)
    }

    pub async fn run_once(&self) -> Result<ProviderPollRun, ProviderPollOrchestratorError> {
        let claim_started_at = Instant::now();
        let Some(lease) = self
            .store
            .claim_poll(&self.scope, &self.owner, self.lease_ms)
            .await?
        else {
            return Ok(ProviderPollRun::Idle);
        };
        self.validate_lease(&lease)?;

        if let Some(publication) = lease.committed_artifact().cloned() {
            let task = self.record_artifact(&lease, publication).await?;
            return Ok(ProviderPollRun::Observed(task));
        }

        let call = ProviderPollDriverCall::new(&lease)
            .map_err(|_| ProviderPollOrchestratorError::InvalidLease)?;
        let stage_context = ProviderArtifactStageContext::from_lease(&lease);
        let mut sink = ControlledProviderArtifactSink::new(
            &self.stagers,
            stage_context,
            std::sync::Arc::clone(&self.materialization_limit),
        );
        let operation = self.driver.poll(&call, &mut sink);
        let poll_budget = Duration::from_millis(lease.remaining_budget_ms());
        let poll_deadline = claim_started_at + poll_budget;
        let (lease, result) =
            tokio::time::timeout_at(poll_deadline, self.run_with_heartbeat(lease, operation))
                .await
                .map_err(|_| ProviderPollOrchestratorError::ProviderDeadlineElapsed)??;
        let task = self.resolve_poll_result(&lease, sink, result).await?;
        Ok(ProviderPollRun::Observed(task))
    }

    fn validate_configuration(&self) -> Result<(), ProviderPollOrchestratorError> {
        let heartbeat_ms = i64::try_from(self.heartbeat_interval.as_millis())
            .map_err(|_| ProviderPollOrchestratorError::InvalidConfiguration)?;
        let heartbeat_budget = heartbeat_ms
            .checked_mul(3)
            .ok_or(ProviderPollOrchestratorError::InvalidConfiguration)?;
        if self.owner.is_empty()
            || self.owner.len() > 255
            || self.owner.bytes().any(|byte| byte.is_ascii_control())
            || self.lease_ms <= 0
            || heartbeat_ms <= 0
            || heartbeat_budget > self.lease_ms
            || self.max_materializations == 0
            || self.scope.provider_id != self.driver.provider_id()
            || self.scope.provider_account_id.is_nil()
        {
            return Err(ProviderPollOrchestratorError::InvalidConfiguration);
        }
        Ok(())
    }

    fn validate_lease(
        &self,
        lease: &ProviderTaskLease,
    ) -> Result<(), ProviderPollOrchestratorError> {
        if lease.task.provider_id != self.scope.provider_id
            || lease.task.provider_account_id != self.scope.provider_account_id
            || lease.task.provider_id != self.driver.provider_id()
            || lease.poll_owner != self.owner
            || lease.poll_lease_epoch <= 0
            || lease.remaining_budget_ms() == 0
            || lease.context().execution_binding_sha256().is_empty()
        {
            return Err(ProviderPollOrchestratorError::InvalidLease);
        }
        Ok(())
    }

    async fn run_with_heartbeat<T>(
        &self,
        mut lease: ProviderTaskLease,
        operation: impl Future<Output = T>,
    ) -> Result<(ProviderTaskLease, T), ProviderPollOrchestratorError> {
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
                            .heartbeat_poll(&heartbeat_lease, self.lease_ms);
                        tokio::pin!(renewal);
                        tokio::select! {
                            biased;
                            result = &mut operation => return Ok((lease, result)),
                            renewed = &mut renewal => renewed,
                        }
                    };
                    if let Some(result) = poll_fn(|context| match operation.as_mut().poll(context) {
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

    async fn resolve_poll_result(
        &self,
        lease: &ProviderTaskLease,
        sink: ControlledProviderArtifactSink<'_, F>,
        result: Result<PollObservation, ProviderFailure>,
    ) -> Result<ProviderRemoteTask, ProviderPollOrchestratorError> {
        match result {
            Ok(PollObservation::Pending { next_poll_after_ms }) => {
                if sink.into_pristine().is_err() {
                    return self
                        .record_uncertain(lease, "provider_poll_artifact_contract")
                        .await;
                }
                let poll_after_ms =
                    bounded_poll_after(next_poll_after_ms.unwrap_or(DEFAULT_PENDING_POLL_AFTER_MS));
                self.record_waiting(lease, poll_after_ms).await
            }
            Ok(PollObservation::Completed(completed)) => {
                let staged = match sink.into_finalized(completed.artifact()) {
                    Ok(staged) => staged,
                    Err(_) => {
                        return self
                            .record_uncertain(lease, "provider_poll_artifact_contract")
                            .await;
                    }
                };
                if !provider_request_matches(
                    lease,
                    completed.provider_request_id().map(|value| value.as_str()),
                ) {
                    return self
                        .record_uncertain(lease, "provider_poll_request_mismatch")
                        .await;
                }
                let publication = self
                    .store
                    .publish_poll_artifact(lease, staged.authority())
                    .await?;
                if !publication.matches_durable_manifest(staged.manifest()) {
                    return Err(ProviderPollOrchestratorError::ArtifactContract(
                        ProviderArtifactSinkContractError::ManifestMismatch,
                    ));
                }
                self.record_artifact(lease, publication).await
            }
            Ok(PollObservation::Failed(failure)) => {
                if sink.into_pristine().is_err() {
                    return self
                        .record_uncertain(lease, "provider_poll_artifact_contract")
                        .await;
                }
                if !verified_terminal_failure(&failure) {
                    return self
                        .record_uncertain(lease, "provider_poll_failure_contract")
                        .await;
                }
                self.record_failed(lease, failure.code()).await
            }
            Ok(PollObservation::Canceled(evidence)) => {
                if sink.into_pristine().is_err() {
                    return self
                        .record_uncertain(lease, "provider_poll_artifact_contract")
                        .await;
                }
                if !lease.task.cancel_requested
                    || !provider_request_matches(
                        lease,
                        evidence.provider_request_id().map(|value| value.as_str()),
                    )
                {
                    return self
                        .record_uncertain(lease, "provider_poll_cancel_mismatch")
                        .await;
                }
                self.record_canceled(lease, "provider_canceled").await
            }
            Err(failure) => {
                if sink.into_pristine().is_err() {
                    return self
                        .record_uncertain(lease, "provider_poll_artifact_contract")
                        .await;
                }
                match retry_poll_after(&failure) {
                    Some(delay) => self.record_waiting(lease, delay).await,
                    None => self.record_uncertain(lease, failure.code()).await,
                }
            }
        }
    }

    async fn record_waiting(
        &self,
        lease: &ProviderTaskLease,
        poll_after_ms: i64,
    ) -> Result<ProviderRemoteTask, ProviderPollOrchestratorError> {
        self.record(
            lease,
            ProviderTaskObservationOutcome::Waiting { poll_after_ms },
        )
        .await
    }

    async fn record_artifact(
        &self,
        lease: &ProviderTaskLease,
        publication: ProviderArtifactPublication,
    ) -> Result<ProviderRemoteTask, ProviderPollOrchestratorError> {
        let artifact_ref = format!("manifest:{}", publication.manifest().manifest_id().simple());
        self.record(
            lease,
            ProviderTaskObservationOutcome::ArtifactReady {
                artifact_ref,
                publication,
            },
        )
        .await
    }

    async fn record_failed(
        &self,
        lease: &ProviderTaskLease,
        error_code: &str,
    ) -> Result<ProviderRemoteTask, ProviderPollOrchestratorError> {
        self.record(
            lease,
            ProviderTaskObservationOutcome::Failed {
                error_code: error_code.to_owned(),
            },
        )
        .await
    }

    async fn record_canceled(
        &self,
        lease: &ProviderTaskLease,
        error_code: &str,
    ) -> Result<ProviderRemoteTask, ProviderPollOrchestratorError> {
        self.record(
            lease,
            ProviderTaskObservationOutcome::Canceled {
                error_code: error_code.to_owned(),
            },
        )
        .await
    }

    async fn record_uncertain(
        &self,
        lease: &ProviderTaskLease,
        error_code: &str,
    ) -> Result<ProviderRemoteTask, ProviderPollOrchestratorError> {
        self.record(
            lease,
            ProviderTaskObservationOutcome::Uncertain {
                error_code: error_code.to_owned(),
            },
        )
        .await
    }

    async fn record(
        &self,
        lease: &ProviderTaskLease,
        outcome: ProviderTaskObservationOutcome,
    ) -> Result<ProviderRemoteTask, ProviderPollOrchestratorError> {
        let observation = ProviderTaskObservation {
            event_identity: event_identity(lease, &outcome),
            source: ProviderTaskObservationSource::Poll,
            outcome,
        };
        self.store
            .record_poll_observation(lease, &observation)
            .await
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderPollRun {
    Idle,
    Observed(ProviderRemoteTask),
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderPollOrchestratorError {
    #[error("provider poll orchestrator configuration is invalid")]
    InvalidConfiguration,
    #[error("provider poll lease does not match the configured driver and scope")]
    InvalidLease,
    #[error("provider poll exceeded the PostgreSQL-derived remaining budget")]
    ProviderDeadlineElapsed,
    #[error(transparent)]
    Store(#[from] ProviderTaskStoreError),
    #[error(transparent)]
    ArtifactSink(#[from] ArtifactSinkError),
    #[error(transparent)]
    ArtifactContract(#[from] ProviderArtifactSinkContractError),
}

fn provider_request_matches(lease: &ProviderTaskLease, observed: Option<&str>) -> bool {
    observed.is_none() || lease.task.provider_request_id.as_deref() == observed
}

fn bounded_poll_after(value: u64) -> i64 {
    i64::try_from(value.min(MAX_POLL_AFTER_MS)).expect("bounded poll delay fits i64")
}

fn retry_poll_after(failure: &ProviderFailure) -> Option<i64> {
    match failure.retry() {
        RetryDirective::Never => None,
        RetryDirective::SafeImmediate => Some(bounded_poll_after(
            failure.retry_after_ms().unwrap_or_default(),
        )),
        RetryDirective::Backoff => Some(bounded_poll_after(
            failure
                .retry_after_ms()
                .unwrap_or(DEFAULT_BACKOFF_POLL_AFTER_MS),
        )),
    }
}

fn verified_terminal_failure(failure: &ProviderFailure) -> bool {
    failure.effect() == EffectCertainty::NoRemoteEffect
        && failure.retry() == RetryDirective::Never
        && failure.class() != ProviderFailureClass::Ambiguous
}

fn event_identity(lease: &ProviderTaskLease, outcome: &ProviderTaskObservationOutcome) -> String {
    let suffix = match outcome {
        ProviderTaskObservationOutcome::Waiting { .. } => "waiting".to_owned(),
        ProviderTaskObservationOutcome::ArtifactReady { publication, .. } => {
            format!("artifact:{}", publication.manifest().manifest_id().simple())
        }
        ProviderTaskObservationOutcome::Failed { error_code } => {
            format!("failed:{error_code}")
        }
        ProviderTaskObservationOutcome::Canceled { error_code } => {
            format!("canceled:{error_code}")
        }
        ProviderTaskObservationOutcome::Uncertain { error_code } => {
            format!("uncertain:{error_code}")
        }
    };
    format!("poll:{}:{suffix}", lease.poll_lease_epoch)
}
