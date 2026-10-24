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
    ProviderArtifactStageContext, ProviderArtifactStagerFactory,
    ProviderNativeBatchPollObservation, ProviderPollDriver, ProviderPollDriverCall,
    batch::ControlledNativeBatchSink,
};
use crate::provider_tasks::{
    NativeBatchMember, ProviderArtifactAuthority, ProviderArtifactPublication, ProviderRemoteTask,
    ProviderTaskClaimScope, ProviderTaskDeadlineStore, ProviderTaskLease, ProviderTaskObservation,
    ProviderTaskObservationOutcome, ProviderTaskObservationSource, ProviderTaskStore,
    ProviderTaskStoreError,
};

const DEFAULT_PENDING_POLL_AFTER_MS: u64 = 1_000;
const DEFAULT_BACKOFF_POLL_AFTER_MS: u64 = 1_000;
const MAX_POLL_AFTER_MS: u64 = 24 * 60 * 60 * 1_000;

pub trait ProviderPollStore: Send + Sync + 'static {
    fn resolve_due_poll_deadline(
        &self,
        _scope: &ProviderTaskClaimScope,
    ) -> impl Future<Output = Result<Option<ProviderRemoteTask>, ProviderTaskStoreError>> + Send
    {
        async { Ok(None) }
    }

    fn native_batch_members(
        &self,
        _lease: &ProviderTaskLease,
    ) -> impl Future<Output = Result<Option<Vec<NativeBatchMember>>, ProviderTaskStoreError>> + Send
    {
        async { Ok(None) }
    }

    fn publish_native_batch_artifact(
        &self,
        _lease: &ProviderTaskLease,
        _output_index: i32,
        _authority: &ProviderArtifactAuthority,
    ) -> impl Future<Output = Result<ProviderArtifactPublication, ProviderTaskStoreError>> + Send
    {
        async { Err(ProviderTaskStoreError::InvalidInput) }
    }

    fn record_native_batch_failure(
        &self,
        _lease: &ProviderTaskLease,
        _output_index: i32,
        _error_code: &str,
    ) -> impl Future<Output = Result<(), ProviderTaskStoreError>> + Send {
        async { Err(ProviderTaskStoreError::InvalidInput) }
    }

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
    S: ProviderTaskStore + ProviderTaskDeadlineStore,
{
    async fn resolve_due_poll_deadline(
        &self,
        scope: &ProviderTaskClaimScope,
    ) -> Result<Option<ProviderRemoteTask>, ProviderTaskStoreError> {
        ProviderTaskDeadlineStore::resolve_due_remote_task_deadline(self, scope).await
    }

    async fn native_batch_members(
        &self,
        lease: &ProviderTaskLease,
    ) -> Result<Option<Vec<NativeBatchMember>>, ProviderTaskStoreError> {
        ProviderTaskStore::native_batch_members(self, lease).await
    }

    async fn publish_native_batch_artifact(
        &self,
        lease: &ProviderTaskLease,
        output_index: i32,
        authority: &ProviderArtifactAuthority,
    ) -> Result<ProviderArtifactPublication, ProviderTaskStoreError> {
        ProviderTaskStore::publish_native_batch_artifact(self, lease, output_index, authority).await
    }

    async fn record_native_batch_failure(
        &self,
        lease: &ProviderTaskLease,
        output_index: i32,
        error_code: &str,
    ) -> Result<(), ProviderTaskStoreError> {
        ProviderTaskStore::record_native_batch_failure(self, lease, output_index, error_code).await
    }

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
        if let Some(task) = self.store.resolve_due_poll_deadline(&self.scope).await? {
            return Ok(ProviderPollRun::Observed(task));
        }
        let claim_started_at = Instant::now();
        let Some(lease) = self
            .store
            .claim_poll(&self.scope, &self.owner, self.lease_ms)
            .await?
        else {
            return Ok(ProviderPollRun::Idle);
        };
        self.validate_lease(&lease)?;

        if lease.task.provider_id == "dreamina-cli"
            && let Some(members) = self.store.native_batch_members(&lease).await?
        {
            // The native CLI download future owns process capture buffers. Keep
            // those buffers off the unchanged single-output poll future's stack.
            return Box::pin(self.run_native_batch(lease, members, claim_started_at)).await;
        }

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

    async fn run_native_batch(
        &self,
        lease: ProviderTaskLease,
        members: Vec<NativeBatchMember>,
        claim_started_at: Instant,
    ) -> Result<ProviderPollRun, ProviderPollOrchestratorError> {
        if lease.task.provider_id != "dreamina-cli"
            || !(2..=10).contains(&members.len())
            || members.iter().enumerate().any(|(index, member)| {
                member.output_index != index as i32
                    || member.committed_artifact.is_some()
                        && member.committed_failure_code.is_some()
            })
            || members[0].submission_id != lease.task.submission_id
            || members[0].executor_execution_id != lease.task.executor_execution_id
        {
            return Err(ProviderPollOrchestratorError::InvalidLease);
        }
        if members.iter().all(|member| {
            member.committed_artifact.is_some() || member.committed_failure_code.is_some()
        }) {
            let publication = members[0]
                .committed_artifact
                .clone()
                .ok_or(ProviderPollOrchestratorError::InvalidLease)?;
            let task = self.record_artifact(&lease, publication).await?;
            return Ok(ProviderPollRun::Observed(task));
        }

        let call = ProviderPollDriverCall::new(&lease)
            .map_err(|_| ProviderPollOrchestratorError::InvalidLease)?;
        let mut sink = ControlledNativeBatchSink::new(
            &self.stagers,
            &members,
            &lease,
            std::sync::Arc::clone(&self.materialization_limit),
        );
        // Do not inline the CLI's streaming buffers into each heartbeat frame.
        let operation = Box::pin(
            self.driver
                .poll_native_batch(&call, members.len(), &mut sink),
        );
        let deadline = claim_started_at + Duration::from_millis(lease.remaining_budget_ms());
        let (renewed, result) =
            tokio::time::timeout_at(deadline, self.run_with_heartbeat(lease.clone(), operation))
                .await
                .map_err(|_| ProviderPollOrchestratorError::ProviderDeadlineElapsed)??;
        let task = match result {
            Ok(ProviderNativeBatchPollObservation::Pending { next_poll_after_ms })
                if sink.is_pristine() =>
            {
                self.record_waiting(
                    &renewed,
                    bounded_poll_after(next_poll_after_ms.unwrap_or(DEFAULT_PENDING_POLL_AFTER_MS)),
                )
                .await?
            }
            Ok(ProviderNativeBatchPollObservation::Failed(failure))
                if sink.is_pristine() && verified_terminal_failure(&failure) =>
            {
                self.record_failed(&renewed, failure.code()).await?
            }
            Ok(ProviderNativeBatchPollObservation::Completed { output_count }) => {
                let staged = match sink.finish(output_count) {
                    Ok(staged) => staged,
                    Err(_) => {
                        return Ok(ProviderPollRun::Observed(
                            self.record_uncertain(
                                &renewed,
                                "provider_native_batch_artifact_contract",
                            )
                            .await?,
                        ));
                    }
                };
                if members[output_count..]
                    .iter()
                    .any(|member| member.committed_artifact.is_some())
                {
                    return Ok(ProviderPollRun::Observed(
                        self.record_uncertain(&renewed, "provider_native_batch_result_changed")
                            .await?,
                    ));
                }
                let mut leader = members[0].committed_artifact.clone();
                let mut staged_leader = None;
                for (index, artifact) in staged {
                    if index == 0 {
                        staged_leader = Some(artifact);
                        continue;
                    }
                    let publication = self
                        .store
                        .publish_native_batch_artifact(&renewed, index as i32, artifact.authority())
                        .await?;
                    if !publication.matches_durable_manifest(artifact.manifest()) {
                        return Err(ProviderPollOrchestratorError::ArtifactContract(
                            ProviderArtifactSinkContractError::ManifestMismatch,
                        ));
                    }
                }
                for (index, member) in members.iter().enumerate().skip(output_count) {
                    if member.committed_failure_code.is_none() {
                        self.store
                            .record_native_batch_failure(
                                &renewed,
                                index as i32,
                                "dreamina_native_batch_result_missing",
                            )
                            .await?;
                    }
                }
                // A committed leader artifact is recoverable as terminal success.
                // Publish it only after every follower has durable result evidence.
                if let Some(artifact) = staged_leader {
                    let publication = self
                        .store
                        .publish_poll_artifact(&renewed, artifact.authority())
                        .await?;
                    if !publication.matches_durable_manifest(artifact.manifest()) {
                        return Err(ProviderPollOrchestratorError::ArtifactContract(
                            ProviderArtifactSinkContractError::ManifestMismatch,
                        ));
                    }
                    leader = Some(publication);
                }
                self.record_artifact(
                    &renewed,
                    leader.ok_or(ProviderPollOrchestratorError::InvalidLease)?,
                )
                .await?
            }
            Err(failure) => match retry_poll_after(&failure) {
                Some(delay) => self.record_waiting(&renewed, delay).await?,
                None => self.record_uncertain(&renewed, failure.code()).await?,
            },
            _ => {
                self.record_uncertain(&renewed, "provider_native_batch_artifact_contract")
                    .await?
            }
        };
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
            Ok(PollObservation::PendingWithProgress {
                next_poll_after_ms,
                progress,
            }) => {
                if sink.into_pristine().is_err() {
                    return self
                        .record_uncertain(lease, "provider_poll_artifact_contract")
                        .await;
                }
                let poll_after_ms =
                    bounded_poll_after(next_poll_after_ms.unwrap_or(DEFAULT_PENDING_POLL_AFTER_MS));
                if progress.validate().is_err() {
                    return self.record_waiting(lease, poll_after_ms).await;
                }
                let outcome = ProviderTaskObservationOutcome::Waiting { poll_after_ms };
                let observation = ProviderTaskObservation {
                    event_identity: event_identity(lease, &outcome),
                    source: ProviderTaskObservationSource::Poll,
                    outcome,
                    progress: Some(progress),
                };
                self.store
                    .record_poll_observation(lease, &observation)
                    .await
                    .map_err(Into::into)
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
            Ok(PollObservation::CompletedRemoteVideo(completed)) => {
                if sink.into_pristine().is_err() || lease.task.provider_id != "dreamina-cli" {
                    return self
                        .record_uncertain(lease, "provider_poll_remote_contract")
                        .await;
                }
                if !provider_request_matches(
                    lease,
                    completed.provider_request_id().map(|value| value.as_str()),
                ) {
                    return self
                        .record_uncertain(lease, "provider_poll_request_mismatch")
                        .await;
                }
                self.record(
                    lease,
                    ProviderTaskObservationOutcome::RemoteReady {
                        result: completed.result().clone(),
                    },
                )
                .await
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
            progress: None,
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
        ProviderTaskObservationOutcome::RemoteReady { .. } => "remote-video".to_owned(),
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
