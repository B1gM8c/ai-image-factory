use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use image_provider_sdk::{
    EffectCertainty, InvocationContext, InvocationDeadline, PendingOperation, ProviderFailure,
    RemoteTaskProvider, SingleOutputCommand, SubmitCall, SubmitIdempotency,
};
use sha2::{Digest, Sha256};

use crate::executor::ExecutorSubmissionLease;

use super::{
    PostgresProviderTaskStore, ProviderExecutionContext, ProviderRemoteTask, ProviderSubmitAcquire,
    ProviderSubmitFailureKind, ProviderSubmitIntent, ProviderSubmitIntentState, ProviderTaskStore,
    ProviderTaskStoreError, RemoteTaskAttach, RemoteTaskQuarantinedReceipt,
    RemoteTaskSubmitFailure, RemoteTaskSubmitReceipt, RemoteTaskSubmitReservation,
    remote_submit::{
        RemoteSubmitJournal, RemoteSubmitJournalError, RemoteSubmitJournalObservation,
        RemoteSubmitJournalSpec, RemoteSubmitJournalTerminal, RemoteSubmitLaunch,
        RemoteSubmitRelease, RemoteSubmitReleasedAuthority,
    },
};

const MAX_POLL_AFTER_MS: u64 = 24 * 60 * 60 * 1_000;

pub struct ProviderSubmitWork<P: RemoteTaskProvider> {
    executor: ExecutorSubmissionLease,
    command: SingleOutputCommand<P::Payload>,
}

impl<P: RemoteTaskProvider> ProviderSubmitWork<P> {
    pub fn new(
        executor: &ExecutorSubmissionLease,
        command: SingleOutputCommand<P::Payload>,
    ) -> Result<Self, ProviderSubmitOrchestratorError> {
        RemoteSubmitJournal::validate_canonical_command(command.canonical_payload())
            .map_err(|_| ProviderSubmitOrchestratorError::InvalidWork)?;
        let output_index = i32::try_from(command.output().index())
            .map_err(|_| ProviderSubmitOrchestratorError::InvalidWork)?;
        if executor.output_index != output_index
            || executor.command_schema != command.schema_id()
            || executor.adapter_revision != command.adapter_revision()
            || executor.command_hash != command.source_command_sha256()
        {
            return Err(ProviderSubmitOrchestratorError::InvalidWork);
        }
        Ok(Self {
            executor: executor.clone(),
            command,
        })
    }

    fn reservation(&self, provider_timeout_ms: i64) -> RemoteTaskSubmitReservation {
        RemoteTaskSubmitReservation::new(
            &self.executor,
            format!("provider-submit-{}", self.executor.submission_id.simple()),
            self.command.output(),
            self.command.identity(),
            provider_timeout_ms,
        )
    }
}

pub struct ProviderSubmitOrchestrator<P: RemoteTaskProvider> {
    store: PostgresProviderTaskStore,
    provider: P,
    provider_timeout_ms: i64,
    journal: Arc<RemoteSubmitJournal>,
}

impl<P: RemoteTaskProvider> ProviderSubmitOrchestrator<P> {
    pub fn new(
        store: PostgresProviderTaskStore,
        provider: P,
        provider_timeout_ms: i64,
        journal_root: impl AsRef<Path>,
    ) -> Result<Self, ProviderSubmitOrchestratorError> {
        if provider_timeout_ms <= 0 {
            return Err(ProviderSubmitOrchestratorError::InvalidWork);
        }
        Ok(Self {
            store,
            provider,
            provider_timeout_ms,
            journal: Arc::new(RemoteSubmitJournal::new(journal_root)?),
        })
    }

    pub async fn submit(
        &self,
        work: ProviderSubmitWork<P>,
    ) -> Result<ProviderSubmitOutcome, ProviderSubmitOrchestratorError> {
        let reservation = work.reservation(self.provider_timeout_ms);
        match self.store.acquire_submit(&reservation).await? {
            ProviderSubmitAcquire::Dispatch(authority) => {
                let intent = authority.intent();
                let context = authority.context();
                if intent.provider_id != self.provider.provider_id()
                    || !context_matches_command(context, &work.command)
                {
                    return self
                        .record_failure(
                            intent,
                            context,
                            ProviderSubmitFailureKind::Rejected,
                            "provider_submit_context_mismatch",
                        )
                        .await;
                }
                self.dispatch_provider(intent, context, authority.remaining_budget_ms(), work)
                    .await
            }
            ProviderSubmitAcquire::AttachOnly(authority) => {
                self.attach_known(authority.intent(), authority.context(), 0)
                    .await
            }
            ProviderSubmitAcquire::Busy(authority) => {
                if authority.intent().provider_id != self.provider.provider_id()
                    || !context_matches_command(authority.context(), &work.command)
                {
                    return Ok(ProviderSubmitOutcome::AwaitingEvidence(
                        authority.intent().clone(),
                    ));
                }
                self.dispatch_provider(
                    authority.intent(),
                    authority.context(),
                    authority.remaining_budget_ms(),
                    work,
                )
                .await
            }
            ProviderSubmitAcquire::ObserveOnly(invocation) => {
                let spec = RemoteSubmitJournalSpec::new(
                    &invocation.intent,
                    invocation.context(),
                    work.command.canonical_payload(),
                )?;
                let journal = Arc::clone(&self.journal);
                let observation = tokio::task::spawn_blocking(move || {
                    journal.prepare(&spec, work.command.canonical_payload())?;
                    journal.observe(&spec)
                })
                .await
                .map_err(|_| ProviderSubmitOrchestratorError::JournalWorkerStopped)??;
                self.replay_journal_observation(
                    &invocation.intent,
                    invocation.context(),
                    observation,
                )
                .await
            }
            ProviderSubmitAcquire::Terminal(intent) => {
                if intent.state == ProviderSubmitIntentState::Attached
                    && let Some(task) = self.store.load(intent.submission_id).await?
                {
                    return Ok(ProviderSubmitOutcome::Attached(task));
                }
                Ok(ProviderSubmitOutcome::Terminal(intent))
            }
        }
    }

    async fn dispatch_provider(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        database_budget_ms: u64,
        work: ProviderSubmitWork<P>,
    ) -> Result<ProviderSubmitOutcome, ProviderSubmitOrchestratorError> {
        if database_budget_ms == 0 {
            return self
                .record_failure(
                    intent,
                    context,
                    ProviderSubmitFailureKind::Rejected,
                    "provider_submit_deadline_elapsed",
                )
                .await;
        }
        let submission_id = intent.submission_id.to_string();
        let attempt = u32::try_from(context.invocation_attempt())
            .map_err(|_| ProviderSubmitOrchestratorError::InvalidFrozenContext)?;
        let provider_timeout_ms = u64::try_from(context.provider_timeout_ms())
            .map_err(|_| ProviderSubmitOrchestratorError::InvalidFrozenContext)?;
        let provider_deadline_unix_ms = u64::try_from(context.provider_deadline_at_ms())
            .map_err(|_| ProviderSubmitOrchestratorError::InvalidFrozenContext)?;
        let invocation = InvocationContext::new(
            &submission_id,
            &intent.provider_id,
            context.operation_id(),
            context.operation_descriptor_revision(),
            context.model(),
            attempt,
            InvocationDeadline::new(provider_timeout_ms, provider_deadline_unix_ms)
                .map_err(|_| ProviderSubmitOrchestratorError::InvalidFrozenContext)?,
        )
        .map_err(|_| ProviderSubmitOrchestratorError::InvalidFrozenContext)?;
        let journal_spec =
            RemoteSubmitJournalSpec::new(intent, context, work.command.canonical_payload())?;
        let journal = Arc::clone(&self.journal);
        let journal_started = Instant::now();
        let (command, journal_spec, dispatch) = tokio::task::spawn_blocking(move || {
            let command = work.command;
            journal.prepare(&journal_spec, command.canonical_payload())?;
            let dispatch = match journal.commit_launch(&journal_spec)? {
                RemoteSubmitLaunch::Launch(launch) => {
                    match journal.release_dispatch(&journal_spec, launch)? {
                        RemoteSubmitRelease::Dispatch(released) => {
                            RemoteSubmitDispatch::Released(released)
                        }
                        RemoteSubmitRelease::Attach(observation) => {
                            RemoteSubmitDispatch::Attach(observation)
                        }
                    }
                }
                RemoteSubmitLaunch::Attach(observation) => {
                    RemoteSubmitDispatch::Attach(observation)
                }
            };
            Ok::<_, RemoteSubmitJournalError>((command, journal_spec, dispatch))
        })
        .await
        .map_err(|_| ProviderSubmitOrchestratorError::JournalWorkerStopped)??;
        let released = match dispatch {
            RemoteSubmitDispatch::Released(released) => released,
            RemoteSubmitDispatch::Attach(observation) => {
                return self
                    .replay_journal_observation(intent, context, observation)
                    .await;
            }
        };
        let remaining_budget_ms =
            remaining_budget_after(database_budget_ms, journal_started.elapsed());
        if remaining_budget_ms == 0 {
            let terminal = RemoteSubmitJournalTerminal::Rejected {
                error_code: "provider_submit_deadline_elapsed".to_owned(),
            };
            self.publish_journal_failure(journal_spec, released, terminal)
                .await?;
            return self
                .record_failure(
                    intent,
                    context,
                    ProviderSubmitFailureKind::Rejected,
                    "provider_submit_deadline_elapsed",
                )
                .await;
        }

        let submit = self.provider.submit(SubmitCall::new(
            invocation,
            &command,
            SubmitIdempotency::submission_bound(),
        ));
        match tokio::time::timeout(Duration::from_millis(remaining_budget_ms), submit).await {
            Ok(Ok(pending)) => {
                let pending = self
                    .publish_journal_accepted(journal_spec, released, pending)
                    .await?;
                self.record_pending(intent, context, pending).await
            }
            Ok(Err(failure)) => {
                let (kind, terminal) = journal_failure(&failure);
                self.publish_journal_failure(journal_spec, released, terminal)
                    .await?;
                self.record_failure(intent, context, kind, failure.code())
                    .await
            }
            Err(_) => {
                self.publish_journal_failure(
                    journal_spec,
                    released,
                    RemoteSubmitJournalTerminal::Unknown {
                        error_code: "provider_submit_timeout".to_owned(),
                    },
                )
                .await?;
                self.record_failure(
                    intent,
                    context,
                    ProviderSubmitFailureKind::OutcomeUnknown,
                    "provider_submit_timeout",
                )
                .await
            }
        }
    }

    async fn publish_journal_accepted(
        &self,
        spec: RemoteSubmitJournalSpec,
        released: RemoteSubmitReleasedAuthority,
        pending: PendingOperation,
    ) -> Result<PendingOperation, ProviderSubmitOrchestratorError> {
        let submission_id = spec.submission_id();
        let journal = Arc::clone(&self.journal);
        let (pending, result) = tokio::task::spawn_blocking(move || {
            let result = journal.publish_accepted(&spec, &released, &pending);
            (pending, result)
        })
        .await
        .map_err(|_| ProviderSubmitOrchestratorError::JournalWorkerStopped)?;
        tolerate_unavailable_journal(result, submission_id)?;
        Ok(pending)
    }

    async fn publish_journal_failure(
        &self,
        spec: RemoteSubmitJournalSpec,
        released: RemoteSubmitReleasedAuthority,
        terminal: RemoteSubmitJournalTerminal,
    ) -> Result<(), ProviderSubmitOrchestratorError> {
        let submission_id = spec.submission_id();
        let journal = Arc::clone(&self.journal);
        let result = tokio::task::spawn_blocking(move || {
            journal.publish_failure(&spec, &released, &terminal)
        })
        .await
        .map_err(|_| ProviderSubmitOrchestratorError::JournalWorkerStopped)?;
        tolerate_unavailable_journal(result, submission_id)
    }

    async fn replay_journal_observation(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        observation: RemoteSubmitJournalObservation,
    ) -> Result<ProviderSubmitOutcome, ProviderSubmitOrchestratorError> {
        match observation {
            RemoteSubmitJournalObservation::Terminal(RemoteSubmitJournalTerminal::Accepted(
                pending,
            )) => self.record_pending(intent, context, pending).await,
            RemoteSubmitJournalObservation::Terminal(RemoteSubmitJournalTerminal::Rejected {
                error_code,
            }) => {
                self.record_failure(
                    intent,
                    context,
                    ProviderSubmitFailureKind::Rejected,
                    &error_code,
                )
                .await
            }
            RemoteSubmitJournalObservation::Terminal(RemoteSubmitJournalTerminal::Unknown {
                error_code,
            }) => {
                self.record_failure(
                    intent,
                    context,
                    ProviderSubmitFailureKind::OutcomeUnknown,
                    &error_code,
                )
                .await
            }
            RemoteSubmitJournalObservation::Prepared
            | RemoteSubmitJournalObservation::LaunchCommitted
            | RemoteSubmitJournalObservation::DispatchReleased => {
                Ok(ProviderSubmitOutcome::AwaitingEvidence(intent.clone()))
            }
        }
    }

    async fn record_failure(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        kind: ProviderSubmitFailureKind,
        error_code: &str,
    ) -> Result<ProviderSubmitOutcome, ProviderSubmitOrchestratorError> {
        let event_identity = evidence_identity(
            "provider-submit-failure",
            &[
                context.execution_binding_sha256(),
                error_code,
                match kind {
                    ProviderSubmitFailureKind::Rejected => "rejected",
                    ProviderSubmitFailureKind::OutcomeUnknown => "outcome_unknown",
                },
            ],
        );
        let recorded = self
            .store
            .record_submit_failure(&RemoteTaskSubmitFailure {
                submission_id: intent.submission_id,
                executor_execution_id: intent.executor_execution_id,
                executor_owner: intent.submit_owner.clone(),
                executor_lease_epoch: intent.submit_lease_epoch,
                kind,
                event_identity,
                error_code: error_code.to_owned(),
                execution_binding_sha256: context.execution_binding_sha256().to_owned(),
                recovery_fence: None,
            })
            .await?;
        Ok(match kind {
            ProviderSubmitFailureKind::Rejected => ProviderSubmitOutcome::Terminal(recorded),
            ProviderSubmitFailureKind::OutcomeUnknown => {
                ProviderSubmitOutcome::AwaitingEvidence(recorded)
            }
        })
    }

    async fn record_pending(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        pending: PendingOperation,
    ) -> Result<ProviderSubmitOutcome, ProviderSubmitOrchestratorError> {
        let operation = pending.operation();
        if operation.provider_id() != intent.provider_id
            || operation.submission_id() != intent.submission_id.to_string()
        {
            let provider_request_id = pending
                .provider_request_id()
                .map(|request| request.as_str().to_owned());
            let event_identity = evidence_identity(
                "provider-submit-quarantined-receipt",
                &[
                    context.execution_binding_sha256(),
                    &intent.provider_id,
                    operation.provider_id(),
                    operation.submission_id(),
                    operation.operation_id(),
                    provider_request_id.as_deref().unwrap_or(""),
                ],
            );
            let recorded = self
                .store
                .quarantine_submit_receipt(&RemoteTaskQuarantinedReceipt {
                    submission_id: intent.submission_id,
                    executor_execution_id: intent.executor_execution_id,
                    executor_owner: intent.submit_owner.clone(),
                    executor_lease_epoch: intent.submit_lease_epoch,
                    event_identity,
                    expected_provider_id: intent.provider_id.clone(),
                    observed_provider_id: operation.provider_id().to_owned(),
                    observed_submission_id: operation.submission_id().to_owned(),
                    remote_operation_id: operation.operation_id().to_owned(),
                    provider_request_id,
                    reason: "provider_submit_receipt_mismatch".to_owned(),
                    execution_binding_sha256: context.execution_binding_sha256().to_owned(),
                })
                .await?;
            return Ok(ProviderSubmitOutcome::AwaitingEvidence(recorded));
        }
        let provider_request_id = pending
            .provider_request_id()
            .map(|request| request.as_str().to_owned());
        let receipt_event = evidence_identity(
            "provider-submit-receipt",
            &[
                context.execution_binding_sha256(),
                operation.operation_id(),
                provider_request_id.as_deref().unwrap_or(""),
            ],
        );
        let recorded = self
            .store
            .record_submit_receipt(&RemoteTaskSubmitReceipt {
                submission_id: intent.submission_id,
                executor_execution_id: intent.executor_execution_id,
                executor_owner: intent.submit_owner.clone(),
                executor_lease_epoch: intent.submit_lease_epoch,
                remote_operation_id: operation.operation_id().to_owned(),
                provider_request_id,
                event_identity: receipt_event,
                execution_binding_sha256: context.execution_binding_sha256().to_owned(),
            })
            .await?;
        let poll_after_ms = pending
            .next_poll_after_ms()
            .unwrap_or(0)
            .min(MAX_POLL_AFTER_MS) as i64;
        self.attach_known(&recorded, context, poll_after_ms).await
    }

    async fn attach_known(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        poll_after_ms: i64,
    ) -> Result<ProviderSubmitOutcome, ProviderSubmitOrchestratorError> {
        let remote_operation_id = intent
            .remote_operation_id
            .as_deref()
            .ok_or(ProviderSubmitOrchestratorError::InvalidFrozenContext)?;
        let event_identity = evidence_identity(
            "provider-submit-attach",
            &[context.execution_binding_sha256(), remote_operation_id],
        );
        let task = self
            .store
            .attach(&RemoteTaskAttach {
                submission_id: intent.submission_id,
                executor_execution_id: intent.executor_execution_id,
                executor_owner: intent.submit_owner.clone(),
                executor_lease_epoch: intent.submit_lease_epoch,
                remote_operation_id: remote_operation_id.to_owned(),
                provider_request_id: intent.provider_request_id.clone(),
                event_identity,
                execution_binding_sha256: context.execution_binding_sha256().to_owned(),
                poll_after_ms,
                recovery_fence: None,
            })
            .await?;
        Ok(ProviderSubmitOutcome::Attached(task))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSubmitOutcome {
    Attached(ProviderRemoteTask),
    AwaitingEvidence(ProviderSubmitIntent),
    Terminal(ProviderSubmitIntent),
}

enum RemoteSubmitDispatch {
    Released(RemoteSubmitReleasedAuthority),
    Attach(RemoteSubmitJournalObservation),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderSubmitOrchestratorError {
    #[error("provider submit work is invalid")]
    InvalidWork,
    #[error("frozen provider submit context is invalid")]
    InvalidFrozenContext,
    #[error("remote submit journal input is invalid")]
    JournalInvalidInput,
    #[error("remote submit journal conflicts with durable state")]
    JournalConflict,
    #[error("remote submit journal integrity validation failed")]
    JournalIntegrity,
    #[error("remote submit journal storage is unavailable")]
    JournalUnavailable,
    #[error("remote submit journal worker stopped unexpectedly")]
    JournalWorkerStopped,
    #[error(transparent)]
    Store(#[from] ProviderTaskStoreError),
}

impl From<RemoteSubmitJournalError> for ProviderSubmitOrchestratorError {
    fn from(error: RemoteSubmitJournalError) -> Self {
        match error {
            RemoteSubmitJournalError::InvalidInput => Self::JournalInvalidInput,
            RemoteSubmitJournalError::Conflict => Self::JournalConflict,
            RemoteSubmitJournalError::Integrity | RemoteSubmitJournalError::NotFound => {
                Self::JournalIntegrity
            }
            RemoteSubmitJournalError::Unavailable => Self::JournalUnavailable,
        }
    }
}

fn journal_failure(
    failure: &ProviderFailure,
) -> (ProviderSubmitFailureKind, RemoteSubmitJournalTerminal) {
    match failure.effect() {
        EffectCertainty::NoRemoteEffect => (
            ProviderSubmitFailureKind::Rejected,
            RemoteSubmitJournalTerminal::Rejected {
                error_code: failure.code().to_owned(),
            },
        ),
        EffectCertainty::UnknownRemoteEffect => (
            ProviderSubmitFailureKind::OutcomeUnknown,
            RemoteSubmitJournalTerminal::Unknown {
                error_code: failure.code().to_owned(),
            },
        ),
    }
}

fn tolerate_unavailable_journal(
    result: Result<(), RemoteSubmitJournalError>,
    submission_id: uuid::Uuid,
) -> Result<(), ProviderSubmitOrchestratorError> {
    match result {
        Ok(()) => Ok(()),
        Err(RemoteSubmitJournalError::Unavailable) => {
            tracing::warn!(
                %submission_id,
                "remote submit journal unavailable; attempting PostgreSQL result fallback"
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn remaining_budget_after(database_budget_ms: u64, elapsed: Duration) -> u64 {
    let elapsed_ms = u64::try_from(elapsed.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX);
    database_budget_ms.saturating_sub(elapsed_ms)
}

fn context_matches_command<P>(
    context: &ProviderExecutionContext,
    command: &SingleOutputCommand<P>,
) -> bool
where
    P: image_provider_sdk::CanonicalCommandPayload,
{
    context.command_schema() == command.schema_id()
        && context.adapter_revision() == command.adapter_revision()
        && context.provider_command_sha256() == hex::encode(command.canonical_sha256())
        && context.completion_mode() == "remote_task"
        && context.idempotency_mode() == "submission_bound"
        && context.operation_binding_version() == 2
        && context.invocation_attempt() > 0
        && context.provider_timeout_ms() > 0
        && context.provider_deadline_at_ms() > 0
}

fn evidence_identity(prefix: &str, values: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"ai-image-factory/provider-submit-event/v1\0");
    for value in values {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    format!("{prefix}:{}", hex::encode(digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_elapsed_time_is_subtracted_from_database_budget() {
        assert_eq!(remaining_budget_after(500, Duration::from_millis(125)), 375);
        assert_eq!(remaining_budget_after(500, Duration::from_micros(1)), 499);
        assert_eq!(remaining_budget_after(500, Duration::from_millis(500)), 0);
        assert_eq!(remaining_budget_after(500, Duration::from_millis(750)), 0);
    }

    #[test]
    fn only_storage_unavailability_allows_database_fallback() {
        let submission_id = uuid::Uuid::new_v4();
        assert!(
            tolerate_unavailable_journal(
                Err(RemoteSubmitJournalError::Unavailable),
                submission_id,
            )
            .is_ok()
        );
        assert!(matches!(
            tolerate_unavailable_journal(Err(RemoteSubmitJournalError::Integrity), submission_id,),
            Err(ProviderSubmitOrchestratorError::JournalIntegrity)
        ));
    }
}
