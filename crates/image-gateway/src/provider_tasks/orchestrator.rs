use std::time::Duration;

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
}

impl<P: RemoteTaskProvider> ProviderSubmitOrchestrator<P> {
    pub fn new(
        store: PostgresProviderTaskStore,
        provider: P,
        provider_timeout_ms: i64,
    ) -> Result<Self, ProviderSubmitOrchestratorError> {
        if provider_timeout_ms <= 0 {
            return Err(ProviderSubmitOrchestratorError::InvalidWork);
        }
        Ok(Self {
            store,
            provider,
            provider_timeout_ms,
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

                if authority.remaining_budget_ms() == 0 {
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
                    &work.command,
                    SubmitIdempotency::submission_bound(),
                ));
                match tokio::time::timeout(
                    Duration::from_millis(authority.remaining_budget_ms()),
                    submit,
                )
                .await
                {
                    Ok(Ok(pending)) => self.record_pending(intent, context, pending).await,
                    Ok(Err(failure)) => {
                        self.record_provider_failure(intent, context, failure).await
                    }
                    Err(_) => {
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
            ProviderSubmitAcquire::AttachOnly(authority) => {
                self.attach_known(authority.intent(), authority.context(), 0)
                    .await
            }
            ProviderSubmitAcquire::Busy(intent) | ProviderSubmitAcquire::ObserveOnly(intent) => {
                Ok(ProviderSubmitOutcome::AwaitingEvidence(intent))
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

    async fn record_provider_failure(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        failure: ProviderFailure,
    ) -> Result<ProviderSubmitOutcome, ProviderSubmitOrchestratorError> {
        let kind = match failure.effect() {
            EffectCertainty::NoRemoteEffect => ProviderSubmitFailureKind::Rejected,
            EffectCertainty::UnknownRemoteEffect => ProviderSubmitFailureKind::OutcomeUnknown,
        };
        self.record_failure(intent, context, kind, failure.code())
            .await
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderSubmitOrchestratorError {
    #[error("provider submit work is invalid")]
    InvalidWork,
    #[error("frozen provider submit context is invalid")]
    InvalidFrozenContext,
    #[error(transparent)]
    Store(#[from] ProviderTaskStoreError),
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
