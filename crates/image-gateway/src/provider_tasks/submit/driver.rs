use std::{path::Path, sync::Arc, time::Duration};

use image_provider_sdk::{
    CanonicalCommandPayload, EffectCertainty, InvocationContext, InvocationDeadline,
    PendingOperation, ProviderFailure, ProviderFailureClass, RemoteTaskProvider, RetryDirective,
    SingleOutputCommand, SubmitCall, SubmitIdempotency,
};
use uuid::Uuid;

use crate::{
    executor::ExecutorInputObject,
    provider_tasks::{
        ProviderExecutionContext, ProviderSubmitCredentialGuard, ProviderSubmitIntent,
        ProviderSubmitOrchestrationStore, ProviderTaskStoreError,
    },
};

pub trait ProviderSubmitDriver: Send + Sync + 'static {
    type Payload: CanonicalCommandPayload + Send + Sync + 'static;
    type Prepared: Send + 'static;

    fn provider_id(&self) -> &'static str;

    fn prepare(
        &self,
        call: &ProviderSubmitDriverCall<Self::Payload>,
    ) -> impl Future<Output = Result<Self::Prepared, ProviderFailure>> + Send;

    fn dispatch(
        &self,
        prepared: Self::Prepared,
        call: &ProviderSubmitDriverCall<Self::Payload>,
    ) -> impl Future<Output = Result<PendingOperation, ProviderFailure>> + Send;

    fn recover_released(
        &self,
        call: &ProviderSubmitDriverCall<Self::Payload>,
    ) -> impl Future<Output = ProviderSubmitDriverRecovery> + Send;
}

#[async_trait::async_trait]
trait SubmitCredentialGate: Send + Sync {
    async fn guard(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
    ) -> Result<ProviderSubmitCredentialGuard, ProviderTaskStoreError>;
}

#[async_trait::async_trait]
impl<S: ProviderSubmitOrchestrationStore> SubmitCredentialGate for S {
    async fn guard(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
    ) -> Result<ProviderSubmitCredentialGuard, ProviderTaskStoreError> {
        self.guard_submit_credentials(intent, context).await
    }
}

pub struct ProviderSubmitDriverCall<P> {
    submission_id: Arc<str>,
    intent: Arc<ProviderSubmitIntent>,
    context: Arc<ProviderExecutionContext>,
    command: Arc<SingleOutputCommand<P>>,
    inputs: Arc<[ExecutorInputObject]>,
    journal_root: Arc<Path>,
    launch_nonce: Uuid,
    remaining_budget_ms: u64,
    credential_gate: Option<Arc<dyn SubmitCredentialGate>>,
}

impl<P> Clone for ProviderSubmitDriverCall<P> {
    fn clone(&self) -> Self {
        Self {
            submission_id: Arc::clone(&self.submission_id),
            intent: Arc::clone(&self.intent),
            context: Arc::clone(&self.context),
            command: Arc::clone(&self.command),
            inputs: Arc::clone(&self.inputs),
            journal_root: Arc::clone(&self.journal_root),
            launch_nonce: self.launch_nonce,
            remaining_budget_ms: self.remaining_budget_ms,
            credential_gate: self.credential_gate.clone(),
        }
    }
}

impl<P> ProviderSubmitDriverCall<P> {
    pub(crate) fn new(
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        command: Arc<SingleOutputCommand<P>>,
        journal_root: Arc<Path>,
        launch_nonce: Uuid,
        remaining_budget_ms: u64,
    ) -> Self {
        Self {
            submission_id: Arc::from(intent.submission_id.to_string()),
            intent: Arc::new(intent.clone()),
            context: Arc::new(context.clone()),
            command,
            inputs: Arc::from([]),
            journal_root,
            launch_nonce,
            remaining_budget_ms,
            credential_gate: None,
        }
    }

    pub(crate) fn with_credential_gate<S: ProviderSubmitOrchestrationStore>(
        mut self,
        store: Arc<S>,
    ) -> Self {
        self.credential_gate = Some(store);
        self
    }

    pub(crate) async fn guard_credentials(
        &self,
    ) -> Result<ProviderSubmitCredentialGuard, ProviderTaskStoreError> {
        if self.intent.provider_id != image_provider_dreamina_cli::PROVIDER_ID {
            return Ok(ProviderSubmitCredentialGuard::unrestricted());
        }
        let gate = self
            .credential_gate
            .as_ref()
            .ok_or(ProviderTaskStoreError::Conflict)?;
        gate.guard(&self.intent, &self.context).await
    }

    pub(crate) fn with_inputs(mut self, inputs: Arc<[ExecutorInputObject]>) -> Self {
        self.inputs = inputs;
        self
    }

    pub fn inputs(&self) -> &[ExecutorInputObject] {
        &self.inputs
    }

    pub fn intent(&self) -> &ProviderSubmitIntent {
        &self.intent
    }

    pub fn execution_context(&self) -> &ProviderExecutionContext {
        &self.context
    }

    pub fn command(&self) -> &SingleOutputCommand<P> {
        &self.command
    }

    pub fn journal_root(&self) -> &Path {
        &self.journal_root
    }

    pub fn launch_nonce(&self) -> Uuid {
        self.launch_nonce
    }

    pub fn remaining_budget_ms(&self) -> u64 {
        self.remaining_budget_ms
    }

    pub(crate) fn with_remaining_budget_ms(&self, remaining_budget_ms: u64) -> Self {
        let mut call = self.clone();
        call.remaining_budget_ms = remaining_budget_ms;
        call
    }

    fn submit_call(&self) -> Result<SubmitCall<'_, P>, ProviderFailure> {
        let attempt = u32::try_from(self.context.invocation_attempt())
            .map_err(|_| invalid_context_failure())?;
        let provider_timeout_ms = u64::try_from(self.context.provider_timeout_ms())
            .map_err(|_| invalid_context_failure())?;
        let provider_deadline_unix_ms = u64::try_from(self.context.provider_deadline_at_ms())
            .map_err(|_| invalid_context_failure())?;
        let invocation = InvocationContext::new(
            &self.submission_id,
            &self.intent.provider_id,
            self.context.operation_id(),
            self.context.operation_descriptor_revision(),
            self.context.model(),
            attempt,
            InvocationDeadline::new(provider_timeout_ms, provider_deadline_unix_ms)
                .map_err(|_| invalid_context_failure())?,
        )
        .map_err(|_| invalid_context_failure())?;
        Ok(SubmitCall::new(
            invocation,
            &self.command,
            SubmitIdempotency::submission_bound(),
        ))
    }
}

pub enum ProviderSubmitDriverRecovery {
    AwaitingEvidence,
    Accepted(PendingOperation),
    Failed(ProviderFailure),
}

impl<P> ProviderSubmitDriver for P
where
    P: RemoteTaskProvider,
{
    type Payload = P::Payload;
    type Prepared = ();

    fn provider_id(&self) -> &'static str {
        RemoteTaskProvider::provider_id(self)
    }

    async fn prepare(
        &self,
        _call: &ProviderSubmitDriverCall<Self::Payload>,
    ) -> Result<Self::Prepared, ProviderFailure> {
        Ok(())
    }

    async fn dispatch(
        &self,
        (): Self::Prepared,
        call: &ProviderSubmitDriverCall<Self::Payload>,
    ) -> Result<PendingOperation, ProviderFailure> {
        if call.remaining_budget_ms == 0 {
            return Err(submit_deadline_elapsed_failure());
        }
        match tokio::time::timeout(
            Duration::from_millis(call.remaining_budget_ms),
            self.submit(call.submit_call()?),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(submit_timeout_failure()),
        }
    }

    async fn recover_released(
        &self,
        _call: &ProviderSubmitDriverCall<Self::Payload>,
    ) -> ProviderSubmitDriverRecovery {
        ProviderSubmitDriverRecovery::AwaitingEvidence
    }
}

fn invalid_context_failure() -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Permanent,
        "provider_submit_context_invalid",
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Never,
    )
    .expect("static provider failure must be valid")
}

fn submit_timeout_failure() -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Ambiguous,
        "provider_submit_timeout",
        EffectCertainty::UnknownRemoteEffect,
        RetryDirective::Never,
    )
    .expect("static provider failure must be valid")
}

fn submit_deadline_elapsed_failure() -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Permanent,
        "provider_submit_deadline_elapsed",
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Never,
    )
    .expect("static provider failure must be valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_budget_is_rejected_before_a_remote_effect() {
        let failure = submit_deadline_elapsed_failure();

        assert_eq!(failure.code(), "provider_submit_deadline_elapsed");
        assert_eq!(failure.effect(), EffectCertainty::NoRemoteEffect);
        assert_eq!(failure.class(), ProviderFailureClass::Permanent);
        assert_eq!(failure.retry(), RetryDirective::Never);
    }
}
