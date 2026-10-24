use image_provider_sdk::{
    ArtifactSink, InvocationContext, InvocationDeadline, PollObservation, ProviderFailure,
    ProviderTaskProgress, RemoteOperationRef, RemoteTaskProvider,
};
use uuid::Uuid;

use super::super::{ProviderExecutionContext, ProviderTaskLease, ProviderTaskState};

/// A native batch selects each output before streaming its bytes. Finalizing
/// releases that output's staging capacity before the next output is selected.
pub trait ProviderNativeBatchArtifactSink: ArtifactSink {
    fn select_output(
        &mut self,
        output_index: usize,
    ) -> Result<(), image_provider_sdk::ArtifactSinkError>;
}

#[derive(Debug)]
pub enum ProviderNativeBatchPollObservation {
    Pending {
        next_poll_after_ms: Option<u64>,
    },
    PendingWithProgress {
        next_poll_after_ms: Option<u64>,
        progress: ProviderTaskProgress,
    },
    Completed {
        output_count: usize,
    },
    Failed(ProviderFailure),
}

pub struct ProviderPollDriverCall {
    submission_id: String,
    provider_id: String,
    provider_account_id: Uuid,
    operation: RemoteOperationRef,
    context: ProviderExecutionContext,
    completion_deadline_at_ms: i64,
}

impl ProviderPollDriverCall {
    pub(crate) fn new(lease: &ProviderTaskLease) -> Result<Self, ProviderPollDriverCallError> {
        let context = lease.context().clone();
        let submission_id = lease.task.submission_id.to_string();
        if lease.task.state != ProviderTaskState::ProviderWaiting
            || context.completion_mode() != "remote_task"
            || context.provider_deadline_at_ms() <= 0
            || lease.completion_deadline_at_ms() < context.provider_deadline_at_ms()
        {
            return Err(ProviderPollDriverCallError);
        }
        let operation = RemoteOperationRef::new(
            lease.task.provider_id.clone(),
            submission_id.clone(),
            lease.task.remote_operation_id.clone(),
        )
        .map_err(|_| ProviderPollDriverCallError)?;
        Ok(Self {
            submission_id,
            provider_id: lease.task.provider_id.clone(),
            provider_account_id: lease.task.provider_account_id,
            operation,
            context,
            completion_deadline_at_ms: lease.completion_deadline_at_ms(),
        })
    }

    pub fn execution_context(&self) -> &ProviderExecutionContext {
        &self.context
    }

    pub fn operation(&self) -> &RemoteOperationRef {
        &self.operation
    }

    pub fn provider_account_id(&self) -> Uuid {
        self.provider_account_id
    }

    fn invocation_context(&self) -> Result<InvocationContext<'_>, ProviderPollDriverCallError> {
        let attempt = u32::try_from(self.context.invocation_attempt())
            .map_err(|_| ProviderPollDriverCallError)?;
        let timeout = u64::try_from(self.context.provider_timeout_ms())
            .map_err(|_| ProviderPollDriverCallError)?;
        let deadline = u64::try_from(self.completion_deadline_at_ms)
            .map_err(|_| ProviderPollDriverCallError)?;
        InvocationContext::new(
            &self.submission_id,
            &self.provider_id,
            self.context.operation_id(),
            self.context.operation_descriptor_revision(),
            self.context.model(),
            attempt,
            InvocationDeadline::new(timeout, deadline).map_err(|_| ProviderPollDriverCallError)?,
        )
        .map_err(|_| ProviderPollDriverCallError)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderPollDriverCallError;

pub trait ProviderPollDriver: Send + Sync + 'static {
    fn provider_id(&self) -> &'static str;

    fn poll<S: ArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> impl Future<Output = Result<PollObservation, ProviderFailure>> + Send;

    fn poll_native_batch<S: ProviderNativeBatchArtifactSink>(
        &self,
        _call: &ProviderPollDriverCall,
        _output_count: usize,
        _sink: &mut S,
    ) -> impl Future<Output = Result<ProviderNativeBatchPollObservation, ProviderFailure>> + Send
    {
        async { Err(invalid_poll_context()) }
    }
}

impl<P> ProviderPollDriver for P
where
    P: RemoteTaskProvider,
{
    fn provider_id(&self) -> &'static str {
        RemoteTaskProvider::provider_id(self)
    }

    async fn poll<S: ArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        let context = call
            .invocation_context()
            .map_err(|_| invalid_poll_context())?;
        RemoteTaskProvider::poll(self, context, call.operation(), sink).await
    }
}

fn invalid_poll_context() -> ProviderFailure {
    ProviderFailure::new(
        image_provider_sdk::ProviderFailureClass::Permanent,
        "provider_poll_context_invalid",
        image_provider_sdk::EffectCertainty::NoRemoteEffect,
        image_provider_sdk::RetryDirective::Never,
    )
    .expect("static provider failure must be valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_invocation_uses_completion_budget_and_keeps_submit_context_frozen() {
        let mut lease = super::super::tests::lease(None);
        lease.context.provider_deadline_at_ms = 1;
        lease.completion_deadline_at_ms = 86_400_001;
        let call = ProviderPollDriverCall::new(&lease).unwrap();
        assert_eq!(call.execution_context().provider_deadline_at_ms(), 1);
        let invocation = call.invocation_context().unwrap();
        assert_eq!(invocation.provider_deadline_unix_ms(), 86_400_001);
        assert_eq!(
            invocation.provider_timeout_ms(),
            lease.context.provider_timeout_ms() as u64,
        );
        lease.completion_deadline_at_ms = 0;
        assert!(ProviderPollDriverCall::new(&lease).is_err());
    }
}
