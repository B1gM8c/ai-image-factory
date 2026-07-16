use image_provider_sdk::{
    ArtifactSink, InvocationContext, InvocationDeadline, PollObservation, ProviderFailure,
    RemoteOperationRef, RemoteTaskProvider,
};

use super::super::{ProviderExecutionContext, ProviderTaskLease, ProviderTaskState};

pub struct ProviderPollDriverCall {
    submission_id: String,
    provider_id: String,
    operation: RemoteOperationRef,
    context: ProviderExecutionContext,
}

impl ProviderPollDriverCall {
    pub(crate) fn new(lease: &ProviderTaskLease) -> Result<Self, ProviderPollDriverCallError> {
        let context = lease.context().clone();
        let submission_id = lease.task.submission_id.to_string();
        if lease.task.state != ProviderTaskState::ProviderWaiting
            || context.completion_mode() != "remote_task"
            || context.provider_deadline_at_ms() <= 0
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
            operation,
            context,
        })
    }

    pub fn execution_context(&self) -> &ProviderExecutionContext {
        &self.context
    }

    pub fn operation(&self) -> &RemoteOperationRef {
        &self.operation
    }

    fn invocation_context(&self) -> Result<InvocationContext<'_>, ProviderPollDriverCallError> {
        let attempt = u32::try_from(self.context.invocation_attempt())
            .map_err(|_| ProviderPollDriverCallError)?;
        let timeout = u64::try_from(self.context.provider_timeout_ms())
            .map_err(|_| ProviderPollDriverCallError)?;
        let deadline = u64::try_from(self.context.provider_deadline_at_ms())
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
