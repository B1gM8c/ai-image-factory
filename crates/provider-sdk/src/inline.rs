use crate::{ArtifactSink, Completed, InvocationContext, ProviderFailure, SingleOutputCommand};

pub trait InlineProvider: Sync {
    type Payload: Sync;

    fn execute<S: ArtifactSink>(
        &self,
        context: InvocationContext<'_>,
        command: &SingleOutputCommand<Self::Payload>,
        sink: &mut S,
    ) -> impl std::future::Future<Output = Result<Completed, ProviderFailure>> + Send;
}
