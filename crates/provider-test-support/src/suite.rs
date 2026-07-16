use std::{error::Error, fmt};

use image_provider_sdk::{
    ArtifactSink, Completed, InvocationContext, PollObservation, ProviderFailure,
    RemoteOperationRef, RemoteTaskProvider, SingleOutputCommand, Submission, SubmitIdempotency,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConformanceError {
    Provider(ProviderFailure),
    TerminalFailure(ProviderFailure),
    Canceled,
    PollLimitExceeded,
    SubmissionMismatch,
}

impl fmt::Display for ConformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider(error) => write!(formatter, "provider call failed: {error}"),
            Self::TerminalFailure(error) => write!(formatter, "remote task failed: {error}"),
            Self::Canceled => formatter.write_str("remote task was canceled"),
            Self::PollLimitExceeded => formatter.write_str("remote task exceeded poll limit"),
            Self::SubmissionMismatch => {
                formatter.write_str("remote operation belongs to another submission")
            }
        }
    }
}

impl Error for ConformanceError {}

pub async fn drive_remote_to_completion<P, S>(
    provider: &P,
    context: InvocationContext<'_>,
    idempotency: SubmitIdempotency<'_>,
    command: &SingleOutputCommand<P::Payload>,
    sink: &mut S,
    max_polls: usize,
) -> Result<Completed, ConformanceError>
where
    P: RemoteTaskProvider,
    S: ArtifactSink,
{
    let pending = match provider
        .submit(context, idempotency, command, sink)
        .await
        .map_err(ConformanceError::Provider)?
    {
        Submission::Completed(completed) => return Ok(completed),
        Submission::Pending(pending) => pending,
    };

    if pending.operation().submission_id() != context.submission_id() {
        return Err(ConformanceError::SubmissionMismatch);
    }

    drive_existing_operation(provider, context, pending.operation(), sink, max_polls).await
}

pub async fn drive_existing_operation<P, S>(
    provider: &P,
    context: InvocationContext<'_>,
    operation: &RemoteOperationRef,
    sink: &mut S,
    max_polls: usize,
) -> Result<Completed, ConformanceError>
where
    P: RemoteTaskProvider,
    S: ArtifactSink,
{
    if operation.submission_id() != context.submission_id() {
        return Err(ConformanceError::SubmissionMismatch);
    }
    for _ in 0..max_polls {
        match provider
            .poll(context, operation, sink)
            .await
            .map_err(ConformanceError::Provider)?
        {
            PollObservation::Pending { .. } => {}
            PollObservation::Completed(completed) => return Ok(completed),
            PollObservation::Failed(error) => {
                return Err(ConformanceError::TerminalFailure(error));
            }
            PollObservation::Canceled(_) => return Err(ConformanceError::Canceled),
        }
    }

    Err(ConformanceError::PollLimitExceeded)
}
