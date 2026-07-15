use std::{error::Error, fmt};

use crate::{
    ArtifactSink, DurableArtifactManifest, InvocationContext, ProviderFailure, SingleOutputCommand,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OpaqueProviderId(String);

impl OpaqueProviderId {
    pub fn new(value: impl Into<String>) -> Result<Self, OpaqueProviderIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(OpaqueProviderIdError::Empty);
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
        {
            return Err(OpaqueProviderIdError::InvalidCharacter);
        }
        if value.len() > 255 {
            return Err(OpaqueProviderIdError::TooLong);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpaqueProviderIdError {
    Empty,
    InvalidCharacter,
    TooLong,
}

impl fmt::Display for OpaqueProviderIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("provider identifier must not be empty"),
            Self::InvalidCharacter => {
                formatter.write_str("provider identifier is not a durable opaque identifier")
            }
            Self::TooLong => formatter.write_str("provider identifier exceeds 255 bytes"),
        }
    }
}

impl Error for OpaqueProviderIdError {}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProviderRequestId(OpaqueProviderId);

impl ProviderRequestId {
    pub fn new(value: impl Into<String>) -> Result<Self, OpaqueProviderIdError> {
        OpaqueProviderId::new(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteOperationRef {
    provider_id: OpaqueProviderId,
    submission_id: OpaqueProviderId,
    operation_id: OpaqueProviderId,
}

impl RemoteOperationRef {
    pub fn new(
        provider_id: impl Into<String>,
        submission_id: impl Into<String>,
        operation_id: impl Into<String>,
    ) -> Result<Self, OpaqueProviderIdError> {
        Ok(Self {
            provider_id: OpaqueProviderId::new(provider_id)?,
            submission_id: OpaqueProviderId::new(submission_id)?,
            operation_id: OpaqueProviderId::new(operation_id)?,
        })
    }

    pub fn provider_id(&self) -> &str {
        self.provider_id.as_str()
    }

    pub fn submission_id(&self) -> &str {
        self.submission_id.as_str()
    }

    pub fn operation_id(&self) -> &str {
        self.operation_id.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Completed {
    artifact: DurableArtifactManifest,
    provider_request_id: Option<ProviderRequestId>,
}

impl Completed {
    pub fn new(
        artifact: DurableArtifactManifest,
        provider_request_id: Option<ProviderRequestId>,
    ) -> Self {
        Self {
            artifact,
            provider_request_id,
        }
    }

    pub fn artifact(&self) -> &DurableArtifactManifest {
        &self.artifact
    }

    pub fn provider_request_id(&self) -> Option<&ProviderRequestId> {
        self.provider_request_id.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingOperation {
    operation: RemoteOperationRef,
    provider_request_id: Option<ProviderRequestId>,
    next_poll_after_ms: Option<u64>,
}

impl PendingOperation {
    pub fn new(
        operation: RemoteOperationRef,
        provider_request_id: Option<ProviderRequestId>,
        next_poll_after_ms: Option<u64>,
    ) -> Self {
        Self {
            operation,
            provider_request_id,
            next_poll_after_ms,
        }
    }

    pub fn operation(&self) -> &RemoteOperationRef {
        &self.operation
    }

    pub fn provider_request_id(&self) -> Option<&ProviderRequestId> {
        self.provider_request_id.as_ref()
    }

    pub fn next_poll_after_ms(&self) -> Option<u64> {
        self.next_poll_after_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Submission {
    Completed(Completed),
    Pending(PendingOperation),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanceledEvidence {
    provider_request_id: Option<ProviderRequestId>,
}

impl CanceledEvidence {
    pub fn new(provider_request_id: Option<ProviderRequestId>) -> Self {
        Self {
            provider_request_id,
        }
    }

    pub fn provider_request_id(&self) -> Option<&ProviderRequestId> {
        self.provider_request_id.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PollObservation {
    Pending { next_poll_after_ms: Option<u64> },
    Completed(Completed),
    Failed(ProviderFailure),
    Canceled(CanceledEvidence),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelState {
    Accepted,
    AlreadyTerminal,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelReceipt {
    state: CancelState,
    provider_request_id: Option<ProviderRequestId>,
}

impl CancelReceipt {
    pub fn new(state: CancelState, provider_request_id: Option<ProviderRequestId>) -> Self {
        Self {
            state,
            provider_request_id,
        }
    }

    pub fn state(&self) -> CancelState {
        self.state
    }

    pub fn provider_request_id(&self) -> Option<&ProviderRequestId> {
        self.provider_request_id.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallbackKind {
    StateChanged,
    TerminalHint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallbackEnvelope<'a> {
    pub headers: &'a [(&'a str, &'a [u8])],
    pub body: &'a [u8],
    pub received_at_unix_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallbackReceipt {
    event_id: OpaqueProviderId,
    operation: RemoteOperationRef,
    kind: CallbackKind,
}

impl CallbackReceipt {
    pub fn new(
        event_id: OpaqueProviderId,
        operation: RemoteOperationRef,
        kind: CallbackKind,
    ) -> Self {
        Self {
            event_id,
            operation,
            kind,
        }
    }

    pub fn event_id(&self) -> &str {
        self.event_id.as_str()
    }

    pub fn operation(&self) -> &RemoteOperationRef {
        &self.operation
    }

    pub fn kind(&self) -> CallbackKind {
        self.kind
    }
}

pub trait RemoteTaskProvider: Sync {
    type Payload: Sync;

    fn submit<S: ArtifactSink>(
        &self,
        context: InvocationContext<'_>,
        command: &SingleOutputCommand<Self::Payload>,
        sink: &mut S,
    ) -> impl std::future::Future<Output = Result<Submission, ProviderFailure>> + Send;

    fn poll<S: ArtifactSink>(
        &self,
        context: InvocationContext<'_>,
        operation: &RemoteOperationRef,
        sink: &mut S,
    ) -> impl std::future::Future<Output = Result<PollObservation, ProviderFailure>> + Send;

    fn cancel(
        &self,
        context: InvocationContext<'_>,
        operation: &RemoteOperationRef,
    ) -> impl std::future::Future<Output = Result<CancelReceipt, ProviderFailure>> + Send;

    fn verify_callback(
        &self,
        envelope: CallbackEnvelope<'_>,
    ) -> Result<CallbackReceipt, ProviderFailure>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_receipt_ids_reject_urls_and_paths() {
        assert!(RemoteOperationRef::new("seedance", "submission-1", "task-1").is_ok());
        assert!(RemoteOperationRef::new("seedance", "submission-1", "https://temp").is_err());
        assert!(ProviderRequestId::new("/tmp/request").is_err());
    }
}
