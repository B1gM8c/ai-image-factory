use std::{error::Error, fmt};

use crate::{
    ArtifactSink, DurableArtifactManifest, InvocationContext, ProviderFailure, SubmitCall,
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

/// Provider-hosted video delivery. This is URL evidence, not a durable media manifest.
#[derive(Clone, Eq, PartialEq)]
pub struct RemoteVideoResult {
    video_url: String,
    expires_at_ms: Option<i64>,
}

impl RemoteVideoResult {
    pub fn new(
        video_url: String,
        expires_at_ms: Option<i64>,
    ) -> Result<Self, RemoteVideoResultError> {
        if video_url.is_empty()
            || video_url.len() > 8_192
            || video_url
                .chars()
                .any(|value| value.is_control() || value.is_whitespace())
            || video_url.contains('\\')
        {
            return Err(RemoteVideoResultError);
        }
        let authority = video_url
            .get(8..)
            .filter(|_| {
                video_url
                    .get(..8)
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
            })
            .and_then(|remainder| remainder.split(['/', '?', '#']).next())
            .ok_or(RemoteVideoResultError)?;
        if authority.contains('@') {
            return Err(RemoteVideoResultError);
        }
        let parsed = url::Url::parse(&video_url).map_err(|_| RemoteVideoResultError)?;
        if parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
            || expires_at_ms.is_some_and(|value| value <= 0)
        {
            return Err(RemoteVideoResultError);
        }
        Ok(Self {
            video_url,
            expires_at_ms,
        })
    }

    pub fn video_url(&self) -> &str {
        &self.video_url
    }
    pub fn expires_at_ms(&self) -> Option<i64> {
        self.expires_at_ms
    }
}

impl fmt::Debug for RemoteVideoResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteVideoResult")
            .field("video_url", &"[redacted]")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteVideoResultError;

impl fmt::Display for RemoteVideoResultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "remote video result requires a bounded HTTPS URL and a valid optional expiry",
        )
    }
}
impl Error for RemoteVideoResultError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedRemoteVideo {
    result: RemoteVideoResult,
    provider_request_id: Option<ProviderRequestId>,
}

impl CompletedRemoteVideo {
    pub fn new(result: RemoteVideoResult, provider_request_id: Option<ProviderRequestId>) -> Self {
        Self {
            result,
            provider_request_id,
        }
    }
    pub fn result(&self) -> &RemoteVideoResult {
        &self.result
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
pub struct ProviderTaskProgress {
    pub provider_status: String,
    pub queue_position: Option<u64>,
    pub queue_total: Option<u64>,
    pub estimated_remaining_seconds: Option<u64>,
}

impl ProviderTaskProgress {
    pub fn validate(&self) -> Result<(), ProviderTaskProgressError> {
        if self.provider_status.is_empty()
            || self.provider_status.len() > 64
            || !self
                .provider_status
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(ProviderTaskProgressError::InvalidStatus);
        }
        if self
            .queue_position
            .is_some_and(|value| value > u32::MAX as u64)
            || self
                .queue_total
                .is_some_and(|value| value > u32::MAX as u64)
            || matches!((self.queue_position, self.queue_total), (Some(position), Some(total)) if position > total)
        {
            return Err(ProviderTaskProgressError::InvalidQueue);
        }
        if self
            .estimated_remaining_seconds
            .is_some_and(|value| value > 30 * 24 * 60 * 60)
        {
            return Err(ProviderTaskProgressError::InvalidEstimate);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderTaskProgressError {
    InvalidStatus,
    InvalidQueue,
    InvalidEstimate,
}

impl fmt::Display for ProviderTaskProgressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("provider task progress is invalid")
    }
}

impl Error for ProviderTaskProgressError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PollObservation {
    Pending {
        next_poll_after_ms: Option<u64>,
    },
    PendingWithProgress {
        next_poll_after_ms: Option<u64>,
        progress: ProviderTaskProgress,
    },
    Completed(Completed),
    CompletedRemoteVideo(CompletedRemoteVideo),
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

pub trait RemoteTaskProvider: Send + Sync + 'static {
    type Payload: crate::CanonicalCommandPayload + Send + Sync + 'static;

    fn provider_id(&self) -> &'static str;

    fn submit(
        &self,
        call: SubmitCall<'_, Self::Payload>,
    ) -> impl std::future::Future<Output = Result<PendingOperation, ProviderFailure>> + Send;

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

    #[test]
    fn progress_preserves_provider_numbers_and_optional_unknowns() {
        let progress = ProviderTaskProgress {
            provider_status: "Queueing".to_owned(),
            queue_position: Some(3341),
            queue_total: Some(476_961),
            estimated_remaining_seconds: Some(27_131),
        };
        assert!(progress.validate().is_ok());
        assert!(
            ProviderTaskProgress {
                queue_position: Some(0),
                estimated_remaining_seconds: None,
                ..progress.clone()
            }
            .validate()
            .is_ok()
        );
        assert!(
            ProviderTaskProgress {
                queue_position: None,
                queue_total: None,
                estimated_remaining_seconds: None,
                ..progress
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn progress_rejects_unbounded_or_inconsistent_metadata() {
        let progress = ProviderTaskProgress {
            provider_status: "Queueing".to_owned(),
            queue_position: Some(3),
            queue_total: Some(10),
            estimated_remaining_seconds: Some(60),
        };
        for provider_status in ["", "Queueing\n", "https://example.test", &"a".repeat(65)] {
            assert_eq!(
                ProviderTaskProgress {
                    provider_status: provider_status.to_owned(),
                    ..progress.clone()
                }
                .validate(),
                Err(ProviderTaskProgressError::InvalidStatus)
            );
        }
        assert_eq!(
            ProviderTaskProgress {
                queue_position: Some(11),
                ..progress.clone()
            }
            .validate(),
            Err(ProviderTaskProgressError::InvalidQueue)
        );
        assert_eq!(
            ProviderTaskProgress {
                queue_total: Some(u64::MAX),
                ..progress.clone()
            }
            .validate(),
            Err(ProviderTaskProgressError::InvalidQueue)
        );
        assert_eq!(
            ProviderTaskProgress {
                estimated_remaining_seconds: Some(u64::MAX),
                ..progress
            }
            .validate(),
            Err(ProviderTaskProgressError::InvalidEstimate)
        );
    }
}

#[cfg(test)]
mod remote_video_tests {
    use super::*;

    #[test]
    fn remote_video_preserves_signed_url_without_claiming_expiry() {
        let url = "https://media.example.test/video.mp4?signature=private%2Ftoken&x=1";
        let result = RemoteVideoResult::new(url.to_owned(), None).unwrap();
        assert_eq!(result.video_url(), url);
        assert_eq!(result.expires_at_ms(), None);
        assert!(!format!("{result:?}").contains("private"));
        assert!(!format!("{result:?}").contains("media.example"));
    }

    #[test]
    fn remote_video_rejects_unsafe_or_ambiguous_urls_and_expiry() {
        for url in [
            "http://example.test/a",
            "https:example.test/a",
            "https://u:p@example.test/a",
            "https://@example.test/a",
            "https://example.test/a#fragment",
            "https://example.test/a\n",
            "https://example.test/a b",
            "https://",
        ] {
            assert!(
                RemoteVideoResult::new(url.to_owned(), None).is_err(),
                "accepted {url:?}"
            );
        }
        assert!(
            RemoteVideoResult::new(format!("https://example.test/{}", "a".repeat(8192)), None)
                .is_err()
        );
        assert!(RemoteVideoResult::new("https://example.test/a".to_owned(), Some(0)).is_err());
        assert!(RemoteVideoResult::new("https://example.test/a".to_owned(), Some(-1)).is_err());
    }
}
