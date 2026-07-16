#![forbid(unsafe_code)]

pub mod artifact;
pub mod command;
pub mod failure;
pub mod inline;
pub mod remote;

pub use artifact::{
    ArtifactMetadata, ArtifactSink, ArtifactSinkError, ArtifactSinkErrorKind,
    DurableArtifactManifest, DurableArtifactManifestError, DurableArtifactRef,
    DurableArtifactRefError,
};
pub use command::{
    CanonicalCommandPayload, CommandIdentityError, InvocationContext, InvocationDeadline,
    OutputSlot, OutputSlotError, ProviderCommandIdentity, SingleOutputCommand, SubmitCall,
    SubmitIdempotency,
};
pub use failure::{
    EffectCertainty, ProviderFailure, ProviderFailureClass, ProviderFailureValidationError,
    RetryDirective,
};

pub use inline::InlineProvider;
pub use remote::{
    CallbackEnvelope, CallbackKind, CallbackReceipt, CancelReceipt, CancelState, CanceledEvidence,
    Completed, OpaqueProviderId, OpaqueProviderIdError, PendingOperation, PollObservation,
    ProviderRequestId, RemoteOperationRef, RemoteTaskProvider,
};

#[cfg(test)]
mod semantic_tests {
    use super::*;

    #[test]
    fn unknown_remote_effect_is_never_automatically_retryable() {
        assert!(
            ProviderFailure::new(
                ProviderFailureClass::Ambiguous,
                "submit_effect_unknown",
                EffectCertainty::UnknownRemoteEffect,
                RetryDirective::SafeImmediate,
            )
            .is_err()
        );
        assert!(
            ProviderFailure::new(
                ProviderFailureClass::Ambiguous,
                "submit_effect_unknown",
                EffectCertainty::UnknownRemoteEffect,
                RetryDirective::Never,
            )
            .is_ok()
        );
    }
}
