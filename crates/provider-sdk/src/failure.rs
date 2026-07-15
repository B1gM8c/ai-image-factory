use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderFailureClass {
    Permanent,
    Authentication,
    Throttled,
    Transient,
    Ambiguous,
    ArtifactInvalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectCertainty {
    NoRemoteEffect,
    UnknownRemoteEffect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDirective {
    Never,
    SafeImmediate,
    Backoff,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderFailure {
    class: ProviderFailureClass,
    code: String,
    effect: EffectCertainty,
    retry: RetryDirective,
    retry_after_ms: Option<u64>,
}

impl ProviderFailure {
    pub fn new(
        class: ProviderFailureClass,
        code: impl Into<String>,
        effect: EffectCertainty,
        retry: RetryDirective,
    ) -> Result<Self, ProviderFailureValidationError> {
        let code = code.into();
        if code.is_empty()
            || code.len() > 128
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        {
            return Err(ProviderFailureValidationError);
        }
        if effect == EffectCertainty::UnknownRemoteEffect && retry != RetryDirective::Never {
            return Err(ProviderFailureValidationError);
        }
        Ok(Self {
            class,
            code,
            effect,
            retry,
            retry_after_ms: None,
        })
    }

    pub fn with_retry_after_ms(mut self, retry_after_ms: u64) -> Self {
        self.retry_after_ms = Some(retry_after_ms);
        self
    }

    pub fn class(&self) -> ProviderFailureClass {
        self.class
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn effect(&self) -> EffectCertainty {
        self.effect
    }

    pub fn retry(&self) -> RetryDirective {
        self.retry
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        self.retry_after_ms
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderFailureValidationError;

impl fmt::Display for ProviderFailureValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("provider failure semantics are invalid")
    }
}

impl Error for ProviderFailureValidationError {}

impl fmt::Display for ProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "provider failure: {}", self.code)
    }
}

impl Error for ProviderFailure {}
