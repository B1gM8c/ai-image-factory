use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutputSlot {
    index: u32,
    total: u32,
}

impl OutputSlot {
    pub fn new(index: u32, total: u32) -> Result<Self, OutputSlotError> {
        if total == 0 {
            return Err(OutputSlotError::EmptyRequest);
        }
        if index >= total {
            return Err(OutputSlotError::OutOfRange { index, total });
        }
        Ok(Self { index, total })
    }

    pub fn index(self) -> u32 {
        self.index
    }

    pub fn total(self) -> u32 {
        self.total
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputSlotError {
    EmptyRequest,
    OutOfRange { index: u32, total: u32 },
}

impl fmt::Display for OutputSlotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyRequest => formatter.write_str("output total must be greater than zero"),
            Self::OutOfRange { index, total } => {
                write!(formatter, "output index {index} is outside total {total}")
            }
        }
    }
}

impl Error for OutputSlotError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SingleOutputCommand<P> {
    schema_id: &'static str,
    adapter_revision: &'static str,
    canonical_sha256: [u8; 32],
    output: OutputSlot,
    payload: P,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderCommandIdentity {
    canonical_sha256: [u8; 32],
}

impl ProviderCommandIdentity {
    pub fn canonical_sha256(&self) -> &[u8; 32] {
        &self.canonical_sha256
    }
}

impl<P> SingleOutputCommand<P> {
    pub fn new(
        schema_id: &'static str,
        adapter_revision: &'static str,
        canonical_sha256: [u8; 32],
        output: OutputSlot,
        payload: P,
    ) -> Result<Self, CommandIdentityError> {
        if !valid_text(schema_id) || !valid_text(adapter_revision) {
            return Err(CommandIdentityError::InvalidCommandIdentity);
        }
        Ok(Self {
            schema_id,
            adapter_revision,
            canonical_sha256,
            output,
            payload,
        })
    }

    pub fn schema_id(&self) -> &'static str {
        self.schema_id
    }

    pub fn adapter_revision(&self) -> &'static str {
        self.adapter_revision
    }

    pub fn canonical_sha256(&self) -> &[u8; 32] {
        &self.canonical_sha256
    }

    pub fn identity(&self) -> ProviderCommandIdentity {
        ProviderCommandIdentity {
            canonical_sha256: self.canonical_sha256,
        }
    }

    pub fn output(&self) -> OutputSlot {
        self.output
    }

    pub fn payload(&self) -> &P {
        &self.payload
    }

    pub fn into_payload(self) -> P {
        self.payload
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvocationContext<'a> {
    submission_id: &'a str,
    provider_id: &'a str,
    operation_id: &'a str,
    descriptor_revision: &'a str,
    model: &'a str,
    attempt: u32,
}

impl<'a> InvocationContext<'a> {
    pub fn new(
        submission_id: &'a str,
        provider_id: &'a str,
        operation_id: &'a str,
        descriptor_revision: &'a str,
        model: &'a str,
        attempt: u32,
    ) -> Result<Self, CommandIdentityError> {
        if !valid_identity(submission_id)
            || !valid_identity(provider_id)
            || !valid_identity(operation_id)
            || !valid_text(descriptor_revision)
            || !valid_text(model)
            || attempt == 0
        {
            return Err(CommandIdentityError::InvalidInvocationContext);
        }
        Ok(Self {
            submission_id,
            provider_id,
            operation_id,
            descriptor_revision,
            model,
            attempt,
        })
    }

    pub fn submission_id(self) -> &'a str {
        self.submission_id
    }

    pub fn provider_id(self) -> &'a str {
        self.provider_id
    }

    pub fn operation_id(self) -> &'a str {
        self.operation_id
    }

    pub fn descriptor_revision(self) -> &'a str {
        self.descriptor_revision
    }

    pub fn model(self) -> &'a str {
        self.model
    }

    pub fn attempt(self) -> u32 {
        self.attempt
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubmitIdempotency<'a> {
    provider_token: Option<&'a str>,
}

impl<'a> SubmitIdempotency<'a> {
    pub const fn submission_bound() -> Self {
        Self {
            provider_token: None,
        }
    }

    pub fn provider_token(token: &'a str) -> Result<Self, CommandIdentityError> {
        if !valid_text(token) {
            return Err(CommandIdentityError::InvalidSubmitIdempotency);
        }
        Ok(Self {
            provider_token: Some(token),
        })
    }

    pub fn token(self) -> Option<&'a str> {
        self.provider_token
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandIdentityError {
    InvalidCommandIdentity,
    InvalidInvocationContext,
    InvalidSubmitIdempotency,
}

impl fmt::Display for CommandIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommandIdentity => {
                formatter.write_str("provider command identity is invalid")
            }
            Self::InvalidInvocationContext => {
                formatter.write_str("provider invocation context is invalid")
            }
            Self::InvalidSubmitIdempotency => {
                formatter.write_str("provider submit idempotency is invalid")
            }
        }
    }
}

impl Error for CommandIdentityError {}

fn valid_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn valid_text(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && !value.bytes().any(|byte| byte.is_ascii_control())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_slot_is_always_a_single_valid_projection() {
        assert_eq!(OutputSlot::new(0, 2).unwrap().index(), 0);
        assert_eq!(OutputSlot::new(1, 2).unwrap().total(), 2);
        assert_eq!(OutputSlot::new(0, 0), Err(OutputSlotError::EmptyRequest));
        assert!(matches!(
            OutputSlot::new(2, 2),
            Err(OutputSlotError::OutOfRange { .. })
        ));
    }

    #[test]
    fn durable_command_and_invocation_identity_fail_closed() {
        assert!(
            InvocationContext::new(
                "submission-1",
                "provider",
                "images.generations",
                "provider/images.generations/v1",
                "model",
                1,
            )
            .is_ok()
        );
        assert!(
            InvocationContext::new(
                "",
                "provider",
                "images.generations",
                "provider/images.generations/v1",
                "model",
                1,
            )
            .is_err()
        );
        assert!(
            InvocationContext::new(
                "submission-1",
                "provider",
                "images.generations",
                "provider/images.generations/v1",
                "model",
                0,
            )
            .is_err()
        );
        assert_eq!(SubmitIdempotency::submission_bound().token(), None);
        assert_eq!(
            SubmitIdempotency::provider_token("provider-token-1")
                .unwrap()
                .token(),
            Some("provider-token-1")
        );
        assert!(SubmitIdempotency::provider_token("").is_err());
        let command = SingleOutputCommand::new(
            "provider.command.v1",
            "adapter-v1",
            [7; 32],
            OutputSlot::new(0, 1).unwrap(),
            (),
        )
        .unwrap();
        assert_eq!(command.identity().canonical_sha256(), &[7; 32]);
        assert!(
            SingleOutputCommand::new(
                "",
                "adapter-v1",
                [0; 32],
                OutputSlot::new(0, 1).unwrap(),
                ()
            )
            .is_err()
        );
    }
}
