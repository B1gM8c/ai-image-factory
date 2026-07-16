use std::{error::Error, fmt, marker::PhantomData};

use sha2::{Digest, Sha256};

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
    source_command_sha256: String,
    canonical_payload: Box<[u8]>,
    output: OutputSlot,
    payload_type: PhantomData<fn() -> P>,
}

pub trait CanonicalCommandPayload {
    const SCHEMA_ID: &'static str;
    const ADAPTER_REVISION: &'static str;

    fn source_command_sha256(&self) -> &str;
    fn into_canonical_bytes(self, output: OutputSlot) -> Vec<u8>;
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

impl<P: CanonicalCommandPayload> SingleOutputCommand<P> {
    pub fn new(output: OutputSlot, payload: P) -> Result<Self, CommandIdentityError> {
        if !valid_text(P::SCHEMA_ID)
            || !valid_text(P::ADAPTER_REVISION)
            || !valid_sha256(payload.source_command_sha256())
        {
            return Err(CommandIdentityError::InvalidCommandIdentity);
        }
        let source_command_sha256 = payload.source_command_sha256().to_owned();
        let canonical_payload = payload.into_canonical_bytes(output).into_boxed_slice();
        let canonical_sha256 =
            command_sha256::<P>(&source_command_sha256, output, canonical_payload.as_ref());
        Ok(Self {
            schema_id: P::SCHEMA_ID,
            adapter_revision: P::ADAPTER_REVISION,
            canonical_sha256,
            source_command_sha256,
            canonical_payload,
            output,
            payload_type: PhantomData,
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

    pub fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }

    pub fn identity(&self) -> ProviderCommandIdentity {
        ProviderCommandIdentity {
            canonical_sha256: self.canonical_sha256,
        }
    }

    pub fn output(&self) -> OutputSlot {
        self.output
    }

    pub fn canonical_payload(&self) -> &[u8] {
        &self.canonical_payload
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvocationDeadline {
    provider_timeout_ms: u64,
    provider_deadline_unix_ms: u64,
}

impl InvocationDeadline {
    pub fn new(
        provider_timeout_ms: u64,
        provider_deadline_unix_ms: u64,
    ) -> Result<Self, CommandIdentityError> {
        if provider_timeout_ms == 0 || provider_deadline_unix_ms == 0 {
            return Err(CommandIdentityError::InvalidInvocationContext);
        }
        Ok(Self {
            provider_timeout_ms,
            provider_deadline_unix_ms,
        })
    }

    pub fn provider_timeout_ms(self) -> u64 {
        self.provider_timeout_ms
    }

    pub fn provider_deadline_unix_ms(self) -> u64 {
        self.provider_deadline_unix_ms
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
    deadline: InvocationDeadline,
}

impl<'a> InvocationContext<'a> {
    pub fn new(
        submission_id: &'a str,
        provider_id: &'a str,
        operation_id: &'a str,
        descriptor_revision: &'a str,
        model: &'a str,
        attempt: u32,
        deadline: InvocationDeadline,
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
            deadline,
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

    pub fn provider_timeout_ms(self) -> u64 {
        self.deadline.provider_timeout_ms()
    }

    pub fn provider_deadline_unix_ms(self) -> u64 {
        self.deadline.provider_deadline_unix_ms()
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
pub struct SubmitCall<'a, P> {
    context: InvocationContext<'a>,
    command: &'a SingleOutputCommand<P>,
    idempotency: SubmitIdempotency<'a>,
}

impl<'a, P> SubmitCall<'a, P> {
    pub fn new(
        context: InvocationContext<'a>,
        command: &'a SingleOutputCommand<P>,
        idempotency: SubmitIdempotency<'a>,
    ) -> Self {
        Self {
            context,
            command,
            idempotency,
        }
    }

    pub fn context(&self) -> InvocationContext<'a> {
        self.context
    }

    pub fn command(&self) -> &'a SingleOutputCommand<P> {
        self.command
    }

    pub fn idempotency(&self) -> SubmitIdempotency<'a> {
        self.idempotency
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

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn command_sha256<P: CanonicalCommandPayload>(
    source_command_sha256: &str,
    output: OutputSlot,
    canonical_payload: &[u8],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"ai-image-factory/provider-command/v1\0");
    for value in [
        P::SCHEMA_ID.as_bytes(),
        P::ADAPTER_REVISION.as_bytes(),
        source_command_sha256.as_bytes(),
        output.index().to_be_bytes().as_slice(),
        output.total().to_be_bytes().as_slice(),
        canonical_payload,
    ] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestPayload([u8; 32]);

    impl CanonicalCommandPayload for TestPayload {
        const SCHEMA_ID: &'static str = "provider.command.v1";
        const ADAPTER_REVISION: &'static str = "adapter-v1";

        fn source_command_sha256(&self) -> &str {
            "1111111111111111111111111111111111111111111111111111111111111111"
        }

        fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
            self.0.to_vec()
        }
    }

    struct InvalidPayload;

    impl CanonicalCommandPayload for InvalidPayload {
        const SCHEMA_ID: &'static str = "";
        const ADAPTER_REVISION: &'static str = "adapter-v1";

        fn source_command_sha256(&self) -> &str {
            "1111111111111111111111111111111111111111111111111111111111111111"
        }

        fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
            Vec::new()
        }
    }

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
                InvocationDeadline::new(60_000, 1_800_000_000_000).unwrap(),
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
                InvocationDeadline::new(60_000, 1_800_000_000_000).unwrap(),
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
                InvocationDeadline::new(60_000, 1_800_000_000_000).unwrap(),
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
        let command =
            SingleOutputCommand::new(OutputSlot::new(0, 1).unwrap(), TestPayload([7; 32])).unwrap();
        let replay =
            SingleOutputCommand::new(OutputSlot::new(0, 1).unwrap(), TestPayload([7; 32])).unwrap();
        assert_eq!(command.identity(), replay.identity());
        assert_eq!(command.canonical_payload(), &[7; 32]);
        assert!(SingleOutputCommand::new(OutputSlot::new(0, 1).unwrap(), InvalidPayload).is_err());
    }
}
