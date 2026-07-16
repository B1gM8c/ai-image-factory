use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, CallbackEnvelope, CallbackReceipt, CancelReceipt,
    CanonicalCommandPayload, Completed, EffectCertainty, InlineProvider, InvocationContext,
    OutputSlot, PendingOperation, PollObservation, ProviderFailure, ProviderFailureClass,
    ProviderRequestId, RemoteTaskProvider, RetryDirective, SingleOutputCommand, SubmitCall,
};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPayload {
    bytes: Vec<u8>,
    source_command_sha256: String,
}

impl TestPayload {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        let bytes = bytes.into();
        let source_command_sha256 = hex_sha256(&bytes);
        Self {
            bytes,
            source_command_sha256,
        }
    }

    pub fn bound_to(bytes: impl Into<Vec<u8>>, source_command_sha256: impl Into<String>) -> Self {
        Self {
            bytes: bytes.into(),
            source_command_sha256: source_command_sha256.into(),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl CanonicalCommandPayload for TestPayload {
    const SCHEMA_ID: &'static str = "provider-command-v1";
    const ADAPTER_REVISION: &'static str = "provider-test-adapter-v1";

    fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }

    fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
        self.bytes
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    let mut encoded = [0_u8; 64];
    hex::encode_to_slice(digest.finalize(), &mut encoded)
        .expect("SHA-256 always encodes into 64 hexadecimal bytes");
    String::from_utf8(encoded.to_vec()).expect("hexadecimal output is ASCII")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputPlan {
    pub chunks: Vec<Vec<u8>>,
    pub media_type: String,
    pub provider_request_id: Option<ProviderRequestId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InlineStep {
    Complete(OutputPlan),
    Fail(ProviderFailure),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitStep {
    Pending(PendingOperation),
    Fail(ProviderFailure),
    Never,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PollStep {
    Pending { next_poll_after_ms: Option<u64> },
    Complete(OutputPlan),
    Failed(ProviderFailure),
    Error(ProviderFailure),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FakeCallCounts {
    pub inline: usize,
    pub submit: usize,
    pub poll: usize,
    pub cancel: usize,
    pub callback: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedSubmitIdempotency {
    SubmissionBound,
    ProviderToken(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedSubmitCall {
    pub submission_id: String,
    pub provider_id: String,
    pub operation_id: String,
    pub descriptor_revision: String,
    pub model: String,
    pub attempt: u32,
    pub provider_timeout_ms: u64,
    pub provider_deadline_unix_ms: u64,
    pub command_schema: &'static str,
    pub adapter_revision: &'static str,
    pub command_sha256: [u8; 32],
    pub canonical_payload: Vec<u8>,
    pub output: OutputSlot,
    pub idempotency: ObservedSubmitIdempotency,
}

#[derive(Default)]
struct FakeState {
    inline: VecDeque<InlineStep>,
    submit: VecDeque<SubmitStep>,
    poll: VecDeque<PollStep>,
    cancel: VecDeque<Result<CancelReceipt, ProviderFailure>>,
    callback: VecDeque<Result<CallbackReceipt, ProviderFailure>>,
    submit_idempotency: Vec<ObservedSubmitIdempotency>,
    submit_calls: Vec<ObservedSubmitCall>,
    calls: FakeCallCounts,
}

#[derive(Clone, Default)]
pub struct ScriptedFakeProvider {
    state: Arc<Mutex<FakeState>>,
}

impl ScriptedFakeProvider {
    pub fn push_inline(&self, step: InlineStep) {
        self.state.lock().unwrap().inline.push_back(step);
    }

    pub fn push_submit(&self, step: SubmitStep) {
        self.state.lock().unwrap().submit.push_back(step);
    }

    pub fn push_poll(&self, step: PollStep) {
        self.state.lock().unwrap().poll.push_back(step);
    }

    pub fn push_cancel(&self, step: Result<CancelReceipt, ProviderFailure>) {
        self.state.lock().unwrap().cancel.push_back(step);
    }

    pub fn push_callback(&self, step: Result<CallbackReceipt, ProviderFailure>) {
        self.state.lock().unwrap().callback.push_back(step);
    }

    pub fn calls(&self) -> FakeCallCounts {
        self.state.lock().unwrap().calls
    }

    pub fn submit_idempotency(&self) -> Vec<ObservedSubmitIdempotency> {
        self.state.lock().unwrap().submit_idempotency.clone()
    }

    pub fn submit_calls(&self) -> Vec<ObservedSubmitCall> {
        self.state.lock().unwrap().submit_calls.clone()
    }
}

async fn emit<S: ArtifactSink>(
    plan: OutputPlan,
    sink: &mut S,
) -> Result<Completed, ProviderFailure> {
    for chunk in plan.chunks {
        sink.write_chunk(&chunk).await.map_err(sink_failure)?;
    }
    let artifact = sink
        .finalize(ArtifactMetadata {
            media_type: &plan.media_type,
        })
        .await
        .map_err(sink_failure)?;
    Ok(Completed::new(artifact, plan.provider_request_id))
}

fn sink_failure(error: image_provider_sdk::ArtifactSinkError) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::ArtifactInvalid,
        error.code(),
        EffectCertainty::UnknownRemoteEffect,
        RetryDirective::Never,
    )
    .expect("artifact sink error codes are controlled by the conformance harness")
}

fn missing_script(method: &str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Permanent,
        format!("missing_{method}_script"),
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Never,
    )
    .expect("fake method names form valid provider error codes")
}

impl InlineProvider for ScriptedFakeProvider {
    type Payload = TestPayload;

    async fn execute<S: ArtifactSink>(
        &self,
        _context: InvocationContext<'_>,
        _command: &SingleOutputCommand<Self::Payload>,
        sink: &mut S,
    ) -> Result<Completed, ProviderFailure> {
        let step = {
            let mut state = self.state.lock().unwrap();
            state.calls.inline += 1;
            state.inline.pop_front()
        }
        .ok_or_else(|| missing_script("inline"))?;

        match step {
            InlineStep::Complete(plan) => emit(plan, sink).await,
            InlineStep::Fail(error) => Err(error),
        }
    }
}

impl RemoteTaskProvider for ScriptedFakeProvider {
    type Payload = TestPayload;

    fn provider_id(&self) -> &'static str {
        "provider-test"
    }

    async fn submit(
        &self,
        call: SubmitCall<'_, Self::Payload>,
    ) -> Result<PendingOperation, ProviderFailure> {
        let context = call.context();
        let idempotency = call.idempotency();
        let command = call.command();
        let step = {
            let mut state = self.state.lock().unwrap();
            state.calls.submit += 1;
            let idempotency = match idempotency.token() {
                Some(token) => ObservedSubmitIdempotency::ProviderToken(token.to_owned()),
                None => ObservedSubmitIdempotency::SubmissionBound,
            };
            state.submit_idempotency.push(idempotency.clone());
            state.submit_calls.push(ObservedSubmitCall {
                submission_id: context.submission_id().to_owned(),
                provider_id: context.provider_id().to_owned(),
                operation_id: context.operation_id().to_owned(),
                descriptor_revision: context.descriptor_revision().to_owned(),
                model: context.model().to_owned(),
                attempt: context.attempt(),
                provider_timeout_ms: context.provider_timeout_ms(),
                provider_deadline_unix_ms: context.provider_deadline_unix_ms(),
                command_schema: command.schema_id(),
                adapter_revision: command.adapter_revision(),
                command_sha256: *command.canonical_sha256(),
                canonical_payload: command.canonical_payload().to_vec(),
                output: command.output(),
                idempotency,
            });
            state.submit.pop_front()
        }
        .ok_or_else(|| missing_script("submit"))?;

        match step {
            SubmitStep::Pending(pending) => Ok(pending),
            SubmitStep::Fail(error) => Err(error),
            SubmitStep::Never => std::future::pending().await,
        }
    }

    async fn poll<S: ArtifactSink>(
        &self,
        _context: InvocationContext<'_>,
        _operation: &image_provider_sdk::RemoteOperationRef,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        let step = {
            let mut state = self.state.lock().unwrap();
            state.calls.poll += 1;
            state.poll.pop_front()
        }
        .ok_or_else(|| missing_script("poll"))?;

        match step {
            PollStep::Pending { next_poll_after_ms } => {
                Ok(PollObservation::Pending { next_poll_after_ms })
            }
            PollStep::Complete(plan) => emit(plan, sink).await.map(PollObservation::Completed),
            PollStep::Failed(error) => Ok(PollObservation::Failed(error)),
            PollStep::Error(error) => Err(error),
        }
    }

    async fn cancel(
        &self,
        _context: InvocationContext<'_>,
        _operation: &image_provider_sdk::RemoteOperationRef,
    ) -> Result<CancelReceipt, ProviderFailure> {
        let mut state = self.state.lock().unwrap();
        state.calls.cancel += 1;
        state
            .cancel
            .pop_front()
            .unwrap_or_else(|| Err(missing_script("cancel")))
    }

    fn verify_callback(
        &self,
        _envelope: CallbackEnvelope<'_>,
    ) -> Result<CallbackReceipt, ProviderFailure> {
        let mut state = self.state.lock().unwrap();
        state.calls.callback += 1;
        state
            .callback
            .pop_front()
            .unwrap_or_else(|| Err(missing_script("callback")))
    }
}
