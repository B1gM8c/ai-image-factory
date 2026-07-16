use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, DurableArtifactRef, EffectCertainty, InlineProvider,
    InvocationContext, InvocationDeadline, OutputSlot, PendingOperation, ProviderFailure,
    ProviderFailureClass, ProviderRequestId, RemoteOperationRef, RetryDirective,
    SingleOutputCommand, SubmitIdempotency,
};

use crate::{
    InlineStep, ObservedSubmitCall, ObservedSubmitIdempotency, OutputPlan, PollStep,
    RecordingArtifactSink, ScriptedFakeProvider, SubmitStep, TestPayload, drive_existing_operation,
    drive_remote_to_completion,
};

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWaker(std::thread::Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn context() -> InvocationContext<'static> {
    InvocationContext::new(
        "submission-1",
        "fake",
        "images.generations",
        "fake/images.generations/v1",
        "model-1",
        1,
        InvocationDeadline::new(60_000, 1_800_000_000_000).unwrap(),
    )
    .unwrap()
}

fn command(index: u32, total: u32) -> SingleOutputCommand<TestPayload> {
    SingleOutputCommand::new(
        OutputSlot::new(index, total).unwrap(),
        TestPayload::new(vec![1, 2, 3]),
    )
    .unwrap()
}

fn sink() -> RecordingArtifactSink {
    RecordingArtifactSink::new(
        DurableArtifactRef::new("tests", "artifact-1").unwrap(),
        [7; 32],
    )
}

fn output() -> OutputPlan {
    OutputPlan {
        chunks: vec![b"hello ".to_vec(), b"world".to_vec()],
        media_type: "image/png".to_string(),
        provider_request_id: Some(ProviderRequestId::new("request-1").unwrap()),
    }
}

#[test]
fn inline_is_single_output_streamed_and_finalized_once() {
    let provider = ScriptedFakeProvider::default();
    provider.push_inline(InlineStep::Complete(output()));
    let mut sink = sink();

    let completed = block_on(provider.execute(context(), &command(1, 2), &mut sink)).unwrap();

    assert_eq!(provider.calls().inline, 1);
    assert_eq!(sink.bytes(), b"hello world");
    assert_eq!(sink.chunk_sizes(), &[6, 5]);
    assert_eq!(sink.finalize_count(), 1);
    assert_eq!(completed.artifact().byte_size(), 11);
    assert_eq!(
        completed.provider_request_id().unwrap().as_str(),
        "request-1"
    );
}

#[test]
fn remote_pending_to_complete_submits_once() {
    let provider = ScriptedFakeProvider::default();
    provider.push_submit(SubmitStep::Pending(PendingOperation::new(
        RemoteOperationRef::new("fake", "submission-1", "operation-1").unwrap(),
        Some(ProviderRequestId::new("submit-request-1").unwrap()),
        Some(10),
    )));
    provider.push_poll(PollStep::Pending {
        next_poll_after_ms: Some(10),
    });
    provider.push_poll(PollStep::Complete(output()));
    let restarted_provider = provider.clone();
    let mut sink = sink();

    block_on(drive_remote_to_completion(
        &restarted_provider,
        context(),
        SubmitIdempotency::submission_bound(),
        &command(0, 1),
        &mut sink,
        3,
    ))
    .unwrap();

    assert_eq!(provider.calls().submit, 1);
    assert_eq!(provider.calls().poll, 2);
    assert_eq!(
        provider.submit_idempotency(),
        [ObservedSubmitIdempotency::SubmissionBound]
    );
    assert_eq!(
        provider.submit_calls(),
        [ObservedSubmitCall {
            submission_id: "submission-1".to_string(),
            provider_id: "fake".to_string(),
            operation_id: "images.generations".to_string(),
            descriptor_revision: "fake/images.generations/v1".to_string(),
            model: "model-1".to_string(),
            attempt: 1,
            provider_timeout_ms: 60_000,
            provider_deadline_unix_ms: 1_800_000_000_000,
            command_schema: "provider-command-v1",
            adapter_revision: "provider-test-adapter-v1",
            command_sha256: *command(0, 1).canonical_sha256(),
            canonical_payload: vec![1, 2, 3],
            output: OutputSlot::new(0, 1).unwrap(),
            idempotency: ObservedSubmitIdempotency::SubmissionBound,
        }]
    );
    assert_eq!(sink.finalize_count(), 1);
}

#[test]
fn remote_submit_observes_validated_provider_token() {
    let provider = ScriptedFakeProvider::default();
    provider.push_submit(SubmitStep::Pending(PendingOperation::new(
        RemoteOperationRef::new("fake", "submission-1", "operation-1").unwrap(),
        None,
        None,
    )));
    provider.push_poll(PollStep::Complete(output()));
    let mut sink = sink();

    block_on(drive_remote_to_completion(
        &provider,
        context(),
        SubmitIdempotency::provider_token("provider-token-1").unwrap(),
        &command(0, 1),
        &mut sink,
        1,
    ))
    .unwrap();

    assert_eq!(
        provider.submit_idempotency(),
        [ObservedSubmitIdempotency::ProviderToken(
            "provider-token-1".to_owned()
        )]
    );
}

#[test]
fn restarted_remote_adapter_attaches_without_resubmit() {
    let provider = ScriptedFakeProvider::default();
    let operation = RemoteOperationRef::new("fake", "submission-1", "operation-1").unwrap();
    provider.push_poll(PollStep::Complete(output()));
    let restarted_provider = provider.clone();
    let mut sink = sink();

    block_on(drive_existing_operation(
        &restarted_provider,
        context(),
        &operation,
        &mut sink,
        1,
    ))
    .unwrap();

    assert_eq!(provider.calls().submit, 0);
    assert_eq!(provider.calls().poll, 1);
    assert_eq!(sink.finalize_count(), 1);
}

#[test]
fn failure_classification_is_preserved() {
    let provider = ScriptedFakeProvider::default();
    let failure = ProviderFailure::new(
        ProviderFailureClass::Throttled,
        "rate_limited",
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Backoff,
    )
    .unwrap()
    .with_retry_after_ms(500);
    provider.push_inline(InlineStep::Fail(failure.clone()));
    let mut sink = sink();

    let observed = block_on(provider.execute(context(), &command(0, 1), &mut sink)).unwrap_err();

    assert_eq!(observed, failure);
    assert_eq!(observed.class(), ProviderFailureClass::Throttled);
    assert_eq!(observed.retry(), RetryDirective::Backoff);
    assert_eq!(observed.retry_after_ms(), Some(500));
    assert_eq!(sink.finalize_count(), 0);
}

#[test]
fn recording_sink_rejects_second_finalize() {
    let mut sink = sink();
    block_on(sink.write_chunk(b"one")).unwrap();
    block_on(sink.finalize(ArtifactMetadata {
        media_type: "image/png",
    }))
    .unwrap();

    assert!(
        block_on(sink.finalize(ArtifactMetadata {
            media_type: "image/png"
        }))
        .is_err()
    );
    assert_eq!(sink.finalize_count(), 1);
}
