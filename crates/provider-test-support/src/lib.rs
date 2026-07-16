#![forbid(unsafe_code)]

pub mod fake;
pub mod sink;
pub mod suite;

pub use fake::{
    FakeCallCounts, InlineStep, ObservedSubmitCall, ObservedSubmitIdempotency, OutputPlan,
    PollStep, ScriptedFakeProvider, SubmitStep, TestPayload,
};
pub use sink::RecordingArtifactSink;
pub use suite::{ConformanceError, drive_existing_operation, drive_remote_to_completion};

#[cfg(test)]
mod tests;
