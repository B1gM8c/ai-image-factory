#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobKind {
    ImageGeneration,
    ImageEdit,
    ImageVariation,
    VideoGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobLifecycleState {
    Accepted,
    Reserved,
    Queued,
    Leased,
    Running,
    ProviderWaiting,
    ArtifactReady,
    Succeeded,
    Failed,
    Canceled,
    TimedOut,
}
