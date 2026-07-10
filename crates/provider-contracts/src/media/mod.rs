#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MediaKind {
    Image,
    Video,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MediaOperation {
    Generation,
    Edit,
    Variation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamingMode {
    None,
    FinalEvent,
    PartialEvents,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobMode {
    Sync,
    Async,
}
