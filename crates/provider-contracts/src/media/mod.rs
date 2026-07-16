#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MediaKind {
    Image,
    Video,
}

impl MediaKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Video => "video",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MediaOperation {
    Generation,
    Edit,
    Variation,
}

impl MediaOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Generation => "generation",
            Self::Edit => "edit",
            Self::Variation => "variation",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamingMode {
    None,
    FinalEvent,
    PartialEvents,
}

impl StreamingMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::FinalEvent => "final_event",
            Self::PartialEvents => "partial_events",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobMode {
    Sync,
    Async,
}
