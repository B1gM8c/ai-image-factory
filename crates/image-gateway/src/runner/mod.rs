mod filesystem;
pub(crate) mod process;

pub use filesystem::FilesystemRunnerJournal;

use crate::executor::RunnerOutcome;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerJournalObservation {
    Prepared,
    LaunchCommitted,
    Terminal(RunnerOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LaunchDecision {
    /// Durable one-shot authority to attempt a launch; it does not prove spawn succeeded.
    ///
    /// If the caller fails after this is returned but before spawn, it must eventually record an
    /// uncertain outcome. The durable launch marker prevents this journal from returning
    /// `LaunchOnce` again.
    LaunchOnce,
    Attach,
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum RunnerJournalError {
    #[error("runner journal input is invalid")]
    InvalidInput,
    #[error("runner journal identity conflicts with durable state")]
    Conflict,
    #[error("runner journal durable evidence failed integrity validation")]
    Integrity,
    #[error("runner journal storage is unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests;
