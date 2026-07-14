use async_trait::async_trait;

use super::{ExecutorResultManifest, ExecutorSubmissionLease, ExecutorSubmissionOutcome};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerError {
    Definite { error_code: String },
    Internal,
    Unavailable,
    Unknown { error_code: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerOutcome {
    Succeeded(ExecutorResultManifest),
    Failed { error_code: String },
    Uncertain { error_code: String },
}

impl RunnerOutcome {
    pub fn from_error(error: RunnerError) -> Self {
        match error {
            RunnerError::Definite { error_code } => Self::Failed { error_code },
            RunnerError::Internal => Self::Uncertain {
                error_code: "runner_internal".to_string(),
            },
            RunnerError::Unavailable => Self::Uncertain {
                error_code: "runner_unavailable".to_string(),
            },
            RunnerError::Unknown { error_code } => Self::Uncertain { error_code },
        }
    }
}

impl From<RunnerOutcome> for ExecutorSubmissionOutcome {
    fn from(outcome: RunnerOutcome) -> Self {
        match outcome {
            RunnerOutcome::Succeeded(manifest) => Self::Succeeded(manifest),
            RunnerOutcome::Failed { error_code } => Self::Failed { error_code },
            RunnerOutcome::Uncertain { error_code } => Self::Uncertain { error_code },
        }
    }
}

#[async_trait]
pub trait DurableRunner: Send + Sync + 'static {
    async fn start_or_attach(&self, lease: ExecutorSubmissionLease) -> RunnerOutcome;
}
