use async_trait::async_trait;

use super::{
    CodexProcessSupervisor, ExecutorLaunchContext, ExecutorSubmissionLease, GrokProcessSupervisor,
    RunnerError, SingleOutputSupervisor, SupervisedOutput,
};
use crate::runner::LaunchDecision;

pub enum ExecutorProcessSupervisor {
    Codex(CodexProcessSupervisor),
    Grok(GrokProcessSupervisor),
}

#[async_trait]
impl SingleOutputSupervisor for ExecutorProcessSupervisor {
    async fn prepare(
        &self,
        lease: &ExecutorSubmissionLease,
        context: &ExecutorLaunchContext,
    ) -> Result<(), RunnerError> {
        match self {
            Self::Codex(supervisor) => supervisor.prepare(lease, context).await,
            Self::Grok(supervisor) => supervisor.prepare(lease, context).await,
        }
    }

    async fn start_or_attach(
        &self,
        lease: &ExecutorSubmissionLease,
        decision: LaunchDecision,
    ) -> Result<SupervisedOutput, RunnerError> {
        match self {
            Self::Codex(supervisor) => supervisor.start_or_attach(lease, decision).await,
            Self::Grok(supervisor) => supervisor.start_or_attach(lease, decision).await,
        }
    }
}
