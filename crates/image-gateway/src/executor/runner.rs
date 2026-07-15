use std::sync::Arc;

use async_trait::async_trait;

use super::{
    ExecutorLaunchContext, ExecutorLaunchContextStore, ExecutorResultManifest,
    ExecutorSubmissionLease, ExecutorSubmissionOutcome,
};
use crate::runner::{
    FilesystemRunnerJournal, LaunchDecision, RunnerJournalError, RunnerJournalObservation,
};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableRunnerResult {
    Terminal(RunnerOutcome),
    Retryable { error_code: String },
}

impl From<RunnerOutcome> for DurableRunnerResult {
    fn from(outcome: RunnerOutcome) -> Self {
        Self::Terminal(outcome)
    }
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
    async fn start_or_attach(
        &self,
        lease: ExecutorSubmissionLease,
        authority: RunnerLaunchAuthority,
    ) -> DurableRunnerResult;
}

#[async_trait]
pub trait DurableEvidenceRecovery: Send + Sync + 'static {
    async fn recover_evidence(&self, lease: ExecutorSubmissionLease) -> DurableRunnerResult;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunnerLaunchAuthority {
    AllowLaunch,
    AttachOnly,
}

#[async_trait]
pub trait SingleOutputSupervisor: Send + Sync + 'static {
    async fn prepare(
        &self,
        lease: &ExecutorSubmissionLease,
        context: &ExecutorLaunchContext,
    ) -> Result<(), RunnerError>;

    async fn start_or_attach(
        &self,
        lease: &ExecutorSubmissionLease,
        decision: LaunchDecision,
    ) -> Result<Vec<u8>, RunnerError>;
}

#[async_trait]
pub trait ExecutorArtifactSink: Send + Sync + 'static {
    async fn publish(
        &self,
        lease: &ExecutorSubmissionLease,
        bytes: &[u8],
    ) -> Result<ExecutorResultManifest, RunnerError>;
}

pub struct JournaledDurableRunner<C, S, A> {
    contexts: C,
    journal: Arc<FilesystemRunnerJournal>,
    supervisor: S,
    artifacts: A,
}

impl<C, S, A> JournaledDurableRunner<C, S, A> {
    pub fn new(
        contexts: C,
        journal: Arc<FilesystemRunnerJournal>,
        supervisor: S,
        artifacts: A,
    ) -> Self {
        Self {
            contexts,
            journal,
            supervisor,
            artifacts,
        }
    }
}

#[async_trait]
impl<C, S, A> DurableRunner for JournaledDurableRunner<C, S, A>
where
    C: ExecutorLaunchContextStore,
    S: SingleOutputSupervisor,
    A: ExecutorArtifactSink,
{
    async fn start_or_attach(
        &self,
        lease: ExecutorSubmissionLease,
        authority: RunnerLaunchAuthority,
    ) -> DurableRunnerResult {
        let observation = match self.journal.start_or_attach(&lease) {
            Ok(RunnerJournalObservation::Terminal(outcome)) => return outcome.into(),
            Ok(observation) => observation,
            Err(error) => return journal_error_outcome(error).into(),
        };
        if observation == RunnerJournalObservation::Prepared
            && authority == RunnerLaunchAuthority::AttachOnly
        {
            return DurableRunnerResult::Retryable {
                error_code: "runner_launch_evidence_missing".to_string(),
            };
        }

        let context = match self.contexts.load_launch_context(&lease).await {
            Ok(context) => context,
            Err(error) => return submission_error_outcome(error).into(),
        };
        if let Err(error) = self.supervisor.prepare(&lease, &context).await {
            return runner_error_result(error);
        }
        let decision = match authority {
            RunnerLaunchAuthority::AllowLaunch => match self.journal.commit_launch(&lease) {
                Ok(decision) => decision,
                Err(error) => return journal_error_outcome(error).into(),
            },
            RunnerLaunchAuthority::AttachOnly => LaunchDecision::Attach,
        };
        let outcome = match self.supervisor.start_or_attach(&lease, decision).await {
            Ok(bytes) => match self.artifacts.publish(&lease, &bytes).await {
                Ok(manifest) => RunnerOutcome::Succeeded(manifest),
                Err(RunnerError::Unavailable) => {
                    return DurableRunnerResult::Retryable {
                        error_code: "artifact_authority_unavailable".to_string(),
                    };
                }
                Err(error) => RunnerOutcome::from_error(error),
            },
            Err(error) => return runner_error_result(error),
        };
        if let Err(error) = self.journal.publish_terminal(&lease, &outcome) {
            return journal_error_outcome(error).into();
        }
        outcome.into()
    }
}

#[async_trait]
impl<C, S, A> DurableEvidenceRecovery for JournaledDurableRunner<C, S, A>
where
    C: Send + Sync + 'static,
    S: SingleOutputSupervisor,
    A: ExecutorArtifactSink,
{
    async fn recover_evidence(&self, lease: ExecutorSubmissionLease) -> DurableRunnerResult {
        match self.journal.start_or_attach(&lease) {
            Ok(RunnerJournalObservation::Terminal(outcome)) => outcome.into(),
            Ok(RunnerJournalObservation::Prepared) => DurableRunnerResult::Retryable {
                error_code: "runner_launch_evidence_missing".to_string(),
            },
            Ok(RunnerJournalObservation::LaunchCommitted) => {
                let outcome = match self
                    .supervisor
                    .start_or_attach(&lease, LaunchDecision::Attach)
                    .await
                {
                    Ok(bytes) => match self.artifacts.publish(&lease, &bytes).await {
                        Ok(manifest) => RunnerOutcome::Succeeded(manifest),
                        Err(RunnerError::Unavailable) => {
                            return DurableRunnerResult::Retryable {
                                error_code: "artifact_authority_unavailable".to_string(),
                            };
                        }
                        Err(error) => RunnerOutcome::from_error(error),
                    },
                    Err(error) => return runner_error_result(error),
                };
                if let Err(error) = self.journal.publish_terminal(&lease, &outcome) {
                    return journal_error_outcome(error).into();
                }
                outcome.into()
            }
            Err(error) => journal_error_outcome(error).into(),
        }
    }
}

fn runner_error_result(error: RunnerError) -> DurableRunnerResult {
    match error {
        RunnerError::Unavailable => DurableRunnerResult::Retryable {
            error_code: "runner_unavailable".to_string(),
        },
        error => RunnerOutcome::from_error(error).into(),
    }
}

fn submission_error_outcome(error: super::ExecutorSubmissionError) -> RunnerOutcome {
    let code = match error {
        super::ExecutorSubmissionError::Unavailable => "launch_context_unavailable",
        super::ExecutorSubmissionError::Conflict => "launch_context_conflict",
        super::ExecutorSubmissionError::InvalidInput => "launch_context_invalid",
        super::ExecutorSubmissionError::StaleLease => "launch_context_stale_lease",
    };
    RunnerOutcome::Uncertain {
        error_code: code.to_string(),
    }
}

fn journal_error_outcome(error: RunnerJournalError) -> RunnerOutcome {
    let code = match error {
        RunnerJournalError::InvalidInput => "runner_journal_invalid",
        RunnerJournalError::Conflict => "runner_journal_conflict",
        RunnerJournalError::Integrity => "runner_journal_integrity",
        RunnerJournalError::Unavailable => "runner_journal_unavailable",
    };
    RunnerOutcome::Uncertain {
        error_code: code.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::{
        admission::GENERATION_COMMAND_SCHEMA,
        executor::{ExecutorSubmissionError, ExecutorSubmissionLease},
    };

    #[derive(Clone)]
    struct FakeContexts {
        calls: Arc<Mutex<u32>>,
        context: ExecutorLaunchContext,
    }

    #[async_trait]
    impl ExecutorLaunchContextStore for FakeContexts {
        async fn load_launch_context(
            &self,
            _lease: &ExecutorSubmissionLease,
        ) -> Result<ExecutorLaunchContext, ExecutorSubmissionError> {
            *self.calls.lock().unwrap() += 1;
            Ok(self.context.clone())
        }
    }

    #[derive(Clone)]
    struct FakeSupervisor {
        decisions: Arc<Mutex<Vec<LaunchDecision>>>,
        bytes: Vec<u8>,
    }

    #[async_trait]
    impl SingleOutputSupervisor for FakeSupervisor {
        async fn prepare(
            &self,
            _lease: &ExecutorSubmissionLease,
            _context: &ExecutorLaunchContext,
        ) -> Result<(), RunnerError> {
            Ok(())
        }

        async fn start_or_attach(
            &self,
            _lease: &ExecutorSubmissionLease,
            decision: LaunchDecision,
        ) -> Result<Vec<u8>, RunnerError> {
            self.decisions.lock().unwrap().push(decision);
            Ok(self.bytes.clone())
        }
    }

    #[derive(Clone)]
    struct FakeArtifacts {
        calls: Arc<Mutex<u32>>,
        manifest: ExecutorResultManifest,
    }

    struct FlakyArtifacts {
        calls: Arc<Mutex<u32>>,
        manifest: ExecutorResultManifest,
    }

    #[async_trait]
    impl ExecutorArtifactSink for FakeArtifacts {
        async fn publish(
            &self,
            _lease: &ExecutorSubmissionLease,
            _bytes: &[u8],
        ) -> Result<ExecutorResultManifest, RunnerError> {
            *self.calls.lock().unwrap() += 1;
            Ok(self.manifest.clone())
        }
    }

    #[async_trait]
    impl ExecutorArtifactSink for FlakyArtifacts {
        async fn publish(
            &self,
            _lease: &ExecutorSubmissionLease,
            _bytes: &[u8],
        ) -> Result<ExecutorResultManifest, RunnerError> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            if *calls == 1 {
                Err(RunnerError::Unavailable)
            } else {
                Ok(self.manifest.clone())
            }
        }
    }

    #[tokio::test]
    async fn fresh_execution_launches_once_and_terminal_replay_skips_dependencies() {
        let temp = TempDir::new().unwrap();
        let lease = lease();
        let context_calls = Arc::new(Mutex::new(0));
        let decisions = Arc::new(Mutex::new(Vec::new()));
        let artifact_calls = Arc::new(Mutex::new(0));
        let manifest =
            ExecutorResultManifest::new(lease.submission_id, lease.executor_execution_id).unwrap();
        let runner = JournaledDurableRunner::new(
            FakeContexts {
                calls: context_calls.clone(),
                context: context(),
            },
            Arc::new(FilesystemRunnerJournal::new(temp.path().join("journal")).unwrap()),
            FakeSupervisor {
                decisions: decisions.clone(),
                bytes: vec![1, 2, 3],
            },
            FakeArtifacts {
                calls: artifact_calls.clone(),
                manifest: manifest.clone(),
            },
        );

        assert_eq!(
            runner
                .start_or_attach(lease.clone(), RunnerLaunchAuthority::AllowLaunch)
                .await,
            DurableRunnerResult::Terminal(RunnerOutcome::Succeeded(manifest.clone()))
        );
        assert_eq!(
            runner
                .start_or_attach(lease, RunnerLaunchAuthority::AttachOnly)
                .await,
            DurableRunnerResult::Terminal(RunnerOutcome::Succeeded(manifest))
        );
        assert_eq!(*context_calls.lock().unwrap(), 1);
        assert_eq!(*artifact_calls.lock().unwrap(), 1);
        assert_eq!(*decisions.lock().unwrap(), [LaunchDecision::LaunchOnce]);
    }

    #[tokio::test]
    async fn attach_only_without_launch_evidence_never_calls_supervisor() {
        let temp = TempDir::new().unwrap();
        let lease = lease();
        let context_calls = Arc::new(Mutex::new(0));
        let decisions = Arc::new(Mutex::new(Vec::new()));
        let artifact_calls = Arc::new(Mutex::new(0));
        let runner = JournaledDurableRunner::new(
            FakeContexts {
                calls: context_calls.clone(),
                context: context(),
            },
            Arc::new(FilesystemRunnerJournal::new(temp.path().join("journal")).unwrap()),
            FakeSupervisor {
                decisions: decisions.clone(),
                bytes: vec![1, 2, 3],
            },
            FakeArtifacts {
                calls: artifact_calls.clone(),
                manifest: ExecutorResultManifest::new(
                    lease.submission_id,
                    lease.executor_execution_id,
                )
                .unwrap(),
            },
        );

        assert_eq!(
            runner
                .start_or_attach(lease, RunnerLaunchAuthority::AttachOnly)
                .await,
            DurableRunnerResult::Retryable {
                error_code: "runner_launch_evidence_missing".to_string(),
            }
        );
        assert_eq!(*context_calls.lock().unwrap(), 0);
        assert_eq!(*artifact_calls.lock().unwrap(), 0);
        assert!(decisions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unavailable_artifact_authority_retries_from_success_spool_before_terminal() {
        let temp = TempDir::new().unwrap();
        let lease = lease();
        let context_calls = Arc::new(Mutex::new(0));
        let decisions = Arc::new(Mutex::new(Vec::new()));
        let artifact_calls = Arc::new(Mutex::new(0));
        let manifest =
            ExecutorResultManifest::new(lease.submission_id, lease.executor_execution_id).unwrap();
        let journal = Arc::new(FilesystemRunnerJournal::new(temp.path().join("journal")).unwrap());
        let runner = JournaledDurableRunner::new(
            FakeContexts {
                calls: context_calls.clone(),
                context: context(),
            },
            journal.clone(),
            FakeSupervisor {
                decisions: decisions.clone(),
                bytes: vec![1, 2, 3],
            },
            FlakyArtifacts {
                calls: artifact_calls.clone(),
                manifest: manifest.clone(),
            },
        );

        assert_eq!(
            runner
                .start_or_attach(lease.clone(), RunnerLaunchAuthority::AllowLaunch)
                .await,
            DurableRunnerResult::Retryable {
                error_code: "artifact_authority_unavailable".to_string(),
            }
        );
        assert_eq!(
            journal.start_or_attach(&lease).unwrap(),
            RunnerJournalObservation::LaunchCommitted
        );
        assert_eq!(
            runner
                .start_or_attach(lease.clone(), RunnerLaunchAuthority::AttachOnly)
                .await,
            DurableRunnerResult::Terminal(RunnerOutcome::Succeeded(manifest.clone()))
        );
        assert_eq!(
            journal.start_or_attach(&lease).unwrap(),
            RunnerJournalObservation::Terminal(RunnerOutcome::Succeeded(manifest))
        );
        assert_eq!(*context_calls.lock().unwrap(), 2);
        assert_eq!(*artifact_calls.lock().unwrap(), 2);
        assert_eq!(
            *decisions.lock().unwrap(),
            [LaunchDecision::LaunchOnce, LaunchDecision::Attach]
        );
    }

    fn context() -> ExecutorLaunchContext {
        ExecutorLaunchContext {
            request_id: "request-1".to_string(),
            api_profile: "openai-images-v1".to_string(),
            output_index: 0,
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            command_hash: "a".repeat(64),
            command_json: json!({"prompt": "draw a lighthouse"}),
        }
    }

    fn lease() -> ExecutorSubmissionLease {
        ExecutorSubmissionLease {
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            output_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            tenant_id: "tenant-1".to_string(),
            provider_id: "openai-codex".to_string(),
            model: "gpt-image-2".to_string(),
            work_item_id: Uuid::new_v4(),
            output_index: 0,
            command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
            command_hash: "a".repeat(64),
            executor_owner: "executor-owner-1".to_string(),
            executor_lease_epoch: 1,
            executor_lease_expires_at_ms: i64::MAX,
        }
    }
}
