#[cfg(not(unix))]
compile_error!("image-cli-runtime supports Unix platforms only");

mod command;
mod output;
mod process;
mod workspace;

use std::{fmt::Display, io::Write, process::ExitStatus};

use thiserror::Error;

pub use command::{
    CommandSpec, CommandSpecError, MAX_STDIN_BYTES, VerifiedExecutable, WorkingDirectory,
};
pub use output::{
    AsyncOutputSealError, AsyncOutputSink, FreshOutputDirectory, OutputContract, OutputError,
    STREAM_BUFFER_BYTES, SealedOutput,
};
pub use process::{
    CapturedStream, MAX_CAPTURED_STREAM_BYTES, NoopSpawnObserver, ProcessBackend,
    ProcessCompletion, ProcessError, SpawnEvidence, SpawnObserver, TokioProcessBackend,
};
pub use workspace::{
    ATTEMPT_WORKSPACE_LOCK_FILENAME, AttemptDirectory, AttemptWorkspaceError,
    ExclusiveAttemptWorkspace,
};

pub trait CliPolicy {
    type Request: Sync;
    type Error: Display;

    fn command(&self, request: &Self::Request) -> Result<CommandSpec, Self::Error>;

    fn classify_exit(&self, status: &ExitStatus) -> ExitClassification;
}

pub trait ReceiptCliPolicy {
    type Request: Sync;
    type Receipt;
    type Error: Display;

    fn command(&self, request: &Self::Request) -> Result<CommandSpec, Self::Error>;

    fn classify_exit(&self, status: &ExitStatus) -> ExitClassification;

    fn parse_receipt(&self, stdout: &[u8]) -> Result<Self::Receipt, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExitClassification {
    Success,
    Failed { code: String },
}

#[derive(Debug)]
pub struct RunSuccess<W> {
    pub evidence: SpawnEvidence,
    pub status: ExitStatus,
    pub output: SealedOutput,
    pub sink: W,
}

#[derive(Debug)]
pub struct ReceiptSuccess<R> {
    pub evidence: SpawnEvidence,
    pub status: ExitStatus,
    pub receipt: R,
}

#[derive(Clone, Debug)]
pub struct CliRuntime<P, B = TokioProcessBackend> {
    policy: P,
    backend: B,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("CLI policy rejected the request: {0}")]
    Policy(String),
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error("CLI process failed: {code}")]
    ProcessFailed { code: String },
    #[error("artifact execution requires an output contract")]
    MissingOutputContract,
    #[error("receipt execution must not declare an artifact output contract")]
    UnexpectedOutputContract,
    #[error("captured process {stream} exceeded the 64 KiB limit")]
    CapturedOutputTooLarge { stream: &'static str },
    #[error("CLI receipt was rejected: {0}")]
    Receipt(String),
    #[error(transparent)]
    Output(#[from] OutputError),
    #[error("output sealing task failed: {0}")]
    OutputTask(String),
}

impl<P> CliRuntime<P, TokioProcessBackend> {
    pub fn new(policy: P) -> Self {
        Self {
            policy,
            backend: TokioProcessBackend,
        }
    }

    pub async fn run_to_sink<O, W>(
        &self,
        request: &P::Request,
        observer: &mut O,
        sink: W,
    ) -> Result<RunSuccess<W>, RuntimeError>
    where
        P: CliPolicy,
        O: SpawnObserver + Send,
        W: Write + Send + 'static,
    {
        let command = self.command(request)?;
        let completion = self.backend.execute_process(&command, observer).await?;
        finish_run(&self.policy, command, completion, sink).await
    }

    pub async fn run_receipt<O>(
        &self,
        request: &P::Request,
        observer: &mut O,
    ) -> Result<ReceiptSuccess<P::Receipt>, RuntimeError>
    where
        P: ReceiptCliPolicy,
        O: SpawnObserver + Send,
    {
        let command = self.receipt_command(request)?;
        if command.output().is_some() {
            return Err(RuntimeError::UnexpectedOutputContract);
        }
        let completion = self.backend.execute_process(&command, observer).await?;
        finish_receipt(&self.policy, completion)
    }
}

impl<P, B> CliRuntime<P, B> {
    pub fn with_backend(policy: P, backend: B) -> Self {
        Self { policy, backend }
    }

    pub fn policy(&self) -> &P {
        &self.policy
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }
}

impl<P, B> CliRuntime<P, B>
where
    P: CliPolicy,
    B: ProcessBackend,
{
    pub async fn run_to_sink_with_backend<O, W>(
        &self,
        request: &P::Request,
        observer: &mut O,
        sink: W,
    ) -> Result<RunSuccess<W>, RuntimeError>
    where
        O: SpawnObserver + Send,
        W: Write + Send + 'static,
    {
        let command = self.command(request)?;
        let completion = self.backend.execute(&command, observer).await?;
        finish_run(&self.policy, command, completion, sink).await
    }
}

impl<P, B> CliRuntime<P, B>
where
    P: ReceiptCliPolicy,
    B: ProcessBackend,
{
    pub async fn run_receipt_with_backend<O>(
        &self,
        request: &P::Request,
        observer: &mut O,
    ) -> Result<ReceiptSuccess<P::Receipt>, RuntimeError>
    where
        O: SpawnObserver + Send,
    {
        let command = self.receipt_command(request)?;
        if command.output().is_some() {
            return Err(RuntimeError::UnexpectedOutputContract);
        }
        let completion = self.backend.execute(&command, observer).await?;
        finish_receipt(&self.policy, completion)
    }
}

impl<P, B> CliRuntime<P, B>
where
    P: CliPolicy,
{
    fn command(&self, request: &P::Request) -> Result<CommandSpec, RuntimeError> {
        self.policy
            .command(request)
            .map_err(|error| RuntimeError::Policy(error.to_string()))
    }
}

impl<P, B> CliRuntime<P, B>
where
    P: ReceiptCliPolicy,
{
    fn receipt_command(&self, request: &P::Request) -> Result<CommandSpec, RuntimeError> {
        self.policy
            .command(request)
            .map_err(|error| RuntimeError::Policy(error.to_string()))
    }
}

async fn finish_run<P, W>(
    policy: &P,
    command: CommandSpec,
    completion: ProcessCompletion,
    sink: W,
) -> Result<RunSuccess<W>, RuntimeError>
where
    P: CliPolicy,
    W: Write + Send + 'static,
{
    match policy.classify_exit(&completion.status) {
        ExitClassification::Success => {}
        ExitClassification::Failed { code } => {
            return Err(RuntimeError::ProcessFailed { code });
        }
    }

    let directory = command.working_directory().directory();
    let contract = command
        .output()
        .cloned()
        .ok_or(RuntimeError::MissingOutputContract)?;
    let (output, sink) =
        tokio::task::spawn_blocking(move || output::seal_to_sink(directory, contract, sink))
            .await
            .map_err(|error| RuntimeError::OutputTask(error.to_string()))??;

    Ok(RunSuccess {
        evidence: completion.evidence,
        status: completion.status,
        output,
        sink,
    })
}

fn finish_receipt<P>(
    policy: &P,
    completion: ProcessCompletion,
) -> Result<ReceiptSuccess<P::Receipt>, RuntimeError>
where
    P: ReceiptCliPolicy,
{
    match policy.classify_exit(&completion.status) {
        ExitClassification::Success => {}
        ExitClassification::Failed { code } => {
            return Err(RuntimeError::ProcessFailed { code });
        }
    }
    if completion.stdout.is_truncated() {
        return Err(RuntimeError::CapturedOutputTooLarge { stream: "stdout" });
    }
    if completion.stderr.is_truncated() {
        return Err(RuntimeError::CapturedOutputTooLarge { stream: "stderr" });
    }
    let receipt = policy
        .parse_receipt(completion.stdout.bytes())
        .map_err(|error| RuntimeError::Receipt(error.to_string()))?;
    Ok(ReceiptSuccess {
        evidence: completion.evidence,
        status: completion.status,
        receipt,
    })
}

pub fn default_exit_classification(status: &ExitStatus) -> ExitClassification {
    if status.success() {
        ExitClassification::Success
    } else {
        ExitClassification::Failed {
            code: process::status_description(status)
                .to_string_lossy()
                .into_owned(),
        }
    }
}

#[cfg(test)]
mod tests;
