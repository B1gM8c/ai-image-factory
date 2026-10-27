use std::{
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use image_cli_runtime::{
    AttemptWorkspaceError, RecoverableAttemptWorkspace, VerifiedExecutable, WorkingDirectory,
};
use image_provider_sdk::{
    CanonicalCommandPayload, EffectCertainty, PendingOperation, ProviderFailure,
    ProviderFailureClass, RetryDirective,
};
use tokio::sync::{Mutex, oneshot};
use uuid::Uuid;

use super::inputs::{GatedCliInputFile, input_failure, stage_inputs};
use crate::{executor::ExecutorInputObject, input_blobs::InputBlobStore};

use super::process::{
    GatedCliBinding, GatedCliCommand, GatedCliObservation, GatedCliProcessError,
    GatedCliProcessOutcome, GatedCliProcessTerminal, GatedCliReady, GatedCliReceiptEvidence,
    GatedCliSubmission,
};
use crate::provider_tasks::{
    ProviderExecutionContext, ProviderSubmitDriver, ProviderSubmitDriverCall,
    ProviderSubmitDriverRecovery, ProviderSubmitIntent,
};

const PROCESS_OBSERVATION_INTERVAL: Duration = Duration::from_millis(10);
const RUNNER_HANDOFF_GRACE: Duration = Duration::from_millis(500);
const TERMINAL_PUBLICATION_GRACE_MS: u64 = 2_000;
const SUBMIT_ATTEMPT_PREFIX: &str = ".provider-submit-";

pub trait GatedCliSubmitCodec: Send + Sync + 'static {
    type Payload: CanonicalCommandPayload + Send + Sync + 'static;

    fn provider_id(&self) -> &'static str;

    /// Match each frozen job input to its canonical command descriptor, in manifest order.
    /// Returned filenames and measurements are verified before the submit gate is prepared.
    fn input_files(
        &self,
        _command: &image_provider_sdk::SingleOutputCommand<Self::Payload>,
        inputs: &[ExecutorInputObject],
    ) -> Result<Vec<GatedCliInputFile>, ProviderFailure> {
        if !inputs.is_empty() {
            return Err(input_failure("provider_submit_input_manifest_invalid"));
        }
        Ok(Vec::new())
    }

    fn command(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        command: &image_provider_sdk::SingleOutputCommand<Self::Payload>,
        workspace: &WorkingDirectory,
    ) -> Result<GatedCliCommand, ProviderFailure>;

    /// Transport projection used only by the normal one-shot submit driver.
    /// Operator recovery reconstructs `command` without changing historical argv.
    fn normal_command(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        command: &image_provider_sdk::SingleOutputCommand<Self::Payload>,
        workspace: &WorkingDirectory,
    ) -> Result<GatedCliCommand, ProviderFailure> {
        self.command(intent, context, command, workspace)
    }

    /// Only an explicit pre-acceptance refusal may return `NoRemoteEffect` with
    /// `Never`; malformed receipts and post-acceptance failures remain unknown.
    fn decode_receipt(
        &self,
        intent: &ProviderSubmitIntent,
        command: &image_provider_sdk::SingleOutputCommand<Self::Payload>,
        stdout: &[u8],
    ) -> Result<PendingOperation, ProviderFailure>;

    /// Observe the already executed attempt only. Implementations must persist
    /// independent acceptance evidence before returning an accepted identity;
    /// neither a local candidate nor a failed cache entry authorizes a replay.
    fn recover_receipt(
        &self,
        _call: &ProviderSubmitDriverCall<Self::Payload>,
        _evidence: Arc<GatedCliReceiptEvidence>,
    ) -> impl Future<Output = Result<Option<PendingOperation>, ProviderFailure>> + Send {
        async { Ok(None) }
    }

    /// Return only a fixed diagnostic code for a complete, nonzero CLI exit.
    /// The driver always preserves unknown effect and `Never` for this path.
    fn nonzero_exit_diagnostic(&self, _exit_code: i32, _stderr: &[u8]) -> Option<&'static str> {
        None
    }
}

pub struct GatedCliSubmitDriver<C> {
    codec: Arc<C>,
    runner_executable: PathBuf,
    runner_sha256: [u8; 32],
    runner_start: Mutex<()>,
    workspace: Arc<RecoverableAttemptWorkspace>,
    input_blobs: Option<Arc<dyn InputBlobStore>>,
}

pub struct GatedCliPreparedSubmission {
    process: GatedProcessRef,
    ready: Option<GatedCliReady>,
}

#[derive(Clone)]
struct GatedProcessRef {
    root: PathBuf,
    submission_id: Uuid,
    binding: GatedCliBinding,
    attempt_key: String,
}

impl<C> GatedCliSubmitDriver<C>
where
    C: GatedCliSubmitCodec,
{
    pub fn new(
        codec: C,
        runner_executable: impl AsRef<Path>,
        runner_sha256: impl AsRef<str>,
        workspace_root: WorkingDirectory,
    ) -> Result<Self, GatedCliProcessError> {
        let runner_sha256 = parse_sha256(runner_sha256.as_ref())?;
        let runner = VerifiedExecutable::new_with_sha256(runner_executable, runner_sha256)
            .map_err(|_| GatedCliProcessError::InvalidInput)?;
        let workspace = RecoverableAttemptWorkspace::new(&workspace_root, SUBMIT_ATTEMPT_PREFIX)
            .map_err(map_workspace_config_error)?;
        Ok(Self {
            codec: Arc::new(codec),
            runner_executable: runner.path().to_owned(),
            runner_sha256,
            runner_start: Mutex::new(()),
            workspace: Arc::new(workspace),
            input_blobs: None,
        })
    }

    pub fn with_input_blobs(mut self, store: Arc<dyn InputBlobStore>) -> Self {
        self.input_blobs = Some(store);
        self
    }

    async fn prepare_process(
        &self,
        call: &ProviderSubmitDriverCall<C::Payload>,
    ) -> Result<GatedCliPreparedSubmission, ProviderFailure> {
        let prepare_started = Instant::now();
        let process = GatedProcessRef::new(call)?;
        let workspace = Arc::clone(&self.workspace);
        let attempt_key = process.attempt_key.clone();
        let working_directory = tokio::task::spawn_blocking(move || {
            workspace.open_or_create(&attempt_key)?.working_directory()
        })
        .await
        .map_err(|_| local_process_failure("provider_submit_workspace_worker_stopped"))?
        .map_err(map_workspace_runtime_error)?;
        let inputs = match self.codec.input_files(call.command(), call.inputs()) {
            Ok(inputs) => inputs,
            Err(failure) => {
                self.cleanup_workspace(&process).await;
                return Err(if failure.effect() == EffectCertainty::NoRemoteEffect {
                    failure
                } else {
                    local_process_failure("provider_submit_codec_effect_invalid")
                });
            }
        };
        if let Err(failure) = stage_inputs(
            self.input_blobs.as_ref(),
            call.inputs(),
            inputs,
            &working_directory,
            prepare_started,
            Duration::from_millis(call.remaining_budget_ms()),
        )
        .await
        {
            self.cleanup_workspace(&process).await;
            return Err(failure);
        }
        let codec = Arc::clone(&self.codec);
        let owned_call = call.clone();
        let projected_workspace = working_directory.clone();
        let command = tokio::task::spawn_blocking(move || {
            codec.normal_command(
                owned_call.intent(),
                owned_call.execution_context(),
                owned_call.command(),
                &projected_workspace,
            )
        })
        .await;
        let command = match command {
            Ok(Ok(command)) => command,
            Ok(Err(failure)) => {
                self.cleanup_workspace(&process).await;
                return Err(if failure.effect() == EffectCertainty::NoRemoteEffect {
                    failure
                } else {
                    local_process_failure("provider_submit_codec_effect_invalid")
                });
            }
            Err(_) => {
                self.cleanup_workspace(&process).await;
                return Err(local_process_failure(
                    "provider_submit_codec_worker_stopped",
                ));
            }
        };
        if command.working_directory() != working_directory.path() {
            self.cleanup_workspace(&process).await;
            return Err(local_process_failure(
                "provider_submit_workspace_binding_mismatch",
            ));
        }
        if prepare_started.elapsed() >= Duration::from_millis(call.remaining_budget_ms()) {
            self.cleanup_workspace(&process).await;
            return Err(local_process_failure("provider_submit_deadline_elapsed"));
        }
        let prepare_process = process.clone();
        tokio::task::spawn_blocking(move || {
            let submission =
                GatedCliSubmission::new(&prepare_process.root, prepare_process.submission_id)?;
            submission.prepare(&prepare_process.binding, &command)
        })
        .await
        .map_err(|_| local_process_failure("provider_submit_process_worker_stopped"))?
        .map_err(pre_release_process_failure)?;

        let mut observation = observe_process(&process)
            .await
            .map_err(pre_release_process_failure)?;
        let mut runner_start = None;
        let runner_exit = if matches!(observation, GatedCliObservation::AwaitingHelper) {
            let guard = self.runner_start.lock().await;
            observation = observe_process(&process)
                .await
                .map_err(pre_release_process_failure)?;
            if matches!(observation, GatedCliObservation::AwaitingHelper) {
                runner_start = Some(guard);
                match self.spawn_runner(&process).await {
                    Ok(runner) => Some(runner),
                    Err(failure) => {
                        self.cleanup_workspace(&process).await;
                        return Err(failure);
                    }
                }
            } else {
                None
            }
        } else {
            None
        };
        let prepared = wait_until_prepared(
            process,
            observation,
            runner_exit,
            prepare_started,
            Duration::from_millis(call.remaining_budget_ms()),
        )
        .await;
        drop(runner_start);
        prepared
    }

    async fn spawn_runner(
        &self,
        process: &GatedProcessRef,
    ) -> Result<oneshot::Receiver<Result<ExitStatus, std::io::Error>>, ProviderFailure> {
        let runner_path = self.runner_executable.clone();
        let runner_sha256 = self.runner_sha256;
        let runner = tokio::task::spawn_blocking(move || {
            VerifiedExecutable::new_with_sha256(runner_path, runner_sha256)
        })
        .await
        .map_err(|_| local_process_failure("provider_submit_runner_verify_worker_stopped"))?
        .map_err(|_| local_process_failure("provider_submit_runner_changed"))?;
        let mut command = tokio::process::Command::new(runner.path());
        command
            .arg(&process.root)
            .arg(process.submission_id.to_string())
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|_| local_process_failure("provider_submit_runner_unavailable"))?;
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = child.wait().await;
            let _ = sender.send(result);
        });
        Ok(receiver)
    }

    async fn resolve_released(
        &self,
        call: &ProviderSubmitDriverCall<C::Payload>,
        prepared: Option<GatedCliPreparedSubmission>,
    ) -> ProviderSubmitDriverRecovery {
        let process = match prepared {
            Some(prepared) => {
                if let Some(ready) = prepared.ready {
                    match release_process(&prepared.process, ready, call).await {
                        Ok(())
                        | Err(ProcessReleaseFailure::Process(GatedCliProcessError::NotReady)) => {}
                        Err(ProcessReleaseFailure::Credentials) => {
                            return ProviderSubmitDriverRecovery::AwaitingEvidence;
                        }
                        Err(_) => {
                            return ProviderSubmitDriverRecovery::Failed(unknown_process_failure(
                                "provider_submit_release_failed",
                            ));
                        }
                    }
                }
                prepared.process
            }
            None => match GatedProcessRef::new(call) {
                Ok(process) => process,
                Err(failure) => {
                    return ProviderSubmitDriverRecovery::Failed(unknown_process_failure(
                        failure.code(),
                    ));
                }
            },
        };
        match wait_for_terminal(&process, call).await {
            Ok(terminal) => {
                let result = self.resolve_terminal(call, &process, terminal).await;
                if cleanup_after_resolution(&result) {
                    self.cleanup_workspace(&process).await;
                }
                result
            }
            Err(failure)
                if failure.failure.code() == crate::provider_tasks::SUBMIT_CREDENTIAL_BLOCKED =>
            {
                ProviderSubmitDriverRecovery::AwaitingEvidence
            }
            Err(failure) => {
                if failure.cleanup_safe
                    && failure.failure.effect() == EffectCertainty::NoRemoteEffect
                {
                    self.cleanup_workspace(&process).await;
                }
                ProviderSubmitDriverRecovery::Failed(failure.failure)
            }
        }
    }

    async fn cleanup_workspace(&self, process: &GatedProcessRef) {
        let workspace = Arc::clone(&self.workspace);
        let attempt_key = process.attempt_key.clone();
        let result = tokio::task::spawn_blocking(move || workspace.remove(&attempt_key)).await;
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::error!(
                error = %error,
                "provider submit attempt workspace cleanup failed"
            ),
            Err(error) => tracing::error!(
                error = %error,
                "provider submit attempt workspace cleanup worker stopped"
            ),
        }
    }

    async fn resolve_terminal(
        &self,
        call: &ProviderSubmitDriverCall<C::Payload>,
        process: &GatedProcessRef,
        terminal: GatedCliProcessTerminal,
    ) -> ProviderSubmitDriverRecovery {
        if !terminal.released() || !terminal.exec_started() {
            return ProviderSubmitDriverRecovery::Failed(no_effect_terminal_failure(&terminal));
        }
        if terminal.stdout_truncated() {
            return self
                .recover_terminal_receipt(
                    call,
                    process,
                    terminal,
                    unknown_process_failure("provider_submit_receipt_truncated"),
                )
                .await;
        }
        if !matches!(
            terminal.outcome(),
            GatedCliProcessOutcome::Exited {
                exit_code: Some(0),
                signal: None,
            }
        ) {
            if let Some(code) = nonzero_exit_diagnostic(
                self.codec.as_ref(),
                terminal.outcome(),
                terminal.stdout(),
                terminal.stdout_truncated(),
                terminal.stderr(),
                terminal.stderr_truncated(),
            ) {
                return ProviderSubmitDriverRecovery::Failed(unknown_process_failure(code));
            }
            let failure = unknown_terminal_failure(&terminal);
            return self
                .recover_terminal_receipt(call, process, terminal, failure)
                .await;
        }
        let codec = Arc::clone(&self.codec);
        let owned_call = call.clone();
        let stdout = terminal.stdout().to_vec();
        let failure = match tokio::task::spawn_blocking(move || {
            codec.decode_receipt(owned_call.intent(), owned_call.command(), &stdout)
        })
        .await
        {
            Ok(Ok(pending)) => return ProviderSubmitDriverRecovery::Accepted(pending),
            Ok(Err(failure))
                if failure.effect() == EffectCertainty::NoRemoteEffect
                    && failure.retry() == RetryDirective::Never
                    && !terminal.stderr_truncated() =>
            {
                return ProviderSubmitDriverRecovery::Failed(failure);
            }
            Ok(Err(failure)) => unknown_process_failure(failure.code()),
            Err(_) => unknown_process_failure("provider_submit_receipt_worker_stopped"),
        };
        self.recover_terminal_receipt(call, process, terminal, failure)
            .await
    }

    async fn recover_terminal_receipt(
        &self,
        call: &ProviderSubmitDriverCall<C::Payload>,
        process: &GatedProcessRef,
        terminal: GatedCliProcessTerminal,
        original_failure: ProviderFailure,
    ) -> ProviderSubmitDriverRecovery {
        let process = process.clone();
        let snapshot = tokio::task::spawn_blocking(move || {
            GatedCliSubmission::open_existing(&process.root, process.submission_id)?
                .receipt_evidence(&process.binding, &terminal)
        })
        .await;
        let Ok(Ok(snapshot)) = snapshot else {
            return ProviderSubmitDriverRecovery::Failed(original_failure);
        };
        match self.codec.recover_receipt(call, Arc::new(snapshot)).await {
            Ok(Some(pending)) => ProviderSubmitDriverRecovery::Accepted(pending),
            Ok(None) => ProviderSubmitDriverRecovery::Failed(original_failure),
            Err(failure) => {
                tracing::warn!(
                    submission_id = %call.intent().submission_id,
                    provider_id = self.codec.provider_id(),
                    error_code = failure.code(),
                    "provider receipt recovery did not establish acceptance"
                );
                ProviderSubmitDriverRecovery::Failed(unknown_process_failure(failure.code()))
            }
        }
    }
}

fn cleanup_after_resolution(result: &ProviderSubmitDriverRecovery) -> bool {
    match result {
        ProviderSubmitDriverRecovery::Accepted(_) => true,
        ProviderSubmitDriverRecovery::Failed(failure) => {
            failure.effect() == EffectCertainty::NoRemoteEffect
        }
        ProviderSubmitDriverRecovery::AwaitingEvidence => false,
    }
}

impl<C> ProviderSubmitDriver for GatedCliSubmitDriver<C>
where
    C: GatedCliSubmitCodec,
{
    type Payload = C::Payload;
    type Prepared = GatedCliPreparedSubmission;

    fn provider_id(&self) -> &'static str {
        self.codec.provider_id()
    }

    async fn prepare(
        &self,
        call: &ProviderSubmitDriverCall<Self::Payload>,
    ) -> Result<Self::Prepared, ProviderFailure> {
        self.prepare_process(call).await
    }

    async fn dispatch(
        &self,
        prepared: Self::Prepared,
        call: &ProviderSubmitDriverCall<Self::Payload>,
    ) -> Result<PendingOperation, ProviderFailure> {
        match self.resolve_released(call, Some(prepared)).await {
            ProviderSubmitDriverRecovery::Accepted(pending) => Ok(pending),
            ProviderSubmitDriverRecovery::Failed(failure) => Err(failure),
            ProviderSubmitDriverRecovery::AwaitingEvidence => Err(unknown_process_failure(
                crate::provider_tasks::SUBMIT_CREDENTIAL_BLOCKED,
            )),
        }
    }

    async fn recover_released(
        &self,
        call: &ProviderSubmitDriverCall<Self::Payload>,
    ) -> ProviderSubmitDriverRecovery {
        self.resolve_released(call, None).await
    }
}

impl GatedProcessRef {
    fn new<P>(call: &ProviderSubmitDriverCall<P>) -> Result<Self, ProviderFailure> {
        let absolute_deadline_unix_ms =
            u64::try_from(call.execution_context().provider_deadline_at_ms())
                .map_err(|_| local_process_failure("provider_submit_deadline_invalid"))?;
        let launch_nonce = call.launch_nonce();
        let binding = GatedCliBinding::new(
            call.execution_context().execution_binding_sha256(),
            launch_nonce,
            absolute_deadline_unix_ms,
        )
        .map_err(pre_release_process_failure)?;
        Ok(Self {
            root: call.journal_root().to_owned(),
            submission_id: call.intent().submission_id,
            binding,
            attempt_key: format!(
                "{}-{}",
                call.intent().submission_id.simple(),
                launch_nonce.simple()
            ),
        })
    }

    fn evidence_deadline_unix_ms(&self) -> u64 {
        self.binding
            .absolute_deadline_unix_ms()
            .saturating_add(TERMINAL_PUBLICATION_GRACE_MS)
    }
}

async fn wait_until_prepared(
    process: GatedProcessRef,
    mut observation: GatedCliObservation,
    mut runner_exit: Option<oneshot::Receiver<Result<ExitStatus, std::io::Error>>>,
    prepare_started: Instant,
    prepare_budget: Duration,
) -> Result<GatedCliPreparedSubmission, ProviderFailure> {
    let mut runner_stopped_at = None;
    loop {
        let budget_expired = prepare_started.elapsed() >= prepare_budget;
        match observation {
            GatedCliObservation::Ready(ready) if !budget_expired => {
                return Ok(GatedCliPreparedSubmission {
                    process,
                    ready: Some(ready),
                });
            }
            GatedCliObservation::Ready(_) => {}
            GatedCliObservation::Running | GatedCliObservation::Terminal(_) => {
                return Ok(GatedCliPreparedSubmission {
                    process,
                    ready: None,
                });
            }
            GatedCliObservation::Lost { released: true, .. } => {
                return Ok(GatedCliPreparedSubmission {
                    process,
                    ready: None,
                });
            }
            GatedCliObservation::Lost {
                released: false,
                child_alive,
            } => {
                if child_alive {
                    let _ = terminate_orphan(&process).await;
                }
                return Err(local_process_failure("provider_submit_gate_lost"));
            }
            GatedCliObservation::AwaitingHelper => {
                if runner_has_exited(&mut runner_exit)? {
                    runner_stopped_at.get_or_insert_with(Instant::now);
                }
                if runner_stopped_at
                    .is_some_and(|stopped| stopped.elapsed() >= RUNNER_HANDOFF_GRACE)
                {
                    return Err(local_process_failure(
                        "provider_submit_runner_stopped_before_ready",
                    ));
                }
            }
            GatedCliObservation::Starting => {}
        }
        if unix_time_ms() >= process.evidence_deadline_unix_ms() {
            return Err(local_process_failure(
                "provider_submit_runner_ready_timeout",
            ));
        }
        tokio::time::sleep(PROCESS_OBSERVATION_INTERVAL).await;
        observation = observe_process(&process)
            .await
            .map_err(pre_release_process_failure)?;
    }
}

async fn wait_for_terminal<P>(
    process: &GatedProcessRef,
    call: &ProviderSubmitDriverCall<P>,
) -> Result<GatedCliProcessTerminal, ProcessWaitFailure> {
    loop {
        match observe_process(process).await {
            Ok(GatedCliObservation::Ready(ready)) => match release_process(process, ready, call)
                .await
            {
                Ok(()) | Err(ProcessReleaseFailure::Process(GatedCliProcessError::NotReady)) => {}
                Err(ProcessReleaseFailure::Credentials) => {
                    return Err(ProcessWaitFailure {
                        failure: unknown_process_failure(
                            crate::provider_tasks::SUBMIT_CREDENTIAL_BLOCKED,
                        ),
                        cleanup_safe: false,
                    });
                }
                Err(_) => {
                    return Err(ProcessWaitFailure {
                        failure: unknown_process_failure("provider_submit_process_release_failed"),
                        cleanup_safe: false,
                    });
                }
            },
            Ok(GatedCliObservation::Running) => {}
            Ok(GatedCliObservation::Terminal(terminal)) => return Ok(terminal),
            Ok(GatedCliObservation::Lost {
                released,
                child_alive,
            }) => {
                if child_alive {
                    let cleanup_safe = terminate_orphan(process).await.is_ok();
                    return Err(ProcessWaitFailure {
                        failure: if released {
                            unknown_process_failure("provider_submit_evidence_lost")
                        } else {
                            local_process_failure("provider_submit_gate_lost")
                        },
                        cleanup_safe,
                    });
                }
                return Err(ProcessWaitFailure {
                    failure: if released {
                        unknown_process_failure("provider_submit_evidence_lost")
                    } else {
                        local_process_failure("provider_submit_gate_lost")
                    },
                    cleanup_safe: true,
                });
            }
            Ok(GatedCliObservation::AwaitingHelper | GatedCliObservation::Starting) => {
                return Err(ProcessWaitFailure {
                    failure: unknown_process_failure("provider_submit_process_ordering_invalid"),
                    cleanup_safe: false,
                });
            }
            Err(_) => {
                return Err(ProcessWaitFailure {
                    failure: unknown_process_failure("provider_submit_process_observation_failed"),
                    cleanup_safe: false,
                });
            }
        }
        if unix_time_ms() >= process.evidence_deadline_unix_ms() {
            let cleanup_safe = terminate_orphan(process).await.is_ok();
            return Err(ProcessWaitFailure {
                failure: unknown_process_failure("provider_submit_terminal_evidence_timeout"),
                cleanup_safe,
            });
        }
        tokio::time::sleep(PROCESS_OBSERVATION_INTERVAL).await;
    }
}

struct ProcessWaitFailure {
    failure: ProviderFailure,
    cleanup_safe: bool,
}

async fn observe_process(
    process: &GatedProcessRef,
) -> Result<GatedCliObservation, GatedCliProcessError> {
    let process = process.clone();
    tokio::task::spawn_blocking(move || {
        GatedCliSubmission::new(&process.root, process.submission_id)?.observe(&process.binding)
    })
    .await
    .map_err(|_| GatedCliProcessError::Unavailable)?
}

enum ProcessReleaseFailure {
    Credentials,
    Process(GatedCliProcessError),
}

async fn release_process<P>(
    process: &GatedProcessRef,
    ready: GatedCliReady,
    call: &ProviderSubmitDriverCall<P>,
) -> Result<(), ProcessReleaseFailure> {
    let guard = match call.guard_credentials().await {
        Ok(guard) => guard,
        Err(_) => {
            // A concurrent caller may already have released this same process.
            // Observation never needs a fresh credential or a new permission.
            if matches!(
                observe_process(process).await,
                Ok(GatedCliObservation::Running | GatedCliObservation::Terminal(_))
            ) {
                return Ok(());
            }
            return Err(ProcessReleaseFailure::Credentials);
        }
    };
    let process = process.clone();
    // The blocking worker owns the lock guard, including if its caller is
    // cancelled while the durable process permission is being published.
    tokio::task::spawn_blocking(move || {
        let result = GatedCliSubmission::new(&process.root, process.submission_id)
            .and_then(|submission| submission.release(&process.binding, &ready));
        drop(guard);
        result
    })
    .await
    .map_err(|_| ProcessReleaseFailure::Process(GatedCliProcessError::Unavailable))?
    .map_err(ProcessReleaseFailure::Process)
}

async fn terminate_orphan(process: &GatedProcessRef) -> Result<bool, GatedCliProcessError> {
    let process = process.clone();
    tokio::task::spawn_blocking(move || {
        GatedCliSubmission::new(&process.root, process.submission_id)?
            .terminate_orphan(&process.binding)
    })
    .await
    .map_err(|_| GatedCliProcessError::Unavailable)?
}

fn runner_has_exited(
    receiver: &mut Option<oneshot::Receiver<Result<ExitStatus, std::io::Error>>>,
) -> Result<bool, ProviderFailure> {
    let Some(active) = receiver.as_mut() else {
        return Ok(false);
    };
    match active.try_recv() {
        Ok(Ok(_)) => {
            *receiver = None;
            Ok(true)
        }
        Ok(Err(_)) | Err(oneshot::error::TryRecvError::Closed) => {
            *receiver = None;
            Err(local_process_failure("provider_submit_runner_wait_failed"))
        }
        Err(oneshot::error::TryRecvError::Empty) => Ok(false),
    }
}

fn pre_release_process_failure(error: GatedCliProcessError) -> ProviderFailure {
    let code = match error {
        GatedCliProcessError::InvalidInput => "provider_submit_process_invalid",
        GatedCliProcessError::Conflict => "provider_submit_process_conflict",
        GatedCliProcessError::Integrity => "provider_submit_process_integrity",
        GatedCliProcessError::Unavailable => "provider_submit_process_unavailable",
        GatedCliProcessError::Busy => "provider_submit_process_busy",
        GatedCliProcessError::NotReady => "provider_submit_process_not_ready",
    };
    local_process_failure(code)
}

fn map_workspace_config_error(error: AttemptWorkspaceError) -> GatedCliProcessError {
    match error {
        AttemptWorkspaceError::InvalidConfiguration => GatedCliProcessError::InvalidInput,
        AttemptWorkspaceError::Unavailable => GatedCliProcessError::Unavailable,
        AttemptWorkspaceError::Integrity | AttemptWorkspaceError::AlreadyLocked => {
            GatedCliProcessError::Integrity
        }
    }
}

fn map_workspace_runtime_error(error: AttemptWorkspaceError) -> ProviderFailure {
    let code = match error {
        AttemptWorkspaceError::Unavailable => "provider_submit_workspace_unavailable",
        AttemptWorkspaceError::InvalidConfiguration
        | AttemptWorkspaceError::Integrity
        | AttemptWorkspaceError::AlreadyLocked => "provider_submit_workspace_invalid",
    };
    local_process_failure(code)
}

fn no_effect_terminal_failure(terminal: &GatedCliProcessTerminal) -> ProviderFailure {
    let code = match terminal.outcome() {
        GatedCliProcessOutcome::AbsoluteDeadlineElapsed => "provider_submit_deadline_elapsed",
        GatedCliProcessOutcome::GateFailed { .. } => "provider_submit_gate_failed",
        _ => "provider_submit_not_executed",
    };
    local_process_failure(code)
}

fn unknown_terminal_failure(terminal: &GatedCliProcessTerminal) -> ProviderFailure {
    let code = match terminal.outcome() {
        GatedCliProcessOutcome::TimedOut => "provider_submit_timeout",
        GatedCliProcessOutcome::AbsoluteDeadlineElapsed => "provider_submit_deadline_elapsed",
        GatedCliProcessOutcome::GateFailed { .. } => "provider_submit_gate_failed",
        GatedCliProcessOutcome::ResidualProcessGroup => "provider_submit_process_tree_lost",
        GatedCliProcessOutcome::Exited { .. } => "provider_submit_cli_failed",
    };
    unknown_process_failure(code)
}

fn nonzero_exit_diagnostic<C: GatedCliSubmitCodec>(
    codec: &C,
    outcome: &GatedCliProcessOutcome,
    stdout: &[u8],
    stdout_truncated: bool,
    stderr: &[u8],
    stderr_truncated: bool,
) -> Option<&'static str> {
    if !stdout.is_empty() || stdout_truncated || stderr_truncated {
        return None;
    }
    match outcome {
        GatedCliProcessOutcome::Exited {
            exit_code: Some(code),
            signal: None,
        } if *code != 0 => codec.nonzero_exit_diagnostic(*code, stderr),
        _ => None,
    }
}

fn local_process_failure(code: &str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Permanent,
        code,
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Never,
    )
    .expect("static provider failure must be valid")
}

fn unknown_process_failure(code: &str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Ambiguous,
        code,
        EffectCertainty::UnknownRemoteEffect,
        RetryDirective::Never,
    )
    .or_else(|_| {
        ProviderFailure::new(
            ProviderFailureClass::Ambiguous,
            "provider_submit_receipt_invalid",
            EffectCertainty::UnknownRemoteEffect,
            RetryDirective::Never,
        )
    })
    .expect("static fallback provider failure must be valid")
}

fn parse_sha256(value: &str) -> Result<[u8; 32], GatedCliProcessError> {
    let bytes = hex::decode(value).map_err(|_| GatedCliProcessError::InvalidInput)?;
    bytes
        .try_into()
        .map_err(|_| GatedCliProcessError::InvalidInput)
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;
