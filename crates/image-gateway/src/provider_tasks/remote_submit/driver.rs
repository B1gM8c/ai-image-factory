use std::{
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use image_cli_runtime::VerifiedExecutable;
use image_provider_sdk::{
    CanonicalCommandPayload, EffectCertainty, PendingOperation, ProviderFailure,
    ProviderFailureClass, RetryDirective,
};
use tokio::sync::{Mutex, oneshot};
use uuid::Uuid;

use super::process::{
    GatedCliBinding, GatedCliCommand, GatedCliObservation, GatedCliProcessError,
    GatedCliProcessOutcome, GatedCliProcessTerminal, GatedCliReady, GatedCliSubmission,
};
use crate::provider_tasks::{
    ProviderExecutionContext, ProviderSubmitDriver, ProviderSubmitDriverCall,
    ProviderSubmitDriverRecovery, ProviderSubmitIntent,
};

const PROCESS_OBSERVATION_INTERVAL: Duration = Duration::from_millis(10);
const RUNNER_HANDOFF_GRACE: Duration = Duration::from_millis(500);
const TERMINAL_PUBLICATION_GRACE_MS: u64 = 2_000;

pub trait GatedCliSubmitCodec: Send + Sync + 'static {
    type Payload: CanonicalCommandPayload + Send + Sync + 'static;

    fn provider_id(&self) -> &'static str;

    fn command(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        command: &image_provider_sdk::SingleOutputCommand<Self::Payload>,
    ) -> Result<GatedCliCommand, ProviderFailure>;

    fn decode_receipt(
        &self,
        intent: &ProviderSubmitIntent,
        command: &image_provider_sdk::SingleOutputCommand<Self::Payload>,
        stdout: &[u8],
    ) -> Result<PendingOperation, ProviderFailure>;
}

pub struct GatedCliSubmitDriver<C> {
    codec: Arc<C>,
    runner_executable: PathBuf,
    runner_sha256: [u8; 32],
    runner_start: Mutex<()>,
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
}

impl<C> GatedCliSubmitDriver<C>
where
    C: GatedCliSubmitCodec,
{
    pub fn new(
        codec: C,
        runner_executable: impl AsRef<Path>,
        runner_sha256: impl AsRef<str>,
    ) -> Result<Self, GatedCliProcessError> {
        let runner_sha256 = parse_sha256(runner_sha256.as_ref())?;
        let runner = VerifiedExecutable::new_with_sha256(runner_executable, runner_sha256)
            .map_err(|_| GatedCliProcessError::InvalidInput)?;
        Ok(Self {
            codec: Arc::new(codec),
            runner_executable: runner.path().to_owned(),
            runner_sha256,
            runner_start: Mutex::new(()),
        })
    }

    async fn prepare_process(
        &self,
        call: &ProviderSubmitDriverCall<C::Payload>,
    ) -> Result<GatedCliPreparedSubmission, ProviderFailure> {
        let prepare_started = Instant::now();
        let codec = Arc::clone(&self.codec);
        let owned_call = call.clone();
        let command = tokio::task::spawn_blocking(move || {
            codec.command(
                owned_call.intent(),
                owned_call.execution_context(),
                owned_call.command(),
            )
        })
        .await
        .map_err(|_| local_process_failure("provider_submit_codec_worker_stopped"))?
        .map_err(|failure| {
            if failure.effect() == EffectCertainty::NoRemoteEffect {
                failure
            } else {
                local_process_failure("provider_submit_codec_effect_invalid")
            }
        })?;
        let process = GatedProcessRef::new(call)?;
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
                Some(self.spawn_runner(&process).await?)
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
                    match release_process(&prepared.process, ready).await {
                        Ok(()) | Err(GatedCliProcessError::NotReady) => {}
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
        match wait_for_terminal(&process).await {
            Ok(terminal) => self.resolve_terminal(call, terminal).await,
            Err(failure) => ProviderSubmitDriverRecovery::Failed(failure),
        }
    }

    async fn resolve_terminal(
        &self,
        call: &ProviderSubmitDriverCall<C::Payload>,
        terminal: GatedCliProcessTerminal,
    ) -> ProviderSubmitDriverRecovery {
        if !terminal.released() || !terminal.exec_started() {
            return ProviderSubmitDriverRecovery::Failed(no_effect_terminal_failure(&terminal));
        }
        if terminal.stdout_truncated() {
            return ProviderSubmitDriverRecovery::Failed(unknown_process_failure(
                "provider_submit_receipt_truncated",
            ));
        }
        if !matches!(
            terminal.outcome(),
            GatedCliProcessOutcome::Exited {
                exit_code: Some(0),
                signal: None,
            }
        ) {
            return ProviderSubmitDriverRecovery::Failed(unknown_terminal_failure(&terminal));
        }
        let codec = Arc::clone(&self.codec);
        let owned_call = call.clone();
        let stdout = terminal.stdout().to_vec();
        match tokio::task::spawn_blocking(move || {
            codec.decode_receipt(owned_call.intent(), owned_call.command(), &stdout)
        })
        .await
        {
            Ok(Ok(pending)) => ProviderSubmitDriverRecovery::Accepted(pending),
            Ok(Err(failure)) => {
                ProviderSubmitDriverRecovery::Failed(unknown_process_failure(failure.code()))
            }
            Err(_) => ProviderSubmitDriverRecovery::Failed(unknown_process_failure(
                "provider_submit_receipt_worker_stopped",
            )),
        }
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
            ProviderSubmitDriverRecovery::AwaitingEvidence => {
                Err(unknown_process_failure("provider_submit_evidence_pending"))
            }
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
        let binding = GatedCliBinding::new(
            call.execution_context().execution_binding_sha256(),
            call.launch_nonce(),
            absolute_deadline_unix_ms,
        )
        .map_err(pre_release_process_failure)?;
        Ok(Self {
            root: call.journal_root().to_owned(),
            submission_id: call.intent().submission_id,
            binding,
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
        if prepare_started.elapsed() >= prepare_budget {
            let _ = terminate_orphan(&process).await;
            return Err(local_process_failure("provider_submit_deadline_elapsed"));
        }
        match observation {
            GatedCliObservation::Ready(ready) => {
                return Ok(GatedCliPreparedSubmission {
                    process,
                    ready: Some(ready),
                });
            }
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

async fn wait_for_terminal(
    process: &GatedProcessRef,
) -> Result<GatedCliProcessTerminal, ProviderFailure> {
    loop {
        match observe_process(process).await {
            Ok(GatedCliObservation::Ready(ready)) => match release_process(process, ready).await {
                Ok(()) | Err(GatedCliProcessError::NotReady) => {}
                Err(_) => {
                    return Err(unknown_process_failure(
                        "provider_submit_process_release_failed",
                    ));
                }
            },
            Ok(GatedCliObservation::Running) => {}
            Ok(GatedCliObservation::Terminal(terminal)) => return Ok(terminal),
            Ok(GatedCliObservation::Lost {
                released,
                child_alive,
            }) => {
                if child_alive {
                    let _ = terminate_orphan(process).await;
                }
                return Err(if released {
                    unknown_process_failure("provider_submit_evidence_lost")
                } else {
                    local_process_failure("provider_submit_gate_lost")
                });
            }
            Ok(GatedCliObservation::AwaitingHelper | GatedCliObservation::Starting) => {
                return Err(unknown_process_failure(
                    "provider_submit_process_ordering_invalid",
                ));
            }
            Err(_) => {
                return Err(unknown_process_failure(
                    "provider_submit_process_observation_failed",
                ));
            }
        }
        if unix_time_ms() >= process.evidence_deadline_unix_ms() {
            let _ = terminate_orphan(process).await;
            return Err(unknown_process_failure(
                "provider_submit_terminal_evidence_timeout",
            ));
        }
        tokio::time::sleep(PROCESS_OBSERVATION_INTERVAL).await;
    }
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

async fn release_process(
    process: &GatedProcessRef,
    ready: GatedCliReady,
) -> Result<(), GatedCliProcessError> {
    let process = process.clone();
    tokio::task::spawn_blocking(move || {
        GatedCliSubmission::new(&process.root, process.submission_id)?
            .release(&process.binding, &ready)
    })
    .await
    .map_err(|_| GatedCliProcessError::Unavailable)?
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
