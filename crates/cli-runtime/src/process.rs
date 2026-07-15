use std::{
    ffi::OsString,
    fmt::Display,
    io,
    os::unix::process::ExitStatusExt,
    process::{ExitStatus, Stdio},
    time::Duration,
};

use thiserror::Error;
use tokio::{
    io::AsyncWriteExt,
    process::{Child, Command},
    runtime::Handle,
    time::{Instant, sleep, timeout, timeout_at},
};

use crate::command::{CommandSpec, CommandSpecError};

const KILL_REAP_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpawnEvidence {
    pub pid: u32,
    /// The initial process group used for cleanup. Descendants that call `setsid(2)` or move to
    /// another group are no longer covered by group-wide termination.
    pub process_group_id: u32,
}

#[derive(Debug)]
pub struct ProcessCompletion {
    pub evidence: SpawnEvidence,
    pub status: ExitStatus,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TokioProcessBackend;

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopSpawnObserver;

pub trait SpawnObserver {
    type Error: Display + Send;

    fn observe_spawn(&mut self, evidence: &SpawnEvidence) -> Result<(), Self::Error>;
}

pub trait ProcessBackend {
    fn execute<'a, O>(
        &'a self,
        command: &'a CommandSpec,
        observer: &'a mut O,
    ) -> impl std::future::Future<Output = Result<ProcessCompletion, ProcessError>> + Send + 'a
    where
        O: SpawnObserver + Send + 'a;
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error(transparent)]
    InvalidCommand(#[from] CommandSpecError),
    #[error("process spawn failed: {0}")]
    Spawn(#[source] io::Error),
    #[error("spawned process identity is unavailable")]
    IdentityUnavailable,
    #[error("spawn observer failed: {message}; cleanup error: {cleanup_error:?}")]
    Observer {
        message: String,
        cleanup_error: Option<String>,
    },
    #[error("process stdin failed: {source}; cleanup error: {cleanup_error:?}")]
    Stdin {
        source: io::Error,
        cleanup_error: Option<String>,
    },
    #[error("process exceeded its hard wall timeout; cleanup error: {cleanup_error:?}")]
    TimedOut {
        evidence: SpawnEvidence,
        cleanup_error: Option<String>,
    },
    #[error("process wait failed: {source}; cleanup error: {cleanup_error:?}")]
    Wait {
        source: io::Error,
        cleanup_error: Option<String>,
    },
    #[error(
        "process leader exited while its process group remained alive; cleanup error: {cleanup_error:?}"
    )]
    ResidualProcessGroup { cleanup_error: Option<String> },
}

impl SpawnObserver for NoopSpawnObserver {
    type Error = std::convert::Infallible;

    fn observe_spawn(&mut self, _evidence: &SpawnEvidence) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct ChildProcessGuard {
    child: Option<Child>,
    evidence: SpawnEvidence,
    armed: bool,
}

impl ChildProcessGuard {
    fn new(child: Child, evidence: SpawnEvidence) -> Self {
        Self {
            child: Some(child),
            evidence,
            armed: true,
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("child guard is populated")
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        // This reaches only the original process group. Descendants that call setsid(2) or
        // otherwise leave that group are outside this runtime's process-group guarantee.
        let _ = signal_process_group(self.evidence.process_group_id, libc::SIGKILL);
        let Some(mut child) = self.child.take() else {
            return;
        };
        let _ = child.start_kill();

        if let Ok(handle) = Handle::try_current() {
            std::mem::drop(handle.spawn(async move {
                let _ = timeout(KILL_REAP_TIMEOUT, child.wait()).await;
            }));
        }
    }
}

impl TokioProcessBackend {
    pub async fn execute_process<O>(
        &self,
        spec: &CommandSpec,
        observer: &mut O,
    ) -> Result<ProcessCompletion, ProcessError>
    where
        O: SpawnObserver + Send,
    {
        execute_process(spec, observer).await
    }
}

impl ProcessBackend for TokioProcessBackend {
    fn execute<'a, O>(
        &'a self,
        spec: &'a CommandSpec,
        observer: &'a mut O,
    ) -> impl std::future::Future<Output = Result<ProcessCompletion, ProcessError>> + Send + 'a
    where
        O: SpawnObserver + Send + 'a,
    {
        self.execute_process(spec, observer)
    }
}

async fn execute_process<O>(
    spec: &CommandSpec,
    observer: &mut O,
) -> Result<ProcessCompletion, ProcessError>
where
    O: SpawnObserver + Send,
{
    spec.revalidate()?;
    let mut command = build_command(spec);
    let child = command.spawn().map_err(ProcessError::Spawn)?;
    let Some(pid) = child.id() else {
        let _ = cleanup_unidentified_child(child).await;
        return Err(ProcessError::IdentityUnavailable);
    };
    if pid > libc::pid_t::MAX as u32 {
        let _ = cleanup_unidentified_child(child).await;
        return Err(ProcessError::IdentityUnavailable);
    }
    let evidence = SpawnEvidence {
        pid,
        process_group_id: pid,
    };
    let mut process = ChildProcessGuard::new(child, evidence);
    let deadline = Instant::now() + spec.wall_timeout();

    match observer.observe_spawn(&evidence) {
        Ok(()) => {}
        Err(error) => {
            let cleanup_error = cleanup(process, spec.termination_grace()).await;
            return Err(ProcessError::Observer {
                message: error.to_string(),
                cleanup_error,
            });
        }
    }

    if let Some(mut stdin) = process.child_mut().stdin.take() {
        let write_result = timeout_at(deadline, async {
            stdin.write_all(spec.stdin_bytes()).await?;
            stdin.shutdown().await
        })
        .await;
        match write_result {
            Ok(Ok(())) => {}
            Ok(Err(source)) => {
                let cleanup_error = cleanup(process, spec.termination_grace()).await;
                return Err(ProcessError::Stdin {
                    source,
                    cleanup_error,
                });
            }
            Err(_) => {
                let cleanup_error = cleanup(process, spec.termination_grace()).await;
                return Err(ProcessError::TimedOut {
                    evidence,
                    cleanup_error,
                });
            }
        }
    }

    match timeout_at(deadline, process.child_mut().wait()).await {
        Ok(Ok(status)) => {
            match terminate_remaining_group(evidence, spec.termination_grace()).await {
                Ok(false) => {
                    process.disarm();
                    Ok(ProcessCompletion { evidence, status })
                }
                Ok(true) => {
                    process.disarm();
                    Err(ProcessError::ResidualProcessGroup {
                        cleanup_error: None,
                    })
                }
                Err(error) => Err(ProcessError::ResidualProcessGroup {
                    cleanup_error: Some(error),
                }),
            }
        }
        Ok(Err(source)) => {
            let cleanup_error = cleanup(process, spec.termination_grace()).await;
            Err(ProcessError::Wait {
                source,
                cleanup_error,
            })
        }
        Err(_) => {
            let cleanup_error = cleanup(process, spec.termination_grace()).await;
            Err(ProcessError::TimedOut {
                evidence,
                cleanup_error,
            })
        }
    }
}

fn build_command(spec: &CommandSpec) -> Command {
    let mut command = Command::new(spec.executable().path());
    command
        .args(spec.arguments())
        .current_dir(spec.working_directory().path())
        .env_clear()
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .process_group(0);
    for (key, value) in spec.environment() {
        command.env(key, value);
    }
    if spec.stdin_bytes().is_empty() {
        command.stdin(Stdio::null());
    } else {
        command.stdin(Stdio::piped());
    }
    command
}

async fn cleanup(mut process: ChildProcessGuard, grace: Duration) -> Option<String> {
    let mut errors = Vec::new();
    let mut leader_reaped = false;
    if let Err(error) = signal_process_group(process.evidence.process_group_id, libc::SIGTERM) {
        errors.push(format!("TERM: {error}"));
    }

    let grace_wait = timeout(grace, process.child_mut().wait()).await;
    if grace_wait.is_err() {
        if let Err(error) = signal_process_group(process.evidence.process_group_id, libc::SIGKILL) {
            errors.push(format!("KILL group: {error}"));
        }
        if let Err(error) = process.child_mut().start_kill()
            && error.kind() != io::ErrorKind::InvalidInput
        {
            errors.push(format!("KILL leader: {error}"));
        }
        match reap_after_kill(process.child_mut()).await {
            Ok(()) => leader_reaped = true,
            Err(error) => errors.push(error),
        }
    } else if let Ok(wait_result) = grace_wait {
        match wait_result {
            Ok(_) => leader_reaped = true,
            Err(error) => {
                errors.push(format!("wait after TERM: {error}"));
                if let Err(error) =
                    signal_process_group(process.evidence.process_group_id, libc::SIGKILL)
                {
                    errors.push(format!("KILL group: {error}"));
                }
                if let Err(error) = process.child_mut().start_kill()
                    && error.kind() != io::ErrorKind::InvalidInput
                {
                    errors.push(format!("KILL leader: {error}"));
                }
                match reap_after_kill(process.child_mut()).await {
                    Ok(()) => leader_reaped = true,
                    Err(error) => errors.push(error),
                }
            }
        }
    }

    let group_gone = match terminate_remaining_group(process.evidence, grace).await {
        Ok(_) => true,
        Err(error) => {
            errors.push(error);
            false
        }
    };
    if leader_reaped && group_gone {
        process.disarm();
    }

    if errors.is_empty() {
        None
    } else {
        Some(errors.join("; "))
    }
}

async fn cleanup_unidentified_child(mut child: Child) -> Option<String> {
    let mut errors = Vec::new();
    if let Err(error) = child.start_kill()
        && error.kind() != io::ErrorKind::InvalidInput
    {
        errors.push(format!("KILL leader: {error}"));
    }
    if let Err(error) = reap_after_kill(&mut child).await {
        errors.push(error);
    }

    if errors.is_empty() {
        None
    } else {
        Some(errors.join("; "))
    }
}

async fn reap_after_kill(child: &mut Child) -> Result<(), String> {
    match timeout(KILL_REAP_TIMEOUT, child.wait()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(format!("wait after KILL: {error}")),
        Err(_) => Err(format!(
            "wait after KILL exceeded {} ms",
            KILL_REAP_TIMEOUT.as_millis()
        )),
    }
}

async fn terminate_remaining_group(
    evidence: SpawnEvidence,
    grace: Duration,
) -> Result<bool, String> {
    let exists = process_group_exists(evidence.process_group_id)
        .map_err(|error| format!("probe process group: {error}"))?;
    if !exists {
        return Ok(false);
    }
    signal_process_group(evidence.process_group_id, libc::SIGKILL)
        .map_err(|error| format!("KILL residual process group: {error}"))?;
    let deadline = Instant::now() + grace;
    loop {
        if !process_group_exists(evidence.process_group_id)
            .map_err(|error| format!("verify process group cleanup: {error}"))?
        {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Err("residual process group did not terminate before the deadline".to_string());
        }
        sleep(std::time::Duration::from_millis(10)).await;
    }
}

fn process_group_exists(process_group_id: u32) -> io::Result<bool> {
    let result = unsafe { libc::kill(-(process_group_id as libc::pid_t), 0) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(error),
    }
}

fn signal_process_group(process_group_id: u32, signal: libc::c_int) -> io::Result<()> {
    let result = unsafe { libc::kill(-(process_group_id as libc::pid_t), signal) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

pub(crate) fn status_description(status: &ExitStatus) -> OsString {
    match (status.code(), status.signal()) {
        (Some(code), _) => format!("exit_{code}").into(),
        (None, Some(signal)) => format!("signal_{signal}").into(),
        (None, None) => "unknown_exit".into(),
    }
}
