use std::{
    collections::BTreeMap,
    fs,
    os::fd::OwnedFd,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(test)]
use base64::{Engine as _, engine::general_purpose::STANDARD};
use image_cli_runtime::{CommandSpec, MAX_STDIN_BYTES, VerifiedExecutable, WorkingDirectory};
use rustix::{
    fs::{self as rfs, Mode, OFlags},
    io::Errno,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

mod protocol;
mod runner;
mod unix;

use protocol::{
    DiskChildIdentity, DiskExecStart, DiskHelperIdentity, DiskOutcome, DiskRelease, DiskRequest,
    DiskTerminal,
};
pub use runner::{run_remote_submit_gate, run_remote_submit_runner};
#[cfg(test)]
use unix::unix_time_ms;
use unix::{
    ensure_lock_file, try_exclusive_lock, unlock, validate_bound_directory, validate_lock_stat,
};

#[cfg(test)]
use super::journal::sha256;
use super::journal::{
    RemoteSubmitJournalError, prepare_root, publish_bytes, publish_or_compare, read_optional_bytes,
    read_optional_json, read_required_json, valid_sha256, validate_directory,
};

const REQUEST_FILE: &str = "process-request.json";
const STDIN_FILE: &str = "process-stdin.bin";
const LOCK_FILE: &str = "process-runner.lock";
const HELPER_FILE: &str = "process-helper.json";
const CHILD_FILE: &str = "process-ready.json";
const RELEASE_FILE: &str = "process-dispatch-released.json";
const EXEC_STARTED_FILE: &str = "process-exec-started.json";
const TERMINAL_FILE: &str = "process-terminal.json";
const REQUEST_SCHEMA: &str = "ai-image-factory/gated-cli-request/v1";
const REQUEST_VERSION: u16 = 1;
const MAX_REQUEST_BYTES: u64 = 256 * 1024;
const MAX_IDENTITY_BYTES: u64 = 64 * 1024;
const MAX_TERMINAL_BYTES: u64 = 256 * 1024;
const MAX_CAPTURED_STREAM_BYTES: usize = 64 * 1024;
const MAX_ARGUMENTS: usize = 256;
const MAX_ENVIRONMENT: usize = 128;
const MAX_FIELD_BYTES: usize = MAX_REQUEST_BYTES as usize;
const MAX_EXEC_STATUS_BYTES: usize = 256;
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Debug, Eq, PartialEq)]
/// Immutable identity required to authorize one local CLI release.
pub struct GatedCliBinding {
    execution_binding_sha256: String,
    launch_nonce: Uuid,
    absolute_deadline_unix_ms: u64,
}

#[derive(Clone, Debug)]
/// A frozen CLI command whose non-stdin fields are persisted in the private journal.
///
/// Environment values are durable evidence and must not contain credentials or other secrets.
pub struct GatedCliCommand {
    executable: String,
    executable_sha256: String,
    working_directory: String,
    arguments: Vec<String>,
    environment: BTreeMap<String, String>,
    stdin: Vec<u8>,
    wall_timeout_ms: u64,
    termination_grace_ms: u64,
}

pub struct GatedCliSubmission {
    root_path: PathBuf,
    entry_path: PathBuf,
    directory: OwnedFd,
    submission_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Unforgeable ready evidence returned only after the blocked child identity is durable.
pub struct GatedCliReady {
    execution_binding_sha256: String,
    launch_nonce: Uuid,
    helper_nonce: Uuid,
    child_nonce: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Durable process observation for recovery and release decisions.
pub enum GatedCliObservation {
    AwaitingHelper,
    Starting,
    Ready(GatedCliReady),
    Running,
    Terminal(GatedCliProcessTerminal),
    Lost { released: bool, child_alive: bool },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatedCliProcessOutcome {
    Exited {
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
    TimedOut,
    AbsoluteDeadlineElapsed,
    GateFailed {
        error_code: String,
    },
    ResidualProcessGroup,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatedCliProcessTerminal {
    released: bool,
    exec_started: bool,
    outcome: GatedCliProcessOutcome,
    stdout: Vec<u8>,
    stdout_truncated: bool,
    stderr: Vec<u8>,
    stderr_truncated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GatedCliProcessError {
    #[error("gated CLI process input is invalid")]
    InvalidInput,
    #[error("gated CLI process evidence conflicts with durable state")]
    Conflict,
    #[error("gated CLI process evidence failed integrity validation")]
    Integrity,
    #[error("gated CLI process storage or operating-system support is unavailable")]
    Unavailable,
    #[error("gated CLI process helper is already active")]
    Busy,
    #[error("gated CLI process is not ready for release")]
    NotReady,
}

struct HelperLock {
    file: fs::File,
    device: u64,
    inode: u64,
}

struct CapturedStream {
    bytes: Vec<u8>,
    truncated: bool,
}

impl GatedCliBinding {
    pub fn new(
        execution_binding_sha256: impl Into<String>,
        launch_nonce: Uuid,
        absolute_deadline_unix_ms: u64,
    ) -> Result<Self, GatedCliProcessError> {
        let binding = Self {
            execution_binding_sha256: execution_binding_sha256.into(),
            launch_nonce,
            absolute_deadline_unix_ms,
        };
        binding.validate()?;
        Ok(binding)
    }

    fn validate(&self) -> Result<(), GatedCliProcessError> {
        if !valid_sha256(&self.execution_binding_sha256)
            || self.launch_nonce.is_nil()
            || self.absolute_deadline_unix_ms == 0
        {
            return Err(GatedCliProcessError::InvalidInput);
        }
        Ok(())
    }

    pub(crate) fn absolute_deadline_unix_ms(&self) -> u64 {
        self.absolute_deadline_unix_ms
    }
}

impl GatedCliCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        executable: impl AsRef<Path>,
        executable_sha256: impl Into<String>,
        working_directory: impl AsRef<Path>,
        arguments: Vec<String>,
        environment: BTreeMap<String, String>,
        stdin: Vec<u8>,
        wall_timeout: Duration,
        termination_grace: Duration,
    ) -> Result<Self, GatedCliProcessError> {
        let executable_sha256 = executable_sha256.into();
        let expected_digest = parse_sha256(&executable_sha256)?;
        let executable = VerifiedExecutable::new_with_sha256(executable, expected_digest)
            .map_err(|_| GatedCliProcessError::InvalidInput)?;
        let working_directory = WorkingDirectory::new(working_directory)
            .map_err(|_| GatedCliProcessError::InvalidInput)?;
        let mut command = CommandSpec::new_receipt(
            executable.clone(),
            working_directory.clone(),
            wall_timeout,
            termination_grace,
        )
        .map_err(|_| GatedCliProcessError::InvalidInput)?;
        if arguments.len() > MAX_ARGUMENTS || environment.len() > MAX_ENVIRONMENT {
            return Err(GatedCliProcessError::InvalidInput);
        }
        for argument in &arguments {
            validate_value(argument)?;
            command = command
                .arg(argument)
                .map_err(|_| GatedCliProcessError::InvalidInput)?;
        }
        for (name, value) in &environment {
            validate_environment_name(name)?;
            validate_value(value)?;
            command = command
                .env(name, value)
                .map_err(|_| GatedCliProcessError::InvalidInput)?;
        }
        let _command = command
            .stdin(stdin.clone())
            .map_err(|_| GatedCliProcessError::InvalidInput)?;
        let executable = executable
            .path()
            .to_str()
            .ok_or(GatedCliProcessError::InvalidInput)?
            .to_owned();
        let working_directory = working_directory
            .path()
            .to_str()
            .ok_or(GatedCliProcessError::InvalidInput)?
            .to_owned();
        let wall_timeout_ms = duration_millis(wall_timeout)?;
        let termination_grace_ms = duration_millis(termination_grace)?;
        let value = Self {
            executable,
            executable_sha256,
            working_directory,
            arguments,
            environment,
            stdin,
            wall_timeout_ms,
            termination_grace_ms,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), GatedCliProcessError> {
        if !valid_sha256(&self.executable_sha256)
            || self.executable.is_empty()
            || self.working_directory.is_empty()
            || self.arguments.len() > MAX_ARGUMENTS
            || self.environment.len() > MAX_ENVIRONMENT
            || self.stdin.len() > MAX_STDIN_BYTES
            || self.wall_timeout_ms == 0
            || self.termination_grace_ms == 0
        {
            return Err(GatedCliProcessError::InvalidInput);
        }
        validate_nonempty_value(&self.executable)?;
        validate_nonempty_value(&self.working_directory)?;
        for argument in &self.arguments {
            validate_value(argument)?;
        }
        for (name, value) in &self.environment {
            validate_environment_name(name)?;
            validate_value(value)?;
        }
        Ok(())
    }
}

impl GatedCliSubmission {
    pub fn new(root: impl AsRef<Path>, submission_id: Uuid) -> Result<Self, GatedCliProcessError> {
        if submission_id.is_nil() {
            return Err(GatedCliProcessError::InvalidInput);
        }
        let root_path = prepare_root(root.as_ref()).map_err(map_journal_error)?;
        let root = rfs::open(
            &root_path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| GatedCliProcessError::Integrity)?;
        validate_directory(&root, RemoteSubmitJournalError::Integrity)
            .map_err(map_journal_error)?;
        let name = submission_id.simple().to_string();
        match rfs::mkdirat(&root, &name, Mode::RWXU) {
            Ok(()) => rfs::fsync(&root).map_err(|_| GatedCliProcessError::Unavailable)?,
            Err(Errno::EXIST) => {}
            Err(_) => return Err(GatedCliProcessError::Unavailable),
        }
        let directory = rfs::openat(
            &root,
            &name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| GatedCliProcessError::Integrity)?;
        validate_directory(&directory, RemoteSubmitJournalError::Integrity)
            .map_err(map_journal_error)?;
        ensure_lock_file(&directory)?;
        let entry_path = root_path.join(name);
        validate_bound_directory(&entry_path, &directory)?;
        Ok(Self {
            root_path,
            entry_path,
            directory,
            submission_id,
        })
    }

    pub fn submission_id(&self) -> Uuid {
        self.submission_id
    }

    pub fn prepare(
        &self,
        binding: &GatedCliBinding,
        command: &GatedCliCommand,
    ) -> Result<(), GatedCliProcessError> {
        binding.validate()?;
        command.validate()?;
        let request = DiskRequest::new(self.submission_id, binding, command)?;
        let bytes = serialize_bounded(&request, MAX_REQUEST_BYTES)?;
        if command.stdin.is_empty() {
            if read_optional_bytes(&self.directory, STDIN_FILE, MAX_STDIN_BYTES as u64)
                .map_err(map_journal_error)?
                .is_some()
            {
                return Err(GatedCliProcessError::Conflict);
            }
        } else {
            publish_or_compare(
                &self.directory,
                STDIN_FILE,
                &command.stdin,
                MAX_STDIN_BYTES as u64,
            )
            .map_err(map_journal_error)?;
        }
        publish_or_compare(&self.directory, REQUEST_FILE, &bytes, MAX_REQUEST_BYTES)
            .map_err(map_journal_error)
    }

    pub fn observe(
        &self,
        binding: &GatedCliBinding,
    ) -> Result<GatedCliObservation, GatedCliProcessError> {
        let request = self.read_request(binding)?;
        let helper = read_optional_json::<DiskHelperIdentity>(
            &self.directory,
            HELPER_FILE,
            MAX_IDENTITY_BYTES,
        )
        .map_err(map_journal_error)?;
        let child = read_optional_json::<DiskChildIdentity>(
            &self.directory,
            CHILD_FILE,
            MAX_IDENTITY_BYTES,
        )
        .map_err(map_journal_error)?;
        let release =
            read_optional_json::<DiskRelease>(&self.directory, RELEASE_FILE, MAX_IDENTITY_BYTES)
                .map_err(map_journal_error)?;
        let exec_start = read_optional_json::<DiskExecStart>(
            &self.directory,
            EXEC_STARTED_FILE,
            MAX_IDENTITY_BYTES,
        )
        .map_err(map_journal_error)?;
        let terminal =
            read_optional_json::<DiskTerminal>(&self.directory, TERMINAL_FILE, MAX_TERMINAL_BYTES)
                .map_err(map_journal_error)?;
        if child.is_some() && helper.is_none()
            || release.is_some() && child.is_none()
            || exec_start.is_some() && release.is_none()
            || terminal.is_some() && child.is_none()
        {
            return Err(GatedCliProcessError::Integrity);
        }
        if let Some(helper) = &helper {
            helper.validate()?;
            self.validate_lock_binding(helper)?;
        }
        if let (Some(helper), Some(child)) = (&helper, &child) {
            child.validate(helper)?;
        }
        if let (Some(request), Some(helper), Some(child), Some(release)) =
            (Some(&request), &helper, &child, &release)
        {
            release.validate(request, helper, child)?;
        }
        if let (Some(helper), Some(child), Some(exec_start)) = (&helper, &child, &exec_start) {
            exec_start.validate(&request, helper, child, release.as_ref())?;
        }
        if let Some(terminal) = terminal {
            let helper = helper.as_ref().ok_or(GatedCliProcessError::Integrity)?;
            let child = child.as_ref().ok_or(GatedCliProcessError::Integrity)?;
            terminal.validate(
                &request,
                helper,
                child,
                release.as_ref(),
                exec_start.as_ref(),
            )?;
            return Ok(GatedCliObservation::Terminal(terminal.into_public()?));
        }
        let Some(helper) = helper else {
            return Ok(GatedCliObservation::AwaitingHelper);
        };
        let mut helper_current = helper.is_current_process()?;
        let mut lock_held = self.helper_lock_is_held(&helper)?;
        if helper_current != lock_held {
            if let Some(terminal) = read_optional_json::<DiskTerminal>(
                &self.directory,
                TERMINAL_FILE,
                MAX_TERMINAL_BYTES,
            )
            .map_err(map_journal_error)?
            {
                let child = child.as_ref().ok_or(GatedCliProcessError::Integrity)?;
                terminal.validate(
                    &request,
                    &helper,
                    child,
                    release.as_ref(),
                    exec_start.as_ref(),
                )?;
                return Ok(GatedCliObservation::Terminal(terminal.into_public()?));
            }
            helper_current = helper.is_current_process()?;
            lock_held = self.helper_lock_is_held(&helper)?;
            if helper_current != lock_held {
                if let Some(terminal) = read_optional_json::<DiskTerminal>(
                    &self.directory,
                    TERMINAL_FILE,
                    MAX_TERMINAL_BYTES,
                )
                .map_err(map_journal_error)?
                {
                    let child = child.as_ref().ok_or(GatedCliProcessError::Integrity)?;
                    terminal.validate(
                        &request,
                        &helper,
                        child,
                        release.as_ref(),
                        exec_start.as_ref(),
                    )?;
                    return Ok(GatedCliObservation::Terminal(terminal.into_public()?));
                }
                return Err(GatedCliProcessError::Integrity);
            }
        }
        if helper_current {
            let Some(child) = child else {
                return Ok(GatedCliObservation::Starting);
            };
            if child.is_current_process()? {
                return Ok(match release {
                    Some(_) => GatedCliObservation::Running,
                    None => {
                        GatedCliObservation::Ready(GatedCliReady::new(&request, &helper, &child))
                    }
                });
            }
            return Ok(GatedCliObservation::Running);
        }
        let child_alive = child
            .as_ref()
            .map(DiskChildIdentity::is_current_process)
            .transpose()?
            .unwrap_or(false);
        Ok(GatedCliObservation::Lost {
            released: release.is_some(),
            child_alive,
        })
    }

    pub fn release(
        &self,
        binding: &GatedCliBinding,
        ready: &GatedCliReady,
    ) -> Result<(), GatedCliProcessError> {
        if unix::unix_time_ms()? >= binding.absolute_deadline_unix_ms {
            return Err(GatedCliProcessError::NotReady);
        }
        let request = self.read_request(binding)?;
        let helper = read_required_json::<DiskHelperIdentity>(
            &self.directory,
            HELPER_FILE,
            MAX_IDENTITY_BYTES,
        )
        .map_err(map_journal_error)?;
        let child = read_required_json::<DiskChildIdentity>(
            &self.directory,
            CHILD_FILE,
            MAX_IDENTITY_BYTES,
        )
        .map_err(map_journal_error)?;
        helper.validate()?;
        child.validate(&helper)?;
        self.validate_lock_binding(&helper)?;
        if *ready != GatedCliReady::new(&request, &helper, &child)
            || !helper.is_current_process()?
            || !self.helper_lock_is_held(&helper)?
            || !child.is_current_process()?
            || unix::unix_time_ms()? >= binding.absolute_deadline_unix_ms
        {
            return Err(GatedCliProcessError::NotReady);
        }
        let release = DiskRelease::new(&request, &helper, &child);
        let bytes = serialize_bounded(&release, MAX_IDENTITY_BYTES)?;
        match publish_bytes(&self.directory, RELEASE_FILE, &bytes, MAX_IDENTITY_BYTES)
            .map_err(map_journal_error)?
        {
            true => Ok(()),
            false => {
                let existing = read_required_json::<DiskRelease>(
                    &self.directory,
                    RELEASE_FILE,
                    MAX_IDENTITY_BYTES,
                )
                .map_err(map_journal_error)?;
                if existing == release {
                    Ok(())
                } else {
                    Err(GatedCliProcessError::Conflict)
                }
            }
        }
    }

    pub fn terminate_orphan(
        &self,
        binding: &GatedCliBinding,
    ) -> Result<bool, GatedCliProcessError> {
        let _request = self.read_request(binding)?;
        let helper = read_required_json::<DiskHelperIdentity>(
            &self.directory,
            HELPER_FILE,
            MAX_IDENTITY_BYTES,
        )
        .map_err(map_journal_error)?;
        let child = read_required_json::<DiskChildIdentity>(
            &self.directory,
            CHILD_FILE,
            MAX_IDENTITY_BYTES,
        )
        .map_err(map_journal_error)?;
        helper.validate()?;
        child.validate(&helper)?;
        self.validate_lock_binding(&helper)?;
        if helper.is_current_process()? || self.helper_lock_is_held(&helper)? {
            return Err(GatedCliProcessError::Busy);
        }
        child.kill_process_group_if_current()
    }

    fn read_request(&self, binding: &GatedCliBinding) -> Result<DiskRequest, GatedCliProcessError> {
        binding.validate()?;
        validate_bound_directory(&self.entry_path, &self.directory)?;
        let request =
            read_required_json::<DiskRequest>(&self.directory, REQUEST_FILE, MAX_REQUEST_BYTES)
                .map_err(map_journal_error)?;
        request.validate(self.submission_id, binding)?;
        let stdin = read_optional_bytes(&self.directory, STDIN_FILE, MAX_STDIN_BYTES as u64)
            .map_err(map_journal_error)?;
        request.validate_stdin(stdin.as_deref())?;
        Ok(request)
    }

    fn read_unbound_request(&self) -> Result<DiskRequest, GatedCliProcessError> {
        validate_bound_directory(&self.entry_path, &self.directory)?;
        let request =
            read_required_json::<DiskRequest>(&self.directory, REQUEST_FILE, MAX_REQUEST_BYTES)
                .map_err(map_journal_error)?;
        request.validate_self(self.submission_id)?;
        let stdin = read_optional_bytes(&self.directory, STDIN_FILE, MAX_STDIN_BYTES as u64)
            .map_err(map_journal_error)?;
        request.validate_stdin(stdin.as_deref())?;
        Ok(request)
    }

    fn acquire_helper_lock(&self) -> Result<HelperLock, GatedCliProcessError> {
        let fd = rfs::openat(
            &self.directory,
            LOCK_FILE,
            OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|_| GatedCliProcessError::Integrity)?;
        let stat = rfs::fstat(&fd).map_err(|_| GatedCliProcessError::Unavailable)?;
        validate_lock_stat(&stat)?;
        let file = fs::File::from(fd);
        if !try_exclusive_lock(&file)? {
            return Err(GatedCliProcessError::Busy);
        }
        Ok(HelperLock {
            file,
            device: stat.st_dev as u64,
            inode: stat.st_ino,
        })
    }

    fn validate_lock_binding(
        &self,
        helper: &DiskHelperIdentity,
    ) -> Result<(), GatedCliProcessError> {
        let fd = rfs::openat(
            &self.directory,
            LOCK_FILE,
            OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|_| GatedCliProcessError::Integrity)?;
        let stat = rfs::fstat(&fd).map_err(|_| GatedCliProcessError::Unavailable)?;
        validate_lock_stat(&stat)?;
        if stat.st_dev as u64 != helper.lock_device || stat.st_ino != helper.lock_inode {
            return Err(GatedCliProcessError::Integrity);
        }
        Ok(())
    }

    fn helper_lock_is_held(
        &self,
        helper: &DiskHelperIdentity,
    ) -> Result<bool, GatedCliProcessError> {
        self.validate_lock_binding(helper)?;
        let fd = rfs::openat(
            &self.directory,
            LOCK_FILE,
            OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|_| GatedCliProcessError::Integrity)?;
        let file = fs::File::from(fd);
        let acquired = try_exclusive_lock(&file)?;
        if acquired {
            unlock(&file)?;
        }
        Ok(!acquired)
    }

    fn read_release(
        &self,
        request: &DiskRequest,
        helper: &DiskHelperIdentity,
        child: &DiskChildIdentity,
    ) -> Result<Option<DiskRelease>, GatedCliProcessError> {
        let release =
            read_optional_json::<DiskRelease>(&self.directory, RELEASE_FILE, MAX_IDENTITY_BYTES)
                .map_err(map_journal_error)?;
        if let Some(release) = &release {
            release.validate(request, helper, child)?;
        }
        Ok(release)
    }

    fn read_exec_start(
        &self,
        request: &DiskRequest,
        helper: &DiskHelperIdentity,
        child: &DiskChildIdentity,
        release: Option<&DiskRelease>,
    ) -> Result<Option<DiskExecStart>, GatedCliProcessError> {
        let exec_start = read_optional_json::<DiskExecStart>(
            &self.directory,
            EXEC_STARTED_FILE,
            MAX_IDENTITY_BYTES,
        )
        .map_err(map_journal_error)?;
        if let Some(exec_start) = &exec_start {
            exec_start.validate(request, helper, child, release)?;
        }
        Ok(exec_start)
    }

    fn publish_helper(&self, helper: &DiskHelperIdentity) -> Result<(), GatedCliProcessError> {
        let bytes = serialize_bounded(helper, MAX_IDENTITY_BYTES)?;
        publish_or_compare(&self.directory, HELPER_FILE, &bytes, MAX_IDENTITY_BYTES)
            .map_err(map_journal_error)
    }

    fn publish_child(&self, child: &DiskChildIdentity) -> Result<(), GatedCliProcessError> {
        let bytes = serialize_bounded(child, MAX_IDENTITY_BYTES)?;
        publish_or_compare(&self.directory, CHILD_FILE, &bytes, MAX_IDENTITY_BYTES)
            .map_err(map_journal_error)
    }

    fn publish_exec_start(&self, exec_start: &DiskExecStart) -> Result<(), GatedCliProcessError> {
        let bytes = serialize_bounded(exec_start, MAX_IDENTITY_BYTES)?;
        publish_or_compare(
            &self.directory,
            EXEC_STARTED_FILE,
            &bytes,
            MAX_IDENTITY_BYTES,
        )
        .map_err(map_journal_error)
    }

    fn publish_terminal(&self, terminal: &DiskTerminal) -> Result<(), GatedCliProcessError> {
        let bytes = serialize_bounded(terminal, MAX_TERMINAL_BYTES)?;
        publish_or_compare(&self.directory, TERMINAL_FILE, &bytes, MAX_TERMINAL_BYTES)
            .map_err(map_journal_error)
    }
}

impl GatedCliProcessTerminal {
    pub fn released(&self) -> bool {
        self.released
    }

    pub fn exec_started(&self) -> bool {
        self.exec_started
    }

    pub fn outcome(&self) -> &GatedCliProcessOutcome {
        &self.outcome
    }

    pub fn stdout(&self) -> &[u8] {
        &self.stdout
    }

    pub fn stdout_truncated(&self) -> bool {
        self.stdout_truncated
    }

    pub fn stderr(&self) -> &[u8] {
        &self.stderr
    }

    pub fn stderr_truncated(&self) -> bool {
        self.stderr_truncated
    }
}

impl GatedCliReady {
    fn new(request: &DiskRequest, helper: &DiskHelperIdentity, child: &DiskChildIdentity) -> Self {
        Self {
            execution_binding_sha256: request.execution_binding_sha256.clone(),
            launch_nonce: request.launch_nonce,
            helper_nonce: helper.helper_nonce,
            child_nonce: child.child_nonce,
        }
    }
}

fn duration_millis(duration: Duration) -> Result<u64, GatedCliProcessError> {
    if duration.is_zero() {
        return Err(GatedCliProcessError::InvalidInput);
    }
    u64::try_from(duration.as_millis())
        .ok()
        .filter(|value| *value > 0)
        .ok_or(GatedCliProcessError::InvalidInput)
}

fn parse_sha256(value: &str) -> Result<[u8; 32], GatedCliProcessError> {
    if !valid_sha256(value) {
        return Err(GatedCliProcessError::InvalidInput);
    }
    let bytes = hex::decode(value).map_err(|_| GatedCliProcessError::InvalidInput)?;
    bytes
        .try_into()
        .map_err(|_| GatedCliProcessError::InvalidInput)
}

fn validate_value(value: &str) -> Result<(), GatedCliProcessError> {
    if value.len() > MAX_FIELD_BYTES || value.as_bytes().contains(&0) {
        return Err(GatedCliProcessError::InvalidInput);
    }
    Ok(())
}

fn validate_nonempty_value(value: &str) -> Result<(), GatedCliProcessError> {
    if value.is_empty() {
        return Err(GatedCliProcessError::InvalidInput);
    }
    validate_value(value)
}

fn validate_environment_name(value: &str) -> Result<(), GatedCliProcessError> {
    validate_nonempty_value(value)?;
    if value.as_bytes().contains(&b'=') {
        return Err(GatedCliProcessError::InvalidInput);
    }
    Ok(())
}

fn serialize_bounded(
    value: &impl Serialize,
    max_bytes: u64,
) -> Result<Vec<u8>, GatedCliProcessError> {
    let bytes = serde_json::to_vec(value).map_err(|_| GatedCliProcessError::InvalidInput)?;
    if bytes.is_empty() || bytes.len() as u64 > max_bytes {
        return Err(GatedCliProcessError::InvalidInput);
    }
    Ok(bytes)
}

fn hash_field(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

fn map_journal_error(error: RemoteSubmitJournalError) -> GatedCliProcessError {
    match error {
        RemoteSubmitJournalError::InvalidInput | RemoteSubmitJournalError::NotFound => {
            GatedCliProcessError::InvalidInput
        }
        RemoteSubmitJournalError::Conflict => GatedCliProcessError::Conflict,
        RemoteSubmitJournalError::Integrity => GatedCliProcessError::Integrity,
        RemoteSubmitJournalError::Unavailable => GatedCliProcessError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_digest_binds_command_and_release_identity() {
        let temp = tempfile::tempdir().unwrap();
        let executable = std::env::current_exe().unwrap();
        let executable_sha256 = file_sha256(&executable);
        let binding = GatedCliBinding::new(
            sha256(b"binding"),
            Uuid::new_v4(),
            unix_time_ms().unwrap() + 10_000,
        )
        .unwrap();
        let command = GatedCliCommand::new(
            executable,
            executable_sha256,
            temp.path(),
            vec!["--help".to_owned()],
            BTreeMap::new(),
            b"stdin".to_vec(),
            Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .unwrap();
        let request = DiskRequest::new(Uuid::new_v4(), &binding, &command).unwrap();
        assert_eq!(request.payload_sha256, request.canonical_payload_sha256());
        let mut changed = request.clone();
        changed.arguments.push("changed".to_owned());
        assert_ne!(changed.payload_sha256, changed.canonical_payload_sha256());
    }

    #[test]
    fn command_preserves_empty_and_multiline_argument_values() {
        let temp = tempfile::tempdir().unwrap();
        let executable = std::env::current_exe().unwrap();
        let mut environment = BTreeMap::new();
        environment.insert("EMPTY".to_owned(), String::new());
        let command = GatedCliCommand::new(
            &executable,
            file_sha256(&executable),
            temp.path(),
            vec![String::new(), "line one\nline two".to_owned()],
            environment,
            Vec::new(),
            Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .unwrap();
        let binding = GatedCliBinding::new(
            sha256(b"binding"),
            Uuid::new_v4(),
            unix_time_ms().unwrap() + 10_000,
        )
        .unwrap();

        DiskRequest::new(Uuid::new_v4(), &binding, &command).unwrap();
    }

    #[test]
    fn oversized_request_is_rejected_before_stdin_is_published() {
        let temp = tempfile::tempdir().unwrap();
        let executable = std::env::current_exe().unwrap();
        let submission =
            GatedCliSubmission::new(temp.path().join("journal"), Uuid::new_v4()).unwrap();
        let command = GatedCliCommand::new(
            &executable,
            file_sha256(&executable),
            temp.path(),
            vec!["x".repeat(MAX_REQUEST_BYTES as usize)],
            BTreeMap::new(),
            b"stdin".to_vec(),
            Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .unwrap();
        let binding = GatedCliBinding::new(
            sha256(b"binding"),
            Uuid::new_v4(),
            unix_time_ms().unwrap() + 10_000,
        )
        .unwrap();

        assert_eq!(
            submission.prepare(&binding, &command),
            Err(GatedCliProcessError::InvalidInput)
        );
        assert!(!submission.entry_path.join(STDIN_FILE).exists());
    }

    #[test]
    fn private_process_entry_rejects_conflicting_replay() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("journal");
        let submission_id = Uuid::new_v4();
        let submission = GatedCliSubmission::new(&root, submission_id).unwrap();
        let executable = std::env::current_exe().unwrap();
        let command = GatedCliCommand::new(
            &executable,
            file_sha256(&executable),
            temp.path(),
            vec!["--help".to_owned()],
            BTreeMap::new(),
            Vec::new(),
            Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .unwrap();
        let binding = GatedCliBinding::new(
            sha256(b"binding"),
            Uuid::new_v4(),
            unix_time_ms().unwrap() + 10_000,
        )
        .unwrap();
        submission.prepare(&binding, &command).unwrap();
        submission.prepare(&binding, &command).unwrap();
        let conflict = GatedCliBinding::new(
            sha256(b"other"),
            binding.launch_nonce,
            binding.absolute_deadline_unix_ms,
        )
        .unwrap();
        assert_eq!(
            submission.observe(&conflict),
            Err(GatedCliProcessError::Conflict)
        );
    }

    #[test]
    fn terminal_self_digest_rejects_validly_encoded_stream_tampering() {
        let temp = tempfile::tempdir().unwrap();
        let executable = std::env::current_exe().unwrap();
        let command = GatedCliCommand::new(
            &executable,
            file_sha256(&executable),
            temp.path(),
            vec!["--help".to_owned()],
            BTreeMap::new(),
            Vec::new(),
            Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .unwrap();
        let binding = GatedCliBinding::new(
            sha256(b"binding"),
            Uuid::new_v4(),
            unix_time_ms().unwrap() + 10_000,
        )
        .unwrap();
        let request = DiskRequest::new(Uuid::new_v4(), &binding, &command).unwrap();
        let helper = DiskHelperIdentity {
            pid: 100,
            start_token: "test-helper".to_owned(),
            boot_token: "test-boot".to_owned(),
            helper_nonce: Uuid::new_v4(),
            lock_device: 1,
            lock_inode: 1,
        };
        let child = DiskChildIdentity {
            pid: 101,
            start_token: "test-child".to_owned(),
            boot_token: helper.boot_token.clone(),
            process_group_id: 101,
            helper_nonce: helper.helper_nonce,
            child_nonce: Uuid::new_v4(),
        };
        let release = DiskRelease::new(&request, &helper, &child);
        let exec_start = DiskExecStart::new(&request, &helper, &child);
        let mut terminal = DiskTerminal::new(
            &request,
            &helper,
            &child,
            true,
            true,
            DiskOutcome::Exited {
                exit_code: Some(0),
                signal: None,
            },
            CapturedStream {
                bytes: b"receipt".to_vec(),
                truncated: false,
            },
            CapturedStream {
                bytes: Vec::new(),
                truncated: false,
            },
        );
        terminal.stdout_base64 = STANDARD.encode(b"tampered");

        assert_eq!(
            terminal.validate(&request, &helper, &child, Some(&release), Some(&exec_start),),
            Err(GatedCliProcessError::Integrity)
        );
    }

    fn file_sha256(path: &Path) -> String {
        let bytes = fs::read(path).unwrap();
        hex::encode(Sha256::digest(bytes))
    }
}
