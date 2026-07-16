use std::{
    collections::BTreeMap,
    thread,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use image_cli_runtime::{CommandSpec, MAX_STDIN_BYTES, VerifiedExecutable, WorkingDirectory};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    CapturedStream, GatedCliBinding, GatedCliCommand, GatedCliProcessError, GatedCliProcessOutcome,
    GatedCliProcessTerminal, HelperLock, MAX_ARGUMENTS, MAX_CAPTURED_STREAM_BYTES, MAX_ENVIRONMENT,
    PROCESS_POLL_INTERVAL, REQUEST_SCHEMA, REQUEST_VERSION, hash_field, parse_sha256,
    unix::{
        boot_token, current_process_token_matches, process_group_id, process_start_token,
        signal_process_group,
    },
    validate_environment_name, validate_nonempty_value, validate_value,
};
use crate::provider_tasks::remote_submit::journal::{sha256, valid_error_code, valid_sha256};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DiskRequest {
    pub(super) schema: String,
    pub(super) schema_version: u16,
    pub(super) submission_id: Uuid,
    pub(super) execution_binding_sha256: String,
    pub(super) launch_nonce: Uuid,
    pub(super) absolute_deadline_unix_ms: u64,
    pub(super) executable: String,
    pub(super) executable_sha256: String,
    pub(super) working_directory: String,
    pub(super) arguments: Vec<String>,
    pub(super) environment: BTreeMap<String, String>,
    pub(super) stdin_sha256: String,
    pub(super) stdin_byte_size: u64,
    pub(super) wall_timeout_ms: u64,
    pub(super) termination_grace_ms: u64,
    pub(super) payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DiskHelperIdentity {
    pub(super) pid: u32,
    pub(super) start_token: String,
    pub(super) boot_token: String,
    pub(super) helper_nonce: Uuid,
    pub(super) lock_device: u64,
    pub(super) lock_inode: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DiskChildIdentity {
    pub(super) pid: u32,
    pub(super) start_token: String,
    pub(super) boot_token: String,
    pub(super) process_group_id: u32,
    pub(super) helper_nonce: Uuid,
    pub(super) child_nonce: Uuid,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DiskRelease {
    pub(super) execution_binding_sha256: String,
    pub(super) launch_nonce: Uuid,
    pub(super) helper_nonce: Uuid,
    pub(super) child_nonce: Uuid,
    pub(super) payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DiskExecStart {
    pub(super) execution_binding_sha256: String,
    pub(super) launch_nonce: Uuid,
    pub(super) helper_nonce: Uuid,
    pub(super) child_nonce: Uuid,
    pub(super) payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum DiskOutcome {
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DiskTerminal {
    pub(super) execution_binding_sha256: String,
    pub(super) launch_nonce: Uuid,
    pub(super) helper_nonce: Uuid,
    pub(super) child_nonce: Uuid,
    pub(super) released: bool,
    pub(super) exec_started: bool,
    pub(super) outcome: DiskOutcome,
    pub(super) stdout_base64: String,
    pub(super) stdout_truncated: bool,
    pub(super) stderr_base64: String,
    pub(super) stderr_truncated: bool,
    pub(super) payload_sha256: String,
}

impl DiskRequest {
    pub(super) fn new(
        submission_id: Uuid,
        binding: &GatedCliBinding,
        command: &GatedCliCommand,
    ) -> Result<Self, GatedCliProcessError> {
        let mut request = Self {
            schema: REQUEST_SCHEMA.to_owned(),
            schema_version: REQUEST_VERSION,
            submission_id,
            execution_binding_sha256: binding.execution_binding_sha256.clone(),
            launch_nonce: binding.launch_nonce,
            absolute_deadline_unix_ms: binding.absolute_deadline_unix_ms,
            executable: command.executable.clone(),
            executable_sha256: command.executable_sha256.clone(),
            working_directory: command.working_directory.clone(),
            arguments: command.arguments.clone(),
            environment: command.environment.clone(),
            stdin_sha256: sha256(&command.stdin),
            stdin_byte_size: command.stdin.len() as u64,
            wall_timeout_ms: command.wall_timeout_ms,
            termination_grace_ms: command.termination_grace_ms,
            payload_sha256: String::new(),
        };
        request.payload_sha256 = request.canonical_payload_sha256();
        request.validate_self(submission_id)?;
        Ok(request)
    }

    pub(super) fn validate(
        &self,
        submission_id: Uuid,
        binding: &GatedCliBinding,
    ) -> Result<(), GatedCliProcessError> {
        self.validate_self(submission_id)?;
        if self.execution_binding_sha256 != binding.execution_binding_sha256
            || self.launch_nonce != binding.launch_nonce
            || self.absolute_deadline_unix_ms != binding.absolute_deadline_unix_ms
        {
            return Err(GatedCliProcessError::Conflict);
        }
        Ok(())
    }

    pub(super) fn validate_self(&self, submission_id: Uuid) -> Result<(), GatedCliProcessError> {
        if self.schema != REQUEST_SCHEMA
            || self.schema_version != REQUEST_VERSION
            || self.submission_id != submission_id
            || submission_id.is_nil()
            || !valid_sha256(&self.execution_binding_sha256)
            || self.launch_nonce.is_nil()
            || self.absolute_deadline_unix_ms == 0
            || !valid_sha256(&self.executable_sha256)
            || self.arguments.len() > MAX_ARGUMENTS
            || self.environment.len() > MAX_ENVIRONMENT
            || self.stdin_byte_size > MAX_STDIN_BYTES as u64
            || !valid_sha256(&self.stdin_sha256)
            || self.wall_timeout_ms == 0
            || self.termination_grace_ms == 0
            || !valid_sha256(&self.payload_sha256)
            || self.payload_sha256 != self.canonical_payload_sha256()
        {
            return Err(GatedCliProcessError::Integrity);
        }
        validate_nonempty_value(&self.executable).map_err(|_| GatedCliProcessError::Integrity)?;
        validate_nonempty_value(&self.working_directory)
            .map_err(|_| GatedCliProcessError::Integrity)?;
        for argument in &self.arguments {
            validate_value(argument).map_err(|_| GatedCliProcessError::Integrity)?;
        }
        for (name, value) in &self.environment {
            validate_environment_name(name).map_err(|_| GatedCliProcessError::Integrity)?;
            validate_value(value).map_err(|_| GatedCliProcessError::Integrity)?;
        }
        Ok(())
    }

    pub(super) fn validate_stdin(&self, bytes: Option<&[u8]>) -> Result<(), GatedCliProcessError> {
        match (self.stdin_byte_size, bytes) {
            (0, None) if self.stdin_sha256 == sha256(&[]) => Ok(()),
            (size, Some(bytes))
                if size == bytes.len() as u64 && self.stdin_sha256 == sha256(bytes) =>
            {
                Ok(())
            }
            _ => Err(GatedCliProcessError::Integrity),
        }
    }

    pub(super) fn canonical_payload_sha256(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"ai-image-factory/gated-cli-request-payload/v1\0");
        hash_field(&mut digest, self.schema.as_bytes());
        digest.update(self.schema_version.to_be_bytes());
        digest.update(self.submission_id.as_bytes());
        hash_field(&mut digest, self.execution_binding_sha256.as_bytes());
        digest.update(self.launch_nonce.as_bytes());
        digest.update(self.absolute_deadline_unix_ms.to_be_bytes());
        hash_field(&mut digest, self.executable.as_bytes());
        hash_field(&mut digest, self.executable_sha256.as_bytes());
        hash_field(&mut digest, self.working_directory.as_bytes());
        digest.update((self.arguments.len() as u64).to_be_bytes());
        for argument in &self.arguments {
            hash_field(&mut digest, argument.as_bytes());
        }
        digest.update((self.environment.len() as u64).to_be_bytes());
        for (name, value) in &self.environment {
            hash_field(&mut digest, name.as_bytes());
            hash_field(&mut digest, value.as_bytes());
        }
        hash_field(&mut digest, self.stdin_sha256.as_bytes());
        digest.update(self.stdin_byte_size.to_be_bytes());
        digest.update(self.wall_timeout_ms.to_be_bytes());
        digest.update(self.termination_grace_ms.to_be_bytes());
        hex::encode(digest.finalize())
    }

    pub(super) fn rebuild_command(&self) -> Result<CommandSpec, GatedCliProcessError> {
        let executable = VerifiedExecutable::new_with_sha256(
            &self.executable,
            parse_sha256(&self.executable_sha256)?,
        )
        .map_err(|_| GatedCliProcessError::Integrity)?;
        let working_directory = WorkingDirectory::new(&self.working_directory)
            .map_err(|_| GatedCliProcessError::Integrity)?;
        let mut command = CommandSpec::new_receipt(
            executable,
            working_directory,
            Duration::from_millis(self.wall_timeout_ms),
            Duration::from_millis(self.termination_grace_ms),
        )
        .map_err(|_| GatedCliProcessError::Integrity)?;
        for argument in &self.arguments {
            command = command
                .arg(argument)
                .map_err(|_| GatedCliProcessError::Integrity)?;
        }
        for (name, value) in &self.environment {
            command = command
                .env(name, value)
                .map_err(|_| GatedCliProcessError::Integrity)?;
        }
        Ok(command)
    }
}

impl DiskHelperIdentity {
    pub(super) fn capture(lock: &HelperLock) -> Result<Self, GatedCliProcessError> {
        let pid = std::process::id();
        let identity = Self {
            pid,
            start_token: process_start_token(pid)?,
            boot_token: boot_token()?,
            helper_nonce: Uuid::new_v4(),
            lock_device: lock.device,
            lock_inode: lock.inode,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub(super) fn validate(&self) -> Result<(), GatedCliProcessError> {
        if self.pid <= 1
            || self.start_token.is_empty()
            || self.start_token.len() > 128
            || self.boot_token.is_empty()
            || self.boot_token.len() > 128
            || self.helper_nonce.is_nil()
            || self.lock_inode == 0
        {
            return Err(GatedCliProcessError::Integrity);
        }
        Ok(())
    }

    pub(super) fn is_current_process(&self) -> Result<bool, GatedCliProcessError> {
        self.validate()?;
        if boot_token()? != self.boot_token {
            return Ok(false);
        }
        current_process_token_matches(self.pid, &self.start_token)
    }
}

impl DiskChildIdentity {
    pub(super) fn capture(
        pid: u32,
        helper: &DiskHelperIdentity,
    ) -> Result<Self, GatedCliProcessError> {
        let start_token = process_start_token(pid)?;
        let process_group_id = process_group_id(pid)?;
        if process_start_token(pid)? != start_token {
            return Err(GatedCliProcessError::Unavailable);
        }
        let identity = Self {
            pid,
            start_token,
            boot_token: helper.boot_token.clone(),
            process_group_id,
            helper_nonce: helper.helper_nonce,
            child_nonce: Uuid::new_v4(),
        };
        identity.validate(helper)?;
        Ok(identity)
    }

    pub(super) fn validate(&self, helper: &DiskHelperIdentity) -> Result<(), GatedCliProcessError> {
        if self.pid <= 1
            || self.process_group_id != self.pid
            || self.start_token.is_empty()
            || self.start_token.len() > 128
            || self.boot_token != helper.boot_token
            || self.helper_nonce != helper.helper_nonce
            || self.child_nonce.is_nil()
            || self.pid == helper.pid
        {
            return Err(GatedCliProcessError::Integrity);
        }
        Ok(())
    }

    pub(super) fn is_current_process(&self) -> Result<bool, GatedCliProcessError> {
        if boot_token()? != self.boot_token {
            return Ok(false);
        }
        if !current_process_token_matches(self.pid, &self.start_token)? {
            return Ok(false);
        }
        Ok(process_group_id(self.pid).is_ok_and(|value| value == self.process_group_id))
    }

    pub(super) fn kill_process_group_if_current(&self) -> Result<bool, GatedCliProcessError> {
        if !self.is_current_process()? || !self.is_current_process()? {
            return Ok(false);
        }
        signal_process_group(self.process_group_id, libc::SIGKILL)?;
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if !self.is_current_process()? {
                return Ok(true);
            }
            thread::sleep(PROCESS_POLL_INTERVAL);
        }
        Err(GatedCliProcessError::Unavailable)
    }
}

impl DiskRelease {
    pub(super) fn new(
        request: &DiskRequest,
        helper: &DiskHelperIdentity,
        child: &DiskChildIdentity,
    ) -> Self {
        let mut release = Self {
            execution_binding_sha256: request.execution_binding_sha256.clone(),
            launch_nonce: request.launch_nonce,
            helper_nonce: helper.helper_nonce,
            child_nonce: child.child_nonce,
            payload_sha256: String::new(),
        };
        release.payload_sha256 = release.canonical_payload_sha256();
        release
    }

    pub(super) fn validate(
        &self,
        request: &DiskRequest,
        helper: &DiskHelperIdentity,
        child: &DiskChildIdentity,
    ) -> Result<(), GatedCliProcessError> {
        if self.execution_binding_sha256 != request.execution_binding_sha256
            || self.launch_nonce != request.launch_nonce
            || self.helper_nonce != helper.helper_nonce
            || self.child_nonce != child.child_nonce
            || !valid_sha256(&self.payload_sha256)
            || self.payload_sha256 != self.canonical_payload_sha256()
        {
            return Err(GatedCliProcessError::Integrity);
        }
        Ok(())
    }

    fn canonical_payload_sha256(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"ai-image-factory/gated-cli-release/v1\0");
        hash_field(&mut digest, self.execution_binding_sha256.as_bytes());
        digest.update(self.launch_nonce.as_bytes());
        digest.update(self.helper_nonce.as_bytes());
        digest.update(self.child_nonce.as_bytes());
        hex::encode(digest.finalize())
    }
}

impl DiskExecStart {
    pub(super) fn new(
        request: &DiskRequest,
        helper: &DiskHelperIdentity,
        child: &DiskChildIdentity,
    ) -> Self {
        let mut exec_start = Self {
            execution_binding_sha256: request.execution_binding_sha256.clone(),
            launch_nonce: request.launch_nonce,
            helper_nonce: helper.helper_nonce,
            child_nonce: child.child_nonce,
            payload_sha256: String::new(),
        };
        exec_start.payload_sha256 = exec_start.canonical_payload_sha256();
        exec_start
    }

    pub(super) fn validate(
        &self,
        request: &DiskRequest,
        helper: &DiskHelperIdentity,
        child: &DiskChildIdentity,
        release: Option<&DiskRelease>,
    ) -> Result<(), GatedCliProcessError> {
        if release.is_none()
            || self.execution_binding_sha256 != request.execution_binding_sha256
            || self.launch_nonce != request.launch_nonce
            || self.helper_nonce != helper.helper_nonce
            || self.child_nonce != child.child_nonce
            || !valid_sha256(&self.payload_sha256)
            || self.payload_sha256 != self.canonical_payload_sha256()
        {
            return Err(GatedCliProcessError::Integrity);
        }
        Ok(())
    }

    fn canonical_payload_sha256(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"ai-image-factory/gated-cli-exec-start/v1\0");
        hash_field(&mut digest, self.execution_binding_sha256.as_bytes());
        digest.update(self.launch_nonce.as_bytes());
        digest.update(self.helper_nonce.as_bytes());
        digest.update(self.child_nonce.as_bytes());
        hex::encode(digest.finalize())
    }
}

impl DiskTerminal {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        request: &DiskRequest,
        helper: &DiskHelperIdentity,
        child: &DiskChildIdentity,
        released: bool,
        exec_started: bool,
        outcome: DiskOutcome,
        stdout: CapturedStream,
        stderr: CapturedStream,
    ) -> Self {
        let mut terminal = Self {
            execution_binding_sha256: request.execution_binding_sha256.clone(),
            launch_nonce: request.launch_nonce,
            helper_nonce: helper.helper_nonce,
            child_nonce: child.child_nonce,
            released,
            exec_started,
            outcome,
            stdout_base64: STANDARD.encode(stdout.bytes),
            stdout_truncated: stdout.truncated,
            stderr_base64: STANDARD.encode(stderr.bytes),
            stderr_truncated: stderr.truncated,
            payload_sha256: String::new(),
        };
        terminal.payload_sha256 = terminal.canonical_payload_sha256();
        terminal
    }

    pub(super) fn validate(
        &self,
        request: &DiskRequest,
        helper: &DiskHelperIdentity,
        child: &DiskChildIdentity,
        release: Option<&DiskRelease>,
        exec_start: Option<&DiskExecStart>,
    ) -> Result<(), GatedCliProcessError> {
        if self.execution_binding_sha256 != request.execution_binding_sha256
            || self.launch_nonce != request.launch_nonce
            || self.helper_nonce != helper.helper_nonce
            || self.child_nonce != child.child_nonce
            || self.released != release.is_some()
            || self.exec_started != exec_start.is_some()
            || self.exec_started && !self.released
            || !valid_sha256(&self.payload_sha256)
            || self.payload_sha256 != self.canonical_payload_sha256()
        {
            return Err(GatedCliProcessError::Integrity);
        }
        let stdout = STANDARD
            .decode(&self.stdout_base64)
            .map_err(|_| GatedCliProcessError::Integrity)?;
        let stderr = STANDARD
            .decode(&self.stderr_base64)
            .map_err(|_| GatedCliProcessError::Integrity)?;
        if stdout.len() > MAX_CAPTURED_STREAM_BYTES || stderr.len() > MAX_CAPTURED_STREAM_BYTES {
            return Err(GatedCliProcessError::Integrity);
        }
        self.outcome.validate(self.released, self.exec_started)
    }

    fn canonical_payload_sha256(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"ai-image-factory/gated-cli-terminal/v1\0");
        hash_field(&mut digest, self.execution_binding_sha256.as_bytes());
        digest.update(self.launch_nonce.as_bytes());
        digest.update(self.helper_nonce.as_bytes());
        digest.update(self.child_nonce.as_bytes());
        digest.update([u8::from(self.released), u8::from(self.exec_started)]);
        hash_outcome(&mut digest, &self.outcome);
        hash_field(&mut digest, self.stdout_base64.as_bytes());
        digest.update([u8::from(self.stdout_truncated)]);
        hash_field(&mut digest, self.stderr_base64.as_bytes());
        digest.update([u8::from(self.stderr_truncated)]);
        hex::encode(digest.finalize())
    }

    pub(super) fn into_public(self) -> Result<GatedCliProcessTerminal, GatedCliProcessError> {
        Ok(GatedCliProcessTerminal {
            released: self.released,
            exec_started: self.exec_started,
            outcome: self.outcome.into_public(),
            stdout: STANDARD
                .decode(self.stdout_base64)
                .map_err(|_| GatedCliProcessError::Integrity)?,
            stdout_truncated: self.stdout_truncated,
            stderr: STANDARD
                .decode(self.stderr_base64)
                .map_err(|_| GatedCliProcessError::Integrity)?,
            stderr_truncated: self.stderr_truncated,
        })
    }
}

fn hash_outcome(digest: &mut Sha256, outcome: &DiskOutcome) {
    match outcome {
        DiskOutcome::Exited { exit_code, signal } => {
            digest.update([0]);
            hash_optional_i32(digest, *exit_code);
            hash_optional_i32(digest, *signal);
        }
        DiskOutcome::TimedOut => digest.update([1]),
        DiskOutcome::AbsoluteDeadlineElapsed => digest.update([2]),
        DiskOutcome::GateFailed { error_code } => {
            digest.update([3]);
            hash_field(digest, error_code.as_bytes());
        }
        DiskOutcome::ResidualProcessGroup => digest.update([4]),
    }
}

fn hash_optional_i32(digest: &mut Sha256, value: Option<i32>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
}

impl DiskOutcome {
    fn validate(&self, released: bool, exec_started: bool) -> Result<(), GatedCliProcessError> {
        match self {
            Self::Exited { exit_code, signal } => {
                if exit_code.is_some() == signal.is_some() || !released || !exec_started {
                    return Err(GatedCliProcessError::Integrity);
                }
            }
            Self::TimedOut | Self::ResidualProcessGroup if !exec_started => {
                return Err(GatedCliProcessError::Integrity);
            }
            Self::GateFailed { error_code } => {
                if !valid_error_code(error_code) {
                    return Err(GatedCliProcessError::Integrity);
                }
            }
            Self::TimedOut | Self::AbsoluteDeadlineElapsed | Self::ResidualProcessGroup => {}
        }
        Ok(())
    }

    pub(super) fn into_public(self) -> GatedCliProcessOutcome {
        match self {
            Self::Exited { exit_code, signal } => {
                GatedCliProcessOutcome::Exited { exit_code, signal }
            }
            Self::TimedOut => GatedCliProcessOutcome::TimedOut,
            Self::AbsoluteDeadlineElapsed => GatedCliProcessOutcome::AbsoluteDeadlineElapsed,
            Self::GateFailed { error_code } => GatedCliProcessOutcome::GateFailed { error_code },
            Self::ResidualProcessGroup => GatedCliProcessOutcome::ResidualProcessGroup,
        }
    }
}
