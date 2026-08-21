use std::{
    fs,
    io::{Read, Write},
    os::fd::{AsRawFd, OwnedFd},
    path::{Component, Path, PathBuf},
};

use image_provider_contracts::ProviderReportedCostEvidenceV1;
use rustix::{
    fs::{self as rfs, AtFlags, FileType, Mode, OFlags, RenameFlags},
    io::Errno,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{FilesystemRunnerJournal, RunnerJournalError};
use crate::executor::{ExecutorSubmissionLease, SupervisedOutput, error_code_is_valid};

const REQUEST_FILE: &str = "provider-request.json";
const PROCESS_FILE: &str = "process.json";
const PROVIDER_PROCESS_FILE: &str = "provider-process.json";
const LOCK_FILE: &str = "runner.lock";
const OUTPUT_FILE: &str = "output.bin";
const RESULT_FILE: &str = "result.json";
const MAX_DIAGNOSTIC_BYTES: u64 = 64 * 1024;
pub(crate) const CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE: &str = "codex-app-server-failure.json";
const WORKSPACE_DIR: &str = "workspace";
const CODEX_HOME_DIR: &str = "codex-home";
const RUNTIME_HOME_DIR: &str = "runtime-home";
const PROVIDER_HOME_DIR: &str = "provider-home";
const PROVIDER_WORKSPACES_DIR: &str = "provider-workspaces";
const PROVIDER_ATTEMPT_DIR: &str = "attempt";
const MAX_REQUEST_BYTES: u64 = 1024 * 1024;
const MAX_MARKER_BYTES: u64 = 64 * 1024;
const MAX_OUTPUT_BYTES: u64 = 256 * 1024 * 1024;

pub(crate) struct ExecutionSpool {
    directory: OwnedFd,
    #[cfg(test)]
    path: PathBuf,
    workspace: PrivateDirectory,
    codex_home: PrivateDirectory,
    runtime_home: PrivateDirectory,
    provider_home: PrivateDirectory,
    provider_workspaces: PrivateDirectory,
    provider_attempt: PrivateDirectory,
}

pub(crate) struct CodexExtensionOutputRoot {
    directory: PrivateDirectory,
}

struct PrivateDirectory {
    fd: OwnedFd,
    path: PathBuf,
}

pub(crate) struct RunnerLock {
    file: fs::File,
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessIdentity {
    pub(crate) pid: u32,
    pub(crate) start_token: String,
    pub(crate) nonce: String,
    pub(crate) lock_device: u64,
    pub(crate) lock_inode: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderProcessIdentity {
    pid: u32,
    start_token: String,
    pgid: u32,
    helper_nonce: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ProcessTerminal {
    Succeeded {
        helper_nonce: String,
        sha256_hex: String,
        byte_size: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_reported_cost: Option<ProviderReportedCostEvidenceV1>,
    },
    Failed {
        helper_nonce: String,
        error_code: String,
    },
    Uncertain {
        helper_nonce: String,
        error_code: String,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ProcessObservation {
    AwaitingProcess,
    Running(ProcessIdentity),
    Succeeded(SupervisedOutput),
    Failed {
        error_code: String,
    },
    Uncertain {
        error_code: String,
    },
    Lost {
        provider: Option<ProviderProcessIdentity>,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum WorkspaceOutputSnapshot {
    Missing,
    Incomplete,
    Bytes(Vec<u8>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ProcessSpoolError {
    #[error("process spool input is invalid")]
    InvalidInput,
    #[error("process spool durable identity conflicts with existing evidence")]
    Conflict,
    #[error("process spool integrity validation failed")]
    Integrity,
    #[error("process spool storage is unavailable")]
    Unavailable,
}

impl ExecutionSpool {
    pub(crate) fn for_lease(
        journal: &FilesystemRunnerJournal,
        lease: &ExecutorSubmissionLease,
    ) -> Result<Self, ProcessSpoolError> {
        let path = journal.execution_path(lease).map_err(map_journal_error)?;
        Self::open_execution(path)
    }

    pub(crate) fn open(
        root: &Path,
        executor_execution_id: Uuid,
    ) -> Result<Self, ProcessSpoolError> {
        if !root.is_absolute() || executor_execution_id.is_nil() {
            return Err(ProcessSpoolError::InvalidInput);
        }
        let root = open_private_directory(root, ProcessSpoolError::InvalidInput)?;
        let name = executor_execution_id.simple().to_string();
        let path = root.path.join(&name);
        let directory = open_private_directory_at(&root.fd, &root.path, &name)?;
        Self::from_directory(directory.fd, path)
    }

    fn open_execution(path: PathBuf) -> Result<Self, ProcessSpoolError> {
        let directory = open_private_directory(&path, ProcessSpoolError::Integrity)?;
        Self::from_directory(directory.fd, path)
    }

    fn from_directory(directory: OwnedFd, path: PathBuf) -> Result<Self, ProcessSpoolError> {
        let workspace = ensure_private_directory_at(&directory, &path, WORKSPACE_DIR)?;
        let codex_home = ensure_private_directory_at(&directory, &path, CODEX_HOME_DIR)?;
        let runtime_home = ensure_private_directory_at(&directory, &path, RUNTIME_HOME_DIR)?;
        let provider_home = ensure_private_directory_at(&directory, &path, PROVIDER_HOME_DIR)?;
        let provider_workspaces =
            ensure_private_directory_at(&directory, &path, PROVIDER_WORKSPACES_DIR)?;
        let provider_attempt = ensure_private_directory_at(
            &provider_workspaces.fd,
            &provider_workspaces.path,
            PROVIDER_ATTEMPT_DIR,
        )?;
        ensure_lock_file(&directory)?;
        Ok(Self {
            directory,
            #[cfg(test)]
            path,
            workspace,
            codex_home,
            runtime_home,
            provider_home,
            provider_workspaces,
            provider_attempt,
        })
    }

    pub(crate) fn prepare_request(&self, bytes: &[u8]) -> Result<(), ProcessSpoolError> {
        if bytes.is_empty() || bytes.len() as u64 > MAX_REQUEST_BYTES {
            return Err(ProcessSpoolError::InvalidInput);
        }
        publish_or_compare(&self.directory, REQUEST_FILE, bytes, MAX_REQUEST_BYTES)
    }

    pub(crate) fn read_request(&self) -> Result<Vec<u8>, ProcessSpoolError> {
        read_required_bytes(&self.directory, REQUEST_FILE, MAX_REQUEST_BYTES)
    }

    pub(crate) fn acquire_runner_lock(&self) -> Result<RunnerLock, ProcessSpoolError> {
        let fd = rfs::openat(
            &self.directory,
            LOCK_FILE,
            OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|_| ProcessSpoolError::Integrity)?;
        validate_lock_stat(&rfs::fstat(&fd).map_err(|_| ProcessSpoolError::Unavailable)?)?;
        let stat = rfs::fstat(&fd).map_err(|_| ProcessSpoolError::Unavailable)?;
        let file = fs::File::from(fd);
        if !try_exclusive_lock(&file)? {
            return Err(ProcessSpoolError::Conflict);
        }
        Ok(RunnerLock {
            file,
            device: stat.st_dev as u64,
            inode: stat.st_ino,
        })
    }

    pub(crate) fn publish_process(
        &self,
        runner_lock: &RunnerLock,
        identity: &ProcessIdentity,
    ) -> Result<(), ProcessSpoolError> {
        identity.validate()?;
        runner_lock.validate_identity(identity)?;
        self.validate_lock_binding(identity)?;
        publish_json_or_compare(&self.directory, PROCESS_FILE, identity, MAX_MARKER_BYTES)
    }

    pub(crate) fn publish_provider_process(
        &self,
        runner_lock: &RunnerLock,
        helper: &ProcessIdentity,
        provider: &ProviderProcessIdentity,
    ) -> Result<(), ProcessSpoolError> {
        helper.validate()?;
        runner_lock.validate_identity(helper)?;
        self.validate_lock_binding(helper)?;
        self.validate_persisted_process(helper)?;
        provider.validate()?;
        if provider.helper_nonce != helper.nonce || provider.pid == helper.pid {
            return Err(ProcessSpoolError::Integrity);
        }
        publish_json_or_compare(
            &self.directory,
            PROVIDER_PROCESS_FILE,
            provider,
            MAX_MARKER_BYTES,
        )
    }

    pub(crate) fn publish_output(&self, bytes: &[u8]) -> Result<(), ProcessSpoolError> {
        if bytes.is_empty() || bytes.len() as u64 > MAX_OUTPUT_BYTES {
            return Err(ProcessSpoolError::InvalidInput);
        }
        publish_or_compare(&self.directory, OUTPUT_FILE, bytes, MAX_OUTPUT_BYTES)
    }

    pub(crate) fn publish_terminal(
        &self,
        runner_lock: &RunnerLock,
        terminal: &ProcessTerminal,
    ) -> Result<(), ProcessSpoolError> {
        terminal.validate()?;
        let process =
            read_optional_json::<ProcessIdentity>(&self.directory, PROCESS_FILE, MAX_MARKER_BYTES)?
                .ok_or(ProcessSpoolError::Integrity)?;
        process.validate()?;
        runner_lock.validate_identity(&process)?;
        self.validate_lock_binding(&process)?;
        if terminal.helper_nonce() != process.nonce {
            return Err(ProcessSpoolError::Integrity);
        }
        publish_json_or_compare(&self.directory, RESULT_FILE, terminal, MAX_MARKER_BYTES)
    }

    pub(crate) fn observe(&self) -> Result<ProcessObservation, ProcessSpoolError> {
        let Some(identity) =
            read_optional_json::<ProcessIdentity>(&self.directory, PROCESS_FILE, MAX_MARKER_BYTES)?
        else {
            if read_optional_bytes(&self.directory, RESULT_FILE, MAX_MARKER_BYTES)?.is_some()
                || read_optional_bytes(&self.directory, PROVIDER_PROCESS_FILE, MAX_MARKER_BYTES)?
                    .is_some()
            {
                return Err(ProcessSpoolError::Integrity);
            }
            return Ok(ProcessObservation::AwaitingProcess);
        };
        identity.validate()?;
        self.validate_lock_binding(&identity)?;
        if let Some(terminal) = read_optional_json(&self.directory, RESULT_FILE, MAX_MARKER_BYTES)?
        {
            return self.terminal_observation(&identity, terminal);
        }
        let provider = self.read_provider_process(&identity)?;
        if identity.is_current_process()? && self.runner_lock_is_held(&identity)? {
            return Ok(ProcessObservation::Running(identity));
        }
        if let Some(terminal) = read_optional_json(&self.directory, RESULT_FILE, MAX_MARKER_BYTES)?
        {
            return self.terminal_observation(&identity, terminal);
        }
        Ok(ProcessObservation::Lost { provider })
    }

    fn terminal_observation(
        &self,
        process: &ProcessIdentity,
        terminal: ProcessTerminal,
    ) -> Result<ProcessObservation, ProcessSpoolError> {
        terminal.validate()?;
        if terminal.helper_nonce() != process.nonce {
            return Err(ProcessSpoolError::Integrity);
        }
        match terminal {
            ProcessTerminal::Succeeded {
                helper_nonce: _,
                sha256_hex,
                byte_size,
                provider_reported_cost,
            } => {
                let bytes = read_required_bytes(&self.directory, OUTPUT_FILE, MAX_OUTPUT_BYTES)?;
                if bytes.len() as u64 != byte_size || sha256(&bytes) != sha256_hex {
                    return Err(ProcessSpoolError::Integrity);
                }
                let output = SupervisedOutput::from_parts(bytes, provider_reported_cost)
                    .ok_or(ProcessSpoolError::Integrity)?;
                Ok(ProcessObservation::Succeeded(output))
            }
            ProcessTerminal::Failed {
                helper_nonce: _,
                error_code,
            } => Ok(ProcessObservation::Failed { error_code }),
            ProcessTerminal::Uncertain {
                helper_nonce: _,
                error_code,
            } => Ok(ProcessObservation::Uncertain { error_code }),
        }
    }

    fn validate_persisted_process(
        &self,
        expected: &ProcessIdentity,
    ) -> Result<(), ProcessSpoolError> {
        let persisted =
            read_required_json::<ProcessIdentity>(&self.directory, PROCESS_FILE, MAX_MARKER_BYTES)?;
        if persisted != *expected {
            return Err(ProcessSpoolError::Integrity);
        }
        Ok(())
    }

    fn read_provider_process(
        &self,
        helper: &ProcessIdentity,
    ) -> Result<Option<ProviderProcessIdentity>, ProcessSpoolError> {
        let provider = read_optional_json::<ProviderProcessIdentity>(
            &self.directory,
            PROVIDER_PROCESS_FILE,
            MAX_MARKER_BYTES,
        )?;
        if let Some(provider) = &provider {
            provider.validate()?;
            if provider.helper_nonce != helper.nonce || provider.pid == helper.pid {
                return Err(ProcessSpoolError::Integrity);
            }
        }
        Ok(provider)
    }

    fn validate_lock_binding(&self, identity: &ProcessIdentity) -> Result<(), ProcessSpoolError> {
        let fd = self.open_bound_runner_lock(identity)?;
        drop(fd);
        Ok(())
    }

    fn runner_lock_is_held(&self, identity: &ProcessIdentity) -> Result<bool, ProcessSpoolError> {
        let file = fs::File::from(self.open_bound_runner_lock(identity)?);
        let acquired = try_exclusive_lock(&file)?;
        if acquired {
            unlock(&file)?;
        }
        Ok(!acquired)
    }

    fn open_bound_runner_lock(
        &self,
        identity: &ProcessIdentity,
    ) -> Result<OwnedFd, ProcessSpoolError> {
        let fd = rfs::openat(
            &self.directory,
            LOCK_FILE,
            OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|_| ProcessSpoolError::Integrity)?;
        let stat = rfs::fstat(&fd).map_err(|_| ProcessSpoolError::Unavailable)?;
        validate_lock_stat(&stat)?;
        if stat.st_dev as u64 != identity.lock_device || stat.st_ino != identity.lock_inode {
            return Err(ProcessSpoolError::Integrity);
        }
        Ok(fd)
    }

    pub(crate) fn workspace_path(&self) -> Result<&Path, ProcessSpoolError> {
        validate_bound_path(&self.workspace.path, &self.workspace.fd)?;
        Ok(&self.workspace.path)
    }

    #[cfg(test)]
    pub(crate) fn read_workspace_output(
        &self,
        filename: &str,
        max_bytes: u64,
    ) -> Result<WorkspaceOutputSnapshot, ProcessSpoolError> {
        if !valid_single_component(filename) || max_bytes == 0 {
            return Err(ProcessSpoolError::InvalidInput);
        }
        validate_bound_path(&self.workspace.path, &self.workspace.fd)?;
        read_workspace_output(&self.workspace.fd, filename, max_bytes)
    }

    #[cfg(test)]
    pub(crate) fn read_runtime_output(
        &self,
        filename: &str,
        max_bytes: u64,
    ) -> Result<WorkspaceOutputSnapshot, ProcessSpoolError> {
        if !valid_single_component(filename) || max_bytes == 0 {
            return Err(ProcessSpoolError::InvalidInput);
        }
        validate_bound_path(&self.runtime_home.path, &self.runtime_home.fd)?;
        read_workspace_output(&self.runtime_home.fd, filename, max_bytes)
    }

    #[cfg(test)]
    pub(crate) fn seal_codex_extension_output(
        &self,
        thread_id: &str,
        call_id: &str,
        output_filename: &str,
        max_bytes: u64,
    ) -> Result<bool, ProcessSpoolError> {
        if !valid_single_component(output_filename) || max_bytes == 0 {
            return Err(ProcessSpoolError::InvalidInput);
        }
        validate_bound_path(&self.codex_home.path, &self.codex_home.fd)?;
        validate_bound_path(&self.runtime_home.path, &self.runtime_home.fd)?;
        let Some(bytes) = read_codex_extension_output_at(
            &self.codex_home.fd,
            thread_id,
            call_id,
            max_bytes,
            || {},
        )?
        else {
            return Ok(false);
        };
        publish_or_compare(&self.runtime_home.fd, output_filename, &bytes, max_bytes)?;
        Ok(true)
    }

    pub(crate) fn codex_home_path(&self) -> Result<&Path, ProcessSpoolError> {
        validate_bound_path(&self.codex_home.path, &self.codex_home.fd)?;
        Ok(&self.codex_home.path)
    }

    pub(crate) fn runtime_home_path(&self) -> Result<&Path, ProcessSpoolError> {
        validate_bound_path(&self.runtime_home.path, &self.runtime_home.fd)?;
        Ok(&self.runtime_home.path)
    }

    pub(crate) fn provider_home_path(&self) -> Result<&Path, ProcessSpoolError> {
        validate_bound_path(&self.provider_home.path, &self.provider_home.fd)?;
        Ok(&self.provider_home.path)
    }

    pub(crate) fn provider_workspaces_path(&self) -> Result<&Path, ProcessSpoolError> {
        validate_bound_path(&self.provider_workspaces.path, &self.provider_workspaces.fd)?;
        Ok(&self.provider_workspaces.path)
    }

    pub(crate) fn provider_attempt_path(&self) -> Result<&Path, ProcessSpoolError> {
        validate_bound_path(&self.provider_attempt.path, &self.provider_attempt.fd)?;
        Ok(&self.provider_attempt.path)
    }

    pub(crate) fn stage_provider_input(
        &self,
        filename: &str,
        bytes: &[u8],
        max_bytes: u64,
    ) -> Result<(), ProcessSpoolError> {
        let mut components = Path::new(filename).components();
        if !matches!(components.next(), Some(Component::Normal(_)))
            || components.next().is_some()
            || filename.is_empty()
            || filename.len() > 255
            || bytes.is_empty()
            || bytes.len() as u64 > max_bytes
        {
            return Err(ProcessSpoolError::InvalidInput);
        }
        validate_bound_path(&self.provider_attempt.path, &self.provider_attempt.fd)?;
        publish_or_compare(&self.provider_attempt.fd, filename, bytes, max_bytes)
    }

    pub(crate) fn open_provider_input(
        &self,
        filename: &str,
    ) -> Result<fs::File, ProcessSpoolError> {
        let mut components = Path::new(filename).components();
        if !matches!(components.next(), Some(Component::Normal(_)))
            || components.next().is_some()
            || filename.is_empty()
            || filename.len() > 255
        {
            return Err(ProcessSpoolError::InvalidInput);
        }
        validate_bound_path(&self.provider_attempt.path, &self.provider_attempt.fd)?;
        let fd = rfs::openat(
            &self.provider_attempt.fd,
            filename,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|_| ProcessSpoolError::Unavailable)?;
        let stat = rfs::fstat(&fd).map_err(|_| ProcessSpoolError::Unavailable)?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
            || stat.st_nlink != 1
            || stat.st_uid != unsafe { libc::geteuid() }
            || stat.st_mode & 0o077 != 0
        {
            return Err(ProcessSpoolError::Integrity);
        }
        Ok(fs::File::from(fd))
    }

    pub(crate) fn publish_diagnostic<T>(
        &self,
        filename: &str,
        diagnostic: &T,
    ) -> Result<(), ProcessSpoolError>
    where
        T: Serialize + DeserializeOwned + Eq,
    {
        let valid_name = filename == CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE
            || (filename.starts_with("grok-")
                && filename.ends_with(".json")
                && filename.len() <= 64
                && filename.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'-' | b'.')
                }));
        if !valid_name {
            return Err(ProcessSpoolError::InvalidInput);
        }
        publish_json_or_compare(&self.directory, filename, diagnostic, MAX_DIAGNOSTIC_BYTES)
    }

    pub(crate) fn cleanup_provider_runtime(&self) -> Result<(), ProcessSpoolError> {
        for directory in [
            &self.provider_home,
            &self.provider_workspaces,
            &self.runtime_home,
        ] {
            validate_bound_path(&directory.path, &directory.fd)?;
            fs::remove_dir_all(&directory.path).map_err(|_| ProcessSpoolError::Unavailable)?;
        }
        rfs::fsync(&self.directory).map_err(|_| ProcessSpoolError::Unavailable)
    }

    pub(crate) fn cleanup_codex_runtime(&self) -> Result<(), ProcessSpoolError> {
        for directory in [&self.codex_home, &self.workspace, &self.runtime_home] {
            validate_bound_path(&directory.path, &directory.fd)?;
            fs::remove_dir_all(&directory.path).map_err(|_| ProcessSpoolError::Unavailable)?;
        }
        rfs::fsync(&self.directory).map_err(|_| ProcessSpoolError::Unavailable)
    }

    pub(crate) fn open_provider_file(
        &self,
        relative_path: &Path,
    ) -> Result<fs::File, ProcessSpoolError> {
        validate_bound_path(&self.provider_home.path, &self.provider_home.fd)?;
        let components = relative_path
            .components()
            .map(|component| match component {
                Component::Normal(name) => Ok(name),
                _ => Err(ProcessSpoolError::InvalidInput),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (filename, directories) = components
            .split_last()
            .ok_or(ProcessSpoolError::InvalidInput)?;
        let mut current_directory = None;
        for name in directories {
            let fd = match current_directory.as_ref() {
                Some(directory) => open_provider_directory_at(directory, name),
                None => open_provider_directory_at(&self.provider_home.fd, name),
            }?;
            current_directory = Some(fd);
        }
        let directory = current_directory.as_ref().unwrap_or(&self.provider_home.fd);
        let fd = rfs::openat(
            directory,
            *filename,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|_| ProcessSpoolError::Integrity)?;
        Ok(fs::File::from(fd))
    }

    #[cfg(test)]
    fn root_path(&self) -> &Path {
        &self.path
    }
}

impl CodexExtensionOutputRoot {
    pub(crate) fn open(codex_home: &Path) -> Result<Self, ProcessSpoolError> {
        open_private_directory(codex_home, ProcessSpoolError::Integrity)
            .map(|directory| Self { directory })
    }

    pub(crate) fn read(
        &self,
        thread_id: &str,
        call_id: &str,
        max_bytes: u64,
    ) -> Result<Option<Vec<u8>>, ProcessSpoolError> {
        validate_bound_path(&self.directory.path, &self.directory.fd)?;
        read_codex_extension_output_at(&self.directory.fd, thread_id, call_id, max_bytes, || {})
    }
}

impl RunnerLock {
    pub(crate) fn identity(&self) -> Result<ProcessIdentity, ProcessSpoolError> {
        let pid = std::process::id();
        Ok(ProcessIdentity {
            pid,
            start_token: process_start_token(pid)?,
            nonce: Uuid::new_v4().to_string(),
            lock_device: self.device,
            lock_inode: self.inode,
        })
    }

    fn validate_identity(&self, identity: &ProcessIdentity) -> Result<(), ProcessSpoolError> {
        if identity.pid != std::process::id()
            || identity.lock_device != self.device
            || identity.lock_inode != self.inode
            || !identity.is_current_process()?
        {
            return Err(ProcessSpoolError::Integrity);
        }
        Ok(())
    }
}

impl Drop for RunnerLock {
    fn drop(&mut self) {
        let _ = unlock(&self.file);
    }
}

impl ProcessIdentity {
    fn validate(&self) -> Result<(), ProcessSpoolError> {
        if self.pid <= 1
            || Uuid::parse_str(&self.nonce)
                .ok()
                .is_none_or(|nonce| nonce.is_nil() || nonce.to_string() != self.nonce)
            || self.start_token.is_empty()
            || self.start_token.len() > 128
            || self.start_token.chars().any(char::is_control)
            || self.lock_inode == 0
        {
            return Err(ProcessSpoolError::Integrity);
        }
        Ok(())
    }

    fn is_current_process(&self) -> Result<bool, ProcessSpoolError> {
        match process_start_token(self.pid) {
            Ok(token) => Ok(token == self.start_token),
            Err(ProcessSpoolError::Unavailable) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

impl ProcessTerminal {
    fn validate(&self) -> Result<(), ProcessSpoolError> {
        if !nonce_is_valid(self.helper_nonce()) {
            return Err(ProcessSpoolError::Integrity);
        }
        match self {
            Self::Succeeded {
                helper_nonce: _,
                sha256_hex,
                byte_size,
                provider_reported_cost,
            } => {
                if !is_sha256(sha256_hex)
                    || !(1..=MAX_OUTPUT_BYTES).contains(byte_size)
                    || provider_reported_cost
                        .as_ref()
                        .is_some_and(|evidence| evidence.validate().is_err())
                {
                    return Err(ProcessSpoolError::Integrity);
                }
            }
            Self::Failed {
                helper_nonce: _,
                error_code,
            }
            | Self::Uncertain {
                helper_nonce: _,
                error_code,
            } => {
                if !error_code_is_valid(error_code) {
                    return Err(ProcessSpoolError::Integrity);
                }
            }
        }
        Ok(())
    }

    fn helper_nonce(&self) -> &str {
        match self {
            Self::Succeeded { helper_nonce, .. }
            | Self::Failed { helper_nonce, .. }
            | Self::Uncertain { helper_nonce, .. } => helper_nonce,
        }
    }
}

impl ProviderProcessIdentity {
    pub(crate) fn capture(pid: u32, helper_nonce: &str) -> Result<Self, ProcessSpoolError> {
        let start_token = process_start_token(pid)?;
        let pgid = process_group_id(pid)?;
        if process_start_token(pid)? != start_token {
            return Err(ProcessSpoolError::Unavailable);
        }
        let identity = Self {
            pid,
            start_token,
            pgid,
            helper_nonce: helper_nonce.to_string(),
        };
        identity.validate()?;
        Ok(identity)
    }

    fn validate(&self) -> Result<(), ProcessSpoolError> {
        if self.pid <= 1
            || self.pgid != self.pid
            || self.start_token.is_empty()
            || self.start_token.len() > 128
            || self.start_token.chars().any(char::is_control)
            || !nonce_is_valid(&self.helper_nonce)
        {
            return Err(ProcessSpoolError::Integrity);
        }
        Ok(())
    }

    pub(crate) fn is_current_process_group(&self) -> Result<bool, ProcessSpoolError> {
        self.validate()?;
        if !self.matches_current_process_group()? {
            return Ok(false);
        }
        self.matches_current_process_group()
    }

    pub(crate) fn kill_process_group_if_current(&self) -> Result<bool, ProcessSpoolError> {
        self.validate()?;
        if !self.matches_current_process_group()? || !self.matches_current_process_group()? {
            return Ok(false);
        }
        let pgid = libc::pid_t::try_from(self.pgid).map_err(|_| ProcessSpoolError::Integrity)?;
        if unsafe { libc::kill(-pgid, libc::SIGKILL) } == 0 {
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(ProcessSpoolError::Unavailable)
        }
    }

    fn matches_current_process_group(&self) -> Result<bool, ProcessSpoolError> {
        let first_token = match process_start_token(self.pid) {
            Ok(token) => token,
            Err(ProcessSpoolError::Unavailable) => return Ok(false),
            Err(error) => return Err(error),
        };
        if first_token != self.start_token {
            return Ok(false);
        }
        let pgid = match process_group_id(self.pid) {
            Ok(pgid) => pgid,
            Err(ProcessSpoolError::Unavailable) => return Ok(false),
            Err(error) => return Err(error),
        };
        if pgid != self.pgid {
            return Ok(false);
        }
        match process_start_token(self.pid) {
            Ok(token) => Ok(token == self.start_token),
            Err(ProcessSpoolError::Unavailable) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

fn nonce_is_valid(nonce: &str) -> bool {
    Uuid::parse_str(nonce)
        .ok()
        .is_some_and(|value| !value.is_nil() && value.to_string() == nonce)
}

fn open_private_directory(
    path: &Path,
    invalid: ProcessSpoolError,
) -> Result<PrivateDirectory, ProcessSpoolError> {
    validate_private_directory_path(path, invalid)?;
    let fd = rfs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ProcessSpoolError::Integrity)?;
    validate_directory_stat(&rfs::fstat(&fd).map_err(|_| ProcessSpoolError::Unavailable)?)?;
    validate_bound_path(path, &fd)?;
    Ok(PrivateDirectory {
        fd,
        path: path.to_path_buf(),
    })
}

fn open_private_directory_at(
    parent: &OwnedFd,
    parent_path: &Path,
    name: &str,
) -> Result<PrivateDirectory, ProcessSpoolError> {
    let fd = rfs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ProcessSpoolError::Integrity)?;
    validate_directory_stat(&rfs::fstat(&fd).map_err(|_| ProcessSpoolError::Unavailable)?)?;
    let path = parent_path.join(name);
    validate_bound_path(&path, &fd)?;
    Ok(PrivateDirectory { fd, path })
}

fn ensure_private_directory_at(
    parent: &OwnedFd,
    parent_path: &Path,
    name: &str,
) -> Result<PrivateDirectory, ProcessSpoolError> {
    match rfs::mkdirat(parent, name, Mode::RWXU) {
        Ok(()) => rfs::fsync(parent).map_err(|_| ProcessSpoolError::Unavailable)?,
        Err(Errno::EXIST) => {}
        Err(_) => return Err(ProcessSpoolError::Unavailable),
    }
    open_private_directory_at(parent, parent_path, name)
}

fn ensure_lock_file(directory: &OwnedFd) -> Result<(), ProcessSpoolError> {
    match rfs::openat(
        directory,
        LOCK_FILE,
        OFlags::RDWR
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::CLOEXEC
            | OFlags::NONBLOCK,
        Mode::RUSR | Mode::WUSR,
    ) {
        Ok(fd) => {
            rfs::fchmod(&fd, Mode::RUSR | Mode::WUSR)
                .map_err(|_| ProcessSpoolError::Unavailable)?;
            rfs::fsync(&fd).map_err(|_| ProcessSpoolError::Unavailable)?;
            rfs::fsync(directory).map_err(|_| ProcessSpoolError::Unavailable)
        }
        Err(Errno::EXIST) => {
            let fd = rfs::openat(
                directory,
                LOCK_FILE,
                OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|_| ProcessSpoolError::Integrity)?;
            validate_lock_stat(&rfs::fstat(&fd).map_err(|_| ProcessSpoolError::Unavailable)?)
        }
        Err(_) => Err(ProcessSpoolError::Unavailable),
    }
}

fn publish_json_or_compare<T: Serialize + DeserializeOwned + Eq>(
    directory: &OwnedFd,
    name: &str,
    value: &T,
    max_bytes: u64,
) -> Result<(), ProcessSpoolError> {
    let bytes = serde_json::to_vec(value).map_err(|_| ProcessSpoolError::InvalidInput)?;
    match publish_bytes(directory, name, &bytes, max_bytes)? {
        true => Ok(()),
        false => {
            let existing: T = read_required_json(directory, name, max_bytes)?;
            if existing == *value {
                Ok(())
            } else {
                Err(ProcessSpoolError::Conflict)
            }
        }
    }
}

fn publish_or_compare(
    directory: &OwnedFd,
    name: &str,
    bytes: &[u8],
    max_bytes: u64,
) -> Result<(), ProcessSpoolError> {
    match publish_bytes(directory, name, bytes, max_bytes)? {
        true => Ok(()),
        false => {
            let existing = read_required_bytes(directory, name, max_bytes)?;
            if existing == bytes {
                Ok(())
            } else {
                Err(ProcessSpoolError::Conflict)
            }
        }
    }
}

fn publish_bytes(
    directory: &OwnedFd,
    name: &str,
    bytes: &[u8],
    max_bytes: u64,
) -> Result<bool, ProcessSpoolError> {
    if bytes.is_empty() || bytes.len() as u64 > max_bytes {
        return Err(ProcessSpoolError::InvalidInput);
    }
    let temporary = format!(".tmp-{}", Uuid::new_v4().simple());
    let fd = rfs::openat(
        directory,
        &temporary,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::CLOEXEC
            | OFlags::NONBLOCK,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| ProcessSpoolError::Unavailable)?;
    rfs::fchmod(&fd, Mode::RUSR | Mode::WUSR).map_err(|_| ProcessSpoolError::Unavailable)?;
    let mut file = fs::File::from(fd);
    if file.write_all(bytes).is_err() || rfs::fsync(&file).is_err() {
        let _ = rfs::unlinkat(directory, &temporary, AtFlags::empty());
        return Err(ProcessSpoolError::Unavailable);
    }
    match rfs::renameat_with(
        directory,
        &temporary,
        directory,
        name,
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {
            rfs::fsync(directory).map_err(|_| ProcessSpoolError::Unavailable)?;
            Ok(true)
        }
        Err(Errno::EXIST) => {
            rfs::unlinkat(directory, &temporary, AtFlags::empty())
                .map_err(|_| ProcessSpoolError::Unavailable)?;
            rfs::fsync(directory).map_err(|_| ProcessSpoolError::Unavailable)?;
            Ok(false)
        }
        Err(_) => {
            let _ = rfs::unlinkat(directory, &temporary, AtFlags::empty());
            Err(ProcessSpoolError::Unavailable)
        }
    }
}

fn read_required_json<T: DeserializeOwned>(
    directory: &OwnedFd,
    name: &str,
    max_bytes: u64,
) -> Result<T, ProcessSpoolError> {
    let bytes = read_required_bytes(directory, name, max_bytes)?;
    serde_json::from_slice(&bytes).map_err(|_| ProcessSpoolError::Integrity)
}

fn read_optional_json<T: DeserializeOwned>(
    directory: &OwnedFd,
    name: &str,
    max_bytes: u64,
) -> Result<Option<T>, ProcessSpoolError> {
    let Some(bytes) = read_optional_bytes(directory, name, max_bytes)? else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| ProcessSpoolError::Integrity)
}

fn read_required_bytes(
    directory: &OwnedFd,
    name: &str,
    max_bytes: u64,
) -> Result<Vec<u8>, ProcessSpoolError> {
    read_optional_bytes(directory, name, max_bytes)?.ok_or(ProcessSpoolError::Integrity)
}

fn read_optional_bytes(
    directory: &OwnedFd,
    name: &str,
    max_bytes: u64,
) -> Result<Option<Vec<u8>>, ProcessSpoolError> {
    let fd = match rfs::openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(None),
        Err(_) => return Err(ProcessSpoolError::Integrity),
    };
    let mut file = fs::File::from(fd);
    let stat = rfs::fstat(&file).map_err(|_| ProcessSpoolError::Unavailable)?;
    let size = validate_regular_file_stat(&stat, max_bytes)?;
    let mut bytes = Vec::with_capacity(size);
    Read::by_ref(&mut file)
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ProcessSpoolError::Unavailable)?;
    let final_stat = rfs::fstat(&file).map_err(|_| ProcessSpoolError::Unavailable)?;
    if validate_regular_file_stat(&final_stat, max_bytes)? != size || bytes.len() != size {
        return Err(ProcessSpoolError::Integrity);
    }
    Ok(Some(bytes))
}

#[cfg(test)]
fn read_workspace_output(
    directory: &OwnedFd,
    name: &str,
    max_bytes: u64,
) -> Result<WorkspaceOutputSnapshot, ProcessSpoolError> {
    read_workspace_output_with_hook(directory, name, max_bytes, || {})
}

fn read_workspace_output_with_hook<F>(
    directory: &OwnedFd,
    name: &str,
    max_bytes: u64,
    after_open: F,
) -> Result<WorkspaceOutputSnapshot, ProcessSpoolError>
where
    F: FnOnce(),
{
    let fd = match rfs::openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(WorkspaceOutputSnapshot::Missing),
        Err(_) => return Err(ProcessSpoolError::Integrity),
    };
    let mut file = fs::File::from(fd);
    let initial = rfs::fstat(&file).map_err(|_| ProcessSpoolError::Unavailable)?;
    let Some(size) = validate_workspace_output_stat(&initial, max_bytes)? else {
        return Ok(WorkspaceOutputSnapshot::Incomplete);
    };
    after_open();
    let mut bytes = Vec::with_capacity(size);
    Read::by_ref(&mut file)
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ProcessSpoolError::Unavailable)?;
    let final_stat = rfs::fstat(&file).map_err(|_| ProcessSpoolError::Unavailable)?;
    if validate_workspace_output_stat(&final_stat, max_bytes)? != Some(size)
        || !same_file_snapshot(&initial, &final_stat)
        || bytes.len() != size
    {
        return Ok(WorkspaceOutputSnapshot::Incomplete);
    }

    let current = match rfs::openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(WorkspaceOutputSnapshot::Incomplete),
        Err(_) => return Err(ProcessSpoolError::Integrity),
    };
    let current_stat = rfs::fstat(&current).map_err(|_| ProcessSpoolError::Unavailable)?;
    if validate_workspace_output_stat(&current_stat, max_bytes)? != Some(size)
        || !same_file_snapshot(&final_stat, &current_stat)
    {
        return Ok(WorkspaceOutputSnapshot::Incomplete);
    }
    Ok(WorkspaceOutputSnapshot::Bytes(bytes))
}

fn validate_workspace_output_stat(
    stat: &rfs::Stat,
    max_bytes: u64,
) -> Result<Option<usize>, ProcessSpoolError> {
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || stat.st_uid != unsafe { libc::geteuid() }
        || stat.st_nlink != 1
        || Mode::from_raw_mode(stat.st_mode).bits() & 0o7022 != 0
        || stat.st_size < 0
        || stat.st_size as u64 > max_bytes
    {
        return Err(ProcessSpoolError::Integrity);
    }
    if stat.st_size == 0 {
        return Ok(None);
    }
    usize::try_from(stat.st_size)
        .map(Some)
        .map_err(|_| ProcessSpoolError::Integrity)
}

fn same_file_snapshot(first: &rfs::Stat, second: &rfs::Stat) -> bool {
    first.st_dev == second.st_dev
        && first.st_ino == second.st_ino
        && first.st_mode == second.st_mode
        && first.st_uid == second.st_uid
        && first.st_gid == second.st_gid
        && first.st_nlink == second.st_nlink
        && first.st_size == second.st_size
        && first.st_mtime == second.st_mtime
        && first.st_mtime_nsec == second.st_mtime_nsec
        && first.st_ctime == second.st_ctime
        && first.st_ctime_nsec == second.st_ctime_nsec
}

#[cfg(test)]
pub(crate) fn read_codex_extension_output(
    codex_home: &Path,
    thread_id: &str,
    call_id: &str,
    max_bytes: u64,
) -> Result<Option<Vec<u8>>, ProcessSpoolError> {
    if max_bytes == 0 {
        return Err(ProcessSpoolError::InvalidInput);
    }
    validate_private_directory_path(codex_home, ProcessSpoolError::Integrity)?;
    let root = rfs::open(
        codex_home,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ProcessSpoolError::Integrity)?;
    read_codex_extension_output_at(&root, thread_id, call_id, max_bytes, || {})
}

fn read_codex_extension_output_at<F>(
    codex_home: &OwnedFd,
    thread_id: &str,
    call_id: &str,
    max_bytes: u64,
    after_open: F,
) -> Result<Option<Vec<u8>>, ProcessSpoolError>
where
    F: FnOnce(),
{
    if Uuid::parse_str(thread_id).map(|value| value.to_string()) != Ok(thread_id.to_string())
        || !valid_codex_call_id(call_id)
        || max_bytes == 0
    {
        return Err(ProcessSpoolError::InvalidInput);
    }
    let generated = match open_private_artifact_directory_at(codex_home, "generated_images")? {
        Some(directory) => directory,
        None => return Ok(None),
    };
    let thread = match open_private_artifact_directory_at(&generated, thread_id)? {
        Some(directory) => directory,
        None => return Ok(None),
    };
    let filename = format!("{call_id}.png");
    match read_workspace_output_with_hook(&thread, &filename, max_bytes, after_open)? {
        WorkspaceOutputSnapshot::Missing => Ok(None),
        WorkspaceOutputSnapshot::Incomplete => Err(ProcessSpoolError::Integrity),
        WorkspaceOutputSnapshot::Bytes(bytes) => Ok(Some(bytes)),
    }
}

fn valid_codex_call_id(call_id: &str) -> bool {
    !call_id.is_empty()
        && call_id.len() <= 255 - ".png".len()
        && call_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn open_private_artifact_directory_at(
    parent: &OwnedFd,
    name: &str,
) -> Result<Option<OwnedFd>, ProcessSpoolError> {
    if !valid_single_component(name) {
        return Err(ProcessSpoolError::InvalidInput);
    }
    let fd = match rfs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(None),
        Err(_) => return Err(ProcessSpoolError::Integrity),
    };
    let stat = rfs::fstat(&fd).map_err(|_| ProcessSpoolError::Unavailable)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
        || stat.st_uid != unsafe { libc::geteuid() }
        || Mode::from_raw_mode(stat.st_mode).bits() & 0o077 != 0
    {
        return Err(ProcessSpoolError::Integrity);
    }
    Ok(Some(fd))
}

fn valid_single_component(name: &str) -> bool {
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(_)))
        && components.next().is_none()
        && !name.is_empty()
        && name.len() <= 255
        && !name.as_bytes().contains(&0)
}

fn validate_private_directory_path(
    path: &Path,
    invalid: ProcessSpoolError,
) -> Result<(), ProcessSpoolError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| ProcessSpoolError::Unavailable)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(invalid);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o777 != 0o700
        {
            return Err(invalid);
        }
    }
    Ok(())
}

fn validate_directory_stat(stat: &rfs::Stat) -> Result<(), ProcessSpoolError> {
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
        || Mode::from_raw_mode(stat.st_mode) != Mode::RWXU
        || stat.st_uid != unsafe { libc::geteuid() }
    {
        return Err(ProcessSpoolError::Integrity);
    }
    Ok(())
}

fn open_provider_directory_at(
    parent: &OwnedFd,
    name: &std::ffi::OsStr,
) -> Result<OwnedFd, ProcessSpoolError> {
    let fd = rfs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ProcessSpoolError::Integrity)?;
    let stat = rfs::fstat(&fd).map_err(|_| ProcessSpoolError::Unavailable)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
        || stat.st_uid != unsafe { libc::geteuid() }
        || Mode::from_raw_mode(stat.st_mode).bits() & 0o022 != 0
    {
        return Err(ProcessSpoolError::Integrity);
    }
    Ok(fd)
}

fn validate_bound_path(path: &Path, fd: &OwnedFd) -> Result<(), ProcessSpoolError> {
    validate_private_directory_path(path, ProcessSpoolError::Integrity)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let metadata = fs::symlink_metadata(path).map_err(|_| ProcessSpoolError::Integrity)?;
        let stat = rfs::fstat(fd).map_err(|_| ProcessSpoolError::Unavailable)?;
        if metadata.dev() != stat.st_dev as u64 || metadata.ino() != stat.st_ino {
            return Err(ProcessSpoolError::Integrity);
        }
    }
    Ok(())
}

fn validate_lock_stat(stat: &rfs::Stat) -> Result<(), ProcessSpoolError> {
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || Mode::from_raw_mode(stat.st_mode) != Mode::RUSR | Mode::WUSR
        || stat.st_uid != unsafe { libc::geteuid() }
        || stat.st_nlink != 1
        || stat.st_size != 0
    {
        return Err(ProcessSpoolError::Integrity);
    }
    Ok(())
}

fn validate_regular_file_stat(
    stat: &rfs::Stat,
    max_bytes: u64,
) -> Result<usize, ProcessSpoolError> {
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || Mode::from_raw_mode(stat.st_mode) != Mode::RUSR | Mode::WUSR
        || stat.st_uid != unsafe { libc::geteuid() }
        || stat.st_nlink != 1
        || stat.st_size <= 0
        || stat.st_size as u64 > max_bytes
    {
        return Err(ProcessSpoolError::Integrity);
    }
    usize::try_from(stat.st_size).map_err(|_| ProcessSpoolError::Integrity)
}

fn try_exclusive_lock(file: &fs::File) -> Result<bool, ProcessSpoolError> {
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error
        .raw_os_error()
        .is_some_and(|code| code == libc::EWOULDBLOCK || code == libc::EAGAIN)
    {
        Ok(false)
    } else {
        Err(ProcessSpoolError::Unavailable)
    }
}

fn unlock(file: &fs::File) -> Result<(), ProcessSpoolError> {
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) } == 0 {
        Ok(())
    } else {
        Err(ProcessSpoolError::Unavailable)
    }
}

fn process_group_id(pid: u32) -> Result<u32, ProcessSpoolError> {
    let pid = libc::pid_t::try_from(pid).map_err(|_| ProcessSpoolError::Integrity)?;
    let pgid = unsafe { libc::getpgid(pid) };
    if pgid <= 1 {
        return Err(ProcessSpoolError::Unavailable);
    }
    u32::try_from(pgid).map_err(|_| ProcessSpoolError::Integrity)
}

#[cfg(target_os = "linux")]
fn process_start_token(pid: u32) -> Result<String, ProcessSpoolError> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|_| ProcessSpoolError::Unavailable)?;
    let fields = stat
        .rsplit_once(')')
        .map(|(_, fields)| fields.split_whitespace().collect::<Vec<_>>())
        .ok_or(ProcessSpoolError::Integrity)?;
    let start_ticks = fields.get(19).ok_or(ProcessSpoolError::Integrity)?;
    start_ticks
        .parse::<u64>()
        .map_err(|_| ProcessSpoolError::Integrity)?;
    Ok(format!("linux:{start_ticks}"))
}

#[cfg(target_os = "macos")]
fn process_start_token(pid: u32) -> Result<String, ProcessSpoolError> {
    let mut info = unsafe { std::mem::zeroed::<libc::proc_bsdinfo>() };
    let expected = std::mem::size_of::<libc::proc_bsdinfo>() as i32;
    let actual = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            0,
            &mut info as *mut _ as *mut _,
            expected,
        )
    };
    if actual != expected || info.pbi_pid != pid {
        return Err(ProcessSpoolError::Unavailable);
    }
    Ok(format!(
        "macos:{}:{}",
        info.pbi_start_tvsec, info.pbi_start_tvusec
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_start_token(_pid: u32) -> Result<String, ProcessSpoolError> {
    Err(ProcessSpoolError::Unavailable)
}

fn map_journal_error(error: RunnerJournalError) -> ProcessSpoolError {
    match error {
        RunnerJournalError::InvalidInput => ProcessSpoolError::InvalidInput,
        RunnerJournalError::Conflict => ProcessSpoolError::Conflict,
        RunnerJournalError::Integrity => ProcessSpoolError::Integrity,
        RunnerJournalError::Unavailable => ProcessSpoolError::Unavailable,
    }
}

pub(crate) fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use image_provider_contracts::{ProviderCostEvidenceScope, ProviderReportedCostEvidenceV1};
    use tempfile::TempDir;

    use super::*;
    use crate::executor::ExecutorSubmissionLease;

    #[test]
    fn live_identity_requires_start_token_and_held_lock() {
        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        spool.prepare_request(br#"{"schema_version":1}"#).unwrap();
        let lock = spool.acquire_runner_lock().unwrap();
        let identity = lock.identity().unwrap();
        spool.publish_process(&lock, &identity).unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Running(identity)
        );
        drop(lock);
        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Lost { provider: None }
        );
    }

    #[test]
    fn terminal_output_is_atomic_verified_and_replayable() {
        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let lock = spool.acquire_runner_lock().unwrap();
        let identity = lock.identity().unwrap();
        spool.publish_process(&lock, &identity).unwrap();
        let bytes = b"bounded-output";
        spool.publish_output(bytes).unwrap();
        let provider_reported_cost = ProviderReportedCostEvidenceV1::usd_ticks(
            ProviderCostEvidenceScope::CliInvocation,
            "provider-test",
            "provider_cli",
            "provider-operation-1",
            200_000_000,
            br#"{"total_cost_usd_ticks":200000000}"#,
            "end.total_cost_usd_ticks",
        )
        .unwrap();
        let terminal = ProcessTerminal::Succeeded {
            helper_nonce: identity.nonce.clone(),
            sha256_hex: sha256(bytes),
            byte_size: bytes.len() as u64,
            provider_reported_cost: Some(provider_reported_cost.clone()),
        };
        let terminal_json = serde_json::to_vec(&terminal).unwrap();
        assert_eq!(
            serde_json::from_slice::<ProcessTerminal>(&terminal_json).unwrap(),
            terminal
        );
        spool.publish_terminal(&lock, &terminal).unwrap();
        spool.publish_terminal(&lock, &terminal).unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(
                SupervisedOutput::from_parts(bytes.to_vec(), Some(provider_reported_cost)).unwrap()
            )
        );
    }

    #[test]
    fn orphan_output_without_terminal_is_never_promoted() {
        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let lock = spool.acquire_runner_lock().unwrap();
        let identity = lock.identity().unwrap();
        spool.publish_process(&lock, &identity).unwrap();
        spool.publish_output(b"orphan-output").unwrap();

        drop(lock);

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Lost { provider: None }
        );
    }

    #[test]
    fn durable_success_survives_runtime_cleanup_failure() {
        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let lock = spool.acquire_runner_lock().unwrap();
        let identity = lock.identity().unwrap();
        spool.publish_process(&lock, &identity).unwrap();
        let bytes = b"durable-output";
        spool.publish_output(bytes).unwrap();
        spool
            .publish_terminal(
                &lock,
                &ProcessTerminal::Succeeded {
                    helper_nonce: identity.nonce,
                    sha256_hex: sha256(bytes),
                    byte_size: bytes.len() as u64,
                    provider_reported_cost: None,
                },
            )
            .unwrap();

        let workspace = spool.workspace_path().unwrap();
        let displaced = workspace.with_extension("displaced");
        fs::rename(&workspace, &displaced).unwrap();
        fs::create_dir(&workspace).unwrap();
        assert_eq!(
            spool.cleanup_codex_runtime(),
            Err(ProcessSpoolError::Integrity)
        );

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(
                SupervisedOutput::from_parts(bytes.to_vec(), None).unwrap()
            )
        );
    }

    #[test]
    fn workspace_output_reader_accepts_owned_read_only_regular_files() {
        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let output = spool.workspace_path().unwrap().join("provider-output.png");

        assert_eq!(
            spool
                .read_workspace_output("provider-output.png", 1024)
                .unwrap(),
            WorkspaceOutputSnapshot::Missing
        );
        fs::write(&output, []).unwrap();
        assert_eq!(
            spool
                .read_workspace_output("provider-output.png", 1024)
                .unwrap(),
            WorkspaceOutputSnapshot::Incomplete
        );
        fs::write(&output, b"complete-image").unwrap();
        assert_eq!(
            spool
                .read_workspace_output("provider-output.png", 1024)
                .unwrap(),
            WorkspaceOutputSnapshot::Bytes(b"complete-image".to_vec())
        );
    }

    #[test]
    fn runtime_output_reader_uses_the_same_bounded_snapshot_contract() {
        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let output = spool.runtime_home_path().unwrap().join("sealed-output.bin");

        assert_eq!(
            spool
                .read_runtime_output("sealed-output.bin", 1024)
                .unwrap(),
            WorkspaceOutputSnapshot::Missing
        );
        fs::write(&output, b"complete-image").unwrap();
        assert_eq!(
            spool
                .read_runtime_output("sealed-output.bin", 1024)
                .unwrap(),
            WorkspaceOutputSnapshot::Bytes(b"complete-image".to_vec())
        );
    }

    #[cfg(unix)]
    #[test]
    fn codex_extension_output_is_bound_to_exact_thread_and_call() {
        use std::os::unix::fs::PermissionsExt;

        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let thread_id = "019fd9f5-badb-7dd3-8903-28ffded0ef54";
        let call_id = "call_exact_image";
        assert_eq!(
            read_codex_extension_output(spool.codex_home_path().unwrap(), thread_id, call_id, 1024)
                .unwrap(),
            None
        );
        let generated = spool.codex_home_path().unwrap().join("generated_images");
        let thread = generated.join(thread_id);
        fs::create_dir(&generated).unwrap();
        fs::create_dir(&thread).unwrap();
        fs::set_permissions(&generated, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&thread, fs::Permissions::from_mode(0o700)).unwrap();
        let output = thread.join(format!("{call_id}.png"));
        fs::write(&output, b"exact-native-image").unwrap();
        fs::set_permissions(&output, fs::Permissions::from_mode(0o600)).unwrap();

        assert_eq!(
            read_codex_extension_output(spool.codex_home_path().unwrap(), thread_id, call_id, 1024)
                .unwrap(),
            Some(b"exact-native-image".to_vec())
        );
        assert!(
            spool
                .seal_codex_extension_output(thread_id, call_id, "sealed-output.bin", 1024)
                .unwrap()
        );
        assert_eq!(
            spool
                .read_runtime_output("sealed-output.bin", 1024)
                .unwrap(),
            WorkspaceOutputSnapshot::Bytes(b"exact-native-image".to_vec())
        );
        assert_eq!(
            read_codex_extension_output(
                spool.codex_home_path().unwrap(),
                thread_id,
                "call_other_image",
                1024,
            )
            .unwrap(),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn codex_extension_output_rejects_aliases_bounds_and_unsafe_identifiers() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let (temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let thread_id = "019fd9f5-badb-7dd3-8903-28ffded0ef54";
        let call_id = "call_exact_image";
        let generated = spool.codex_home_path().unwrap().join("generated_images");
        let thread = generated.join(thread_id);
        fs::create_dir(&generated).unwrap();
        fs::create_dir(&thread).unwrap();
        fs::set_permissions(&generated, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&thread, fs::Permissions::from_mode(0o700)).unwrap();
        let output = thread.join(format!("{call_id}.png"));
        let outside = temp.path().join("outside.png");
        fs::write(&outside, b"outside").unwrap();

        symlink(&outside, &output).unwrap();
        assert_eq!(
            read_codex_extension_output(spool.codex_home_path().unwrap(), thread_id, call_id, 1024),
            Err(ProcessSpoolError::Integrity)
        );
        fs::remove_file(&output).unwrap();
        fs::hard_link(&outside, &output).unwrap();
        assert_eq!(
            read_codex_extension_output(spool.codex_home_path().unwrap(), thread_id, call_id, 1024),
            Err(ProcessSpoolError::Integrity)
        );
        fs::remove_file(&output).unwrap();
        let file = fs::File::create(&output).unwrap();
        file.set_len(1025).unwrap();
        fs::set_permissions(&output, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            read_codex_extension_output(spool.codex_home_path().unwrap(), thread_id, call_id, 1024),
            Err(ProcessSpoolError::Integrity)
        );
        assert_eq!(
            read_codex_extension_output(
                spool.codex_home_path().unwrap(),
                "../../other-thread",
                call_id,
                1024,
            ),
            Err(ProcessSpoolError::InvalidInput)
        );
        assert_eq!(
            read_codex_extension_output(
                spool.codex_home_path().unwrap(),
                thread_id,
                "../other-call",
                1024,
            ),
            Err(ProcessSpoolError::InvalidInput)
        );
    }

    #[cfg(unix)]
    #[test]
    fn codex_extension_output_rejects_same_name_replacement_after_open() {
        use std::os::unix::fs::PermissionsExt;

        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let thread_id = "019fd9f5-badb-7dd3-8903-28ffded0ef54";
        let call_id = "call_exact_image";
        let generated = spool.codex_home_path().unwrap().join("generated_images");
        let thread = generated.join(thread_id);
        fs::create_dir(&generated).unwrap();
        fs::create_dir(&thread).unwrap();
        fs::set_permissions(&generated, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&thread, fs::Permissions::from_mode(0o700)).unwrap();
        let output = thread.join(format!("{call_id}.png"));
        let displaced = thread.join("displaced.png");
        fs::write(&output, b"original-native-image").unwrap();
        fs::set_permissions(&output, fs::Permissions::from_mode(0o600)).unwrap();
        let opened = Arc::new(std::sync::Barrier::new(2));
        let continue_read = Arc::new(std::sync::Barrier::new(2));
        let reader = {
            let opened = Arc::clone(&opened);
            let continue_read = Arc::clone(&continue_read);
            std::thread::spawn(move || {
                read_codex_extension_output_at(
                    &spool.codex_home.fd,
                    thread_id,
                    call_id,
                    1024,
                    || {
                        opened.wait();
                        continue_read.wait();
                    },
                )
            })
        };

        opened.wait();
        fs::rename(&output, &displaced).unwrap();
        fs::write(&output, b"replacement-native-image").unwrap();
        fs::set_permissions(&output, fs::Permissions::from_mode(0o600)).unwrap();
        continue_read.wait();

        assert_eq!(reader.join().unwrap(), Err(ProcessSpoolError::Integrity));
        assert_eq!(fs::read(displaced).unwrap(), b"original-native-image");
        assert_eq!(fs::read(output).unwrap(), b"replacement-native-image");
    }

    #[cfg(unix)]
    #[test]
    fn workspace_output_reader_rejects_file_aliases() {
        use std::os::unix::fs::symlink;

        let (temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let outside = temp.path().join("outside.png");
        fs::write(&outside, b"outside").unwrap();
        let output = spool.workspace_path().unwrap().join("provider-output.png");
        symlink(&outside, &output).unwrap();
        assert_eq!(
            spool.read_workspace_output("provider-output.png", 1024),
            Err(ProcessSpoolError::Integrity)
        );

        fs::remove_file(&output).unwrap();
        fs::hard_link(&outside, &output).unwrap();
        assert_eq!(
            spool.read_workspace_output("provider-output.png", 1024),
            Err(ProcessSpoolError::Integrity)
        );
    }

    #[cfg(unix)]
    #[test]
    fn workspace_output_reader_rejects_unsafe_modes_and_oversized_files() {
        use std::os::unix::fs::PermissionsExt;

        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let output = spool.workspace_path().unwrap().join("provider-output.png");
        fs::write(&output, b"image").unwrap();

        fs::set_permissions(&output, fs::Permissions::from_mode(0o666)).unwrap();
        assert_eq!(
            spool.read_workspace_output("provider-output.png", 1024),
            Err(ProcessSpoolError::Integrity)
        );
        fs::set_permissions(&output, fs::Permissions::from_mode(0o4644)).unwrap();
        assert_eq!(
            spool.read_workspace_output("provider-output.png", 1024),
            Err(ProcessSpoolError::Integrity)
        );
        fs::set_permissions(&output, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            spool.read_workspace_output("provider-output.png", 4),
            Err(ProcessSpoolError::Integrity)
        );
    }

    #[test]
    fn codex_failure_diagnostic_is_exact_bounded_and_idempotent() {
        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let diagnostic = serde_json::json!({
            "schema_version": 1,
            "failure_category": "codex_image_tool_failed",
            "class": "rate_limit"
        });

        spool
            .publish_diagnostic(CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE, &diagnostic)
            .unwrap();
        spool
            .publish_diagnostic(CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE, &diagnostic)
            .unwrap();
        assert_eq!(
            spool.publish_diagnostic(
                CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE,
                &serde_json::json!({ "schema_version": 2 })
            ),
            Err(ProcessSpoolError::Conflict)
        );
        assert_eq!(
            spool.publish_diagnostic("codex-other.json", &diagnostic),
            Err(ProcessSpoolError::InvalidInput)
        );
        assert_eq!(
            spool.publish_diagnostic(
                CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE,
                &serde_json::json!({ "payload": "x".repeat(MAX_DIAGNOSTIC_BYTES as usize) })
            ),
            Err(ProcessSpoolError::InvalidInput)
        );
        assert!(
            fs::metadata(
                spool
                    .root_path()
                    .join(CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE)
            )
            .unwrap()
            .len()
                <= MAX_DIAGNOSTIC_BYTES
        );
    }

    #[cfg(unix)]
    #[test]
    fn codex_failure_diagnostic_rejects_symlink_and_hardlink_targets() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        for hardlink in [false, true] {
            let (temp, journal, lease) = fixture();
            let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
            let outside = temp.path().join("outside-diagnostic");
            fs::write(&outside, b"outside-must-remain").unwrap();
            fs::set_permissions(&outside, fs::Permissions::from_mode(0o600)).unwrap();
            let diagnostic = spool
                .root_path()
                .join(CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE);
            if hardlink {
                fs::hard_link(&outside, &diagnostic).unwrap();
            } else {
                symlink(&outside, &diagnostic).unwrap();
            }

            assert_eq!(
                spool.publish_diagnostic(
                    CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE,
                    &serde_json::json!({ "schema_version": 1 })
                ),
                Err(ProcessSpoolError::Integrity)
            );
            assert_eq!(fs::read(&outside).unwrap(), b"outside-must-remain");
        }
    }

    #[test]
    fn conflicting_request_and_terminal_are_rejected() {
        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        spool.prepare_request(b"request-a").unwrap();
        assert_eq!(
            spool.prepare_request(b"request-b"),
            Err(ProcessSpoolError::Conflict)
        );
        let lock = spool.acquire_runner_lock().unwrap();
        let identity = lock.identity().unwrap();
        spool.publish_process(&lock, &identity).unwrap();
        spool
            .publish_terminal(
                &lock,
                &ProcessTerminal::Failed {
                    helper_nonce: identity.nonce.clone(),
                    error_code: "provider_failed".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            spool.publish_terminal(
                &lock,
                &ProcessTerminal::Uncertain {
                    helper_nonce: identity.nonce,
                    error_code: "runner_lost".to_string(),
                }
            ),
            Err(ProcessSpoolError::Conflict)
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_output_fails_integrity_without_following() {
        use std::os::unix::fs::symlink;

        let (temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let lock = spool.acquire_runner_lock().unwrap();
        let identity = lock.identity().unwrap();
        spool.publish_process(&lock, &identity).unwrap();
        let outside = temp.path().join("outside");
        fs::write(&outside, b"secret").unwrap();
        symlink(&outside, spool.root_path().join(OUTPUT_FILE)).unwrap();

        assert_eq!(
            spool.publish_output(b"image"),
            Err(ProcessSpoolError::Integrity)
        );
        assert_eq!(fs::read(outside).unwrap(), b"secret");
    }

    #[test]
    fn forged_terminal_with_another_helper_nonce_is_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let lock = spool.acquire_runner_lock().unwrap();
        let identity = lock.identity().unwrap();
        spool.publish_process(&lock, &identity).unwrap();
        let forged = ProcessTerminal::Failed {
            helper_nonce: Uuid::new_v4().to_string(),
            error_code: "provider_failed".to_string(),
        };

        assert_eq!(
            spool.publish_terminal(&lock, &forged),
            Err(ProcessSpoolError::Integrity)
        );
        let result = spool.root_path().join(RESULT_FILE);
        fs::write(&result, serde_json::to_vec(&forged).unwrap()).unwrap();
        fs::set_permissions(&result, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(spool.observe(), Err(ProcessSpoolError::Integrity));
    }

    #[test]
    fn swapped_lock_inode_is_integrity_failure_not_lost() {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let (_temp, journal, lease) = fixture();
        let spool = ExecutionSpool::for_lease(&journal, &lease).unwrap();
        let lock = spool.acquire_runner_lock().unwrap();
        let identity = lock.identity().unwrap();
        spool.publish_process(&lock, &identity).unwrap();
        let lock_path = spool.root_path().join(LOCK_FILE);
        fs::rename(&lock_path, spool.root_path().join("runner.lock.displaced")).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lock_path)
            .unwrap();
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600)).unwrap();

        assert_eq!(spool.observe(), Err(ProcessSpoolError::Integrity));
    }

    #[test]
    fn reused_pid_start_token_does_not_signal_provider_group() {
        use std::{os::unix::process::CommandExt, process::Command};

        let mut child = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let identity =
            ProviderProcessIdentity::capture(child.id(), &Uuid::new_v4().to_string()).unwrap();
        let mut reused = identity.clone();
        reused.start_token.push_str("-reused");

        assert!(!reused.kill_process_group_if_current().unwrap());
        assert!(child.try_wait().unwrap().is_none());
        assert!(identity.kill_process_group_if_current().unwrap());
        child.wait().unwrap();
    }

    fn fixture() -> (
        TempDir,
        Arc<FilesystemRunnerJournal>,
        ExecutorSubmissionLease,
    ) {
        let temp = TempDir::new().unwrap();
        let journal = Arc::new(FilesystemRunnerJournal::new(temp.path().join("journal")).unwrap());
        let lease = ExecutorSubmissionLease {
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            output_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            tenant_id: "tenant-1".to_string(),
            provider_id: "openai-codex".to_string(),
            model: "gpt-image-2".to_string(),
            work_item_id: Uuid::new_v4(),
            output_index: 0,
            command_schema: "openai.images.generation.v1".to_string(),
            command_hash: "a".repeat(64),
            execution_profile_id: Uuid::new_v4(),
            adapter_revision: "openai-codex-generation-v1".to_string(),
            executor_owner: "owner-1".to_string(),
            executor_lease_epoch: 1,
            executor_lease_expires_at_ms: i64::MAX,
        };
        journal.start_or_attach(&lease).unwrap();
        (temp, journal, lease)
    }
}
