use std::{
    fs,
    io::{self, Read, Write},
    os::fd::OwnedFd,
    path::{Path, PathBuf},
};

use rustix::{
    fs::{self as rfs, AtFlags, FileType, Mode, OFlags, RenameFlags},
    io::Errno,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::executor::{
    ExecutorResultManifest, ExecutorSubmissionLease, RunnerOutcome, error_code_is_valid,
    result_manifest_is_valid,
};

use super::{LaunchDecision, RunnerJournalError, RunnerJournalObservation};

const SPEC_FILE: &str = "spec.json";
const LAUNCH_FILE: &str = "launch.json";
const TERMINAL_FILE: &str = "terminal.json";
const MAX_JOURNAL_FILE_BYTES: u64 = 64 * 1024;
const MAX_TEXT_BYTES: usize = 1024;

pub struct FilesystemRunnerJournal {
    root: OwnedFd,
}

struct ExecutionDirectory {
    fd: OwnedFd,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskSpec {
    executor_execution_id: String,
    submission_id: String,
    output_id: String,
    job_id: String,
    work_item_id: String,
    output_index: i32,
    provider_id: String,
    model: String,
    command_schema: String,
    command_hash: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskLaunch {
    owner: String,
    epoch: i64,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum DiskTerminal {
    Succeeded {
        manifest_id: String,
        artifact_authority_id: String,
    },
    Failed {
        error_code: String,
    },
    Uncertain {
        error_code: String,
    },
}

enum PublishResult {
    Created,
    Exists,
}

impl FilesystemRunnerJournal {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, RunnerJournalError> {
        let root = prepare_root(root.as_ref())?;
        let root = rfs::open(
            &root,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| RunnerJournalError::InvalidInput)?;
        validate_directory_fd(&root, RunnerJournalError::InvalidInput)?;
        Ok(Self { root })
    }

    pub fn start_or_attach(
        &self,
        lease: &ExecutorSubmissionLease,
    ) -> Result<RunnerJournalObservation, RunnerJournalError> {
        let directory = self.ensure_spec(lease)?;
        self.observe(&directory, lease)
    }

    pub fn commit_launch(
        &self,
        lease: &ExecutorSubmissionLease,
    ) -> Result<LaunchDecision, RunnerJournalError> {
        validate_lease(lease)?;
        let directory = self.ensure_spec(lease)?;
        if self.observe(&directory, lease)? != RunnerJournalObservation::Prepared {
            return Ok(LaunchDecision::Attach);
        }
        let launch = DiskLaunch::from_lease(lease);
        match self.publish_json(&directory, LAUNCH_FILE, &launch)? {
            PublishResult::Created => Ok(LaunchDecision::LaunchOnce),
            PublishResult::Exists => {
                self.read_launch(&directory, lease)?;
                Ok(LaunchDecision::Attach)
            }
        }
    }

    pub fn publish_terminal(
        &self,
        lease: &ExecutorSubmissionLease,
        outcome: &RunnerOutcome,
    ) -> Result<(), RunnerJournalError> {
        validate_lease(lease)?;
        let terminal = DiskTerminal::from_outcome(outcome)?;
        let directory = self.ensure_spec(lease)?;
        match self.observe(&directory, lease)? {
            RunnerJournalObservation::Prepared => return Err(RunnerJournalError::Integrity),
            RunnerJournalObservation::Terminal(existing) if existing != *outcome => {
                return Err(RunnerJournalError::Conflict);
            }
            RunnerJournalObservation::Terminal(_) => return Ok(()),
            RunnerJournalObservation::LaunchCommitted => {}
        }
        match self.publish_json(&directory, TERMINAL_FILE, &terminal)? {
            PublishResult::Created => Ok(()),
            PublishResult::Exists if self.read_terminal(&directory)? == terminal => Ok(()),
            PublishResult::Exists => Err(RunnerJournalError::Conflict),
        }
    }

    fn ensure_spec(
        &self,
        lease: &ExecutorSubmissionLease,
    ) -> Result<ExecutionDirectory, RunnerJournalError> {
        validate_lease(lease)?;
        let directory = self.execution_directory(lease.executor_execution_id)?;
        let expected = DiskSpec::from_lease(lease);
        match self.publish_json(&directory, SPEC_FILE, &expected)? {
            PublishResult::Created => Ok(directory),
            PublishResult::Exists => {
                let actual = self.read_json::<DiskSpec>(&directory, SPEC_FILE)?;
                actual.validate()?;
                if actual == expected {
                    Ok(directory)
                } else {
                    Err(RunnerJournalError::Conflict)
                }
            }
        }
    }

    fn observe(
        &self,
        directory: &ExecutionDirectory,
        lease: &ExecutorSubmissionLease,
    ) -> Result<RunnerJournalObservation, RunnerJournalError> {
        let (terminal, launch) = read_markers_in_order(
            || self.read_optional_json::<DiskTerminal>(directory, TERMINAL_FILE),
            || self.read_optional_json::<DiskLaunch>(directory, LAUNCH_FILE),
        )?;
        if launch.is_none() && terminal.is_some() {
            return Err(RunnerJournalError::Integrity);
        }
        if let Some(launch) = launch {
            launch.validate_for(lease)?;
        } else {
            return Ok(RunnerJournalObservation::Prepared);
        }
        match terminal {
            Some(value) => Ok(RunnerJournalObservation::Terminal(value.into_outcome()?)),
            None => Ok(RunnerJournalObservation::LaunchCommitted),
        }
    }

    fn execution_directory(&self, id: Uuid) -> Result<ExecutionDirectory, RunnerJournalError> {
        validate_directory_fd(&self.root, RunnerJournalError::Integrity)?;
        let name = id.simple().to_string();
        match rfs::mkdirat(&self.root, &name, Mode::RWXU) {
            Ok(()) => rfs::fsync(&self.root).map_err(|_| RunnerJournalError::Unavailable)?,
            Err(Errno::EXIST) => {}
            Err(_) => return Err(RunnerJournalError::Unavailable),
        }
        let fd = rfs::openat(
            &self.root,
            &name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| RunnerJournalError::Integrity)?;
        validate_directory_fd(&fd, RunnerJournalError::Integrity)?;
        Ok(ExecutionDirectory { fd })
    }

    fn publish_json<T: Serialize>(
        &self,
        directory: &ExecutionDirectory,
        name: &str,
        value: &T,
    ) -> Result<PublishResult, RunnerJournalError> {
        publish_json_at(&directory.fd, name, value)
    }

    fn read_json<T: DeserializeOwned>(
        &self,
        directory: &ExecutionDirectory,
        name: &str,
    ) -> Result<T, RunnerJournalError> {
        self.read_optional_json(directory, name)?
            .ok_or(RunnerJournalError::Integrity)
    }

    fn read_optional_json<T: DeserializeOwned>(
        &self,
        directory: &ExecutionDirectory,
        name: &str,
    ) -> Result<Option<T>, RunnerJournalError> {
        read_optional_json_at(&directory.fd, name)
    }

    fn read_launch(
        &self,
        directory: &ExecutionDirectory,
        lease: &ExecutorSubmissionLease,
    ) -> Result<DiskLaunch, RunnerJournalError> {
        let launch = self.read_json::<DiskLaunch>(directory, LAUNCH_FILE)?;
        launch.validate_for(lease)?;
        Ok(launch)
    }

    fn read_terminal(
        &self,
        directory: &ExecutionDirectory,
    ) -> Result<DiskTerminal, RunnerJournalError> {
        let terminal = self.read_json::<DiskTerminal>(directory, TERMINAL_FILE)?;
        terminal.clone_for_validation()?;
        Ok(terminal)
    }
}

fn read_markers_in_order<T, L, E>(
    read_terminal: impl FnOnce() -> Result<T, E>,
    read_launch: impl FnOnce() -> Result<L, E>,
) -> Result<(T, L), E> {
    let terminal = read_terminal()?;
    let launch = read_launch()?;
    Ok((terminal, launch))
}

impl DiskSpec {
    fn from_lease(lease: &ExecutorSubmissionLease) -> Self {
        Self {
            executor_execution_id: lease.executor_execution_id.to_string(),
            submission_id: lease.submission_id.to_string(),
            output_id: lease.output_id.to_string(),
            job_id: lease.job_id.to_string(),
            work_item_id: lease.work_item_id.to_string(),
            output_index: lease.output_index,
            provider_id: lease.provider_id.clone(),
            model: lease.model.clone(),
            command_schema: lease.command_schema.clone(),
            command_hash: lease.command_hash.clone(),
        }
    }

    fn validate(&self) -> Result<(), RunnerJournalError> {
        for value in [
            &self.executor_execution_id,
            &self.submission_id,
            &self.output_id,
            &self.job_id,
            &self.work_item_id,
        ] {
            if parse_uuid(value)?.to_string() != *value {
                return Err(RunnerJournalError::Integrity);
            }
        }
        for value in [&self.provider_id, &self.model, &self.command_schema] {
            validate_text(value)?;
        }
        if self.output_index < 0 || !is_sha256(&self.command_hash) {
            return Err(RunnerJournalError::Integrity);
        }
        Ok(())
    }
}

impl DiskLaunch {
    fn from_lease(lease: &ExecutorSubmissionLease) -> Self {
        Self {
            owner: lease.executor_owner.clone(),
            epoch: lease.executor_lease_epoch,
        }
    }

    fn validate_for(&self, lease: &ExecutorSubmissionLease) -> Result<(), RunnerJournalError> {
        validate_text(&self.owner)?;
        if self.epoch < 0 {
            return Err(RunnerJournalError::Integrity);
        }
        if self.owner != lease.executor_owner || self.epoch != lease.executor_lease_epoch {
            return Err(RunnerJournalError::Conflict);
        }
        Ok(())
    }
}

impl DiskTerminal {
    fn from_outcome(outcome: &RunnerOutcome) -> Result<Self, RunnerJournalError> {
        match outcome {
            RunnerOutcome::Succeeded(manifest) => {
                validate_manifest(manifest, RunnerJournalError::InvalidInput)?;
                Ok(Self::Succeeded {
                    manifest_id: manifest.manifest_id.to_string(),
                    artifact_authority_id: manifest.artifact_authority_id.to_string(),
                })
            }
            RunnerOutcome::Failed { error_code } => Ok(Self::Failed {
                error_code: validated_error_code(error_code)?,
            }),
            RunnerOutcome::Uncertain { error_code } => Ok(Self::Uncertain {
                error_code: validated_error_code(error_code)?,
            }),
        }
    }

    fn into_outcome(self) -> Result<RunnerOutcome, RunnerJournalError> {
        match self {
            Self::Succeeded {
                manifest_id,
                artifact_authority_id,
            } => {
                let manifest = ExecutorResultManifest::new(
                    parse_uuid(&manifest_id)?,
                    parse_uuid(&artifact_authority_id)?,
                )
                .ok_or(RunnerJournalError::Integrity)?;
                validate_manifest(&manifest, RunnerJournalError::Integrity)?;
                Ok(RunnerOutcome::Succeeded(manifest))
            }
            Self::Failed { error_code } => {
                validate_text(&error_code)?;
                Ok(RunnerOutcome::Failed { error_code })
            }
            Self::Uncertain { error_code } => {
                validate_text(&error_code)?;
                Ok(RunnerOutcome::Uncertain { error_code })
            }
        }
    }
}

fn prepare_root(root: &Path) -> Result<PathBuf, RunnerJournalError> {
    if !root.is_absolute() {
        return Err(RunnerJournalError::InvalidInput);
    }
    match fs::symlink_metadata(root) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => match create_private_dir(root) {
            Ok(()) => sync_parent(root)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(RunnerJournalError::Unavailable),
        },
        Err(_) => return Err(RunnerJournalError::Unavailable),
    }
    secure_directory(root, RunnerJournalError::InvalidInput)?;
    sync_directory(root)?;
    fs::canonicalize(root).map_err(|_| RunnerJournalError::Unavailable)
}

fn secure_directory(path: &Path, invalid: RunnerJournalError) -> Result<(), RunnerJournalError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| RunnerJournalError::Unavailable)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(invalid);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        // SAFETY: geteuid has no preconditions and does not dereference pointers.
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o777 != 0o700
        {
            return Err(invalid);
        }
    }
    Ok(())
}

fn validate_directory_fd(
    fd: &OwnedFd,
    invalid: RunnerJournalError,
) -> Result<(), RunnerJournalError> {
    let stat = rfs::fstat(fd).map_err(|_| RunnerJournalError::Unavailable)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
        || Mode::from_raw_mode(stat.st_mode) != Mode::RWXU
    {
        return Err(invalid);
    }
    // SAFETY: geteuid has no preconditions and does not dereference pointers.
    if stat.st_uid != unsafe { libc::geteuid() } {
        return Err(invalid);
    }
    Ok(())
}

fn publish_json_at<T: Serialize>(
    directory: &OwnedFd,
    name: &str,
    value: &T,
) -> Result<PublishResult, RunnerJournalError> {
    let bytes = serde_json::to_vec(value).map_err(|_| RunnerJournalError::InvalidInput)?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_JOURNAL_FILE_BYTES {
        return Err(RunnerJournalError::InvalidInput);
    }
    let temporary = format!(".tmp-{}", Uuid::new_v4().simple());
    if let Err(error) = write_temporary_at(directory, &temporary, &bytes) {
        cleanup_temporary(directory, &temporary);
        return Err(error);
    }
    match rfs::renameat_with(
        directory,
        &temporary,
        directory,
        name,
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {
            rfs::fsync(directory).map_err(|_| RunnerJournalError::Unavailable)?;
            Ok(PublishResult::Created)
        }
        Err(Errno::EXIST) => {
            rfs::unlinkat(directory, &temporary, AtFlags::empty())
                .map_err(|_| RunnerJournalError::Unavailable)?;
            rfs::fsync(directory).map_err(|_| RunnerJournalError::Unavailable)?;
            Ok(PublishResult::Exists)
        }
        Err(_) => {
            cleanup_temporary(directory, &temporary);
            Err(RunnerJournalError::Unavailable)
        }
    }
}

fn write_temporary_at(
    directory: &OwnedFd,
    name: &str,
    bytes: &[u8],
) -> Result<(), RunnerJournalError> {
    let fd = rfs::openat(
        directory,
        name,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::CLOEXEC
            | OFlags::NONBLOCK,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| RunnerJournalError::Unavailable)?;
    rfs::fchmod(&fd, Mode::RUSR | Mode::WUSR).map_err(|_| RunnerJournalError::Unavailable)?;
    let mut file = fs::File::from(fd);
    file.write_all(bytes)
        .map_err(|_| RunnerJournalError::Unavailable)?;
    rfs::fsync(&file).map_err(|_| RunnerJournalError::Unavailable)
}

fn cleanup_temporary(directory: &OwnedFd, name: &str) {
    let _ = rfs::unlinkat(directory, name, AtFlags::empty());
    let _ = rfs::fsync(directory);
}

fn read_optional_json_at<T: DeserializeOwned>(
    directory: &OwnedFd,
    name: &str,
) -> Result<Option<T>, RunnerJournalError> {
    let fd = match rfs::openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(None),
        Err(_) => return Err(RunnerJournalError::Integrity),
    };
    let mut file = fs::File::from(fd);
    let stat = rfs::fstat(&file).map_err(|_| RunnerJournalError::Unavailable)?;
    let size = validate_file_stat(&stat)?;
    let mut bytes = Vec::with_capacity(size);
    Read::by_ref(&mut file)
        .take(MAX_JOURNAL_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| RunnerJournalError::Unavailable)?;
    let final_stat = rfs::fstat(&file).map_err(|_| RunnerJournalError::Unavailable)?;
    if validate_file_stat(&final_stat)? != size || bytes.len() != size {
        return Err(RunnerJournalError::Integrity);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| RunnerJournalError::Integrity)
}

fn validate_file_stat(stat: &rfs::Stat) -> Result<usize, RunnerJournalError> {
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || Mode::from_raw_mode(stat.st_mode) != Mode::RUSR | Mode::WUSR
        || stat.st_nlink != 1
        // SAFETY: geteuid has no preconditions and does not dereference pointers.
        || stat.st_uid != unsafe { libc::geteuid() }
        || stat.st_size <= 0
        || stat.st_size > MAX_JOURNAL_FILE_BYTES as i64
    {
        return Err(RunnerJournalError::Integrity);
    }
    usize::try_from(stat.st_size).map_err(|_| RunnerJournalError::Integrity)
}

fn validate_lease(lease: &ExecutorSubmissionLease) -> Result<(), RunnerJournalError> {
    if lease.output_index < 0 || lease.executor_lease_epoch < 0 {
        return Err(RunnerJournalError::InvalidInput);
    }
    for value in [
        &lease.provider_id,
        &lease.model,
        &lease.command_schema,
        &lease.executor_owner,
    ] {
        validate_input_text(value)?;
    }
    if !is_sha256(&lease.command_hash) {
        return Err(RunnerJournalError::InvalidInput);
    }
    Ok(())
}

fn validate_manifest(
    manifest: &ExecutorResultManifest,
    error: RunnerJournalError,
) -> Result<(), RunnerJournalError> {
    if !result_manifest_is_valid(manifest) {
        return Err(error);
    }
    Ok(())
}

fn validated_error_code(value: &str) -> Result<String, RunnerJournalError> {
    error_code_is_valid(value)
        .then(|| value.to_string())
        .ok_or(RunnerJournalError::InvalidInput)
}

fn validate_input_text(value: &str) -> Result<(), RunnerJournalError> {
    validate_text(value).map_err(|_| RunnerJournalError::InvalidInput)
}

fn validate_text(value: &str) -> Result<(), RunnerJournalError> {
    if value.is_empty() || value.len() > MAX_TEXT_BYTES || value.chars().any(char::is_control) {
        return Err(RunnerJournalError::Integrity);
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_uuid(value: &str) -> Result<Uuid, RunnerJournalError> {
    Uuid::parse_str(value).map_err(|_| RunnerJournalError::Integrity)
}

fn sync_parent(path: &Path) -> Result<(), RunnerJournalError> {
    let parent = path.parent().ok_or(RunnerJournalError::InvalidInput)?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), RunnerJournalError> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| RunnerJournalError::Unavailable)
}

impl DiskTerminal {
    fn clone_for_validation(&self) -> Result<(), RunnerJournalError> {
        match self {
            Self::Succeeded {
                manifest_id,
                artifact_authority_id,
            } => {
                parse_uuid(manifest_id)?;
                parse_uuid(artifact_authority_id)?;
                let manifest = ExecutorResultManifest::new(
                    parse_uuid(manifest_id)?,
                    parse_uuid(artifact_authority_id)?,
                )
                .ok_or(RunnerJournalError::Integrity)?;
                result_manifest_is_valid(&manifest)
                    .then_some(())
                    .ok_or(RunnerJournalError::Integrity)
            }
            Self::Failed { error_code } | Self::Uncertain { error_code } => {
                error_code_is_valid(error_code)
                    .then_some(())
                    .ok_or(RunnerJournalError::Integrity)
            }
        }
    }
}

#[cfg(unix)]
fn create_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700).create(path)
}

#[cfg(not(unix))]
fn create_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir(path)
}

#[cfg(test)]
mod fd_tests {
    use std::cell::RefCell;

    use super::*;

    #[test]
    fn terminal_marker_is_read_before_launch_marker() {
        let reads = RefCell::new(Vec::new());

        read_markers_in_order(
            || {
                reads.borrow_mut().push("terminal");
                Ok::<_, ()>(())
            },
            || {
                reads.borrow_mut().push("launch");
                Ok::<_, ()>(())
            },
        )
        .unwrap();

        assert_eq!(*reads.borrow(), ["terminal", "launch"]);
    }

    #[test]
    fn opened_execution_fd_is_not_redirected_by_path_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("journal");
        let journal = FilesystemRunnerJournal::new(&root).unwrap();
        let id = Uuid::new_v4();
        let directory = journal.execution_directory(id).unwrap();
        let visible = root.join(id.simple().to_string());
        let original = temp.path().join("original-execution");
        fs::rename(&visible, &original).unwrap();
        fs::create_dir(&visible).unwrap();

        assert!(matches!(
            publish_json_at(
                &directory.fd,
                "bound.json",
                &serde_json::json!({ "bound": true })
            ),
            Ok(PublishResult::Created)
        ));
        assert!(original.join("bound.json").is_file());
        assert!(fs::read_dir(visible).unwrap().next().is_none());
    }
}
