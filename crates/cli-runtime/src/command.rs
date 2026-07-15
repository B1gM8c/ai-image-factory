use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs::{self, File, OpenOptions},
    io::Read,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::output::OutputContract;

pub const MAX_STDIN_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct VerifiedExecutable {
    path: PathBuf,
    identity: FileIdentity,
}

#[derive(Clone, Debug)]
pub struct WorkingDirectory {
    path: PathBuf,
    directory: Arc<File>,
    identity: FileIdentity,
}

#[derive(Clone, Debug)]
pub struct CommandSpec {
    executable: VerifiedExecutable,
    arguments: Vec<OsString>,
    environment: BTreeMap<OsString, OsString>,
    working_directory: WorkingDirectory,
    stdin: Vec<u8>,
    wall_timeout: Duration,
    termination_grace: Duration,
    output: Option<OutputContract>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    mode: u32,
    size: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[derive(Debug, Error)]
pub enum CommandSpecError {
    #[error("executable path must be absolute")]
    ExecutableNotAbsolute,
    #[error("executable is unavailable: {0}")]
    ExecutableUnavailable(#[source] std::io::Error),
    #[error("executable must be a non-writable regular executable")]
    InvalidExecutable,
    #[error("executable identity changed after verification")]
    ExecutableChanged,
    #[error("executable SHA-256 does not match the configured digest")]
    ExecutableDigestMismatch,
    #[error("working directory path must be absolute")]
    WorkingDirectoryNotAbsolute,
    #[error("working directory is unavailable: {0}")]
    WorkingDirectoryUnavailable(#[source] std::io::Error),
    #[error("working directory must be a real directory")]
    InvalidWorkingDirectory,
    #[error("working directory identity changed after verification")]
    WorkingDirectoryChanged,
    #[error("wall timeout and termination grace must be non-zero")]
    InvalidTimeout,
    #[error("argument contains a NUL byte")]
    InvalidArgument,
    #[error("environment key is empty or contains '=' or a NUL byte")]
    InvalidEnvironmentKey,
    #[error("environment value contains a NUL byte")]
    InvalidEnvironmentValue,
    #[error("output filename must be one non-empty relative path component")]
    InvalidOutputFilename,
    #[error("output size limit must be non-zero")]
    InvalidOutputLimit,
    #[error("stdin exceeds the 8 MiB runtime limit")]
    StdinTooLarge,
}

impl VerifiedExecutable {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, CommandSpecError> {
        let path = path.as_ref();
        if !path.is_absolute() {
            return Err(CommandSpecError::ExecutableNotAbsolute);
        }
        let canonical = fs::canonicalize(path).map_err(CommandSpecError::ExecutableUnavailable)?;
        let metadata = metadata_without_symlink(&canonical)
            .map_err(CommandSpecError::ExecutableUnavailable)?;
        if !metadata.is_file()
            || metadata.permissions().mode() & 0o111 == 0
            || metadata.permissions().mode() & 0o022 != 0
        {
            return Err(CommandSpecError::InvalidExecutable);
        }
        Ok(Self {
            path: canonical,
            identity: FileIdentity::from_metadata(&metadata),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn new_with_sha256(
        path: impl AsRef<Path>,
        expected_sha256: [u8; 32],
    ) -> Result<Self, CommandSpecError> {
        let executable = Self::new(path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&executable.path)
            .map_err(CommandSpecError::ExecutableUnavailable)?;
        let metadata = file
            .metadata()
            .map_err(CommandSpecError::ExecutableUnavailable)?;
        if FileIdentity::from_metadata(&metadata) != executable.identity {
            return Err(CommandSpecError::ExecutableChanged);
        }

        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(CommandSpecError::ExecutableUnavailable)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        executable.revalidate()?;
        let actual: [u8; 32] = hasher.finalize().into();
        if actual != expected_sha256 {
            return Err(CommandSpecError::ExecutableDigestMismatch);
        }
        Ok(executable)
    }

    fn revalidate(&self) -> Result<(), CommandSpecError> {
        let metadata = metadata_without_symlink(&self.path)
            .map_err(|_| CommandSpecError::ExecutableChanged)?;
        if !metadata.is_file()
            || metadata.permissions().mode() & 0o111 == 0
            || metadata.permissions().mode() & 0o022 != 0
            || FileIdentity::from_metadata(&metadata) != self.identity
        {
            return Err(CommandSpecError::ExecutableChanged);
        }
        Ok(())
    }
}

impl WorkingDirectory {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, CommandSpecError> {
        let path = path.as_ref();
        if !path.is_absolute() {
            return Err(CommandSpecError::WorkingDirectoryNotAbsolute);
        }
        let canonical =
            fs::canonicalize(path).map_err(CommandSpecError::WorkingDirectoryUnavailable)?;
        let metadata = metadata_without_symlink(&canonical)
            .map_err(CommandSpecError::WorkingDirectoryUnavailable)?;
        if !metadata.is_dir() {
            return Err(CommandSpecError::InvalidWorkingDirectory);
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&canonical)
            .map_err(CommandSpecError::WorkingDirectoryUnavailable)?;
        Ok(Self {
            path: canonical,
            directory: Arc::new(directory),
            identity: FileIdentity::from_metadata(&metadata),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn directory(&self) -> Arc<File> {
        Arc::clone(&self.directory)
    }

    fn revalidate(&self) -> Result<(), CommandSpecError> {
        let metadata = metadata_without_symlink(&self.path)
            .map_err(|_| CommandSpecError::WorkingDirectoryChanged)?;
        if !self.identity.matches_directory(&metadata) {
            return Err(CommandSpecError::WorkingDirectoryChanged);
        }
        let bound = self
            .directory
            .metadata()
            .map_err(|_| CommandSpecError::WorkingDirectoryChanged)?;
        if !self.identity.matches_directory(&bound) {
            return Err(CommandSpecError::WorkingDirectoryChanged);
        }
        Ok(())
    }
}

impl CommandSpec {
    pub fn new(
        executable: VerifiedExecutable,
        working_directory: WorkingDirectory,
        output: OutputContract,
        wall_timeout: Duration,
        termination_grace: Duration,
    ) -> Result<Self, CommandSpecError> {
        Self::new_inner(
            executable,
            working_directory,
            Some(output),
            wall_timeout,
            termination_grace,
        )
    }

    pub fn new_receipt(
        executable: VerifiedExecutable,
        working_directory: WorkingDirectory,
        wall_timeout: Duration,
        termination_grace: Duration,
    ) -> Result<Self, CommandSpecError> {
        Self::new_inner(
            executable,
            working_directory,
            None,
            wall_timeout,
            termination_grace,
        )
    }

    pub fn arg(mut self, argument: impl Into<OsString>) -> Result<Self, CommandSpecError> {
        let argument = argument.into();
        validate_no_nul(&argument).map_err(|_| CommandSpecError::InvalidArgument)?;
        self.arguments.push(argument);
        Ok(self)
    }

    pub fn env(
        mut self,
        key: impl Into<OsString>,
        value: impl Into<OsString>,
    ) -> Result<Self, CommandSpecError> {
        let key = key.into();
        let value = value.into();
        let key_bytes = key.as_os_str().as_bytes();
        if key_bytes.is_empty() || key_bytes.contains(&b'=') || key_bytes.contains(&0) {
            return Err(CommandSpecError::InvalidEnvironmentKey);
        }
        validate_no_nul(&value).map_err(|_| CommandSpecError::InvalidEnvironmentValue)?;
        self.environment.insert(key, value);
        Ok(self)
    }

    pub fn stdin(mut self, stdin: impl Into<Vec<u8>>) -> Result<Self, CommandSpecError> {
        let stdin = stdin.into();
        if stdin.len() > MAX_STDIN_BYTES {
            return Err(CommandSpecError::StdinTooLarge);
        }
        self.stdin = stdin;
        Ok(self)
    }

    pub fn executable(&self) -> &VerifiedExecutable {
        &self.executable
    }

    pub fn arguments(&self) -> &[OsString] {
        &self.arguments
    }

    pub fn environment(&self) -> &BTreeMap<OsString, OsString> {
        &self.environment
    }

    pub fn working_directory(&self) -> &WorkingDirectory {
        &self.working_directory
    }

    pub fn stdin_bytes(&self) -> &[u8] {
        &self.stdin
    }

    pub fn wall_timeout(&self) -> Duration {
        self.wall_timeout
    }

    pub fn termination_grace(&self) -> Duration {
        self.termination_grace
    }

    pub fn output(&self) -> Option<&OutputContract> {
        self.output.as_ref()
    }

    pub(crate) fn revalidate(&self) -> Result<(), CommandSpecError> {
        self.executable.revalidate()?;
        self.working_directory.revalidate()?;
        Ok(())
    }

    fn new_inner(
        executable: VerifiedExecutable,
        working_directory: WorkingDirectory,
        output: Option<OutputContract>,
        wall_timeout: Duration,
        termination_grace: Duration,
    ) -> Result<Self, CommandSpecError> {
        if wall_timeout.is_zero() || termination_grace.is_zero() {
            return Err(CommandSpecError::InvalidTimeout);
        }
        Ok(Self {
            executable,
            arguments: Vec::new(),
            environment: BTreeMap::new(),
            working_directory,
            stdin: Vec::new(),
            wall_timeout,
            termination_grace,
            output,
        })
    }
}

impl FileIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            mode: metadata.mode(),
            size: metadata.size(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    }

    fn matches_directory(&self, metadata: &fs::Metadata) -> bool {
        metadata.is_dir()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
            && metadata.mode() & libc::S_IFMT as u32 == self.mode & libc::S_IFMT as u32
    }
}

pub(crate) fn validate_output_filename(path: &Path) -> Result<(), CommandSpecError> {
    let mut components = path.components();
    let Some(Component::Normal(name)) = components.next() else {
        return Err(CommandSpecError::InvalidOutputFilename);
    };
    if components.next().is_some() || name.is_empty() || name.as_bytes().contains(&0) {
        return Err(CommandSpecError::InvalidOutputFilename);
    }
    Ok(())
}

fn metadata_without_symlink(path: &Path) -> std::io::Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "symbolic link is not allowed",
        ));
    }
    Ok(metadata)
}

fn validate_no_nul(value: &OsStr) -> Result<(), ()> {
    if value.as_bytes().contains(&0) {
        Err(())
    } else {
        Ok(())
    }
}
