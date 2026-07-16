use std::{
    ffi::{OsStr, OsString},
    fs::{self, File},
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        fs::MetadataExt,
    },
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use rustix::{
    fs::{self as rfs, AtFlags, Dir, FileType, FlockOperation, Mode, OFlags},
    io::Errno,
};
use thiserror::Error;

use crate::WorkingDirectory;

pub const ATTEMPT_WORKSPACE_LOCK_FILENAME: &str = ".cli-attempt-workspace.lock";
const ATTEMPT_NAME_RETRIES: usize = 32;
const MAX_CLEANUP_DEPTH: usize = 32;
const MAX_CLEANUP_ENTRIES: usize = 1024;
static NEXT_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

pub struct ExclusiveAttemptWorkspace {
    root: PathBuf,
    directory: File,
    attempt_prefix: String,
    device: u64,
    _lock: File,
    cleaned_attempts: u64,
}

pub struct AttemptDirectory {
    root: File,
    path: PathBuf,
    name: OsString,
    directory: File,
    root_device: u64,
}

pub struct RecoverableAttemptWorkspace {
    root: PathBuf,
    directory: File,
    attempt_prefix: String,
    device: u64,
}

pub struct RecoverableAttemptDirectory {
    path: PathBuf,
    directory: File,
    root_device: u64,
}

impl ExclusiveAttemptWorkspace {
    pub fn acquire(
        root: &WorkingDirectory,
        attempt_prefix: &str,
    ) -> Result<Self, AttemptWorkspaceError> {
        validate_prefix(attempt_prefix)?;
        let path = root.path().to_owned();
        let directory = root
            .directory()
            .try_clone()
            .map_err(|_| AttemptWorkspaceError::Unavailable)?;
        let stat = rfs::fstat(&directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        let device = stat.st_dev as u64;
        validate_bound_directory(&path, &directory, device)?;
        let lock = acquire_lock(&directory)?;
        let cleaned_attempts = cleanup_attempts(&directory, attempt_prefix.as_bytes(), device)?;
        Ok(Self {
            root: path,
            directory,
            attempt_prefix: attempt_prefix.to_owned(),
            device,
            _lock: lock,
            cleaned_attempts,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn cleaned_attempts(&self) -> u64 {
        self.cleaned_attempts
    }

    pub fn create_attempt(&self) -> Result<AttemptDirectory, AttemptWorkspaceError> {
        validate_bound_directory(&self.root, &self.directory, self.device)?;
        for _ in 0..ATTEMPT_NAME_RETRIES {
            let name = self.next_attempt_name();
            match rfs::mkdirat(&self.directory, &name, Mode::RWXU) {
                Ok(()) => return self.bind_created_attempt(name),
                Err(Errno::EXIST) => continue,
                Err(_) => return Err(AttemptWorkspaceError::Unavailable),
            }
        }
        Err(AttemptWorkspaceError::Unavailable)
    }

    fn next_attempt_name(&self) -> OsString {
        let sequence = NEXT_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed);
        format!(
            "{}{:08x}-{sequence:016x}",
            self.attempt_prefix,
            process::id()
        )
        .into()
    }

    fn bind_created_attempt(
        &self,
        name: OsString,
    ) -> Result<AttemptDirectory, AttemptWorkspaceError> {
        let result = (|| {
            let descriptor = open_directory_at(&self.directory, &name)?;
            rfs::fchmod(&descriptor, Mode::RWXU).map_err(|_| AttemptWorkspaceError::Unavailable)?;
            let stat = rfs::fstat(&descriptor).map_err(|_| AttemptWorkspaceError::Unavailable)?;
            if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
                || Mode::from_raw_mode(stat.st_mode) != Mode::RWXU
                || stat.st_uid != effective_user_id()
                || stat.st_dev as u64 != self.device
            {
                return Err(AttemptWorkspaceError::Integrity);
            }
            let current = stat_without_following(&self.directory, &name)?;
            if !same_object(&stat, &current) {
                return Err(AttemptWorkspaceError::Integrity);
            }
            validate_bound_directory(&self.root, &self.directory, self.device)?;
            Ok(AttemptDirectory {
                root: self
                    .directory
                    .try_clone()
                    .map_err(|_| AttemptWorkspaceError::Unavailable)?,
                path: self.root.join(&name),
                name: name.clone(),
                directory: File::from(descriptor),
                root_device: self.device,
            })
        })();
        if result.is_err() {
            let _ = rfs::unlinkat(&self.directory, &name, AtFlags::REMOVEDIR);
        }
        result
    }
}

impl RecoverableAttemptWorkspace {
    pub fn new(
        root: &WorkingDirectory,
        attempt_prefix: &str,
    ) -> Result<Self, AttemptWorkspaceError> {
        validate_prefix(attempt_prefix)?;
        let path = root.path().to_owned();
        let directory = root
            .directory()
            .try_clone()
            .map_err(|_| AttemptWorkspaceError::Unavailable)?;
        let stat = rfs::fstat(&directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        let device = stat.st_dev as u64;
        validate_bound_directory(&path, &directory, device)?;
        Ok(Self {
            root: path,
            directory,
            attempt_prefix: attempt_prefix.to_owned(),
            device,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn open_or_create(
        &self,
        attempt_key: &str,
    ) -> Result<RecoverableAttemptDirectory, AttemptWorkspaceError> {
        let name = self.attempt_name(attempt_key)?;
        validate_bound_directory(&self.root, &self.directory, self.device)?;
        let created = match rfs::mkdirat(&self.directory, &name, Mode::RWXU) {
            Ok(()) => true,
            Err(Errno::EXIST) => false,
            Err(_) => return Err(AttemptWorkspaceError::Unavailable),
        };
        let result = self.bind_attempt(&name, created);
        if result.is_err() && created {
            let _ = rfs::unlinkat(&self.directory, &name, AtFlags::REMOVEDIR);
        }
        result
    }

    pub fn remove(&self, attempt_key: &str) -> Result<bool, AttemptWorkspaceError> {
        let name = self.attempt_name(attempt_key)?;
        validate_bound_directory(&self.root, &self.directory, self.device)?;
        let mut budget = CleanupBudget::new();
        let removed = remove_recoverable_attempt(&self.directory, &name, self.device, &mut budget)?;
        if removed {
            rfs::fsync(&self.directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        }
        Ok(removed)
    }

    fn attempt_name(&self, attempt_key: &str) -> Result<OsString, AttemptWorkspaceError> {
        validate_attempt_key(attempt_key)?;
        let name = format!("{}{attempt_key}", self.attempt_prefix);
        if name.len() > 255 {
            return Err(AttemptWorkspaceError::InvalidConfiguration);
        }
        Ok(name.into())
    }

    fn bind_attempt(
        &self,
        name: &OsStr,
        created: bool,
    ) -> Result<RecoverableAttemptDirectory, AttemptWorkspaceError> {
        let descriptor = open_directory_at(&self.directory, name)?;
        if created {
            rfs::fchmod(&descriptor, Mode::RWXU).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        }
        let stat = rfs::fstat(&descriptor).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
            || Mode::from_raw_mode(stat.st_mode) != Mode::RWXU
            || stat.st_uid != effective_user_id()
            || stat.st_dev as u64 != self.device
        {
            return Err(AttemptWorkspaceError::Integrity);
        }
        let current = stat_without_following(&self.directory, name)?;
        if !same_object(&stat, &current) {
            return Err(AttemptWorkspaceError::Integrity);
        }
        validate_bound_directory(&self.root, &self.directory, self.device)?;
        // A concurrent opener can observe the directory before its creator reaches fsync.
        rfs::fsync(&descriptor).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        rfs::fsync(&self.directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        Ok(RecoverableAttemptDirectory {
            path: self.root.join(name),
            directory: File::from(descriptor),
            root_device: self.device,
        })
    }
}

impl AttemptDirectory {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn working_directory(&self) -> Result<WorkingDirectory, AttemptWorkspaceError> {
        let working = WorkingDirectory::new_private(&self.path)
            .map_err(|_| AttemptWorkspaceError::Integrity)?;
        let opened = rfs::fstat(&self.directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        let working_directory = working.directory();
        let rebound =
            rfs::fstat(&working_directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        if !same_object(&opened, &rebound)
            || Mode::from_raw_mode(opened.st_mode) != Mode::RWXU
            || opened.st_dev as u64 != self.root_device
        {
            return Err(AttemptWorkspaceError::Integrity);
        }
        Ok(working)
    }
}

impl RecoverableAttemptDirectory {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn working_directory(&self) -> Result<WorkingDirectory, AttemptWorkspaceError> {
        let working = WorkingDirectory::new_private(&self.path)
            .map_err(|_| AttemptWorkspaceError::Integrity)?;
        let opened = rfs::fstat(&self.directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        let working_directory = working.directory();
        let rebound =
            rfs::fstat(&working_directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
        if !same_object(&opened, &rebound)
            || Mode::from_raw_mode(opened.st_mode) != Mode::RWXU
            || opened.st_dev as u64 != self.root_device
        {
            return Err(AttemptWorkspaceError::Integrity);
        }
        Ok(working)
    }
}

impl Drop for AttemptDirectory {
    fn drop(&mut self) {
        let mut budget = CleanupBudget::new();
        if clear_directory(&self.directory, self.root_device, false, 0, &mut budget).is_err() {
            return;
        }
        let Ok(current) = stat_without_following(&self.root, &self.name) else {
            return;
        };
        let Ok(opened) = rfs::fstat(&self.directory) else {
            return;
        };
        if same_object(&opened, &current) {
            let _ = rfs::unlinkat(&self.root, &self.name, AtFlags::REMOVEDIR);
        }
    }
}

#[derive(Debug, Error)]
pub enum AttemptWorkspaceError {
    #[error("attempt workspace configuration is invalid")]
    InvalidConfiguration,
    #[error("attempt workspace is unavailable")]
    Unavailable,
    #[error("attempt workspace integrity validation failed")]
    Integrity,
    #[error("attempt workspace is already owned by another process")]
    AlreadyLocked,
}

struct CleanupBudget {
    remaining_entries: usize,
}

impl CleanupBudget {
    fn new() -> Self {
        Self {
            remaining_entries: MAX_CLEANUP_ENTRIES,
        }
    }

    fn consume(&mut self) -> Result<(), AttemptWorkspaceError> {
        self.remaining_entries = self
            .remaining_entries
            .checked_sub(1)
            .ok_or(AttemptWorkspaceError::Integrity)?;
        Ok(())
    }
}

fn validate_prefix(prefix: &str) -> Result<(), AttemptWorkspaceError> {
    if prefix.len() < 3
        || prefix.len() > 64
        || !prefix.starts_with('.')
        || !prefix.ends_with('-')
        || !prefix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        || ATTEMPT_WORKSPACE_LOCK_FILENAME.starts_with(prefix)
    {
        return Err(AttemptWorkspaceError::InvalidConfiguration);
    }
    Ok(())
}

fn validate_attempt_key(value: &str) -> Result<(), AttemptWorkspaceError> {
    if value.is_empty()
        || value.len() > 192
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(AttemptWorkspaceError::InvalidConfiguration);
    }
    Ok(())
}

fn validate_bound_directory(
    path: &Path,
    directory: &File,
    expected_device: u64,
) -> Result<(), AttemptWorkspaceError> {
    let opened = rfs::fstat(directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
    if FileType::from_raw_mode(opened.st_mode) != FileType::Directory
        || Mode::from_raw_mode(opened.st_mode) != Mode::RWXU
        || opened.st_uid != effective_user_id()
        || opened.st_dev as u64 != expected_device
    {
        return Err(AttemptWorkspaceError::Integrity);
    }
    let current = fs::symlink_metadata(path).map_err(|_| AttemptWorkspaceError::Integrity)?;
    if current.file_type().is_symlink()
        || !current.is_dir()
        || current.mode() & 0o7777 != 0o700
        || current.uid() != effective_user_id()
        || current.dev() != opened.st_dev as u64
        || current.ino() != opened.st_ino as u64
    {
        return Err(AttemptWorkspaceError::Integrity);
    }
    Ok(())
}

fn acquire_lock(root: &File) -> Result<File, AttemptWorkspaceError> {
    let (descriptor, created) = match rfs::openat(
        root,
        ATTEMPT_WORKSPACE_LOCK_FILENAME,
        OFlags::RDWR
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::CLOEXEC
            | OFlags::NONBLOCK,
        Mode::RUSR | Mode::WUSR,
    ) {
        Ok(descriptor) => (descriptor, true),
        Err(Errno::EXIST) => (
            rfs::openat(
                root,
                ATTEMPT_WORKSPACE_LOCK_FILENAME,
                OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|_| AttemptWorkspaceError::Integrity)?,
            false,
        ),
        Err(_) => return Err(AttemptWorkspaceError::Unavailable),
    };
    if created
        && (rfs::fchmod(&descriptor, Mode::RUSR | Mode::WUSR).is_err() || rfs::fsync(root).is_err())
    {
        let _ = rfs::unlinkat(root, ATTEMPT_WORKSPACE_LOCK_FILENAME, AtFlags::empty());
        return Err(AttemptWorkspaceError::Unavailable);
    }
    let lock = File::from(descriptor);
    let stat = rfs::fstat(&lock).map_err(|_| AttemptWorkspaceError::Unavailable)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || Mode::from_raw_mode(stat.st_mode) != Mode::RUSR | Mode::WUSR
        || stat.st_uid != effective_user_id()
        || stat.st_nlink != 1
        || stat.st_size != 0
    {
        return Err(AttemptWorkspaceError::Integrity);
    }
    match rfs::flock(&lock, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(lock),
        Err(error) if error == Errno::WOULDBLOCK || error == Errno::AGAIN => {
            Err(AttemptWorkspaceError::AlreadyLocked)
        }
        Err(_) => Err(AttemptWorkspaceError::Unavailable),
    }
}

fn cleanup_attempts(
    root: &File,
    attempt_prefix: &[u8],
    root_device: u64,
) -> Result<u64, AttemptWorkspaceError> {
    let mut attempts = Vec::new();
    let mut budget = CleanupBudget::new();
    for name in directory_entries(root, MAX_CLEANUP_ENTRIES + 1)? {
        let bytes = name.as_os_str().as_bytes();
        if bytes == ATTEMPT_WORKSPACE_LOCK_FILENAME.as_bytes() {
            continue;
        }
        if !bytes.starts_with(attempt_prefix) {
            return Err(AttemptWorkspaceError::Integrity);
        }
        budget.consume()?;
        attempts.push(name);
    }
    for attempt in &attempts {
        remove_attempt(root, attempt, root_device, &mut budget)?;
    }
    if !attempts.is_empty() {
        rfs::fsync(root).map_err(|_| AttemptWorkspaceError::Unavailable)?;
    }
    u64::try_from(attempts.len()).map_err(|_| AttemptWorkspaceError::Unavailable)
}

fn remove_attempt(
    root: &File,
    name: &OsStr,
    root_device: u64,
    budget: &mut CleanupBudget,
) -> Result<(), AttemptWorkspaceError> {
    let before = stat_without_following(root, name)?;
    if FileType::from_raw_mode(before.st_mode) != FileType::Directory
        || Mode::from_raw_mode(before.st_mode) != Mode::RWXU
        || before.st_uid != effective_user_id()
        || before.st_dev as u64 != root_device
    {
        return Err(AttemptWorkspaceError::Integrity);
    }
    let directory = open_directory_at(root, name)?;
    let opened = rfs::fstat(&directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
    if !same_object(&before, &opened) {
        return Err(AttemptWorkspaceError::Integrity);
    }
    clear_directory(&directory, root_device, true, 0, budget)?;
    let current = stat_without_following(root, name)?;
    if !same_object(&opened, &current) {
        return Err(AttemptWorkspaceError::Integrity);
    }
    rfs::unlinkat(root, name, AtFlags::REMOVEDIR).map_err(|_| AttemptWorkspaceError::Unavailable)
}

fn remove_recoverable_attempt(
    root: &File,
    name: &OsStr,
    root_device: u64,
    budget: &mut CleanupBudget,
) -> Result<bool, AttemptWorkspaceError> {
    let Some(before) = stat_optional_without_following(root, name)? else {
        return Ok(false);
    };
    if FileType::from_raw_mode(before.st_mode) != FileType::Directory
        || Mode::from_raw_mode(before.st_mode) != Mode::RWXU
        || before.st_uid != effective_user_id()
        || before.st_dev as u64 != root_device
    {
        return Err(AttemptWorkspaceError::Integrity);
    }
    let Some(directory) = open_optional_directory_at(root, name)? else {
        return Ok(false);
    };
    let opened = rfs::fstat(&directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
    if !same_object(&before, &opened) {
        return Err(AttemptWorkspaceError::Integrity);
    }
    match rfs::flock(&directory, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {}
        Err(error) if error == Errno::WOULDBLOCK || error == Errno::AGAIN => return Ok(false),
        Err(_) => return Err(AttemptWorkspaceError::Unavailable),
    }
    let Some(current) = stat_optional_without_following(root, name)? else {
        return Ok(false);
    };
    if !same_object(&opened, &current) {
        return Err(AttemptWorkspaceError::Integrity);
    }
    clear_directory(&directory, root_device, true, 0, budget)?;
    let current = stat_without_following(root, name)?;
    if !same_object(&opened, &current) {
        return Err(AttemptWorkspaceError::Integrity);
    }
    rfs::unlinkat(root, name, AtFlags::REMOVEDIR)
        .map_err(|_| AttemptWorkspaceError::Unavailable)?;
    Ok(true)
}

fn clear_directory(
    directory: &impl std::os::fd::AsFd,
    root_device: u64,
    durable: bool,
    depth: usize,
    budget: &mut CleanupBudget,
) -> Result<(), AttemptWorkspaceError> {
    if depth > MAX_CLEANUP_DEPTH {
        return Err(AttemptWorkspaceError::Integrity);
    }
    for name in directory_entries(directory, budget.remaining_entries)? {
        budget.consume()?;
        let before = stat_without_following(directory, &name)?;
        if before.st_uid != effective_user_id() {
            return Err(AttemptWorkspaceError::Integrity);
        }
        if FileType::from_raw_mode(before.st_mode) == FileType::Directory {
            if before.st_dev as u64 != root_device {
                return Err(AttemptWorkspaceError::Integrity);
            }
            let child = open_directory_at(directory, &name)?;
            let opened = rfs::fstat(&child).map_err(|_| AttemptWorkspaceError::Unavailable)?;
            if !same_object(&before, &opened) {
                return Err(AttemptWorkspaceError::Integrity);
            }
            clear_directory(&child, root_device, durable, depth + 1, budget)?;
            let current = stat_without_following(directory, &name)?;
            if !same_object(&opened, &current) {
                return Err(AttemptWorkspaceError::Integrity);
            }
            rfs::unlinkat(directory, &name, AtFlags::REMOVEDIR)
                .map_err(|_| AttemptWorkspaceError::Unavailable)?;
        } else {
            rfs::unlinkat(directory, &name, AtFlags::empty())
                .map_err(|_| AttemptWorkspaceError::Unavailable)?;
        }
    }
    if durable {
        rfs::fsync(directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
    }
    Ok(())
}

fn open_directory_at(
    parent: &impl std::os::fd::AsFd,
    name: &OsStr,
) -> Result<std::os::fd::OwnedFd, AttemptWorkspaceError> {
    rfs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|_| AttemptWorkspaceError::Integrity)
}

fn open_optional_directory_at(
    parent: &impl std::os::fd::AsFd,
    name: &OsStr,
) -> Result<Option<std::os::fd::OwnedFd>, AttemptWorkspaceError> {
    match rfs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(directory) => Ok(Some(directory)),
        Err(Errno::NOENT) => Ok(None),
        Err(_) => Err(AttemptWorkspaceError::Integrity),
    }
}

fn stat_without_following(
    parent: &impl std::os::fd::AsFd,
    name: &OsStr,
) -> Result<rfs::Stat, AttemptWorkspaceError> {
    rfs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| AttemptWorkspaceError::Unavailable)
}

fn stat_optional_without_following(
    parent: &impl std::os::fd::AsFd,
    name: &OsStr,
) -> Result<Option<rfs::Stat>, AttemptWorkspaceError> {
    match rfs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(stat)),
        Err(Errno::NOENT) => Ok(None),
        Err(_) => Err(AttemptWorkspaceError::Unavailable),
    }
}

fn directory_entries(
    directory: &impl std::os::fd::AsFd,
    max_entries: usize,
) -> Result<Vec<OsString>, AttemptWorkspaceError> {
    let mut entries = Vec::new();
    let mut stream = Dir::read_from(directory).map_err(|_| AttemptWorkspaceError::Unavailable)?;
    while let Some(entry) = stream.read() {
        let entry = entry.map_err(|_| AttemptWorkspaceError::Unavailable)?;
        let name = entry.file_name().to_bytes();
        if matches!(name, b"." | b"..") {
            continue;
        }
        if entries.len() == max_entries {
            return Err(AttemptWorkspaceError::Integrity);
        }
        entries.push(OsString::from_vec(name.to_vec()));
    }
    Ok(entries)
}

fn same_object(left: &rfs::Stat, right: &rfs::Stat) -> bool {
    FileType::from_raw_mode(left.st_mode) == FileType::Directory
        && FileType::from_raw_mode(right.st_mode) == FileType::Directory
        && left.st_dev == right.st_dev
        && left.st_ino == right.st_ino
        && right.st_uid == effective_user_id()
}

fn effective_user_id() -> u32 {
    unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
    };

    use tempfile::TempDir;

    use super::*;

    const PREFIX: &str = ".provider-poll-";

    #[test]
    fn cleans_nested_crash_left_attempt_without_following_symlinks() {
        let root = private_root();
        let outside_root = TempDir::new().unwrap();
        let outside = outside_root.path().join("outside");
        fs::write(&outside, b"authority").unwrap();
        let attempt = root.path().join(format!("{PREFIX}old"));
        let nested = attempt.join("nested");
        fs::create_dir(&attempt).unwrap();
        fs::set_permissions(&attempt, fs::Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(&nested).unwrap();
        fs::write(nested.join("artifact.bin"), b"bytes").unwrap();
        symlink(&outside, attempt.join("outside-link")).unwrap();

        let workspace = acquire(root.path()).unwrap();

        assert_eq!(workspace.cleaned_attempts(), 1);
        assert!(!attempt.exists());
        assert_eq!(fs::read(&outside).unwrap(), b"authority");
        assert_eq!(workspace.root(), fs::canonicalize(root.path()).unwrap());
    }

    #[test]
    fn exclusive_lock_fences_another_workspace_owner() {
        let root = private_root();
        let owner = acquire(root.path()).unwrap();

        assert!(matches!(
            acquire(root.path()),
            Err(AttemptWorkspaceError::AlreadyLocked)
        ));

        drop(owner);
        assert!(acquire(root.path()).is_ok());
    }

    #[test]
    fn concurrent_attempt_creation_produces_unique_fd_bound_directories() {
        let root = private_root();
        let workspace = acquire(root.path()).unwrap();
        let names = std::thread::scope(|scope| {
            let handles = (0..32)
                .map(|_| {
                    scope.spawn(|| {
                        let attempt = workspace.create_attempt().unwrap();
                        let name = attempt.path().file_name().unwrap().to_owned();
                        assert!(attempt.working_directory().is_ok());
                        name
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<std::collections::BTreeSet<_>>()
        });

        assert_eq!(names.len(), 32);
        assert_eq!(
            fs::read_dir(root.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| { entry.file_name().as_bytes().starts_with(PREFIX.as_bytes()) })
                .count(),
            0
        );
    }

    #[test]
    fn recoverable_attempt_reopens_one_deterministic_private_directory() {
        let root = private_root();
        let working = WorkingDirectory::new_private(root.path()).unwrap();
        let workspace = RecoverableAttemptWorkspace::new(&working, ".provider-submit-").unwrap();

        let first = workspace.open_or_create("submission-launch").unwrap();
        let first_path = first.path().to_owned();
        let first_directory = first.working_directory().unwrap();
        drop(first);
        let reopened = workspace.open_or_create("submission-launch").unwrap();
        let reopened_directory = reopened.working_directory().unwrap();

        assert_eq!(first_path, reopened.path());
        assert_eq!(first_directory.path(), reopened_directory.path());
        assert!(first_path.is_dir());
    }

    #[test]
    fn recoverable_attempt_cleanup_is_bounded_and_idempotent() {
        let root = private_root();
        let outside_root = TempDir::new().unwrap();
        let outside = outside_root.path().join("outside");
        fs::write(&outside, b"authority").unwrap();
        let working = WorkingDirectory::new_private(root.path()).unwrap();
        let workspace = RecoverableAttemptWorkspace::new(&working, ".provider-submit-").unwrap();
        let attempt = workspace.open_or_create("cleanup").unwrap();
        let nested = attempt.path().join("nested");
        fs::create_dir(&nested).unwrap();
        fs::write(nested.join("provider.tmp"), b"bytes").unwrap();
        symlink(&outside, attempt.path().join("outside-link")).unwrap();
        drop(attempt);

        assert!(workspace.remove("cleanup").unwrap());
        assert!(!workspace.remove("cleanup").unwrap());
        assert_eq!(fs::read(outside).unwrap(), b"authority");
    }

    #[test]
    fn concurrent_recoverable_cleanup_serializes_per_attempt() {
        let root = private_root();
        let working = WorkingDirectory::new_private(root.path()).unwrap();
        let workspace = std::sync::Arc::new(
            RecoverableAttemptWorkspace::new(&working, ".provider-submit-").unwrap(),
        );
        let attempt = workspace.open_or_create("concurrent-cleanup").unwrap();
        fs::write(attempt.path().join("provider.tmp"), b"bytes").unwrap();
        drop(attempt);

        let removed = std::thread::scope(|scope| {
            let handles = (0..32)
                .map(|_| {
                    let workspace = std::sync::Arc::clone(&workspace);
                    scope.spawn(move || workspace.remove("concurrent-cleanup").unwrap())
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        assert_eq!(removed.into_iter().filter(|removed| *removed).count(), 1);
        assert!(
            !root
                .path()
                .join(".provider-submit-concurrent-cleanup")
                .exists()
        );
    }

    #[test]
    fn recoverable_attempt_stays_bound_to_the_original_root() {
        let outer = TempDir::new().unwrap();
        let root = outer.path().join("workspace");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let working = WorkingDirectory::new_private(&root).unwrap();
        let workspace = RecoverableAttemptWorkspace::new(&working, ".provider-submit-").unwrap();
        let attempt = workspace.open_or_create("original").unwrap();
        fs::write(attempt.path().join("provider.tmp"), b"original").unwrap();

        let moved = outer.path().join("moved-workspace");
        fs::rename(&root, &moved).unwrap();
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let replacement = root.join(".provider-submit-original");
        fs::create_dir(&replacement).unwrap();
        fs::write(replacement.join("sentinel"), b"replacement").unwrap();

        assert!(matches!(
            workspace.remove("original"),
            Err(AttemptWorkspaceError::Integrity)
        ));
        assert_eq!(
            fs::read(replacement.join("sentinel")).unwrap(),
            b"replacement"
        );
        assert!(moved.join(".provider-submit-original").is_dir());
    }

    #[test]
    fn attempt_drop_remains_bound_to_the_original_root_after_path_replacement() {
        let outer = TempDir::new().unwrap();
        let root = outer.path().join("workspace");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let workspace = acquire(&root).unwrap();
        let attempt = workspace.create_attempt().unwrap();
        assert!(attempt.working_directory().is_ok());
        let attempt_name = attempt.path().file_name().unwrap().to_owned();
        fs::write(attempt.path().join("artifact.bin"), b"original").unwrap();

        let moved = outer.path().join("moved-workspace");
        fs::rename(&root, &moved).unwrap();
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let replacement_attempt = root.join(&attempt_name);
        fs::create_dir(&replacement_attempt).unwrap();
        fs::write(replacement_attempt.join("sentinel"), b"replacement").unwrap();

        drop(attempt);

        assert!(!moved.join(&attempt_name).exists());
        assert_eq!(
            fs::read(replacement_attempt.join("sentinel")).unwrap(),
            b"replacement"
        );
        assert!(matches!(
            workspace.create_attempt(),
            Err(AttemptWorkspaceError::Integrity)
        ));
    }

    #[test]
    fn rejects_malformed_lock_files() {
        let wrong_mode = private_root();
        let path = wrong_mode.path().join(ATTEMPT_WORKSPACE_LOCK_FILENAME);
        fs::write(&path, b"").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            acquire(wrong_mode.path()),
            Err(AttemptWorkspaceError::Integrity)
        ));

        let nonempty = private_root();
        let path = nonempty.path().join(ATTEMPT_WORKSPACE_LOCK_FILENAME);
        fs::write(&path, b"owner").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(matches!(
            acquire(nonempty.path()),
            Err(AttemptWorkspaceError::Integrity)
        ));

        let hardlinked = private_root();
        let path = hardlinked.path().join(ATTEMPT_WORKSPACE_LOCK_FILENAME);
        fs::write(&path, b"").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::hard_link(&path, hardlinked.path().join("lock-alias")).unwrap();
        assert!(matches!(
            acquire(hardlinked.path()),
            Err(AttemptWorkspaceError::Integrity)
        ));
    }

    #[test]
    fn rejects_unknown_entries_symlinked_attempts_and_unsafe_roots() {
        let unknown = private_root();
        fs::write(unknown.path().join("operator-file"), b"keep").unwrap();
        assert!(matches!(
            acquire(unknown.path()),
            Err(AttemptWorkspaceError::Integrity)
        ));
        assert!(unknown.path().join("operator-file").exists());

        let linked = private_root();
        let outside_root = TempDir::new().unwrap();
        let outside = outside_root.path().join("outside");
        fs::create_dir(&outside).unwrap();
        symlink(&outside, linked.path().join(format!("{PREFIX}linked"))).unwrap();
        assert!(matches!(
            acquire(linked.path()),
            Err(AttemptWorkspaceError::Integrity)
        ));
        assert!(outside.exists());

        let unsafe_root = private_root();
        fs::set_permissions(unsafe_root.path(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            acquire(unsafe_root.path()),
            Err(AttemptWorkspaceError::Integrity)
        ));
    }

    #[test]
    fn cleanup_budget_rejects_excessive_entries_and_depth() {
        let excessive_entries = private_root();
        let attempt = excessive_entries.path().join(format!("{PREFIX}wide"));
        fs::create_dir(&attempt).unwrap();
        fs::set_permissions(&attempt, fs::Permissions::from_mode(0o700)).unwrap();
        for index in 0..=MAX_CLEANUP_ENTRIES {
            fs::write(attempt.join(index.to_string()), b"x").unwrap();
        }
        assert!(matches!(
            acquire(excessive_entries.path()),
            Err(AttemptWorkspaceError::Integrity)
        ));
        assert!(attempt.exists());

        let excessive_depth = private_root();
        let attempt = excessive_depth.path().join(format!("{PREFIX}deep"));
        fs::create_dir(&attempt).unwrap();
        fs::set_permissions(&attempt, fs::Permissions::from_mode(0o700)).unwrap();
        let mut nested = attempt.clone();
        for index in 0..=MAX_CLEANUP_DEPTH {
            nested = nested.join(index.to_string());
            fs::create_dir(&nested).unwrap();
        }
        assert!(matches!(
            acquire(excessive_depth.path()),
            Err(AttemptWorkspaceError::Integrity)
        ));
        assert!(attempt.exists());
    }

    fn private_root() -> TempDir {
        let root = TempDir::new().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        root
    }

    fn acquire(path: &Path) -> Result<ExclusiveAttemptWorkspace, AttemptWorkspaceError> {
        let working = WorkingDirectory::new(path).unwrap();
        ExclusiveAttemptWorkspace::acquire(&working, PREFIX)
    }
}
