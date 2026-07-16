use std::{
    fs, io,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::fs::{MetadataExt, PermissionsExt},
    },
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use rustix::{
    fs::{self as rfs, FileType, Mode, OFlags},
    io::Errno,
};
#[cfg(target_os = "linux")]
use uuid::Uuid;

use super::{GatedCliProcessError, HelperLock, LOCK_FILE};

pub(super) fn ensure_lock_file(directory: &OwnedFd) -> Result<(), GatedCliProcessError> {
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
                .map_err(|_| GatedCliProcessError::Unavailable)?;
            rfs::fsync(&fd).map_err(|_| GatedCliProcessError::Unavailable)?;
            rfs::fsync(directory).map_err(|_| GatedCliProcessError::Unavailable)
        }
        Err(Errno::EXIST) => {
            let fd = rfs::openat(
                directory,
                LOCK_FILE,
                OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|_| GatedCliProcessError::Integrity)?;
            validate_lock_stat(&rfs::fstat(&fd).map_err(|_| GatedCliProcessError::Unavailable)?)
        }
        Err(_) => Err(GatedCliProcessError::Unavailable),
    }
}

pub(super) fn validate_lock_stat(stat: &rfs::Stat) -> Result<(), GatedCliProcessError> {
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || Mode::from_raw_mode(stat.st_mode) != Mode::RUSR | Mode::WUSR
        || stat.st_uid != unsafe { libc::geteuid() }
        || stat.st_nlink != 1
        || stat.st_size != 0
    {
        return Err(GatedCliProcessError::Integrity);
    }
    Ok(())
}

pub(super) fn validate_bound_directory(
    path: &Path,
    fd: &OwnedFd,
) -> Result<(), GatedCliProcessError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| GatedCliProcessError::Integrity)?;
    let stat = rfs::fstat(fd).map_err(|_| GatedCliProcessError::Unavailable)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.permissions().mode() & 0o777 != 0o700
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.dev() != stat.st_dev as u64
        || metadata.ino() != stat.st_ino
    {
        return Err(GatedCliProcessError::Integrity);
    }
    Ok(())
}

pub(super) fn try_exclusive_lock(file: &fs::File) -> Result<bool, GatedCliProcessError> {
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error
        .raw_os_error()
        .is_some_and(|code| code == libc::EWOULDBLOCK || code == libc::EAGAIN)
    {
        Ok(false)
    } else {
        Err(GatedCliProcessError::Unavailable)
    }
}

pub(super) fn unlock(file: &fs::File) -> Result<(), GatedCliProcessError> {
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) } == 0 {
        Ok(())
    } else {
        Err(GatedCliProcessError::Unavailable)
    }
}

impl Drop for HelperLock {
    fn drop(&mut self) {
        let _ = unlock(&self.file);
    }
}

pub(super) fn create_pipe() -> Result<(OwnedFd, OwnedFd), GatedCliProcessError> {
    let mut fds = [-1; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(GatedCliProcessError::Unavailable);
    }
    let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    set_cloexec(read.as_raw_fd(), true)?;
    set_cloexec(write.as_raw_fd(), true)?;
    Ok((read, write))
}

pub(super) fn set_cloexec(fd: RawFd, enabled: bool) -> Result<(), GatedCliProcessError> {
    let current = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if current < 0 {
        return Err(GatedCliProcessError::Unavailable);
    }
    let flags = if enabled {
        current | libc::FD_CLOEXEC
    } else {
        current & !libc::FD_CLOEXEC
    };
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags) } == 0 {
        Ok(())
    } else {
        Err(GatedCliProcessError::Unavailable)
    }
}

pub(super) fn process_group_id(pid: u32) -> Result<u32, GatedCliProcessError> {
    let pid = libc::pid_t::try_from(pid).map_err(|_| GatedCliProcessError::Integrity)?;
    let pgid = unsafe { libc::getpgid(pid) };
    if pgid <= 1 {
        return Err(GatedCliProcessError::Unavailable);
    }
    u32::try_from(pgid).map_err(|_| GatedCliProcessError::Integrity)
}

pub(super) fn process_group_exists(process_group_id: u32) -> Result<bool, GatedCliProcessError> {
    let result = unsafe { libc::kill(-(process_group_id as libc::pid_t), 0) };
    if result == 0 {
        return Ok(true);
    }
    match io::Error::last_os_error().raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(GatedCliProcessError::Unavailable),
    }
}

pub(super) fn signal_process_group(
    process_group_id: u32,
    signal: libc::c_int,
) -> Result<(), GatedCliProcessError> {
    if unsafe { libc::kill(-(process_group_id as libc::pid_t), signal) } == 0 {
        return Ok(());
    }
    if io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(GatedCliProcessError::Unavailable)
    }
}

pub(super) fn current_process_token_matches(
    pid: u32,
    expected: &str,
) -> Result<bool, GatedCliProcessError> {
    match process_start_token(pid) {
        Ok(first) if first == expected => match process_start_token(pid) {
            Ok(second) => Ok(second == expected),
            Err(GatedCliProcessError::Unavailable) => Ok(false),
            Err(error) => Err(error),
        },
        Ok(_) | Err(GatedCliProcessError::Unavailable) => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "linux")]
pub(super) fn process_start_token(pid: u32) -> Result<String, GatedCliProcessError> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|_| GatedCliProcessError::Unavailable)?;
    let fields = stat
        .rsplit_once(')')
        .map(|(_, fields)| fields.split_whitespace().collect::<Vec<_>>())
        .ok_or(GatedCliProcessError::Integrity)?;
    let start_ticks = fields.get(19).ok_or(GatedCliProcessError::Integrity)?;
    start_ticks
        .parse::<u64>()
        .map_err(|_| GatedCliProcessError::Integrity)?;
    Ok(format!("linux:{start_ticks}"))
}

#[cfg(target_os = "macos")]
pub(super) fn process_start_token(pid: u32) -> Result<String, GatedCliProcessError> {
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
        return Err(GatedCliProcessError::Unavailable);
    }
    Ok(format!(
        "macos:{}:{}",
        info.pbi_start_tvsec, info.pbi_start_tvusec
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(super) fn process_start_token(_pid: u32) -> Result<String, GatedCliProcessError> {
    Err(GatedCliProcessError::Unavailable)
}

#[cfg(target_os = "linux")]
pub(super) fn boot_token() -> Result<String, GatedCliProcessError> {
    let token = fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map_err(|_| GatedCliProcessError::Unavailable)?;
    let token = token.trim();
    Uuid::parse_str(token)
        .ok()
        .filter(|value| !value.is_nil())
        .map(|value| format!("linux:{value}"))
        .ok_or(GatedCliProcessError::Integrity)
}

#[cfg(target_os = "macos")]
pub(super) fn boot_token() -> Result<String, GatedCliProcessError> {
    let name = b"kern.boottime\0";
    let mut value = unsafe { std::mem::zeroed::<libc::timeval>() };
    let mut size = std::mem::size_of::<libc::timeval>();
    let result = unsafe {
        libc::sysctlbyname(
            name.as_ptr().cast(),
            (&mut value as *mut libc::timeval).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 || size != std::mem::size_of::<libc::timeval>() || value.tv_sec <= 0 {
        return Err(GatedCliProcessError::Unavailable);
    }
    Ok(format!("macos:{}:{}", value.tv_sec, value.tv_usec))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(super) fn boot_token() -> Result<String, GatedCliProcessError> {
    Err(GatedCliProcessError::Unavailable)
}

pub(super) fn unix_time_ms() -> Result<u64, GatedCliProcessError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GatedCliProcessError::Unavailable)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| GatedCliProcessError::Unavailable)
}
