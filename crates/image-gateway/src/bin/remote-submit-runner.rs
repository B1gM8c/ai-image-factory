use std::{
    env,
    os::fd::{FromRawFd, OwnedFd, RawFd},
    path::PathBuf,
};

use gpt_image_2_gateway::{GatedCliProcessError, run_remote_submit_gate, run_remote_submit_runner};
use uuid::Uuid;

fn main() -> Result<(), GatedCliProcessError> {
    let mut args = env::args_os().skip(1);
    let first = args.next().ok_or(GatedCliProcessError::InvalidInput)?;
    if first == "--gate" {
        let root = args
            .next()
            .map(PathBuf::from)
            .ok_or(GatedCliProcessError::InvalidInput)?;
        let submission_id = parse_uuid(args.next())?;
        let release_fd = parse_fd(args.next())?;
        let exec_status_fd = parse_fd(args.next())?;
        let helper_nonce = parse_uuid(args.next())?;
        if args.next().is_some() || release_fd == exec_status_fd {
            return Err(GatedCliProcessError::InvalidInput);
        }
        return run_remote_submit_gate(
            root,
            submission_id,
            take_owned_fd(release_fd)?,
            take_owned_fd(exec_status_fd)?,
            helper_nonce,
        );
    }

    let root = PathBuf::from(first);
    let submission_id = parse_uuid(args.next())?;
    if args.next().is_some() {
        return Err(GatedCliProcessError::InvalidInput);
    }
    run_remote_submit_runner(root, submission_id)
}

fn parse_uuid(value: Option<std::ffi::OsString>) -> Result<Uuid, GatedCliProcessError> {
    value
        .and_then(|value| value.into_string().ok())
        .and_then(|value| Uuid::parse_str(&value).ok())
        .filter(|value| !value.is_nil())
        .ok_or(GatedCliProcessError::InvalidInput)
}

fn parse_fd(value: Option<std::ffi::OsString>) -> Result<RawFd, GatedCliProcessError> {
    value
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.parse::<RawFd>().ok())
        .filter(|value| *value > libc::STDERR_FILENO)
        .ok_or(GatedCliProcessError::InvalidInput)
}

fn take_owned_fd(value: RawFd) -> Result<OwnedFd, GatedCliProcessError> {
    if unsafe { libc::fcntl(value, libc::F_GETFD) } < 0 {
        return Err(GatedCliProcessError::InvalidInput);
    }
    // Gate descriptors are inherited specifically for this process to consume.
    Ok(unsafe { OwnedFd::from_raw_fd(value) })
}
