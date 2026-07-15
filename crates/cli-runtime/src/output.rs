use std::{
    ffi::CString,
    fs::{self, File},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::command::{CommandSpecError, validate_output_filename};

pub const STREAM_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputContract {
    relative_filename: PathBuf,
    max_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedOutput {
    pub relative_filename: PathBuf,
    pub byte_size: u64,
    pub sha256_hex: String,
}

#[derive(Debug, Error)]
pub enum OutputError {
    #[error("output could not be opened safely: {0}")]
    Unavailable(#[source] std::io::Error),
    #[error("output must be a regular file")]
    NotRegular,
    #[error("output must not be empty")]
    Empty,
    #[error("output exceeds the configured size limit")]
    TooLarge,
    #[error("output changed while it was being sealed")]
    ChangedDuringRead,
    #[error("output sink failed: {0}")]
    Sink(#[source] std::io::Error),
}

impl OutputContract {
    pub fn new(
        relative_filename: impl Into<PathBuf>,
        max_bytes: u64,
    ) -> Result<Self, CommandSpecError> {
        let relative_filename = relative_filename.into();
        validate_output_filename(&relative_filename)?;
        if max_bytes == 0 {
            return Err(CommandSpecError::InvalidOutputLimit);
        }
        Ok(Self {
            relative_filename,
            max_bytes,
        })
    }

    pub fn relative_filename(&self) -> &Path {
        &self.relative_filename
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }
}

pub(crate) fn seal_to_sink<W: Write>(
    directory: Arc<File>,
    contract: OutputContract,
    mut sink: W,
) -> Result<(SealedOutput, W), OutputError> {
    let mut file = open_output_at(&directory, contract.relative_filename())?;
    let before = file.metadata().map_err(OutputError::Unavailable)?;
    validate_regular_output(&before, contract.max_bytes())?;
    let identity = OutputIdentity::from_metadata(&before);

    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; STREAM_BUFFER_BYTES];
    let mut byte_size = 0_u64;
    loop {
        let count = file.read(&mut buffer).map_err(OutputError::Unavailable)?;
        if count == 0 {
            break;
        }
        byte_size = byte_size
            .checked_add(count as u64)
            .ok_or(OutputError::TooLarge)?;
        if byte_size > contract.max_bytes() {
            return Err(OutputError::TooLarge);
        }
        hasher.update(&buffer[..count]);
        sink.write_all(&buffer[..count])
            .map_err(OutputError::Sink)?;
    }
    sink.flush().map_err(OutputError::Sink)?;

    let after = file.metadata().map_err(OutputError::Unavailable)?;
    if byte_size != before.len() || OutputIdentity::from_metadata(&after) != identity {
        return Err(OutputError::ChangedDuringRead);
    }

    let digest = hasher.finalize();
    Ok((
        SealedOutput {
            relative_filename: contract.relative_filename,
            byte_size,
            sha256_hex: hex_digest(&digest),
        },
        sink,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OutputIdentity {
    device: u64,
    inode: u64,
    mode: u32,
    links: u64,
    size: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

impl OutputIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            mode: metadata.mode(),
            links: metadata.nlink(),
            size: metadata.size(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    }
}

fn open_output_at(directory: &File, relative_filename: &Path) -> Result<File, OutputError> {
    let filename = CString::new(relative_filename.as_os_str().as_bytes())
        .map_err(|_| OutputError::Unavailable(std::io::Error::from_raw_os_error(libc::EINVAL)))?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            filename.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        return Err(OutputError::Unavailable(std::io::Error::last_os_error()));
    }
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn validate_regular_output(metadata: &fs::Metadata, max_bytes: u64) -> Result<(), OutputError> {
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(OutputError::NotRegular);
    }
    if metadata.len() == 0 {
        return Err(OutputError::Empty);
    }
    if metadata.len() > max_bytes {
        return Err(OutputError::TooLarge);
    }
    Ok(())
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}
