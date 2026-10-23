use std::{
    error::Error,
    ffi::{CString, OsString},
    fmt,
    fs::{self, File},
    future::Future,
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{
            ffi::{OsStrExt, OsStringExt},
            fs::MetadataExt,
        },
    },
    path::{Path, PathBuf},
    sync::Arc,
};

use rustix::fs::Dir;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::AsyncReadExt;

use crate::{
    WorkingDirectory,
    command::{CommandSpecError, validate_output_filename},
};

pub const STREAM_BUFFER_BYTES: usize = 64 * 1024;
pub const MAX_BATCH_OUTPUT_FILES: usize = 10;

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

/// A one-shot capability for validating provider-selected artifacts in a
/// private, initially empty working directory.
#[derive(Debug)]
pub struct FreshOutputDirectory {
    directory: Arc<File>,
    max_bytes: u64,
}

/// One artifact in a validated, filename-ordered snapshot of a fresh directory.
///
/// The directory and every artifact must remain unchanged until all outputs have
/// been sealed. Sinks remain provisional until their sealing operation succeeds.
#[derive(Debug)]
pub struct VerifiedFreshOutputFile {
    file: File,
    relative_filename: PathBuf,
    identity: OutputIdentity,
    snapshot: Arc<VerifiedOutputDirectory>,
}

#[derive(Debug)]
struct VerifiedOutputDirectory {
    directory: Arc<File>,
    entries: Vec<(PathBuf, OutputIdentity)>,
    max_bytes: u64,
}

pub trait AsyncOutputSink: Send {
    type Error: Error + Send + Sync + 'static;

    fn write_chunk(&mut self, chunk: &[u8])
    -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug)]
pub enum AsyncOutputSealError<E> {
    Output(OutputError),
    Sink(E),
}

impl<E> From<OutputError> for AsyncOutputSealError<E> {
    fn from(error: OutputError) -> Self {
        Self::Output(error)
    }
}

impl<E> fmt::Display for AsyncOutputSealError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Output(error) => error.fmt(formatter),
            Self::Sink(_) => formatter.write_str("output sink failed"),
        }
    }
}

impl<E> Error for AsyncOutputSealError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Output(error) => Some(error),
            Self::Sink(error) => Some(error),
        }
    }
}

#[derive(Debug, Error)]
pub enum OutputError {
    #[error("output could not be opened safely: {0}")]
    Unavailable(#[source] std::io::Error),
    #[error("output directory must be private and owned by the current user")]
    UnsafeDirectory,
    #[error("output size limit must be non-zero")]
    InvalidLimit,
    #[error("output directory must be empty before execution")]
    DirectoryNotEmpty,
    #[error("output directory does not contain an artifact")]
    Missing,
    #[error("output directory contains more than one entry")]
    MultipleEntries,
    #[error("output file count limit must be between 1 and 10")]
    InvalidFileLimit,
    #[error("output directory exceeds the configured file count limit")]
    TooManyEntries,
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

impl FreshOutputDirectory {
    pub fn new(directory: &WorkingDirectory, max_bytes: u64) -> Result<Self, OutputError> {
        if max_bytes == 0 {
            return Err(OutputError::InvalidLimit);
        }
        let directory = directory.directory();
        validate_private_directory(&directory)?;
        if first_two_entries(&directory)?.next().is_some() {
            return Err(OutputError::DirectoryNotEmpty);
        }
        Ok(Self {
            directory,
            max_bytes,
        })
    }

    pub fn ensure_empty(&self) -> Result<(), OutputError> {
        validate_private_directory(&self.directory)?;
        if first_two_entries(&self.directory)?.next().is_some() {
            return Err(OutputError::DirectoryNotEmpty);
        }
        Ok(())
    }

    /// Opens and validates every output before returning any artifact handle.
    ///
    /// Artifacts are ordered lexicographically by the raw filename bytes, not
    /// filesystem enumeration order. The returned count may be less than the
    /// requested maximum; callers decide how to handle a partial provider result.
    pub fn verify_files(
        self,
        max_files: usize,
    ) -> Result<Vec<VerifiedFreshOutputFile>, OutputError> {
        if !(1..=MAX_BATCH_OUTPUT_FILES).contains(&max_files) {
            return Err(OutputError::InvalidFileLimit);
        }
        validate_private_directory(&self.directory)?;
        let filenames = sorted_entries(&self.directory, max_files)?;
        if filenames.is_empty() {
            return Err(OutputError::Missing);
        }
        let mut files = Vec::with_capacity(filenames.len());
        let mut entries = Vec::with_capacity(filenames.len());
        for relative_filename in filenames {
            let file = open_output_at(&self.directory, &relative_filename)?;
            let metadata = file.metadata().map_err(OutputError::Unavailable)?;
            validate_regular_output(&metadata, self.max_bytes)?;
            let identity = OutputIdentity::from_metadata(&metadata);
            entries.push((relative_filename.clone(), identity));
            files.push((file, relative_filename, identity));
        }
        let snapshot = Arc::new(VerifiedOutputDirectory {
            directory: self.directory,
            entries,
            max_bytes: self.max_bytes,
        });
        snapshot.validate()?;
        Ok(files
            .into_iter()
            .map(
                |(file, relative_filename, identity)| VerifiedFreshOutputFile {
                    file,
                    relative_filename,
                    identity,
                    snapshot: Arc::clone(&snapshot),
                },
            )
            .collect())
    }

    /// Streams the sole output into a provisional sink and returns the sink
    /// only after the file and directory identity checks succeed.
    ///
    /// The sink must not publish its bytes as authoritative before this method
    /// returns `Ok`; failures after streaming intentionally drop the sink.
    pub fn seal_single_file_to_sink<W: Write>(
        self,
        sink: W,
    ) -> Result<(SealedOutput, W), OutputError> {
        validate_private_directory(&self.directory)?;
        let mut entries = first_two_entries(&self.directory)?;
        let filename = entries.next().ok_or(OutputError::Missing)?;
        if entries.next().is_some() {
            return Err(OutputError::MultipleEntries);
        }
        let relative_filename = PathBuf::from(&filename);
        let file = open_output_at(&self.directory, &relative_filename)?;
        let (sealed, sink, identity) =
            seal_open_file(file, relative_filename.clone(), self.max_bytes, sink)?;
        validate_private_directory(&self.directory)?;
        let mut final_entries = first_two_entries(&self.directory)?;
        if final_entries.next().as_ref() != Some(&filename) || final_entries.next().is_some() {
            return Err(OutputError::ChangedDuringRead);
        }
        validate_current_output(&self.directory, &relative_filename, identity)?;
        Ok((sealed, sink))
    }

    pub async fn seal_single_file_to_async_sink<S>(
        self,
        mut sink: S,
    ) -> Result<(SealedOutput, S), AsyncOutputSealError<S::Error>>
    where
        S: AsyncOutputSink,
    {
        validate_private_directory(&self.directory)?;
        let mut entries = first_two_entries(&self.directory)?;
        let filename = entries.next().ok_or(OutputError::Missing)?;
        if entries.next().is_some() {
            return Err(OutputError::MultipleEntries.into());
        }
        let relative_filename = PathBuf::from(&filename);
        let file = open_output_at(&self.directory, &relative_filename)?;
        let before = file.metadata().map_err(OutputError::Unavailable)?;
        validate_regular_output(&before, self.max_bytes)?;
        let identity = OutputIdentity::from_metadata(&before);
        let mut file = tokio::fs::File::from_std(file);
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; STREAM_BUFFER_BYTES];
        let mut byte_size = 0_u64;

        loop {
            let count = file
                .read(&mut buffer)
                .await
                .map_err(OutputError::Unavailable)?;
            if count == 0 {
                break;
            }
            byte_size = byte_size
                .checked_add(count as u64)
                .ok_or(OutputError::TooLarge)?;
            if byte_size > self.max_bytes {
                return Err(OutputError::TooLarge.into());
            }
            hasher.update(&buffer[..count]);
            sink.write_chunk(&buffer[..count])
                .await
                .map_err(AsyncOutputSealError::Sink)?;
        }

        let after = file.metadata().await.map_err(OutputError::Unavailable)?;
        if byte_size != before.len() || OutputIdentity::from_metadata(&after) != identity {
            return Err(OutputError::ChangedDuringRead.into());
        }
        validate_private_directory(&self.directory)?;
        let mut final_entries = first_two_entries(&self.directory)?;
        if final_entries.next().as_ref() != Some(&filename) || final_entries.next().is_some() {
            return Err(OutputError::ChangedDuringRead.into());
        }
        validate_current_output(&self.directory, &relative_filename, identity)?;

        Ok((
            SealedOutput {
                relative_filename,
                byte_size,
                sha256_hex: hex_digest(&hasher.finalize()),
            },
            sink,
        ))
    }
}

impl VerifiedFreshOutputFile {
    pub fn relative_filename(&self) -> &Path {
        &self.relative_filename
    }

    pub async fn seal_to_async_sink<S>(
        self,
        mut sink: S,
    ) -> Result<(SealedOutput, S), AsyncOutputSealError<S::Error>>
    where
        S: AsyncOutputSink,
    {
        self.snapshot.validate()?;
        let mut file = tokio::fs::File::from_std(self.file);
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; STREAM_BUFFER_BYTES];
        let mut byte_size = 0_u64;
        loop {
            let count = file
                .read(&mut buffer)
                .await
                .map_err(OutputError::Unavailable)?;
            if count == 0 {
                break;
            }
            byte_size = byte_size
                .checked_add(count as u64)
                .ok_or(OutputError::TooLarge)?;
            if byte_size > self.snapshot.max_bytes {
                return Err(OutputError::TooLarge.into());
            }
            hasher.update(&buffer[..count]);
            sink.write_chunk(&buffer[..count])
                .await
                .map_err(AsyncOutputSealError::Sink)?;
        }
        let metadata = file.metadata().await.map_err(OutputError::Unavailable)?;
        if byte_size != self.identity.size
            || OutputIdentity::from_metadata(&metadata) != self.identity
        {
            return Err(OutputError::ChangedDuringRead.into());
        }
        self.snapshot.validate()?;
        Ok((
            SealedOutput {
                relative_filename: self.relative_filename,
                byte_size,
                sha256_hex: hex_digest(&hasher.finalize()),
            },
            sink,
        ))
    }
}

impl VerifiedOutputDirectory {
    fn validate(&self) -> Result<(), OutputError> {
        validate_private_directory(&self.directory)?;
        let filenames = sorted_entries(&self.directory, MAX_BATCH_OUTPUT_FILES).map_err(
            |error| match error {
                OutputError::TooManyEntries => OutputError::ChangedDuringRead,
                error => error,
            },
        )?;
        if filenames.len() != self.entries.len()
            || filenames
                .iter()
                .zip(&self.entries)
                .any(|(current, (expected, _))| current != expected)
        {
            return Err(OutputError::ChangedDuringRead);
        }
        for (filename, identity) in &self.entries {
            validate_current_output(&self.directory, filename, *identity)?;
        }
        Ok(())
    }
}

pub(crate) fn seal_to_sink<W: Write>(
    directory: Arc<File>,
    contract: OutputContract,
    sink: W,
) -> Result<(SealedOutput, W), OutputError> {
    let file = open_output_at(&directory, contract.relative_filename())?;
    let relative_filename = contract.relative_filename;
    let (sealed, sink, identity) =
        seal_open_file(file, relative_filename.clone(), contract.max_bytes, sink)?;
    validate_current_output(&directory, &relative_filename, identity)?;
    Ok((sealed, sink))
}

fn seal_open_file<W: Write>(
    mut file: File,
    relative_filename: PathBuf,
    max_bytes: u64,
    mut sink: W,
) -> Result<(SealedOutput, W, OutputIdentity), OutputError> {
    let before = file.metadata().map_err(OutputError::Unavailable)?;
    validate_regular_output(&before, max_bytes)?;
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
        if byte_size > max_bytes {
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
            relative_filename,
            byte_size,
            sha256_hex: hex_digest(&digest),
        },
        sink,
        identity,
    ))
}

fn validate_current_output(
    directory: &File,
    relative_filename: &Path,
    expected: OutputIdentity,
) -> Result<(), OutputError> {
    let current = open_output_at(directory, relative_filename)?;
    let metadata = current.metadata().map_err(OutputError::Unavailable)?;
    if OutputIdentity::from_metadata(&metadata) != expected {
        return Err(OutputError::ChangedDuringRead);
    }
    Ok(())
}

fn validate_private_directory(directory: &File) -> Result<(), OutputError> {
    let metadata = directory.metadata().map_err(OutputError::Unavailable)?;
    if !metadata.is_dir()
        || metadata.mode() & 0o7777 != 0o700
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(OutputError::UnsafeDirectory);
    }
    Ok(())
}

fn first_two_entries(directory: &File) -> Result<std::vec::IntoIter<OsString>, OutputError> {
    let mut stream = Dir::read_from(directory)
        .map_err(|error| OutputError::Unavailable(std::io::Error::from(error)))?;
    let mut entries = Vec::with_capacity(2);
    while let Some(entry) = stream.read() {
        let entry = entry.map_err(|error| OutputError::Unavailable(std::io::Error::from(error)))?;
        let name = entry.file_name().to_bytes();
        if matches!(name, b"." | b"..") {
            continue;
        }
        entries.push(OsString::from_vec(name.to_vec()));
        if entries.len() == 2 {
            break;
        }
    }
    Ok(entries.into_iter())
}

fn sorted_entries(directory: &File, max_files: usize) -> Result<Vec<PathBuf>, OutputError> {
    let mut stream = Dir::read_from(directory)
        .map_err(|error| OutputError::Unavailable(std::io::Error::from(error)))?;
    let mut entries = Vec::with_capacity(max_files);
    while let Some(entry) = stream.read() {
        let entry = entry.map_err(|error| OutputError::Unavailable(std::io::Error::from(error)))?;
        let name = entry.file_name().to_bytes();
        if matches!(name, b"." | b"..") {
            continue;
        }
        if entries.len() == max_files {
            return Err(OutputError::TooManyEntries);
        }
        entries.push(PathBuf::from(OsString::from_vec(name.to_vec())));
    }
    entries.sort_by(|left, right| {
        left.as_os_str()
            .as_bytes()
            .cmp(right.as_os_str().as_bytes())
    });
    Ok(entries)
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
