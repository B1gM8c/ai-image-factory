use std::{
    collections::HashSet,
    os::fd::OwnedFd,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use image_cli_runtime::WorkingDirectory;
use image_provider_sdk::{EffectCertainty, ProviderFailure, ProviderFailureClass, RetryDirective};
use rustix::fs::{self as rfs, Mode, OFlags};
use sha2::{Digest, Sha256};

use crate::{
    executor::ExecutorInputObject,
    input_blobs::{InputBlobReadError, InputBlobStore},
    media_inputs::{MediaInputKind, probe_input},
};

use super::journal::{RemoteSubmitJournalError, publish_or_compare, validate_bound_root};

const MAX_INPUT_BYTES: u64 = 256 * 1024 * 1024;

/// A codec-approved local filename and media measurements for one frozen job input.
#[derive(Clone, Debug)]
pub struct GatedCliInputFile {
    filename: String,
    max_bytes: u64,
    metadata: InputMetadata,
}

#[derive(Clone, Debug)]
enum InputMetadata {
    Image { width: u32, height: u32 },
    Video { duration_ms: u64 },
    Audio { duration_ms: u64 },
}

impl GatedCliInputFile {
    pub fn image(
        filename: impl Into<String>,
        max_bytes: u64,
        width: u32,
        height: u32,
    ) -> Result<Self, ProviderFailure> {
        if width == 0 || height == 0 {
            return Err(input_failure("provider_submit_input_manifest_invalid"));
        }
        Self::new(
            filename.into(),
            max_bytes,
            InputMetadata::Image { width, height },
        )
    }

    pub fn video(
        filename: impl Into<String>,
        max_bytes: u64,
        duration_ms: u64,
    ) -> Result<Self, ProviderFailure> {
        if duration_ms == 0 {
            return Err(input_failure("provider_submit_input_manifest_invalid"));
        }
        Self::new(
            filename.into(),
            max_bytes,
            InputMetadata::Video { duration_ms },
        )
    }

    fn new(
        filename: String,
        max_bytes: u64,
        metadata: InputMetadata,
    ) -> Result<Self, ProviderFailure> {
        if filename.is_empty()
            || filename.len() > 255
            || !filename.as_bytes()[0].is_ascii_alphanumeric()
            || !filename
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            || max_bytes == 0
            || max_bytes > MAX_INPUT_BYTES
        {
            return Err(input_failure("provider_submit_input_manifest_invalid"));
        }
        Ok(Self {
            filename,
            max_bytes,
            metadata,
        })
    }

    pub fn audio(
        filename: impl Into<String>,
        max_bytes: u64,
        duration_ms: u64,
    ) -> Result<Self, ProviderFailure> {
        if duration_ms == 0 {
            return Err(input_failure("provider_submit_input_manifest_invalid"));
        }
        Self::new(
            filename.into(),
            max_bytes,
            InputMetadata::Audio { duration_ms },
        )
    }
}

pub(super) async fn stage_inputs(
    store: Option<&Arc<dyn InputBlobStore>>,
    inputs: &[ExecutorInputObject],
    files: Vec<GatedCliInputFile>,
    workspace: &WorkingDirectory,
    started: Instant,
    budget: Duration,
) -> Result<(), ProviderFailure> {
    validate_manifest(inputs, &files)?;
    // Text-only submissions do not open the blob store or an input directory.
    if files.is_empty() {
        return Ok(());
    }
    let store = store.ok_or_else(|| input_failure("provider_submit_input_store_unavailable"))?;
    let directory = Arc::new(InputDirectory::open(workspace)?);
    for (input, file) in inputs.iter().zip(files) {
        let remaining = budget
            .checked_sub(started.elapsed())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| input_failure("provider_submit_deadline_elapsed"))?;
        let bytes = tokio::time::timeout(remaining, store.get(input.blob()))
            .await
            .map_err(|_| input_failure("provider_submit_deadline_elapsed"))?
            .map_err(|error| {
                input_failure(match error {
                    InputBlobReadError::Unavailable => "provider_submit_input_store_unavailable",
                    InputBlobReadError::Integrity => "provider_submit_input_integrity_failed",
                })
            })?;
        let input = input.clone();
        let directory = Arc::clone(&directory);
        tokio::task::spawn_blocking(move || {
            validate_bytes(&input, &file, &bytes)?;
            directory.publish(&file, &bytes)
        })
        .await
        .map_err(|_| input_failure("provider_submit_input_worker_stopped"))??;
    }
    Ok(())
}

fn validate_manifest(
    inputs: &[ExecutorInputObject],
    files: &[GatedCliInputFile],
) -> Result<(), ProviderFailure> {
    if inputs.len() != files.len() || inputs.len() > 50 {
        return Err(input_failure("provider_submit_input_manifest_invalid"));
    }
    let mut names = HashSet::new();
    let mut ids = HashSet::new();
    let mut keys = HashSet::new();
    let session = inputs
        .first()
        .map(|input| input.blob().key.admission_session_id);
    for (input, file) in inputs.iter().zip(files) {
        let blob = input.blob();
        if !names.insert(&file.filename)
            || !ids.insert(blob.key.input_id)
            || !keys.insert((&blob.storage_backend, &blob.object_key))
            || Some(blob.key.admission_session_id) != session
            || blob.key.admission_session_id.is_nil()
            || blob.key.input_id.is_nil()
            || blob.byte_size == 0
            || blob.byte_size > file.max_bytes
        {
            return Err(input_failure("provider_submit_input_manifest_invalid"));
        }
    }
    Ok(())
}

fn validate_bytes(
    input: &ExecutorInputObject,
    file: &GatedCliInputFile,
    bytes: &[u8],
) -> Result<(), ProviderFailure> {
    let invalid = || input_failure("provider_submit_input_integrity_failed");
    if bytes.len() as u64 != input.blob().byte_size
        || bytes.len() as u64 > file.max_bytes
        || hex::encode(Sha256::digest(bytes)) != input.blob().sha256_hex
    {
        return Err(invalid());
    }
    let kind = match file.metadata {
        InputMetadata::Image { .. } => MediaInputKind::Image,
        InputMetadata::Video { .. } => MediaInputKind::Video,
        InputMetadata::Audio { .. } => MediaInputKind::Audio,
    };
    let metadata = probe_input(bytes, kind).map_err(|_| invalid())?;
    let valid = metadata.media_type() == input.media_type()
        && match file.metadata {
            InputMetadata::Image { width, height } => {
                metadata.dimensions() == Some((width, height))
            }
            InputMetadata::Video { duration_ms } | InputMetadata::Audio { duration_ms } => {
                metadata.duration_ms() == Some(duration_ms)
            }
        };
    if !valid {
        return Err(invalid());
    }
    Ok(())
}

struct InputDirectory {
    path: PathBuf,
    directory: OwnedFd,
}

impl InputDirectory {
    fn open(workspace: &WorkingDirectory) -> Result<Self, ProviderFailure> {
        let path = workspace.path().to_owned();
        let directory = rfs::open(
            &path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| input_failure("provider_submit_input_workspace_invalid"))?;
        let bound = Self { path, directory };
        bound.validate()?;
        Ok(bound)
    }

    fn validate(&self) -> Result<(), ProviderFailure> {
        validate_bound_root(
            &self.path,
            &self.directory,
            RemoteSubmitJournalError::Integrity,
        )
        .map_err(|_| input_failure("provider_submit_input_workspace_invalid"))
    }

    fn publish(&self, file: &GatedCliInputFile, bytes: &[u8]) -> Result<(), ProviderFailure> {
        self.validate()?;
        // Same atomic, no-follow, 0600, publish-or-compare primitive as the submit journal.
        publish_or_compare(&self.directory, &file.filename, bytes, file.max_bytes)
            .map_err(|_| input_failure("provider_submit_input_workspace_invalid"))?;
        self.validate()
    }
}

pub(super) fn input_failure(code: &'static str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Permanent,
        code,
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Never,
    )
    .expect("static input failure is valid")
}

#[cfg(test)]
mod tests;
