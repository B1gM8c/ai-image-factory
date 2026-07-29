use std::{
    fs,
    io::{Read, Write},
    os::fd::AsFd,
    path::{Path, PathBuf},
    sync::Arc,
};

#[cfg(target_os = "macos")]
use std::os::fd::AsRawFd;

use async_trait::async_trait;
use rustix::{
    fs::{self as rfs, AtFlags, Dir, FileType, Mode, OFlags, RenameFlags},
    io::Errno,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::{
    ArtifactBlobStore, ArtifactIdentity, ArtifactMetadata, ArtifactReadError, ArtifactWriteError,
    ExecutorArtifactReference, FILESYSTEM_BACKEND, customer_object_key, executor_object_key,
    sha256_hex,
};
use crate::{
    ImageGatewayError,
    batches::{
        BatchFileBlob, BatchFileBlobError, BatchFileBlobStore, MAX_FILE_BYTES,
        batch_file_object_key,
    },
    input_blobs::{
        InputBlobDeleteError, InputBlobKey, InputBlobReadError, InputBlobRef, InputBlobStore,
        InputBlobWriteError,
    },
};

pub(crate) const MAX_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;
const STORAGE_NAMESPACE_MARKER: &str = ".storage-namespace-id";

pub struct FilesystemArtifactBlobStore {
    root: PathBuf,
    executor_namespace_id: Uuid,
    batch_files: Arc<fs::File>,
    customer_objects: Arc<fs::File>,
    executor_objects: Arc<fs::File>,
    executor_staging: Arc<fs::File>,
}

struct PendingArtifact {
    temporary: PathBuf,
}

pub(super) struct FilesystemExecutorArtifactStage {
    staging_directory: Arc<fs::File>,
    temporary_name: String,
    executor_objects: Arc<fs::File>,
    artifact_id: Uuid,
    file: Option<tokio::fs::File>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExecutorArtifactStageError {
    Unavailable,
    Integrity,
}

impl PendingArtifact {
    fn new(temporary: PathBuf) -> Self {
        Self { temporary }
    }
}

impl Drop for PendingArtifact {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.temporary);
    }
}

impl FilesystemExecutorArtifactStage {
    pub(super) async fn write_chunk(
        &mut self,
        chunk: &[u8],
    ) -> Result<(), ExecutorArtifactStageError> {
        let file = self
            .file
            .as_mut()
            .ok_or(ExecutorArtifactStageError::Unavailable)?;
        file.write_all(chunk)
            .await
            .map_err(|_| ExecutorArtifactStageError::Unavailable)
    }

    pub(super) async fn finish_writes(&mut self) -> Result<(), ExecutorArtifactStageError> {
        let Some(mut file) = self.file.take() else {
            return Err(ExecutorArtifactStageError::Unavailable);
        };
        file.flush()
            .await
            .map_err(|_| ExecutorArtifactStageError::Unavailable)?;
        let file = file.into_std().await;
        tokio::task::spawn_blocking(move || sync_artifact_file(&file))
            .await
            .map_err(|_| ExecutorArtifactStageError::Unavailable)?
            .map_err(|_| ExecutorArtifactStageError::Unavailable)
    }

    pub(super) async fn open_reader(
        &self,
        expected_size: u64,
    ) -> Result<fs::File, ExecutorArtifactStageError> {
        let directory = Arc::clone(&self.staging_directory);
        let temporary_name = self.temporary_name.clone();
        tokio::task::spawn_blocking(move || {
            open_staged_executor_file(&directory, &temporary_name, expected_size)
        })
        .await
        .map_err(|_| ExecutorArtifactStageError::Unavailable)?
    }

    pub(super) async fn commit(
        &mut self,
        expected_sha256: [u8; 32],
        expected_size: u64,
    ) -> Result<(), ExecutorArtifactStageError> {
        if self.file.is_some() {
            return Err(ExecutorArtifactStageError::Unavailable);
        }
        let staging_directory = Arc::clone(&self.staging_directory);
        let temporary_name = self.temporary_name.clone();
        let executor_objects = Arc::clone(&self.executor_objects);
        let artifact_id = self.artifact_id;
        tokio::task::spawn_blocking(move || {
            commit_staged_executor_object(
                &staging_directory,
                &temporary_name,
                &executor_objects,
                artifact_id,
                expected_sha256,
                expected_size,
            )
        })
        .await
        .map_err(|_| ExecutorArtifactStageError::Unavailable)?
    }
}

impl Drop for FilesystemExecutorArtifactStage {
    fn drop(&mut self) {
        cleanup_executor_temporary(&self.staging_directory, &self.temporary_name);
    }
}

impl FilesystemArtifactBlobStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, ImageGatewayError> {
        let root = validate_root(root.as_ref())?;
        let executor_namespace_id = load_or_create_storage_namespace_id(&root)?;
        let batch_files = root.join("batch-files");
        let objects = root.join("objects");
        let executor_objects = root.join("executor-objects");
        let executor_staging = root.join("executor-staging");
        let inputs = root.join("inputs");
        prepare_storage_directory(
            &batch_files,
            "batch file directory is not writable",
            "batch file directory must be a directory and must not be a symlink",
        )?;
        prepare_storage_directory(
            &objects,
            "artifact object directory is not writable",
            "artifact object directory must be a directory and must not be a symlink",
        )?;
        prepare_storage_directory(
            &executor_objects,
            "executor artifact directory is not writable",
            "executor artifact directory must be a directory and must not be a symlink",
        )?;
        prepare_storage_directory(
            &executor_staging,
            "executor staging directory is not writable",
            "executor staging directory must be a directory and must not be a symlink",
        )?;
        prepare_storage_directory(
            &inputs,
            "input blob directory is not writable",
            "input blob directory must be a directory and must not be a symlink",
        )?;
        sync_directory(&root)
            .map_err(|_| ImageGatewayError::config("artifact root cannot be synchronized"))?;
        let batch_files = open_private_directory(&batch_files).map_err(|_| {
            ImageGatewayError::config("batch file directory could not be opened safely")
        })?;
        let customer_objects = open_private_directory(&objects).map_err(|_| {
            ImageGatewayError::config("customer artifact directory could not be opened safely")
        })?;
        let executor_objects = open_private_directory(&executor_objects).map_err(|_| {
            ImageGatewayError::config("executor artifact directory could not be opened safely")
        })?;
        let executor_staging = open_private_directory(&executor_staging).map_err(|_| {
            ImageGatewayError::config("executor staging directory could not be opened safely")
        })?;
        Ok(Self {
            root,
            executor_namespace_id,
            batch_files: Arc::new(batch_files),
            customer_objects: Arc::new(customer_objects),
            executor_objects: Arc::new(executor_objects),
            executor_staging: Arc::new(executor_staging),
        })
    }

    fn object_key(identity: &ArtifactIdentity) -> String {
        customer_object_key(identity.artifact_id)
    }

    pub(crate) async fn put_blob_key(
        &self,
        object_key: &str,
        bytes: &[u8],
    ) -> Result<(String, u64), ArtifactWriteError> {
        if bytes.is_empty() || bytes.len() as u64 > MAX_ARTIFACT_BYTES {
            return Err(ArtifactWriteError::Unavailable);
        }
        let result = (sha256_hex(bytes), bytes.len() as u64);
        let object_path = self.root.join(object_key);
        let parent = object_path
            .parent()
            .ok_or(ArtifactWriteError::Unavailable)?;
        match tokio::fs::create_dir(parent).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(ArtifactWriteError::Unavailable),
        }
        let parent_metadata = tokio::fs::symlink_metadata(parent)
            .await
            .map_err(|_| ArtifactWriteError::Unavailable)?;
        if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
            return Err(ArtifactWriteError::Unavailable);
        }
        set_private_directory_permissions(parent).map_err(|_| ArtifactWriteError::Unavailable)?;
        let namespace = parent.parent().ok_or(ArtifactWriteError::Unavailable)?;
        sync_directory_async(namespace.to_path_buf())
            .await
            .map_err(|_| ArtifactWriteError::Unavailable)?;

        let temporary = parent.join(format!(".tmp-{}", Uuid::new_v4().simple()));
        let pending = PendingArtifact::new(temporary.clone());
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .await
            .map_err(|_| ArtifactWriteError::Unavailable)?;
        let write_result = async {
            file.write_all(bytes).await?;
            file.flush().await?;
            file.sync_all().await
        }
        .await;
        drop(file);
        if write_result.is_err() {
            return Err(ArtifactWriteError::Unavailable);
        }

        match tokio::fs::hard_link(&temporary, &object_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = self
                    .get_blob_key(object_key, &result.0, result.1)
                    .await
                    .map_err(|_| ArtifactWriteError::Unavailable)?;
                if existing != bytes {
                    return Err(ArtifactWriteError::Unavailable);
                }
                return Ok(result);
            }
            Err(_) => return Err(ArtifactWriteError::Unavailable),
        }
        if tokio::fs::remove_file(&temporary).await.is_err()
            || sync_directory_async(parent.to_path_buf()).await.is_err()
        {
            return Err(ArtifactWriteError::Unavailable);
        }

        drop(pending);
        Ok(result)
    }

    pub(crate) async fn get_blob_key(
        &self,
        object_key: &str,
        expected_sha256: &str,
        expected_size: u64,
    ) -> Result<Vec<u8>, ArtifactReadError> {
        if expected_size == 0 || expected_size > MAX_ARTIFACT_BYTES {
            return Err(ArtifactReadError::Integrity);
        }
        let path = self.root.join(object_key);
        let std_file = open_regular_no_follow(&path)?;
        let metadata = std_file
            .metadata()
            .map_err(|_| ArtifactReadError::Unavailable)?;
        if !metadata.is_file() || metadata.len() != expected_size {
            return Err(ArtifactReadError::Integrity);
        }
        let capacity = usize::try_from(expected_size).map_err(|_| ArtifactReadError::Integrity)?;
        let mut file = tokio::fs::File::from_std(std_file).take(expected_size + 1);
        let mut bytes = Vec::with_capacity(capacity);
        file.read_to_end(&mut bytes)
            .await
            .map_err(|_| ArtifactReadError::Unavailable)?;
        if bytes.len() as u64 != expected_size || sha256_hex(&bytes) != expected_sha256 {
            return Err(ArtifactReadError::Integrity);
        }
        Ok(bytes)
    }

    pub(crate) async fn delete_blob_key(&self, object_key: &str) -> Result<(), ArtifactWriteError> {
        let path = self.root.join(object_key);
        let parent = path
            .parent()
            .ok_or(ArtifactWriteError::Unavailable)?
            .to_path_buf();
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return tokio::task::spawn_blocking(move || {
                    open_private_directory(&parent)
                        .map(|_| ())
                        .map_err(|_| ArtifactWriteError::Unavailable)
                })
                .await
                .map_err(|_| ArtifactWriteError::Unavailable)?;
            }
            Err(_) => return Err(ArtifactWriteError::Unavailable),
        }
        sync_directory_async(parent)
            .await
            .map_err(|_| ArtifactWriteError::Unavailable)
    }

    pub(crate) async fn put_executor_artifact(
        &self,
        identity: ArtifactIdentity,
        bytes: &[u8],
    ) -> Result<ArtifactMetadata, ArtifactWriteError> {
        self.executor_storage_namespace()
            .map_err(|_| ArtifactWriteError::Unavailable)?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_ARTIFACT_BYTES {
            return Err(ArtifactWriteError::Unavailable);
        }
        let object_key = executor_object_key(identity.artifact_id);
        let expected_sha256 = sha256_hex(bytes);
        let byte_size = bytes.len() as u64;
        let root = self.executor_objects.clone();
        let artifact_id = identity.artifact_id;
        let owned_bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || {
            put_executor_object_at(&root, artifact_id, &owned_bytes, &expected_sha256)
        })
        .await
        .map_err(|_| ArtifactWriteError::Unavailable)??;
        Ok(ArtifactMetadata {
            identity,
            storage_backend: FILESYSTEM_BACKEND.to_string(),
            object_key,
            sha256_hex: sha256_hex(bytes),
            byte_size,
        })
    }

    pub(crate) async fn get_executor_artifact(
        &self,
        artifact: &ArtifactMetadata,
    ) -> Result<Vec<u8>, ArtifactReadError> {
        self.executor_storage_namespace()?;
        if artifact.storage_backend != FILESYSTEM_BACKEND
            || artifact.object_key != executor_object_key(artifact.identity.artifact_id)
        {
            return Err(ArtifactReadError::Integrity);
        }
        let root = self.executor_objects.clone();
        let artifact_id = artifact.identity.artifact_id;
        let expected_sha256 = artifact.sha256_hex.clone();
        let expected_size = artifact.byte_size;
        tokio::task::spawn_blocking(move || {
            read_executor_object_at(&root, artifact_id, &expected_sha256, expected_size)
        })
        .await
        .map_err(|_| ArtifactReadError::Unavailable)?
    }

    pub(crate) async fn read_executor_reference(
        &self,
        artifact: &ExecutorArtifactReference<'_>,
    ) -> Result<Vec<u8>, ArtifactReadError> {
        if artifact.storage_backend != FILESYSTEM_BACKEND
            || !self.executor_storage_namespace_matches(artifact.storage_namespace)?
            || artifact.object_key != executor_object_key(artifact.authority_id)
        {
            return Err(ArtifactReadError::Integrity);
        }
        let root = self.executor_objects.clone();
        let authority_id = artifact.authority_id;
        let expected_sha256 = artifact.sha256_hex.to_string();
        let expected_size = artifact.byte_size;
        tokio::task::spawn_blocking(move || {
            read_executor_object_at(&root, authority_id, &expected_sha256, expected_size)
        })
        .await
        .map_err(|_| ArtifactReadError::Unavailable)?
    }

    pub(crate) async fn delete_executor_reference(
        &self,
        artifact: &ExecutorArtifactReference<'_>,
    ) -> Result<(), ArtifactWriteError> {
        if artifact.storage_backend != FILESYSTEM_BACKEND
            || !self
                .executor_storage_namespace_matches(artifact.storage_namespace)
                .map_err(|_| ArtifactWriteError::Unavailable)?
            || artifact.object_key != executor_object_key(artifact.authority_id)
            || artifact.sha256_hex.len() != 64
            || artifact.byte_size == 0
        {
            return Err(ArtifactWriteError::Unavailable);
        }
        let root = self.executor_objects.clone();
        let authority_id = artifact.authority_id;
        let expected_sha256 = artifact.sha256_hex.to_owned();
        let expected_size = artifact.byte_size;
        tokio::task::spawn_blocking(move || {
            delete_verified_executor_object_at(&root, authority_id, &expected_sha256, expected_size)
        })
        .await
        .map_err(|_| ArtifactWriteError::Unavailable)?
    }

    pub(crate) async fn delete_unpublished_executor_artifact(
        &self,
        artifact: &ArtifactMetadata,
    ) -> Result<(), ArtifactWriteError> {
        self.executor_storage_namespace()
            .map_err(|_| ArtifactWriteError::Unavailable)?;
        if artifact.storage_backend != FILESYSTEM_BACKEND
            || artifact.object_key != executor_object_key(artifact.identity.artifact_id)
        {
            return Err(ArtifactWriteError::Unavailable);
        }
        let root = self.executor_objects.clone();
        let artifact_id = artifact.identity.artifact_id;
        tokio::task::spawn_blocking(move || delete_executor_object_at(&root, artifact_id))
            .await
            .map_err(|_| ArtifactWriteError::Unavailable)?
    }

    pub(super) async fn begin_executor_artifact_stage(
        &self,
        artifact_id: Uuid,
        lease_epoch: i64,
    ) -> Result<FilesystemExecutorArtifactStage, ExecutorArtifactStageError> {
        if artifact_id.is_nil() || lease_epoch <= 0 {
            return Err(ExecutorArtifactStageError::Integrity);
        }
        self.executor_storage_namespace()
            .map_err(map_stage_read_error)?;
        self.validate_executor_staging_namespace()
            .map_err(map_stage_read_error)?;
        let executor_staging = Arc::clone(&self.executor_staging);
        let executor_objects = Arc::clone(&self.executor_objects);
        let (staging_directory, temporary_name, file) = tokio::task::spawn_blocking(move || {
            begin_executor_artifact_stage_at(&executor_staging, artifact_id, lease_epoch)
        })
        .await
        .map_err(|_| ExecutorArtifactStageError::Unavailable)??;
        Ok(FilesystemExecutorArtifactStage {
            staging_directory: Arc::new(staging_directory),
            temporary_name,
            executor_objects,
            artifact_id,
            file: Some(tokio::fs::File::from_std(file)),
        })
    }

    pub(crate) fn executor_storage_namespace(&self) -> Result<String, ArtifactReadError> {
        validate_bound_private_directory(
            &self.root.join("executor-objects"),
            self.executor_objects.as_ref(),
        )?;
        Ok(format!(
            "{FILESYSTEM_BACKEND}:{}#executor={}",
            self.root.display(),
            self.executor_namespace_id
        ))
    }

    fn executor_storage_namespace_matches(
        &self,
        candidate: &str,
    ) -> Result<bool, ArtifactReadError> {
        if candidate == self.executor_storage_namespace()? {
            return Ok(true);
        }
        let prefix = format!("{FILESYSTEM_BACKEND}:{}#executor=", self.root.display());
        let Some(legacy) = candidate.strip_prefix(&prefix) else {
            return Ok(false);
        };
        let Some((device, inode)) = legacy.split_once(':') else {
            return Ok(false);
        };
        Ok(!device.is_empty()
            && !inode.is_empty()
            && device.bytes().all(|byte| byte.is_ascii_digit())
            && inode.bytes().all(|byte| byte.is_ascii_digit()))
    }

    fn validate_executor_staging_namespace(&self) -> Result<(), ArtifactReadError> {
        validate_bound_private_directory(
            &self.root.join("executor-staging"),
            self.executor_staging.as_ref(),
        )
        .map(|_| ())
    }

    fn validate_customer_namespace(&self) -> Result<(), ArtifactReadError> {
        validate_bound_private_directory(&self.root.join("objects"), self.customer_objects.as_ref())
            .map(|_| ())
    }

    fn validate_batch_file_namespace(&self) -> Result<(), ArtifactReadError> {
        validate_bound_private_directory(&self.root.join("batch-files"), self.batch_files.as_ref())
            .map(|_| ())
    }
}

fn put_executor_object_at(
    root: &fs::File,
    artifact_id: Uuid,
    bytes: &[u8],
    expected_sha256: &str,
) -> Result<(), ArtifactWriteError> {
    let (shard_name, object_name) = executor_object_names(artifact_id);
    let shard = open_or_create_executor_shard(root, &shard_name)?;
    let temporary = format!(".tmp-{}", Uuid::new_v4().simple());
    let fd = rfs::openat(
        &shard,
        temporary.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| ArtifactWriteError::Unavailable)?;
    if rfs::fchmod(&fd, Mode::RUSR | Mode::WUSR).is_err() {
        drop(fd);
        cleanup_executor_temporary(&shard, &temporary);
        return Err(ArtifactWriteError::Unavailable);
    }
    let mut file = fs::File::from(fd);
    if file.write_all(bytes).is_err() || sync_artifact_file(&file).is_err() {
        drop(file);
        cleanup_executor_temporary(&shard, &temporary);
        return Err(ArtifactWriteError::Unavailable);
    }
    drop(file);

    match rfs::renameat_with(
        &shard,
        temporary.as_str(),
        &shard,
        object_name.as_str(),
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => rfs::fsync(&shard).map_err(|_| ArtifactWriteError::Unavailable),
        Err(Errno::EXIST) => {
            cleanup_executor_temporary(&shard, &temporary);
            rfs::fsync(&shard).map_err(|_| ArtifactWriteError::Unavailable)?;
            let existing = read_executor_object_from_shard(
                &shard,
                &object_name,
                expected_sha256,
                bytes.len() as u64,
            )
            .map_err(|_| ArtifactWriteError::Unavailable)?;
            if existing == bytes {
                Ok(())
            } else {
                Err(ArtifactWriteError::Unavailable)
            }
        }
        Err(_) => {
            cleanup_executor_temporary(&shard, &temporary);
            Err(ArtifactWriteError::Unavailable)
        }
    }
}

fn begin_executor_artifact_stage_at(
    staging_root: &fs::File,
    artifact_id: Uuid,
    lease_epoch: i64,
) -> Result<(fs::File, String, fs::File), ExecutorArtifactStageError> {
    let (shard_name, execution_name) = executor_object_names(artifact_id);
    let shard = open_or_create_private_directory_at(staging_root, &shard_name)?;
    let execution = open_or_create_private_directory_at(&shard, &execution_name)?;
    cleanup_abandoned_executor_stages(&execution, lease_epoch)?;
    let temporary_name = format!(".epoch-{lease_epoch}-{}", Uuid::new_v4().simple());
    let fd = rfs::openat(
        &execution,
        temporary_name.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| ExecutorArtifactStageError::Unavailable)?;
    if rfs::fchmod(&fd, Mode::RUSR | Mode::WUSR).is_err() {
        drop(fd);
        cleanup_executor_temporary(&execution, &temporary_name);
        return Err(ExecutorArtifactStageError::Unavailable);
    }
    Ok((execution, temporary_name, fs::File::from(fd)))
}

fn open_or_create_private_directory_at(
    parent: &fs::File,
    name: &str,
) -> Result<fs::File, ExecutorArtifactStageError> {
    match rfs::mkdirat(parent, name, Mode::RWXU) {
        Ok(()) | Err(Errno::EXIST) => {}
        Err(_) => return Err(ExecutorArtifactStageError::Unavailable),
    }
    rfs::fsync(parent).map_err(|_| ExecutorArtifactStageError::Unavailable)?;
    let fd = rfs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| match error {
        Errno::LOOP | Errno::NOTDIR => ExecutorArtifactStageError::Integrity,
        _ => ExecutorArtifactStageError::Unavailable,
    })?;
    let directory = fs::File::from(fd);
    validate_private_directory_fd(&directory).map_err(|_| ExecutorArtifactStageError::Integrity)?;
    Ok(directory)
}

fn cleanup_abandoned_executor_stages(
    directory: &fs::File,
    current_epoch: i64,
) -> Result<(), ExecutorArtifactStageError> {
    let mut entries =
        Dir::read_from(directory).map_err(|_| ExecutorArtifactStageError::Unavailable)?;
    let mut removed = false;
    while let Some(entry) = entries.read() {
        let entry = entry.map_err(|_| ExecutorArtifactStageError::Unavailable)?;
        let name = entry.file_name();
        let bytes = name.to_bytes();
        if matches!(bytes, b"." | b"..") {
            continue;
        }
        let Some(epoch) = executor_stage_epoch(bytes) else {
            return Err(ExecutorArtifactStageError::Integrity);
        };
        if epoch > current_epoch {
            return Err(ExecutorArtifactStageError::Integrity);
        }
        let stat = rfs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| ExecutorArtifactStageError::Unavailable)?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
            return Err(ExecutorArtifactStageError::Integrity);
        }
        rfs::unlinkat(directory, name, AtFlags::empty())
            .map_err(|_| ExecutorArtifactStageError::Unavailable)?;
        removed = true;
    }
    if removed {
        rfs::fsync(directory).map_err(|_| ExecutorArtifactStageError::Unavailable)?;
    }
    Ok(())
}

fn executor_stage_epoch(name: &[u8]) -> Option<i64> {
    let epoch = name.strip_prefix(b".epoch-")?;
    let separator = epoch.iter().position(|byte| *byte == b'-')?;
    let (epoch, nonce) = epoch.split_at(separator);
    if epoch.is_empty()
        || nonce.len() != 33
        || nonce[0] != b'-'
        || !epoch.iter().all(u8::is_ascii_digit)
        || !nonce[1..].iter().all(u8::is_ascii_hexdigit)
    {
        return None;
    }
    std::str::from_utf8(epoch).ok()?.parse().ok()
}

fn open_staged_executor_file(
    directory: &fs::File,
    temporary_name: &str,
    expected_size: u64,
) -> Result<fs::File, ExecutorArtifactStageError> {
    if expected_size == 0 || expected_size > MAX_ARTIFACT_BYTES {
        return Err(ExecutorArtifactStageError::Integrity);
    }
    let fd = rfs::openat(
        directory,
        temporary_name,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| match error {
        Errno::NOENT | Errno::LOOP => ExecutorArtifactStageError::Integrity,
        _ => ExecutorArtifactStageError::Unavailable,
    })?;
    let file = fs::File::from(fd);
    validate_private_regular_file(&file, expected_size).map_err(map_stage_read_error)?;
    Ok(file)
}

fn commit_staged_executor_object(
    staging_directory: &fs::File,
    temporary_name: &str,
    executor_objects: &fs::File,
    artifact_id: Uuid,
    expected_sha256: [u8; 32],
    expected_size: u64,
) -> Result<(), ExecutorArtifactStageError> {
    let (shard_name, object_name) = executor_object_names(artifact_id);
    let object_shard = open_or_create_executor_shard(executor_objects, &shard_name)
        .map_err(|_| ExecutorArtifactStageError::Unavailable)?;
    match rfs::renameat_with(
        staging_directory,
        temporary_name,
        &object_shard,
        object_name.as_str(),
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {
            rfs::fsync(&object_shard).map_err(|_| ExecutorArtifactStageError::Unavailable)?;
            rfs::fsync(staging_directory).map_err(|_| ExecutorArtifactStageError::Unavailable)
        }
        Err(Errno::EXIST) => {
            let matches = executor_object_matches_from_shard(
                &object_shard,
                &object_name,
                expected_sha256,
                expected_size,
            )
            .map_err(map_stage_read_error)?;
            cleanup_executor_temporary(staging_directory, temporary_name);
            if matches {
                Ok(())
            } else {
                Err(ExecutorArtifactStageError::Integrity)
            }
        }
        Err(_) => {
            cleanup_executor_temporary(staging_directory, temporary_name);
            Err(ExecutorArtifactStageError::Unavailable)
        }
    }
}

fn read_executor_object_at(
    root: &fs::File,
    artifact_id: Uuid,
    expected_sha256: &str,
    expected_size: u64,
) -> Result<Vec<u8>, ArtifactReadError> {
    let (shard_name, object_name) = executor_object_names(artifact_id);
    let shard = open_executor_shard(root, &shard_name)?;
    read_executor_object_from_shard(&shard, &object_name, expected_sha256, expected_size)
}

fn read_executor_object_from_shard(
    shard: &fs::File,
    object_name: &str,
    expected_sha256: &str,
    expected_size: u64,
) -> Result<Vec<u8>, ArtifactReadError> {
    if expected_size == 0 || expected_size > MAX_ARTIFACT_BYTES {
        return Err(ArtifactReadError::Integrity);
    }
    let fd = rfs::openat(
        shard,
        object_name,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| match error {
        Errno::NOENT | Errno::LOOP => ArtifactReadError::Integrity,
        _ => ArtifactReadError::Unavailable,
    })?;
    let mut file = fs::File::from(fd);
    validate_private_regular_file(&file, expected_size)?;
    let capacity = usize::try_from(expected_size).map_err(|_| ArtifactReadError::Integrity)?;
    let mut bytes = Vec::with_capacity(capacity);
    Read::by_ref(&mut file)
        .take(expected_size + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ArtifactReadError::Unavailable)?;
    let final_stat = rfs::fstat(&file).map_err(|_| ArtifactReadError::Unavailable)?;
    if final_stat.st_size != i64::try_from(expected_size).unwrap_or(-1)
        || bytes.len() as u64 != expected_size
        || sha256_hex(&bytes) != expected_sha256
    {
        return Err(ArtifactReadError::Integrity);
    }
    Ok(bytes)
}

fn executor_object_matches_from_shard(
    shard: &fs::File,
    object_name: &str,
    expected_sha256: [u8; 32],
    expected_size: u64,
) -> Result<bool, ArtifactReadError> {
    if expected_size == 0 || expected_size > MAX_ARTIFACT_BYTES {
        return Err(ArtifactReadError::Integrity);
    }
    let fd = rfs::openat(
        shard,
        object_name,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| match error {
        Errno::NOENT | Errno::LOOP => ArtifactReadError::Integrity,
        _ => ArtifactReadError::Unavailable,
    })?;
    let mut file = fs::File::from(fd);
    validate_private_regular_file(&file, expected_size)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut byte_size = 0_u64;
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| ArtifactReadError::Unavailable)?;
        if count == 0 {
            break;
        }
        byte_size = byte_size
            .checked_add(count as u64)
            .ok_or(ArtifactReadError::Integrity)?;
        if byte_size > expected_size {
            return Ok(false);
        }
        hasher.update(&buffer[..count]);
    }
    let final_stat = rfs::fstat(&file).map_err(|_| ArtifactReadError::Unavailable)?;
    Ok(byte_size == expected_size
        && final_stat.st_size == i64::try_from(expected_size).unwrap_or(-1)
        && <[u8; 32]>::from(hasher.finalize()) == expected_sha256)
}

fn delete_executor_object_at(root: &fs::File, artifact_id: Uuid) -> Result<(), ArtifactWriteError> {
    let (shard_name, object_name) = executor_object_names(artifact_id);
    let shard = match open_executor_shard(root, &shard_name) {
        Ok(shard) => shard,
        Err(ArtifactReadError::Integrity | ArtifactReadError::Unavailable) => {
            return Err(ArtifactWriteError::Unavailable);
        }
    };
    match rfs::unlinkat(&shard, object_name.as_str(), AtFlags::empty()) {
        Ok(()) => rfs::fsync(&shard).map_err(|_| ArtifactWriteError::Unavailable),
        Err(Errno::NOENT) => Ok(()),
        Err(_) => Err(ArtifactWriteError::Unavailable),
    }
}

fn delete_verified_executor_object_at(
    root: &fs::File,
    artifact_id: Uuid,
    expected_sha256: &str,
    expected_size: u64,
) -> Result<(), ArtifactWriteError> {
    let (shard_name, object_name) = executor_object_names(artifact_id);
    let shard =
        open_executor_shard(root, &shard_name).map_err(|_| ArtifactWriteError::Unavailable)?;
    match read_executor_object_from_shard(&shard, &object_name, expected_sha256, expected_size) {
        Ok(_) => {}
        Err(ArtifactReadError::Integrity) => {
            let missing = rfs::openat(
                &shard,
                object_name.as_str(),
                OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .is_err_and(|error| error == Errno::NOENT);
            if missing {
                return Ok(());
            }
            return Err(ArtifactWriteError::Unavailable);
        }
        Err(ArtifactReadError::Unavailable) => return Err(ArtifactWriteError::Unavailable),
    }
    match rfs::unlinkat(&shard, object_name.as_str(), AtFlags::empty()) {
        Ok(()) => rfs::fsync(&shard).map_err(|_| ArtifactWriteError::Unavailable),
        Err(Errno::NOENT) => Ok(()),
        Err(_) => Err(ArtifactWriteError::Unavailable),
    }
}

fn delete_customer_object_at(root: &fs::File, artifact_id: Uuid) -> Result<(), ArtifactWriteError> {
    delete_executor_object_at(root, artifact_id)
}

fn open_or_create_executor_shard(
    root: &fs::File,
    shard_name: &str,
) -> Result<fs::File, ArtifactWriteError> {
    match rfs::mkdirat(root, shard_name, Mode::RWXU) {
        Ok(()) | Err(Errno::EXIST) => {}
        Err(_) => return Err(ArtifactWriteError::Unavailable),
    }
    rfs::fsync(root).map_err(|_| ArtifactWriteError::Unavailable)?;
    open_executor_shard(root, shard_name).map_err(|_| ArtifactWriteError::Unavailable)
}

fn open_executor_shard(root: &fs::File, shard_name: &str) -> Result<fs::File, ArtifactReadError> {
    let fd = rfs::openat(
        root,
        shard_name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| match error {
        Errno::NOENT | Errno::LOOP => ArtifactReadError::Integrity,
        _ => ArtifactReadError::Unavailable,
    })?;
    let file = fs::File::from(fd);
    validate_private_directory_fd(&file).map_err(|_| ArtifactReadError::Integrity)?;
    Ok(file)
}

fn open_private_directory(path: &Path) -> std::io::Result<fs::File> {
    let fd = rfs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(std::io::Error::from)?;
    let file = fs::File::from(fd);
    validate_private_directory_fd(&file)?;
    Ok(file)
}

fn validate_bound_private_directory(
    path: &Path,
    opened: &fs::File,
) -> Result<rfs::Stat, ArtifactReadError> {
    let current = open_private_directory(path).map_err(|_| ArtifactReadError::Integrity)?;
    let opened_stat = rfs::fstat(opened).map_err(|_| ArtifactReadError::Unavailable)?;
    let current_stat = rfs::fstat(&current).map_err(|_| ArtifactReadError::Unavailable)?;
    if opened_stat.st_dev != current_stat.st_dev || opened_stat.st_ino != current_stat.st_ino {
        return Err(ArtifactReadError::Integrity);
    }
    Ok(opened_stat)
}

fn validate_private_directory_fd(directory: &impl AsFd) -> std::io::Result<()> {
    let stat = rfs::fstat(directory).map_err(std::io::Error::from)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
        || Mode::from_raw_mode(stat.st_mode) != Mode::RWXU
    {
        return Err(std::io::Error::other("artifact directory is not private"));
    }
    // SAFETY: geteuid has no preconditions and does not dereference pointers.
    if stat.st_uid != unsafe { libc::geteuid() } {
        return Err(std::io::Error::other(
            "artifact directory has an unexpected owner",
        ));
    }
    Ok(())
}

fn validate_private_regular_file(
    file: &impl AsFd,
    expected_size: u64,
) -> Result<(), ArtifactReadError> {
    let stat = rfs::fstat(file).map_err(|_| ArtifactReadError::Unavailable)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || Mode::from_raw_mode(stat.st_mode) != Mode::RUSR | Mode::WUSR
        || stat.st_nlink != 1
        || stat.st_size != i64::try_from(expected_size).unwrap_or(-1)
    {
        return Err(ArtifactReadError::Integrity);
    }
    // SAFETY: geteuid has no preconditions and does not dereference pointers.
    if stat.st_uid != unsafe { libc::geteuid() } {
        return Err(ArtifactReadError::Integrity);
    }
    Ok(())
}

fn cleanup_executor_temporary(shard: &fs::File, temporary: &str) {
    let _ = rfs::unlinkat(shard, temporary, AtFlags::empty());
    let _ = rfs::fsync(shard);
}

fn map_stage_read_error(error: ArtifactReadError) -> ExecutorArtifactStageError {
    match error {
        ArtifactReadError::Unavailable => ExecutorArtifactStageError::Unavailable,
        ArtifactReadError::Integrity => ExecutorArtifactStageError::Integrity,
    }
}

fn executor_object_names(artifact_id: Uuid) -> (String, String) {
    let object_name = artifact_id.simple().to_string();
    (object_name[..2].to_string(), object_name)
}

fn input_blob_key(key: &InputBlobKey) -> String {
    format!(
        "inputs/{}/{}",
        key.admission_session_id.simple(),
        key.input_id.simple()
    )
}

#[async_trait]
impl BatchFileBlobStore for FilesystemArtifactBlobStore {
    async fn put(
        &self,
        file_uuid: Uuid,
        bytes: &[u8],
    ) -> Result<BatchFileBlob, BatchFileBlobError> {
        self.validate_batch_file_namespace()
            .map_err(map_batch_read_error)?;
        if file_uuid.is_nil() || bytes.is_empty() || bytes.len() as u64 > MAX_FILE_BYTES {
            return Err(BatchFileBlobError::Integrity);
        }
        let expected_sha256 = sha256_hex(bytes);
        let root = Arc::clone(&self.batch_files);
        let owned_bytes = bytes.to_vec();
        let write_sha256 = expected_sha256.clone();
        tokio::task::spawn_blocking(move || {
            put_executor_object_at(&root, file_uuid, &owned_bytes, &write_sha256)
        })
        .await
        .map_err(|_| BatchFileBlobError::Unavailable)?
        .map_err(|_| BatchFileBlobError::Unavailable)?;
        Ok(BatchFileBlob {
            storage_backend: FILESYSTEM_BACKEND.to_string(),
            object_key: batch_file_object_key(file_uuid),
            sha256_hex: expected_sha256,
            byte_size: bytes.len() as u64,
        })
    }

    async fn get(
        &self,
        file_uuid: Uuid,
        blob: &BatchFileBlob,
    ) -> Result<Vec<u8>, BatchFileBlobError> {
        self.validate_batch_file_namespace()
            .map_err(map_batch_read_error)?;
        if file_uuid.is_nil()
            || blob.storage_backend != FILESYSTEM_BACKEND
            || blob.object_key != batch_file_object_key(file_uuid)
            || blob.sha256_hex.len() != 64
            || blob.byte_size == 0
            || blob.byte_size > MAX_FILE_BYTES
        {
            return Err(BatchFileBlobError::Integrity);
        }
        let root = Arc::clone(&self.batch_files);
        let expected_sha256 = blob.sha256_hex.clone();
        let expected_size = blob.byte_size;
        tokio::task::spawn_blocking(move || {
            read_executor_object_at(&root, file_uuid, &expected_sha256, expected_size)
        })
        .await
        .map_err(|_| BatchFileBlobError::Unavailable)?
        .map_err(map_batch_read_error)
    }

    async fn delete(
        &self,
        file_uuid: Uuid,
        blob: &BatchFileBlob,
    ) -> Result<(), BatchFileBlobError> {
        self.validate_batch_file_namespace()
            .map_err(map_batch_read_error)?;
        if file_uuid.is_nil()
            || blob.storage_backend != FILESYSTEM_BACKEND
            || blob.object_key != batch_file_object_key(file_uuid)
            || blob.sha256_hex.len() != 64
            || blob.byte_size == 0
            || blob.byte_size > MAX_FILE_BYTES
        {
            return Err(BatchFileBlobError::Integrity);
        }
        let root = Arc::clone(&self.batch_files);
        tokio::task::spawn_blocking(move || delete_executor_object_at(&root, file_uuid))
            .await
            .map_err(|_| BatchFileBlobError::Unavailable)?
            .map_err(|_| BatchFileBlobError::Unavailable)
    }
}

fn map_batch_read_error(error: ArtifactReadError) -> BatchFileBlobError {
    match error {
        ArtifactReadError::Integrity => BatchFileBlobError::Integrity,
        ArtifactReadError::Unavailable => BatchFileBlobError::Unavailable,
    }
}

#[async_trait]
impl ArtifactBlobStore for FilesystemArtifactBlobStore {
    fn storage_identity(&self) -> String {
        format!("{FILESYSTEM_BACKEND}:{}", self.root.display())
    }

    async fn put(
        &self,
        identity: ArtifactIdentity,
        bytes: &[u8],
    ) -> Result<ArtifactMetadata, ArtifactWriteError> {
        let object_key = Self::object_key(&identity);
        let (sha256_hex, byte_size) = self.put_blob_key(&object_key, bytes).await?;
        Ok(ArtifactMetadata {
            identity,
            storage_backend: FILESYSTEM_BACKEND.to_string(),
            object_key,
            sha256_hex,
            byte_size,
        })
    }

    async fn get(&self, artifact: &ArtifactMetadata) -> Result<Vec<u8>, ArtifactReadError> {
        if artifact.storage_backend != FILESYSTEM_BACKEND
            || artifact.object_key != Self::object_key(&artifact.identity)
        {
            return Err(ArtifactReadError::Integrity);
        }
        self.get_blob_key(
            &artifact.object_key,
            &artifact.sha256_hex,
            artifact.byte_size,
        )
        .await
    }

    async fn delete(&self, artifact: &ArtifactMetadata) -> Result<(), ArtifactWriteError> {
        if artifact.storage_backend != FILESYSTEM_BACKEND
            || artifact.object_key != Self::object_key(&artifact.identity)
        {
            return Err(ArtifactWriteError::Unavailable);
        }
        self.validate_customer_namespace()
            .map_err(|_| ArtifactWriteError::Unavailable)?;
        let root = Arc::clone(&self.customer_objects);
        let artifact_id = artifact.identity.artifact_id;
        tokio::task::spawn_blocking(move || delete_customer_object_at(&root, artifact_id))
            .await
            .map_err(|_| ArtifactWriteError::Unavailable)?
    }
}

#[async_trait]
impl InputBlobStore for FilesystemArtifactBlobStore {
    fn storage_identity(&self) -> String {
        format!("{FILESYSTEM_BACKEND}:{}", self.root.display())
    }

    async fn put(
        &self,
        key: InputBlobKey,
        bytes: &[u8],
    ) -> Result<InputBlobRef, InputBlobWriteError> {
        let object_key = input_blob_key(&key);
        let (sha256_hex, byte_size) = self
            .put_blob_key(&object_key, bytes)
            .await
            .map_err(|_| InputBlobWriteError::Unavailable)?;
        Ok(InputBlobRef {
            key,
            storage_backend: FILESYSTEM_BACKEND.to_string(),
            object_key,
            sha256_hex,
            byte_size,
        })
    }

    async fn get(&self, blob: &InputBlobRef) -> Result<Vec<u8>, InputBlobReadError> {
        if blob.storage_backend != FILESYSTEM_BACKEND
            || blob.object_key != input_blob_key(&blob.key)
        {
            return Err(InputBlobReadError::Integrity);
        }
        self.get_blob_key(&blob.object_key, &blob.sha256_hex, blob.byte_size)
            .await
            .map_err(|error| match error {
                ArtifactReadError::Unavailable => InputBlobReadError::Unavailable,
                ArtifactReadError::Integrity => InputBlobReadError::Integrity,
            })
    }

    async fn delete(&self, blob: &InputBlobRef) -> Result<(), InputBlobDeleteError> {
        if blob.storage_backend != FILESYSTEM_BACKEND
            || blob.object_key != input_blob_key(&blob.key)
        {
            return Err(InputBlobDeleteError::Unavailable);
        }
        self.delete_blob_key(&blob.object_key)
            .await
            .map_err(|_| InputBlobDeleteError::Unavailable)
    }

    async fn delete_session(&self, admission_session_id: Uuid) -> Result<(), InputBlobDeleteError> {
        let inputs_root = self.root.join("inputs");
        let session_path = inputs_root.join(admission_session_id.simple().to_string());
        match tokio::fs::symlink_metadata(&session_path).await {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(InputBlobDeleteError::Unavailable);
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(InputBlobDeleteError::Unavailable),
        }
        tokio::fs::remove_dir_all(&session_path)
            .await
            .map_err(|_| InputBlobDeleteError::Unavailable)?;
        sync_directory_async(inputs_root)
            .await
            .map_err(|_| InputBlobDeleteError::Unavailable)
    }
}

fn validate_root(root: &Path) -> Result<PathBuf, ImageGatewayError> {
    if !root.is_absolute() {
        return Err(ImageGatewayError::config(
            "GATEWAY_ARTIFACT_ROOT must be an absolute path",
        ));
    }
    let metadata = fs::symlink_metadata(root).map_err(|_| {
        ImageGatewayError::config("GATEWAY_ARTIFACT_ROOT must be an existing directory")
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ImageGatewayError::config(
            "GATEWAY_ARTIFACT_ROOT must be a directory and must not be a symlink",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        // SAFETY: geteuid has no preconditions and does not dereference pointers.
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(ImageGatewayError::config(
                "GATEWAY_ARTIFACT_ROOT must be owned by the service user",
            ));
        }
        if metadata.permissions().mode() & 0o022 != 0 {
            return Err(ImageGatewayError::config(
                "GATEWAY_ARTIFACT_ROOT must not be group or world writable",
            ));
        }
    }
    fs::canonicalize(root)
        .map_err(|_| ImageGatewayError::config("GATEWAY_ARTIFACT_ROOT could not be canonicalized"))
}

fn load_or_create_storage_namespace_id(root: &Path) -> Result<Uuid, ImageGatewayError> {
    let marker = root.join(STORAGE_NAMESPACE_MARKER);
    match read_storage_namespace_id(&marker) {
        Ok(id) => return Ok(id),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(ImageGatewayError::config(
                "artifact storage namespace marker is invalid",
            ));
        }
    }

    let temporary = root.join(format!(
        "{STORAGE_NAMESPACE_MARKER}.tmp-{}",
        Uuid::new_v4().simple()
    ));
    let namespace_id = Uuid::new_v4();
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(&temporary).map_err(|_| {
        ImageGatewayError::config("artifact storage namespace marker could not be created")
    })?;
    file.write_all(format!("{namespace_id}\n").as_bytes())
        .and_then(|_| file.sync_all())
        .map_err(|_| {
            ImageGatewayError::config("artifact storage namespace marker could not be persisted")
        })?;
    drop(file);
    let link_result = fs::hard_link(&temporary, &marker);
    let remove_result = fs::remove_file(&temporary);
    if remove_result.is_err() {
        return Err(ImageGatewayError::config(
            "artifact storage namespace temporary marker could not be removed",
        ));
    }
    match link_result {
        Ok(()) => sync_directory(root).map_err(|_| {
            ImageGatewayError::config("artifact storage namespace marker could not be synchronized")
        })?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => {
            return Err(ImageGatewayError::config(
                "artifact storage namespace marker could not be installed",
            ));
        }
    }
    read_storage_namespace_id(&marker)
        .map_err(|_| ImageGatewayError::config("artifact storage namespace marker is invalid"))
}

fn read_storage_namespace_id(path: &Path) -> std::io::Result<Uuid> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid artifact storage namespace marker",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "artifact storage namespace marker permissions are invalid",
            ));
        }
    }
    let mut text = String::with_capacity(metadata.len() as usize);
    Read::by_ref(&mut file).take(65).read_to_string(&mut text)?;
    Uuid::parse_str(text.trim()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "artifact storage namespace marker is not a UUID",
        )
    })
}

fn set_private_directory_permissions(path: &Path) -> Result<(), ImageGatewayError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|_| {
            ImageGatewayError::config("artifact directory permissions could not be secured")
        })?;
    }
    Ok(())
}

fn prepare_storage_directory(
    path: &Path,
    unavailable_message: &'static str,
    invalid_message: &'static str,
) -> Result<(), ImageGatewayError> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|_| ImageGatewayError::config(unavailable_message))?;
        }
        Err(_) => return Err(ImageGatewayError::config(unavailable_message)),
    }
    let metadata =
        fs::symlink_metadata(path).map_err(|_| ImageGatewayError::config(unavailable_message))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ImageGatewayError::config(invalid_message));
    }
    set_private_directory_permissions(path)
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    fs::File::open(path)?.sync_all()
}

fn sync_artifact_file(file: &fs::File) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: fcntl only reads the valid file descriptor and F_FULLFSYNC takes no pointer.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == 0 {
            return Ok(());
        }
        Err(std::io::Error::last_os_error())
    }
    #[cfg(not(target_os = "macos"))]
    {
        file.sync_all()
    }
}

async fn sync_directory_async(path: PathBuf) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || sync_directory(&path))
        .await
        .map_err(std::io::Error::other)?
}

fn open_regular_no_follow(path: &Path) -> Result<fs::File, ArtifactReadError> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput => {
            ArtifactReadError::Integrity
        }
        _ => ArtifactReadError::Unavailable,
    })
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn pending_artifact_drop_removes_only_its_temporary_link() {
        let directory = tempfile::tempdir().unwrap();
        let temporary = directory.path().join("temporary");
        let object = directory.path().join("object");
        std::fs::write(&temporary, b"artifact").unwrap();
        std::fs::hard_link(&temporary, &object).unwrap();

        let pending = PendingArtifact::new(temporary.clone());
        drop(pending);

        assert!(!temporary.exists());
        assert_eq!(std::fs::read(object).unwrap(), b"artifact");
    }

    #[test]
    fn committed_artifact_is_not_removed() {
        let directory = tempfile::tempdir().unwrap();
        let temporary = directory.path().join("temporary");
        let object = directory.path().join("object");
        std::fs::write(&temporary, b"artifact").unwrap();
        std::fs::hard_link(&temporary, &object).unwrap();
        std::fs::remove_file(&temporary).unwrap();

        let pending = PendingArtifact::new(temporary);
        drop(pending);

        assert_eq!(std::fs::read(object).unwrap(), b"artifact");
    }

    #[tokio::test]
    async fn retained_executor_reference_delete_is_typed_and_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let store = FilesystemArtifactBlobStore::new(root.path()).unwrap();
        let authority_id = Uuid::new_v4();
        let stored = store
            .put_executor_artifact(
                ArtifactIdentity {
                    artifact_id: authority_id,
                    tenant_id: "tenant-retention".to_string(),
                    job_id: Uuid::new_v4(),
                    work_item_id: Uuid::new_v4(),
                    execution_id: Uuid::new_v4(),
                    lease_epoch: 1,
                    output_index: 0,
                    media_type: "image/png".to_string(),
                },
                b"retained executor bytes",
            )
            .await
            .unwrap();
        let namespace = store.executor_storage_namespace().unwrap();
        let reference = ExecutorArtifactReference {
            authority_id,
            storage_backend: &stored.storage_backend,
            storage_namespace: &namespace,
            object_key: &stored.object_key,
            sha256_hex: &stored.sha256_hex,
            byte_size: stored.byte_size,
        };

        store.delete_executor_reference(&reference).await.unwrap();
        store.delete_executor_reference(&reference).await.unwrap();
        assert!(!root.path().join(&stored.object_key).exists());

        let forged_key = "executor-objects/00/forged";
        let forged = ExecutorArtifactReference {
            object_key: forged_key,
            ..reference
        };
        assert!(store.delete_executor_reference(&forged).await.is_err());
    }

    #[test]
    fn executor_namespace_is_stable_across_store_restarts() {
        let root = tempfile::tempdir().unwrap();
        let first = FilesystemArtifactBlobStore::new(root.path())
            .unwrap()
            .executor_storage_namespace()
            .unwrap();
        let second = FilesystemArtifactBlobStore::new(root.path())
            .unwrap()
            .executor_storage_namespace()
            .unwrap();

        assert_eq!(first, second);
        assert!(first.contains("#executor="));
        assert_eq!(
            fs::metadata(root.path().join(STORAGE_NAMESPACE_MARKER))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[tokio::test]
    async fn legacy_executor_namespace_requires_exact_object_integrity_before_delete() {
        let root = tempfile::tempdir().unwrap();
        let store = FilesystemArtifactBlobStore::new(root.path()).unwrap();
        let authority_id = Uuid::new_v4();
        let stored = store
            .put_executor_artifact(
                ArtifactIdentity {
                    artifact_id: authority_id,
                    tenant_id: "tenant-retention".to_string(),
                    job_id: Uuid::new_v4(),
                    work_item_id: Uuid::new_v4(),
                    execution_id: Uuid::new_v4(),
                    lease_epoch: 1,
                    output_index: 0,
                    media_type: "video/mp4".to_string(),
                },
                b"legacy executor bytes",
            )
            .await
            .unwrap();
        let legacy_namespace = format!(
            "{FILESYSTEM_BACKEND}:{}#executor=16777231:42",
            fs::canonicalize(root.path()).unwrap().display()
        );
        let invalid_hash = "0".repeat(64);
        let invalid = ExecutorArtifactReference {
            authority_id,
            storage_backend: &stored.storage_backend,
            storage_namespace: &legacy_namespace,
            object_key: &stored.object_key,
            sha256_hex: &invalid_hash,
            byte_size: stored.byte_size,
        };

        assert!(store.delete_executor_reference(&invalid).await.is_err());
        assert!(root.path().join(&stored.object_key).exists());

        let valid = ExecutorArtifactReference {
            sha256_hex: &stored.sha256_hex,
            ..invalid
        };
        store.delete_executor_reference(&valid).await.unwrap();
        store.delete_executor_reference(&valid).await.unwrap();
        assert!(!root.path().join(&stored.object_key).exists());
    }

    #[tokio::test]
    async fn retained_customer_delete_does_not_acknowledge_a_missing_shard() {
        let root = tempfile::tempdir().unwrap();
        let store = FilesystemArtifactBlobStore::new(root.path()).unwrap();
        let stored = ArtifactBlobStore::put(
            &store,
            ArtifactIdentity {
                artifact_id: Uuid::new_v4(),
                tenant_id: "tenant-retention".to_string(),
                job_id: Uuid::new_v4(),
                work_item_id: Uuid::new_v4(),
                execution_id: Uuid::new_v4(),
                lease_epoch: 1,
                output_index: 0,
                media_type: "image/png".to_string(),
            },
            b"retained customer bytes",
        )
        .await
        .unwrap();
        let object = root.path().join(&stored.object_key);
        let shard = object.parent().unwrap();
        let moved = shard.with_extension("moved");
        fs::rename(shard, &moved).unwrap();

        assert!(ArtifactBlobStore::delete(&store, &stored).await.is_err());
        assert!(moved.join(object.file_name().unwrap()).exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn retained_customer_delete_never_follows_a_replaced_shard() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let root = tempfile::tempdir().unwrap();
        let store = FilesystemArtifactBlobStore::new(root.path()).unwrap();
        let stored = ArtifactBlobStore::put(
            &store,
            ArtifactIdentity {
                artifact_id: Uuid::new_v4(),
                tenant_id: "tenant-retention".to_string(),
                job_id: Uuid::new_v4(),
                work_item_id: Uuid::new_v4(),
                execution_id: Uuid::new_v4(),
                lease_epoch: 1,
                output_index: 0,
                media_type: "image/png".to_string(),
            },
            b"retained customer bytes",
        )
        .await
        .unwrap();
        let object = root.path().join(&stored.object_key);
        let shard = object.parent().unwrap();
        let moved = shard.with_extension("moved");
        fs::rename(shard, &moved).unwrap();
        let outside = root.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o700)).unwrap();
        let outside_object = outside.join(object.file_name().unwrap());
        fs::write(&outside_object, b"must survive").unwrap();
        symlink(&outside, shard).unwrap();

        assert!(ArtifactBlobStore::delete(&store, &stored).await.is_err());
        assert_eq!(fs::read(outside_object).unwrap(), b"must survive");
        assert!(moved.join(object.file_name().unwrap()).exists());
    }

    #[tokio::test]
    async fn retained_executor_delete_does_not_acknowledge_a_missing_shard() {
        let root = tempfile::tempdir().unwrap();
        let store = FilesystemArtifactBlobStore::new(root.path()).unwrap();
        let authority_id = Uuid::new_v4();
        let stored = store
            .put_executor_artifact(
                ArtifactIdentity {
                    artifact_id: authority_id,
                    tenant_id: "tenant-retention".to_string(),
                    job_id: Uuid::new_v4(),
                    work_item_id: Uuid::new_v4(),
                    execution_id: Uuid::new_v4(),
                    lease_epoch: 1,
                    output_index: 0,
                    media_type: "image/png".to_string(),
                },
                b"retained executor bytes",
            )
            .await
            .unwrap();
        let object = root.path().join(&stored.object_key);
        let shard = object.parent().unwrap();
        let moved = shard.with_extension("moved");
        fs::rename(shard, &moved).unwrap();
        let namespace = store.executor_storage_namespace().unwrap();
        let reference = ExecutorArtifactReference {
            authority_id,
            storage_backend: &stored.storage_backend,
            storage_namespace: &namespace,
            object_key: &stored.object_key,
            sha256_hex: &stored.sha256_hex,
            byte_size: stored.byte_size,
        };

        assert!(store.delete_executor_reference(&reference).await.is_err());
        assert!(moved.join(object.file_name().unwrap()).exists());
    }

    #[cfg(unix)]
    #[test]
    fn executor_namespace_symlink_is_rejected() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), root.path().join("executor-objects")).unwrap();

        assert!(FilesystemArtifactBlobStore::new(root.path()).is_err());
    }

    #[tokio::test]
    async fn executor_namespace_replacement_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let store = FilesystemArtifactBlobStore::new(root.path()).unwrap();
        let opened = root.path().join("executor-opened");
        fs::rename(root.path().join("executor-objects"), &opened).unwrap();
        let replacement = root.path().join("executor-objects");
        fs::create_dir(&replacement).unwrap();
        set_private_directory_permissions(&replacement).unwrap();
        let identity = ArtifactIdentity {
            artifact_id: Uuid::new_v4(),
            tenant_id: "tenant-test".to_string(),
            job_id: Uuid::new_v4(),
            work_item_id: Uuid::new_v4(),
            execution_id: Uuid::new_v4(),
            lease_epoch: 1,
            output_index: 0,
            media_type: "image/png".to_string(),
        };

        let artifact_id = identity.artifact_id;
        assert_eq!(
            store
                .put_executor_artifact(identity, b"durable-bytes")
                .await,
            Err(ArtifactWriteError::Unavailable)
        );
        let object_name = artifact_id.simple().to_string();
        let shard_name = object_name[..2].to_string();
        let relative = Path::new(&shard_name).join(object_name);
        assert!(!opened.join(&relative).exists());
        assert!(!replacement.join(relative).exists());
    }

    #[tokio::test]
    async fn executor_staging_namespace_replacement_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let store = FilesystemArtifactBlobStore::new(root.path()).unwrap();
        let opened = root.path().join("staging-opened");
        fs::rename(root.path().join("executor-staging"), &opened).unwrap();
        let replacement = root.path().join("executor-staging");
        fs::create_dir(&replacement).unwrap();
        set_private_directory_permissions(&replacement).unwrap();

        assert!(matches!(
            store.begin_executor_artifact_stage(Uuid::new_v4(), 1).await,
            Err(ExecutorArtifactStageError::Integrity)
        ));
        assert!(fs::read_dir(opened).unwrap().next().is_none());
        assert!(fs::read_dir(replacement).unwrap().next().is_none());
    }
}
