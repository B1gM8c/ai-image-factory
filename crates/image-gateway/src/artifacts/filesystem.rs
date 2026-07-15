use std::{
    fs,
    io::{Read, Write},
    os::fd::AsFd,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use rustix::{
    fs::{self as rfs, AtFlags, FileType, Mode, OFlags, RenameFlags},
    io::Errno,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::{
    ArtifactBlobStore, ArtifactIdentity, ArtifactMetadata, ArtifactReadError, ArtifactWriteError,
    FILESYSTEM_BACKEND, executor_object_key, sha256_hex,
};
use crate::{
    ImageGatewayError,
    input_blobs::{
        InputBlobDeleteError, InputBlobKey, InputBlobReadError, InputBlobRef, InputBlobStore,
        InputBlobWriteError,
    },
};

const MAX_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;

pub struct FilesystemArtifactBlobStore {
    root: PathBuf,
    executor_objects: Arc<fs::File>,
}

struct PendingArtifact {
    temporary: PathBuf,
    object: PathBuf,
    linked: bool,
    committed: bool,
}

impl PendingArtifact {
    fn new(temporary: PathBuf, object: PathBuf) -> Self {
        Self {
            temporary,
            object,
            linked: false,
            committed: false,
        }
    }

    fn mark_linked(&mut self) {
        self.linked = true;
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for PendingArtifact {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _ = fs::remove_file(&self.temporary);
        if self.linked {
            let _ = fs::remove_file(&self.object);
            if let Some(parent) = self.object.parent() {
                let _ = sync_directory(parent);
            }
        }
    }
}

impl FilesystemArtifactBlobStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, ImageGatewayError> {
        let root = validate_root(root.as_ref())?;
        let objects = root.join("objects");
        let executor_objects = root.join("executor-objects");
        let inputs = root.join("inputs");
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
            &inputs,
            "input blob directory is not writable",
            "input blob directory must be a directory and must not be a symlink",
        )?;
        sync_directory(&root)
            .map_err(|_| ImageGatewayError::config("artifact root cannot be synchronized"))?;
        let executor_objects = open_private_directory(&executor_objects).map_err(|_| {
            ImageGatewayError::config("executor artifact directory could not be opened safely")
        })?;
        Ok(Self {
            root,
            executor_objects: Arc::new(executor_objects),
        })
    }

    fn object_key(identity: &ArtifactIdentity) -> String {
        let artifact_id = identity.artifact_id.simple().to_string();
        format!("objects/{}/{}", &artifact_id[..2], artifact_id)
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
        let mut pending = PendingArtifact::new(temporary.clone(), object_path.clone());
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
            Ok(()) => pending.mark_linked(),
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

        pending.commit();
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
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(ArtifactWriteError::Unavailable),
        }
        let parent = path.parent().ok_or(ArtifactWriteError::Unavailable)?;
        sync_directory_async(parent.to_path_buf())
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

    pub(crate) fn executor_storage_namespace(&self) -> Result<String, ArtifactReadError> {
        let current = open_private_directory(&self.root.join("executor-objects"))
            .map_err(|_| ArtifactReadError::Integrity)?;
        let opened_stat = rfs::fstat(self.executor_objects.as_ref())
            .map_err(|_| ArtifactReadError::Unavailable)?;
        let current_stat = rfs::fstat(&current).map_err(|_| ArtifactReadError::Unavailable)?;
        if opened_stat.st_dev != current_stat.st_dev || opened_stat.st_ino != current_stat.st_ino {
            return Err(ArtifactReadError::Integrity);
        }
        Ok(format!(
            "{FILESYSTEM_BACKEND}:{}#executor={}:{}",
            self.root.display(),
            opened_stat.st_dev,
            opened_stat.st_ino
        ))
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
    if file.write_all(bytes).is_err() || rfs::fsync(&file).is_err() {
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

fn delete_executor_object_at(root: &fs::File, artifact_id: Uuid) -> Result<(), ArtifactWriteError> {
    let (shard_name, object_name) = executor_object_names(artifact_id);
    let shard = match open_executor_shard(root, &shard_name) {
        Ok(shard) => shard,
        Err(ArtifactReadError::Integrity) => return Ok(()),
        Err(ArtifactReadError::Unavailable) => return Err(ArtifactWriteError::Unavailable),
    };
    match rfs::unlinkat(&shard, object_name.as_str(), AtFlags::empty()) {
        Ok(()) => rfs::fsync(&shard).map_err(|_| ArtifactWriteError::Unavailable),
        Err(Errno::NOENT) => Ok(()),
        Err(_) => Err(ArtifactWriteError::Unavailable),
    }
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
        self.delete_blob_key(&artifact.object_key).await
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
    use super::*;

    #[test]
    fn pending_artifact_drop_removes_temporary_and_linked_object() {
        let directory = tempfile::tempdir().unwrap();
        let temporary = directory.path().join("temporary");
        let object = directory.path().join("object");
        std::fs::write(&temporary, b"artifact").unwrap();
        std::fs::hard_link(&temporary, &object).unwrap();

        let mut pending = PendingArtifact::new(temporary.clone(), object.clone());
        pending.mark_linked();
        drop(pending);

        assert!(!temporary.exists());
        assert!(!object.exists());
    }

    #[test]
    fn committed_artifact_is_not_removed() {
        let directory = tempfile::tempdir().unwrap();
        let temporary = directory.path().join("temporary");
        let object = directory.path().join("object");
        std::fs::write(&temporary, b"artifact").unwrap();
        std::fs::hard_link(&temporary, &object).unwrap();
        std::fs::remove_file(&temporary).unwrap();

        let mut pending = PendingArtifact::new(temporary, object.clone());
        pending.mark_linked();
        pending.commit();

        assert_eq!(std::fs::read(object).unwrap(), b"artifact");
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
}
