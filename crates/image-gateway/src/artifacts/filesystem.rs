use std::{
    fs,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::{
    ArtifactBlobStore, ArtifactIdentity, ArtifactMetadata, ArtifactReadError, ArtifactWriteError,
    FILESYSTEM_BACKEND, sha256_hex,
};
use crate::ImageGatewayError;

const MAX_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;

pub struct FilesystemArtifactBlobStore {
    root: PathBuf,
}

impl FilesystemArtifactBlobStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, ImageGatewayError> {
        let root = validate_root(root.as_ref())?;
        let objects = root.join("objects");
        fs::create_dir_all(&objects)
            .map_err(|_| ImageGatewayError::config("artifact object directory is not writable"))?;
        set_private_directory_permissions(&objects)?;
        sync_directory(&root)
            .map_err(|_| ImageGatewayError::config("artifact root cannot be synchronized"))?;
        Ok(Self { root })
    }

    fn object_key(identity: &ArtifactIdentity) -> String {
        let artifact_id = identity.artifact_id.simple().to_string();
        format!("objects/{}/{}", &artifact_id[..2], artifact_id)
    }

    fn object_path(&self, identity: &ArtifactIdentity) -> PathBuf {
        self.root.join(Self::object_key(identity))
    }
}

#[async_trait]
impl ArtifactBlobStore for FilesystemArtifactBlobStore {
    async fn put(
        &self,
        identity: ArtifactIdentity,
        bytes: &[u8],
    ) -> Result<ArtifactMetadata, ArtifactWriteError> {
        if bytes.is_empty() || bytes.len() as u64 > MAX_ARTIFACT_BYTES {
            return Err(ArtifactWriteError::Unavailable);
        }
        let object_key = Self::object_key(&identity);
        let object_path = self.object_path(&identity);
        let parent = object_path
            .parent()
            .ok_or(ArtifactWriteError::Unavailable)?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|_| ArtifactWriteError::Unavailable)?;
        set_private_directory_permissions(parent).map_err(|_| ArtifactWriteError::Unavailable)?;

        let temporary = parent.join(format!(".tmp-{}", Uuid::new_v4().simple()));
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
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(ArtifactWriteError::Unavailable);
        }

        if tokio::fs::hard_link(&temporary, &object_path)
            .await
            .is_err()
        {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(ArtifactWriteError::Unavailable);
        }
        if tokio::fs::remove_file(&temporary).await.is_err()
            || sync_directory_async(parent.to_path_buf()).await.is_err()
        {
            return Err(ArtifactWriteError::Unavailable);
        }

        Ok(ArtifactMetadata {
            identity,
            storage_backend: FILESYSTEM_BACKEND.to_string(),
            object_key,
            sha256_hex: sha256_hex(bytes),
            byte_size: bytes.len() as u64,
        })
    }

    async fn get(&self, artifact: &ArtifactMetadata) -> Result<Vec<u8>, ArtifactReadError> {
        if artifact.storage_backend != FILESYSTEM_BACKEND
            || artifact.object_key != Self::object_key(&artifact.identity)
        {
            return Err(ArtifactReadError::Integrity);
        }
        if artifact.byte_size == 0 || artifact.byte_size > MAX_ARTIFACT_BYTES {
            return Err(ArtifactReadError::Integrity);
        }
        let path = self.object_path(&artifact.identity);
        let std_file = open_regular_no_follow(&path)?;
        let metadata = std_file
            .metadata()
            .map_err(|_| ArtifactReadError::Unavailable)?;
        if !metadata.is_file() || metadata.len() != artifact.byte_size {
            return Err(ArtifactReadError::Integrity);
        }
        let capacity =
            usize::try_from(artifact.byte_size).map_err(|_| ArtifactReadError::Integrity)?;
        let mut file = tokio::fs::File::from_std(std_file).take(artifact.byte_size + 1);
        let mut bytes = Vec::with_capacity(capacity);
        file.read_to_end(&mut bytes)
            .await
            .map_err(|_| ArtifactReadError::Unavailable)?;
        if bytes.len() as u64 != artifact.byte_size || sha256_hex(&bytes) != artifact.sha256_hex {
            return Err(ArtifactReadError::Integrity);
        }
        Ok(bytes)
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
