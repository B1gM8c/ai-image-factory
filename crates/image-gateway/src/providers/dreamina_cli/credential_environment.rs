use std::{
    fs,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use sha2::{Digest, Sha256};
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};
use uuid::Uuid;

const KEYCHAIN_PASSWORD_BYTES: usize = 64;
const MAX_KEYCHAIN_BYTES: u64 = 64 * 1024 * 1024;
const SECURITY_TIMEOUT: Duration = Duration::from_secs(15);

pub struct DreaminaKeychainReplacement {
    destination: PathBuf,
    original: Vec<u8>,
}

impl DreaminaKeychainReplacement {
    pub async fn install(
        source_home: &Path,
        destination_home: &Path,
        operation_id: Uuid,
    ) -> Result<Self, DreaminaCredentialEnvironmentError> {
        validate_private_directory(source_home)?;
        validate_private_directory(destination_home)?;
        #[cfg(target_os = "macos")]
        {
            let source = source_home.join("Library/Keychains/login.keychain-db");
            let destination = destination_home.join("Library/Keychains/login.keychain-db");
            let original = read_keychain_file(&destination)?;
            let source_password = read_keychain_password(source_home)?;
            let destination_password = read_keychain_password(destination_home)?;
            let commands = format!(
                "set-keychain-password -o {} -p {} {}\n",
                security_quote(&source_password),
                security_quote(&destination_password),
                security_quote_path(&source)?,
            );
            run_security(source_home, commands.as_bytes()).await?;
            let fresh = read_keychain_file(&source)?;
            atomic_write_keychain(destination_home, &destination, &fresh, operation_id)?;
            Ok(Self {
                destination,
                original,
            })
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = operation_id;
            Err(DreaminaCredentialEnvironmentError::UnsupportedPlatform)
        }
    }

    pub fn rollback(self) -> Result<(), DreaminaCredentialEnvironmentError> {
        let home = self
            .destination
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
        atomic_write_keychain(home, &self.destination, &self.original, Uuid::new_v4())
    }
}

pub fn dreamina_account_isolation_available() -> bool {
    cfg!(target_os = "macos") && Path::new("/usr/bin/security").is_file()
}

pub async fn prepare_dreamina_account_home(
    home: &Path,
) -> Result<(), DreaminaCredentialEnvironmentError> {
    validate_private_directory(home)?;
    #[cfg(target_os = "macos")]
    {
        prepare_macos_keychain(home).await
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = home;
        Err(DreaminaCredentialEnvironmentError::UnsupportedPlatform)
    }
}

pub fn dreamina_credential_fingerprint(
    home: &Path,
) -> Result<String, DreaminaCredentialEnvironmentError> {
    validate_private_directory(home)?;
    let password = read_keychain_password(home)?;
    let mut digest = Sha256::new();
    digest.update(b"ai-image-factory/dreamina-credential-environment/v1\0");
    digest.update(password.as_bytes());
    Ok(hex::encode(digest.finalize()))
}

#[cfg(target_os = "macos")]
async fn prepare_macos_keychain(home: &Path) -> Result<(), DreaminaCredentialEnvironmentError> {
    let keychains = home.join("Library/Keychains");
    create_private_directory(&keychains)?;
    let factory = home.join(".factory");
    create_private_directory(&factory)?;
    let keychain = keychains.join("login.keychain-db");
    let password_path = factory.join("dreamina-keychain-password");

    let keychain_exists = regular_file_exists(&keychain)?;
    let password_exists = regular_file_exists(&password_path)?;
    if keychain_exists != password_exists {
        return Err(DreaminaCredentialEnvironmentError::IncompleteEnvironment);
    }

    if !keychain_exists {
        let password = generate_keychain_password();
        write_secret_file(&password_path, password.as_bytes())?;
        let create = format!(
            "create-keychain -p {} {}\n",
            security_quote(&password),
            security_quote_path(&keychain)?
        );
        if let Err(error) = run_security(home, create.as_bytes()).await {
            let _ = fs::remove_file(&password_path);
            let _ = fs::remove_file(&keychain);
            return Err(error);
        }
    }
    fs::set_permissions(&keychain, fs::Permissions::from_mode(0o600))
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;

    let password = read_keychain_password(home)?;
    let keychain = security_quote_path(&keychain)?;
    let commands = format!(
        "unlock-keychain -p {} {}\nset-keychain-settings {}\n",
        security_quote(&password),
        keychain,
        keychain,
    );
    run_security(home, commands.as_bytes()).await
}

#[cfg(target_os = "macos")]
async fn run_security(
    home: &Path,
    commands: &[u8],
) -> Result<(), DreaminaCredentialEnvironmentError> {
    let mut child = Command::new("/usr/bin/security")
        .arg("-i")
        .env_clear()
        .env("HOME", home)
        .env("PATH", "/usr/bin:/bin")
        .current_dir(home)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or(DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    stdin
        .write_all(commands)
        .await
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    stdin
        .shutdown()
        .await
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    drop(stdin);
    let status = timeout(SECURITY_TIMEOUT, child.wait())
        .await
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    if status.success() {
        Ok(())
    } else {
        Err(DreaminaCredentialEnvironmentError::KeychainUnavailable)
    }
}

fn generate_keychain_password() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn read_keychain_password(home: &Path) -> Result<String, DreaminaCredentialEnvironmentError> {
    let path = home.join(".factory/dreamina-keychain-password");
    let metadata = fs::symlink_metadata(&path)
        .map_err(|_| DreaminaCredentialEnvironmentError::IncompleteEnvironment)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() != KEYCHAIN_PASSWORD_BYTES as u64
    {
        return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
    }
    let value = fs::read_to_string(path)
        .map_err(|_| DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
    if value.len() != KEYCHAIN_PASSWORD_BYTES || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
    }
    Ok(value)
}

fn create_private_directory(path: &Path) -> Result<(), DreaminaCredentialEnvironmentError> {
    fs::create_dir_all(path)
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
    validate_private_directory(path)
}

fn validate_private_directory(path: &Path) -> Result<(), DreaminaCredentialEnvironmentError> {
    if !path.is_absolute() {
        return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
    }
    Ok(())
}

fn regular_file_exists(path: &Path) -> Result<bool, DreaminaCredentialEnvironmentError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(DreaminaCredentialEnvironmentError::InvalidEnvironment),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(DreaminaCredentialEnvironmentError::EnvironmentUnavailable),
    }
}

fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<(), DreaminaCredentialEnvironmentError> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)
}

fn read_keychain_file(path: &Path) -> Result<Vec<u8>, DreaminaCredentialEnvironmentError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > MAX_KEYCHAIN_BYTES
    {
        return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
    }
    fs::read(path).map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)
}

fn atomic_write_keychain(
    home: &Path,
    destination: &Path,
    bytes: &[u8],
    operation_id: Uuid,
) -> Result<(), DreaminaCredentialEnvironmentError> {
    validate_private_directory(home)?;
    let keychains = destination
        .parent()
        .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
    validate_private_directory(keychains)?;
    let temporary = keychains.join(format!(".dreamina-keychain-{}.tmp", operation_id.simple()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
    let result = (|| -> std::io::Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, destination)?;
        fs::File::open(keychains)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
        return Err(DreaminaCredentialEnvironmentError::EnvironmentUnavailable);
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn security_quote_path(path: &Path) -> Result<String, DreaminaCredentialEnvironmentError> {
    let path = path
        .to_str()
        .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
    Ok(security_quote(path))
}

#[cfg(target_os = "macos")]
fn security_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DreaminaCredentialEnvironmentError {
    #[error("Dreamina managed-account isolation is unsupported on this platform")]
    UnsupportedPlatform,
    #[error("Dreamina credential environment is invalid")]
    InvalidEnvironment,
    #[error("Dreamina credential environment is incomplete")]
    IncompleteEnvironment,
    #[error("Dreamina credential environment is unavailable")]
    EnvironmentUnavailable,
    #[error("Dreamina account keychain is unavailable")]
    KeychainUnavailable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_fingerprint_is_stable_and_bound_to_the_home_secret() {
        let home = tempfile::tempdir().unwrap();
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let factory = home.path().join(".factory");
        create_private_directory(&factory).unwrap();
        write_secret_file(
            &factory.join("dreamina-keychain-password"),
            b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap();

        let first = dreamina_credential_fingerprint(home.path()).unwrap();
        let second = dreamina_credential_fingerprint(home.path()).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn security_values_are_single_quoted() {
        assert_eq!(security_quote("a'b"), "'a'\\''b'");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn separate_homes_use_separate_login_keychains() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        fs::set_permissions(first.path(), fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(second.path(), fs::Permissions::from_mode(0o700)).unwrap();
        prepare_dreamina_account_home(first.path()).await.unwrap();
        prepare_dreamina_account_home(second.path()).await.unwrap();
        assert_ne!(
            dreamina_credential_fingerprint(first.path()).unwrap(),
            dreamina_credential_fingerprint(second.path()).unwrap()
        );

        run_security(
            first.path(),
            b"add-generic-password -U -s 'aif-dreamina-test' -a 'profile-a' -w 'isolated'\n",
        )
        .await
        .unwrap();
        let first_read = Command::new("/usr/bin/security")
            .args([
                "find-generic-password",
                "-s",
                "aif-dreamina-test",
                "-wa",
                "profile-a",
            ])
            .env_clear()
            .env("HOME", first.path())
            .output()
            .await
            .unwrap();
        let second_read = Command::new("/usr/bin/security")
            .args([
                "find-generic-password",
                "-s",
                "aif-dreamina-test",
                "-wa",
                "profile-a",
            ])
            .env_clear()
            .env("HOME", second.path())
            .output()
            .await
            .unwrap();
        assert!(first_read.status.success());
        assert_eq!(first_read.stdout, b"isolated\n");
        assert!(!second_read.status.success());

        run_security(
            second.path(),
            b"add-generic-password -U -s 'aif-dreamina-test' -a 'profile-a' -w 'replacement'\n",
        )
        .await
        .unwrap();

        let replacement =
            DreaminaKeychainReplacement::install(second.path(), first.path(), Uuid::new_v4())
                .await
                .unwrap();
        prepare_dreamina_account_home(first.path()).await.unwrap();
        let replaced_read = Command::new("/usr/bin/security")
            .args([
                "find-generic-password",
                "-s",
                "aif-dreamina-test",
                "-wa",
                "profile-a",
            ])
            .env_clear()
            .env("HOME", first.path())
            .output()
            .await
            .unwrap();
        assert!(replaced_read.status.success());
        assert_eq!(replaced_read.stdout, b"replacement\n");

        replacement.rollback().unwrap();
        prepare_dreamina_account_home(first.path()).await.unwrap();
        let restored_read = Command::new("/usr/bin/security")
            .args([
                "find-generic-password",
                "-s",
                "aif-dreamina-test",
                "-wa",
                "profile-a",
            ])
            .env_clear()
            .env("HOME", first.path())
            .output()
            .await
            .unwrap();
        assert!(restored_read.status.success());
        assert_eq!(restored_read.stdout, b"isolated\n");
    }
}
