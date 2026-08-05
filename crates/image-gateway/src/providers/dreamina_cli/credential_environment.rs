use std::{
    fs,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

#[cfg(target_os = "linux")]
use image_provider_dreamina_cli::dreamina_secret_service_bus_address;
use sha2::{Digest, Sha256};
#[cfg(target_os = "linux")]
use std::{ffi::OsString, fs::File};
#[cfg(target_os = "linux")]
use tokio::time::{Instant, sleep};
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};
use uuid::Uuid;

const KEYCHAIN_PASSWORD_BYTES: usize = 64;
const MAX_KEYCHAIN_BYTES: u64 = 64 * 1024 * 1024;
#[cfg(target_os = "macos")]
const SECURITY_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(target_os = "linux")]
const DBUS_DAEMON: &str = "/usr/bin/dbus-daemon";
#[cfg(target_os = "linux")]
const DBUS_SEND: &str = "/usr/bin/dbus-send";
#[cfg(target_os = "linux")]
const GNOME_KEYRING_DAEMON: &str = "/usr/bin/gnome-keyring-daemon";
#[cfg(target_os = "linux")]
const SECRET_SERVICE_START_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(target_os = "linux")]
const SECRET_SERVICE_STOP_TIMEOUT: Duration = Duration::from_secs(3);

#[must_use = "the credential replacement must be committed or rolled back"]
pub struct DreaminaKeychainReplacement {
    destination: PathBuf,
    #[cfg(target_os = "macos")]
    original: Vec<u8>,
    #[cfg(target_os = "linux")]
    backup: Option<PathBuf>,
    #[cfg(target_os = "linux")]
    home: Option<PathBuf>,
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
        #[cfg(target_os = "linux")]
        {
            install_linux_keyring_replacement(source_home, destination_home, operation_id).await
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = operation_id;
            Err(DreaminaCredentialEnvironmentError::UnsupportedPlatform)
        }
    }

    pub fn commit(self) -> Result<(), DreaminaCredentialEnvironmentError> {
        #[cfg(target_os = "linux")]
        {
            let mut replacement = self;
            if let Some(backup) = replacement.backup.take() {
                let parent = backup
                    .parent()
                    .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
                validate_private_directory(parent)?;
                fs::remove_dir_all(&backup)
                    .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
                File::open(parent)
                    .and_then(|directory| directory.sync_all())
                    .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
            }
        }
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), DreaminaCredentialEnvironmentError> {
        #[cfg(target_os = "macos")]
        {
            let home = self
                .destination
                .parent()
                .and_then(Path::parent)
                .and_then(Path::parent)
                .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
            return atomic_write_keychain(home, &self.destination, &self.original, Uuid::new_v4());
        }
        #[cfg(target_os = "linux")]
        {
            let mut replacement = self;
            let backup = replacement
                .backup
                .take()
                .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
            let home = replacement
                .home
                .take()
                .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
            return rollback_linux_keyring_replacement(&home, &replacement.destination, &backup)
                .await;
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        Err(DreaminaCredentialEnvironmentError::UnsupportedPlatform)
    }
}

pub fn dreamina_account_isolation_available() -> bool {
    #[cfg(target_os = "macos")]
    {
        return Path::new("/usr/bin/security").is_file();
    }
    #[cfg(target_os = "linux")]
    {
        return [DBUS_DAEMON, DBUS_SEND, GNOME_KEYRING_DAEMON]
            .iter()
            .all(|path| Path::new(path).is_file());
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    false
}

pub async fn prepare_dreamina_account_home(
    home: &Path,
) -> Result<(), DreaminaCredentialEnvironmentError> {
    validate_private_directory(home)?;
    #[cfg(target_os = "macos")]
    {
        prepare_macos_keychain(home).await
    }
    #[cfg(target_os = "linux")]
    {
        prepare_linux_secret_service(home).await
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = home;
        Err(DreaminaCredentialEnvironmentError::UnsupportedPlatform)
    }
}

pub async fn shutdown_dreamina_account_home(
    home: &Path,
) -> Result<(), DreaminaCredentialEnvironmentError> {
    #[cfg(target_os = "linux")]
    {
        validate_private_directory(home)?;
        prepare_linux_layout(home)?;
        let _lock = acquire_linux_service_lock(home).await?;
        return stop_linux_secret_service(home).await;
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = home;
        Ok(())
    }
}

pub fn seed_dreamina_reauthorization_home(
    fresh_home: &Path,
    destination_home: &Path,
) -> Result<(), DreaminaCredentialEnvironmentError> {
    validate_private_directory(fresh_home)?;
    validate_private_directory(destination_home)?;
    #[cfg(target_os = "linux")]
    {
        let factory = fresh_home.join(".factory");
        create_private_directory(&factory)?;
        let password_path = factory.join("dreamina-keychain-password");
        if regular_file_exists(&password_path)? {
            return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
        }
        let password = read_keychain_password(destination_home)?;
        write_secret_file(&password_path, password.as_bytes())?;
    }
    Ok(())
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

#[cfg(target_os = "linux")]
async fn prepare_linux_secret_service(
    home: &Path,
) -> Result<(), DreaminaCredentialEnvironmentError> {
    if !dreamina_account_isolation_available() {
        return Err(DreaminaCredentialEnvironmentError::KeychainUnavailable);
    }
    prepare_linux_layout(home)?;
    let _lock = acquire_linux_service_lock(home).await?;
    if linux_secret_service_ready(home).await {
        return Ok(());
    }
    stop_linux_secret_service(home).await?;
    start_linux_secret_service(home).await
}

#[cfg(target_os = "linux")]
fn prepare_linux_layout(home: &Path) -> Result<(), DreaminaCredentialEnvironmentError> {
    let factory = home.join(".factory");
    create_private_directory(&factory)?;
    let password_path = factory.join("dreamina-keychain-password");
    if regular_file_exists(&password_path)? {
        read_keychain_password(home)?;
    } else {
        write_secret_file(&password_path, generate_keychain_password().as_bytes())?;
    }

    let local = home.join(".local");
    let share = local.join("share");
    let keyrings = share.join("keyrings");
    create_private_directory(&local)?;
    create_private_directory(&share)?;
    create_private_directory(&keyrings)?;

    let runtime = linux_service_runtime(home);
    create_private_directory(&runtime)?;
    create_private_directory(&runtime.join("control"))?;
    create_private_directory(&runtime.join("xdg"))?;
    Ok(())
}

#[cfg(target_os = "linux")]
async fn acquire_linux_service_lock(
    home: &Path,
) -> Result<File, DreaminaCredentialEnvironmentError> {
    let lock_path = home.join(".factory/dreamina-secret-service.lock");
    let lock = tokio::task::spawn_blocking(move || {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .open(lock_path)
            .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
        let metadata = file
            .metadata()
            .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
        }
        file.lock()
            .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
        Ok(file)
    })
    .await
    .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)??;
    Ok(lock)
}

#[cfg(target_os = "linux")]
async fn start_linux_secret_service(home: &Path) -> Result<(), DreaminaCredentialEnvironmentError> {
    let result = start_linux_secret_service_processes(home).await;
    if result.is_err() {
        let _ = stop_linux_secret_service(home).await;
    }
    result
}

#[cfg(target_os = "linux")]
async fn start_linux_secret_service_processes(
    home: &Path,
) -> Result<(), DreaminaCredentialEnvironmentError> {
    let runtime = linux_service_runtime(home);
    let control = runtime.join("control");
    let xdg_runtime = runtime.join("xdg");
    let bus_address = dreamina_secret_service_bus_address(home);
    let bus_marker = format!("--address={bus_address}");
    let mut bus = Command::new(DBUS_DAEMON);
    bus.args(["--session", "--nofork", "--nopidfile", bus_marker.as_str()])
        .env_clear()
        .env("HOME", home)
        .env("PATH", "/usr/bin:/bin")
        .env("XDG_RUNTIME_DIR", &xdg_runtime)
        .current_dir(home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut bus = bus
        .spawn()
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    let bus_pid = bus
        .id()
        .ok_or(DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    if let Err(error) = write_linux_pid(&runtime.join("dbus.pid"), bus_pid) {
        let _ = bus.kill().await;
        return Err(error);
    }
    drop(bus);

    let control_argument = format!("--control-directory={}", control.to_string_lossy());
    let mut keyring = Command::new(GNOME_KEYRING_DAEMON);
    keyring
        .args([
            "--foreground",
            "--unlock",
            "--components=secrets",
            control_argument.as_str(),
        ])
        .env_clear()
        .env("HOME", home)
        .env("PATH", "/usr/bin:/bin")
        .env("XDG_RUNTIME_DIR", &xdg_runtime)
        .env("DBUS_SESSION_BUS_ADDRESS", &bus_address)
        .current_dir(home)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut keyring = keyring
        .spawn()
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    let keyring_pid = keyring
        .id()
        .ok_or(DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    if let Err(error) = write_linux_pid(&runtime.join("keyring.pid"), keyring_pid) {
        let _ = keyring.kill().await;
        return Err(error);
    }
    let mut stdin = keyring
        .stdin
        .take()
        .ok_or(DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    let mut unlock = read_keychain_password(home)?.into_bytes();
    unlock.push(b'\n');
    stdin
        .write_all(&unlock)
        .await
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    stdin
        .shutdown()
        .await
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    drop(stdin);
    drop(keyring);

    let deadline = Instant::now() + SECRET_SERVICE_START_TIMEOUT;
    while Instant::now() < deadline {
        if linux_secret_service_ready(home).await {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(DreaminaCredentialEnvironmentError::KeychainUnavailable)
}

#[cfg(target_os = "linux")]
async fn linux_secret_service_ready(home: &Path) -> bool {
    let runtime = linux_service_runtime(home);
    let bus_address = dreamina_secret_service_bus_address(home);
    let bus_marker = format!("--address={bus_address}");
    let control_marker = format!(
        "--control-directory={}",
        runtime.join("control").to_string_lossy()
    );
    let bus_running = read_linux_pid(&runtime.join("dbus.pid"))
        .ok()
        .flatten()
        .is_some_and(|pid| linux_process_matches(pid, DBUS_DAEMON, &bus_marker));
    let keyring_running = read_linux_pid(&runtime.join("keyring.pid"))
        .ok()
        .flatten()
        .is_some_and(|pid| linux_process_matches(pid, GNOME_KEYRING_DAEMON, &control_marker));
    if !bus_running || !keyring_running {
        return false;
    }
    let status = Command::new(DBUS_SEND)
        .args([
            "--session",
            "--print-reply",
            "--reply-timeout=1000",
            "--dest=org.freedesktop.secrets",
            "/org/freedesktop/secrets",
            "org.freedesktop.DBus.Peer.Ping",
        ])
        .env_clear()
        .env("HOME", home)
        .env("PATH", "/usr/bin:/bin")
        .env("DBUS_SESSION_BUS_ADDRESS", bus_address)
        .current_dir(home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    matches!(timeout(Duration::from_secs(2), status).await, Ok(Ok(status)) if status.success())
}

#[cfg(target_os = "linux")]
async fn stop_linux_secret_service(home: &Path) -> Result<(), DreaminaCredentialEnvironmentError> {
    let runtime = linux_service_runtime(home);
    let control_marker = format!(
        "--control-directory={}",
        runtime.join("control").to_string_lossy()
    );
    stop_linux_process(
        &runtime.join("keyring.pid"),
        GNOME_KEYRING_DAEMON,
        &control_marker,
    )
    .await?;
    let bus_marker = format!("--address={}", dreamina_secret_service_bus_address(home));
    stop_linux_process(&runtime.join("dbus.pid"), DBUS_DAEMON, &bus_marker).await?;
    Ok(())
}

#[cfg(target_os = "linux")]
async fn stop_linux_process(
    pid_path: &Path,
    executable: &str,
    marker: &str,
) -> Result<(), DreaminaCredentialEnvironmentError> {
    let Some(pid) = read_linux_pid(pid_path)? else {
        return Ok(());
    };
    if linux_process_matches(pid, executable, marker) {
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + SECRET_SERVICE_STOP_TIMEOUT;
        while Instant::now() < deadline && Path::new(&format!("/proc/{pid}")).exists() {
            sleep(Duration::from_millis(50)).await;
        }
        if linux_process_matches(pid, executable, marker) {
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGKILL);
            }
        }
    }
    match fs::remove_file(pid_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(DreaminaCredentialEnvironmentError::EnvironmentUnavailable),
    }
}

#[cfg(target_os = "linux")]
fn linux_process_matches(pid: u32, executable: &str, marker: &str) -> bool {
    let status = match fs::read_to_string(format!("/proc/{pid}/status")) {
        Ok(status) => status,
        Err(_) => return false,
    };
    let effective_uid = unsafe { libc::geteuid() };
    let Some(uid_line) = status.lines().find(|line| line.starts_with("Uid:")) else {
        return false;
    };
    let mut uids = uid_line.split_ascii_whitespace().skip(1);
    let real_uid = uids.next().and_then(|value| value.parse::<u32>().ok());
    let process_effective_uid = uids.next().and_then(|value| value.parse::<u32>().ok());
    if real_uid != Some(effective_uid) || process_effective_uid != Some(effective_uid) {
        return false;
    }
    let command = match fs::read(format!("/proc/{pid}/cmdline")) {
        Ok(command) => command,
        Err(_) => return false,
    };
    let arguments: Vec<_> = command
        .split(|byte| *byte == 0)
        .filter(|value| !value.is_empty())
        .collect();
    arguments
        .first()
        .is_some_and(|value| *value == executable.as_bytes())
        && arguments.iter().any(|value| *value == marker.as_bytes())
}

#[cfg(target_os = "linux")]
fn read_linux_pid(path: &Path) -> Result<Option<u32>, DreaminaCredentialEnvironmentError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(DreaminaCredentialEnvironmentError::EnvironmentUnavailable),
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() > 32
    {
        return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
    }
    let value = fs::read_to_string(path)
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
    value
        .trim()
        .parse::<u32>()
        .map(Some)
        .map_err(|_| DreaminaCredentialEnvironmentError::InvalidEnvironment)
}

#[cfg(target_os = "linux")]
fn write_linux_pid(path: &Path, pid: u32) -> Result<(), DreaminaCredentialEnvironmentError> {
    let parent = path
        .parent()
        .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
    validate_private_directory(parent)?;
    let temporary = parent.join(format!(".pid-{}.tmp", Uuid::new_v4().simple()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
    let result = (|| -> std::io::Result<()> {
        writeln!(file, "{pid}")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
        return Err(DreaminaCredentialEnvironmentError::EnvironmentUnavailable);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_service_runtime(home: &Path) -> PathBuf {
    home.join(".factory/dreamina-secret-service")
}

#[cfg(target_os = "linux")]
struct LinuxKeyringFile {
    name: OsString,
    bytes: Vec<u8>,
}

#[cfg(target_os = "linux")]
async fn install_linux_keyring_replacement(
    source_home: &Path,
    destination_home: &Path,
    operation_id: Uuid,
) -> Result<DreaminaKeychainReplacement, DreaminaCredentialEnvironmentError> {
    prepare_linux_secret_service(source_home).await?;
    prepare_linux_secret_service(destination_home).await?;
    if read_keychain_password(source_home)? != read_keychain_password(destination_home)? {
        return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
    }
    shutdown_dreamina_account_home(source_home).await?;
    let source = source_home.join(".local/share/keyrings");
    let snapshot = read_linux_keyring_snapshot(&source)?;

    let _lock = acquire_linux_service_lock(destination_home).await?;
    stop_linux_secret_service(destination_home).await?;
    let destination = destination_home.join(".local/share/keyrings");
    let backup = replace_linux_keyring_directory(&destination, &snapshot, operation_id)?;
    if let Err(error) = start_linux_secret_service(destination_home).await {
        let _ = restore_linux_keyring_directory(&destination, &backup);
        let _ = start_linux_secret_service(destination_home).await;
        return Err(error);
    }
    Ok(DreaminaKeychainReplacement {
        destination,
        backup: Some(backup),
        home: Some(destination_home.to_path_buf()),
    })
}

#[cfg(target_os = "linux")]
async fn rollback_linux_keyring_replacement(
    home: &Path,
    destination: &Path,
    backup: &Path,
) -> Result<(), DreaminaCredentialEnvironmentError> {
    let _lock = acquire_linux_service_lock(home).await?;
    stop_linux_secret_service(home).await?;
    restore_linux_keyring_directory(destination, backup)?;
    start_linux_secret_service(home).await
}

#[cfg(target_os = "linux")]
fn read_linux_keyring_snapshot(
    directory: &Path,
) -> Result<Vec<LinuxKeyringFile>, DreaminaCredentialEnvironmentError> {
    validate_private_directory(directory)?;
    let mut files = Vec::new();
    let mut total = 0_u64;
    let entries = fs::read_dir(directory)
        .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
    for entry in entries {
        let entry = entry.map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
        let metadata = entry
            .metadata()
            .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?;
        if !metadata.is_file()
            || entry
                .file_type()
                .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?
                .is_symlink()
            || metadata.permissions().mode() & 0o077 != 0
            || metadata.len() == 0
        {
            return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
        }
        total = total
            .checked_add(metadata.len())
            .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
        if total > MAX_KEYCHAIN_BYTES {
            return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
        }
        files.push(LinuxKeyringFile {
            name: entry.file_name(),
            bytes: fs::read(entry.path())
                .map_err(|_| DreaminaCredentialEnvironmentError::KeychainUnavailable)?,
        });
    }
    if !files
        .iter()
        .any(|file| file.name.as_os_str() == "login.keyring")
    {
        return Err(DreaminaCredentialEnvironmentError::IncompleteEnvironment);
    }
    Ok(files)
}

#[cfg(target_os = "linux")]
fn replace_linux_keyring_directory(
    destination: &Path,
    snapshot: &[LinuxKeyringFile],
    operation_id: Uuid,
) -> Result<PathBuf, DreaminaCredentialEnvironmentError> {
    validate_private_directory(destination)?;
    let parent = destination
        .parent()
        .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
    validate_private_directory(parent)?;
    let suffix = operation_id.simple();
    let temporary = parent.join(format!(".dreamina-keyrings-{suffix}.tmp"));
    let backup = parent.join(format!(".dreamina-keyrings-{suffix}.bak"));
    if temporary.exists() || backup.exists() {
        return Err(DreaminaCredentialEnvironmentError::InvalidEnvironment);
    }
    create_private_directory(&temporary)?;
    let write_result = (|| -> Result<(), DreaminaCredentialEnvironmentError> {
        for keyring in snapshot {
            write_secret_file(&temporary.join(&keyring.name), &keyring.bytes)?;
        }
        File::open(&temporary)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)
    })();
    if write_result.is_err() {
        let _ = fs::remove_dir_all(&temporary);
        return Err(DreaminaCredentialEnvironmentError::EnvironmentUnavailable);
    }
    if fs::rename(destination, &backup).is_err() {
        let _ = fs::remove_dir_all(&temporary);
        return Err(DreaminaCredentialEnvironmentError::EnvironmentUnavailable);
    }
    if fs::rename(&temporary, destination).is_err() {
        let _ = fs::rename(&backup, destination);
        let _ = fs::remove_dir_all(&temporary);
        return Err(DreaminaCredentialEnvironmentError::EnvironmentUnavailable);
    }
    if File::open(parent)
        .and_then(|directory| directory.sync_all())
        .is_err()
    {
        let _ = fs::remove_dir_all(destination);
        let _ = fs::rename(&backup, destination);
        let _ = File::open(parent).and_then(|directory| directory.sync_all());
        return Err(DreaminaCredentialEnvironmentError::EnvironmentUnavailable);
    }
    Ok(backup)
}

#[cfg(target_os = "linux")]
fn restore_linux_keyring_directory(
    destination: &Path,
    backup: &Path,
) -> Result<(), DreaminaCredentialEnvironmentError> {
    let parent = destination
        .parent()
        .ok_or(DreaminaCredentialEnvironmentError::InvalidEnvironment)?;
    validate_private_directory(parent)?;
    validate_private_directory(destination)?;
    validate_private_directory(backup)?;
    fs::remove_dir_all(destination)
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
    fs::rename(backup, destination)
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| DreaminaCredentialEnvironmentError::EnvironmentUnavailable)
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

        replacement.rollback().await.unwrap();
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

    #[cfg(target_os = "linux")]
    async fn run_secret_tool(
        home: &Path,
        arguments: &[&str],
        stdin: &[u8],
    ) -> std::process::Output {
        let mut command = Command::new("/usr/bin/secret-tool");
        command
            .args(arguments)
            .env_clear()
            .env("HOME", home)
            .env("PATH", "/usr/bin:/bin")
            .env(
                "DBUS_SESSION_BUS_ADDRESS",
                dreamina_secret_service_bus_address(home),
            )
            .current_dir(home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        if !stdin.is_empty() {
            child.stdin.take().unwrap().write_all(stdin).await.unwrap();
        }
        child.wait_with_output().await.unwrap()
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_secret_service_isolates_persists_replaces_and_rolls_back_credentials() {
        if !dreamina_account_isolation_available() || !Path::new("/usr/bin/secret-tool").is_file() {
            return;
        }
        let destination = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        for home in [destination.path(), source.path(), other.path()] {
            fs::set_permissions(home, fs::Permissions::from_mode(0o700)).unwrap();
        }

        prepare_dreamina_account_home(destination.path())
            .await
            .unwrap();
        let stored = run_secret_tool(
            destination.path(),
            &[
                "store",
                "--label=aif-test",
                "service",
                "aif-test",
                "account",
                "primary",
            ],
            b"original",
        )
        .await;
        assert!(stored.status.success());

        seed_dreamina_reauthorization_home(source.path(), destination.path()).unwrap();
        prepare_dreamina_account_home(source.path()).await.unwrap();
        let stored = run_secret_tool(
            source.path(),
            &[
                "store",
                "--label=aif-test",
                "service",
                "aif-test",
                "account",
                "primary",
            ],
            b"replacement",
        )
        .await;
        assert!(stored.status.success());
        assert_eq!(
            dreamina_credential_fingerprint(source.path()).unwrap(),
            dreamina_credential_fingerprint(destination.path()).unwrap()
        );

        prepare_dreamina_account_home(other.path()).await.unwrap();
        let isolated = run_secret_tool(
            other.path(),
            &["lookup", "service", "aif-test", "account", "primary"],
            b"",
        )
        .await;
        assert!(!isolated.status.success());
        assert!(isolated.stdout.is_empty());

        let replacement =
            DreaminaKeychainReplacement::install(source.path(), destination.path(), Uuid::new_v4())
                .await
                .unwrap();
        let replaced = run_secret_tool(
            destination.path(),
            &["lookup", "service", "aif-test", "account", "primary"],
            b"",
        )
        .await;
        assert!(replaced.status.success());
        assert_eq!(replaced.stdout, b"replacement");

        replacement.rollback().await.unwrap();
        let restored = run_secret_tool(
            destination.path(),
            &["lookup", "service", "aif-test", "account", "primary"],
            b"",
        )
        .await;
        assert!(restored.status.success());
        assert_eq!(restored.stdout, b"original");

        shutdown_dreamina_account_home(destination.path())
            .await
            .unwrap();
        shutdown_dreamina_account_home(source.path()).await.unwrap();
        shutdown_dreamina_account_home(other.path()).await.unwrap();
    }
}
