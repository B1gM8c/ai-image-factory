use std::{
    fs,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use toml::{Table, Value};

use crate::provider_uploads::GrokVideoOutputS3Configuration;
use crate::runner::process::sha256;

pub(super) const AUTH_FILE: &str = "auth.json";
pub(super) const MAX_AUTH_BYTES: u64 = 1024 * 1024;
const CONFIG_FILE: &str = "config.toml";
const MAX_CONFIG_BYTES: u64 = 256 * 1024;

pub(crate) fn auth_file_sha256(home: &Path) -> std::io::Result<String> {
    if !home.is_absolute() {
        return Err(invalid_auth());
    }
    read_private_auth(&home.join(AUTH_FILE)).map(|bytes| sha256(&bytes))
}

pub(crate) fn read_verified_auth(home: &Path, expected_sha256: &str) -> std::io::Result<Vec<u8>> {
    if !home.is_absolute() || !is_sha256(expected_sha256) {
        return Err(invalid_auth());
    }
    let bytes = read_private_auth(&home.join(AUTH_FILE))?;
    ensure_auth_digest(&bytes, expected_sha256)?;
    Ok(bytes)
}

pub(super) fn read_verified_bearer(
    home: &Path,
    expected_sha256: &str,
    minimum_valid_for: std::time::Duration,
) -> std::io::Result<String> {
    let bytes = read_verified_auth(home, expected_sha256)?;
    let document: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| invalid_auth())?;
    let entries = document.as_object().ok_or_else(invalid_auth)?;
    let now = OffsetDateTime::now_utc();
    let minimum_valid_for =
        time::Duration::try_from(minimum_valid_for).map_err(|_| invalid_auth())?;
    let mut eligible = entries
        .iter()
        .filter_map(|(authority, entry)| {
            let entry = entry.as_object()?;
            let issuer = entry.get("oidc_issuer")?.as_str()?;
            if !authority.starts_with("https://auth.x.ai") || issuer != "https://auth.x.ai" {
                return None;
            }
            let key = entry.get("key")?.as_str()?;
            if key.trim().is_empty() || key.len() > 32 * 1024 || key.contains(['\r', '\n', '\0']) {
                return None;
            }
            let expires_at = entry
                .get("expires_at")?
                .as_str()
                .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())?;
            if expires_at <= now + minimum_valid_for {
                return None;
            }
            Some((expires_at, key.to_owned()))
        })
        .collect::<Vec<_>>();
    eligible.sort_unstable_by_key(|(expires_at, _)| *expires_at);
    eligible.pop().map(|(_, key)| key).ok_or_else(invalid_auth)
}

pub(super) fn read_isolated_grok_video_output(
    home: &Path,
) -> std::io::Result<GrokVideoOutputS3Configuration> {
    let output = read_grok_video_output(home)?.ok_or_else(invalid_auth)?;
    let credentials = output
        .get("read_write")
        .and_then(Value::as_table)
        .ok_or_else(invalid_auth)?;
    let text = |table: &Table, name: &str| {
        table
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|value| !value.trim().is_empty() && !value.contains('\0'))
            .ok_or_else(invalid_auth)
    };
    let expires_secs = output
        .get("expires_secs")
        .and_then(Value::as_integer)
        .filter(|value| (60..=3_600).contains(value))
        .ok_or_else(invalid_auth)?;
    Ok(GrokVideoOutputS3Configuration {
        bucket: text(&output, "bucket")?,
        region: text(&output, "region")?,
        endpoint: text(&output, "endpoint")?,
        key_prefix: text(&output, "key_prefix")?,
        expires_secs,
        access_key_id: text(credentials, "access_key_id")?,
        secret_access_key: text(credentials, "secret_access_key")?,
    })
}

pub(super) fn validate_auth_source(home: &Path, expected_sha256: &str) -> std::io::Result<PathBuf> {
    if !home.is_absolute() || !is_sha256(expected_sha256) {
        return Err(invalid_auth());
    }
    let source = home.join(AUTH_FILE);
    let bytes = read_private_auth(&source)?;
    ensure_auth_digest(&bytes, expected_sha256)?;
    Ok(source)
}

pub(super) fn prepare_isolated_auth(
    destination_home: &Path,
    source: &Path,
    expected_sha256: &str,
) -> std::io::Result<()> {
    let destination = destination_home.join(AUTH_FILE);
    match fs::symlink_metadata(&destination) {
        Ok(metadata) => {
            validate_private_file_metadata(&metadata)?;
            let bytes = read_private_auth(&destination)?;
            return ensure_auth_digest(&bytes, expected_sha256);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let bytes = read_private_auth(source)?;
    ensure_auth_digest(&bytes, expected_sha256)?;
    let mut destination_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&destination)?;
    destination_file.write_all(&bytes)?;
    destination_file.sync_all()?;
    fs::File::open(destination_home)?.sync_all()
}

pub(super) fn prepare_isolated_grok_config(
    destination_home: &Path,
    source_home: &Path,
) -> std::io::Result<bool> {
    if !destination_home.is_absolute() || !source_home.is_absolute() {
        return Err(invalid_auth());
    }
    let Some(output) = read_grok_video_output(source_home)? else {
        return Ok(false);
    };
    write_isolated_grok_video_output(destination_home, output)?;
    Ok(true)
}

pub(super) fn prepare_isolated_grok_fallback_config(
    destination_home: &Path,
    configuration: &GrokVideoOutputS3Configuration,
) -> std::io::Result<()> {
    let mut credentials = Table::new();
    credentials.insert(
        "access_key_id".to_owned(),
        Value::String(configuration.access_key_id.clone()),
    );
    credentials.insert(
        "secret_access_key".to_owned(),
        Value::String(configuration.secret_access_key.clone()),
    );
    let mut output = Table::new();
    output.insert(
        "bucket".to_owned(),
        Value::String(configuration.bucket.clone()),
    );
    output.insert(
        "region".to_owned(),
        Value::String(configuration.region.clone()),
    );
    output.insert(
        "endpoint".to_owned(),
        Value::String(configuration.endpoint.clone()),
    );
    output.insert(
        "key_prefix".to_owned(),
        Value::String(configuration.key_prefix.clone()),
    );
    output.insert(
        "expires_secs".to_owned(),
        Value::Integer(configuration.expires_secs),
    );
    output.insert("read_write".to_owned(), Value::Table(credentials));
    write_isolated_grok_video_output(destination_home, output)
}

fn write_isolated_grok_video_output(destination_home: &Path, output: Table) -> std::io::Result<()> {
    let mut tools = Table::new();
    tools.insert("zdr_video_output_s3".to_owned(), Value::Table(output));
    let mut document = Table::new();
    document.insert("tools".to_owned(), Value::Table(tools));
    let mut contents = toml::to_string_pretty(&document).map_err(|_| invalid_auth())?;
    if !contents.ends_with('\n') {
        contents.push('\n');
    }
    write_private_file_if_absent_or_equal(
        destination_home,
        &destination_home.join(CONFIG_FILE),
        contents.as_bytes(),
        MAX_CONFIG_BYTES,
    )
}

fn read_grok_video_output(source_home: &Path) -> std::io::Result<Option<Table>> {
    let path = source_home.join(CONFIG_FILE);
    let mut file = match open_owned_read_only_file(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let size = file.metadata()?.len();
    if size == 0 || size > MAX_CONFIG_BYTES {
        return Err(invalid_auth());
    }
    let mut bytes = Vec::with_capacity(size as usize);
    Read::by_ref(&mut file)
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size {
        return Err(invalid_auth());
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| invalid_auth())?;
    let document = text.parse::<Table>().map_err(|_| invalid_auth())?;
    Ok(document
        .get("tools")
        .and_then(Value::as_table)
        .and_then(|tools| tools.get("zdr_video_output_s3"))
        .and_then(Value::as_table)
        .cloned())
}

fn write_private_file_if_absent_or_equal(
    destination_home: &Path,
    destination: &Path,
    bytes: &[u8],
    max_bytes: u64,
) -> std::io::Result<()> {
    if bytes.is_empty() || bytes.len() as u64 > max_bytes {
        return Err(invalid_auth());
    }
    match fs::symlink_metadata(destination) {
        Ok(metadata) => {
            validate_private_file_metadata(&metadata)?;
            let existing = read_bounded_private_file(destination, max_bytes)?;
            if existing == bytes {
                return Ok(());
            }
            return Err(invalid_auth());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut destination_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(destination)?;
    destination_file.write_all(bytes)?;
    destination_file.sync_all()?;
    fs::File::open(destination_home)?.sync_all()
}

fn read_private_auth(path: &Path) -> std::io::Result<Vec<u8>> {
    read_bounded_private_file(path, MAX_AUTH_BYTES)
}

fn read_bounded_private_file(path: &Path, max_bytes: u64) -> std::io::Result<Vec<u8>> {
    let mut file = open_private_file(path)?;
    let size = file.metadata()?.len();
    if size == 0 || size > max_bytes {
        return Err(invalid_auth());
    }
    let mut bytes = Vec::with_capacity(size as usize);
    Read::by_ref(&mut file)
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size {
        return Err(invalid_auth());
    }
    Ok(bytes)
}

fn ensure_auth_digest(bytes: &[u8], expected_sha256: &str) -> std::io::Result<()> {
    if sha256(bytes) == expected_sha256 {
        Ok(())
    } else {
        Err(invalid_auth())
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn open_private_file(path: &Path) -> std::io::Result<fs::File> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    validate_private_file_metadata(&file.metadata()?)?;
    Ok(file)
}

fn open_owned_read_only_file(path: &Path) -> std::io::Result<fs::File> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let metadata = file.metadata()?;
    if metadata.is_file()
        && metadata.nlink() == 1
        && metadata.uid() == unsafe { libc::geteuid() }
        && metadata.permissions().mode() & 0o022 == 0
    {
        Ok(file)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "owned configuration file validation failed",
        ))
    }
}

fn validate_private_file_metadata(metadata: &fs::Metadata) -> std::io::Result<()> {
    if metadata.is_file()
        && metadata.nlink() == 1
        && metadata.uid() == unsafe { libc::geteuid() }
        && metadata.permissions().mode() & 0o077 == 0
    {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "private auth file validation failed",
        ))
    }
}

fn invalid_auth() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid private auth file")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn private_directory(path: &Path) {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn isolated_grok_config_projects_only_managed_video_output() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        private_directory(&source);
        private_directory(&destination);
        fs::write(
            source.join(CONFIG_FILE),
            r#"
[marketplace]
enabled = true

[tools.zdr_video_output_s3]
bucket = "video-output"
region = "z2"
endpoint = "https://s3-z2.qiniucs.com"
key_prefix = "grok-videos"
expires_secs = 900

[tools.zdr_video_output_s3.read_write]
access_key_id = "ak"
secret_access_key = "sk"
"#,
        )
        .unwrap();
        fs::set_permissions(source.join(CONFIG_FILE), fs::Permissions::from_mode(0o600)).unwrap();

        assert!(prepare_isolated_grok_config(&destination, &source).unwrap());

        let projected = fs::read_to_string(destination.join(CONFIG_FILE)).unwrap();
        assert!(!projected.contains("marketplace"));
        assert!(projected.contains("[tools.zdr_video_output_s3]"));
        assert!(projected.contains("[tools.zdr_video_output_s3.read_write]"));
        assert_eq!(
            fs::metadata(destination.join(CONFIG_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(prepare_isolated_grok_config(&destination, &source).unwrap());
    }

    #[test]
    fn isolated_grok_config_ignores_unrelated_account_config() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        private_directory(&source);
        private_directory(&destination);
        fs::write(source.join(CONFIG_FILE), "[marketplace]\nenabled = true\n").unwrap();
        fs::set_permissions(source.join(CONFIG_FILE), fs::Permissions::from_mode(0o644)).unwrap();

        assert!(!prepare_isolated_grok_config(&destination, &source).unwrap());

        assert!(!destination.join(CONFIG_FILE).exists());
    }

    #[test]
    fn isolated_grok_fallback_projects_only_ephemeral_video_output() {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("destination");
        private_directory(&destination);
        let configuration = GrokVideoOutputS3Configuration {
            bucket: "aif-provider-uploads".to_owned(),
            region: "auto".to_owned(),
            endpoint: "https://uploads.example.com/v1/internal/provider-uploads/s3".to_owned(),
            key_prefix: "a".repeat(32),
            expires_secs: 900,
            access_key_id: format!("AIF{}", "B".repeat(32)),
            secret_access_key: "c".repeat(64),
        };

        prepare_isolated_grok_fallback_config(&destination, &configuration).unwrap();

        let projected = fs::read_to_string(destination.join(CONFIG_FILE)).unwrap();
        let document = projected.parse::<Table>().unwrap();
        assert_eq!(
            document["tools"]["zdr_video_output_s3"]["endpoint"].as_str(),
            Some(configuration.endpoint.as_str())
        );
        assert_eq!(
            document["tools"]["zdr_video_output_s3"]["key_prefix"].as_str(),
            Some(configuration.key_prefix.as_str())
        );
        assert_eq!(
            document["tools"]["zdr_video_output_s3"]["read_write"]["access_key_id"].as_str(),
            Some(configuration.access_key_id.as_str())
        );
        assert_eq!(
            fs::metadata(destination.join(CONFIG_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn isolated_grok_config_rejects_changed_retry_projection() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        private_directory(&source);
        private_directory(&destination);
        fs::write(
            source.join(CONFIG_FILE),
            "[tools.zdr_video_output_s3]\nbucket = \"first\"\n",
        )
        .unwrap();
        fs::set_permissions(source.join(CONFIG_FILE), fs::Permissions::from_mode(0o600)).unwrap();
        prepare_isolated_grok_config(&destination, &source).unwrap();
        fs::write(
            source.join(CONFIG_FILE),
            "[tools.zdr_video_output_s3]\nbucket = \"second\"\n",
        )
        .unwrap();

        assert!(prepare_isolated_grok_config(&destination, &source).is_err());
    }

    #[test]
    fn verified_bearer_selects_only_a_live_xai_oidc_entry() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        private_directory(&home);
        let auth = br#"{
          "legacy": {"key":"do-not-use"},
          "https://auth.x.ai::account": {
            "key":"live-bearer",
            "oidc_issuer":"https://auth.x.ai",
            "expires_at":"2099-01-01T00:00:00Z"
          }
        }"#;
        fs::write(home.join(AUTH_FILE), auth).unwrap();
        fs::set_permissions(home.join(AUTH_FILE), fs::Permissions::from_mode(0o600)).unwrap();
        let digest = sha256(auth);

        assert_eq!(
            read_verified_bearer(&home, &digest, std::time::Duration::from_secs(60)).unwrap(),
            "live-bearer"
        );
        assert!(
            read_verified_bearer(&home, &"0".repeat(64), std::time::Duration::from_secs(60))
                .is_err()
        );
    }
}
