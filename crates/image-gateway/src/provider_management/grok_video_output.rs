use std::{
    fs,
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
};

use toml::{Table, Value};
use uuid::Uuid;

use crate::ImageGatewayError;

use super::{GrokVideoOutputView, UpdateGrokVideoOutputRequest};

const CONFIG_FILE: &str = "config.toml";
const MAX_CONFIG_BYTES: usize = 256 * 1024;
const DEFAULT_KEY_PREFIX: &str = "grok-videos";
const MIN_EXPIRES_SECS: i64 = 60;
const MAX_EXPIRES_SECS: i64 = 3_600;

pub(super) fn read(
    provider_account_id: Uuid,
    home: &Path,
) -> Result<GrokVideoOutputView, ImageGatewayError> {
    let document = read_document(home)?;
    Ok(view(provider_account_id, output_table(&document)))
}

pub(super) fn update(
    provider_account_id: Uuid,
    home: &Path,
    request: UpdateGrokVideoOutputRequest,
) -> Result<GrokVideoOutputView, ImageGatewayError> {
    validate_request(&request)?;
    let mut document = read_document(home)?;
    if request.enabled {
        let existing = output_table(&document).cloned().unwrap_or_default();
        let credentials = credentials_for_update(&existing, &request)?;
        let read_only = existing.get("read_only").cloned();
        let mut output = Table::new();
        output.insert("bucket".to_owned(), Value::String(request.bucket));
        output.insert("region".to_owned(), Value::String(request.region));
        if let Some(endpoint) = normalized_optional(request.endpoint) {
            output.insert("endpoint".to_owned(), Value::String(endpoint));
        }
        output.insert(
            "key_prefix".to_owned(),
            Value::String(normalized_key_prefix(&request.key_prefix)),
        );
        output.insert(
            "expires_secs".to_owned(),
            Value::Integer(request.expires_secs),
        );
        if let Some(value) = read_only {
            output.insert("read_only".to_owned(), value);
        }
        output.insert("read_write".to_owned(), Value::Table(credentials));
        tools_table_mut(&mut document)?
            .insert("zdr_video_output_s3".to_owned(), Value::Table(output));
    } else if let Some(tools) = document.get_mut("tools").and_then(Value::as_table_mut) {
        tools.remove("zdr_video_output_s3");
        if tools.is_empty() {
            document.remove("tools");
        }
    }
    atomic_write_document(home, &document)?;
    Ok(view(provider_account_id, output_table(&document)))
}

fn read_document(home: &Path) -> Result<Table, ImageGatewayError> {
    validate_home(home)?;
    let path = home.join(CONFIG_FILE);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Table::new()),
        Err(_) => return Err(config_unavailable()),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(config_invalid());
    }
    let bytes = fs::read(&path).map_err(|_| config_unavailable())?;
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(config_invalid());
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| config_invalid())?;
    text.parse::<Table>().map_err(|_| config_invalid())
}

fn validate_home(home: &Path) -> Result<(), ImageGatewayError> {
    if !home.is_absolute() {
        return Err(config_invalid());
    }
    let metadata = fs::symlink_metadata(home).map_err(|_| config_unavailable())?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(config_invalid());
    }
    Ok(())
}

fn atomic_write_document(home: &Path, document: &Table) -> Result<(), ImageGatewayError> {
    let mut contents = toml::to_string_pretty(document).map_err(|_| config_invalid())?;
    if !contents.ends_with('\n') {
        contents.push('\n');
    }
    if contents.len() > MAX_CONFIG_BYTES {
        return Err(config_invalid());
    }
    let temporary = home.join(format!(".config-{}.tmp", Uuid::new_v4().simple()));
    let destination = home.join(CONFIG_FILE);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|_| config_unavailable())?;
    let result = (|| -> std::io::Result<()> {
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, &destination)?;
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o600))?;
        fs::File::open(home)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
        return Err(config_unavailable());
    }
    Ok(())
}

fn tools_table_mut(document: &mut Table) -> Result<&mut Table, ImageGatewayError> {
    if !document.contains_key("tools") {
        document.insert("tools".to_owned(), Value::Table(Table::new()));
    }
    document
        .get_mut("tools")
        .and_then(Value::as_table_mut)
        .ok_or_else(config_invalid)
}

fn output_table(document: &Table) -> Option<&Table> {
    document
        .get("tools")
        .and_then(Value::as_table)
        .and_then(|tools| tools.get("zdr_video_output_s3"))
        .and_then(Value::as_table)
}

fn credentials_for_update(
    existing: &Table,
    request: &UpdateGrokVideoOutputRequest,
) -> Result<Table, ImageGatewayError> {
    match (&request.access_key_id, &request.secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            let mut credentials = Table::new();
            credentials.insert(
                "access_key_id".to_owned(),
                Value::String(access_key_id.trim().to_owned()),
            );
            credentials.insert(
                "secret_access_key".to_owned(),
                Value::String(secret_access_key.to_owned()),
            );
            Ok(credentials)
        }
        (None, None) => existing
            .get("read_write")
            .and_then(Value::as_table)
            .filter(|credentials| credentials_ready(credentials))
            .cloned()
            .ok_or_else(|| {
                ImageGatewayError::invalid_request(
                    "S3 access key and secret are required",
                    Some("access_key_id".to_owned()),
                    "grok_video_output_credentials_required",
                )
            }),
        _ => Err(ImageGatewayError::invalid_request(
            "S3 access key and secret must be provided together",
            Some("access_key_id".to_owned()),
            "grok_video_output_credentials_incomplete",
        )),
    }
}

fn validate_request(request: &UpdateGrokVideoOutputRequest) -> Result<(), ImageGatewayError> {
    if !request.enabled {
        return Ok(());
    }
    if !valid_bucket(request.bucket.trim())
        || !valid_region(request.region.trim())
        || !valid_key_prefix(&normalized_key_prefix(&request.key_prefix))
        || !(MIN_EXPIRES_SECS..=MAX_EXPIRES_SECS).contains(&request.expires_secs)
        || request
            .endpoint
            .as_deref()
            .is_some_and(|value| !valid_endpoint(value.trim()))
        || request.access_key_id.as_deref().is_some_and(|value| {
            value.trim().is_empty() || value.len() > 512 || value.chars().any(char::is_control)
        })
        || request.secret_access_key.as_deref().is_some_and(|value| {
            value.is_empty() || value.len() > 1_024 || value.chars().any(char::is_control)
        })
    {
        return Err(ImageGatewayError::invalid_request(
            "Grok video output storage configuration is invalid",
            None,
            "invalid_grok_video_output_configuration",
        ));
    }
    Ok(())
}

fn valid_bucket(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
        && value
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && value
            .as_bytes()
            .last()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
}

fn valid_region(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn valid_endpoint(value: &str) -> bool {
    value.len() <= 2_048
        && value.starts_with("https://")
        && !value.contains(['\r', '\n', '\t'])
        && value["https://".len()..]
            .chars()
            .any(|character| character.is_ascii_alphanumeric())
}

fn valid_key_prefix(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.starts_with('/')
        && !value.contains("..")
        && !value.chars().any(char::is_control)
}

fn normalized_key_prefix(value: &str) -> String {
    let value = value.trim().trim_end_matches('/');
    if value.is_empty() {
        DEFAULT_KEY_PREFIX.to_owned()
    } else {
        value.to_owned()
    }
}

fn normalized_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().trim_end_matches('/').to_owned();
        (!value.is_empty()).then_some(value)
    })
}

fn credentials_ready(table: &Table) -> bool {
    table
        .get("access_key_id")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty())
        && table
            .get("secret_access_key")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
}

fn view(provider_account_id: Uuid, output: Option<&Table>) -> GrokVideoOutputView {
    let credentials = output
        .and_then(|table| table.get("read_write"))
        .and_then(Value::as_table);
    let read_only = output
        .and_then(|table| table.get("read_only"))
        .and_then(Value::as_table);
    let bucket = output
        .and_then(|table| table.get("bucket"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let region = output
        .and_then(|table| table.get("region"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let endpoint = output
        .and_then(|table| table.get("endpoint"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let key_prefix = output
        .and_then(|table| table.get("key_prefix"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let expires_secs = output
        .and_then(|table| table.get("expires_secs"))
        .and_then(Value::as_integer);
    let has_read_write_credentials = credentials.is_some_and(credentials_ready);
    let has_read_only_credentials = read_only.is_some_and(credentials_ready);
    let ready = bucket.as_deref().is_some_and(valid_bucket)
        && region.as_deref().is_some_and(valid_region)
        && key_prefix.as_deref().is_some_and(valid_key_prefix)
        && expires_secs.is_some_and(|value| (MIN_EXPIRES_SECS..=MAX_EXPIRES_SECS).contains(&value))
        && has_read_write_credentials;
    GrokVideoOutputView {
        provider_account_id,
        enabled: output.is_some(),
        ready,
        bucket,
        region,
        endpoint,
        key_prefix,
        expires_secs,
        has_read_write_credentials,
        has_read_only_credentials,
    }
}

fn config_invalid() -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Grok account configuration is invalid")
}

fn config_unavailable() -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Grok account configuration is unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_redacted_ready_configuration_and_preserves_unrelated_settings() {
        let home = tempfile::tempdir().unwrap();
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(home.path().join(CONFIG_FILE), "[ui]\nsimple_mode = true\n").unwrap();

        let account_id = Uuid::new_v4();
        let result = update(
            account_id,
            home.path(),
            UpdateGrokVideoOutputRequest {
                enabled: true,
                bucket: "factory-videos".to_owned(),
                region: "auto".to_owned(),
                endpoint: Some("https://example.r2.cloudflarestorage.com/".to_owned()),
                key_prefix: "generated/videos/".to_owned(),
                expires_secs: 900,
                access_key_id: Some("access".to_owned()),
                secret_access_key: Some("secret".to_owned()),
            },
        )
        .unwrap();

        assert!(result.enabled);
        assert!(result.ready);
        assert_eq!(result.bucket.as_deref(), Some("factory-videos"));
        assert_eq!(result.key_prefix.as_deref(), Some("generated/videos"));
        let config = fs::read_to_string(home.path().join(CONFIG_FILE)).unwrap();
        assert!(config.contains("[ui]"));
        assert!(config.contains("simple_mode = true"));
        assert!(config.contains("[tools.zdr_video_output_s3.read_write]"));
        assert!(config.contains("secret_access_key = \"secret\""));
        assert_eq!(
            fs::metadata(home.path().join(CONFIG_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let reread = read(account_id, home.path()).unwrap();
        assert!(reread.ready);
    }

    #[test]
    fn blank_credentials_preserve_existing_secret_and_disable_removes_only_output_table() {
        let home = tempfile::tempdir().unwrap();
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let account_id = Uuid::new_v4();
        update(
            account_id,
            home.path(),
            UpdateGrokVideoOutputRequest {
                enabled: true,
                bucket: "factory-videos".to_owned(),
                region: "us-east-1".to_owned(),
                endpoint: None,
                key_prefix: String::new(),
                expires_secs: 900,
                access_key_id: Some("access".to_owned()),
                secret_access_key: Some("secret".to_owned()),
            },
        )
        .unwrap();
        update(
            account_id,
            home.path(),
            UpdateGrokVideoOutputRequest {
                enabled: true,
                bucket: "factory-videos-2".to_owned(),
                region: "us-west-2".to_owned(),
                endpoint: None,
                key_prefix: "videos".to_owned(),
                expires_secs: 600,
                access_key_id: None,
                secret_access_key: None,
            },
        )
        .unwrap();
        let config = fs::read_to_string(home.path().join(CONFIG_FILE)).unwrap();
        assert!(config.contains("secret_access_key = \"secret\""));

        let disabled = update(
            account_id,
            home.path(),
            UpdateGrokVideoOutputRequest {
                enabled: false,
                bucket: String::new(),
                region: String::new(),
                endpoint: None,
                key_prefix: String::new(),
                expires_secs: 900,
                access_key_id: None,
                secret_access_key: None,
            },
        )
        .unwrap();
        assert!(!disabled.enabled);
        assert!(
            !fs::read_to_string(home.path().join(CONFIG_FILE))
                .unwrap()
                .contains("zdr_video_output_s3")
        );
    }

    #[test]
    fn writes_qiniu_kodo_s3_compatible_configuration() {
        let home = tempfile::tempdir().unwrap();
        fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let account_id = Uuid::new_v4();

        let output = update(
            account_id,
            home.path(),
            UpdateGrokVideoOutputRequest {
                enabled: true,
                bucket: "factory-videos".to_owned(),
                region: "cn-east-1".to_owned(),
                endpoint: Some("https://s3.cn-east-1.qiniucs.com".to_owned()),
                key_prefix: "grok-videos".to_owned(),
                expires_secs: 900,
                access_key_id: Some("qiniu-access-key".to_owned()),
                secret_access_key: Some("qiniu-secret-key".to_owned()),
            },
        )
        .unwrap();

        assert!(output.ready);
        assert_eq!(output.region.as_deref(), Some("cn-east-1"));
        assert_eq!(
            output.endpoint.as_deref(),
            Some("https://s3.cn-east-1.qiniucs.com")
        );
        let config = fs::read_to_string(home.path().join(CONFIG_FILE)).unwrap();
        assert!(config.contains("endpoint = \"https://s3.cn-east-1.qiniucs.com\""));
        assert!(config.contains("access_key_id = \"qiniu-access-key\""));
    }
}
