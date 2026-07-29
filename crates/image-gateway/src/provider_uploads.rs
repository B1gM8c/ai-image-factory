use std::{
    env, fs,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    http::{HeaderMap, Method, Uri},
};
use futures_util::StreamExt;
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::{ImageGatewayError, artifacts::MAX_ARTIFACT_BYTES, executor::ExecutorSubmissionLease};

const PUBLIC_BASE_URL_ENV: &str = "GATEWAY_PROVIDER_UPLOAD_PUBLIC_BASE_URL";
const NAMESPACE: &str = ".provider-uploads";
const MANIFEST_FILE: &str = "manifest.json";
const OBJECT_FILE: &str = "object.mp4";
const BUCKET: &str = "aif-provider-uploads";
const REGION: &str = "auto";
const ROUTE_PREFIX: &str = "/v1/internal/provider-uploads/s3/";
const MIN_TTL: Duration = Duration::from_secs(60);
const MAX_TTL: Duration = Duration::from_secs(3_600);
const CLOCK_SKEW: Duration = Duration::from_secs(300);
const MAX_QUERY_BYTES: usize = 16 * 1024;
const MAX_SIGNED_HEADERS: usize = 16;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct ProviderUploadService {
    root: PathBuf,
    public_endpoint: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GrokVideoOutputS3Configuration {
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) endpoint: String,
    pub(crate) key_prefix: String,
    pub(crate) expires_secs: i64,
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
}

#[derive(Clone)]
pub(crate) struct AuthorizedProviderUpload {
    ticket_directory: PathBuf,
}

pub(crate) struct ProviderUploadObject {
    pub(crate) file: tokio::fs::File,
    pub(crate) byte_size: u64,
    pub(crate) etag: String,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderUploadError {
    #[error("provider upload authentication failed")]
    Unauthorized,
    #[error("provider upload object was not found")]
    NotFound,
    #[error("provider upload conflicts with an existing object")]
    Conflict,
    #[error("provider upload exceeds the maximum artifact size")]
    TooLarge,
    #[error("provider upload content is invalid")]
    InvalidContent,
    #[error("provider upload storage is unavailable")]
    Unavailable,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TicketManifest {
    schema_version: u16,
    access_key_id: String,
    secret_access_key: String,
    bucket: String,
    region: String,
    key_prefix: String,
    execution_id: Uuid,
    lease_epoch: i64,
    created_at_ms: i64,
    expires_at_ms: i64,
}

impl ProviderUploadService {
    pub fn from_env(root: impl AsRef<Path>) -> Result<Self, ImageGatewayError> {
        let public_base_url = env::var(PUBLIC_BASE_URL_ENV)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        Self::new(root, public_base_url.as_deref())
    }

    pub fn new(
        root: impl AsRef<Path>,
        public_base_url: Option<&str>,
    ) -> Result<Self, ImageGatewayError> {
        let root = fs::canonicalize(root.as_ref()).map_err(|_| {
            ImageGatewayError::config("provider upload artifact root is unavailable")
        })?;
        let metadata = fs::symlink_metadata(&root).map_err(|_| {
            ImageGatewayError::config("provider upload artifact root is unavailable")
        })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(ImageGatewayError::config(
                "provider upload artifact root is invalid",
            ));
        }
        let namespace = root.join(NAMESPACE);
        match fs::create_dir(&namespace) {
            Ok(()) => fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700)).map_err(
                |_| ImageGatewayError::config("provider upload namespace is unavailable"),
            )?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => {
                return Err(ImageGatewayError::config(
                    "provider upload namespace is unavailable",
                ));
            }
        }
        validate_private_directory(&namespace)
            .map_err(|_| ImageGatewayError::config("provider upload namespace is invalid"))?;
        let public_endpoint = public_base_url.map(provider_upload_endpoint).transpose()?;
        Ok(Self {
            root: namespace,
            public_endpoint,
        })
    }

    pub(crate) fn issue_grok_video_output(
        &self,
        lease: &ExecutorSubmissionLease,
        request_timeout: Duration,
    ) -> Result<Option<GrokVideoOutputS3Configuration>, ProviderUploadError> {
        let Some(endpoint) = &self.public_endpoint else {
            return Ok(None);
        };
        if lease.executor_execution_id.is_nil() || lease.executor_lease_epoch <= 0 {
            return Err(ProviderUploadError::Unavailable);
        }
        let ttl = request_timeout
            .saturating_add(Duration::from_secs(300))
            .clamp(MIN_TTL, MAX_TTL);
        let now_ms = now_ms().ok_or(ProviderUploadError::Unavailable)?;
        let expires_at_ms = now_ms
            .checked_add(
                i64::try_from(ttl.as_millis()).map_err(|_| ProviderUploadError::Unavailable)?,
            )
            .ok_or(ProviderUploadError::Unavailable)?;
        let ticket_id = ticket_id(lease.executor_execution_id, lease.executor_lease_epoch);
        let access_key_id = format!("AIF{}", ticket_id.to_ascii_uppercase());
        let ticket_directory = self.root.join(&access_key_id);
        match fs::create_dir(&ticket_directory) {
            Ok(()) => {
                fs::set_permissions(&ticket_directory, fs::Permissions::from_mode(0o700))
                    .map_err(|_| ProviderUploadError::Unavailable)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(ProviderUploadError::Unavailable),
        }
        validate_private_directory(&ticket_directory)
            .map_err(|_| ProviderUploadError::Unavailable)?;
        let manifest_path = ticket_directory.join(MANIFEST_FILE);
        let manifest = match read_manifest(&manifest_path) {
            Ok(existing) => {
                if existing.execution_id != lease.executor_execution_id
                    || existing.lease_epoch != lease.executor_lease_epoch
                    || existing.access_key_id != access_key_id
                    || existing.expires_at_ms <= now_ms
                {
                    return Err(ProviderUploadError::Unavailable);
                }
                existing
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let manifest = TicketManifest {
                    schema_version: 1,
                    access_key_id,
                    secret_access_key: format!(
                        "{}{}",
                        Uuid::new_v4().simple(),
                        Uuid::new_v4().simple()
                    ),
                    bucket: BUCKET.to_owned(),
                    region: REGION.to_owned(),
                    key_prefix: ticket_id,
                    execution_id: lease.executor_execution_id,
                    lease_epoch: lease.executor_lease_epoch,
                    created_at_ms: now_ms,
                    expires_at_ms,
                };
                write_manifest_create_new(&manifest_path, &manifest)
                    .map_err(|_| ProviderUploadError::Unavailable)?;
                manifest
            }
            Err(_) => return Err(ProviderUploadError::Unavailable),
        };
        Ok(Some(GrokVideoOutputS3Configuration {
            bucket: manifest.bucket,
            region: manifest.region,
            endpoint: endpoint.clone(),
            key_prefix: manifest.key_prefix,
            expires_secs: i64::try_from(ttl.as_secs())
                .map_err(|_| ProviderUploadError::Unavailable)?,
            access_key_id: manifest.access_key_id,
            secret_access_key: manifest.secret_access_key,
        }))
    }

    pub(crate) fn authorize(
        &self,
        method: &Method,
        uri: &Uri,
        raw_query: Option<&str>,
        headers: &HeaderMap,
    ) -> Result<AuthorizedProviderUpload, ProviderUploadError> {
        if !matches!(*method, Method::PUT | Method::GET | Method::HEAD) {
            return Err(ProviderUploadError::Unauthorized);
        }
        let raw_query = raw_query
            .filter(|query| !query.is_empty() && query.len() <= MAX_QUERY_BYTES)
            .ok_or(ProviderUploadError::Unauthorized)?;
        let query = ParsedQuery::parse(raw_query)?;
        if query.required("X-Amz-Algorithm")? != "AWS4-HMAC-SHA256" {
            return Err(ProviderUploadError::Unauthorized);
        }
        let credential = query.required("X-Amz-Credential")?;
        let mut credential_parts = credential.split('/');
        let access_key_id = credential_parts
            .next()
            .filter(|value| valid_access_key_id(value))
            .ok_or(ProviderUploadError::Unauthorized)?;
        let scope_date = credential_parts
            .next()
            .ok_or(ProviderUploadError::Unauthorized)?;
        let scope_region = credential_parts
            .next()
            .ok_or(ProviderUploadError::Unauthorized)?;
        if credential_parts.next() != Some("s3")
            || credential_parts.next() != Some("aws4_request")
            || credential_parts.next().is_some()
        {
            return Err(ProviderUploadError::Unauthorized);
        }
        let ticket_directory = self.root.join(access_key_id);
        let manifest = read_manifest(&ticket_directory.join(MANIFEST_FILE)).map_err(|error| {
            match error.kind() {
                std::io::ErrorKind::NotFound => ProviderUploadError::Unauthorized,
                _ => ProviderUploadError::Unavailable,
            }
        })?;
        validate_manifest(&manifest, access_key_id)?;
        let now_ms = now_ms().ok_or(ProviderUploadError::Unavailable)?;
        if manifest.expires_at_ms <= now_ms
            || manifest.region != scope_region
            || manifest.bucket != BUCKET
        {
            return Err(ProviderUploadError::Unauthorized);
        }
        let amz_date = query.required("X-Amz-Date")?;
        if amz_date.get(..8) != Some(scope_date) {
            return Err(ProviderUploadError::Unauthorized);
        }
        let signed_at = parse_amz_date(&amz_date)?;
        let expires = query
            .required("X-Amz-Expires")?
            .parse::<u64>()
            .ok()
            .filter(|value| (1..=MAX_TTL.as_secs()).contains(value))
            .ok_or(ProviderUploadError::Unauthorized)?;
        let now = OffsetDateTime::now_utc();
        if signed_at > now + CLOCK_SKEW
            || now > signed_at + Duration::from_secs(expires) + CLOCK_SKEW
        {
            return Err(ProviderUploadError::Unauthorized);
        }
        let key = object_key(uri.path(), &manifest)?;
        if !valid_object_key(key, &manifest.key_prefix) {
            return Err(ProviderUploadError::Unauthorized);
        }
        let signed_headers = query.required("X-Amz-SignedHeaders")?;
        let canonical_headers = canonical_headers(headers, &signed_headers)?;
        let canonical_query = query.canonical_without_signature();
        let payload_hash = query
            .optional("X-Amz-Content-Sha256")?
            .unwrap_or_else(|| "UNSIGNED-PAYLOAD".to_owned());
        if payload_hash != "UNSIGNED-PAYLOAD" {
            return Err(ProviderUploadError::Unauthorized);
        }
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            uri.path(),
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash
        );
        let credential_scope = format!("{scope_date}/{scope_region}/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signature = hex::decode(query.required("X-Amz-Signature")?)
            .ok()
            .filter(|value| value.len() == 32)
            .ok_or(ProviderUploadError::Unauthorized)?;
        let signing_key = signing_key(&manifest.secret_access_key, scope_date, scope_region, "s3")?;
        let mut mac = HmacSha256::new_from_slice(&signing_key)
            .map_err(|_| ProviderUploadError::Unavailable)?;
        mac.update(string_to_sign.as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| ProviderUploadError::Unauthorized)?;
        Ok(AuthorizedProviderUpload { ticket_directory })
    }

    pub(crate) async fn put(
        &self,
        authorization: AuthorizedProviderUpload,
        body: Body,
    ) -> Result<(u64, String), ProviderUploadError> {
        let temporary = authorization
            .ticket_directory
            .join(format!(".upload-{}", Uuid::new_v4().simple()));
        let destination = authorization.ticket_directory.join(OBJECT_FILE);
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&temporary)
            .await
            .map_err(|_| ProviderUploadError::Unavailable)?;
        let mut body = body.into_data_stream();
        let mut size = 0_u64;
        let mut hasher = Sha256::new();
        let result = async {
            while let Some(chunk) = body.next().await {
                let chunk = chunk.map_err(|_| ProviderUploadError::InvalidContent)?;
                size = size
                    .checked_add(
                        u64::try_from(chunk.len()).map_err(|_| ProviderUploadError::TooLarge)?,
                    )
                    .ok_or(ProviderUploadError::TooLarge)?;
                if size > MAX_ARTIFACT_BYTES {
                    return Err(ProviderUploadError::TooLarge);
                }
                hasher.update(&chunk);
                file.write_all(&chunk)
                    .await
                    .map_err(|_| ProviderUploadError::Unavailable)?;
            }
            if size < 12 {
                return Err(ProviderUploadError::InvalidContent);
            }
            file.flush()
                .await
                .map_err(|_| ProviderUploadError::Unavailable)?;
            file.sync_all()
                .await
                .map_err(|_| ProviderUploadError::Unavailable)?;
            drop(file);
            let mut probe = tokio::fs::File::open(&temporary)
                .await
                .map_err(|_| ProviderUploadError::Unavailable)?;
            let mut header = [0_u8; 12];
            probe
                .read_exact(&mut header)
                .await
                .map_err(|_| ProviderUploadError::InvalidContent)?;
            if &header[4..8] != b"ftyp" {
                return Err(ProviderUploadError::InvalidContent);
            }
            let etag = hex::encode(hasher.finalize());
            commit_upload(&temporary, &destination, size, &etag).await?;
            Ok((size, etag))
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result
    }

    pub(crate) async fn open(
        &self,
        authorization: AuthorizedProviderUpload,
    ) -> Result<ProviderUploadObject, ProviderUploadError> {
        let path = authorization.ticket_directory.join(OBJECT_FILE);
        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::NotFound => ProviderUploadError::NotFound,
                _ => ProviderUploadError::Unavailable,
            })?;
        let metadata = file
            .metadata()
            .await
            .map_err(|_| ProviderUploadError::Unavailable)?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_ARTIFACT_BYTES {
            return Err(ProviderUploadError::InvalidContent);
        }
        let etag = tokio::fs::read_to_string(authorization.ticket_directory.join(".etag"))
            .await
            .map_err(|_| ProviderUploadError::Unavailable)?;
        let etag = etag.trim();
        if etag.len() != 64 || !etag.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ProviderUploadError::InvalidContent);
        }
        Ok(ProviderUploadObject {
            file,
            byte_size: metadata.len(),
            etag: etag.to_ascii_lowercase(),
        })
    }

    pub async fn cleanup_expired(&self) -> Result<u64, ProviderUploadError> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || cleanup_expired_sync(&root))
            .await
            .map_err(|_| ProviderUploadError::Unavailable)?
    }
}

struct ParsedQuery<'a> {
    pairs: Vec<(&'a str, &'a str)>,
}

impl<'a> ParsedQuery<'a> {
    fn parse(raw: &'a str) -> Result<Self, ProviderUploadError> {
        let mut pairs = Vec::new();
        for component in raw.split('&') {
            let (name, value) = component
                .split_once('=')
                .ok_or(ProviderUploadError::Unauthorized)?;
            if name.is_empty() {
                return Err(ProviderUploadError::Unauthorized);
            }
            pairs.push((name, value));
        }
        Ok(Self { pairs })
    }

    fn required(&self, name: &str) -> Result<String, ProviderUploadError> {
        self.optional(name)?
            .ok_or(ProviderUploadError::Unauthorized)
    }

    fn optional(&self, name: &str) -> Result<Option<String>, ProviderUploadError> {
        let mut matches = self
            .pairs
            .iter()
            .filter(|(candidate, _)| *candidate == name)
            .map(|(_, value)| *value);
        let value = matches.next();
        if matches.next().is_some() {
            return Err(ProviderUploadError::Unauthorized);
        }
        value.map(percent_decode).transpose()
    }

    fn canonical_without_signature(&self) -> String {
        let mut pairs = self
            .pairs
            .iter()
            .filter(|(name, _)| *name != "X-Amz-Signature")
            .copied()
            .collect::<Vec<_>>();
        pairs.sort_unstable();
        pairs
            .into_iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("&")
    }
}

fn provider_upload_endpoint(value: &str) -> Result<String, ImageGatewayError> {
    let mut url = reqwest::Url::parse(value).map_err(|_| {
        ImageGatewayError::config(format!("{PUBLIC_BASE_URL_ENV} must be a valid URL"))
    })?;
    if url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(ImageGatewayError::config(format!(
            "{PUBLIC_BASE_URL_ENV} must be an origin without credentials, path, query, or fragment"
        )));
    }
    let host = url.host_str().ok_or_else(|| {
        ImageGatewayError::config(format!("{PUBLIC_BASE_URL_ENV} must include a host"))
    })?;
    let secure = url.scheme() == "https";
    let local_http = url.scheme() == "http" && matches!(host, "localhost" | "127.0.0.1" | "::1");
    if !secure && !local_http {
        return Err(ImageGatewayError::config(format!(
            "{PUBLIC_BASE_URL_ENV} must use HTTPS outside loopback development"
        )));
    }
    // The AWS S3 SDK resolves bucket paths relative to the endpoint. Keep the
    // trailing slash so it does not concatenate `s3` and the bucket name.
    url.set_path(ROUTE_PREFIX);
    Ok(url.to_string())
}

fn ticket_id(execution_id: Uuid, lease_epoch: i64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"aif-provider-upload-v1");
    hasher.update(execution_id.as_bytes());
    hasher.update(lease_epoch.to_be_bytes());
    hex::encode(hasher.finalize())[..32].to_owned()
}

fn write_manifest_create_new(path: &Path, manifest: &TicketManifest) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(manifest).map_err(|_| invalid_data())?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::File::open(path.parent().ok_or_else(invalid_data)?)?.sync_all()
}

fn read_manifest(path: &Path) -> std::io::Result<TicketManifest> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() == 0
        || metadata.len() > 16 * 1024
    {
        return Err(invalid_data());
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(16 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() {
        return Err(invalid_data());
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid_data())
}

fn validate_manifest(
    manifest: &TicketManifest,
    access_key_id: &str,
) -> Result<(), ProviderUploadError> {
    if manifest.schema_version != 1
        || manifest.access_key_id != access_key_id
        || !valid_access_key_id(&manifest.access_key_id)
        || manifest.secret_access_key.len() != 64
        || !manifest
            .secret_access_key
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || manifest.bucket != BUCKET
        || manifest.region != REGION
        || manifest.key_prefix.len() != 32
        || !manifest
            .key_prefix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || manifest.execution_id.is_nil()
        || manifest.lease_epoch <= 0
        || manifest.created_at_ms <= 0
        || manifest.expires_at_ms <= manifest.created_at_ms
    {
        return Err(ProviderUploadError::Unauthorized);
    }
    Ok(())
}

fn valid_access_key_id(value: &str) -> bool {
    value.len() == 35
        && value.starts_with("AIF")
        && value[3..]
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

fn object_key<'a>(
    path: &'a str,
    manifest: &TicketManifest,
) -> Result<&'a str, ProviderUploadError> {
    let remainder = path
        .strip_prefix(ROUTE_PREFIX)
        .ok_or(ProviderUploadError::Unauthorized)?;
    Ok(remainder
        .strip_prefix(&format!("{}/", manifest.bucket))
        .unwrap_or(remainder))
}

fn valid_object_key(value: &str, key_prefix: &str) -> bool {
    let Some(suffix) = value
        .strip_prefix(key_prefix)
        .and_then(|value| value.strip_prefix('/'))
    else {
        return false;
    };
    !suffix.is_empty()
        && suffix.len() <= 512
        && !suffix.contains("..")
        && !suffix.contains("//")
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
}

fn canonical_headers(
    headers: &HeaderMap,
    signed_headers: &str,
) -> Result<String, ProviderUploadError> {
    let names = signed_headers.split(';').collect::<Vec<_>>();
    if names.is_empty()
        || names.len() > MAX_SIGNED_HEADERS
        || names.binary_search(&"host").is_err()
        || names.windows(2).any(|pair| pair[0] >= pair[1])
        || names.iter().any(|name| {
            name.is_empty()
                || name
                    .bytes()
                    .any(|byte| !(byte.is_ascii_lowercase() || byte == b'-'))
        })
    {
        return Err(ProviderUploadError::Unauthorized);
    }
    let mut canonical = String::new();
    for name in names {
        let values = headers
            .get_all(name)
            .iter()
            .map(|value| value.to_str().map(normalize_header_value))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ProviderUploadError::Unauthorized)?;
        if values.is_empty() {
            return Err(ProviderUploadError::Unauthorized);
        }
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(&values.join(","));
        canonical.push('\n');
    }
    Ok(canonical)
}

fn normalize_header_value(value: &str) -> String {
    value.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn percent_decode(value: &str) -> Result<String, ProviderUploadError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len() {
            return Err(ProviderUploadError::Unauthorized);
        }
        let high = hex_nibble(bytes[index + 1]).ok_or(ProviderUploadError::Unauthorized)?;
        let low = hex_nibble(bytes[index + 2]).ok_or(ProviderUploadError::Unauthorized)?;
        decoded.push((high << 4) | low);
        index += 3;
    }
    String::from_utf8(decoded).map_err(|_| ProviderUploadError::Unauthorized)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_amz_date(value: &str) -> Result<OffsetDateTime, ProviderUploadError> {
    if value.len() != 16
        || value.as_bytes().get(8) != Some(&b'T')
        || value.as_bytes().get(15) != Some(&b'Z')
    {
        return Err(ProviderUploadError::Unauthorized);
    }
    let number = |range: std::ops::Range<usize>| {
        value
            .get(range)
            .and_then(|part| part.parse::<u8>().ok())
            .ok_or(ProviderUploadError::Unauthorized)
    };
    let year = value
        .get(0..4)
        .and_then(|part| part.parse::<i32>().ok())
        .ok_or(ProviderUploadError::Unauthorized)?;
    let month = Month::try_from(number(4..6)?).map_err(|_| ProviderUploadError::Unauthorized)?;
    let date = Date::from_calendar_date(year, month, number(6..8)?)
        .map_err(|_| ProviderUploadError::Unauthorized)?;
    let time = Time::from_hms(number(9..11)?, number(11..13)?, number(13..15)?)
        .map_err(|_| ProviderUploadError::Unauthorized)?;
    Ok(PrimitiveDateTime::new(date, time).assume_utc())
}

fn signing_key(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
) -> Result<Vec<u8>, ProviderUploadError> {
    let date_key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let region_key = hmac(&date_key, region.as_bytes())?;
    let service_key = hmac(&region_key, service.as_bytes())?;
    hmac(&service_key, b"aws4_request")
}

fn hmac(key: &[u8], message: &[u8]) -> Result<Vec<u8>, ProviderUploadError> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| ProviderUploadError::Unavailable)?;
    mac.update(message);
    Ok(mac.finalize().into_bytes().to_vec())
}

async fn commit_upload(
    temporary: &Path,
    destination: &Path,
    size: u64,
    etag: &str,
) -> Result<(), ProviderUploadError> {
    let temporary = temporary.to_owned();
    let destination = destination.to_owned();
    let etag = etag.to_owned();
    tokio::task::spawn_blocking(move || match fs::hard_link(&temporary, &destination) {
        Ok(()) => {
            fs::remove_file(&temporary).map_err(|_| ProviderUploadError::Unavailable)?;
            let etag_path = destination
                .parent()
                .ok_or(ProviderUploadError::Unavailable)?
                .join(".etag");
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&etag_path)
                .map_err(|_| ProviderUploadError::Unavailable)?;
            file.write_all(etag.as_bytes())
                .map_err(|_| ProviderUploadError::Unavailable)?;
            file.sync_all()
                .map_err(|_| ProviderUploadError::Unavailable)?;
            fs::File::open(
                destination
                    .parent()
                    .ok_or(ProviderUploadError::Unavailable)?,
            )
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ProviderUploadError::Unavailable)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata =
                fs::metadata(&destination).map_err(|_| ProviderUploadError::Unavailable)?;
            let existing_etag = fs::read_to_string(
                destination
                    .parent()
                    .ok_or(ProviderUploadError::Unavailable)?
                    .join(".etag"),
            )
            .map_err(|_| ProviderUploadError::Conflict)?;
            if metadata.len() == size && existing_etag.trim().eq_ignore_ascii_case(&etag) {
                fs::remove_file(&temporary).map_err(|_| ProviderUploadError::Unavailable)?;
                Ok(())
            } else {
                Err(ProviderUploadError::Conflict)
            }
        }
        Err(_) => Err(ProviderUploadError::Unavailable),
    })
    .await
    .map_err(|_| ProviderUploadError::Unavailable)?
}

fn cleanup_expired_sync(root: &Path) -> Result<u64, ProviderUploadError> {
    let now = now_ms().ok_or(ProviderUploadError::Unavailable)?;
    let entries = fs::read_dir(root).map_err(|_| ProviderUploadError::Unavailable)?;
    let mut deleted = 0_u64;
    for entry in entries {
        let entry = entry.map_err(|_| ProviderUploadError::Unavailable)?;
        let file_type = entry
            .file_type()
            .map_err(|_| ProviderUploadError::Unavailable)?;
        if !file_type.is_dir() || file_type.is_symlink() {
            continue;
        }
        let manifest = match read_manifest(&entry.path().join(MANIFEST_FILE)) {
            Ok(manifest) => manifest,
            Err(_) => continue,
        };
        if manifest.expires_at_ms > now {
            continue;
        }
        fs::remove_dir_all(entry.path()).map_err(|_| ProviderUploadError::Unavailable)?;
        deleted = deleted.saturating_add(1);
    }
    Ok(deleted)
}

fn validate_private_directory(path: &Path) -> std::io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_dir()
        && !metadata.file_type().is_symlink()
        && metadata.uid() == unsafe { libc::geteuid() }
        && metadata.permissions().mode() & 0o077 == 0
    {
        Ok(())
    } else {
        Err(invalid_data())
    }
}

fn now_ms() -> Option<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
}

fn invalid_data() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "invalid provider upload state",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, Method, Uri};

    fn lease() -> ExecutorSubmissionLease {
        ExecutorSubmissionLease {
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            output_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            tenant_id: "tenant".to_owned(),
            provider_id: "grok".to_owned(),
            model: "grok-imagine-video".to_owned(),
            work_item_id: Uuid::new_v4(),
            output_index: 0,
            command_schema: "grok.cli.video.v1".to_owned(),
            command_hash: "a".repeat(64),
            execution_profile_id: Uuid::new_v4(),
            adapter_revision: "test".to_owned(),
            executor_owner: "executor".to_owned(),
            executor_lease_epoch: 1,
            executor_lease_expires_at_ms: now_ms().unwrap() + 60_000,
        }
    }

    #[test]
    fn issue_is_retry_stable_and_scoped() {
        let root = tempfile::tempdir().unwrap();
        let service =
            ProviderUploadService::new(root.path(), Some("http://127.0.0.1:8787")).unwrap();
        let lease = lease();
        let first = service
            .issue_grok_video_output(&lease, Duration::from_secs(900))
            .unwrap()
            .unwrap();
        let second = service
            .issue_grok_video_output(&lease, Duration::from_secs(900))
            .unwrap()
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.endpoint,
            "http://127.0.0.1:8787/v1/internal/provider-uploads/s3/"
        );
        assert_eq!(first.bucket, BUCKET);
        assert_eq!(first.region, REGION);
        assert_eq!(first.expires_secs, 1_200);
    }

    #[test]
    fn public_endpoint_requires_https_off_loopback() {
        let root = tempfile::tempdir().unwrap();
        assert!(ProviderUploadService::new(root.path(), Some("http://example.com")).is_err());
        assert!(ProviderUploadService::new(root.path(), Some("https://example.com")).is_ok());
    }

    #[test]
    fn signed_request_authenticates_and_tampering_fails() {
        let root = tempfile::tempdir().unwrap();
        let service =
            ProviderUploadService::new(root.path(), Some("http://127.0.0.1:8787")).unwrap();
        let lease = lease();
        let config = service
            .issue_grok_video_output(&lease, Duration::from_secs(900))
            .unwrap()
            .unwrap();
        let path = format!(
            "{ROUTE_PREFIX}{}/{}/video.mp4",
            config.bucket, config.key_prefix
        );
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("127.0.0.1:8787"));
        let query = signed_query(&Method::PUT, &path, &headers, &config);
        let uri = format!("{path}?{query}").parse::<Uri>().unwrap();
        assert!(
            service
                .authorize(&Method::PUT, &uri, Some(&query), &headers)
                .is_ok()
        );
        let tampered = format!("{query}0");
        assert!(matches!(
            service.authorize(&Method::PUT, &uri, Some(&tampered), &headers),
            Err(ProviderUploadError::Unauthorized)
        ));
    }

    #[test]
    fn canonical_headers_accepts_aws_sdk_signed_header_order() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("uploads.example.com"));
        headers.insert("content-type", HeaderValue::from_static("video/mp4"));

        assert_eq!(
            canonical_headers(&headers, "content-type;host").unwrap(),
            "content-type:video/mp4\nhost:uploads.example.com\n"
        );
    }

    #[tokio::test]
    async fn signed_put_and_get_stream_the_same_video_bytes() {
        let root = tempfile::tempdir().unwrap();
        let service =
            ProviderUploadService::new(root.path(), Some("http://127.0.0.1:8787")).unwrap();
        let lease = lease();
        let config = service
            .issue_grok_video_output(&lease, Duration::from_secs(900))
            .unwrap()
            .unwrap();
        let path = format!(
            "{ROUTE_PREFIX}{}/{}/video.mp4",
            config.bucket, config.key_prefix
        );
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("127.0.0.1:8787"));
        let video = b"\0\0\0\x0cftypisomtest-video".to_vec();

        let put_query = signed_query(&Method::PUT, &path, &headers, &config);
        let put_uri = format!("{path}?{put_query}").parse::<Uri>().unwrap();
        let authorization = service
            .authorize(&Method::PUT, &put_uri, Some(&put_query), &headers)
            .unwrap();
        let (size, etag) = service
            .put(authorization, Body::from(video.clone()))
            .await
            .unwrap();
        assert_eq!(size, video.len() as u64);
        assert_eq!(etag, hex::encode(Sha256::digest(&video)));

        let retry_query = signed_query(&Method::PUT, &path, &headers, &config);
        let retry_uri = format!("{path}?{retry_query}").parse::<Uri>().unwrap();
        let retry_authorization = service
            .authorize(&Method::PUT, &retry_uri, Some(&retry_query), &headers)
            .unwrap();
        assert_eq!(
            service
                .put(retry_authorization, Body::from(video.clone()))
                .await
                .unwrap(),
            (size, etag.clone())
        );

        let get_query = signed_query(&Method::GET, &path, &headers, &config);
        let get_uri = format!("{path}?{get_query}").parse::<Uri>().unwrap();
        let authorization = service
            .authorize(&Method::GET, &get_uri, Some(&get_query), &headers)
            .unwrap();
        let mut object = service.open(authorization).await.unwrap();
        let mut downloaded = Vec::new();
        object.file.read_to_end(&mut downloaded).await.unwrap();
        assert_eq!(object.byte_size, size);
        assert_eq!(object.etag, etag);
        assert_eq!(downloaded, video);

        let conflicting = b"\0\0\0\x0cftypisomother-video".to_vec();
        let conflict_query = signed_query(&Method::PUT, &path, &headers, &config);
        let conflict_uri = format!("{path}?{conflict_query}").parse::<Uri>().unwrap();
        let authorization = service
            .authorize(&Method::PUT, &conflict_uri, Some(&conflict_query), &headers)
            .unwrap();
        assert_eq!(
            service
                .put(authorization, Body::from(conflicting))
                .await
                .unwrap_err(),
            ProviderUploadError::Conflict
        );
    }

    #[tokio::test]
    async fn cleanup_removes_only_expired_ticket_directory() {
        let root = tempfile::tempdir().unwrap();
        let service =
            ProviderUploadService::new(root.path(), Some("http://127.0.0.1:8787")).unwrap();
        let lease = lease();
        let config = service
            .issue_grok_video_output(&lease, Duration::from_secs(900))
            .unwrap()
            .unwrap();
        let ticket_directory = service.root.join(&config.access_key_id);
        let manifest_path = ticket_directory.join(MANIFEST_FILE);
        let mut manifest = read_manifest(&manifest_path).unwrap();
        manifest.expires_at_ms = now_ms().unwrap() - 1;
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        assert_eq!(service.cleanup_expired().await.unwrap(), 1);
        assert!(!ticket_directory.exists());
        assert_eq!(service.cleanup_expired().await.unwrap(), 0);
    }

    fn signed_query(
        method: &Method,
        path: &str,
        headers: &HeaderMap,
        config: &GrokVideoOutputS3Configuration,
    ) -> String {
        let now = OffsetDateTime::now_utc();
        let year = now.year();
        let month = u8::from(now.month());
        let day = now.day();
        let date = format!("{year:04}{month:02}{day:02}");
        let amz_date = format!(
            "{date}T{:02}{:02}{:02}Z",
            now.hour(),
            now.minute(),
            now.second()
        );
        let credential = format!(
            "{}%2F{}%2F{}%2Fs3%2Faws4_request",
            config.access_key_id, date, config.region
        );
        let mut pairs = vec![
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_owned()),
            ("X-Amz-Credential", credential),
            ("X-Amz-Date", amz_date.clone()),
            ("X-Amz-Expires", config.expires_secs.to_string()),
            ("X-Amz-SignedHeaders", "host".to_owned()),
        ];
        pairs.sort_unstable();
        let canonical_query = pairs
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        let canonical = format!(
            "{}\n{}\n{}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
            method,
            path,
            canonical_query,
            headers.get("host").unwrap().to_str().unwrap()
        );
        let scope = format!("{date}/{}/s3/aws4_request", config.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical.as_bytes()))
        );
        let key = signing_key(&config.secret_access_key, &date, &config.region, "s3").unwrap();
        let signature = hmac(&key, string_to_sign.as_bytes()).unwrap();
        format!(
            "{canonical_query}&X-Amz-Signature={}",
            hex::encode(signature)
        )
    }
}
