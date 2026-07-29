mod maintenance;
mod postgres;

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use factory_identity::{
    AccessTokenCodec, AuthPolicy, IdentityError, IdentityService, RefreshTokenKeyring,
};
use sqlx::PgPool;

use crate::ImageGatewayError;

pub use maintenance::{IdentityMaintenanceOutcome, PostgresIdentityMaintenanceStore};
pub use postgres::PostgresIdentityRepository;

pub async fn service_from_env(
    pool: PgPool,
) -> Result<Option<Arc<IdentityService>>, ImageGatewayError> {
    if !identity_enabled()? {
        return Ok(None);
    }
    let policy = AuthPolicy {
        client_id: optional_env("GATEWAY_AUTH_CLIENT_ID")
            .unwrap_or_else(|| "ai-image-factory-admin-bff".to_string()),
        access_ttl_seconds: env_u64("GATEWAY_AUTH_ACCESS_TTL_SECONDS", 300)?,
        session_idle_ttl_seconds: env_u64("GATEWAY_AUTH_IDLE_TTL_SECONDS", 8 * 60 * 60)?,
        session_absolute_ttl_seconds: env_u64(
            "GATEWAY_AUTH_ABSOLUTE_TTL_SECONDS",
            30 * 24 * 60 * 60,
        )?,
        clock_skew_seconds: env_u64("GATEWAY_AUTH_CLOCK_SKEW_SECONDS", 30)?,
        max_failed_logins: env_u32("GATEWAY_AUTH_MAX_FAILED_LOGINS", 5)?,
        lockout_seconds: env_u64("GATEWAY_AUTH_LOCKOUT_SECONDS", 15 * 60)?,
        password_hash_concurrency: env_usize("GATEWAY_AUTH_PASSWORD_CONCURRENCY", 4)?,
        login_throttle_window_seconds: env_u64("GATEWAY_AUTH_LOGIN_THROTTLE_WINDOW_SECONDS", 60)?,
        max_account_login_attempts: env_u32("GATEWAY_AUTH_MAX_ACCOUNT_LOGIN_ATTEMPTS", 10)?,
        max_global_login_attempts: env_u32("GATEWAY_AUTH_MAX_GLOBAL_LOGIN_ATTEMPTS", 1_000)?,
    };
    policy
        .validate()
        .map_err(|_| ImageGatewayError::config("identity policy is invalid"))?;

    let active_kid = required_env("GATEWAY_JWT_ACTIVE_KID")?;
    let private_key = read_key_file("GATEWAY_JWT_PRIVATE_KEY_PATH", true)?;
    let public_keys = parse_public_keys(&required_env("GATEWAY_JWT_PUBLIC_KEYS")?)?;
    let access_tokens = AccessTokenCodec::new(
        active_kid,
        &private_key,
        public_keys,
        required_env("GATEWAY_AUTH_ISSUER")?,
        required_env("GATEWAY_AUTH_AUDIENCE")?,
        &policy,
    )
    .map_err(|_| ImageGatewayError::config("JWT keyring is invalid"))?;
    let refresh_tokens = parse_refresh_peppers()?;
    let repository = Arc::new(PostgresIdentityRepository::new(pool));
    let service = IdentityService::new(repository, access_tokens, refresh_tokens, policy)
        .map_err(|_| ImageGatewayError::config("identity service configuration is invalid"))?;
    Ok(Some(Arc::new(service)))
}

fn identity_enabled() -> Result<bool, ImageGatewayError> {
    match env::var("GATEWAY_IDENTITY_ENABLED").as_deref() {
        Ok("1" | "true" | "TRUE" | "yes" | "YES") => Ok(true),
        Ok("0" | "false" | "FALSE" | "no" | "NO") | Err(env::VarError::NotPresent) => Ok(false),
        Err(env::VarError::NotUnicode(_)) | Ok(_) => Err(ImageGatewayError::config(
            "GATEWAY_IDENTITY_ENABLED must be a boolean",
        )),
    }
}

fn parse_public_keys(raw: &str) -> Result<Vec<(String, Vec<u8>)>, ImageGatewayError> {
    let mut keys = Vec::new();
    for entry in raw.split(',') {
        let (kid, path) = entry.split_once(':').ok_or_else(|| {
            ImageGatewayError::config("GATEWAY_JWT_PUBLIC_KEYS must use kid:/absolute/path entries")
        })?;
        let kid = kid.trim();
        if kid.is_empty() || kid.len() > 128 || keys.iter().any(|(existing, _)| existing == kid) {
            return Err(ImageGatewayError::config(
                "GATEWAY_JWT_PUBLIC_KEYS contains an invalid or duplicate kid",
            ));
        }
        let pem = read_path(Path::new(path.trim()), false)?;
        keys.push((kid.to_string(), pem));
    }
    if keys.is_empty() {
        return Err(ImageGatewayError::config(
            "GATEWAY_JWT_PUBLIC_KEYS must contain at least one key",
        ));
    }
    Ok(keys)
}

fn parse_refresh_peppers() -> Result<RefreshTokenKeyring, ImageGatewayError> {
    let current = required_env("GATEWAY_REFRESH_TOKEN_CURRENT_PEPPER_VERSION")?
        .parse::<u16>()
        .map_err(|_| {
            ImageGatewayError::config("refresh token pepper version must be an integer")
        })?;
    let raw = read_key_file("GATEWAY_REFRESH_TOKEN_PEPPERS_PATH", true)?;
    let raw = std::str::from_utf8(&raw)
        .map_err(|_| ImageGatewayError::config("refresh token pepper file must be UTF-8"))?;
    let mut peppers = BTreeMap::new();
    for entry in raw.lines().map(str::trim).filter(|entry| !entry.is_empty()) {
        let (version, encoded) = entry.split_once(':').ok_or_else(|| {
            ImageGatewayError::config("refresh token peppers must use version:64-hex entries")
        })?;
        let version = version.trim().parse::<u16>().map_err(|_| {
            ImageGatewayError::config("refresh token pepper versions must be integers")
        })?;
        let value = hex::decode(encoded.trim())
            .map_err(|_| ImageGatewayError::config("refresh token peppers must be hexadecimal"))?;
        if peppers.insert(version, value).is_some() {
            return Err(ImageGatewayError::config(
                "refresh token keyring contains a duplicate version",
            ));
        }
    }
    RefreshTokenKeyring::new(current, peppers)
        .map_err(|_| ImageGatewayError::config("refresh token keyring is invalid"))
}

fn read_key_file(name: &str, private: bool) -> Result<Vec<u8>, ImageGatewayError> {
    let path = PathBuf::from(required_env(name)?);
    read_path(&path, private)
}

fn read_path(path: &Path, private: bool) -> Result<Vec<u8>, ImageGatewayError> {
    if !path.is_absolute() {
        return Err(ImageGatewayError::config(
            "identity key paths must be absolute",
        ));
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| ImageGatewayError::config("identity key file is unavailable"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 64 * 1024 {
        return Err(ImageGatewayError::config(
            "identity key path must be a regular non-symlink file smaller than 64 KiB",
        ));
    }
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ImageGatewayError::config(
                "JWT private key permissions must not grant group or other access",
            ));
        }
    }
    fs::read(path).map_err(|_| ImageGatewayError::config("identity key file could not be read"))
}

fn required_env(name: &str) -> Result<String, ImageGatewayError> {
    optional_env(name).ok_or_else(|| ImageGatewayError::config(format!("{name} is required")))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_u64(name: &str, default: u64) -> Result<u64, ImageGatewayError> {
    optional_env(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| ImageGatewayError::config(format!("{name} must be an integer")))
        .map(|value| value.unwrap_or(default))
}

fn env_u32(name: &str, default: u32) -> Result<u32, ImageGatewayError> {
    env_u64(name, u64::from(default)).and_then(|value| {
        u32::try_from(value).map_err(|_| ImageGatewayError::config(format!("{name} is too large")))
    })
}

fn env_usize(name: &str, default: usize) -> Result<usize, ImageGatewayError> {
    env_u64(name, default as u64).and_then(|value| {
        usize::try_from(value)
            .map_err(|_| ImageGatewayError::config(format!("{name} is too large")))
    })
}

fn map_repository_error(_: sqlx::Error) -> IdentityError {
    IdentityError::Unavailable
}
