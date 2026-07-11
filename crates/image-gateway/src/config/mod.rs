use std::{
    env,
    fs::{self, OpenOptions},
    net::SocketAddr,
    path::Path,
    time::Duration,
};

use crate::ImageGatewayError;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub bind: SocketAddr,
    pub auth_token: Option<String>,
    pub admin_token: Option<String>,
    pub database_url: Option<String>,
    pub five_hour_image_limit: u32,
    pub seven_day_image_limit: u32,
    pub max_concurrent_jobs: usize,
    pub max_queue_size: usize,
    pub max_concurrent_jobs_per_tenant: usize,
    pub max_queue_size_per_tenant: usize,
    pub queue_timeout: Duration,
    pub request_timeout: Duration,
    pub max_upload_bytes: usize,
    pub proxy: ProxyConfig,
    pub codex_home: Option<String>,
    pub cleanup_codex_outputs: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ProxyConfig {
    pub http_proxy: Option<String>,
    pub https_proxy: Option<String>,
    pub all_proxy: Option<String>,
    pub no_proxy: Option<String>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ImageGatewayError> {
        let bind = env::var("GATEWAY_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8787".to_string())
            .parse()
            .map_err(|_| ImageGatewayError::config("GATEWAY_BIND must be a socket address"))?;

        let database_url = env::var("DATABASE_URL")
            .ok()
            .or_else(|| env::var("GATEWAY_DATABASE_URL").ok());

        Ok(Self {
            bind,
            auth_token: non_empty_env("GATEWAY_API_TOKEN"),
            admin_token: non_empty_env("GATEWAY_ADMIN_TOKEN"),
            database_url,
            five_hour_image_limit: env_u32("GATEWAY_IMAGE_LIMIT_5H", 40)?,
            seven_day_image_limit: env_u32("GATEWAY_IMAGE_LIMIT_7D", 200)?,
            max_concurrent_jobs: env_usize("GATEWAY_MAX_CONCURRENT_JOBS", 1)?,
            max_queue_size: env_usize("GATEWAY_MAX_QUEUE_SIZE", 8)?,
            max_concurrent_jobs_per_tenant: env_usize(
                "GATEWAY_MAX_CONCURRENT_JOBS_PER_TENANT",
                env_usize("GATEWAY_MAX_CONCURRENT_JOBS", 1)?,
            )?,
            max_queue_size_per_tenant: env_usize(
                "GATEWAY_MAX_QUEUE_SIZE_PER_TENANT",
                env_usize("GATEWAY_MAX_QUEUE_SIZE", 8)?,
            )?,
            queue_timeout: Duration::from_secs(env_u64("GATEWAY_QUEUE_TIMEOUT_SECS", 120)?),
            request_timeout: Duration::from_secs(env_u64("GATEWAY_REQUEST_TIMEOUT_SECS", 900)?),
            max_upload_bytes: env_usize("GATEWAY_MAX_UPLOAD_BYTES", 32 * 1024 * 1024)?,
            proxy: ProxyConfig {
                http_proxy: env::var("GATEWAY_HTTP_PROXY")
                    .ok()
                    .or_else(|| env::var("HTTP_PROXY").ok()),
                https_proxy: env::var("GATEWAY_HTTPS_PROXY")
                    .ok()
                    .or_else(|| env::var("HTTPS_PROXY").ok()),
                all_proxy: env::var("GATEWAY_ALL_PROXY")
                    .ok()
                    .or_else(|| env::var("ALL_PROXY").ok()),
                no_proxy: env::var("GATEWAY_NO_PROXY")
                    .ok()
                    .or_else(|| env::var("NO_PROXY").ok()),
            },
            codex_home: non_empty_env("GATEWAY_CODEX_HOME"),
            cleanup_codex_outputs: env_bool("GATEWAY_CLEANUP_CODEX_OUTPUTS", false),
        })
    }

    pub fn validate_startup(&self) -> Result<(), ImageGatewayError> {
        let auth_token = self.auth_token.as_deref().map(str::trim);
        let admin_token = self.admin_token.as_deref().map(str::trim);

        if auth_token.is_some_and(str::is_empty) {
            return Err(ImageGatewayError::config(
                "GATEWAY_API_TOKEN must not be empty",
            ));
        }
        if admin_token.is_some_and(str::is_empty) {
            return Err(ImageGatewayError::config(
                "GATEWAY_ADMIN_TOKEN must not be empty",
            ));
        }
        if auth_token.is_none() && admin_token.is_none() {
            return Err(ImageGatewayError::config(
                "GATEWAY_API_TOKEN or GATEWAY_ADMIN_TOKEN is required",
            ));
        }
        if auth_token
            .zip(admin_token)
            .is_some_and(|(api, admin)| api == admin)
        {
            return Err(ImageGatewayError::config(
                "GATEWAY_API_TOKEN and GATEWAY_ADMIN_TOKEN must be different",
            ));
        }

        self.validate_worker_startup()?;

        if !self.bind.ip().is_loopback() {
            return Err(ImageGatewayError::config(
                "native TLS is not implemented; set GATEWAY_BIND to a loopback address and expose it only through a TLS reverse proxy",
            ));
        }
        Ok(())
    }

    pub fn validate_worker_startup(&self) -> Result<(), ImageGatewayError> {
        let codex_home = self
            .codex_home
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ImageGatewayError::config("GATEWAY_CODEX_HOME is required"))?;
        if !Path::new(codex_home).is_absolute() {
            return Err(ImageGatewayError::config(
                "GATEWAY_CODEX_HOME must be an absolute path",
            ));
        }
        validate_production_codex_home(Path::new(codex_home))?;
        Ok(())
    }
}

fn validate_production_codex_home(codex_home: &Path) -> Result<(), ImageGatewayError> {
    validate_production_codex_home_with_probe(codex_home, |probe_path| {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(probe_path)
    })
}

fn validate_production_codex_home_with_probe<F>(
    codex_home: &Path,
    open_probe: F,
) -> Result<(), ImageGatewayError>
where
    F: FnOnce(&Path) -> std::io::Result<std::fs::File>,
{
    let metadata = fs::symlink_metadata(codex_home).map_err(|_| {
        ImageGatewayError::config("GATEWAY_CODEX_HOME must be an existing directory")
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ImageGatewayError::config(
            "GATEWAY_CODEX_HOME must be an existing directory and must not be a symlink",
        ));
    }
    let canonical_home = fs::canonicalize(codex_home).map_err(|_| {
        ImageGatewayError::config("GATEWAY_CODEX_HOME must be an existing directory")
    })?;
    if canonical_home.parent().is_none() {
        return Err(ImageGatewayError::config(
            "GATEWAY_CODEX_HOME must not be the filesystem root",
        ));
    }

    let probe_path = canonical_home.join(format!(
        ".image-gateway-write-probe-{}",
        Uuid::new_v4().simple()
    ));
    let probe = open_probe(&probe_path)
        .map_err(|_| ImageGatewayError::config("GATEWAY_CODEX_HOME must be writable"))?;
    drop(probe);
    fs::remove_file(probe_path).map_err(|_| {
        ImageGatewayError::config("GATEWAY_CODEX_HOME write probe could not be removed")
    })?;
    Ok(())
}

fn env_u32(name: &str, default: u32) -> Result<u32, ImageGatewayError> {
    env::var(name)
        .map(|v| {
            v.parse()
                .map_err(|_| ImageGatewayError::config(format!("{name} must be an integer")))
        })
        .unwrap_or(Ok(default))
}

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_u64(name: &str, default: u64) -> Result<u64, ImageGatewayError> {
    env::var(name)
        .map(|v| {
            v.parse()
                .map_err(|_| ImageGatewayError::config(format!("{name} must be an integer")))
        })
        .unwrap_or(Ok(default))
}

fn env_usize(name: &str, default: usize) -> Result<usize, ImageGatewayError> {
    env::var(name)
        .map(|v| {
            v.parse()
                .map_err(|_| ImageGatewayError::config(format!("{name} must be an integer")))
        })
        .unwrap_or(Ok(default))
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|v| match v.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Some(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_for_bind(bind: &str, token: Option<&str>) -> AppConfig {
        AppConfig {
            bind: bind.parse().unwrap(),
            auth_token: token.map(str::to_string),
            admin_token: None,
            database_url: Some("postgres://localhost/test".to_string()),
            five_hour_image_limit: 1,
            seven_day_image_limit: 1,
            max_concurrent_jobs: 1,
            max_queue_size: 0,
            max_concurrent_jobs_per_tenant: 1,
            max_queue_size_per_tenant: 0,
            queue_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            max_upload_bytes: 1024,
            proxy: ProxyConfig::default(),
            codex_home: None,
            cleanup_codex_outputs: false,
        }
    }

    fn config_for_bind_with_admin(bind: &str, admin_token: Option<&str>) -> AppConfig {
        let mut config = config_for_bind(bind, None);
        config.admin_token = admin_token.map(str::to_string);
        config
    }

    #[test]
    fn every_bind_requires_an_api_or_admin_token() {
        let codex_home = tempfile::tempdir().unwrap();
        for bind in ["127.0.0.1:8787", "0.0.0.0:8787"] {
            let mut config = config_for_bind(bind, None);
            config.codex_home = Some(codex_home.path().to_string_lossy().into_owned());

            let error = format!("{:?}", config.validate_startup().unwrap_err());

            assert!(error.contains("GATEWAY_API_TOKEN or GATEWAY_ADMIN_TOKEN is required"));
        }
    }

    #[test]
    fn every_bind_rejects_blank_codex_home() {
        let mut config = config_for_bind("127.0.0.1:8787", Some("token"));
        config.codex_home = Some("   ".to_string());

        assert!(config.validate_startup().is_err());
    }

    #[test]
    fn every_bind_requires_explicit_codex_home() {
        let config = config_for_bind("127.0.0.1:8787", Some("token"));

        let error = format!("{:?}", config.validate_startup().unwrap_err());

        assert!(error.contains("GATEWAY_CODEX_HOME is required"));
    }

    #[test]
    fn loopback_bind_accepts_absolute_explicit_codex_home() {
        let codex_home = tempfile::tempdir().unwrap();
        let mut config = config_for_bind("127.0.0.1:8787", Some("token"));
        config.codex_home = Some(codex_home.path().to_string_lossy().into_owned());

        assert!(config.validate_startup().is_ok());
        assert_eq!(std::fs::read_dir(codex_home.path()).unwrap().count(), 0);
    }

    #[test]
    fn every_bind_rejects_relative_explicit_codex_home() {
        let mut config = config_for_bind("127.0.0.1:8787", Some("token"));
        config.codex_home = Some("relative/codex-home".to_string());

        assert!(config.validate_startup().is_err());
    }

    #[test]
    fn public_bind_rejects_filesystem_root_as_codex_home() {
        let mut config = config_for_bind("0.0.0.0:8787", Some("token"));
        config.codex_home = Some("/".to_string());

        assert!(config.validate_startup().is_err());
    }

    #[test]
    fn public_bind_rejects_filesystem_root_alias_as_codex_home() {
        let result = validate_production_codex_home_with_probe(Path::new("/."), |_| {
            panic!("root aliases must be rejected before opening a write probe")
        });

        assert!(result.is_err());
    }

    #[test]
    fn public_bind_rejects_nonexistent_codex_home() {
        let parent = tempfile::tempdir().unwrap();
        let mut config = config_for_bind("0.0.0.0:8787", Some("token"));
        config.codex_home = Some(parent.path().join("missing").to_string_lossy().into_owned());

        assert!(config.validate_startup().is_err());
    }

    #[test]
    fn public_bind_rejects_regular_file_as_codex_home() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut config = config_for_bind("0.0.0.0:8787", Some("token"));
        config.codex_home = Some(file.path().to_string_lossy().into_owned());

        assert!(config.validate_startup().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn public_bind_rejects_symlink_as_codex_home() {
        let target = tempfile::tempdir().unwrap();
        let parent = tempfile::tempdir().unwrap();
        let symlink = parent.path().join("codex-home");
        std::os::unix::fs::symlink(target.path(), &symlink).unwrap();
        let mut config = config_for_bind("0.0.0.0:8787", Some("token"));
        config.codex_home = Some(symlink.to_string_lossy().into_owned());

        assert!(config.validate_startup().is_err());
    }

    #[test]
    fn public_bind_rejects_unwritable_codex_home() {
        let codex_home = tempfile::tempdir().unwrap();
        let result = validate_production_codex_home_with_probe(codex_home.path(), |_| {
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
        });

        assert!(result.is_err());
        assert_eq!(std::fs::read_dir(codex_home.path()).unwrap().count(), 0);
    }

    #[test]
    fn auth_token_cannot_be_empty() {
        assert!(
            config_for_bind("0.0.0.0:8787", Some(""))
                .validate_startup()
                .is_err()
        );
        assert!(
            config_for_bind("0.0.0.0:8787", Some("   "))
                .validate_startup()
                .is_err()
        );
    }

    #[test]
    fn api_and_admin_tokens_must_be_distinct() {
        let codex_home = tempfile::tempdir().unwrap();
        let mut config = config_for_bind("127.0.0.1:8787", Some("shared-token"));
        config.admin_token = Some("shared-token".to_string());
        config.codex_home = Some(codex_home.path().to_string_lossy().into_owned());

        let error = format!("{:?}", config.validate_startup().unwrap_err());

        assert!(error.contains("GATEWAY_API_TOKEN and GATEWAY_ADMIN_TOKEN must be different"));
    }

    #[test]
    fn non_loopback_bind_requires_tls_reverse_proxy() {
        let codex_home = tempfile::tempdir().unwrap();
        let mut config = config_for_bind("0.0.0.0:8787", Some("api-token"));
        config.codex_home = Some(codex_home.path().to_string_lossy().into_owned());

        let error = format!("{:?}", config.validate_startup().unwrap_err());

        assert!(error.contains("loopback"));
        assert!(error.contains("TLS reverse proxy"));
    }

    #[test]
    fn loopback_allows_admin_only_bootstrap_with_safe_codex_home() {
        let codex_home = tempfile::tempdir().unwrap();
        let mut config = config_for_bind_with_admin("127.0.0.1:8787", Some("admin-token"));
        config.codex_home = Some(codex_home.path().to_string_lossy().into_owned());

        assert!(config.validate_startup().is_ok());
    }
}
