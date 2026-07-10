use std::{env, net::SocketAddr, time::Duration};

use crate::ImageGatewayError;

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
            codex_home: env::var("GATEWAY_CODEX_HOME").ok(),
            cleanup_codex_outputs: env_bool("GATEWAY_CLEANUP_CODEX_OUTPUTS", false),
        })
    }

    pub fn validate_startup(&self) -> Result<(), ImageGatewayError> {
        if self
            .auth_token
            .as_deref()
            .is_some_and(|token| token.trim().is_empty())
        {
            return Err(ImageGatewayError::config(
                "GATEWAY_API_TOKEN must not be empty",
            ));
        }
        if !self.bind.ip().is_loopback() && self.auth_token.is_none() && self.admin_token.is_none()
        {
            return Err(ImageGatewayError::config(
                "GATEWAY_API_TOKEN or GATEWAY_ADMIN_TOKEN is required when GATEWAY_BIND is not loopback",
            ));
        }
        Ok(())
    }
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
            admin_token: token.map(str::to_string),
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
    fn public_bind_requires_auth_token() {
        assert!(
            config_for_bind("0.0.0.0:8787", None)
                .validate_startup()
                .is_err()
        );
        assert!(
            config_for_bind("0.0.0.0:8787", Some("token"))
                .validate_startup()
                .is_ok()
        );
        assert!(
            config_for_bind_with_admin("0.0.0.0:8787", Some("admin-token"))
                .validate_startup()
                .is_ok()
        );
        assert!(
            config_for_bind("127.0.0.1:8787", None)
                .validate_startup()
                .is_ok()
        );
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
}
