use axum::http::{HeaderMap, header};

use crate::{AppConfig, ImageGatewayError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthContext {
    pub tenant_id: String,
    pub project_id: String,
    pub api_key_id: Option<String>,
    pub is_admin: bool,
}

pub fn authorize_legacy(
    headers: &HeaderMap,
    config: &AppConfig,
) -> Result<AuthContext, ImageGatewayError> {
    let Some(expected) = config.auth_token.as_deref() else {
        return Ok(AuthContext::legacy_default());
    };

    let token = bearer_token(headers)?;
    if !expected.trim().is_empty() && constant_time_eq(token.as_bytes(), expected.as_bytes()) {
        Ok(AuthContext::legacy_default())
    } else {
        Err(ImageGatewayError::authentication())
    }
}

pub fn authorize_admin(headers: &HeaderMap, config: &AppConfig) -> Result<(), ImageGatewayError> {
    let Some(expected) = config.admin_token.as_deref() else {
        return Err(ImageGatewayError::authentication());
    };

    let token = bearer_token(headers)?;
    if !expected.trim().is_empty() && constant_time_eq(token.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(ImageGatewayError::authentication())
    }
}

pub fn bearer_token(headers: &HeaderMap) -> Result<&str, ImageGatewayError> {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(ImageGatewayError::authentication());
    };

    let Some(token) = value.strip_prefix("Bearer ") else {
        return Err(ImageGatewayError::authentication());
    };

    Ok(token)
}

impl AuthContext {
    pub fn legacy_default() -> Self {
        Self {
            tenant_id: "tenant_default".to_string(),
            project_id: "proj_default".to_string(),
            api_key_id: None,
            is_admin: false,
        }
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for idx in 0..max_len {
        let left_byte = left.get(idx).copied().unwrap_or(0);
        let right_byte = right.get(idx).copied().unwrap_or(0);
        diff |= usize::from(left_byte ^ right_byte);
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::SocketAddr, time::Duration};

    fn config_with_token(token: &str) -> AppConfig {
        AppConfig {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            auth_token: Some(token.to_string()),
            admin_token: Some(token.to_string()),
            database_url: None,
            generation_admission_contract: Default::default(),
            five_hour_image_limit: 1,
            seven_day_image_limit: 1,
            max_concurrent_jobs: 1,
            max_queue_size: 0,
            max_concurrent_jobs_per_tenant: 1,
            max_queue_size_per_tenant: 0,
            queue_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            max_upload_bytes: 1024,
            proxy: Default::default(),
            codex_home: None,
            cleanup_codex_outputs: false,
        }
    }

    #[test]
    fn rejects_empty_configured_token() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer ".parse().unwrap());

        assert!(authorize_legacy(&headers, &config_with_token("")).is_err());
    }

    #[test]
    fn compares_bearer_token_exactly() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer secret".parse().unwrap());

        assert!(authorize_legacy(&headers, &config_with_token("secret")).is_ok());
        assert!(authorize_legacy(&headers, &config_with_token("Secret")).is_err());
    }
}
