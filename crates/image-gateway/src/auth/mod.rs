use std::collections::BTreeMap;

use axum::http::{HeaderMap, header};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{AppConfig, ImageGatewayError, service_tiers::ProjectServiceTier};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestRouteAttribution {
    pub public_model_id: String,
    pub api_profile: String,
    pub provider_id: String,
    pub operation_id: String,
    pub command_schema: String,
    pub media_kind: String,
    pub route_id: Uuid,
    pub route_revision: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthContext {
    pub tenant_id: String,
    pub project_id: String,
    pub project_service_tier: ProjectServiceTier,
    pub service_account_id: Option<String>,
    pub api_key_id: Option<String>,
    pub credential_authz_version: Option<i64>,
    pub credential_owner_user_id: Option<Uuid>,
    pub actor_user_id: Option<Uuid>,
    pub actor_session_id: Option<Uuid>,
    pub actor_authz_version: Option<i64>,
    pub api_key_permission_mode: ApiKeyPermissionMode,
    pub api_key_permissions: ApiKeyPermissions,
    pub route: Option<RequestRouteAttribution>,
    pub is_admin: bool,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiKeyPermissionMode {
    #[default]
    All,
    Restricted,
    ReadOnly,
}

impl ApiKeyPermissionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Restricted => "restricted",
            Self::ReadOnly => "read_only",
        }
    }

    pub fn from_database(value: &str) -> Option<Self> {
        match value {
            "all" => Some(Self::All),
            "restricted" => Some(Self::Restricted),
            "read_only" => Some(Self::ReadOnly),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiKeyPermissionLevel {
    #[default]
    None,
    Read,
    Write,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(transparent)]
pub struct ApiKeyPermissions(pub BTreeMap<String, ApiKeyPermissionLevel>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiKeyCapability {
    ModelsRead,
    ImagesWrite,
    VideosRead,
    VideosWrite,
    FilesRead,
    FilesWrite,
    BatchesRead,
    BatchesWrite,
}

impl ApiKeyCapability {
    fn resource(self) -> &'static str {
        match self {
            Self::ModelsRead => "models",
            Self::ImagesWrite => "images",
            Self::VideosRead | Self::VideosWrite => "videos",
            Self::FilesRead | Self::FilesWrite => "files",
            Self::BatchesRead | Self::BatchesWrite => "batches",
        }
    }

    fn is_write(self) -> bool {
        matches!(
            self,
            Self::ImagesWrite | Self::VideosWrite | Self::FilesWrite | Self::BatchesWrite
        )
    }
}

impl ApiKeyPermissions {
    pub fn validate(&self) -> bool {
        self.0.keys().all(|resource| {
            matches!(
                resource.as_str(),
                "models" | "images" | "videos" | "files" | "batches"
            )
        })
    }

    fn allows(&self, capability: ApiKeyCapability) -> bool {
        match self
            .0
            .get(capability.resource())
            .copied()
            .unwrap_or_default()
        {
            ApiKeyPermissionLevel::None => false,
            ApiKeyPermissionLevel::Read => !capability.is_write(),
            ApiKeyPermissionLevel::Write => true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestAttribution {
    pub project_id: String,
    pub service_account_id: Option<String>,
    pub api_key_id: Option<String>,
    pub credential_authz_version: Option<i64>,
    pub credential_owner_user_id: Option<Uuid>,
    pub actor_user_id: Option<Uuid>,
    pub actor_session_id: Option<Uuid>,
    pub actor_authz_version: Option<i64>,
    pub route: Option<RequestRouteAttribution>,
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
    pub fn require_api_key_capability(
        &self,
        capability: ApiKeyCapability,
    ) -> Result<(), ImageGatewayError> {
        let permitted = match self.api_key_permission_mode {
            ApiKeyPermissionMode::All => true,
            ApiKeyPermissionMode::ReadOnly => !capability.is_write(),
            ApiKeyPermissionMode::Restricted => self.api_key_permissions.allows(capability),
        };
        if permitted {
            Ok(())
        } else {
            Err(ImageGatewayError::forbidden(
                "API key permission does not allow this operation",
            ))
        }
    }

    pub fn attribution(&self) -> RequestAttribution {
        RequestAttribution {
            project_id: self.project_id.clone(),
            service_account_id: self.service_account_id.clone(),
            api_key_id: self.api_key_id.clone(),
            credential_authz_version: self.credential_authz_version,
            credential_owner_user_id: self.credential_owner_user_id,
            actor_user_id: self.actor_user_id,
            actor_session_id: self.actor_session_id,
            actor_authz_version: self.actor_authz_version,
            route: self.route.clone(),
        }
    }

    pub fn legacy_default() -> Self {
        Self {
            tenant_id: "tenant_default".to_string(),
            project_id: "proj_default".to_string(),
            project_service_tier: ProjectServiceTier::Default,
            service_account_id: None,
            api_key_id: None,
            credential_authz_version: None,
            credential_owner_user_id: None,
            actor_user_id: None,
            actor_session_id: None,
            actor_authz_version: None,
            api_key_permission_mode: ApiKeyPermissionMode::All,
            api_key_permissions: ApiKeyPermissions::default(),
            route: None,
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
            legacy_admin_auth_enabled: true,
            database_url: None,
            generation_admission_contract: Default::default(),
            enable_xai_video_api: false,
            five_hour_image_limit: 1,
            seven_day_image_limit: 1,
            five_hour_video_second_limit: i32::MAX as u32,
            seven_day_video_second_limit: i32::MAX as u32,
            max_concurrent_jobs: 1,
            max_queue_size: 0,
            max_concurrent_jobs_per_tenant: 1,
            max_queue_size_per_tenant: 0,
            queue_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            readiness_timeout: Duration::from_millis(500),
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
