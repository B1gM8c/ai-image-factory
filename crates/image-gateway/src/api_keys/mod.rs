use std::{
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    auth::{ApiKeyPermissionMode, ApiKeyPermissions, AuthContext},
    service_tiers::ProjectServiceTier,
};

mod credentials;

pub use credentials::ApiKeyKeyring;
use credentials::{HMAC_ALGORITHM, LEGACY_ALGORITHM, key_id_from_token, new_key_value};

const MAX_NAME_CHARS: usize = 128;
const LAST_USED_COALESCE_SECONDS: i64 = 60;

#[async_trait]
pub trait ApiKeyStore: Send + Sync + 'static {
    async fn create_project(&self, name: &str) -> Result<Project, ImageGatewayError>;
    async fn create_project_for_tenant(
        &self,
        tenant_id: &str,
        owner_user_id: Option<Uuid>,
        name: &str,
    ) -> Result<Project, ImageGatewayError>;

    async fn list_projects(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectList, ImageGatewayError>;
    async fn list_projects_for_ids(
        &self,
        project_ids: &[String],
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectList, ImageGatewayError>;
    async fn get_project(&self, project_id: &str) -> Result<Project, ImageGatewayError>;
    async fn update_project_settings(
        &self,
        project_id: &str,
        actor_user_id: Uuid,
        name: &str,
        service_tier: ProjectServiceTier,
        user_api_keys_disabled: bool,
        expected_settings_version: i64,
    ) -> Result<Project, ImageGatewayError>;

    async fn project_tenant(&self, project_id: &str) -> Result<Option<String>, ImageGatewayError>;
    async fn project_runtime_defaults(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectRuntimeDefaults>, ImageGatewayError>;

    async fn create_service_account(
        &self,
        project_id: &str,
        name: &str,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<ProjectServiceAccount, ImageGatewayError>;

    async fn create_service_account_for_actor(
        &self,
        project_id: &str,
        name: &str,
        actor_user_id: Uuid,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        let _ = actor_user_id;
        self.create_service_account(project_id, name, permission_mode, permissions)
            .await
    }

    async fn create_service_account_with_route(
        &self,
        project_id: &str,
        name: &str,
        route_id: Uuid,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        let _ = (project_id, name, route_id, permission_mode, permissions);
        Err(ImageGatewayError::service_unavailable(
            "routed API key creation is not configured",
        ))
    }

    async fn create_service_account_with_route_for_actor(
        &self,
        project_id: &str,
        name: &str,
        route_id: Uuid,
        actor_user_id: Uuid,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        let _ = actor_user_id;
        self.create_service_account_with_route(
            project_id,
            name,
            route_id,
            permission_mode,
            permissions,
        )
        .await
    }

    async fn create_user_api_key(
        &self,
        project_id: &str,
        owner_user_id: Uuid,
        owner_name: &str,
        owner_email: &str,
        name: &str,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<CreatedProjectApiKey, ImageGatewayError>;

    async fn authenticate(&self, bearer: &str) -> Result<Option<AuthContext>, ImageGatewayError>;

    async fn list_project_api_keys(
        &self,
        project_id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectApiKeyList, ImageGatewayError>;

    async fn list_project_api_keys_for_user(
        &self,
        project_id: &str,
        owner_user_id: Uuid,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectApiKeyList, ImageGatewayError>;

    async fn delete_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
    ) -> Result<ProjectApiKeyDeleted, ImageGatewayError>;

    async fn delete_user_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
        owner_user_id: Uuid,
    ) -> Result<ProjectApiKeyDeleted, ImageGatewayError>;

    async fn update_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
        actor_user_id: Uuid,
        can_manage_shared_credentials: bool,
        name: &str,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<UpdatedProjectApiKey, ImageGatewayError>;

    async fn rotate_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
        actor_user_id: Uuid,
        can_manage_shared_credentials: bool,
    ) -> Result<RotatedProjectApiKey, ImageGatewayError>;

    async fn delete_project_api_key_for_actor(
        &self,
        project_id: &str,
        api_key_id: &str,
        actor_user_id: Uuid,
    ) -> Result<ProjectApiKeyDeleted, ImageGatewayError> {
        let _ = actor_user_id;
        self.delete_project_api_key(project_id, api_key_id).await
    }

    async fn delete_service_account(
        &self,
        project_id: &str,
        service_account_id: &str,
    ) -> Result<ProjectServiceAccountDeleted, ImageGatewayError>;

    async fn delete_service_account_for_actor(
        &self,
        project_id: &str,
        service_account_id: &str,
        actor_user_id: Uuid,
    ) -> Result<ProjectServiceAccountDeleted, ImageGatewayError> {
        let _ = actor_user_id;
        self.delete_service_account(project_id, service_account_id)
            .await
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct Project {
    #[schema(value_type = String)]
    pub object: &'static str,
    pub id: String,
    pub name: String,
    pub created_at: i64,
    pub archived_at: Option<i64>,
    pub service_tier: ProjectServiceTier,
    pub user_api_keys_disabled: bool,
    pub settings_version: i64,
    #[schema(value_type = String)]
    pub status: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectRuntimeDefaults {
    pub tenant_id: String,
    pub service_tier: ProjectServiceTier,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProjectList {
    #[schema(value_type = String)]
    pub object: &'static str,
    pub data: Vec<Project>,
    pub has_more: bool,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProjectServiceAccount {
    #[schema(value_type = String)]
    pub object: &'static str,
    pub id: String,
    pub name: String,
    #[schema(value_type = String)]
    pub role: &'static str,
    pub created_at: i64,
    pub api_key: CreatedProjectApiKey,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct CreatedProjectApiKey {
    #[schema(value_type = String)]
    pub object: &'static str,
    pub value: String,
    pub name: String,
    pub created_at: i64,
    pub id: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectApiKey {
    #[schema(value_type = String)]
    pub object: &'static str,
    pub redacted_value: String,
    pub name: String,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
    pub expires_at: Option<i64>,
    pub id: String,
    pub owner: ProjectApiKeyOwner,
    pub provider_routes: Vec<ProjectApiKeyProviderRoute>,
    pub permission_mode: ApiKeyPermissionMode,
    pub permissions: ApiKeyPermissions,
    #[schema(value_type = String)]
    pub owner_project_access: &'static str,
    #[schema(value_type = String)]
    pub status: &'static str,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ProjectApiKeyProviderRoute {
    pub route_id: String,
    pub route_revision: i64,
    pub display_name: String,
    pub route_kind: String,
    pub provider_id: String,
    pub operation_id: String,
    pub model_count: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectApiKeyOwner {
    #[serde(rename = "type")]
    #[schema(value_type = String)]
    pub owner_type: &'static str,
    pub service_account: Option<ProjectApiKeyServiceAccountOwner>,
    pub user: Option<ProjectApiKeyUserOwner>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectApiKeyServiceAccountOwner {
    pub id: String,
    pub name: String,
    #[schema(value_type = String)]
    pub role: &'static str,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectApiKeyUserOwner {
    pub id: String,
    pub name: String,
    pub email: String,
    #[schema(value_type = String)]
    pub role: &'static str,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProjectApiKeyList {
    #[schema(value_type = String)]
    pub object: &'static str,
    pub data: Vec<ProjectApiKey>,
    pub has_more: bool,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProjectApiKeyDeleted {
    #[schema(value_type = String)]
    pub object: &'static str,
    pub id: String,
    pub deleted: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct UpdatedProjectApiKey {
    #[schema(value_type = String)]
    pub object: &'static str,
    pub id: String,
    pub name: String,
    pub permission_mode: ApiKeyPermissionMode,
    pub permissions: ApiKeyPermissions,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RotatedProjectApiKey {
    #[schema(value_type = String)]
    pub object: &'static str,
    pub replaced_api_key_id: String,
    pub api_key: CreatedProjectApiKey,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProjectServiceAccountDeleted {
    #[schema(value_type = String)]
    pub object: &'static str,
    pub id: String,
    pub deleted: bool,
}

pub struct InMemoryApiKeyStore {
    state: Mutex<InMemoryApiKeyState>,
    keyring: ApiKeyKeyring,
}

impl Default for InMemoryApiKeyStore {
    fn default() -> Self {
        Self::new(ApiKeyKeyring::ephemeral())
    }
}

impl InMemoryApiKeyStore {
    pub fn new(keyring: ApiKeyKeyring) -> Self {
        Self {
            state: Mutex::new(InMemoryApiKeyState::with_default_project()),
            keyring,
        }
    }
}

struct InMemoryApiKeyState {
    projects: Vec<StoredProject>,
    service_accounts: Vec<StoredServiceAccount>,
    api_keys: Vec<StoredApiKey>,
}

impl InMemoryApiKeyState {
    fn with_default_project() -> Self {
        Self {
            projects: vec![StoredProject {
                id: "proj_default".to_string(),
                tenant_id: "tenant_default".to_string(),
                name: "Default project".to_string(),
                created_at: now_seconds(),
                archived_at: None,
                service_tier: ProjectServiceTier::Default,
                user_api_keys_disabled: false,
                settings_version: 1,
            }],
            service_accounts: Vec::new(),
            api_keys: Vec::new(),
        }
    }
}

#[derive(Clone)]
struct StoredProject {
    id: String,
    tenant_id: String,
    name: String,
    created_at: i64,
    archived_at: Option<i64>,
    service_tier: ProjectServiceTier,
    user_api_keys_disabled: bool,
    settings_version: i64,
}

#[derive(Clone)]
struct StoredServiceAccount {
    id: String,
    project_id: String,
    name: String,
    created_at: i64,
    owner_user_id: Option<Uuid>,
    owner_email: Option<String>,
    deleted: bool,
}

#[derive(Clone)]
struct StoredApiKey {
    id: String,
    project_id: String,
    service_account_id: String,
    name: String,
    hash: String,
    pepper_version: u16,
    redacted_value: String,
    created_at: i64,
    last_used_at: Option<i64>,
    expires_at: Option<i64>,
    permission_mode: ApiKeyPermissionMode,
    permissions: ApiKeyPermissions,
    deleted: bool,
    authz_version: i64,
}

#[async_trait]
impl ApiKeyStore for InMemoryApiKeyStore {
    async fn create_project(&self, name: &str) -> Result<Project, ImageGatewayError> {
        let name = validate_name(name)?;
        let id = new_id("proj");
        let project = StoredProject {
            tenant_id: id.clone(),
            id,
            name,
            created_at: now_seconds(),
            archived_at: None,
            service_tier: ProjectServiceTier::Default,
            user_api_keys_disabled: false,
            settings_version: 1,
        };
        let response = project_from_memory(&project);
        self.state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?
            .projects
            .push(project);
        Ok(response)
    }

    async fn create_project_for_tenant(
        &self,
        tenant_id: &str,
        _owner_user_id: Option<Uuid>,
        name: &str,
    ) -> Result<Project, ImageGatewayError> {
        let name = validate_name(name)?;
        let id = new_id("proj");
        let project = StoredProject {
            tenant_id: tenant_id.to_string(),
            id,
            name,
            created_at: now_seconds(),
            archived_at: None,
            service_tier: ProjectServiceTier::Default,
            user_api_keys_disabled: false,
            settings_version: 1,
        };
        let response = project_from_memory(&project);
        self.state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?
            .projects
            .push(project);
        Ok(response)
    }

    async fn list_projects(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectList, ImageGatewayError> {
        let limit = limit.clamp(1, 100);
        let state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let start = match after {
            Some(after) => state
                .projects
                .iter()
                .position(|project| project.id == after)
                .map(|index| index + 1)
                .ok_or_else(|| project_cursor_not_found(after))?,
            None => 0,
        };
        let mut data = state
            .projects
            .iter()
            .skip(start)
            .take(limit + 1)
            .map(project_from_memory)
            .collect::<Vec<_>>();
        let has_more = data.len() > limit;
        data.truncate(limit);
        Ok(project_list(data, has_more))
    }

    async fn list_projects_for_ids(
        &self,
        project_ids: &[String],
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectList, ImageGatewayError> {
        let limit = limit.clamp(1, 100);
        let state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let mut projects = state
            .projects
            .iter()
            .filter(|project| project_ids.contains(&project.id))
            .collect::<Vec<_>>();
        projects.sort_by_key(|project| (project.created_at, project.id.clone()));
        if let Some(after) = after
            && !projects.iter().any(|project| project.id == after)
        {
            return Err(project_cursor_not_found(after));
        }
        let start = after
            .and_then(|cursor| projects.iter().position(|project| project.id == cursor))
            .map_or(0, |position| position + 1);
        let has_more = projects.len().saturating_sub(start) > limit;
        let data = projects
            .into_iter()
            .skip(start)
            .take(limit)
            .map(project_from_memory)
            .collect();
        Ok(project_list(data, has_more))
    }

    async fn get_project(&self, project_id: &str) -> Result<Project, ImageGatewayError> {
        validate_project_id(project_id)?;
        let state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        state
            .projects
            .iter()
            .find(|project| project.id == project_id)
            .map(project_from_memory)
            .ok_or_else(|| project_not_found(project_id))
    }

    async fn update_project_settings(
        &self,
        project_id: &str,
        _actor_user_id: Uuid,
        name: &str,
        service_tier: ProjectServiceTier,
        user_api_keys_disabled: bool,
        expected_settings_version: i64,
    ) -> Result<Project, ImageGatewayError> {
        validate_project_id(project_id)?;
        let name = validate_name(name)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let project = state
            .projects
            .iter_mut()
            .find(|project| project.id == project_id && project.archived_at.is_none())
            .ok_or_else(|| project_not_found(project_id))?;
        if project.settings_version != expected_settings_version {
            return Err(project_settings_conflict());
        }
        project.name = name;
        project.service_tier = service_tier;
        project.user_api_keys_disabled = user_api_keys_disabled;
        project.settings_version = project.settings_version.saturating_add(1);
        Ok(project_from_memory(project))
    }

    async fn project_tenant(&self, project_id: &str) -> Result<Option<String>, ImageGatewayError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?
            .projects
            .iter()
            .find(|project| project.id == project_id)
            .map(|project| project.tenant_id.clone()))
    }

    async fn project_runtime_defaults(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectRuntimeDefaults>, ImageGatewayError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?
            .projects
            .iter()
            .find(|project| project.id == project_id && project.archived_at.is_none())
            .map(|project| ProjectRuntimeDefaults {
                tenant_id: project.tenant_id.clone(),
                service_tier: project.service_tier,
            }))
    }

    async fn create_service_account(
        &self,
        project_id: &str,
        name: &str,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        validate_project_id(project_id)?;
        let name = validate_name(name)?;
        validate_permissions(permission_mode, &permissions)?;
        let created_at = now_seconds();
        let service_account_id = new_id("svc_acct");
        let key_id = new_id("key");
        let value = new_key_value(&key_id);
        let hash = self.keyring.digest_current(&value);
        let redacted_value = redact_key(&value);

        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        if !state
            .projects
            .iter()
            .any(|project| project.id == project_id && project.archived_at.is_none())
        {
            return Err(project_not_found(project_id));
        }
        state.service_accounts.push(StoredServiceAccount {
            id: service_account_id.clone(),
            project_id: project_id.to_string(),
            name: name.clone(),
            created_at,
            owner_user_id: None,
            owner_email: None,
            deleted: false,
        });
        state.api_keys.push(StoredApiKey {
            id: key_id.clone(),
            project_id: project_id.to_string(),
            service_account_id: service_account_id.clone(),
            name: "Secret Key".to_string(),
            hash,
            pepper_version: self.keyring.current_version(),
            redacted_value,
            created_at,
            last_used_at: None,
            expires_at: None,
            permission_mode,
            permissions,
            deleted: false,
            authz_version: 1,
        });

        Ok(ProjectServiceAccount {
            object: "organization.project.service_account",
            id: service_account_id,
            name,
            role: "member",
            created_at,
            api_key: CreatedProjectApiKey {
                object: "organization.project.service_account.api_key",
                value,
                name: "Secret Key".to_string(),
                created_at,
                id: key_id,
            },
        })
    }

    async fn create_user_api_key(
        &self,
        project_id: &str,
        owner_user_id: Uuid,
        owner_name: &str,
        owner_email: &str,
        name: &str,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<CreatedProjectApiKey, ImageGatewayError> {
        validate_project_id(project_id)?;
        let owner_name = validate_name(owner_name)?;
        let name = validate_name(name)?;
        validate_permissions(permission_mode, &permissions)?;
        let created_at = now_seconds();
        let key_id = new_id("key");
        let value = new_key_value(&key_id);
        let hash = self.keyring.digest_current(&value);
        let redacted_value = redact_key(&value);

        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let project = state
            .projects
            .iter()
            .find(|project| project.id == project_id && project.archived_at.is_none())
            .ok_or_else(|| project_not_found(project_id))?;
        if project.user_api_keys_disabled {
            return Err(ImageGatewayError::user_api_keys_disabled());
        }
        let service_account_id = state
            .service_accounts
            .iter()
            .find(|account| {
                !account.deleted
                    && account.project_id == project_id
                    && account.owner_user_id == Some(owner_user_id)
            })
            .map(|account| account.id.clone())
            .unwrap_or_else(|| {
                let id = new_id("user_acct");
                state.service_accounts.push(StoredServiceAccount {
                    id: id.clone(),
                    project_id: project_id.to_string(),
                    name: owner_name,
                    created_at,
                    owner_user_id: Some(owner_user_id),
                    owner_email: Some(owner_email.to_string()),
                    deleted: false,
                });
                id
            });
        state.api_keys.push(StoredApiKey {
            id: key_id.clone(),
            project_id: project_id.to_string(),
            service_account_id,
            name: name.clone(),
            hash,
            pepper_version: self.keyring.current_version(),
            redacted_value,
            created_at,
            last_used_at: None,
            expires_at: None,
            permission_mode,
            permissions,
            deleted: false,
            authz_version: 1,
        });
        Ok(CreatedProjectApiKey {
            object: "organization.project.api_key",
            value,
            name,
            created_at,
            id: key_id,
        })
    }

    async fn authenticate(&self, bearer: &str) -> Result<Option<AuthContext>, ImageGatewayError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let Some(key_id) = key_id_from_token(bearer) else {
            return Ok(None);
        };
        let Some(api_key_index) = state
            .api_keys
            .iter()
            .position(|api_key| !api_key.deleted && api_key.id == key_id)
        else {
            return Ok(None);
        };
        let api_key = state.api_keys[api_key_index].clone();
        let Some(service_account) = state.service_accounts.iter().find(|account| {
            !account.deleted
                && account.id == api_key.service_account_id
                && account.project_id == api_key.project_id
        }) else {
            return Ok(None);
        };
        let Some(project) = state
            .projects
            .iter()
            .find(|project| project.id == api_key.project_id && project.archived_at.is_none())
        else {
            return Ok(None);
        };
        let service_account_id = service_account.id.clone();
        let credential_owner_user_id = service_account.owner_user_id;
        let tenant_id = project.tenant_id.clone();
        let project_service_tier = project.service_tier;
        if credential_owner_user_id.is_some() && project.user_api_keys_disabled {
            return Ok(None);
        }
        if !self
            .keyring
            .verify(api_key.pepper_version, bearer, &api_key.hash)
        {
            return Ok(None);
        }
        state.api_keys[api_key_index].last_used_at = Some(now_seconds());
        Ok(Some(AuthContext {
            tenant_id,
            project_id: api_key.project_id.clone(),
            project_service_tier,
            service_account_id: Some(service_account_id),
            api_key_id: Some(api_key.id.clone()),
            credential_authz_version: Some(api_key.authz_version),
            credential_owner_user_id,
            actor_user_id: None,
            actor_session_id: None,
            actor_authz_version: None,
            api_key_permission_mode: api_key.permission_mode,
            api_key_permissions: api_key.permissions,
            route: None,
            is_admin: false,
        }))
    }

    async fn list_project_api_keys(
        &self,
        project_id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectApiKeyList, ImageGatewayError> {
        validate_project_id(project_id)?;
        let limit = limit.clamp(1, 100);
        let state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let project_keys = state
            .api_keys
            .iter()
            .filter(|api_key| {
                !api_key.deleted
                    && api_key.project_id == project_id
                    && state.service_accounts.iter().any(|account| {
                        !account.deleted
                            && account.id == api_key.service_account_id
                            && account.project_id == project_id
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        let start = match after {
            Some(after) => project_keys
                .iter()
                .position(|api_key| api_key.id == after)
                .map(|index| index + 1)
                .ok_or_else(|| api_key_cursor_not_found(after))?,
            None => 0,
        };
        let mut keys = project_keys
            .into_iter()
            .skip(start)
            .take(limit + 1)
            .collect::<Vec<_>>();
        let has_more = keys.len() > limit;
        keys.truncate(limit);
        let data = keys
            .into_iter()
            .filter_map(|api_key| project_api_key_from_memory(&state, api_key))
            .collect::<Vec<_>>();
        Ok(project_api_key_list(data, has_more))
    }

    async fn list_project_api_keys_for_user(
        &self,
        project_id: &str,
        owner_user_id: Uuid,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectApiKeyList, ImageGatewayError> {
        validate_project_id(project_id)?;
        let limit = limit.clamp(1, 100);
        let state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let project_keys = state
            .api_keys
            .iter()
            .filter(|api_key| {
                !api_key.deleted
                    && api_key.project_id == project_id
                    && state.service_accounts.iter().any(|account| {
                        !account.deleted
                            && account.id == api_key.service_account_id
                            && account.owner_user_id == Some(owner_user_id)
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        let start = match after {
            Some(after) => project_keys
                .iter()
                .position(|api_key| api_key.id == after)
                .map(|index| index + 1)
                .ok_or_else(|| api_key_cursor_not_found(after))?,
            None => 0,
        };
        let mut keys = project_keys
            .into_iter()
            .skip(start)
            .take(limit + 1)
            .collect::<Vec<_>>();
        let has_more = keys.len() > limit;
        keys.truncate(limit);
        let data = keys
            .into_iter()
            .filter_map(|api_key| project_api_key_from_memory(&state, api_key))
            .collect::<Vec<_>>();
        Ok(project_api_key_list(data, has_more))
    }

    async fn delete_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
    ) -> Result<ProjectApiKeyDeleted, ImageGatewayError> {
        validate_project_id(project_id)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let Some(api_key) = state.api_keys.iter_mut().find(|api_key| {
            !api_key.deleted && api_key.project_id == project_id && api_key.id == api_key_id
        }) else {
            return Err(ImageGatewayError::not_found(
                "API key not found",
                Some("api_key_id".to_string()),
                "not_found",
            ));
        };
        api_key.deleted = true;
        api_key.authz_version = api_key.authz_version.saturating_add(1);
        Ok(ProjectApiKeyDeleted {
            object: "organization.project.api_key.deleted",
            id: api_key_id.to_string(),
            deleted: true,
        })
    }

    async fn delete_user_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
        owner_user_id: Uuid,
    ) -> Result<ProjectApiKeyDeleted, ImageGatewayError> {
        validate_project_id(project_id)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let owned_account_ids = state
            .service_accounts
            .iter()
            .filter(|account| {
                !account.deleted
                    && account.project_id == project_id
                    && account.owner_user_id == Some(owner_user_id)
            })
            .map(|account| account.id.clone())
            .collect::<Vec<_>>();
        let Some(api_key) = state.api_keys.iter_mut().find(|api_key| {
            !api_key.deleted
                && api_key.project_id == project_id
                && api_key.id == api_key_id
                && owned_account_ids.contains(&api_key.service_account_id)
        }) else {
            return Err(api_key_not_found(api_key_id));
        };
        api_key.deleted = true;
        api_key.authz_version = api_key.authz_version.saturating_add(1);
        Ok(ProjectApiKeyDeleted {
            object: "organization.project.api_key.deleted",
            id: api_key_id.to_string(),
            deleted: true,
        })
    }

    async fn update_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
        actor_user_id: Uuid,
        can_manage_shared_credentials: bool,
        name: &str,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<UpdatedProjectApiKey, ImageGatewayError> {
        validate_project_id(project_id)?;
        let name = validate_name(name)?;
        validate_permissions(permission_mode, &permissions)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let Some(api_key_index) = state.api_keys.iter().position(|api_key| {
            !api_key.deleted && api_key.project_id == project_id && api_key.id == api_key_id
        }) else {
            return Err(api_key_not_found(api_key_id));
        };
        let service_account_id = state.api_keys[api_key_index].service_account_id.clone();
        let authorized = state.service_accounts.iter().any(|account| {
            !account.deleted
                && account.project_id == project_id
                && account.id == service_account_id
                && match account.owner_user_id {
                    Some(owner_user_id) => owner_user_id == actor_user_id,
                    None => can_manage_shared_credentials,
                }
        });
        if !authorized {
            return Err(api_key_not_found(api_key_id));
        }
        let api_key = &mut state.api_keys[api_key_index];
        api_key.name = name.clone();
        api_key.permission_mode = permission_mode;
        api_key.permissions = permissions.clone();
        api_key.authz_version = api_key.authz_version.saturating_add(1);
        Ok(UpdatedProjectApiKey {
            object: "organization.project.api_key",
            id: api_key_id.to_string(),
            name,
            permission_mode,
            permissions,
        })
    }

    async fn rotate_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
        actor_user_id: Uuid,
        can_manage_shared_credentials: bool,
    ) -> Result<RotatedProjectApiKey, ImageGatewayError> {
        validate_project_id(project_id)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let Some(api_key_index) = state.api_keys.iter().position(|api_key| {
            !api_key.deleted && api_key.project_id == project_id && api_key.id == api_key_id
        }) else {
            return Err(api_key_not_found(api_key_id));
        };
        let old = state.api_keys[api_key_index].clone();
        let authorized = state.service_accounts.iter().any(|account| {
            !account.deleted
                && account.project_id == project_id
                && account.id == old.service_account_id
                && match account.owner_user_id {
                    Some(owner_user_id) => owner_user_id == actor_user_id,
                    None => can_manage_shared_credentials,
                }
        });
        if !authorized {
            return Err(api_key_not_found(api_key_id));
        }

        let created_at = now_seconds();
        let key_id = new_id("key");
        let value = new_key_value(&key_id);
        state.api_keys[api_key_index].deleted = true;
        state.api_keys[api_key_index].authz_version = state.api_keys[api_key_index]
            .authz_version
            .saturating_add(1);
        state.api_keys.push(StoredApiKey {
            id: key_id.clone(),
            project_id: old.project_id,
            service_account_id: old.service_account_id,
            name: old.name.clone(),
            hash: self.keyring.digest_current(&value),
            pepper_version: self.keyring.current_version(),
            redacted_value: redact_key(&value),
            created_at,
            last_used_at: None,
            expires_at: old.expires_at,
            permission_mode: old.permission_mode,
            permissions: old.permissions,
            deleted: false,
            authz_version: 1,
        });
        Ok(RotatedProjectApiKey {
            object: "organization.project.api_key.rotation",
            replaced_api_key_id: api_key_id.to_string(),
            api_key: CreatedProjectApiKey {
                object: "organization.project.api_key",
                value,
                name: old.name,
                created_at,
                id: key_id,
            },
        })
    }

    async fn delete_service_account(
        &self,
        project_id: &str,
        service_account_id: &str,
    ) -> Result<ProjectServiceAccountDeleted, ImageGatewayError> {
        validate_project_id(project_id)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let Some(account) = state.service_accounts.iter_mut().find(|account| {
            account.project_id == project_id && account.id == service_account_id && !account.deleted
        }) else {
            return Err(service_account_not_found(service_account_id));
        };
        account.deleted = true;
        for key in &mut state.api_keys {
            if key.project_id == project_id && key.service_account_id == service_account_id {
                key.deleted = true;
            }
        }
        Ok(ProjectServiceAccountDeleted {
            object: "organization.project.service_account.deleted",
            id: service_account_id.to_string(),
            deleted: true,
        })
    }
}

#[derive(Clone)]
pub struct PostgresApiKeyStore {
    pool: PgPool,
    keyring: ApiKeyKeyring,
}

impl PostgresApiKeyStore {
    pub fn new(pool: PgPool, keyring: ApiKeyKeyring) -> Self {
        Self { pool, keyring }
    }

    async fn create_service_account_internal(
        &self,
        project_id: &str,
        name: &str,
        route_id: Option<Uuid>,
        actor_user_id: Option<Uuid>,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        validate_project_id(project_id)?;
        let name = validate_name(name)?;
        validate_permissions(permission_mode, &permissions)?;
        let created_at = now_seconds();
        let created_at_ms = created_at.saturating_mul(1_000);
        let service_account_id = new_id("svc_acct");
        let key_id = new_id("key");
        let value = new_key_value(&key_id);
        let redacted_value = redact_key(&value);
        let hash = self.keyring.digest_current(&value);
        let pepper_version = i32::from(self.keyring.current_version());
        let permissions_json = serde_json::to_value(&permissions)
            .map_err(|_| ImageGatewayError::internal("failed to encode API key permissions"))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        let tenant_id: Option<String> = sqlx::query_scalar(
            r#"
            SELECT tenant_id
            FROM gateway_projects
            WHERE id = $1 AND archived_at IS NULL
            FOR SHARE
            "#,
        )
        .bind(project_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        let Some(tenant_id) = tenant_id else {
            return Err(project_not_found(project_id));
        };
        let route = if let Some(route_id) = route_id {
            let route = sqlx::query_as::<_, (String, String, String, i64)>(
                r#"
                SELECT head.provider_id, head.operation_id, head.command_schema,
                       head.current_revision
                FROM provider_route_heads head
                WHERE head.route_id = $1
                  AND head.route_kind = 'group'
                  AND head.state = 'enabled'
                FOR SHARE OF head
                "#,
            )
            .bind(route_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("provider route unavailable"))?
            .ok_or_else(|| {
                ImageGatewayError::not_found(
                    "Provider account group not found",
                    Some("route_id".to_string()),
                    "provider_account_group_not_found",
                )
            })?;
            Some((route_id, route))
        } else {
            None
        };
        sqlx::query(
            r#"
            INSERT INTO gateway_service_accounts
              (id, project_id, tenant_id, name, role, created_at)
            VALUES ($1, $2, $3, $4, 'member', $5)
            "#,
        )
        .bind(&service_account_id)
        .bind(project_id)
        .bind(&tenant_id)
        .bind(&name)
        .bind(created_at)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;

        sqlx::query(
            r#"
            INSERT INTO gateway_api_keys
              (id, project_id, tenant_id, service_account_id, name, key_hash,
               hash_algorithm, pepper_version, redacted_value, created_at,
               created_by_user_id, permission_mode, permissions)
            VALUES ($1, $2, $3, $4, 'Secret Key', $5, $6, $7, $8, $9, $10,
                    $11, $12)
            "#,
        )
        .bind(&key_id)
        .bind(project_id)
        .bind(&tenant_id)
        .bind(&service_account_id)
        .bind(hash)
        .bind(HMAC_ALGORITHM)
        .bind(pepper_version)
        .bind(&redacted_value)
        .bind(created_at)
        .bind(actor_user_id)
        .bind(permission_mode.as_str())
        .bind(permissions_json)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;

        if let Some((route_id, (provider_id, operation_id, command_schema, revision))) = route {
            sqlx::query(
                r#"
                INSERT INTO gateway_api_key_provider_routes
                  (api_key_id, service_account_id, project_id, tenant_id,
                   provider_id, operation_id, command_schema, route_id,
                   route_revision, bound_at_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
            )
            .bind(&key_id)
            .bind(&service_account_id)
            .bind(project_id)
            .bind(&tenant_id)
            .bind(provider_id)
            .bind(operation_id)
            .bind(command_schema)
            .bind(route_id)
            .bind(revision)
            .bind(created_at_ms)
            .execute(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("provider route unavailable"))?;
        }

        if let Some(actor_user_id) = actor_user_id {
            sqlx::query(
                r#"
                INSERT INTO identity_audit_events
                  (event_id, actor_user_id, action, resource_type, resource_id,
                   outcome, metadata, created_at_ms)
                VALUES ($1, $2, 'project.service_account.create', 'service_account', $3,
                        'success', $4, $5)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(actor_user_id)
            .bind(&service_account_id)
            .bind(serde_json::json!({
                "project_id": project_id,
                "api_key_id": key_id,
                "route_id": route_id,
            }))
            .bind(created_at_ms)
            .execute(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("audit state unavailable"))?;
        }

        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;

        Ok(ProjectServiceAccount {
            object: "organization.project.service_account",
            id: service_account_id,
            name,
            role: "member",
            created_at,
            api_key: CreatedProjectApiKey {
                object: "organization.project.service_account.api_key",
                value,
                name: "Secret Key".to_string(),
                created_at,
                id: key_id,
            },
        })
    }

    async fn create_user_api_key_internal(
        &self,
        project_id: &str,
        owner_user_id: Uuid,
        name: &str,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<CreatedProjectApiKey, ImageGatewayError> {
        validate_project_id(project_id)?;
        let name = validate_name(name)?;
        validate_permissions(permission_mode, &permissions)?;
        let created_at = now_seconds();
        let created_at_ms = created_at.saturating_mul(1_000);
        let key_id = new_id("key");
        let value = new_key_value(&key_id);
        let redacted_value = redact_key(&value);
        let hash = self.keyring.digest_current(&value);
        let pepper_version = i32::from(self.keyring.current_version());
        let permissions_json = serde_json::to_value(&permissions)
            .map_err(|_| ImageGatewayError::internal("failed to encode API key permissions"))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        let owner = sqlx::query_as::<_, (String, String, bool)>(
            r#"
            SELECT project.tenant_id, identity.display_name,
                   project.user_api_keys_disabled
            FROM gateway_projects project
            JOIN identity_project_memberships membership
              ON membership.organization_id = project.tenant_id
             AND membership.project_id = project.id
             AND membership.user_id = $2
             AND membership.state = 'active'
            JOIN identity_users identity
              ON identity.user_id = membership.user_id
             AND identity.disabled_at_ms IS NULL
            WHERE project.id = $1 AND project.archived_at IS NULL
            FOR SHARE OF project, membership, identity
            "#,
        )
        .bind(project_id)
        .bind(owner_user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?
        .ok_or_else(|| project_not_found(project_id))?;
        if owner.2 {
            return Err(ImageGatewayError::user_api_keys_disabled());
        }
        let service_account_id: String = sqlx::query_scalar(
            r#"
            INSERT INTO gateway_service_accounts
              (id, project_id, tenant_id, name, role, created_at,
               owner_type, owner_user_id)
            VALUES ($1, $2, $3, $4, 'member', $5, 'user', $6)
            ON CONFLICT (project_id, owner_user_id)
              WHERE owner_type = 'user' AND deleted_at IS NULL
            DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            "#,
        )
        .bind(new_id("user_acct"))
        .bind(project_id)
        .bind(&owner.0)
        .bind(&owner.1)
        .bind(created_at)
        .bind(owner_user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;

        sqlx::query(
            r#"
            INSERT INTO gateway_api_keys
              (id, project_id, tenant_id, service_account_id, name, key_hash,
               hash_algorithm, pepper_version, redacted_value, created_at,
               permission_mode, permissions, created_by_user_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(&key_id)
        .bind(project_id)
        .bind(&owner.0)
        .bind(&service_account_id)
        .bind(&name)
        .bind(hash)
        .bind(HMAC_ALGORITHM)
        .bind(pepper_version)
        .bind(&redacted_value)
        .bind(created_at)
        .bind(permission_mode.as_str())
        .bind(permissions_json)
        .bind(owner_user_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        sqlx::query(
            r#"
            INSERT INTO identity_audit_events
              (event_id, actor_user_id, action, resource_type, resource_id,
               outcome, metadata, created_at_ms)
            VALUES ($1, $2, 'project.api_key.create', 'api_key', $3,
                    'success', $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(owner_user_id)
        .bind(&key_id)
        .bind(serde_json::json!({
            "project_id": project_id,
            "owner_type": "user",
            "permission_mode": permission_mode.as_str(),
        }))
        .bind(created_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("audit state unavailable"))?;
        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;

        Ok(CreatedProjectApiKey {
            object: "organization.project.api_key",
            value,
            name,
            created_at,
            id: key_id,
        })
    }

    async fn delete_service_account_internal(
        &self,
        project_id: &str,
        service_account_id: &str,
        actor_user_id: Option<Uuid>,
    ) -> Result<ProjectServiceAccountDeleted, ImageGatewayError> {
        validate_project_id(project_id)?;
        let deleted_at = now_seconds();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        let account = sqlx::query(
            r#"
            UPDATE gateway_service_accounts
            SET deleted_at = $1
            WHERE project_id = $2
              AND id = $3
              AND owner_type = 'service_account'
              AND deleted_at IS NULL
            "#,
        )
        .bind(deleted_at)
        .bind(project_id)
        .bind(service_account_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        if account.rows_affected() == 0 {
            return Err(service_account_not_found(service_account_id));
        }
        sqlx::query(
            r#"
            UPDATE gateway_api_keys
            SET deleted_at = $1,
                authz_version = authz_version + 1,
                revoked_by_user_id = $4,
                revocation_reason = CASE
                  WHEN $4::UUID IS NULL THEN NULL
                  ELSE 'service_account_deleted'
                END
            WHERE project_id = $2
              AND service_account_id = $3
              AND deleted_at IS NULL
            "#,
        )
        .bind(deleted_at)
        .bind(project_id)
        .bind(service_account_id)
        .bind(actor_user_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        if let Some(actor_user_id) = actor_user_id {
            sqlx::query(
                r#"
                INSERT INTO identity_audit_events
                  (event_id, actor_user_id, action, resource_type, resource_id,
                   outcome, metadata, created_at_ms)
                VALUES ($1, $2, 'project.service_account.delete', 'service_account', $3,
                        'success', $4, $5)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(actor_user_id)
            .bind(service_account_id)
            .bind(serde_json::json!({ "project_id": project_id }))
            .bind(deleted_at.saturating_mul(1_000))
            .execute(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("audit state unavailable"))?;
        }
        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        Ok(ProjectServiceAccountDeleted {
            object: "organization.project.service_account.deleted",
            id: service_account_id.to_string(),
            deleted: true,
        })
    }
}

#[derive(sqlx::FromRow)]
struct CredentialRow {
    id: String,
    tenant_id: String,
    project_id: String,
    project_service_tier: String,
    service_account_id: String,
    key_hash: String,
    hash_algorithm: String,
    pepper_version: Option<i32>,
    last_used_at: Option<i64>,
    authz_version: i64,
    owner_user_id: Option<Uuid>,
    permission_mode: String,
    permissions: serde_json::Value,
}

type ProjectRow = (String, String, i64, Option<i64>, String, bool, i64);

#[derive(sqlx::FromRow)]
struct ProjectApiKeyRow {
    id: String,
    redacted_value: String,
    name: String,
    created_at: i64,
    last_used_at: Option<i64>,
    expires_at: Option<i64>,
    service_account_id: String,
    service_account_name: String,
    service_account_created_at: i64,
    owner_type: String,
    owner_user_id: Option<Uuid>,
    owner_user_name: Option<String>,
    owner_user_email: Option<String>,
    permission_mode: String,
    permissions: serde_json::Value,
    provider_routes: serde_json::Value,
    owner_project_access: bool,
    user_api_keys_disabled: bool,
}

#[async_trait]
impl ApiKeyStore for PostgresApiKeyStore {
    async fn create_project(&self, name: &str) -> Result<Project, ImageGatewayError> {
        let name = validate_name(name)?;
        let id = new_id("proj");
        let created_at = now_seconds();
        sqlx::query(
            r#"
            INSERT INTO gateway_projects (id, tenant_id, name, created_at)
            VALUES ($1, $1, $2, $3)
            "#,
        )
        .bind(&id)
        .bind(&name)
        .bind(created_at)
        .execute(&self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        Ok(Project {
            object: "organization.project",
            id,
            name,
            created_at,
            archived_at: None,
            service_tier: ProjectServiceTier::Default,
            user_api_keys_disabled: false,
            settings_version: 1,
            status: "active",
        })
    }

    async fn create_project_for_tenant(
        &self,
        tenant_id: &str,
        owner_user_id: Option<Uuid>,
        name: &str,
    ) -> Result<Project, ImageGatewayError> {
        let name = validate_name(name)?;
        let id = new_id("proj");
        let created_at = now_seconds();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        let organization_exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
              SELECT 1
              FROM identity_organizations
              WHERE organization_id = $1
            )
            "#,
        )
        .bind(tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        if !organization_exists {
            return Err(organization_not_found());
        }
        let owner_user_id: Option<Uuid> = if let Some(owner_user_id) = owner_user_id {
            sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT membership.user_id
                FROM identity_organization_memberships membership
                JOIN identity_users identity
                  ON identity.user_id = membership.user_id
                 AND identity.disabled_at_ms IS NULL
                WHERE membership.organization_id = $1
                  AND membership.user_id = $2
                  AND membership.role = 'owner'
                  AND membership.state = 'active'
                FOR SHARE OF membership, identity
                "#,
            )
            .bind(tenant_id)
            .bind(owner_user_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?
            .ok_or_else(organization_not_found)
            .map(Some)?
        } else {
            sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT membership.user_id
                FROM identity_organization_memberships membership
                JOIN identity_users identity
                  ON identity.user_id = membership.user_id
                 AND identity.disabled_at_ms IS NULL
                WHERE membership.organization_id = $1
                  AND membership.role = 'owner'
                  AND membership.state = 'active'
                ORDER BY membership.created_at_ms, membership.user_id
                LIMIT 1
                FOR SHARE OF membership, identity
                "#,
            )
            .bind(tenant_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?
        };
        sqlx::query(
            r#"
            INSERT INTO gateway_projects (id, tenant_id, name, created_at)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(&id)
        .bind(tenant_id)
        .bind(&name)
        .bind(created_at)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        if let Some(owner_user_id) = owner_user_id {
            sqlx::query(
                r#"
                INSERT INTO identity_project_memberships
                  (organization_id, project_id, user_id, role, state, is_default,
                   created_at_ms, updated_at_ms)
                VALUES ($1, $2, $3, 'owner', 'active', FALSE, $4, $4)
                "#,
            )
            .bind(tenant_id)
            .bind(&id)
            .bind(owner_user_id)
            .bind(created_at.saturating_mul(1000))
            .execute(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        }
        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        Ok(Project {
            object: "organization.project",
            id,
            name,
            created_at,
            archived_at: None,
            service_tier: ProjectServiceTier::Default,
            user_api_keys_disabled: false,
            settings_version: 1,
            status: "active",
        })
    }

    async fn list_projects(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectList, ImageGatewayError> {
        let limit = limit.clamp(1, 100);
        if let Some(after) = after {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM gateway_projects WHERE id = $1)")
                    .bind(after)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|_| {
                        ImageGatewayError::service_unavailable("project state unavailable")
                    })?;
            if !exists {
                return Err(project_cursor_not_found(after));
            }
        }
        let rows = sqlx::query_as::<_, ProjectRow>(
            r#"
            SELECT id, name, created_at, archived_at,
                   service_tier, user_api_keys_disabled, settings_version
            FROM gateway_projects
            WHERE ($1::TEXT IS NULL OR (created_at, id) > (
                SELECT created_at, id FROM gateway_projects WHERE id = $1
            ))
            ORDER BY created_at ASC, id ASC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind((limit + 1) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        let has_more = rows.len() > limit;
        let data = rows
            .into_iter()
            .take(limit)
            .map(
                |(
                    id,
                    name,
                    created_at,
                    archived_at,
                    service_tier,
                    user_api_keys_disabled,
                    settings_version,
                )| {
                    Project {
                        object: "organization.project",
                        id,
                        name,
                        created_at,
                        archived_at,
                        service_tier: ProjectServiceTier::from_database(&service_tier)
                            .unwrap_or_default(),
                        user_api_keys_disabled,
                        settings_version,
                        status: if archived_at.is_some() {
                            "archived"
                        } else {
                            "active"
                        },
                    }
                },
            )
            .collect();
        Ok(project_list(data, has_more))
    }

    async fn list_projects_for_ids(
        &self,
        project_ids: &[String],
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectList, ImageGatewayError> {
        let limit = limit.clamp(1, 100);
        if let Some(after) = after {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM gateway_projects WHERE id = $1 AND id = ANY($2))",
            )
            .bind(after)
            .bind(project_ids)
            .fetch_one(&self.pool)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
            if !exists {
                return Err(project_cursor_not_found(after));
            }
        }
        let rows = sqlx::query_as::<_, ProjectRow>(
            r#"
            SELECT id, name, created_at, archived_at,
                   service_tier, user_api_keys_disabled, settings_version
            FROM gateway_projects
            WHERE id = ANY($1)
              AND ($2::TEXT IS NULL OR (created_at, id) > (
                  SELECT created_at, id
                  FROM gateway_projects
                  WHERE id = $2 AND id = ANY($1)
              ))
            ORDER BY created_at ASC, id ASC
            LIMIT $3
            "#,
        )
        .bind(project_ids)
        .bind(after)
        .bind((limit + 1) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        let has_more = rows.len() > limit;
        let data = rows
            .into_iter()
            .take(limit)
            .map(
                |(
                    id,
                    name,
                    created_at,
                    archived_at,
                    service_tier,
                    user_api_keys_disabled,
                    settings_version,
                )| {
                    Project {
                        object: "organization.project",
                        id,
                        name,
                        created_at,
                        archived_at,
                        service_tier: ProjectServiceTier::from_database(&service_tier)
                            .unwrap_or_default(),
                        user_api_keys_disabled,
                        settings_version,
                        status: if archived_at.is_some() {
                            "archived"
                        } else {
                            "active"
                        },
                    }
                },
            )
            .collect();
        Ok(project_list(data, has_more))
    }

    async fn get_project(&self, project_id: &str) -> Result<Project, ImageGatewayError> {
        validate_project_id(project_id)?;
        let row = sqlx::query_as::<_, ProjectRow>(
            r#"
            SELECT id, name, created_at, archived_at,
                   service_tier, user_api_keys_disabled, settings_version
            FROM gateway_projects
            WHERE id = $1
            "#,
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?
        .ok_or_else(|| project_not_found(project_id))?;
        Ok(project_from_postgres_row(row))
    }

    async fn update_project_settings(
        &self,
        project_id: &str,
        actor_user_id: Uuid,
        name: &str,
        service_tier: ProjectServiceTier,
        user_api_keys_disabled: bool,
        expected_settings_version: i64,
    ) -> Result<Project, ImageGatewayError> {
        validate_project_id(project_id)?;
        let name = validate_name(name)?;
        if expected_settings_version <= 0 {
            return Err(ImageGatewayError::invalid_request(
                "expected_settings_version must be greater than zero",
                Some("expected_settings_version".to_string()),
                "invalid_settings_version",
            ));
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        let updated = sqlx::query_as::<_, ProjectRow>(
            r#"
                UPDATE gateway_projects
                SET name = $2,
                    service_tier = $3,
                    user_api_keys_disabled = $4,
                    settings_version = settings_version + 1
                WHERE id = $1
                  AND archived_at IS NULL
                  AND settings_version = $5
                RETURNING id, name, created_at, archived_at,
                          service_tier, user_api_keys_disabled, settings_version
                "#,
        )
        .bind(project_id)
        .bind(&name)
        .bind(service_tier.as_str())
        .bind(user_api_keys_disabled)
        .bind(expected_settings_version)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        let Some(updated) = updated else {
            let current = sqlx::query_as::<_, (Option<i64>, i64)>(
                "SELECT archived_at, settings_version FROM gateway_projects WHERE id = $1",
            )
            .bind(project_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
            return match current {
                Some((None, _)) => Err(project_settings_conflict()),
                _ => Err(project_not_found(project_id)),
            };
        };
        sqlx::query(
            r#"
            INSERT INTO identity_audit_events
              (event_id, actor_user_id, action, resource_type, resource_id,
               outcome, metadata, created_at_ms)
            VALUES ($1, $2, 'project.settings.update', 'project', $3,
                    'success', $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(actor_user_id)
        .bind(project_id)
        .bind(serde_json::json!({
            "name": name,
            "service_tier": service_tier.as_str(),
            "user_api_keys_disabled": user_api_keys_disabled,
            "previous_settings_version": expected_settings_version,
            "settings_version": updated.6,
        }))
        .bind(now_seconds().saturating_mul(1_000))
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("audit state unavailable"))?;
        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        Ok(project_from_postgres_row(updated))
    }

    async fn project_tenant(&self, project_id: &str) -> Result<Option<String>, ImageGatewayError> {
        sqlx::query_scalar("SELECT tenant_id FROM gateway_projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))
    }

    async fn project_runtime_defaults(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectRuntimeDefaults>, ImageGatewayError> {
        let row = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT tenant_id, service_tier
            FROM gateway_projects
            WHERE id = $1 AND archived_at IS NULL
            "#,
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("project state unavailable"))?;
        row.map(|(tenant_id, service_tier)| {
            ProjectServiceTier::from_database(&service_tier)
                .map(|service_tier| ProjectRuntimeDefaults {
                    tenant_id,
                    service_tier,
                })
                .ok_or_else(|| {
                    ImageGatewayError::service_unavailable("project service tier state unavailable")
                })
        })
        .transpose()
    }

    async fn create_service_account(
        &self,
        project_id: &str,
        name: &str,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        self.create_service_account_internal(
            project_id,
            name,
            None,
            None,
            permission_mode,
            permissions,
        )
        .await
    }

    async fn create_service_account_for_actor(
        &self,
        project_id: &str,
        name: &str,
        actor_user_id: Uuid,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        self.create_service_account_internal(
            project_id,
            name,
            None,
            Some(actor_user_id),
            permission_mode,
            permissions,
        )
        .await
    }

    async fn create_service_account_with_route(
        &self,
        project_id: &str,
        name: &str,
        route_id: Uuid,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        self.create_service_account_internal(
            project_id,
            name,
            Some(route_id),
            None,
            permission_mode,
            permissions,
        )
        .await
    }

    async fn create_service_account_with_route_for_actor(
        &self,
        project_id: &str,
        name: &str,
        route_id: Uuid,
        actor_user_id: Uuid,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        self.create_service_account_internal(
            project_id,
            name,
            Some(route_id),
            Some(actor_user_id),
            permission_mode,
            permissions,
        )
        .await
    }

    async fn create_user_api_key(
        &self,
        project_id: &str,
        owner_user_id: Uuid,
        _owner_name: &str,
        _owner_email: &str,
        name: &str,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<CreatedProjectApiKey, ImageGatewayError> {
        self.create_user_api_key_internal(
            project_id,
            owner_user_id,
            name,
            permission_mode,
            permissions,
        )
        .await
    }

    async fn authenticate(&self, bearer: &str) -> Result<Option<AuthContext>, ImageGatewayError> {
        let key_id = key_id_from_token(bearer);
        if key_id.is_none() && !self.keyring.legacy_sha256_enabled() {
            return Ok(None);
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        let row = if let Some(key_id) = key_id {
            sqlx::query_as::<_, CredentialRow>(
                r#"
                SELECT k.id, p.tenant_id, k.project_id, p.service_tier AS project_service_tier,
                       k.service_account_id,
                       k.key_hash, k.hash_algorithm, k.pepper_version, k.last_used_at,
                       k.authz_version, s.owner_user_id, k.permission_mode, k.permissions
                FROM gateway_api_keys k
                JOIN gateway_service_accounts s
                  ON s.id = k.service_account_id
                 AND s.project_id = k.project_id
                 AND s.tenant_id = k.tenant_id
                JOIN gateway_projects p
                  ON p.id = k.project_id AND p.tenant_id = k.tenant_id
                WHERE k.id = $1 AND k.deleted_at IS NULL
                  AND s.deleted_at IS NULL AND p.archived_at IS NULL
                  AND (k.expires_at IS NULL OR k.expires_at > $2)
                  AND (
                    s.owner_type = 'service_account'
                    OR (
                      NOT p.user_api_keys_disabled
                      AND EXISTS (
                        SELECT 1
                        FROM identity_project_memberships membership
                        JOIN identity_users identity
                          ON identity.user_id = membership.user_id
                         AND identity.disabled_at_ms IS NULL
                        WHERE membership.organization_id = p.tenant_id
                          AND membership.project_id = p.id
                          AND membership.user_id = s.owner_user_id
                          AND membership.state = 'active'
                      )
                    )
                  )
                FOR SHARE OF k, s, p
                "#,
            )
            .bind(key_id)
            .bind(now_seconds())
            .fetch_optional(&mut *tx)
            .await
        } else {
            sqlx::query_as::<_, CredentialRow>(
                r#"
                SELECT k.id, p.tenant_id, k.project_id, p.service_tier AS project_service_tier,
                       k.service_account_id,
                       k.key_hash, k.hash_algorithm, k.pepper_version, k.last_used_at,
                       k.authz_version, s.owner_user_id, k.permission_mode, k.permissions
                FROM gateway_api_keys k
                JOIN gateway_service_accounts s
                  ON s.id = k.service_account_id
                 AND s.project_id = k.project_id
                 AND s.tenant_id = k.tenant_id
                JOIN gateway_projects p
                  ON p.id = k.project_id AND p.tenant_id = k.tenant_id
                WHERE k.key_hash = $1 AND k.hash_algorithm = $2
                  AND k.deleted_at IS NULL AND s.deleted_at IS NULL
                  AND p.archived_at IS NULL
                  AND (k.expires_at IS NULL OR k.expires_at > $3)
                  AND (
                    s.owner_type = 'service_account'
                    OR (
                      NOT p.user_api_keys_disabled
                      AND EXISTS (
                        SELECT 1
                        FROM identity_project_memberships membership
                        JOIN identity_users identity
                          ON identity.user_id = membership.user_id
                         AND identity.disabled_at_ms IS NULL
                        WHERE membership.organization_id = p.tenant_id
                          AND membership.project_id = p.id
                          AND membership.user_id = s.owner_user_id
                          AND membership.state = 'active'
                      )
                    )
                  )
                FOR SHARE OF k, s, p
                "#,
            )
            .bind(hash_key(bearer))
            .bind(LEGACY_ALGORITHM)
            .bind(now_seconds())
            .fetch_optional(&mut *tx)
            .await
        }
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;

        let Some(row) = row else {
            tx.rollback()
                .await
                .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
            return Ok(None);
        };
        let verified = match row.hash_algorithm.as_str() {
            HMAC_ALGORITHM => row
                .pepper_version
                .and_then(|version| u16::try_from(version).ok())
                .is_some_and(|version| self.keyring.verify(version, bearer, &row.key_hash)),
            LEGACY_ALGORITHM => {
                self.keyring.legacy_sha256_enabled() && row.key_hash == hash_key(bearer)
            }
            _ => false,
        };
        if !verified {
            tx.rollback()
                .await
                .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
            return Ok(None);
        }
        let permission_mode = ApiKeyPermissionMode::from_database(&row.permission_mode)
            .ok_or_else(|| {
                ImageGatewayError::service_unavailable("api key permission state unavailable")
            })?;
        let permissions: ApiKeyPermissions = serde_json::from_value(row.permissions.clone())
            .map_err(|_| {
                ImageGatewayError::service_unavailable("api key permission state unavailable")
            })?;
        if !permissions.validate() {
            return Err(ImageGatewayError::service_unavailable(
                "api key permission state unavailable",
            ));
        }
        let project_service_tier = ProjectServiceTier::from_database(&row.project_service_tier)
            .ok_or_else(|| {
                ImageGatewayError::service_unavailable("project service tier state unavailable")
            })?;

        let now = now_seconds();
        let should_update_last_used = row
            .last_used_at
            .is_none_or(|last_used| last_used <= now - LAST_USED_COALESCE_SECONDS);
        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        if should_update_last_used {
            if let Err(error) = sqlx::query(
                r#"
                UPDATE gateway_api_keys
                SET last_used_at = $2
                WHERE id = $1 AND deleted_at IS NULL
                  AND (last_used_at IS NULL OR last_used_at <= $3)
                "#,
            )
            .bind(&row.id)
            .bind(now)
            .bind(now - LAST_USED_COALESCE_SECONDS)
            .execute(&self.pool)
            .await
            {
                tracing::warn!(
                    api_key_id = %row.id,
                    %error,
                    "failed to update coalesced API key activity timestamp"
                );
            }
        }
        Ok(Some(AuthContext {
            tenant_id: row.tenant_id,
            project_id: row.project_id,
            project_service_tier,
            service_account_id: Some(row.service_account_id),
            api_key_id: Some(row.id),
            credential_authz_version: Some(row.authz_version),
            credential_owner_user_id: row.owner_user_id,
            actor_user_id: None,
            actor_session_id: None,
            actor_authz_version: None,
            api_key_permission_mode: permission_mode,
            api_key_permissions: permissions,
            route: None,
            is_admin: false,
        }))
    }

    async fn list_project_api_keys(
        &self,
        project_id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectApiKeyList, ImageGatewayError> {
        validate_project_id(project_id)?;
        let limit = limit.clamp(1, 100);
        if let Some(after) = after {
            let exists: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS(
                    SELECT 1 FROM gateway_api_keys
                    WHERE project_id = $1 AND id = $2 AND deleted_at IS NULL
                )
                "#,
            )
            .bind(project_id)
            .bind(after)
            .fetch_one(&self.pool)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
            if !exists {
                return Err(api_key_cursor_not_found(after));
            }
        }
        let rows = sqlx::query_as::<_, ProjectApiKeyRow>(
            r#"
            SELECT
              k.id,
              k.redacted_value,
              k.name,
              k.created_at,
              k.last_used_at,
              k.expires_at,
              s.id AS service_account_id,
              s.name AS service_account_name,
              s.created_at AS service_account_created_at,
              s.owner_type,
              s.owner_user_id,
              identity.display_name AS owner_user_name,
              identity.normalized_email AS owner_user_email,
              k.permission_mode,
              k.permissions,
              COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                  'route_id', binding.route_id::TEXT,
                  'route_revision', binding.route_revision,
                  'display_name', CASE
                    WHEN route.route_kind = 'group' THEN route.display_name
                    ELSE binding.provider_id
                  END,
                  'route_kind', route.route_kind,
                  'provider_id', binding.provider_id,
                  'operation_id', binding.operation_id,
                  'model_count', (
                    SELECT COUNT(DISTINCT (mapping.api_profile, mapping.public_model_id))
                    FROM provider_route_model_mappings mapping
                    WHERE mapping.route_id = binding.route_id
                      AND mapping.route_revision = binding.route_revision
                  )
                ) ORDER BY binding.provider_id, binding.operation_id)
                FROM gateway_api_key_provider_routes binding
                JOIN provider_route_heads head
                  ON head.route_id = binding.route_id
                JOIN provider_routes route
                  ON route.route_id = binding.route_id
                 AND route.revision = binding.route_revision
                WHERE binding.api_key_id = k.id
              ), '[]'::JSONB) AS provider_routes,
              CASE
                WHEN s.owner_type = 'service_account' THEN
                  s.deleted_at IS NULL
                  AND s.project_id = k.project_id
                  AND s.tenant_id = k.tenant_id
                WHEN s.owner_type = 'user' THEN EXISTS(
                  SELECT 1
                  FROM identity_project_memberships membership
                  JOIN identity_users owner_identity
                    ON owner_identity.user_id = membership.user_id
                   AND owner_identity.disabled_at_ms IS NULL
                  WHERE membership.organization_id = k.tenant_id
                    AND membership.project_id = k.project_id
                    AND membership.user_id = s.owner_user_id
                    AND membership.state = 'active'
                )
                ELSE FALSE
              END AS owner_project_access,
              p.user_api_keys_disabled
            FROM gateway_api_keys k
            JOIN gateway_service_accounts s
              ON s.id = k.service_account_id
             AND s.project_id = k.project_id
             AND s.tenant_id = k.tenant_id
            JOIN gateway_projects p
              ON p.id = k.project_id AND p.tenant_id = k.tenant_id
            LEFT JOIN identity_users identity
              ON identity.user_id = s.owner_user_id
            WHERE k.project_id = $1
              AND k.deleted_at IS NULL AND s.deleted_at IS NULL
              AND p.archived_at IS NULL
              AND ($2::TEXT IS NULL OR (k.created_at, k.id) > (
                SELECT created_at, id FROM gateway_api_keys
                WHERE project_id = $1 AND id = $2 AND deleted_at IS NULL
              ))
            ORDER BY k.created_at ASC, k.id ASC
            LIMIT $3
            "#,
        )
        .bind(project_id)
        .bind(after)
        .bind((limit + 1) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;

        let has_more = rows.len() > limit;
        let data = rows
            .into_iter()
            .take(limit)
            .map(project_api_key_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(project_api_key_list(data, has_more))
    }

    async fn list_project_api_keys_for_user(
        &self,
        project_id: &str,
        owner_user_id: Uuid,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectApiKeyList, ImageGatewayError> {
        validate_project_id(project_id)?;
        let limit = limit.clamp(1, 100);
        if let Some(after) = after {
            let exists: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS(
                  SELECT 1
                  FROM gateway_api_keys key
                  JOIN gateway_service_accounts owner
                    ON owner.id = key.service_account_id
                   AND owner.project_id = key.project_id
                   AND owner.tenant_id = key.tenant_id
                  WHERE key.project_id = $1
                    AND key.id = $2
                    AND key.deleted_at IS NULL
                    AND owner.deleted_at IS NULL
                    AND owner.owner_type = 'user'
                    AND owner.owner_user_id = $3
                )
                "#,
            )
            .bind(project_id)
            .bind(after)
            .bind(owner_user_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
            if !exists {
                return Err(api_key_cursor_not_found(after));
            }
        }
        let rows = sqlx::query_as::<_, ProjectApiKeyRow>(
            r#"
            SELECT
              k.id,
              k.redacted_value,
              k.name,
              k.created_at,
              k.last_used_at,
              k.expires_at,
              s.id AS service_account_id,
              s.name AS service_account_name,
              s.created_at AS service_account_created_at,
              s.owner_type,
              s.owner_user_id,
              identity.display_name AS owner_user_name,
              identity.normalized_email AS owner_user_email,
              k.permission_mode,
              k.permissions,
              COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                  'route_id', binding.route_id::TEXT,
                  'route_revision', binding.route_revision,
                  'display_name', CASE
                    WHEN route.route_kind = 'group' THEN route.display_name
                    ELSE binding.provider_id
                  END,
                  'route_kind', route.route_kind,
                  'provider_id', binding.provider_id,
                  'operation_id', binding.operation_id,
                  'model_count', (
                    SELECT COUNT(DISTINCT (mapping.api_profile, mapping.public_model_id))
                    FROM provider_route_model_mappings mapping
                    WHERE mapping.route_id = binding.route_id
                      AND mapping.route_revision = binding.route_revision
                  )
                ) ORDER BY binding.provider_id, binding.operation_id)
                FROM gateway_api_key_provider_routes binding
                JOIN provider_route_heads head
                  ON head.route_id = binding.route_id
                JOIN provider_routes route
                  ON route.route_id = binding.route_id
                 AND route.revision = binding.route_revision
                WHERE binding.api_key_id = k.id
              ), '[]'::JSONB) AS provider_routes,
              CASE
                WHEN s.owner_type = 'service_account' THEN
                  s.deleted_at IS NULL
                  AND s.project_id = k.project_id
                  AND s.tenant_id = k.tenant_id
                WHEN s.owner_type = 'user' THEN EXISTS(
                  SELECT 1
                  FROM identity_project_memberships membership
                  JOIN identity_users owner_identity
                    ON owner_identity.user_id = membership.user_id
                   AND owner_identity.disabled_at_ms IS NULL
                  WHERE membership.organization_id = k.tenant_id
                    AND membership.project_id = k.project_id
                    AND membership.user_id = s.owner_user_id
                    AND membership.state = 'active'
                )
                ELSE FALSE
              END AS owner_project_access,
              p.user_api_keys_disabled
            FROM gateway_api_keys k
            JOIN gateway_service_accounts s
              ON s.id = k.service_account_id
             AND s.project_id = k.project_id
             AND s.tenant_id = k.tenant_id
            JOIN gateway_projects p
              ON p.id = k.project_id AND p.tenant_id = k.tenant_id
            JOIN identity_users identity
              ON identity.user_id = s.owner_user_id
            WHERE k.project_id = $1
              AND s.owner_type = 'user'
              AND s.owner_user_id = $2
              AND k.deleted_at IS NULL AND s.deleted_at IS NULL
              AND p.archived_at IS NULL
              AND ($3::TEXT IS NULL OR (k.created_at, k.id) > (
                SELECT cursor_key.created_at, cursor_key.id
                FROM gateway_api_keys cursor_key
                JOIN gateway_service_accounts cursor_owner
                  ON cursor_owner.id = cursor_key.service_account_id
                 AND cursor_owner.project_id = cursor_key.project_id
                 AND cursor_owner.tenant_id = cursor_key.tenant_id
                WHERE cursor_key.project_id = $1
                  AND cursor_key.id = $3
                  AND cursor_key.deleted_at IS NULL
                  AND cursor_owner.owner_type = 'user'
                  AND cursor_owner.owner_user_id = $2
              ))
            ORDER BY k.created_at ASC, k.id ASC
            LIMIT $4
            "#,
        )
        .bind(project_id)
        .bind(owner_user_id)
        .bind(after)
        .bind((limit + 1) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        let has_more = rows.len() > limit;
        let data = rows
            .into_iter()
            .take(limit)
            .map(project_api_key_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(project_api_key_list(data, has_more))
    }

    async fn delete_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
    ) -> Result<ProjectApiKeyDeleted, ImageGatewayError> {
        validate_project_id(project_id)?;
        let deleted_at = now_seconds();
        let deleted = sqlx::query(
            r#"
            UPDATE gateway_api_keys
            SET deleted_at = $3,
                authz_version = authz_version + 1
            WHERE project_id = $1 AND id = $2 AND deleted_at IS NULL
            "#,
        )
        .bind(project_id)
        .bind(api_key_id)
        .bind(deleted_at)
        .execute(&self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        if deleted.rows_affected() == 0 {
            return Err(ImageGatewayError::not_found(
                "API key not found",
                Some("api_key_id".to_string()),
                "not_found",
            ));
        }
        Ok(ProjectApiKeyDeleted {
            object: "organization.project.api_key.deleted",
            id: api_key_id.to_string(),
            deleted: true,
        })
    }

    async fn delete_user_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
        owner_user_id: Uuid,
    ) -> Result<ProjectApiKeyDeleted, ImageGatewayError> {
        validate_project_id(project_id)?;
        let deleted_at = now_seconds();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        let deleted = sqlx::query(
            r#"
            UPDATE gateway_api_keys key
            SET deleted_at = $4,
                authz_version = key.authz_version + 1,
                revoked_by_user_id = $3,
                revocation_reason = 'user_revoked'
            FROM gateway_service_accounts owner
            WHERE key.project_id = $1
              AND key.id = $2
              AND key.deleted_at IS NULL
              AND owner.id = key.service_account_id
              AND owner.project_id = key.project_id
              AND owner.tenant_id = key.tenant_id
              AND owner.deleted_at IS NULL
              AND owner.owner_type = 'user'
              AND owner.owner_user_id = $3
            "#,
        )
        .bind(project_id)
        .bind(api_key_id)
        .bind(owner_user_id)
        .bind(deleted_at)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        if deleted.rows_affected() == 0 {
            return Err(api_key_not_found(api_key_id));
        }
        sqlx::query(
            r#"
            INSERT INTO identity_audit_events
              (event_id, actor_user_id, action, resource_type, resource_id,
               outcome, metadata, created_at_ms)
            VALUES ($1, $2, 'project.api_key.revoke', 'api_key', $3,
                    'success', $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(owner_user_id)
        .bind(api_key_id)
        .bind(serde_json::json!({
            "project_id": project_id,
            "reason": "user_revoked",
        }))
        .bind(deleted_at.saturating_mul(1_000))
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("audit state unavailable"))?;
        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        Ok(ProjectApiKeyDeleted {
            object: "organization.project.api_key.deleted",
            id: api_key_id.to_string(),
            deleted: true,
        })
    }

    async fn update_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
        actor_user_id: Uuid,
        can_manage_shared_credentials: bool,
        name: &str,
        permission_mode: ApiKeyPermissionMode,
        permissions: ApiKeyPermissions,
    ) -> Result<UpdatedProjectApiKey, ImageGatewayError> {
        validate_project_id(project_id)?;
        let name = validate_name(name)?;
        validate_permissions(permission_mode, &permissions)?;
        let permissions_json = serde_json::to_value(&permissions)
            .map_err(|_| ImageGatewayError::internal("failed to encode API key permissions"))?;
        let updated_at = now_seconds();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        let updated = sqlx::query(
            r#"
            UPDATE gateway_api_keys key
            SET name = $5,
                permission_mode = $6,
                permissions = $7,
                authz_version = key.authz_version + 1
            FROM gateway_service_accounts owner
            WHERE key.project_id = $1
              AND key.id = $2
              AND key.deleted_at IS NULL
              AND owner.id = key.service_account_id
              AND owner.project_id = key.project_id
              AND owner.tenant_id = key.tenant_id
              AND owner.deleted_at IS NULL
              AND (
                (owner.owner_type = 'user' AND owner.owner_user_id = $3)
                OR (owner.owner_type = 'service_account' AND $4)
              )
            "#,
        )
        .bind(project_id)
        .bind(api_key_id)
        .bind(actor_user_id)
        .bind(can_manage_shared_credentials)
        .bind(&name)
        .bind(permission_mode.as_str())
        .bind(permissions_json)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        if updated.rows_affected() == 0 {
            return Err(api_key_not_found(api_key_id));
        }
        sqlx::query(
            r#"
            INSERT INTO identity_audit_events
              (event_id, actor_user_id, action, resource_type, resource_id,
               outcome, metadata, created_at_ms)
            VALUES ($1, $2, 'project.api_key.update', 'api_key', $3,
                    'success', $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(actor_user_id)
        .bind(api_key_id)
        .bind(serde_json::json!({
            "project_id": project_id,
            "permission_mode": permission_mode.as_str(),
        }))
        .bind(updated_at.saturating_mul(1_000))
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("audit state unavailable"))?;
        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        Ok(UpdatedProjectApiKey {
            object: "organization.project.api_key",
            id: api_key_id.to_string(),
            name,
            permission_mode,
            permissions,
        })
    }

    async fn rotate_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
        actor_user_id: Uuid,
        can_manage_shared_credentials: bool,
    ) -> Result<RotatedProjectApiKey, ImageGatewayError> {
        validate_project_id(project_id)?;
        let created_at = now_seconds();
        let created_at_ms = created_at.saturating_mul(1_000);
        let key_id = new_id("key");
        let value = new_key_value(&key_id);
        let redacted_value = redact_key(&value);
        let hash = self.keyring.digest_current(&value);
        let pepper_version = i32::from(self.keyring.current_version());
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        let old = sqlx::query_as::<
            _,
            (
                String,
                String,
                String,
                String,
                serde_json::Value,
                Option<i64>,
            ),
        >(
            r#"
            SELECT key.service_account_id, key.tenant_id, key.name,
                   key.permission_mode, key.permissions, key.expires_at
            FROM gateway_api_keys key
            JOIN gateway_service_accounts owner
              ON owner.id = key.service_account_id
             AND owner.project_id = key.project_id
             AND owner.tenant_id = key.tenant_id
            WHERE key.project_id = $1
              AND key.id = $2
              AND key.deleted_at IS NULL
              AND owner.deleted_at IS NULL
              AND (
                (owner.owner_type = 'user' AND owner.owner_user_id = $3)
                OR (owner.owner_type = 'service_account' AND $4)
              )
            FOR UPDATE OF key, owner
            "#,
        )
        .bind(project_id)
        .bind(api_key_id)
        .bind(actor_user_id)
        .bind(can_manage_shared_credentials)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?
        .ok_or_else(|| api_key_not_found(api_key_id))?;
        let permission_mode = ApiKeyPermissionMode::from_database(&old.3).ok_or_else(|| {
            ImageGatewayError::service_unavailable("api key permission state unavailable")
        })?;
        let permissions: ApiKeyPermissions =
            serde_json::from_value(old.4.clone()).map_err(|_| {
                ImageGatewayError::service_unavailable("api key permission state unavailable")
            })?;
        validate_permissions(permission_mode, &permissions)?;

        sqlx::query(
            r#"
            INSERT INTO gateway_api_keys
              (id, project_id, tenant_id, service_account_id, name, key_hash,
               hash_algorithm, pepper_version, redacted_value, created_at,
               expires_at, permission_mode, permissions, created_by_user_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            "#,
        )
        .bind(&key_id)
        .bind(project_id)
        .bind(&old.1)
        .bind(&old.0)
        .bind(&old.2)
        .bind(hash)
        .bind(HMAC_ALGORITHM)
        .bind(pepper_version)
        .bind(&redacted_value)
        .bind(created_at)
        .bind(old.5)
        .bind(permission_mode.as_str())
        .bind(old.4)
        .bind(actor_user_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        sqlx::query(
            r#"
            INSERT INTO gateway_api_key_provider_routes
              (api_key_id, service_account_id, project_id, tenant_id,
               provider_id, operation_id, command_schema, route_id,
               route_revision, bound_at_ms)
            SELECT $1, service_account_id, project_id, tenant_id,
                   provider_id, operation_id, command_schema, route_id,
                   route_revision, $3
            FROM gateway_api_key_provider_routes
            WHERE api_key_id = $2
            "#,
        )
        .bind(&key_id)
        .bind(api_key_id)
        .bind(created_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("provider route unavailable"))?;
        sqlx::query(
            r#"
            UPDATE gateway_api_keys
            SET deleted_at = $4,
                authz_version = authz_version + 1,
                revoked_by_user_id = $3,
                revocation_reason = 'rotated'
            WHERE project_id = $1 AND id = $2 AND deleted_at IS NULL
            "#,
        )
        .bind(project_id)
        .bind(api_key_id)
        .bind(actor_user_id)
        .bind(created_at)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        sqlx::query(
            r#"
            INSERT INTO identity_audit_events
              (event_id, actor_user_id, action, resource_type, resource_id,
               outcome, metadata, created_at_ms)
            VALUES ($1, $2, 'project.api_key.rotate', 'api_key', $3,
                    'success', $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(actor_user_id)
        .bind(&key_id)
        .bind(serde_json::json!({
            "project_id": project_id,
            "replaced_api_key_id": api_key_id,
        }))
        .bind(created_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("audit state unavailable"))?;
        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        Ok(RotatedProjectApiKey {
            object: "organization.project.api_key.rotation",
            replaced_api_key_id: api_key_id.to_string(),
            api_key: CreatedProjectApiKey {
                object: "organization.project.api_key",
                value,
                name: old.2,
                created_at,
                id: key_id,
            },
        })
    }

    async fn delete_project_api_key_for_actor(
        &self,
        project_id: &str,
        api_key_id: &str,
        actor_user_id: Uuid,
    ) -> Result<ProjectApiKeyDeleted, ImageGatewayError> {
        validate_project_id(project_id)?;
        let deleted_at = now_seconds();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        let deleted = sqlx::query(
            r#"
            UPDATE gateway_api_keys
            SET deleted_at = $4,
                authz_version = authz_version + 1,
                revoked_by_user_id = $3,
                revocation_reason = 'administrator_revoked'
            WHERE project_id = $1 AND id = $2 AND deleted_at IS NULL
            "#,
        )
        .bind(project_id)
        .bind(api_key_id)
        .bind(actor_user_id)
        .bind(deleted_at)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        if deleted.rows_affected() == 0 {
            return Err(api_key_not_found(api_key_id));
        }
        sqlx::query(
            r#"
            INSERT INTO identity_audit_events
              (event_id, actor_user_id, action, resource_type, resource_id,
               outcome, metadata, created_at_ms)
            VALUES ($1, $2, 'project.api_key.revoke', 'api_key', $3,
                    'success', $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(actor_user_id)
        .bind(api_key_id)
        .bind(serde_json::json!({
            "project_id": project_id,
            "reason": "administrator_revoked",
        }))
        .bind(deleted_at.saturating_mul(1_000))
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("audit state unavailable"))?;
        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        Ok(ProjectApiKeyDeleted {
            object: "organization.project.api_key.deleted",
            id: api_key_id.to_string(),
            deleted: true,
        })
    }

    async fn delete_service_account(
        &self,
        project_id: &str,
        service_account_id: &str,
    ) -> Result<ProjectServiceAccountDeleted, ImageGatewayError> {
        self.delete_service_account_internal(project_id, service_account_id, None)
            .await
    }

    async fn delete_service_account_for_actor(
        &self,
        project_id: &str,
        service_account_id: &str,
        actor_user_id: Uuid,
    ) -> Result<ProjectServiceAccountDeleted, ImageGatewayError> {
        self.delete_service_account_internal(project_id, service_account_id, Some(actor_user_id))
            .await
    }
}

fn project_api_key_from_row(row: ProjectApiKeyRow) -> Result<ProjectApiKey, ImageGatewayError> {
    let permission_mode =
        ApiKeyPermissionMode::from_database(&row.permission_mode).ok_or_else(|| {
            ImageGatewayError::service_unavailable("api key permission state unavailable")
        })?;
    let permissions: ApiKeyPermissions = serde_json::from_value(row.permissions).map_err(|_| {
        ImageGatewayError::service_unavailable("api key permission state unavailable")
    })?;
    if !permissions.validate() {
        return Err(ImageGatewayError::service_unavailable(
            "api key permission state unavailable",
        ));
    }
    let user_key_disabled = row.owner_type == "user" && row.user_api_keys_disabled;
    let owner = match row.owner_type.as_str() {
        "service_account" => ProjectApiKeyOwner {
            owner_type: "service_account",
            service_account: Some(ProjectApiKeyServiceAccountOwner {
                id: row.service_account_id,
                name: row.service_account_name,
                role: "member",
                created_at: row.service_account_created_at,
            }),
            user: None,
        },
        "user" => ProjectApiKeyOwner {
            owner_type: "user",
            service_account: None,
            user: Some(ProjectApiKeyUserOwner {
                id: row
                    .owner_user_id
                    .ok_or_else(|| {
                        ImageGatewayError::service_unavailable("api key owner state unavailable")
                    })?
                    .to_string(),
                name: row.owner_user_name.ok_or_else(|| {
                    ImageGatewayError::service_unavailable("api key owner state unavailable")
                })?,
                email: row.owner_user_email.ok_or_else(|| {
                    ImageGatewayError::service_unavailable("api key owner state unavailable")
                })?,
                role: "member",
            }),
        },
        _ => {
            return Err(ImageGatewayError::service_unavailable(
                "api key owner state unavailable",
            ));
        }
    };
    let unexpired = row
        .expires_at
        .is_none_or(|expires_at| expires_at > now_seconds());
    let status = if !row.owner_project_access {
        "owner_access_lost"
    } else if user_key_disabled {
        "project_user_keys_disabled"
    } else if !unexpired {
        "expired"
    } else {
        "active"
    };
    Ok(ProjectApiKey {
        object: "organization.project.api_key",
        redacted_value: row.redacted_value,
        name: row.name,
        created_at: row.created_at,
        last_used_at: row.last_used_at,
        expires_at: row.expires_at,
        id: row.id,
        owner,
        provider_routes: serde_json::from_value(row.provider_routes).unwrap_or_default(),
        permission_mode,
        permissions,
        owner_project_access: if row.owner_project_access {
            "active"
        } else {
            "inactive"
        },
        status,
    })
}

fn project_api_key_from_memory(
    state: &InMemoryApiKeyState,
    api_key: StoredApiKey,
) -> Option<ProjectApiKey> {
    let service_account = state
        .service_accounts
        .iter()
        .find(|account| account.id == api_key.service_account_id)?;
    let unexpired = api_key
        .expires_at
        .is_none_or(|expires_at| expires_at > now_seconds());
    let owner_project_access = !service_account.deleted
        && service_account.project_id == api_key.project_id
        && state
            .projects
            .iter()
            .any(|project| project.id == api_key.project_id && project.archived_at.is_none());
    let user_key_disabled = service_account.owner_user_id.is_some()
        && state
            .projects
            .iter()
            .any(|project| project.id == api_key.project_id && project.user_api_keys_disabled);
    let status = if !owner_project_access {
        "owner_access_lost"
    } else if user_key_disabled {
        "project_user_keys_disabled"
    } else if !unexpired {
        "expired"
    } else {
        "active"
    };
    Some(ProjectApiKey {
        object: "organization.project.api_key",
        redacted_value: api_key.redacted_value,
        name: api_key.name,
        created_at: api_key.created_at,
        last_used_at: api_key.last_used_at,
        expires_at: api_key.expires_at,
        id: api_key.id,
        owner: if let Some(owner_user_id) = service_account.owner_user_id {
            ProjectApiKeyOwner {
                owner_type: "user",
                service_account: None,
                user: Some(ProjectApiKeyUserOwner {
                    id: owner_user_id.to_string(),
                    name: service_account.name.clone(),
                    email: service_account.owner_email.clone().unwrap_or_default(),
                    role: "member",
                }),
            }
        } else {
            ProjectApiKeyOwner {
                owner_type: "service_account",
                service_account: Some(ProjectApiKeyServiceAccountOwner {
                    id: service_account.id.clone(),
                    name: service_account.name.clone(),
                    role: "member",
                    created_at: service_account.created_at,
                }),
                user: None,
            }
        },
        provider_routes: Vec::new(),
        permission_mode: api_key.permission_mode,
        permissions: api_key.permissions,
        owner_project_access: if owner_project_access {
            "active"
        } else {
            "inactive"
        },
        status,
    })
}

fn project_api_key_list(data: Vec<ProjectApiKey>, has_more: bool) -> ProjectApiKeyList {
    ProjectApiKeyList {
        object: "list",
        first_id: data.first().map(|key| key.id.clone()),
        last_id: data.last().map(|key| key.id.clone()),
        data,
        has_more,
    }
}

fn project_from_memory(project: &StoredProject) -> Project {
    Project {
        object: "organization.project",
        id: project.id.clone(),
        name: project.name.clone(),
        created_at: project.created_at,
        archived_at: project.archived_at,
        service_tier: project.service_tier,
        user_api_keys_disabled: project.user_api_keys_disabled,
        settings_version: project.settings_version,
        status: if project.archived_at.is_some() {
            "archived"
        } else {
            "active"
        },
    }
}

fn project_from_postgres_row(row: ProjectRow) -> Project {
    let (id, name, created_at, archived_at, service_tier, user_api_keys_disabled, settings_version) =
        row;
    Project {
        object: "organization.project",
        id,
        name,
        created_at,
        archived_at,
        service_tier: ProjectServiceTier::from_database(&service_tier).unwrap_or_default(),
        user_api_keys_disabled,
        settings_version,
        status: if archived_at.is_some() {
            "archived"
        } else {
            "active"
        },
    }
}

fn project_list(data: Vec<Project>, has_more: bool) -> ProjectList {
    ProjectList {
        object: "list",
        first_id: data.first().map(|project| project.id.clone()),
        last_id: data.last().map(|project| project.id.clone()),
        data,
        has_more,
    }
}

fn project_settings_conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Project settings changed since they were loaded",
        Some("expected_settings_version".to_string()),
        "project_settings_conflict",
    )
}

fn project_not_found(project_id: &str) -> ImageGatewayError {
    ImageGatewayError::not_found(
        format!("Project '{project_id}' was not found"),
        Some("project_id".to_string()),
        "not_found",
    )
}

fn organization_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Organization was not found",
        Some("organization_id".to_string()),
        "organization_not_found",
    )
}

fn project_cursor_not_found(project_id: &str) -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        format!("Project cursor '{project_id}' was not found"),
        Some("after".to_string()),
        "invalid_cursor",
    )
}

fn api_key_cursor_not_found(api_key_id: &str) -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        format!("API key cursor '{api_key_id}' was not found in this project"),
        Some("after".to_string()),
        "invalid_cursor",
    )
}

fn api_key_not_found(api_key_id: &str) -> ImageGatewayError {
    ImageGatewayError::not_found(
        format!("API key '{api_key_id}' was not found"),
        Some("api_key_id".to_string()),
        "not_found",
    )
}

fn service_account_not_found(service_account_id: &str) -> ImageGatewayError {
    ImageGatewayError::not_found(
        format!("Service account '{service_account_id}' was not found"),
        Some("service_account_id".to_string()),
        "not_found",
    )
}

fn validate_project_id(project_id: &str) -> Result<(), ImageGatewayError> {
    if project_id.is_empty()
        || project_id.len() > 128
        || !project_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(ImageGatewayError::invalid_request(
            "project_id must contain only ASCII letters, numbers, underscores, or dashes",
            Some("project_id".to_string()),
            "invalid_value",
        ));
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<String, ImageGatewayError> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > MAX_NAME_CHARS {
        return Err(ImageGatewayError::invalid_request(
            "name must be between 1 and 128 characters",
            Some("name".to_string()),
            "invalid_value",
        ));
    }
    Ok(name.to_string())
}

fn validate_permissions(
    permission_mode: ApiKeyPermissionMode,
    permissions: &ApiKeyPermissions,
) -> Result<(), ImageGatewayError> {
    if !permissions.validate()
        || (!matches!(permission_mode, ApiKeyPermissionMode::Restricted)
            && !permissions.0.is_empty())
    {
        return Err(ImageGatewayError::invalid_request(
            "permissions must contain only models, images, or videos and are used only with restricted mode",
            Some("permissions".to_string()),
            "invalid_value",
        ));
    }
    Ok(())
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

fn hash_key(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}

fn redact_key(key: &str) -> String {
    let suffix = key
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("sk-gw-...{suffix}")
}

fn now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
