use std::{
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{ImageGatewayError, auth::AuthContext};

const MAX_NAME_CHARS: usize = 128;

#[async_trait]
pub trait ApiKeyStore: Send + Sync + 'static {
    async fn create_service_account(
        &self,
        project_id: &str,
        name: &str,
    ) -> Result<ProjectServiceAccount, ImageGatewayError>;

    async fn authenticate(&self, bearer: &str) -> Result<Option<AuthContext>, ImageGatewayError>;

    async fn list_project_api_keys(
        &self,
        project_id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectApiKeyList, ImageGatewayError>;

    async fn delete_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
    ) -> Result<ProjectApiKeyDeleted, ImageGatewayError>;
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
    pub id: String,
    pub owner: ProjectApiKeyOwner,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectApiKeyOwner {
    #[serde(rename = "type")]
    #[schema(value_type = String)]
    pub owner_type: &'static str,
    pub service_account: ProjectApiKeyServiceAccountOwner,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectApiKeyServiceAccountOwner {
    pub id: String,
    pub name: String,
    #[schema(value_type = String)]
    pub role: &'static str,
    pub created_at: i64,
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

#[derive(Default)]
pub struct InMemoryApiKeyStore {
    state: Mutex<InMemoryApiKeyState>,
}

#[derive(Default)]
struct InMemoryApiKeyState {
    service_accounts: Vec<StoredServiceAccount>,
    api_keys: Vec<StoredApiKey>,
}

#[derive(Clone)]
struct StoredServiceAccount {
    id: String,
    name: String,
    created_at: i64,
}

#[derive(Clone)]
struct StoredApiKey {
    id: String,
    project_id: String,
    service_account_id: String,
    name: String,
    hash: String,
    redacted_value: String,
    created_at: i64,
    last_used_at: Option<i64>,
    deleted: bool,
}

#[async_trait]
impl ApiKeyStore for InMemoryApiKeyStore {
    async fn create_service_account(
        &self,
        project_id: &str,
        name: &str,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        validate_project_id(project_id)?;
        let name = validate_name(name)?;
        let created_at = now_seconds();
        let service_account_id = new_id("svc_acct");
        let key_id = new_id("key");
        let value = new_key_value();
        let hash = hash_key(&value);
        let redacted_value = redact_key(&value);

        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        state.service_accounts.push(StoredServiceAccount {
            id: service_account_id.clone(),
            name: name.clone(),
            created_at,
        });
        state.api_keys.push(StoredApiKey {
            id: key_id.clone(),
            project_id: project_id.to_string(),
            service_account_id: service_account_id.clone(),
            name: "Secret Key".to_string(),
            hash,
            redacted_value,
            created_at,
            last_used_at: None,
            deleted: false,
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

    async fn authenticate(&self, bearer: &str) -> Result<Option<AuthContext>, ImageGatewayError> {
        let hash = hash_key(bearer);
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("api key store lock poisoned"))?;
        let Some(api_key) = state
            .api_keys
            .iter_mut()
            .find(|api_key| !api_key.deleted && api_key.hash == hash)
        else {
            return Ok(None);
        };
        api_key.last_used_at = Some(now_seconds());
        Ok(Some(AuthContext {
            tenant_id: api_key.project_id.clone(),
            project_id: api_key.project_id.clone(),
            api_key_id: Some(api_key.id.clone()),
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
            .filter(|api_key| !api_key.deleted && api_key.project_id == project_id)
            .cloned()
            .collect::<Vec<_>>();
        let start = after
            .and_then(|after| project_keys.iter().position(|api_key| api_key.id == after))
            .map(|idx| idx + 1)
            .unwrap_or(0);
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
        let Some(api_key) = state
            .api_keys
            .iter_mut()
            .find(|api_key| api_key.project_id == project_id && api_key.id == api_key_id)
        else {
            return Err(ImageGatewayError::not_found(
                "API key not found",
                Some("api_key_id".to_string()),
                "not_found",
            ));
        };
        api_key.deleted = true;
        Ok(ProjectApiKeyDeleted {
            object: "organization.project.api_key.deleted",
            id: api_key_id.to_string(),
            deleted: true,
        })
    }
}

#[derive(Clone)]
pub struct PostgresApiKeyStore {
    pool: PgPool,
}

impl PostgresApiKeyStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ApiKeyStore for PostgresApiKeyStore {
    async fn create_service_account(
        &self,
        project_id: &str,
        name: &str,
    ) -> Result<ProjectServiceAccount, ImageGatewayError> {
        validate_project_id(project_id)?;
        let name = validate_name(name)?;
        let created_at = now_seconds();
        let service_account_id = new_id("svc_acct");
        let key_id = new_id("key");
        let value = new_key_value();
        let redacted_value = redact_key(&value);
        let hash = hash_key(&value);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        sqlx::query(
            r#"
            INSERT INTO gateway_service_accounts
              (id, project_id, name, role, created_at)
            VALUES ($1, $2, $3, 'member', $4)
            "#,
        )
        .bind(&service_account_id)
        .bind(project_id)
        .bind(&name)
        .bind(created_at)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;

        sqlx::query(
            r#"
            INSERT INTO gateway_api_keys
              (id, project_id, service_account_id, name, key_hash, redacted_value, created_at)
            VALUES ($1, $2, $3, 'Secret Key', $4, $5, $6)
            "#,
        )
        .bind(&key_id)
        .bind(project_id)
        .bind(&service_account_id)
        .bind(hash)
        .bind(&redacted_value)
        .bind(created_at)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;

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

    async fn authenticate(&self, bearer: &str) -> Result<Option<AuthContext>, ImageGatewayError> {
        let hash = hash_key(bearer);
        let row: Option<(String, String)> = sqlx::query_as(
            r#"
            UPDATE gateway_api_keys
            SET last_used_at = $1
            WHERE key_hash = $2 AND deleted_at IS NULL
            RETURNING id, project_id
            "#,
        )
        .bind(now_seconds())
        .bind(hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;

        let Some((api_key_id, project_id)) = row else {
            return Ok(None);
        };
        Ok(Some(AuthContext {
            tenant_id: project_id.clone(),
            project_id,
            api_key_id: Some(api_key_id),
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
        let rows = sqlx::query_as::<
            _,
            (
                String,
                String,
                String,
                i64,
                Option<i64>,
                String,
                String,
                i64,
            ),
        >(
            r#"
            SELECT
              k.id,
              k.redacted_value,
              k.name,
              k.created_at,
              k.last_used_at,
              s.id AS service_account_id,
              s.name AS service_account_name,
              s.created_at AS service_account_created_at
            FROM gateway_api_keys k
            JOIN gateway_service_accounts s ON s.id = k.service_account_id
            WHERE k.project_id = $1
              AND k.deleted_at IS NULL
              AND ($2::TEXT IS NULL OR k.created_at > (
                SELECT created_at FROM gateway_api_keys WHERE id = $2
              ))
            ORDER BY k.created_at ASC
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
            .collect::<Vec<_>>();
        Ok(project_api_key_list(data, has_more))
    }

    async fn delete_project_api_key(
        &self,
        project_id: &str,
        api_key_id: &str,
    ) -> Result<ProjectApiKeyDeleted, ImageGatewayError> {
        validate_project_id(project_id)?;
        let result = sqlx::query(
            r#"
            UPDATE gateway_api_keys
            SET deleted_at = $1
            WHERE project_id = $2 AND id = $3 AND deleted_at IS NULL
            "#,
        )
        .bind(now_seconds())
        .bind(project_id)
        .bind(api_key_id)
        .execute(&self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("api key state unavailable"))?;
        if result.rows_affected() == 0 {
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
}

fn project_api_key_from_row(
    row: (
        String,
        String,
        String,
        i64,
        Option<i64>,
        String,
        String,
        i64,
    ),
) -> ProjectApiKey {
    let (
        id,
        redacted_value,
        name,
        created_at,
        last_used_at,
        service_account_id,
        service_account_name,
        service_account_created_at,
    ) = row;
    ProjectApiKey {
        object: "organization.project.api_key",
        redacted_value,
        name,
        created_at,
        last_used_at,
        id,
        owner: ProjectApiKeyOwner {
            owner_type: "service_account",
            service_account: ProjectApiKeyServiceAccountOwner {
                id: service_account_id,
                name: service_account_name,
                role: "member",
                created_at: service_account_created_at,
            },
        },
    }
}

fn project_api_key_from_memory(
    state: &InMemoryApiKeyState,
    api_key: StoredApiKey,
) -> Option<ProjectApiKey> {
    let service_account = state
        .service_accounts
        .iter()
        .find(|account| account.id == api_key.service_account_id)?;
    Some(ProjectApiKey {
        object: "organization.project.api_key",
        redacted_value: api_key.redacted_value,
        name: api_key.name,
        created_at: api_key.created_at,
        last_used_at: api_key.last_used_at,
        id: api_key.id,
        owner: ProjectApiKeyOwner {
            owner_type: "service_account",
            service_account: ProjectApiKeyServiceAccountOwner {
                id: service_account.id.clone(),
                name: service_account.name.clone(),
                role: "member",
                created_at: service_account.created_at,
            },
        },
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

fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

fn new_key_value() -> String {
    format!(
        "sk-gw-{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
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
