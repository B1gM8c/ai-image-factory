use std::{
    collections::{HashMap, HashSet},
    fs,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use base64::Engine;
use image_provider_contracts::openai_codex;
use image_provider_dreamina_cli::{
    DREAMINA_SUBMIT_COMMAND_SCHEMA, PROVIDER_ID as DREAMINA_PROVIDER_ID,
};
use image_provider_grok_cli::PROVIDER_ID as GROK_PROVIDER_ID;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::task::JoinSet;
use tokio::time::timeout;
use uuid::Uuid;

use crate::{
    CodexExecutionProfileProvisioning, CredentialResolveError,
    DreaminaExecutionProfileProvisioning, DreaminaKeychainReplacement,
    GrokExecutionProfileProvisioning, ImageGatewayError, OperationalCredentialResolver,
    PostgresCredentialStore, codex_auth_file_sha256, dreamina_account_isolation_available,
    dreamina_credential_fingerprint, grok_auth_file_sha256, prepare_codex_auth_copy,
    provision_codex_edit_execution_profile_in_transaction,
    provision_codex_execution_profile_in_transaction,
    provision_dreamina_execution_profile_in_transaction,
    provision_dreamina_video_execution_profile_in_transaction,
    provision_grok_edit_execution_profile_in_transaction,
    provision_grok_execution_profile_in_transaction,
    provision_grok_video_execution_profile_in_transaction,
};

use super::{
    ApiKeyRouteBindingView, CodexLoginMethod, CreateProviderRouteRequest, GrokVideoOutputView,
    ManagedCliProviderCapability, ManagedCliProvidersSnapshot,
    ProviderAccountModelConfigurationView, ProviderAccountModelsView,
    ProviderAccountSchedulingView, ProviderLoginSession, ProviderManagementService,
    ProviderModelRefreshView, ProviderModelsSnapshot, ProviderRouteMemberView,
    ProviderRouteModelMappingRequest, ProviderRouteModelMappingView, ProviderRouteView,
    ProviderRoutesSnapshot, StartCodexLoginRequest, StartProviderLoginRequest,
    StartProviderReauthorizationRequest, UpdateGrokVideoOutputRequest,
    UpdateProviderAccountModelConfigurationRequest, UpdateProviderAccountModelsRequest,
    UpdateProviderAccountSchedulingRequest, UpdateProviderRouteRequest,
    codex_app_server::{
        CodexAccountSnapshot, CodexAppServer, CodexQuotaSnapshot, resolve_executable,
    },
    dreamina_login::{
        DreaminaAccountSnapshot, DreaminaCliPermission, DreaminaLoginProcess,
        observe_dreamina_account,
    },
    grok_billing::{GrokQuotaSnapshot, observe_grok_quota},
    grok_login::{GrokLoginProcess, copy_proxy_environment, refresh_grok_auth},
    model_catalog::{self, ProviderModelExecutables},
    reconcile_execution_profile_routes,
};

const LOGIN_TTL: Duration = Duration::from_secs(15 * 60);
const APP_SERVER_START_TIMEOUT: Duration = Duration::from_secs(20);
const APP_SERVER_READ_TIMEOUT: Duration = Duration::from_secs(30);
const CODEX_LOGIN_ACCOUNT_RETRY_DELAY: Duration = Duration::from_millis(250);
const CODEX_QUOTA_READ_TIMEOUT: Duration = Duration::from_secs(15);
const CODEX_QUOTA_RETRY_DELAY: Duration = Duration::from_millis(250);
const MAX_ACCOUNT_KEY_BYTES: usize = 64;
const MAX_DISPLAY_NAME_CHARS: usize = 128;
const MAX_AUTH_BYTES: usize = 1024 * 1024;
const CREDENTIAL_REFRESH_INTERVAL_MS: i64 = 6 * 60 * 60 * 1_000;
const CREDENTIAL_REFRESH_SKEW_MS: i64 = 15 * 60 * 1_000;
static CODEX_QUOTA_REFRESHES: LazyLock<Mutex<HashSet<Uuid>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
static GROK_QUOTA_REFRESHES: LazyLock<Mutex<HashSet<Uuid>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
static DREAMINA_QUOTA_REFRESHES: LazyLock<Mutex<HashSet<Uuid>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
static GROK_VIDEO_OUTPUT_UPDATES: LazyLock<Mutex<HashSet<Uuid>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[derive(Debug)]
struct CodexQuotaRefreshGuard(Uuid);

impl CodexQuotaRefreshGuard {
    fn acquire(provider_account_id: Uuid) -> Result<Self, ImageGatewayError> {
        let mut in_flight = CODEX_QUOTA_REFRESHES
            .lock()
            .map_err(|_| ImageGatewayError::service_unavailable("Quota refresh is unavailable"))?;
        if !in_flight.insert(provider_account_id) {
            return Err(ImageGatewayError::conflict(
                "A Codex quota refresh is already in progress",
                Some("provider_account_id".to_owned()),
                "quota_refresh_in_progress",
            ));
        }
        Ok(Self(provider_account_id))
    }
}

impl Drop for CodexQuotaRefreshGuard {
    fn drop(&mut self) {
        if let Ok(mut in_flight) = CODEX_QUOTA_REFRESHES.lock() {
            in_flight.remove(&self.0);
        }
    }
}

struct GrokQuotaRefreshGuard(Uuid);

impl GrokQuotaRefreshGuard {
    fn acquire(provider_account_id: Uuid) -> Result<Self, ImageGatewayError> {
        let mut in_flight = GROK_QUOTA_REFRESHES
            .lock()
            .map_err(|_| ImageGatewayError::service_unavailable("Quota refresh is unavailable"))?;
        if !in_flight.insert(provider_account_id) {
            return Err(ImageGatewayError::conflict(
                "A Grok quota refresh is already in progress",
                Some("provider_account_id".to_owned()),
                "quota_refresh_in_progress",
            ));
        }
        Ok(Self(provider_account_id))
    }
}

impl Drop for GrokQuotaRefreshGuard {
    fn drop(&mut self) {
        if let Ok(mut in_flight) = GROK_QUOTA_REFRESHES.lock() {
            in_flight.remove(&self.0);
        }
    }
}

struct GrokVideoOutputUpdateGuard(Uuid);

impl GrokVideoOutputUpdateGuard {
    fn acquire(provider_account_id: Uuid) -> Result<Self, ImageGatewayError> {
        let mut in_flight = GROK_VIDEO_OUTPUT_UPDATES.lock().map_err(|_| {
            ImageGatewayError::service_unavailable("Grok video output configuration is unavailable")
        })?;
        if !in_flight.insert(provider_account_id) {
            return Err(ImageGatewayError::conflict(
                "A Grok video output update is already in progress",
                Some("provider_account_id".to_owned()),
                "grok_video_output_update_in_progress",
            ));
        }
        Ok(Self(provider_account_id))
    }
}

impl Drop for GrokVideoOutputUpdateGuard {
    fn drop(&mut self) {
        if let Ok(mut in_flight) = GROK_VIDEO_OUTPUT_UPDATES.lock() {
            in_flight.remove(&self.0);
        }
    }
}

struct DreaminaQuotaRefreshGuard(Uuid);

impl DreaminaQuotaRefreshGuard {
    fn acquire(provider_account_id: Uuid) -> Result<Self, ImageGatewayError> {
        let mut in_flight = DREAMINA_QUOTA_REFRESHES
            .lock()
            .map_err(|_| ImageGatewayError::service_unavailable("Quota refresh is unavailable"))?;
        if !in_flight.insert(provider_account_id) {
            return Err(ImageGatewayError::conflict(
                "A Dreamina credit refresh is already in progress",
                Some("provider_account_id".to_owned()),
                "quota_refresh_in_progress",
            ));
        }
        Ok(Self(provider_account_id))
    }
}

impl Drop for DreaminaQuotaRefreshGuard {
    fn drop(&mut self) {
        if let Ok(mut in_flight) = DREAMINA_QUOTA_REFRESHES.lock() {
            in_flight.remove(&self.0);
        }
    }
}

#[derive(Clone)]
pub struct PostgresProviderManagementService {
    pool: PgPool,
    credential_store: PostgresCredentialStore,
    homes_root: Arc<PathBuf>,
    codex_executable: Arc<PathBuf>,
    grok_executable: Option<Arc<PathBuf>>,
    dreamina_executable: Option<Arc<PathBuf>>,
}

#[derive(sqlx::FromRow)]
struct LoginSessionRow {
    login_session_id: Uuid,
    provider_id: String,
    account_key: String,
    display_name: String,
    status: String,
    login_method: String,
    authorization_url: Option<String>,
    user_code: Option<String>,
    provider_account_id: Option<Uuid>,
    error_code: Option<String>,
    expires_at_ms: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct ReauthorizationTargetRow {
    provider_id: String,
    display_name: String,
    max_concurrency: i32,
}

#[derive(sqlx::FromRow)]
struct DreaminaVideoBackfillRow {
    provider_account_id: Uuid,
    pool_key: String,
    account_key: String,
    credential_ref: String,
    credential_revision: i64,
    credential_auth_sha256: String,
    max_concurrency: i32,
    display_name: String,
}

#[derive(sqlx::FromRow)]
struct LockedReauthorizationTargetRow {
    provider_id: String,
    environment_ref: String,
    upstream_identity_sha256: String,
    active_revision: i64,
    material_fingerprint_sha256: String,
    access_expires_at_ms: Option<i64>,
}

struct DreaminaLoginCompletion {
    account_key: String,
    display_name: String,
    max_concurrency: i32,
    operation_ids: Vec<String>,
    home: PathBuf,
    account: DreaminaAccountSnapshot,
}

#[derive(sqlx::FromRow)]
struct RouteRow {
    route_id: Uuid,
    revision: i64,
    route_key: String,
    display_name: String,
    provider_id: String,
    operation_id: String,
    command_schema: String,
    route_kind: String,
    selection_strategy: String,
    quota_freshness_ms: i64,
    unknown_quota_policy: String,
    state: String,
    created_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct RouteMemberRow {
    route_id: Uuid,
    route_revision: i64,
    provider_account_id: Uuid,
    account_key: String,
    execution_profile_id: Uuid,
    priority: i16,
    weight: i32,
    minimum_remaining_percent: i16,
}

#[derive(Clone, sqlx::FromRow)]
struct RouteModelMappingRow {
    route_id: Uuid,
    route_revision: i64,
    api_profile: String,
    public_model_id: String,
    provider_model_id: String,
    execution_model_id: String,
    provider_model_display_name: String,
    media_kind: String,
}

#[derive(Clone)]
struct ValidatedRouteModelMapping {
    api_profile: String,
    public_model_id: String,
    provider_model_id: String,
    execution_model_id: String,
    provider_model_display_name: String,
    media_kind: String,
}

#[derive(sqlx::FromRow)]
struct AvailableRouteModelRow {
    model_id: String,
    execution_model_id: String,
    display_name: String,
    media_kind: String,
}

#[derive(sqlx::FromRow)]
struct ProfileMemberRow {
    provider_account_id: Uuid,
    account_key: String,
    execution_profile_id: Uuid,
    command_schema: String,
}

#[derive(sqlx::FromRow)]
struct ApiKeyOwnerRow {
    service_account_id: String,
    tenant_id: String,
}

#[derive(sqlx::FromRow)]
struct ApiKeyRouteRow {
    api_key_id: String,
    project_id: String,
    provider_id: String,
    operation_id: String,
    command_schema: String,
    route_id: Uuid,
    route_revision: i64,
    route_name: String,
    bound_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct AccountExecutionControlRow {
    desired_max_concurrency: i32,
    lifecycle_state: String,
    control_version: i64,
}

#[derive(sqlx::FromRow)]
struct AccountExecutionRuntimeRow {
    hard_max_concurrency: i32,
    allocated_count: i32,
    policy_state: String,
    account_state: String,
    credential_pool_state: String,
    environment_state: String,
    profiles_enabled: bool,
}

impl PostgresProviderManagementService {
    pub fn new(pool: PgPool, homes_root: PathBuf, codex_executable: PathBuf) -> Self {
        Self {
            credential_store: PostgresCredentialStore::new(pool.clone()),
            pool,
            homes_root: Arc::new(homes_root),
            codex_executable: Arc::new(codex_executable),
            grok_executable: None,
            dreamina_executable: None,
        }
    }

    pub async fn from_env(pool: PgPool) -> Result<Self, ImageGatewayError> {
        let configured_root = std::env::var_os("GATEWAY_PROVIDER_HOME_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(".data/provider-homes"));
        let root = if configured_root.is_absolute() {
            configured_root
        } else {
            std::env::current_dir()
                .map_err(|_| ImageGatewayError::config("provider home root is unavailable"))?
                .join(configured_root)
        };
        create_private_directory(&root)?;
        let root = root
            .canonicalize()
            .map_err(|_| ImageGatewayError::config("provider home root is unavailable"))?;
        let configured_executable = std::env::var_os("GATEWAY_MANAGED_CODEX_EXECUTABLE")
            .or_else(|| std::env::var_os("EXECUTOR_CODEX_EXECUTABLE"))
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("codex"));
        let executable = resolve_executable(configured_executable)
            .map_err(|_| ImageGatewayError::config("managed Codex executable is unavailable"))?;
        let grok_executable = resolve_optional_executable(
            "GATEWAY_MANAGED_GROK_EXECUTABLE",
            "EXECUTOR_GROK_EXECUTABLE",
            "grok",
        )?;
        let dreamina_executable = resolve_optional_executable(
            "GATEWAY_MANAGED_DREAMINA_EXECUTABLE",
            "PROVIDER_DREAMINA_EXECUTABLE",
            "dreamina",
        )?;
        let service = Self {
            credential_store: PostgresCredentialStore::new(pool.clone()),
            pool,
            homes_root: Arc::new(root),
            codex_executable: Arc::new(executable),
            grok_executable: grok_executable.map(Arc::new),
            dreamina_executable: dreamina_executable.map(Arc::new),
        };
        service.expire_stale_logins().await?;
        model_catalog::fail_interrupted_refreshes(&service.pool).await?;
        model_catalog::reconcile_adapter_models(&service.pool).await?;
        reconcile_route_model_mappings(&service.pool).await?;
        if service.dreamina_executable.is_some() {
            Self::reconcile_dreamina_video_profiles(&service.pool).await?;
        }
        reconcile_execution_profile_routes(&service.pool).await?;
        Ok(service)
    }

    async fn grok_video_output_home(
        &self,
        provider_account_id: Uuid,
    ) -> Result<PathBuf, ImageGatewayError> {
        let target: Option<(String, String)> = sqlx::query_as(
            r#"
            SELECT account.provider_id, environment.environment_ref
            FROM provider_accounts account
            JOIN provider_account_environments environment
              ON environment.provider_account_id = account.provider_account_id
             AND environment.provider_id = account.provider_id
            WHERE account.provider_account_id = $1
              AND account.state = 'enabled'
            "#,
        )
        .bind(provider_account_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_unavailable)?;
        let (provider_id, environment_ref) = target.ok_or_else(|| {
            ImageGatewayError::not_found(
                "Managed provider account not found",
                Some("provider_account_id".to_owned()),
                "provider_account_not_found",
            )
        })?;
        if provider_id != GROK_PROVIDER_ID {
            return Err(ImageGatewayError::invalid_request(
                "Video output storage is only available for Grok accounts",
                Some("provider_account_id".to_owned()),
                "provider_account_not_grok",
            ));
        }
        let home = PathBuf::from(environment_ref);
        validate_private_home(&home)?;
        let home = home.canonicalize().map_err(|_| {
            ImageGatewayError::service_unavailable("Provider credential environment is unavailable")
        })?;
        if !home.starts_with(self.homes_root.as_ref()) {
            return Err(ImageGatewayError::service_unavailable(
                "Provider credential environment is invalid",
            ));
        }
        Ok(home)
    }

    pub async fn reconcile_dreamina_video_profiles(
        pool: &PgPool,
    ) -> Result<usize, ImageGatewayError> {
        let rows = sqlx::query_as::<_, DreaminaVideoBackfillRow>(
            r#"
            SELECT account.provider_account_id, pool.pool_key, account.account_key,
                   account.credential_ref, account.credential_revision,
                   account.credential_auth_sha256, policy.max_concurrency,
                   environment.display_name
            FROM provider_accounts account
            JOIN provider_credential_pools pool
              ON pool.credential_pool_id = account.credential_pool_id
            JOIN provider_account_environments environment
              ON environment.provider_account_id = account.provider_account_id
            JOIN provider_account_operations capability
              ON capability.provider_account_id = account.provider_account_id
             AND capability.provider_id = account.provider_id
             AND capability.operation_id = 'videos.generations'
             AND capability.state = 'enabled'
            JOIN executor_resource_policies policy
              ON policy.provider_account_id = account.provider_account_id
             AND policy.state = 'enabled'
            WHERE account.provider_id = $1
              AND account.state = 'enabled'
              AND EXISTS (
                SELECT 1 FROM provider_execution_profiles image_profile
                WHERE image_profile.provider_account_id = account.provider_account_id
                  AND image_profile.operation_id = 'images.generations'
                  AND image_profile.command_schema = $2
                  AND image_profile.state = 'enabled'
              )
              AND NOT EXISTS (
                SELECT 1 FROM provider_execution_profiles video_profile
                WHERE video_profile.provider_account_id = account.provider_account_id
                  AND video_profile.operation_id = 'videos.generations'
                  AND video_profile.command_schema = $2
              )
            ORDER BY account.provider_account_id
            "#,
        )
        .bind(DREAMINA_PROVIDER_ID)
        .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
        .fetch_all(pool)
        .await
        .map_err(store_unavailable)?;

        let mut created = 0;
        for row in rows {
            let mut tx = pool.begin().await.map_err(store_unavailable)?;
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(format!(
                    "dreamina-video-backfill:{}",
                    row.provider_account_id
                ))
                .execute(&mut *tx)
                .await
                .map_err(store_unavailable)?;
            let capability_enabled: Option<i32> = sqlx::query_scalar(
                r#"
                SELECT 1
                FROM provider_account_operations capability
                JOIN provider_accounts account
                  ON account.provider_account_id = capability.provider_account_id
                 AND account.provider_id = capability.provider_id
                WHERE capability.provider_account_id = $1
                  AND capability.provider_id = $2
                  AND capability.operation_id = 'videos.generations'
                  AND capability.state = 'enabled'
                  AND account.state = 'enabled'
                FOR UPDATE OF capability, account
                "#,
            )
            .bind(row.provider_account_id)
            .bind(DREAMINA_PROVIDER_ID)
            .fetch_optional(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            if capability_enabled.is_none() {
                tx.commit().await.map_err(store_unavailable)?;
                continue;
            }
            let already_exists: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                  SELECT 1 FROM provider_execution_profiles
                  WHERE provider_account_id = $1
                    AND operation_id = 'videos.generations'
                    AND command_schema = $2
                )
                "#,
            )
            .bind(row.provider_account_id)
            .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
            .fetch_one(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            if already_exists {
                tx.commit().await.map_err(store_unavailable)?;
                continue;
            }
            let provisioning = DreaminaExecutionProfileProvisioning {
                profile_key: format!(
                    "managed.dreamina.videos.backfill.{}",
                    row.provider_account_id.simple()
                ),
                credential_pool_key: row.pool_key,
                provider_account_key: row.account_key,
                credential_ref: row.credential_ref,
                credential_revision: row.credential_revision,
                credential_auth_sha256: row.credential_auth_sha256,
                max_concurrency: row.max_concurrency,
            };
            let video =
                provision_dreamina_video_execution_profile_in_transaction(&mut tx, &provisioning)
                    .await
                    .map_err(map_profile_provisioning)?;
            if video.provider_account_id != row.provider_account_id {
                return Err(ImageGatewayError::internal(
                    "Dreamina video backfill resolved to a different account",
                ));
            }
            let now = database_now(&mut tx).await?;
            insert_account_route(
                &mut tx,
                video.execution_profile_id,
                &row.display_name,
                "videos",
                now,
            )
            .await?;
            tx.commit().await.map_err(store_unavailable)?;
            created += 1;
        }
        Ok(created)
    }

    pub async fn refresh_due_credentials_once(&self) -> Result<usize, ImageGatewayError> {
        let provider_account_ids = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT head.provider_account_id
            FROM provider_account_credential_heads head
            JOIN provider_accounts account USING (provider_account_id)
            WHERE head.refresh_strategy IN ('broker_managed', 'cli_managed')
              AND head.lifecycle_state IN ('active', 'refresh_due', 'refreshing')
              AND account.state = 'enabled'
              AND (head.next_refresh_at_ms IS NULL OR head.next_refresh_at_ms <=
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
                   OR (head.lifecycle_state = 'refreshing'
                       AND head.lease_expires_at_ms <=
                           floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT))
            ORDER BY head.next_refresh_at_ms NULLS FIRST, head.provider_account_id
            LIMIT 32
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(store_unavailable)?;
        let mut tasks = JoinSet::new();
        let mut succeeded = 0_usize;
        for provider_account_id in provider_account_ids {
            while tasks.len() >= 4 {
                if matches!(tasks.join_next().await, Some(Ok(Ok(())))) {
                    succeeded += 1;
                }
            }
            let service = self.clone();
            tasks.spawn(async move {
                service
                    .refresh_operational_credential(provider_account_id, false)
                    .await
            });
        }
        while let Some(result) = tasks.join_next().await {
            if matches!(result, Ok(Ok(()))) {
                succeeded += 1;
            }
        }
        Ok(succeeded)
    }

    async fn expire_stale_logins(&self) -> Result<(), ImageGatewayError> {
        let now = now_ms()?;
        sqlx::query(
            r#"
            UPDATE provider_account_login_sessions
            SET status = 'expired', error_code = 'gateway_restarted',
                updated_at_ms = $1, completed_at_ms = $1,
                authorization_url = NULL, user_code = NULL
            WHERE status IN ('starting', 'waiting_for_user', 'validating')
            "#,
        )
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(store_unavailable)?;
        Ok(())
    }

    async fn refresh_operational_credential(
        &self,
        provider_account_id: Uuid,
        force: bool,
    ) -> Result<(), ImageGatewayError> {
        let owner = format!("gateway.{}", Uuid::new_v4().simple());
        let Some(lease) = self
            .credential_store
            .claim_refresh(provider_account_id, &owner, 90_000, force)
            .await
            .map_err(map_credential_store_error)?
        else {
            return Ok(());
        };
        let result = self.refresh_claimed_credential(&lease).await;
        if let Err((error_code, reauthorization_required)) = result {
            if lease.provider_id == DREAMINA_PROVIDER_ID {
                let _ = mark_dreamina_quota_unavailable(
                    &self.pool,
                    lease.provider_account_id,
                    error_code,
                )
                .await;
            }
            let _ = self
                .credential_store
                .fail_refresh(&lease, error_code, reauthorization_required)
                .await;
            return Err(if reauthorization_required {
                ImageGatewayError::service_unavailable(
                    "Provider account authorization expired; reauthorize the account",
                )
            } else {
                ImageGatewayError::service_unavailable("Provider credential refresh failed")
            });
        }
        Ok(())
    }

    async fn refresh_claimed_credential(
        &self,
        lease: &crate::CredentialRefreshLease,
    ) -> Result<(), (&'static str, bool)> {
        let expected_identity: Option<String> = sqlx::query_scalar(
            r#"
            SELECT upstream_identity_sha256
            FROM provider_account_environments
            WHERE provider_account_id = $1 AND provider_id = $2
              AND (state = 'active' OR ($2 = 'dreamina-cli' AND state = 'disabled'))
            "#,
        )
        .bind(lease.provider_account_id)
        .bind(&lease.provider_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ("credential_store_unavailable", false))?;
        let expected_identity = expected_identity
            .as_deref()
            .ok_or(("credential_identity_unavailable", true))?;
        if lease.provider_id == DREAMINA_PROVIDER_ID {
            let executable = self
                .dreamina_executable
                .as_deref()
                .ok_or(("dreamina_cli_unavailable", false))?;
            let account = observe_dreamina_account(executable, &lease.environment_ref)
                .await
                .map_err(|error| (error.code(), error.reauthorization_required()))?;
            if dreamina_identity_sha256(&account) != expected_identity {
                return Err(("credential_identity_changed", true));
            }
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|_| ("credential_store_unavailable", false))?;
            let observed_at_ms = database_now(&mut tx)
                .await
                .map_err(|_| ("credential_store_unavailable", false))?;
            persist_dreamina_quota(&mut tx, lease.provider_account_id, &account, observed_at_ms)
                .await
                .map_err(|_| ("dreamina_credit_persistence_failed", false))?;
            persist_dreamina_capability_state(
                &mut tx,
                lease.provider_account_id,
                account.cli_permission,
                observed_at_ms,
            )
            .await
            .map_err(|_| ("dreamina_capability_persistence_failed", false))?;
            self.credential_store
                .complete_cli_managed_refresh_in_transaction(&mut tx, lease)
                .await
                .map_err(|_| ("credential_promotion_failed", false))?;
            tx.commit()
                .await
                .map_err(|_| ("credential_store_unavailable", false))?;
            return Ok(());
        }
        let current_fingerprint =
            credential_fingerprint(&lease.provider_id, &lease.environment_ref)
                .map_err(|_| ("credential_material_invalid", true))?;
        if current_fingerprint == lease.material_fingerprint_sha256 {
            match lease.provider_id.as_str() {
                openai_codex::PROVIDER_ID => {
                    let mut server = timeout(
                        APP_SERVER_START_TIMEOUT,
                        CodexAppServer::spawn(&self.codex_executable, &lease.environment_ref),
                    )
                    .await
                    .map_err(|_| ("codex_refresh_timeout", false))?
                    .map_err(|_| ("codex_refresh_unavailable", false))?;
                    let refresh = timeout(APP_SERVER_READ_TIMEOUT, server.refresh_account()).await;
                    server.shutdown().await;
                    refresh
                        .map_err(|_| ("codex_refresh_timeout", false))?
                        .map_err(|error| {
                            if error.reauthorization_required() {
                                ("codex_reauthorization_required", true)
                            } else {
                                ("codex_refresh_unavailable", false)
                            }
                        })?;
                }
                GROK_PROVIDER_ID => {
                    let executable = self
                        .grok_executable
                        .as_deref()
                        .ok_or(("grok_cli_unavailable", false))?;
                    refresh_grok_auth(executable, &lease.environment_ref)
                        .await
                        .map_err(|_| ("grok_refresh_failed", false))?;
                }
                _ => return Err(("credential_refresh_unsupported", false)),
            }
        }
        let observed_identity = credential_identity(&lease.provider_id, &lease.environment_ref)
            .map_err(|_| ("credential_identity_unavailable", true))?;
        if observed_identity != expected_identity {
            return Err(("credential_identity_changed", true));
        }
        let fingerprint = credential_fingerprint(&lease.provider_id, &lease.environment_ref)
            .map_err(|_| ("credential_material_invalid", true))?;
        let expires_at_ms = credential_expires_at_ms(&lease.provider_id, &lease.environment_ref);
        self.credential_store
            .promote_auth_file(lease, &fingerprint, expires_at_ms)
            .await
            .map_err(|_| ("credential_promotion_failed", false))?;
        Ok(())
    }

    fn create_login_home(
        &self,
        provider_namespace: &str,
        login_session_id: Uuid,
    ) -> Result<PathBuf, ImageGatewayError> {
        let provider_root = self.homes_root.join(provider_namespace);
        create_private_directory(&provider_root)?;
        let home = provider_root.join(login_session_id.simple().to_string());
        fs::create_dir(&home).map_err(|_| {
            ImageGatewayError::service_unavailable("provider account home unavailable")
        })?;
        fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).map_err(|_| {
            ImageGatewayError::service_unavailable("provider account home unavailable")
        })?;
        Ok(home)
    }

    async fn reauthorization_target(
        &self,
        provider_account_id: Uuid,
    ) -> Result<ReauthorizationTargetRow, ImageGatewayError> {
        if provider_account_id.is_nil() {
            return Err(ImageGatewayError::invalid_request(
                "Provider account is invalid",
                Some("provider_account_id".to_owned()),
                "invalid_provider_account",
            ));
        }
        sqlx::query_as::<_, ReauthorizationTargetRow>(
            r#"
            SELECT account.provider_id, environment.display_name,
                   control.desired_max_concurrency AS max_concurrency
            FROM provider_accounts account
            JOIN provider_account_environments environment
              ON environment.provider_account_id = account.provider_account_id
             AND environment.provider_id = account.provider_id
            JOIN provider_account_execution_controls control
              ON control.provider_account_id = account.provider_account_id
            WHERE account.provider_account_id = $1
              AND account.state = 'enabled'
              AND environment.state IN ('active', 'disabled', 'invalid')
            "#,
        )
        .bind(provider_account_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Managed provider account not found",
                Some("provider_account_id".to_owned()),
                "provider_account_not_found",
            )
        })
    }

    async fn complete_reauthorization(
        &self,
        login_session_id: Uuid,
        provider_account_id: Uuid,
        provider_id: &str,
        fresh_home: &Path,
        observed_identity_sha256: &str,
        account_email: Option<&str>,
    ) -> Result<Uuid, ImageGatewayError> {
        let fresh_fingerprint = credential_fingerprint(provider_id, fresh_home)?;
        let access_expires_at_ms = credential_expires_at_ms(provider_id, fresh_home);
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "provider-account-reauthorization:{provider_account_id}"
            ))
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
        let target = sqlx::query_as::<_, LockedReauthorizationTargetRow>(
            r#"
            SELECT account.provider_id, environment.environment_ref,
                   environment.upstream_identity_sha256, head.active_revision,
                   revision.material_fingerprint_sha256, revision.access_expires_at_ms
            FROM provider_accounts account
            JOIN provider_account_environments environment
              ON environment.provider_account_id = account.provider_account_id
             AND environment.provider_id = account.provider_id
            JOIN provider_account_credential_heads head
              ON head.provider_account_id = account.provider_account_id
            JOIN provider_account_credential_revisions revision
              ON revision.provider_account_id = head.provider_account_id
             AND revision.revision = head.active_revision
            WHERE account.provider_account_id = $1
              AND account.state = 'enabled'
              AND environment.state IN ('active', 'disabled', 'invalid')
            FOR UPDATE OF account, environment, head
            "#,
        )
        .bind(provider_account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Managed provider account not found",
                Some("provider_account_id".to_owned()),
                "provider_account_not_found",
            )
        })?;
        if target.provider_id != provider_id {
            return Err(ImageGatewayError::invalid_request(
                "Provider account does not match the login provider",
                Some("provider_account_id".to_owned()),
                "provider_account_provider_mismatch",
            ));
        }
        if target.upstream_identity_sha256 != observed_identity_sha256 {
            return Err(ImageGatewayError::invalid_request(
                "The authorized upstream identity does not match this managed account",
                Some("provider_account_id".to_owned()),
                "provider_account_identity_mismatch",
            ));
        }

        let destination_home = PathBuf::from(&target.environment_ref);
        let replacement =
            AuthFileReplacement::install(fresh_home, &destination_home, login_session_id)?;
        let result = async {
            let now = database_now(&mut tx).await?;
            let next_revision = if target.material_fingerprint_sha256 == fresh_fingerprint
                && target.access_expires_at_ms == access_expires_at_ms
            {
                target.active_revision
            } else {
                let next_revision = target.active_revision.checked_add(1).ok_or_else(|| {
                    ImageGatewayError::internal("Provider credential revision overflow")
                })?;
                sqlx::query(
                    r#"
                    INSERT INTO provider_account_credential_revisions
                      (provider_account_id, revision, material_kind,
                       material_fingerprint_sha256, access_expires_at_ms, created_at_ms)
                    VALUES ($1, $2, 'auth_file', $3, $4, $5)
                    "#,
                )
                .bind(provider_account_id)
                .bind(next_revision)
                .bind(&fresh_fingerprint)
                .bind(access_expires_at_ms)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(store_unavailable)?;
                next_revision
            };
            let refresh_at = credential_refresh_deadline(access_expires_at_ms, now);
            sqlx::query(
                r#"
                UPDATE provider_account_credential_heads
                SET active_revision = $2, lifecycle_state = 'active',
                    refresh_after_ms = $3, next_refresh_at_ms = $3,
                    last_attempt_at_ms = $4, last_success_at_ms = $4,
                    consecutive_failures = 0, last_error_code = NULL,
                    lease_owner = NULL, lease_expires_at_ms = NULL,
                    updated_at_ms = $4, control_version = control_version + 1
                WHERE provider_account_id = $1
                "#,
            )
            .bind(provider_account_id)
            .bind(next_revision)
            .bind(refresh_at)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            sqlx::query(
                r#"
                UPDATE provider_account_environments
                SET account_email = COALESCE($2, account_email), state = 'active',
                    updated_at_ms = $3
                WHERE provider_account_id = $1
                "#,
            )
            .bind(provider_account_id)
            .bind(account_email)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            sqlx::query(
                r#"
                INSERT INTO provider_account_credential_events
                  (credential_event_id, provider_account_id, event_type, from_revision,
                   to_revision, lease_epoch, executor_execution_id, error_code, created_at_ms)
                VALUES ($1, $2, 'reauth_succeeded', $3, $4, NULL, NULL, NULL, $5)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(provider_account_id)
            .bind(target.active_revision)
            .bind(next_revision)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            let updated = sqlx::query(
                r#"
                UPDATE provider_account_login_sessions
                SET status = 'succeeded', updated_at_ms = $3, completed_at_ms = $3,
                    error_code = NULL, authorization_url = NULL, user_code = NULL
                WHERE login_session_id = $1 AND provider_account_id = $2
                  AND status = 'validating'
                "#,
            )
            .bind(login_session_id)
            .bind(provider_account_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            if updated.rows_affected() != 1 {
                return Err(ImageGatewayError::conflict(
                    "Provider reauthorization session changed during completion",
                    Some("login_session_id".to_owned()),
                    "provider_reauthorization_session_conflict",
                ));
            }
            tx.commit().await.map_err(store_unavailable)
        }
        .await;
        match result {
            Ok(()) => Ok(provider_account_id),
            Err(error) => {
                if let Err(restore_error) = replacement.rollback() {
                    tracing::error!(
                        %provider_account_id,
                        error = ?restore_error,
                        "provider auth file rollback failed"
                    );
                }
                Err(error)
            }
        }
    }

    async fn set_login_failed(&self, id: Uuid, error_code: &'static str) {
        let Ok(now) = now_ms() else {
            return;
        };
        let _ = sqlx::query(
            r#"
            UPDATE provider_account_login_sessions
            SET status = 'failed', error_code = $2, updated_at_ms = $3,
                completed_at_ms = $3, authorization_url = NULL, user_code = NULL
            WHERE login_session_id = $1 AND status <> 'succeeded'
            "#,
        )
        .bind(id)
        .bind(error_code)
        .bind(now)
        .execute(&self.pool)
        .await;
    }

    async fn complete_login(
        &self,
        login_session_id: Uuid,
        reauthorize_provider_account_id: Option<Uuid>,
        account_key: String,
        display_name: String,
        max_concurrency: i32,
        home: PathBuf,
        account: CodexAccountSnapshot,
    ) -> Result<Uuid, ImageGatewayError> {
        let auth_sha256 = codex_auth_file_sha256(&home)?;
        let upstream_identity_sha256 = upstream_identity_sha256(&home)?;
        if let Some(provider_account_id) = reauthorize_provider_account_id {
            return self
                .complete_reauthorization(
                    login_session_id,
                    provider_account_id,
                    openai_codex::PROVIDER_ID,
                    &home,
                    &upstream_identity_sha256,
                    account.email.as_deref(),
                )
                .await;
        }
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        let identity_lock_key = format!(
            "provider-account-identity:{}:{upstream_identity_sha256}",
            openai_codex::PROVIDER_ID
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(identity_lock_key)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
        let duplicate: Option<Uuid> = sqlx::query_scalar(
            r#"
            SELECT provider_account_id
            FROM provider_account_environments
            WHERE provider_id = $1 AND upstream_identity_sha256 = $2
            "#,
        )
        .bind(openai_codex::PROVIDER_ID)
        .bind(&upstream_identity_sha256)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        if duplicate.is_some() {
            return Err(ImageGatewayError::invalid_request(
                "This Codex account is already managed",
                Some("account_key".to_string()),
                "duplicate_provider_account",
            ));
        }
        let suffix = login_session_id.simple().to_string();
        let provisioned = provision_codex_execution_profile_in_transaction(
            &mut tx,
            &CodexExecutionProfileProvisioning {
                profile_key: format!("managed.codex.images.{suffix}"),
                credential_pool_key: "managed.codex".to_string(),
                provider_account_key: account_key.clone(),
                credential_ref: format!("managed.codex.{suffix}.1"),
                credential_revision: 1,
                credential_auth_sha256: auth_sha256.clone(),
                max_concurrency: 64,
            },
        )
        .await
        .map_err(|error| match error {
            crate::CodexProfileProvisioningError::InvalidInput => {
                ImageGatewayError::invalid_request(
                    "Codex account configuration is invalid",
                    Some("account_key".to_string()),
                    "invalid_provider_account",
                )
            }
            crate::CodexProfileProvisioningError::Conflict => ImageGatewayError::invalid_request(
                "Codex account key already exists",
                Some("account_key".to_string()),
                "provider_account_conflict",
            ),
            crate::CodexProfileProvisioningError::Unavailable => {
                ImageGatewayError::service_unavailable("provider account provisioning unavailable")
            }
        })?;
        let edit_provisioned = provision_codex_edit_execution_profile_in_transaction(
            &mut tx,
            &CodexExecutionProfileProvisioning {
                profile_key: format!("managed.codex.edits.{suffix}"),
                credential_pool_key: "managed.codex".to_string(),
                provider_account_key: account_key.clone(),
                credential_ref: format!("managed.codex.{suffix}.1"),
                credential_revision: 1,
                credential_auth_sha256: auth_sha256.clone(),
                max_concurrency: 64,
            },
        )
        .await
        .map_err(|error| match error {
            crate::CodexProfileProvisioningError::InvalidInput => {
                ImageGatewayError::invalid_request(
                    "Codex edit account configuration is invalid",
                    Some("account_key".to_string()),
                    "invalid_provider_account",
                )
            }
            crate::CodexProfileProvisioningError::Conflict => ImageGatewayError::invalid_request(
                "Codex edit account profile conflicts with existing configuration",
                Some("account_key".to_string()),
                "provider_account_conflict",
            ),
            crate::CodexProfileProvisioningError::Unavailable => {
                ImageGatewayError::service_unavailable("provider account provisioning unavailable")
            }
        })?;
        if edit_provisioned.provider_account_id != provisioned.provider_account_id {
            return Err(ImageGatewayError::internal(
                "Codex generation and edit profiles do not share one account",
            ));
        }
        let now = database_now(&mut tx).await?;
        sqlx::query(
            r#"
            UPDATE provider_account_execution_controls
            SET desired_max_concurrency = $2, updated_at_ms = $3
            WHERE provider_account_id = $1
            "#,
        )
        .bind(provisioned.provider_account_id)
        .bind(max_concurrency)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_environments
              (provider_account_id, provider_id, environment_kind, environment_ref,
               upstream_identity_sha256, display_name, account_email, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'codex_home_v1', $3, $4, $5, $6, 'active', $7, $7)
            "#,
        )
        .bind(provisioned.provider_account_id)
        .bind(openai_codex::PROVIDER_ID)
        .bind(home.to_string_lossy().as_ref())
        .bind(&upstream_identity_sha256)
        .bind(&display_name)
        .bind(&account.email)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(map_identity_insert)?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_quota_snapshots
              (provider_account_id, provider_id, plan_type, status,
               observed_at_ms, last_error_code)
            VALUES ($1, $2, $3, 'unavailable', $4, 'quota_observer_pending')
            ON CONFLICT (provider_account_id) DO UPDATE
            SET plan_type = EXCLUDED.plan_type,
                credits_balance = NULL,
                credits_unlimited = NULL,
                status = 'unavailable',
                observed_at_ms = EXCLUDED.observed_at_ms,
                last_error_code = EXCLUDED.last_error_code
            "#,
        )
        .bind(provisioned.provider_account_id)
        .bind(openai_codex::PROVIDER_ID)
        .bind(&account.plan_type)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        insert_managed_account_route(
            &mut tx,
            provisioned.provider_account_id,
            provisioned.execution_profile_id,
            format!("account.{}", provisioned.provider_account_id.simple()),
            &display_name,
            now,
        )
        .await?;
        insert_managed_account_route(
            &mut tx,
            provisioned.provider_account_id,
            edit_provisioned.execution_profile_id,
            format!("account.{}.edits", provisioned.provider_account_id.simple()),
            &format!("{display_name} edits"),
            now,
        )
        .await?;
        sqlx::query(
            r#"
            UPDATE provider_account_login_sessions
            SET status = 'succeeded', provider_account_id = $2, updated_at_ms = $3,
                completed_at_ms = $3, error_code = NULL,
                authorization_url = NULL, user_code = NULL
            WHERE login_session_id = $1 AND status = 'validating'
            "#,
        )
        .bind(login_session_id)
        .bind(provisioned.provider_account_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        tx.commit().await.map_err(store_unavailable)?;
        Ok(provisioned.provider_account_id)
    }

    async fn observe_quota(
        &self,
        source_home: &Path,
        expected_auth_sha256: &str,
    ) -> Result<(CodexAccountSnapshot, CodexQuotaSnapshot), ImageGatewayError> {
        let observer_root = self.homes_root.join("observers");
        create_private_directory(&observer_root)?;
        let observer = tempfile::Builder::new()
            .prefix("codex-")
            .tempdir_in(&observer_root)
            .map_err(|_| ImageGatewayError::service_unavailable("quota observer unavailable"))?;
        fs::set_permissions(observer.path(), fs::Permissions::from_mode(0o700))
            .map_err(|_| ImageGatewayError::service_unavailable("quota observer unavailable"))?;
        prepare_codex_auth_copy(observer.path(), source_home, expected_auth_sha256)?;
        let mut server = timeout(
            APP_SERVER_START_TIMEOUT,
            CodexAppServer::spawn(&self.codex_executable, observer.path()),
        )
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("Codex quota observer timed out"))?
        .map_err(|error| {
            tracing::warn!(
                error = ?error,
                "Codex quota app-server failed to start"
            );
            ImageGatewayError::service_unavailable("Codex quota observer unavailable")
        })?;
        let account = timeout(APP_SERVER_READ_TIMEOUT, server.account())
            .await
            .map_err(|_| {
                ImageGatewayError::service_unavailable("Codex account observation timed out")
            })?
            .map_err(|error| {
                tracing::warn!(
                    error = ?error,
                    "Codex account observation request failed"
                );
                ImageGatewayError::service_unavailable("Codex account observation unavailable")
            })?;
        let quota = timeout(CODEX_QUOTA_READ_TIMEOUT, server.quota())
            .await
            .map_err(|_| {
                ImageGatewayError::service_unavailable("Codex quota observation timed out")
            })?
            .map_err(|error| {
                tracing::warn!(
                    error = ?error,
                    "Codex quota observation request failed"
                );
                ImageGatewayError::service_unavailable("Codex quota observation unavailable")
            })?;
        server.shutdown().await;
        Ok((account, quota))
    }

    async fn refresh_grok_quota(&self, provider_account_id: Uuid) -> Result<(), ImageGatewayError> {
        let _refresh_guard = GrokQuotaRefreshGuard::acquire(provider_account_id)?;
        self.refresh_operational_credential(provider_account_id, false)
            .await?;
        let mut credential = self
            .credential_store
            .resolve(provider_account_id)
            .await
            .map_err(map_credential_store_error)?;
        if credential.provider_id != GROK_PROVIDER_ID {
            return Err(ImageGatewayError::not_found(
                "Managed Grok account not found",
                Some("provider_account_id".to_owned()),
                "provider_account_not_found",
            ));
        }
        let first =
            observe_grok_quota(credential.home(), &credential.material_fingerprint_sha256).await;
        let quota = match first {
            Err(error) if error.code() == "grok_quota_auth_expired" => {
                self.refresh_operational_credential(provider_account_id, true)
                    .await?;
                credential = self
                    .credential_store
                    .resolve(provider_account_id)
                    .await
                    .map_err(map_credential_store_error)?;
                observe_grok_quota(credential.home(), &credential.material_fingerprint_sha256)
                    .await
                    .map_err(|error| error.into_gateway_error())?
            }
            Ok(quota) => quota,
            Err(error) => {
                let now = now_ms()?;
                let error_code = error.code();
                let _ = sqlx::query(
                    r#"
                    INSERT INTO provider_account_quota_snapshots
                      (provider_account_id, provider_id, status, observed_at_ms, last_error_code)
                    VALUES ($1, $2, 'unavailable', $3, $4)
                    ON CONFLICT (provider_account_id) DO UPDATE
                    SET status = 'unavailable', observed_at_ms = EXCLUDED.observed_at_ms,
                        last_error_code = EXCLUDED.last_error_code
                    "#,
                )
                .bind(provider_account_id)
                .bind(GROK_PROVIDER_ID)
                .bind(now)
                .bind(error_code)
                .execute(&self.pool)
                .await;
                return Err(error.into_gateway_error());
            }
        };
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        let now = database_now(&mut tx).await?;
        persist_grok_quota(&mut tx, provider_account_id, &quota, now).await?;
        tx.commit().await.map_err(store_unavailable)?;
        Ok(())
    }

    async fn start_grok_login(
        &self,
        request: StartProviderLoginRequest,
    ) -> Result<ProviderLoginSession, ImageGatewayError> {
        validate_provider_login_request(&request, "Grok")?;
        let executable = self.grok_executable.clone().ok_or_else(|| {
            ImageGatewayError::service_unavailable("Managed Grok CLI is unavailable")
        })?;
        let login_session_id = Uuid::new_v4();
        let account_key = format!("grok-{}", login_session_id.simple());
        let home = self.create_login_home("grok", login_session_id)?;
        let now = now_ms()?;
        let expires_at_ms = now.saturating_add(LOGIN_TTL.as_millis() as i64);
        let inserted = sqlx::query(
            r#"
            INSERT INTO provider_account_login_sessions
              (login_session_id, provider_id, account_key, display_name,
               environment_ref, status, login_method, max_concurrency,
               provider_account_id, expires_at_ms, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, 'starting', $6, $7, $8, $9, $10, $10)
            "#,
        )
        .bind(login_session_id)
        .bind(GROK_PROVIDER_ID)
        .bind(&account_key)
        .bind(request.display_name.trim())
        .bind(home.to_string_lossy().as_ref())
        .bind(request.login_method.as_str())
        .bind(request.max_concurrency)
        .bind(request.provider_account_id)
        .bind(expires_at_ms)
        .bind(now)
        .execute(&self.pool)
        .await;
        if let Err(error) = inserted {
            let _ = fs::remove_dir_all(&home);
            return Err(map_login_session_insert(error));
        }
        let (process, challenge) =
            match GrokLoginProcess::start(executable.as_ref(), &home, request.login_method).await {
                Ok(started) => started,
                Err(error) => {
                    tracing::warn!(%login_session_id, error = ?error, "Grok login could not start");
                    self.set_login_failed(login_session_id, "grok_login_start_failed")
                        .await;
                    let _ = fs::remove_dir_all(&home);
                    return Err(ImageGatewayError::service_unavailable(
                        "Grok login could not be started",
                    ));
                }
            };
        let updated_at_ms = now_ms()?;
        sqlx::query(
            r#"
            UPDATE provider_account_login_sessions
            SET status = 'waiting_for_user', provider_login_id = $2,
                authorization_url = $3, user_code = $4, updated_at_ms = $5
            WHERE login_session_id = $1 AND status = 'starting'
            "#,
        )
        .bind(login_session_id)
        .bind(login_session_id.to_string())
        .bind(challenge.authorization_url)
        .bind(challenge.user_code)
        .bind(updated_at_ms)
        .execute(&self.pool)
        .await
        .map_err(store_unavailable)?;

        let service = self.clone();
        let display_name = request.display_name.trim().to_owned();
        tokio::spawn(async move {
            let completed = matches!(timeout(LOGIN_TTL, process.wait()).await, Ok(Ok(true)));
            if !completed {
                service
                    .set_login_failed(login_session_id, "grok_login_failed")
                    .await;
                let _ = fs::remove_dir_all(&home);
                return;
            }
            let validating_at = now_ms().unwrap_or(updated_at_ms);
            let transitioned = sqlx::query(
                r#"
                UPDATE provider_account_login_sessions
                SET status = 'validating', updated_at_ms = $2,
                    authorization_url = NULL, user_code = NULL
                WHERE login_session_id = $1 AND status = 'waiting_for_user'
                "#,
            )
            .bind(login_session_id)
            .bind(validating_at)
            .execute(&service.pool)
            .await
            .map(|result| result.rows_affected() == 1)
            .unwrap_or(false);
            if !transitioned {
                return;
            }
            match service
                .complete_grok_login(
                    login_session_id,
                    request.provider_account_id,
                    account_key,
                    display_name,
                    request.max_concurrency,
                    request.operation_ids.clone(),
                    home.clone(),
                )
                .await
            {
                Ok(provider_account_id) => {
                    if request.provider_account_id.is_some() {
                        let _ = fs::remove_dir_all(&home);
                        if let Err(error) = service.refresh_grok_quota(provider_account_id).await {
                            tracing::warn!(
                                %provider_account_id,
                                error = ?error,
                                "Grok quota observation after reauthorization unavailable"
                            );
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(%login_session_id, error = ?error, "Grok account provisioning failed");
                    let error_code = if request.provider_account_id.is_some() {
                        "provider_account_reauthorization_failed"
                    } else {
                        "provider_account_provisioning_failed"
                    };
                    service.set_login_failed(login_session_id, error_code).await;
                    let _ = fs::remove_dir_all(&home);
                }
            }
        });
        self.login_session(login_session_id).await
    }

    async fn complete_grok_login(
        &self,
        login_session_id: Uuid,
        reauthorize_provider_account_id: Option<Uuid>,
        account_key: String,
        display_name: String,
        max_concurrency: i32,
        operation_ids: Vec<String>,
        home: PathBuf,
    ) -> Result<Uuid, ImageGatewayError> {
        self.validate_grok_credentials(&home).await?;
        let credential_auth_sha256 = grok_auth_file_sha256(&home)?;
        let identity = grok_identity(&home)?;
        if let Some(provider_account_id) = reauthorize_provider_account_id {
            return self
                .complete_reauthorization(
                    login_session_id,
                    provider_account_id,
                    GROK_PROVIDER_ID,
                    &home,
                    &identity.upstream_identity_sha256,
                    identity.email.as_deref(),
                )
                .await;
        }
        let initial_quota = observe_grok_quota(&home, &credential_auth_sha256).await;
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "provider-account-identity:{GROK_PROVIDER_ID}:{}",
                identity.upstream_identity_sha256
            ))
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
        let duplicate: Option<Uuid> = sqlx::query_scalar(
            "SELECT provider_account_id FROM provider_account_environments WHERE provider_id = $1 AND upstream_identity_sha256 = $2",
        )
        .bind(GROK_PROVIDER_ID)
        .bind(&identity.upstream_identity_sha256)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        if duplicate.is_some() {
            return Err(ImageGatewayError::invalid_request(
                "This Grok account is already managed",
                Some("provider_id".to_owned()),
                "duplicate_provider_account",
            ));
        }
        let suffix = login_session_id.simple().to_string();
        let base = GrokExecutionProfileProvisioning {
            profile_key: format!("managed.grok.images.{suffix}"),
            credential_pool_key: "managed.grok".to_owned(),
            provider_account_key: account_key,
            credential_ref: format!("managed.grok.{suffix}.1"),
            credential_revision: 1,
            credential_auth_sha256,
            max_concurrency: 64,
        };
        let mut profiles = Vec::with_capacity(operation_ids.len());
        if operation_ids
            .iter()
            .any(|value| value == "images.generations")
        {
            let image = provision_grok_execution_profile_in_transaction(&mut tx, &base)
                .await
                .map_err(map_profile_provisioning)?;
            profiles.push(("images", image));
        }
        if operation_ids.iter().any(|value| value == "images.edits") {
            let mut edit_spec = base.clone();
            edit_spec.profile_key = format!("managed.grok.edits.{suffix}");
            let edit = provision_grok_edit_execution_profile_in_transaction(&mut tx, &edit_spec)
                .await
                .map_err(map_profile_provisioning)?;
            profiles.push(("edits", edit));
        }
        if operation_ids
            .iter()
            .any(|value| value == "videos.generations")
        {
            let mut video_spec = base.clone();
            video_spec.profile_key = format!("managed.grok.videos.{suffix}");
            let video = provision_grok_video_execution_profile_in_transaction(&mut tx, &video_spec)
                .await
                .map_err(map_profile_provisioning)?;
            profiles.push(("videos", video));
        }
        let provider_account_id = profiles
            .first()
            .map(|(_, profile)| profile.provider_account_id)
            .ok_or_else(|| ImageGatewayError::internal("Grok account has no selected operation"))?;
        if profiles
            .iter()
            .any(|(_, profile)| profile.provider_account_id != provider_account_id)
        {
            return Err(ImageGatewayError::internal(
                "Grok execution profiles resolved to different accounts",
            ));
        }
        let now = database_now(&mut tx).await?;
        sqlx::query(
            "UPDATE provider_account_execution_controls SET desired_max_concurrency = $2, updated_at_ms = $3 WHERE provider_account_id = $1",
        )
        .bind(provider_account_id)
        .bind(max_concurrency)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_environments
              (provider_account_id, provider_id, environment_kind, environment_ref,
               upstream_identity_sha256, display_name, account_email, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'grok_home_v1', $3, $4, $5, $6, 'active', $7, $7)
            "#,
        )
        .bind(provider_account_id)
        .bind(GROK_PROVIDER_ID)
        .bind(home.to_string_lossy().as_ref())
        .bind(&identity.upstream_identity_sha256)
        .bind(display_name.trim())
        .bind(&identity.email)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(map_identity_insert)?;
        match initial_quota {
            Ok(quota) => persist_grok_quota(&mut tx, provider_account_id, &quota, now).await?,
            Err(error) => {
                tracing::warn!(
                    %provider_account_id,
                    error_code = error.code(),
                    "initial Grok quota observation unavailable"
                );
                sqlx::query(
                    r#"
                    INSERT INTO provider_account_quota_snapshots
                      (provider_account_id, provider_id, status, observed_at_ms, last_error_code)
                    VALUES ($1, $2, 'unavailable', $3, $4)
                    "#,
                )
                .bind(provider_account_id)
                .bind(GROK_PROVIDER_ID)
                .bind(now)
                .bind(error.code())
                .execute(&mut *tx)
                .await
                .map_err(store_unavailable)?;
            }
        }
        for (route_suffix, profile) in &profiles {
            insert_account_route(
                &mut tx,
                profile.execution_profile_id,
                &display_name,
                route_suffix,
                now,
            )
            .await?;
        }
        let updated = sqlx::query(
            r#"
            UPDATE provider_account_login_sessions
            SET status = 'succeeded', provider_account_id = $2, updated_at_ms = $3,
                completed_at_ms = $3, error_code = NULL,
                authorization_url = NULL, user_code = NULL
            WHERE login_session_id = $1 AND status = 'validating'
            "#,
        )
        .bind(login_session_id)
        .bind(provider_account_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(ImageGatewayError::service_unavailable(
                "Grok login session changed during provisioning",
            ));
        }
        tx.commit().await.map_err(store_unavailable)?;
        Ok(provider_account_id)
    }

    async fn validate_grok_credentials(&self, home: &Path) -> Result<(), ImageGatewayError> {
        let executable = self.grok_executable.as_ref().ok_or_else(|| {
            ImageGatewayError::service_unavailable("Managed Grok CLI is unavailable")
        })?;
        let mut command = tokio::process::Command::new(executable.as_ref());
        command
            .arg("models")
            .env_clear()
            .env("HOME", home)
            .env("GROK_HOME", home)
            .env("GROK_DISABLE_AUTOUPDATER", "1")
            .env("NO_COLOR", "1")
            .env("TERM", "dumb")
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        copy_proxy_environment(&mut command);
        let valid = matches!(
            timeout(APP_SERVER_READ_TIMEOUT, command.status()).await,
            Ok(Ok(status)) if status.success()
        );
        if !valid {
            return Err(ImageGatewayError::service_unavailable(
                "Grok account authentication validation failed",
            ));
        }
        Ok(())
    }

    async fn refresh_dreamina_quota(
        &self,
        provider_account_id: Uuid,
    ) -> Result<(), ImageGatewayError> {
        let _refresh_guard = DreaminaQuotaRefreshGuard::acquire(provider_account_id)?;
        self.refresh_operational_credential(provider_account_id, true)
            .await
    }

    async fn start_dreamina_login(
        &self,
        request: StartProviderLoginRequest,
    ) -> Result<ProviderLoginSession, ImageGatewayError> {
        validate_provider_login_request(&request, "Dreamina")?;
        if request.login_method != CodexLoginMethod::DeviceCode {
            return Err(ImageGatewayError::invalid_request(
                "Dreamina supports OAuth device-code login",
                Some("login_method".to_owned()),
                "unsupported_provider_login_method",
            ));
        }
        let executable = self.dreamina_executable.clone().ok_or_else(|| {
            ImageGatewayError::service_unavailable("Managed Dreamina CLI is unavailable")
        })?;
        if !dreamina_account_isolation_available() {
            return Err(ImageGatewayError::service_unavailable(
                "Dreamina account isolation is unavailable on this node",
            ));
        }
        let login_session_id = Uuid::new_v4();
        let account_key = format!("dreamina-{}", login_session_id.simple());
        let home = self.create_login_home("dreamina", login_session_id)?;
        let now = now_ms()?;
        let expires_at_ms = now.saturating_add(LOGIN_TTL.as_millis() as i64);
        let inserted = sqlx::query(
            r#"
            INSERT INTO provider_account_login_sessions
              (login_session_id, provider_id, account_key, display_name,
               environment_ref, status, login_method, max_concurrency,
               provider_account_id, expires_at_ms, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, 'starting', $6, $7, NULL, $8, $9, $9)
            "#,
        )
        .bind(login_session_id)
        .bind(DREAMINA_PROVIDER_ID)
        .bind(&account_key)
        .bind(request.display_name.trim())
        .bind(home.to_string_lossy().as_ref())
        .bind(request.login_method.as_str())
        .bind(request.max_concurrency)
        .bind(expires_at_ms)
        .bind(now)
        .execute(&self.pool)
        .await;
        if let Err(error) = inserted {
            let _ = fs::remove_dir_all(&home);
            return Err(map_login_session_insert(error));
        }
        let (process, challenge) = match DreaminaLoginProcess::start(
            executable.as_ref(),
            &home,
            request.login_method,
        )
        .await
        {
            Ok(started) => started,
            Err(error) => {
                tracing::warn!(%login_session_id, error = ?error, "Dreamina login could not start");
                self.set_login_failed(login_session_id, "dreamina_login_start_failed")
                    .await;
                let _ = fs::remove_dir_all(&home);
                return Err(ImageGatewayError::service_unavailable(
                    "Dreamina login could not be started",
                ));
            }
        };
        let updated_at_ms = now_ms()?;
        sqlx::query(
            r#"
            UPDATE provider_account_login_sessions
            SET status = 'waiting_for_user', provider_login_id = $2,
                authorization_url = $3, user_code = $4, updated_at_ms = $5
            WHERE login_session_id = $1 AND status = 'starting'
            "#,
        )
        .bind(login_session_id)
        .bind(login_session_id.to_string())
        .bind(challenge.authorization_url)
        .bind(challenge.user_code)
        .bind(updated_at_ms)
        .execute(&self.pool)
        .await
        .map_err(store_unavailable)?;

        let service = self.clone();
        let display_name = request.display_name.trim().to_owned();
        tokio::spawn(async move {
            let account = match timeout(LOGIN_TTL, process.wait()).await {
                Ok(Ok(Some(account))) => account,
                _ => {
                    service
                        .set_login_failed(login_session_id, "dreamina_login_failed")
                        .await;
                    let _ = fs::remove_dir_all(&home);
                    return;
                }
            };
            let validating_at = now_ms().unwrap_or(updated_at_ms);
            let transitioned = sqlx::query(
                r#"
                UPDATE provider_account_login_sessions
                SET status = 'validating', updated_at_ms = $2,
                    authorization_url = NULL, user_code = NULL
                WHERE login_session_id = $1 AND status = 'waiting_for_user'
                "#,
            )
            .bind(login_session_id)
            .bind(validating_at)
            .execute(&service.pool)
            .await
            .map(|result| result.rows_affected() == 1)
            .unwrap_or(false);
            if !transitioned {
                return;
            }
            match service
                .complete_dreamina_login(
                    login_session_id,
                    request.provider_account_id,
                    DreaminaLoginCompletion {
                        account_key,
                        display_name,
                        max_concurrency: request.max_concurrency,
                        operation_ids: request.operation_ids.clone(),
                        home: home.clone(),
                        account,
                    },
                )
                .await
            {
                Ok(_) => {
                    if request.provider_account_id.is_some() {
                        let _ = fs::remove_dir_all(&home);
                    }
                }
                Err(error) => {
                    tracing::warn!(%login_session_id, error = ?error, "Dreamina account provisioning failed");
                    service
                        .set_login_failed(login_session_id, "provider_account_provisioning_failed")
                        .await;
                    let _ = fs::remove_dir_all(&home);
                }
            }
        });
        self.login_session(login_session_id).await
    }

    async fn complete_dreamina_reauthorization(
        &self,
        login_session_id: Uuid,
        provider_account_id: Uuid,
        fresh_home: &Path,
        observed_identity_sha256: &str,
        account: &DreaminaAccountSnapshot,
    ) -> Result<Uuid, ImageGatewayError> {
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "provider-account-reauthorization:{provider_account_id}"
            ))
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
        let target = sqlx::query_as::<_, LockedReauthorizationTargetRow>(
            r#"
            SELECT account.provider_id, environment.environment_ref,
                   environment.upstream_identity_sha256, head.active_revision,
                   revision.material_fingerprint_sha256, revision.access_expires_at_ms
            FROM provider_accounts account
            JOIN provider_account_environments environment
              ON environment.provider_account_id = account.provider_account_id
             AND environment.provider_id = account.provider_id
            JOIN provider_account_credential_heads head
              ON head.provider_account_id = account.provider_account_id
            JOIN provider_account_credential_revisions revision
              ON revision.provider_account_id = head.provider_account_id
             AND revision.revision = head.active_revision
            WHERE account.provider_account_id = $1
              AND account.state = 'enabled' AND environment.state IN ('active', 'invalid')
            FOR UPDATE OF account, environment, head
            "#,
        )
        .bind(provider_account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Managed Dreamina account not found",
                Some("provider_account_id".to_owned()),
                "provider_account_not_found",
            )
        })?;
        if target.provider_id != DREAMINA_PROVIDER_ID {
            return Err(ImageGatewayError::invalid_request(
                "Provider account does not match the login provider",
                Some("provider_account_id".to_owned()),
                "provider_account_provider_mismatch",
            ));
        }
        if target.upstream_identity_sha256 != observed_identity_sha256 {
            return Err(ImageGatewayError::invalid_request(
                "The authorized upstream identity does not match this managed account",
                Some("provider_account_id".to_owned()),
                "provider_account_identity_mismatch",
            ));
        }
        let destination_home = PathBuf::from(&target.environment_ref);
        let current_fingerprint =
            dreamina_credential_fingerprint(&destination_home).map_err(|_| {
                ImageGatewayError::service_unavailable("Dreamina credential environment is invalid")
            })?;
        if current_fingerprint != target.material_fingerprint_sha256 {
            return Err(ImageGatewayError::service_unavailable(
                "Dreamina credential environment changed during reauthorization",
            ));
        }
        let replacement =
            DreaminaKeychainReplacement::install(fresh_home, &destination_home, login_session_id)
                .await
                .map_err(|_| {
                    ImageGatewayError::service_unavailable(
                        "Dreamina credential update is unavailable",
                    )
                })?;
        let result = async {
            let now = database_now(&mut tx).await?;
            let next_refresh_at_ms = now.saturating_add(CREDENTIAL_REFRESH_INTERVAL_MS);
            let updated_head = sqlx::query(
                r#"
                UPDATE provider_account_credential_heads
                SET lifecycle_state = 'active', refresh_strategy = 'cli_managed',
                    refresh_after_ms = $2, next_refresh_at_ms = $2,
                    last_attempt_at_ms = $3, last_success_at_ms = $3,
                    consecutive_failures = 0, last_error_code = NULL,
                    lease_owner = NULL, lease_expires_at_ms = NULL,
                    updated_at_ms = $3, control_version = control_version + 1
                WHERE provider_account_id = $1 AND active_revision = $4
                "#,
            )
            .bind(provider_account_id)
            .bind(next_refresh_at_ms)
            .bind(now)
            .bind(target.active_revision)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            if updated_head.rows_affected() != 1 {
                return Err(ImageGatewayError::conflict(
                    "Dreamina credential changed during reauthorization",
                    Some("provider_account_id".to_owned()),
                    "provider_reauthorization_conflict",
                ));
            }
            sqlx::query(
                r#"
                UPDATE provider_account_environments
                SET state = $2, updated_at_ms = $3
                WHERE provider_account_id = $1 AND provider_id = $4
                "#,
            )
            .bind(provider_account_id)
            .bind(dreamina_environment_state(account.cli_permission))
            .bind(now)
            .bind(DREAMINA_PROVIDER_ID)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            persist_dreamina_quota(&mut tx, provider_account_id, account, now).await?;
            sqlx::query(
                r#"
                INSERT INTO provider_account_credential_events
                  (credential_event_id, provider_account_id, event_type, from_revision,
                   to_revision, lease_epoch, executor_execution_id, error_code, created_at_ms)
                VALUES ($1, $2, 'reauth_succeeded', $3, $3, NULL, NULL, NULL, $4)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(provider_account_id)
            .bind(target.active_revision)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            let updated_session = sqlx::query(
                r#"
                UPDATE provider_account_login_sessions
                SET status = 'succeeded', updated_at_ms = $3, completed_at_ms = $3,
                    error_code = NULL, authorization_url = NULL, user_code = NULL
                WHERE login_session_id = $1 AND provider_account_id = $2
                  AND status = 'validating'
                "#,
            )
            .bind(login_session_id)
            .bind(provider_account_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            if updated_session.rows_affected() != 1 {
                return Err(ImageGatewayError::conflict(
                    "Provider reauthorization session changed during completion",
                    Some("login_session_id".to_owned()),
                    "provider_reauthorization_session_conflict",
                ));
            }
            tx.commit().await.map_err(store_unavailable)
        }
        .await;
        match result {
            Ok(()) => Ok(provider_account_id),
            Err(error) => {
                if let Err(rollback_error) = replacement.rollback() {
                    tracing::error!(
                        %provider_account_id,
                        error = ?rollback_error,
                        "Dreamina keychain rollback failed"
                    );
                }
                Err(error)
            }
        }
    }

    async fn complete_dreamina_login(
        &self,
        login_session_id: Uuid,
        reauthorize_provider_account_id: Option<Uuid>,
        completion: DreaminaLoginCompletion,
    ) -> Result<Uuid, ImageGatewayError> {
        let DreaminaLoginCompletion {
            account_key,
            display_name,
            max_concurrency,
            operation_ids,
            home,
            account,
        } = completion;
        let fingerprint = dreamina_credential_fingerprint(&home).map_err(|_| {
            ImageGatewayError::service_unavailable("Dreamina credential environment is invalid")
        })?;
        let identity_sha256 = dreamina_identity_sha256(&account);
        if let Some(provider_account_id) = reauthorize_provider_account_id {
            return self
                .complete_dreamina_reauthorization(
                    login_session_id,
                    provider_account_id,
                    &home,
                    &identity_sha256,
                    &account,
                )
                .await;
        }
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "provider-account-identity:{DREAMINA_PROVIDER_ID}:{identity_sha256}"
            ))
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
        let duplicate: Option<Uuid> = sqlx::query_scalar(
            "SELECT provider_account_id FROM provider_account_environments WHERE provider_id = $1 AND upstream_identity_sha256 = $2",
        )
        .bind(DREAMINA_PROVIDER_ID)
        .bind(&identity_sha256)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        if duplicate.is_some() {
            return Err(ImageGatewayError::invalid_request(
                "This Dreamina account is already managed",
                Some("provider_id".to_owned()),
                "duplicate_provider_account",
            ));
        }
        let suffix = login_session_id.simple().to_string();
        let base = DreaminaExecutionProfileProvisioning {
            profile_key: format!("managed.dreamina.images.{suffix}"),
            credential_pool_key: "managed.dreamina".to_owned(),
            provider_account_key: account_key,
            credential_ref: format!("managed.dreamina.{suffix}.1"),
            credential_revision: 1,
            credential_auth_sha256: fingerprint,
            max_concurrency: 64,
        };
        let mut profiles = Vec::with_capacity(operation_ids.len());
        if operation_ids
            .iter()
            .any(|value| value == "images.generations")
        {
            let image = provision_dreamina_execution_profile_in_transaction(&mut tx, &base)
                .await
                .map_err(map_profile_provisioning)?;
            profiles.push(("images", image));
        }
        if operation_ids
            .iter()
            .any(|value| value == "videos.generations")
        {
            let mut video_spec = base.clone();
            video_spec.profile_key = format!("managed.dreamina.videos.{suffix}");
            let video =
                provision_dreamina_video_execution_profile_in_transaction(&mut tx, &video_spec)
                    .await
                    .map_err(map_profile_provisioning)?;
            profiles.push(("videos", video));
        }
        let provider_account_id = profiles
            .first()
            .map(|(_, profile)| profile.provider_account_id)
            .ok_or_else(|| {
                ImageGatewayError::internal("Dreamina account has no selected operation")
            })?;
        if profiles
            .iter()
            .any(|(_, profile)| profile.provider_account_id != provider_account_id)
        {
            return Err(ImageGatewayError::internal(
                "Dreamina execution profiles resolved to different accounts",
            ));
        }
        let now = database_now(&mut tx).await?;
        sqlx::query(
            "UPDATE provider_account_execution_controls SET desired_max_concurrency = $2, updated_at_ms = $3 WHERE provider_account_id = $1",
        )
        .bind(provider_account_id)
        .bind(max_concurrency)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_environments
              (provider_account_id, provider_id, environment_kind, environment_ref,
               upstream_identity_sha256, display_name, account_email, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'dreamina_home_v1', $3, $4, $5, NULL, $6, $7, $7)
            "#,
        )
        .bind(provider_account_id)
        .bind(DREAMINA_PROVIDER_ID)
        .bind(home.to_string_lossy().as_ref())
        .bind(identity_sha256)
        .bind(display_name.trim())
        .bind(dreamina_environment_state(account.cli_permission))
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(map_identity_insert)?;
        persist_dreamina_quota(&mut tx, provider_account_id, &account, now).await?;
        let next_refresh_at_ms = now.saturating_add(CREDENTIAL_REFRESH_INTERVAL_MS);
        sqlx::query(
            r#"
            UPDATE provider_account_credential_heads
            SET lifecycle_state = 'active', refresh_after_ms = $2,
                next_refresh_at_ms = $2, last_attempt_at_ms = $3,
                last_success_at_ms = $3, consecutive_failures = 0,
                last_error_code = NULL, updated_at_ms = $3,
                control_version = control_version + 1
            WHERE provider_account_id = $1 AND refresh_strategy = 'cli_managed'
            "#,
        )
        .bind(provider_account_id)
        .bind(next_refresh_at_ms)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        for (route_suffix, profile) in &profiles {
            insert_account_route(
                &mut tx,
                profile.execution_profile_id,
                &display_name,
                route_suffix,
                now,
            )
            .await?;
        }
        let updated = sqlx::query(
            r#"
            UPDATE provider_account_login_sessions
            SET status = 'succeeded', provider_account_id = $2, updated_at_ms = $3,
                completed_at_ms = $3, error_code = NULL,
                authorization_url = NULL, user_code = NULL
            WHERE login_session_id = $1 AND status = 'validating'
            "#,
        )
        .bind(login_session_id)
        .bind(provider_account_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(ImageGatewayError::service_unavailable(
                "Dreamina login session changed during provisioning",
            ));
        }
        tx.commit().await.map_err(store_unavailable)?;
        Ok(provider_account_id)
    }
}

#[async_trait]
impl ProviderManagementService for PostgresProviderManagementService {
    async fn managed_cli_providers(
        &self,
    ) -> Result<ManagedCliProvidersSnapshot, ImageGatewayError> {
        Ok(ManagedCliProvidersSnapshot {
            providers: vec![
                ManagedCliProviderCapability {
                    provider_id: openai_codex::PROVIDER_ID.to_owned(),
                    display_name: "Codex".to_owned(),
                    availability: "available".to_owned(),
                    unavailable_reason: None,
                    login_methods: vec![
                        CodexLoginMethod::BrowserOauth,
                        CodexLoginMethod::DeviceCode,
                    ],
                    operation_ids: vec!["images.generations".to_owned(), "images.edits".to_owned()],
                    quota_kind: "rate_limit_windows".to_owned(),
                    executable_version: executable_version(&self.codex_executable).await,
                    max_concurrency_limit: 64,
                },
                ManagedCliProviderCapability {
                    provider_id: GROK_PROVIDER_ID.to_owned(),
                    display_name: "Grok".to_owned(),
                    availability: if self.grok_executable.is_some() {
                        "available"
                    } else {
                        "unavailable"
                    }
                    .to_owned(),
                    unavailable_reason: self
                        .grok_executable
                        .is_none()
                        .then(|| "Grok CLI 未安装或未配置".to_owned()),
                    login_methods: vec![
                        CodexLoginMethod::BrowserOauth,
                        CodexLoginMethod::DeviceCode,
                    ],
                    operation_ids: vec![
                        "images.generations".to_owned(),
                        "videos.generations".to_owned(),
                    ],
                    quota_kind: "weekly_usage".to_owned(),
                    executable_version: match &self.grok_executable {
                        Some(path) => executable_version(path).await,
                        None => None,
                    },
                    max_concurrency_limit: 64,
                },
                ManagedCliProviderCapability {
                    provider_id: DREAMINA_PROVIDER_ID.to_owned(),
                    display_name: "即梦".to_owned(),
                    availability: if self.dreamina_executable.is_some()
                        && dreamina_account_isolation_available()
                    {
                        "available"
                    } else {
                        "unavailable"
                    }
                    .to_owned(),
                    unavailable_reason: if self.dreamina_executable.is_none() {
                        Some("即梦 CLI 未安装或未配置".to_owned())
                    } else if !dreamina_account_isolation_available() {
                        Some("当前节点尚未配置可隔离的系统凭据环境".to_owned())
                    } else {
                        None
                    },
                    login_methods: vec![CodexLoginMethod::DeviceCode],
                    operation_ids: vec![
                        "images.generations".to_owned(),
                        "videos.generations".to_owned(),
                    ],
                    quota_kind: "credits".to_owned(),
                    executable_version: match &self.dreamina_executable {
                        Some(path) => executable_version(path).await,
                        None => None,
                    },
                    max_concurrency_limit: 64,
                },
            ],
        })
    }

    async fn provider_models(&self) -> Result<ProviderModelsSnapshot, ImageGatewayError> {
        model_catalog::list_models(&self.pool).await
    }

    async fn start_provider_model_refresh(
        &self,
        provider_account_id: Uuid,
    ) -> Result<ProviderModelRefreshView, ImageGatewayError> {
        model_catalog::start_refresh(
            self.pool.clone(),
            ProviderModelExecutables {
                codex: self.codex_executable.clone(),
                grok: self.grok_executable.clone(),
                dreamina: self.dreamina_executable.clone(),
            },
            provider_account_id,
        )
        .await
    }

    async fn provider_model_refresh(
        &self,
        refresh_id: Uuid,
    ) -> Result<ProviderModelRefreshView, ImageGatewayError> {
        model_catalog::refresh(&self.pool, refresh_id).await
    }

    async fn provider_account_models(
        &self,
        provider_account_id: Uuid,
    ) -> Result<ProviderAccountModelsView, ImageGatewayError> {
        model_catalog::account_models(&self.pool, provider_account_id).await
    }

    async fn update_provider_account_models(
        &self,
        provider_account_id: Uuid,
        request: UpdateProviderAccountModelsRequest,
    ) -> Result<ProviderAccountModelsView, ImageGatewayError> {
        model_catalog::update_account_models(&self.pool, provider_account_id, request).await
    }

    async fn update_provider_account_model_configuration(
        &self,
        provider_account_id: Uuid,
        request: UpdateProviderAccountModelConfigurationRequest,
    ) -> Result<ProviderAccountModelConfigurationView, ImageGatewayError> {
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        let model_version = model_catalog::update_account_models_in_transaction(
            &mut tx,
            provider_account_id,
            &UpdateProviderAccountModelsRequest {
                expected_version: request.expected_model_version,
                mode: request.mode,
                enabled_models: request.enabled_models,
            },
        )
        .await?;
        let route_revision = update_account_route_model_mappings_in_transaction(
            &mut tx,
            provider_account_id,
            request.route_id,
            request.expected_route_revision,
            &request.model_mappings,
        )
        .await?;
        tx.commit().await.map_err(store_unavailable)?;
        Ok(ProviderAccountModelConfigurationView {
            provider_account_id,
            model_version,
            route_id: request.route_id,
            route_revision,
        })
    }

    async fn start_provider_login(
        &self,
        mut request: StartProviderLoginRequest,
    ) -> Result<ProviderLoginSession, ImageGatewayError> {
        normalize_provider_login_operations(&mut request)?;
        if let Some(provider_account_id) = request.provider_account_id {
            let target = self.reauthorization_target(provider_account_id).await?;
            if target.provider_id != request.provider_id {
                return Err(ImageGatewayError::invalid_request(
                    "Provider account does not match the selected provider",
                    Some("provider_account_id".to_owned()),
                    "provider_account_provider_mismatch",
                ));
            }
            request.display_name = target.display_name;
            request.max_concurrency = target.max_concurrency;
        }
        match request.provider_id.as_str() {
            openai_codex::PROVIDER_ID => {
                self.start_codex_login(StartCodexLoginRequest {
                    display_name: request.display_name,
                    provider_account_id: request.provider_account_id,
                    login_method: request.login_method,
                    max_concurrency: request.max_concurrency,
                })
                .await
            }
            GROK_PROVIDER_ID => self.start_grok_login(request).await,
            DREAMINA_PROVIDER_ID => self.start_dreamina_login(request).await,
            _ => Err(ImageGatewayError::invalid_request(
                "Managed CLI provider is unsupported",
                Some("provider_id".to_owned()),
                "unsupported_managed_cli_provider",
            )),
        }
    }

    async fn start_provider_reauthorization(
        &self,
        provider_account_id: Uuid,
        request: StartProviderReauthorizationRequest,
    ) -> Result<ProviderLoginSession, ImageGatewayError> {
        let target = self.reauthorization_target(provider_account_id).await?;
        if !matches!(
            target.provider_id.as_str(),
            openai_codex::PROVIDER_ID | GROK_PROVIDER_ID | DREAMINA_PROVIDER_ID
        ) {
            return Err(ImageGatewayError::service_unavailable(
                "This provider does not support managed reauthorization",
            ));
        }
        self.start_provider_login(StartProviderLoginRequest {
            provider_id: target.provider_id,
            display_name: target.display_name,
            operation_ids: Vec::new(),
            provider_account_id: Some(provider_account_id),
            login_method: request.login_method,
            max_concurrency: target.max_concurrency,
        })
        .await
    }

    async fn start_codex_login(
        &self,
        request: StartCodexLoginRequest,
    ) -> Result<ProviderLoginSession, ImageGatewayError> {
        validate_login_request(&request)?;
        let login_session_id = Uuid::new_v4();
        let account_key = managed_codex_account_key(login_session_id);
        let home = self.create_login_home("codex", login_session_id)?;
        let now = now_ms()?;
        let expires_at_ms = now.saturating_add(LOGIN_TTL.as_millis() as i64);
        let inserted = sqlx::query(
            r#"
            INSERT INTO provider_account_login_sessions
              (login_session_id, provider_id, account_key, display_name,
               environment_ref, status, login_method, max_concurrency,
               provider_account_id, expires_at_ms, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, 'starting', $6, $7, $8, $9, $10, $10)
            "#,
        )
        .bind(login_session_id)
        .bind(openai_codex::PROVIDER_ID)
        .bind(&account_key)
        .bind(&request.display_name)
        .bind(home.to_string_lossy().as_ref())
        .bind(request.login_method.as_str())
        .bind(request.max_concurrency)
        .bind(request.provider_account_id)
        .bind(expires_at_ms)
        .bind(now)
        .execute(&self.pool)
        .await;
        if let Err(error) = inserted {
            let _ = fs::remove_dir_all(&home);
            return Err(map_login_session_insert(error));
        }
        let mut server = match timeout(
            APP_SERVER_START_TIMEOUT,
            CodexAppServer::spawn(&self.codex_executable, &home),
        )
        .await
        {
            Ok(Ok(server)) => server,
            _ => {
                self.set_login_failed(login_session_id, "codex_app_server_unavailable")
                    .await;
                let _ = fs::remove_dir_all(&home);
                return Err(ImageGatewayError::service_unavailable(
                    "Codex login service unavailable",
                ));
            }
        };
        let challenge = match timeout(
            APP_SERVER_READ_TIMEOUT,
            server.start_login(request.login_method),
        )
        .await
        {
            Ok(Ok(challenge)) => challenge,
            _ => {
                server.shutdown().await;
                self.set_login_failed(login_session_id, "codex_login_start_failed")
                    .await;
                let _ = fs::remove_dir_all(&home);
                return Err(ImageGatewayError::service_unavailable(
                    "Codex login could not be started",
                ));
            }
        };
        let updated_at_ms = now_ms()?;
        sqlx::query(
            r#"
            UPDATE provider_account_login_sessions
            SET status = 'waiting_for_user', provider_login_id = $2,
                authorization_url = $3, user_code = $4, updated_at_ms = $5
            WHERE login_session_id = $1 AND status = 'starting'
            "#,
        )
        .bind(login_session_id)
        .bind(&challenge.provider_login_id)
        .bind(&challenge.authorization_url)
        .bind(&challenge.user_code)
        .bind(updated_at_ms)
        .execute(&self.pool)
        .await
        .map_err(store_unavailable)?;

        let service = self.clone();
        let provider_login_id = challenge.provider_login_id.clone();
        let display_name = request.display_name.clone();
        tokio::spawn(async move {
            let result = timeout(LOGIN_TTL, server.wait_for_login(&provider_login_id)).await;
            if !matches!(result, Ok(Ok(()))) {
                server.shutdown().await;
                service
                    .set_login_failed(login_session_id, "codex_login_failed")
                    .await;
                let _ = fs::remove_dir_all(&home);
                return;
            }
            let validating_at = now_ms().unwrap_or(updated_at_ms);
            if sqlx::query(
                r#"
                UPDATE provider_account_login_sessions
                SET status = 'validating', updated_at_ms = $2,
                    authorization_url = NULL, user_code = NULL
                WHERE login_session_id = $1 AND status = 'waiting_for_user'
                "#,
            )
            .bind(login_session_id)
            .bind(validating_at)
            .execute(&service.pool)
            .await
            .is_err()
            {
                server.shutdown().await;
                return;
            }
            let account = timeout(
                APP_SERVER_READ_TIMEOUT,
                server.wait_for_account(CODEX_LOGIN_ACCOUNT_RETRY_DELAY),
            )
            .await;
            server.shutdown().await;
            let account = match account {
                Ok(Ok(account)) => account,
                Ok(Err(error)) => {
                    tracing::warn!(
                        %login_session_id,
                        error = ?error,
                        "Codex account validation failed after login completion"
                    );
                    service
                        .set_login_failed(login_session_id, "codex_account_validation_failed")
                        .await;
                    let _ = fs::remove_dir_all(&home);
                    return;
                }
                Err(_) => {
                    tracing::warn!(
                        %login_session_id,
                        "Codex account was not readable before the validation timeout"
                    );
                    service
                        .set_login_failed(login_session_id, "codex_account_validation_timeout")
                        .await;
                    let _ = fs::remove_dir_all(&home);
                    return;
                }
            };
            match service
                .complete_login(
                    login_session_id,
                    request.provider_account_id,
                    account_key,
                    display_name,
                    request.max_concurrency,
                    home.clone(),
                    account,
                )
                .await
            {
                Ok(provider_account_id) => {
                    if request.provider_account_id.is_some() {
                        let _ = fs::remove_dir_all(&home);
                    }
                    if let Err(error) = service.refresh_codex_quota(provider_account_id).await {
                        tracing::warn!(
                            %provider_account_id,
                            error = ?error,
                            "initial Codex quota observation unavailable"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        %login_session_id,
                        error = ?error,
                        "Codex provider account provisioning failed"
                    );
                    let error_code = if request.provider_account_id.is_some() {
                        "provider_account_reauthorization_failed"
                    } else {
                        "provider_account_provisioning_failed"
                    };
                    service.set_login_failed(login_session_id, error_code).await;
                    let _ = fs::remove_dir_all(&home);
                }
            }
        });
        self.login_session(login_session_id).await
    }

    async fn login_session(
        &self,
        login_session_id: Uuid,
    ) -> Result<ProviderLoginSession, ImageGatewayError> {
        let row = sqlx::query_as::<_, LoginSessionRow>(
            r#"
            SELECT login_session_id, provider_id, account_key, display_name,
                   status, login_method, authorization_url, user_code, provider_account_id,
                   error_code, expires_at_ms, created_at_ms, updated_at_ms
            FROM provider_account_login_sessions
            WHERE login_session_id = $1
            "#,
        )
        .bind(login_session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Provider login session not found",
                Some("login_session_id".to_string()),
                "provider_login_session_not_found",
            )
        })?;
        provider_login_session_from_row(row)
    }

    async fn refresh_codex_quota(
        &self,
        provider_account_id: Uuid,
    ) -> Result<(), ImageGatewayError> {
        let _refresh_guard = CodexQuotaRefreshGuard::acquire(provider_account_id)?;
        self.refresh_operational_credential(provider_account_id, false)
            .await?;
        let mut credential = self
            .credential_store
            .resolve(provider_account_id)
            .await
            .map_err(map_credential_store_error)?;
        if credential.provider_id != openai_codex::PROVIDER_ID {
            return Err(ImageGatewayError::not_found(
                "Managed Codex account not found",
                Some("provider_account_id".to_string()),
                "provider_account_not_found",
            ));
        }
        let first = self
            .observe_quota(credential.home(), &credential.material_fingerprint_sha256)
            .await;
        let (account, quota) = match first {
            Ok(observation) => observation,
            Err(first_error) => {
                tracing::warn!(
                    %provider_account_id,
                    error = ?first_error,
                    "Codex quota observation failed; verifying the account credential"
                );
                if let Err(error) = self
                    .refresh_operational_credential(provider_account_id, true)
                    .await
                {
                    let _ = mark_codex_quota_unavailable(
                        &self.pool,
                        provider_account_id,
                        "quota_credential_verification_failed",
                    )
                    .await;
                    return Err(error);
                }
                credential = self
                    .credential_store
                    .resolve(provider_account_id)
                    .await
                    .map_err(map_credential_store_error)?;
                tokio::time::sleep(CODEX_QUOTA_RETRY_DELAY).await;
                match self
                    .observe_quota(credential.home(), &credential.material_fingerprint_sha256)
                    .await
                {
                    Ok(observation) => observation,
                    Err(error) => {
                        tracing::warn!(
                            %provider_account_id,
                            error = ?error,
                            "Codex quota observation unavailable after retry"
                        );
                        let _ = mark_codex_quota_unavailable(
                            &self.pool,
                            provider_account_id,
                            "quota_observer_failed",
                        )
                        .await;
                        return Err(error);
                    }
                }
            }
        };
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        let now = database_now(&mut tx).await?;
        sqlx::query(
            "UPDATE provider_account_environments SET account_email = COALESCE($2, account_email), updated_at_ms = $3 WHERE provider_account_id = $1",
        )
        .bind(provider_account_id)
        .bind(account.email)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        persist_quota(&mut tx, provider_account_id, &quota, now).await?;
        tx.commit().await.map_err(store_unavailable)?;
        Ok(())
    }

    async fn refresh_provider_quota(
        &self,
        provider_account_id: Uuid,
    ) -> Result<(), ImageGatewayError> {
        let provider_id: Option<String> = sqlx::query_scalar(
            "SELECT provider_id FROM provider_account_environments WHERE provider_account_id = $1",
        )
        .bind(provider_account_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_unavailable)?;
        match provider_id.as_deref() {
            Some(openai_codex::PROVIDER_ID) => self.refresh_codex_quota(provider_account_id).await,
            Some(GROK_PROVIDER_ID) => self.refresh_grok_quota(provider_account_id).await,
            Some(DREAMINA_PROVIDER_ID) => self.refresh_dreamina_quota(provider_account_id).await,
            _ => Err(ImageGatewayError::not_found(
                "Managed provider account not found",
                Some("provider_account_id".to_owned()),
                "provider_account_not_found",
            )),
        }
    }

    async fn update_account_scheduling(
        &self,
        provider_account_id: Uuid,
        request: UpdateProviderAccountSchedulingRequest,
    ) -> Result<ProviderAccountSchedulingView, ImageGatewayError> {
        validate_account_scheduling_request(&request)?;
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        let control = sqlx::query_as::<_, AccountExecutionControlRow>(
            r#"
            SELECT desired_max_concurrency, lifecycle_state, control_version
            FROM provider_account_execution_controls
            WHERE provider_account_id = $1
            FOR UPDATE
            "#,
        )
        .bind(provider_account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Managed provider account not found",
                Some("provider_account_id".to_string()),
                "provider_account_not_found",
            )
        })?;
        if control.control_version != request.expected_control_version {
            return Err(ImageGatewayError::conflict(
                "Provider account scheduling changed since it was loaded",
                Some("expected_control_version".to_string()),
                "provider_account_control_version_conflict",
            ));
        }
        let runtime = sqlx::query_as::<_, AccountExecutionRuntimeRow>(
            r#"
            SELECT policy.max_concurrency AS hard_max_concurrency,
                   policy.allocated_count, policy.state AS policy_state,
                   account.state AS account_state,
                   pool.state AS credential_pool_state,
                   environment.state AS environment_state,
                   BOOL_AND(profile.state = 'enabled') AS profiles_enabled
            FROM provider_accounts account
            JOIN provider_credential_pools pool
              ON pool.credential_pool_id = account.credential_pool_id
             AND pool.provider_id = account.provider_id
            JOIN provider_account_environments environment
              ON environment.provider_account_id = account.provider_account_id
             AND environment.provider_id = account.provider_id
            JOIN provider_execution_profiles profile
              ON profile.provider_account_id = account.provider_account_id
             AND profile.provider_id = account.provider_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = profile.resource_policy_id
             AND policy.revision = profile.resource_policy_revision
            WHERE account.provider_account_id = $1
            GROUP BY policy.resource_policy_id, policy.revision,
                     policy.max_concurrency, policy.allocated_count, policy.state,
                     account.state, pool.state, environment.state
            "#,
        )
        .bind(provider_account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Managed provider account runtime not found",
                Some("provider_account_id".to_string()),
                "provider_account_runtime_not_found",
            )
        })?;
        if request.max_concurrency > runtime.hard_max_concurrency {
            return Err(ImageGatewayError::invalid_request(
                "Maximum concurrency exceeds the account safety ceiling",
                Some("max_concurrency".to_string()),
                "provider_account_capacity_exceeds_ceiling",
            ));
        }
        if request.accepting_new_work
            && (runtime.policy_state != "enabled"
                || runtime.account_state != "enabled"
                || runtime.credential_pool_state != "enabled"
                || runtime.environment_state != "active"
                || !runtime.profiles_enabled)
        {
            return Err(ImageGatewayError::conflict(
                "Provider account cannot resume until its runtime is healthy",
                Some("accepting_new_work".to_string()),
                "provider_account_runtime_unhealthy",
            ));
        }
        let now = database_now(&mut tx).await?;
        let lifecycle_state = if request.accepting_new_work {
            "active"
        } else {
            "draining"
        };
        sqlx::query(
            r#"
            INSERT INTO provider_account_execution_control_events
              (event_id, provider_account_id, previous_control_version,
               control_version, previous_max_concurrency, max_concurrency,
               previous_lifecycle_state, lifecycle_state, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(provider_account_id)
        .bind(control.control_version)
        .bind(control.control_version + 1)
        .bind(control.desired_max_concurrency)
        .bind(request.max_concurrency)
        .bind(&control.lifecycle_state)
        .bind(lifecycle_state)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        sqlx::query(
            r#"
            UPDATE provider_account_execution_controls
            SET desired_max_concurrency = $2, lifecycle_state = $3,
                control_version = control_version + 1,
                drain_started_at_ms = CASE
                    WHEN $3 = 'draining' AND lifecycle_state <> 'draining' THEN $4
                    WHEN $3 = 'active' THEN NULL
                    ELSE drain_started_at_ms
                END,
                updated_at_ms = $4
            WHERE provider_account_id = $1 AND control_version = $5
            "#,
        )
        .bind(provider_account_id)
        .bind(request.max_concurrency)
        .bind(lifecycle_state)
        .bind(now)
        .bind(request.expected_control_version)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        tx.commit().await.map_err(store_unavailable)?;
        Ok(ProviderAccountSchedulingView {
            provider_account_id,
            max_concurrency: request.max_concurrency,
            allocated_count: runtime.allocated_count,
            accepting_new_work: request.accepting_new_work,
            scheduling_state: lifecycle_state.to_string(),
            control_version: control.control_version + 1,
            updated_at_ms: now,
        })
    }

    async fn grok_video_output(
        &self,
        provider_account_id: Uuid,
    ) -> Result<GrokVideoOutputView, ImageGatewayError> {
        let home = self.grok_video_output_home(provider_account_id).await?;
        tokio::task::spawn_blocking(move || {
            super::grok_video_output::read(provider_account_id, &home)
        })
        .await
        .map_err(|_| {
            ImageGatewayError::service_unavailable("Grok video output configuration is unavailable")
        })?
    }

    async fn update_grok_video_output(
        &self,
        provider_account_id: Uuid,
        request: UpdateGrokVideoOutputRequest,
    ) -> Result<GrokVideoOutputView, ImageGatewayError> {
        let _guard = GrokVideoOutputUpdateGuard::acquire(provider_account_id)?;
        let home = self.grok_video_output_home(provider_account_id).await?;
        tokio::task::spawn_blocking(move || {
            super::grok_video_output::update(provider_account_id, &home, request)
        })
        .await
        .map_err(|_| {
            ImageGatewayError::service_unavailable("Grok video output configuration is unavailable")
        })?
    }

    async fn list_routes(&self) -> Result<ProviderRoutesSnapshot, ImageGatewayError> {
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
        let as_of_ms = database_now(&mut tx).await?;
        let routes = sqlx::query_as::<_, RouteRow>(
            r#"
            SELECT route.route_id, route.revision, route.route_key, route.display_name,
                   route.provider_id, route.operation_id, route.command_schema,
                   route.route_kind, route.selection_strategy,
                   route.quota_freshness_ms, route.unknown_quota_policy,
                   head.state, route.created_at_ms
            FROM provider_routes route
            JOIN provider_route_heads head
              USING (route_id, provider_id, operation_id, command_schema)
            WHERE head.state = 'enabled' AND route.revision = head.current_revision
            ORDER BY route.route_kind, route.display_name, route.route_key
            "#,
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        let members = sqlx::query_as::<_, RouteMemberRow>(
            r#"
            SELECT member.route_id, member.route_revision, member.provider_account_id,
                   account.account_key, member.execution_profile_id,
                   member.priority, member.weight, member.minimum_remaining_percent
            FROM provider_route_members member
            JOIN provider_accounts account
              ON account.provider_account_id = member.provider_account_id
            JOIN provider_routes route
              ON route.route_id = member.route_id AND route.revision = member.route_revision
            JOIN provider_route_heads head
              ON head.route_id = route.route_id
             AND head.current_revision = route.revision
            WHERE head.state = 'enabled' AND member.state = 'enabled'
            ORDER BY member.route_id, member.priority DESC, account.account_key
            "#,
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        let model_mappings = sqlx::query_as::<_, RouteModelMappingRow>(
            r#"
            SELECT mapping.route_id, mapping.route_revision, mapping.api_profile,
                   mapping.public_model_id, mapping.provider_model_id,
                   mapping.execution_model_id,
                   model.display_name AS provider_model_display_name,
                   mapping.media_kind
            FROM provider_route_model_mappings mapping
            JOIN provider_models model
              ON model.provider_id = mapping.provider_id
             AND model.model_id = mapping.provider_model_id
             AND model.media_kind = mapping.media_kind
            JOIN provider_route_heads head
              ON head.route_id = mapping.route_id
             AND head.current_revision = mapping.route_revision
            WHERE head.state = 'enabled'
            ORDER BY mapping.route_id, mapping.public_model_id
            "#,
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        tx.commit().await.map_err(store_unavailable)?;
        Ok(ProviderRoutesSnapshot {
            as_of_ms,
            routes: assemble_routes(routes, members, model_mappings),
        })
    }

    async fn create_route(
        &self,
        request: CreateProviderRouteRequest,
    ) -> Result<ProviderRouteView, ImageGatewayError> {
        validate_route_request(&request)?;
        let member_policies = request
            .members
            .iter()
            .map(|member| {
                (
                    member.provider_account_id,
                    (
                        member.priority,
                        member.weight,
                        member.minimum_remaining_percent,
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut member_ids = member_policies.keys().copied().collect::<Vec<_>>();
        member_ids.sort_unstable();
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        let now = database_now(&mut tx).await?;
        let profiles = sqlx::query_as::<_, ProfileMemberRow>(
            r#"
            SELECT profile.provider_account_id, account.account_key,
                   profile.execution_profile_id, profile.command_schema
            FROM provider_execution_profiles profile
            JOIN provider_accounts account
              ON account.provider_account_id = profile.provider_account_id
             AND account.provider_id = profile.provider_id
            JOIN provider_account_environments environment
              ON environment.provider_account_id = account.provider_account_id
             AND environment.provider_id = account.provider_id
            WHERE profile.provider_id = $1 AND profile.operation_id = $2
              AND profile.provider_account_id = ANY($3)
              AND profile.state = 'enabled' AND account.state = 'enabled'
              AND environment.state = 'active'
            ORDER BY profile.provider_account_id, profile.execution_profile_id
            FOR SHARE OF profile, account, environment
            "#,
        )
        .bind(&request.provider_id)
        .bind(&request.operation_id)
        .bind(&member_ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        if profiles.len() != member_ids.len()
            || profiles
                .windows(2)
                .any(|pair| pair[0].provider_account_id == pair[1].provider_account_id)
        {
            return Err(ImageGatewayError::invalid_request(
                "Every route member must have exactly one active execution profile",
                Some("members".to_string()),
                "invalid_provider_route_members",
            ));
        }
        let command_schema = profiles[0].command_schema.clone();
        if profiles
            .iter()
            .any(|profile| profile.command_schema != command_schema)
        {
            return Err(ImageGatewayError::invalid_request(
                "Route members have incompatible command schemas",
                Some("members".to_string()),
                "incompatible_provider_route_members",
            ));
        }
        let model_mappings = validated_route_model_mappings(
            &mut tx,
            &request.provider_id,
            &request.operation_id,
            &member_ids,
            request.model_mappings.as_deref(),
        )
        .await?;
        let route_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO provider_routes
              (route_id, revision, route_key, display_name, provider_id, operation_id,
               command_schema, route_kind, selection_strategy, quota_freshness_ms,
               unknown_quota_policy, state, created_at_ms)
            VALUES ($1, 1, $2, $3, $4, $5, $6, 'group', $7, $8, $9,
                    'enabled', $10)
            "#,
        )
        .bind(route_id)
        .bind(&request.route_key)
        .bind(&request.display_name)
        .bind(&request.provider_id)
        .bind(&request.operation_id)
        .bind(&command_schema)
        .bind(&request.selection_strategy)
        .bind(request.quota_freshness_ms)
        .bind(&request.unknown_quota_policy)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(map_route_insert)?;
        sqlx::query(
            r#"
            INSERT INTO provider_route_heads
              (route_id, route_key, provider_id, operation_id, command_schema,
               route_kind, current_revision, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, 'group', 1, 'enabled', $6, $6)
            "#,
        )
        .bind(route_id)
        .bind(&request.route_key)
        .bind(&request.provider_id)
        .bind(&request.operation_id)
        .bind(&command_schema)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(map_route_insert)?;
        for profile in &profiles {
            let policy = member_policies
                .get(&profile.provider_account_id)
                .ok_or_else(|| {
                    ImageGatewayError::service_unavailable(
                        "provider route member policy unavailable",
                    )
                })?;
            sqlx::query(
                r#"
                INSERT INTO provider_route_members
                  (route_id, route_revision, provider_id, operation_id, command_schema,
                   provider_account_id, execution_profile_id, priority, weight, state,
                   minimum_remaining_percent, created_at_ms)
                VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8, 'enabled', $9, $10)
                "#,
            )
            .bind(route_id)
            .bind(&request.provider_id)
            .bind(&request.operation_id)
            .bind(&command_schema)
            .bind(profile.provider_account_id)
            .bind(profile.execution_profile_id)
            .bind(policy.0)
            .bind(policy.1)
            .bind(policy.2)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
        }
        insert_route_model_mappings(
            &mut tx,
            route_id,
            1,
            &request.provider_id,
            &request.operation_id,
            &command_schema,
            &model_mappings,
            now,
        )
        .await?;
        tx.commit().await.map_err(store_unavailable)?;
        Ok(ProviderRouteView {
            route_id,
            revision: 1,
            route_key: request.route_key,
            display_name: request.display_name,
            provider_id: request.provider_id,
            operation_id: request.operation_id,
            command_schema,
            route_kind: "group".to_string(),
            selection_strategy: request.selection_strategy,
            quota_freshness_ms: request.quota_freshness_ms,
            unknown_quota_policy: request.unknown_quota_policy,
            state: "enabled".to_string(),
            members: profiles
                .into_iter()
                .map(|profile| ProviderRouteMemberView {
                    provider_account_id: profile.provider_account_id,
                    account_key: profile.account_key,
                    execution_profile_id: profile.execution_profile_id,
                    priority: member_policies[&profile.provider_account_id].0,
                    weight: member_policies[&profile.provider_account_id].1,
                    minimum_remaining_percent: member_policies[&profile.provider_account_id].2,
                })
                .collect(),
            model_mappings: model_mappings
                .into_iter()
                .map(route_model_mapping_view)
                .collect(),
            created_at_ms: now,
        })
    }

    async fn update_route(
        &self,
        route_id: Uuid,
        request: UpdateProviderRouteRequest,
    ) -> Result<ProviderRouteView, ImageGatewayError> {
        validate_route_update_request(&request)?;
        let member_policies = request
            .members
            .iter()
            .map(|member| {
                (
                    member.provider_account_id,
                    (
                        member.priority,
                        member.weight,
                        member.minimum_remaining_percent,
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut member_ids = member_policies.keys().copied().collect::<Vec<_>>();
        member_ids.sort_unstable();
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        let current = sqlx::query_as::<_, RouteRow>(
            r#"
            SELECT route.route_id, route.revision, route.route_key, route.display_name,
                   route.provider_id, route.operation_id, route.command_schema,
                   route.route_kind, route.selection_strategy,
                   route.quota_freshness_ms, route.unknown_quota_policy,
                   head.state, route.created_at_ms
            FROM provider_route_heads head
            JOIN provider_routes route
              ON route.route_id = head.route_id
             AND route.revision = head.current_revision
             AND route.provider_id = head.provider_id
             AND route.operation_id = head.operation_id
             AND route.command_schema = head.command_schema
            WHERE head.route_id = $1 AND head.state = 'enabled'
            FOR UPDATE OF head
            "#,
        )
        .bind(route_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Provider route not found",
                Some("route_id".to_string()),
                "provider_route_not_found",
            )
        })?;
        if current.revision != request.expected_revision {
            return Err(ImageGatewayError::conflict(
                "Provider route changed since it was loaded",
                Some("expected_revision".to_string()),
                "provider_route_revision_conflict",
            ));
        }
        if current.route_kind == "account" {
            let existing_member_ids: Vec<Uuid> = sqlx::query_scalar(
                r#"
                SELECT provider_account_id
                FROM provider_route_members
                WHERE route_id = $1 AND route_revision = $2 AND state = 'enabled'
                ORDER BY provider_account_id
                "#,
            )
            .bind(route_id)
            .bind(current.revision)
            .fetch_all(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            if existing_member_ids != member_ids {
                return Err(ImageGatewayError::invalid_request(
                    "Single-account route membership is immutable",
                    Some("members".to_owned()),
                    "provider_account_route_members_immutable",
                ));
            }
        }
        let profiles = sqlx::query_as::<_, ProfileMemberRow>(
            r#"
            SELECT profile.provider_account_id, account.account_key,
                   profile.execution_profile_id, profile.command_schema
            FROM provider_execution_profiles profile
            JOIN provider_accounts account
              ON account.provider_account_id = profile.provider_account_id
             AND account.provider_id = profile.provider_id
            JOIN provider_account_environments environment
              ON environment.provider_account_id = account.provider_account_id
             AND environment.provider_id = account.provider_id
            WHERE profile.provider_id = $1 AND profile.operation_id = $2
              AND profile.provider_account_id = ANY($3)
              AND profile.state = 'enabled' AND account.state = 'enabled'
              AND environment.state = 'active'
            ORDER BY profile.provider_account_id, profile.execution_profile_id
            FOR SHARE OF profile, account, environment
            "#,
        )
        .bind(&current.provider_id)
        .bind(&current.operation_id)
        .bind(&member_ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        if profiles.len() != member_ids.len()
            || profiles
                .windows(2)
                .any(|pair| pair[0].provider_account_id == pair[1].provider_account_id)
            || profiles
                .iter()
                .any(|profile| profile.command_schema != current.command_schema)
        {
            return Err(ImageGatewayError::invalid_request(
                "Every route member must have exactly one compatible active execution profile",
                Some("members".to_string()),
                "invalid_provider_route_members",
            ));
        }
        let inherited_mappings = match request.model_mappings.as_ref() {
            Some(_) => None,
            None => Some(existing_route_model_mappings(&mut tx, route_id, current.revision).await?),
        };
        let inherited_requests = inherited_mappings
            .as_ref()
            .filter(|mappings| !mappings.is_empty())
            .map(|mappings| {
                mappings
                    .iter()
                    .map(|mapping| ProviderRouteModelMappingRequest {
                        api_profile: mapping.api_profile.clone(),
                        public_model_id: mapping.public_model_id.clone(),
                        provider_model_id: mapping.provider_model_id.clone(),
                        media_kind: mapping.media_kind.clone(),
                    })
                    .collect::<Vec<_>>()
            });
        let requested_mappings = request
            .model_mappings
            .as_deref()
            .or_else(|| inherited_requests.as_deref());
        let model_mappings = validated_route_model_mappings(
            &mut tx,
            &current.provider_id,
            &current.operation_id,
            &member_ids,
            requested_mappings,
        )
        .await?;
        let revision = current.revision.checked_add(1).ok_or_else(|| {
            ImageGatewayError::service_unavailable("provider route revision exhausted")
        })?;
        let now = database_now(&mut tx).await?;
        sqlx::query(
            r#"
            INSERT INTO provider_routes
              (route_id, revision, route_key, display_name, provider_id, operation_id,
               command_schema, route_kind, selection_strategy, quota_freshness_ms,
               unknown_quota_policy, state, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                    'enabled', $12)
            "#,
        )
        .bind(route_id)
        .bind(revision)
        .bind(&current.route_key)
        .bind(&request.display_name)
        .bind(&current.provider_id)
        .bind(&current.operation_id)
        .bind(&current.command_schema)
        .bind(&current.route_kind)
        .bind(&request.selection_strategy)
        .bind(request.quota_freshness_ms)
        .bind(&request.unknown_quota_policy)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(map_route_insert)?;
        for profile in &profiles {
            let policy = member_policies
                .get(&profile.provider_account_id)
                .ok_or_else(|| {
                    ImageGatewayError::service_unavailable(
                        "provider route member policy unavailable",
                    )
                })?;
            sqlx::query(
                r#"
                INSERT INTO provider_route_members
                  (route_id, route_revision, provider_id, operation_id, command_schema,
                   provider_account_id, execution_profile_id, priority, weight, state,
                   minimum_remaining_percent, created_at_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'enabled', $10, $11)
                "#,
            )
            .bind(route_id)
            .bind(revision)
            .bind(&current.provider_id)
            .bind(&current.operation_id)
            .bind(&current.command_schema)
            .bind(profile.provider_account_id)
            .bind(profile.execution_profile_id)
            .bind(policy.0)
            .bind(policy.1)
            .bind(policy.2)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
        }
        insert_route_model_mappings(
            &mut tx,
            route_id,
            revision,
            &current.provider_id,
            &current.operation_id,
            &current.command_schema,
            &model_mappings,
            now,
        )
        .await?;
        sqlx::query(
            r#"
            UPDATE provider_route_heads
            SET current_revision = $2, updated_at_ms = $3
            WHERE route_id = $1 AND current_revision = $4
            "#,
        )
        .bind(route_id)
        .bind(revision)
        .bind(now)
        .bind(current.revision)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        tx.commit().await.map_err(store_unavailable)?;
        Ok(ProviderRouteView {
            route_id,
            revision,
            route_key: current.route_key,
            display_name: request.display_name,
            provider_id: current.provider_id,
            operation_id: current.operation_id,
            command_schema: current.command_schema,
            route_kind: current.route_kind,
            selection_strategy: request.selection_strategy,
            quota_freshness_ms: request.quota_freshness_ms,
            unknown_quota_policy: request.unknown_quota_policy,
            state: "enabled".to_string(),
            members: profiles
                .into_iter()
                .map(|profile| ProviderRouteMemberView {
                    provider_account_id: profile.provider_account_id,
                    account_key: profile.account_key,
                    execution_profile_id: profile.execution_profile_id,
                    priority: member_policies[&profile.provider_account_id].0,
                    weight: member_policies[&profile.provider_account_id].1,
                    minimum_remaining_percent: member_policies[&profile.provider_account_id].2,
                })
                .collect(),
            model_mappings: model_mappings
                .into_iter()
                .map(route_model_mapping_view)
                .collect(),
            created_at_ms: now,
        })
    }

    async fn bind_api_key_route(
        &self,
        project_id: &str,
        api_key_id: &str,
        route_id: Uuid,
    ) -> Result<ApiKeyRouteBindingView, ImageGatewayError> {
        validate_external_id(project_id, "project_id")?;
        validate_external_id(api_key_id, "api_key_id")?;
        let mut tx = self.pool.begin().await.map_err(store_unavailable)?;
        let owner = sqlx::query_as::<_, ApiKeyOwnerRow>(
            r#"
            SELECT service_account_id, tenant_id
            FROM gateway_api_keys
            WHERE id = $1 AND project_id = $2 AND deleted_at IS NULL
              AND (expires_at IS NULL OR expires_at > $3)
            FOR UPDATE
            "#,
        )
        .bind(api_key_id)
        .bind(project_id)
        .bind(now_seconds()?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "API key not found",
                Some("api_key_id".to_string()),
                "api_key_not_found",
            )
        })?;
        let route = sqlx::query_as::<_, RouteRow>(
            r#"
            SELECT route.route_id, route.revision, route.route_key, route.display_name,
                   route.provider_id, route.operation_id, route.command_schema,
                   route.route_kind, route.selection_strategy,
                   route.quota_freshness_ms, route.unknown_quota_policy,
                   head.state, route.created_at_ms
            FROM provider_route_heads head
            JOIN provider_routes route
              ON route.route_id = head.route_id
             AND route.revision = head.current_revision
            WHERE head.route_id = $1 AND head.state = 'enabled'
            FOR SHARE OF head
            "#,
        )
        .bind(route_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Provider route not found",
                Some("route_id".to_string()),
                "provider_route_not_found",
            )
        })?;
        let now = database_now(&mut tx).await?;
        sqlx::query(
            r#"
            INSERT INTO gateway_api_key_provider_routes
              (api_key_id, service_account_id, project_id, tenant_id,
               provider_id, operation_id, command_schema, route_id,
               route_revision, bound_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (api_key_id, provider_id, operation_id) DO UPDATE
            SET command_schema = EXCLUDED.command_schema,
                route_id = EXCLUDED.route_id,
                route_revision = EXCLUDED.route_revision,
                bound_at_ms = EXCLUDED.bound_at_ms
            "#,
        )
        .bind(api_key_id)
        .bind(&owner.service_account_id)
        .bind(project_id)
        .bind(&owner.tenant_id)
        .bind(&route.provider_id)
        .bind(&route.operation_id)
        .bind(&route.command_schema)
        .bind(route.route_id)
        .bind(route.revision)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        sqlx::query("UPDATE gateway_api_keys SET authz_version = authz_version + 1 WHERE id = $1")
            .bind(api_key_id)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
        tx.commit().await.map_err(store_unavailable)?;
        Ok(ApiKeyRouteBindingView {
            api_key_id: api_key_id.to_string(),
            project_id: project_id.to_string(),
            provider_id: route.provider_id,
            operation_id: route.operation_id,
            command_schema: route.command_schema,
            route_id: route.route_id,
            route_revision: route.revision,
            route_name: route.display_name,
            bound_at_ms: now,
        })
    }

    async fn api_key_route(
        &self,
        project_id: &str,
        api_key_id: &str,
    ) -> Result<Option<ApiKeyRouteBindingView>, ImageGatewayError> {
        validate_external_id(project_id, "project_id")?;
        validate_external_id(api_key_id, "api_key_id")?;
        sqlx::query_as::<_, ApiKeyRouteRow>(
            r#"
            SELECT binding.api_key_id, binding.project_id, binding.provider_id,
                   binding.operation_id, binding.command_schema, binding.route_id,
                   binding.route_revision,
                   route.display_name AS route_name,
                   binding.bound_at_ms
            FROM gateway_api_key_provider_routes binding
            JOIN provider_route_heads head
              ON head.route_id = binding.route_id AND head.state = 'enabled'
            JOIN provider_routes route
              ON route.route_id = binding.route_id
             AND route.revision = binding.route_revision
            JOIN gateway_api_keys api_key
              ON api_key.id = binding.api_key_id
             AND api_key.project_id = binding.project_id
            WHERE binding.api_key_id = $1 AND binding.project_id = $2
              AND api_key.deleted_at IS NULL
            "#,
        )
        .bind(api_key_id)
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await
        .map(|row| row.map(Into::into))
        .map_err(store_unavailable)
    }
}

fn provider_login_session_from_row(
    row: LoginSessionRow,
) -> Result<ProviderLoginSession, ImageGatewayError> {
    Ok(ProviderLoginSession {
        login_session_id: row.login_session_id,
        provider_id: row.provider_id,
        account_key: row.account_key,
        display_name: row.display_name,
        status: row.status,
        login_method: CodexLoginMethod::from_database(&row.login_method)?,
        authorization_url: row.authorization_url,
        user_code: row.user_code,
        provider_account_id: row.provider_account_id,
        error_code: row.error_code,
        expires_at_ms: row.expires_at_ms,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

impl From<ApiKeyRouteRow> for ApiKeyRouteBindingView {
    fn from(row: ApiKeyRouteRow) -> Self {
        Self {
            api_key_id: row.api_key_id,
            project_id: row.project_id,
            provider_id: row.provider_id,
            operation_id: row.operation_id,
            command_schema: row.command_schema,
            route_id: row.route_id,
            route_revision: row.route_revision,
            route_name: row.route_name,
            bound_at_ms: row.bound_at_ms,
        }
    }
}

fn assemble_routes(
    routes: Vec<RouteRow>,
    members: Vec<RouteMemberRow>,
    model_mappings: Vec<RouteModelMappingRow>,
) -> Vec<ProviderRouteView> {
    let mut grouped: HashMap<(Uuid, i64), Vec<ProviderRouteMemberView>> = HashMap::new();
    for member in members {
        grouped
            .entry((member.route_id, member.route_revision))
            .or_default()
            .push(ProviderRouteMemberView {
                provider_account_id: member.provider_account_id,
                account_key: member.account_key,
                execution_profile_id: member.execution_profile_id,
                priority: member.priority,
                weight: member.weight,
                minimum_remaining_percent: member.minimum_remaining_percent,
            });
    }
    let mut grouped_mappings: HashMap<(Uuid, i64), Vec<ProviderRouteModelMappingView>> =
        HashMap::new();
    for mapping in model_mappings {
        grouped_mappings
            .entry((mapping.route_id, mapping.route_revision))
            .or_default()
            .push(ProviderRouteModelMappingView {
                api_profile: mapping.api_profile,
                public_model_id: mapping.public_model_id,
                provider_model_id: mapping.provider_model_id,
                execution_model_id: mapping.execution_model_id,
                provider_model_display_name: mapping.provider_model_display_name,
                media_kind: mapping.media_kind,
            });
    }
    routes
        .into_iter()
        .map(|route| ProviderRouteView {
            members: grouped
                .remove(&(route.route_id, route.revision))
                .unwrap_or_default(),
            model_mappings: grouped_mappings
                .remove(&(route.route_id, route.revision))
                .unwrap_or_default(),
            route_id: route.route_id,
            revision: route.revision,
            route_key: route.route_key,
            display_name: route.display_name,
            provider_id: route.provider_id,
            operation_id: route.operation_id,
            command_schema: route.command_schema,
            route_kind: route.route_kind,
            selection_strategy: route.selection_strategy,
            quota_freshness_ms: route.quota_freshness_ms,
            unknown_quota_policy: route.unknown_quota_policy,
            state: route.state,
            created_at_ms: route.created_at_ms,
        })
        .collect()
}

async fn persist_quota(
    tx: &mut Transaction<'_, Postgres>,
    provider_account_id: Uuid,
    quota: &CodexQuotaSnapshot,
    observed_at_ms: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO provider_account_quota_snapshots
          (provider_account_id, provider_id, plan_type, credits_balance,
           credits_unlimited, status, observed_at_ms, last_error_code)
        VALUES ($1, $2, $3, $4, $5, 'observed', $6, NULL)
        ON CONFLICT (provider_account_id) DO UPDATE
        SET plan_type = EXCLUDED.plan_type,
            credits_balance = EXCLUDED.credits_balance,
            credits_unlimited = EXCLUDED.credits_unlimited,
            status = 'observed', observed_at_ms = EXCLUDED.observed_at_ms,
            last_error_code = NULL
        "#,
    )
    .bind(provider_account_id)
    .bind(openai_codex::PROVIDER_ID)
    .bind(&quota.plan_type)
    .bind(&quota.credits_balance)
    .bind(quota.credits_unlimited)
    .bind(observed_at_ms)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    sqlx::query("DELETE FROM provider_account_quota_windows WHERE provider_account_id = $1")
        .bind(provider_account_id)
        .execute(&mut **tx)
        .await
        .map_err(store_unavailable)?;
    for window in &quota.windows {
        sqlx::query(
            r#"
            INSERT INTO provider_account_quota_windows
              (provider_account_id, provider_id, limit_id, limit_name, window_role,
               window_duration_mins, used_percent, resets_at_ms, observed_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(provider_account_id)
        .bind(openai_codex::PROVIDER_ID)
        .bind(&window.limit_id)
        .bind(&window.limit_name)
        .bind(window.window_role)
        .bind(window.window_duration_mins)
        .bind(window.used_percent)
        .bind(window.resets_at_ms)
        .bind(observed_at_ms)
        .execute(&mut **tx)
        .await
        .map_err(store_unavailable)?;
    }
    Ok(())
}

async fn persist_grok_quota(
    tx: &mut Transaction<'_, Postgres>,
    provider_account_id: Uuid,
    quota: &GrokQuotaSnapshot,
    observed_at_ms: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO provider_account_quota_snapshots
          (provider_account_id, provider_id, plan_type, credits_balance,
           credits_unlimited, status, observed_at_ms, last_error_code)
        VALUES ($1, $2, $3, NULL, NULL, 'observed', $4, NULL)
        ON CONFLICT (provider_account_id) DO UPDATE
        SET plan_type = EXCLUDED.plan_type,
            credits_balance = NULL,
            credits_unlimited = NULL,
            status = 'observed', observed_at_ms = EXCLUDED.observed_at_ms,
            last_error_code = NULL
        "#,
    )
    .bind(provider_account_id)
    .bind(GROK_PROVIDER_ID)
    .bind(&quota.plan_type)
    .bind(observed_at_ms)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    sqlx::query("DELETE FROM provider_account_quota_windows WHERE provider_account_id = $1")
        .bind(provider_account_id)
        .execute(&mut **tx)
        .await
        .map_err(store_unavailable)?;
    for window in &quota.windows {
        sqlx::query(
            r#"
            INSERT INTO provider_account_quota_windows
              (provider_account_id, provider_id, limit_id, limit_name, window_role,
               window_duration_mins, used_percent, resets_at_ms, observed_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(provider_account_id)
        .bind(GROK_PROVIDER_ID)
        .bind(window.limit_id)
        .bind(window.limit_name)
        .bind(window.window_role)
        .bind(window.window_duration_mins)
        .bind(window.used_percent)
        .bind(window.resets_at_ms)
        .bind(observed_at_ms)
        .execute(&mut **tx)
        .await
        .map_err(store_unavailable)?;
    }
    Ok(())
}

async fn mark_codex_quota_unavailable(
    pool: &PgPool,
    provider_account_id: Uuid,
    error_code: &str,
) -> Result<(), ImageGatewayError> {
    let now = now_ms()?;
    sqlx::query(
        r#"
        INSERT INTO provider_account_quota_snapshots
          (provider_account_id, provider_id, status, observed_at_ms, last_error_code)
        VALUES ($1, $2, 'unavailable', $3, $4)
        ON CONFLICT (provider_account_id) DO UPDATE
        SET status = 'unavailable', observed_at_ms = EXCLUDED.observed_at_ms,
            last_error_code = EXCLUDED.last_error_code
        "#,
    )
    .bind(provider_account_id)
    .bind(openai_codex::PROVIDER_ID)
    .bind(now)
    .bind(error_code)
    .execute(pool)
    .await
    .map_err(store_unavailable)?;
    Ok(())
}

async fn persist_dreamina_quota(
    tx: &mut Transaction<'_, Postgres>,
    provider_account_id: Uuid,
    account: &DreaminaAccountSnapshot,
    observed_at_ms: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO provider_account_quota_snapshots
          (provider_account_id, provider_id, plan_type, credits_balance,
           credits_unlimited, status, observed_at_ms, last_error_code)
        VALUES ($1, $2, $3, $4, FALSE, 'observed', $5, NULL)
        ON CONFLICT (provider_account_id) DO UPDATE
        SET plan_type = EXCLUDED.plan_type,
            credits_balance = EXCLUDED.credits_balance,
            credits_unlimited = FALSE,
            status = 'observed', observed_at_ms = EXCLUDED.observed_at_ms,
            last_error_code = NULL
        "#,
    )
    .bind(provider_account_id)
    .bind(DREAMINA_PROVIDER_ID)
    .bind(&account.vip_level)
    .bind(account.total_credit.to_string())
    .bind(observed_at_ms)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    sqlx::query("DELETE FROM provider_account_quota_windows WHERE provider_account_id = $1")
        .bind(provider_account_id)
        .execute(&mut **tx)
        .await
        .map_err(store_unavailable)?;
    Ok(())
}

fn dreamina_environment_state(permission: DreaminaCliPermission) -> &'static str {
    match permission {
        DreaminaCliPermission::Granted => "active",
        DreaminaCliPermission::Required | DreaminaCliPermission::Unknown => "disabled",
    }
}

async fn persist_dreamina_capability_state(
    tx: &mut Transaction<'_, Postgres>,
    provider_account_id: Uuid,
    permission: DreaminaCliPermission,
    observed_at_ms: i64,
) -> Result<(), ImageGatewayError> {
    if permission == DreaminaCliPermission::Unknown {
        return Ok(());
    }
    sqlx::query(
        r#"
        UPDATE provider_account_environments
        SET state = $2, updated_at_ms = $3
        WHERE provider_account_id = $1 AND provider_id = $4
          AND state IS DISTINCT FROM $2
        "#,
    )
    .bind(provider_account_id)
    .bind(dreamina_environment_state(permission))
    .bind(observed_at_ms)
    .bind(DREAMINA_PROVIDER_ID)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    Ok(())
}

async fn mark_dreamina_quota_unavailable(
    pool: &PgPool,
    provider_account_id: Uuid,
    error_code: &str,
) -> Result<(), ImageGatewayError> {
    let now = now_ms()?;
    sqlx::query(
        r#"
        INSERT INTO provider_account_quota_snapshots
          (provider_account_id, provider_id, status, observed_at_ms, last_error_code)
        VALUES ($1, $2, 'unavailable', $3, $4)
        ON CONFLICT (provider_account_id) DO UPDATE
        SET status = 'unavailable', observed_at_ms = EXCLUDED.observed_at_ms,
            last_error_code = EXCLUDED.last_error_code
        "#,
    )
    .bind(provider_account_id)
    .bind(DREAMINA_PROVIDER_ID)
    .bind(now)
    .bind(error_code)
    .execute(pool)
    .await
    .map_err(store_unavailable)?;
    Ok(())
}

fn dreamina_identity_sha256(account: &DreaminaAccountSnapshot) -> String {
    let mut digest = Sha256::new();
    digest.update(b"ai-image-factory/dreamina-account-identity/v1\0");
    digest.update(account.user_id.as_bytes());
    hex::encode(digest.finalize())
}

fn validate_login_request(request: &StartCodexLoginRequest) -> Result<(), ImageGatewayError> {
    if request.display_name.trim().is_empty()
        || request.display_name.chars().count() > MAX_DISPLAY_NAME_CHARS
    {
        return Err(ImageGatewayError::invalid_request(
            "Codex account display name is invalid",
            Some("display_name".to_string()),
            "invalid_provider_account",
        ));
    }
    if !(1..=64).contains(&request.max_concurrency) {
        return Err(ImageGatewayError::invalid_request(
            "Codex account concurrency is invalid",
            Some("max_concurrency".to_string()),
            "invalid_provider_account",
        ));
    }
    Ok(())
}

fn validate_provider_login_request(
    request: &StartProviderLoginRequest,
    provider_name: &str,
) -> Result<(), ImageGatewayError> {
    if request.display_name.trim().is_empty()
        || request.display_name.chars().count() > MAX_DISPLAY_NAME_CHARS
    {
        return Err(ImageGatewayError::invalid_request(
            format!("{provider_name} account display name is invalid"),
            Some("display_name".to_owned()),
            "invalid_provider_account",
        ));
    }
    if !(1..=64).contains(&request.max_concurrency) {
        return Err(ImageGatewayError::invalid_request(
            format!("{provider_name} account concurrency is invalid"),
            Some("max_concurrency".to_owned()),
            "invalid_provider_account",
        ));
    }
    Ok(())
}

fn normalize_provider_login_operations(
    request: &mut StartProviderLoginRequest,
) -> Result<(), ImageGatewayError> {
    let supported = match request.provider_id.as_str() {
        openai_codex::PROVIDER_ID => &["images.generations", "images.edits"][..],
        GROK_PROVIDER_ID => &["images.generations", "images.edits", "videos.generations"][..],
        DREAMINA_PROVIDER_ID => &["images.generations", "videos.generations"][..],
        _ => return Ok(()),
    };
    if request.operation_ids.is_empty() {
        request.operation_ids = supported.iter().map(|value| (*value).to_owned()).collect();
        return Ok(());
    }
    if request.provider_id == GROK_PROVIDER_ID
        && request
            .operation_ids
            .iter()
            .any(|operation| operation == "images.generations")
        && !request
            .operation_ids
            .iter()
            .any(|operation| operation == "images.edits")
    {
        request.operation_ids.push("images.edits".to_owned());
    }
    request.operation_ids.sort();
    request.operation_ids.dedup();
    if request
        .operation_ids
        .iter()
        .any(|operation| !supported.contains(&operation.as_str()))
    {
        return Err(ImageGatewayError::invalid_request(
            "Selected operation is unsupported by this provider",
            Some("operation_ids".to_owned()),
            "unsupported_provider_operation",
        ));
    }
    Ok(())
}

fn managed_codex_account_key(login_session_id: Uuid) -> String {
    format!("codex-{}", login_session_id.simple())
}

fn validate_account_scheduling_request(
    request: &UpdateProviderAccountSchedulingRequest,
) -> Result<(), ImageGatewayError> {
    if request.expected_control_version <= 0 || !(1..=64).contains(&request.max_concurrency) {
        return Err(ImageGatewayError::invalid_request(
            "Provider account scheduling configuration is invalid",
            Some("max_concurrency".to_string()),
            "invalid_provider_account_scheduling",
        ));
    }
    Ok(())
}

async fn validated_route_model_mappings(
    tx: &mut Transaction<'_, Postgres>,
    provider_id: &str,
    operation_id: &str,
    member_ids: &[Uuid],
    requested: Option<&[ProviderRouteModelMappingRequest]>,
) -> Result<Vec<ValidatedRouteModelMapping>, ImageGatewayError> {
    let available = sqlx::query_as::<_, AvailableRouteModelRow>(
        r#"
        SELECT model_id, execution_model_id, display_name, media_kind
        FROM provider_models
        WHERE provider_id = $1 AND $2 = ANY(operation_ids)
          AND adapter_state = 'supported' AND lifecycle_state = 'enabled'
        ORDER BY media_kind, model_id
        FOR SHARE
        "#,
    )
    .bind(provider_id)
    .bind(operation_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    let by_target = available
        .iter()
        .map(|model| ((model.model_id.as_str(), model.media_kind.as_str()), model))
        .collect::<HashMap<_, _>>();

    let candidates = match requested {
        Some(mappings) => {
            if mappings.is_empty() || mappings.len() > 100 {
                return Err(invalid_route_models());
            }
            mappings
                .iter()
                .map(|mapping| {
                    (
                        mapping.api_profile.clone(),
                        mapping.public_model_id.trim().to_owned(),
                        mapping.provider_model_id.clone(),
                        mapping.media_kind.clone(),
                    )
                })
                .collect::<Vec<_>>()
        }
        None => available
            .iter()
            .flat_map(|model| {
                default_model_aliases(provider_id, operation_id, &model.model_id)
                    .into_iter()
                    .map(|(api_profile, public_model_id)| {
                        (
                            api_profile,
                            public_model_id,
                            model.model_id.clone(),
                            model.media_kind.clone(),
                        )
                    })
            })
            .collect::<Vec<_>>(),
    };

    let mut public_ids = HashSet::new();
    let mut execution_targets = HashSet::new();
    let mut validated = Vec::with_capacity(candidates.len());
    for (api_profile, public_model_id, provider_model_id, media_kind) in candidates {
        let execution_provider_model_id = provider_model_id;
        let Some(execution_model) =
            by_target.get(&(execution_provider_model_id.as_str(), media_kind.as_str()))
        else {
            return Err(invalid_route_models());
        };
        let provider_model_id =
            pricing_provider_model_id(provider_id, &execution_provider_model_id).to_owned();
        if !by_target.contains_key(&(provider_model_id.as_str(), media_kind.as_str())) {
            return Err(invalid_route_models());
        }
        if !valid_simple_identifier(&api_profile)
            || !supported_api_profile(provider_id, operation_id, &api_profile)
            || !valid_public_model_id(&public_model_id)
            || !public_ids.insert((api_profile.clone(), public_model_id.clone()))
            || !execution_targets.insert((
                api_profile.clone(),
                execution_model.execution_model_id.clone(),
            ))
        {
            return Err(invalid_route_models());
        }
        if requested.is_some()
            && !route_members_support_model(
                tx,
                provider_id,
                operation_id,
                member_ids,
                &execution_provider_model_id,
                &media_kind,
            )
            .await?
        {
            return Err(ImageGatewayError::invalid_request(
                "No selected route member can execute the mapped provider model",
                Some("model_mappings".to_owned()),
                "unroutable_provider_model",
            ));
        }
        let execution_model_id = execution_model.execution_model_id.clone();
        let provider_model_display_name = execution_model.display_name.clone();
        validated.push(ValidatedRouteModelMapping {
            api_profile,
            public_model_id,
            provider_model_id,
            execution_model_id,
            provider_model_display_name,
            media_kind,
        });
    }
    if validated.is_empty() {
        return Err(invalid_route_models());
    }
    Ok(validated)
}

fn pricing_provider_model_id<'a>(provider_id: &str, provider_model_id: &'a str) -> &'a str {
    if provider_id == openai_codex::PROVIDER_ID
        && provider_model_id == openai_codex::MODEL_GPT_IMAGE_2_SNAPSHOT
    {
        openai_codex::MODEL_GPT_IMAGE_2
    } else {
        provider_model_id
    }
}

async fn route_members_support_model(
    tx: &mut Transaction<'_, Postgres>,
    provider_id: &str,
    operation_id: &str,
    member_ids: &[Uuid],
    model_id: &str,
    media_kind: &str,
) -> Result<bool, ImageGatewayError> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM provider_execution_profiles profile
          LEFT JOIN provider_account_model_configurations configuration
            ON configuration.provider_account_id = profile.provider_account_id
           AND configuration.provider_id = profile.provider_id
          WHERE profile.provider_id = $1 AND profile.operation_id = $2
            AND profile.provider_account_id = ANY($3)
            AND profile.state = 'enabled'
            AND (
              configuration.provider_account_id IS NULL
              OR configuration.mode = 'automatic'
              OR EXISTS (
                SELECT 1
                FROM provider_account_model_bindings binding
                WHERE binding.provider_account_id = profile.provider_account_id
                  AND binding.provider_id = profile.provider_id
                  AND binding.model_id = $4 AND binding.media_kind = $5
              )
            )
        )
        "#,
    )
    .bind(provider_id)
    .bind(operation_id)
    .bind(member_ids)
    .bind(model_id)
    .bind(media_kind)
    .fetch_one(&mut **tx)
    .await
    .map_err(store_unavailable)
}

async fn existing_route_model_mappings(
    tx: &mut Transaction<'_, Postgres>,
    route_id: Uuid,
    revision: i64,
) -> Result<Vec<ValidatedRouteModelMapping>, ImageGatewayError> {
    sqlx::query_as::<_, RouteModelMappingRow>(
        r#"
        SELECT mapping.route_id, mapping.route_revision, mapping.api_profile,
               mapping.public_model_id, mapping.provider_model_id,
               mapping.execution_model_id,
               model.display_name AS provider_model_display_name,
               mapping.media_kind
        FROM provider_route_model_mappings mapping
        JOIN provider_models model
          ON model.provider_id = mapping.provider_id
         AND model.model_id = mapping.provider_model_id
         AND model.media_kind = mapping.media_kind
        WHERE mapping.route_id = $1 AND mapping.route_revision = $2
        ORDER BY mapping.api_profile, mapping.public_model_id
        "#,
    )
    .bind(route_id)
    .bind(revision)
    .fetch_all(&mut **tx)
    .await
    .map_err(store_unavailable)
    .map(|rows| {
        rows.into_iter()
            .map(|row| ValidatedRouteModelMapping {
                api_profile: row.api_profile,
                public_model_id: row.public_model_id,
                provider_model_id: row.provider_model_id,
                execution_model_id: row.execution_model_id,
                provider_model_display_name: row.provider_model_display_name,
                media_kind: row.media_kind,
            })
            .collect()
    })
}

#[allow(clippy::too_many_arguments)]
async fn insert_route_model_mappings(
    tx: &mut Transaction<'_, Postgres>,
    route_id: Uuid,
    route_revision: i64,
    provider_id: &str,
    operation_id: &str,
    command_schema: &str,
    mappings: &[ValidatedRouteModelMapping],
    created_at_ms: i64,
) -> Result<(), ImageGatewayError> {
    for mapping in mappings {
        sqlx::query(
            r#"
            INSERT INTO provider_route_model_mappings
              (route_id, route_revision, provider_id, operation_id, command_schema,
               api_profile, public_model_id, provider_model_id, execution_model_id,
               media_kind, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(route_id)
        .bind(route_revision)
        .bind(provider_id)
        .bind(operation_id)
        .bind(command_schema)
        .bind(&mapping.api_profile)
        .bind(&mapping.public_model_id)
        .bind(&mapping.provider_model_id)
        .bind(&mapping.execution_model_id)
        .bind(&mapping.media_kind)
        .bind(created_at_ms)
        .execute(&mut **tx)
        .await
        .map_err(store_unavailable)?;
    }
    Ok(())
}

async fn insert_default_route_model_mappings(
    tx: &mut Transaction<'_, Postgres>,
    route_id: Uuid,
    route_revision: i64,
    created_at_ms: i64,
) -> Result<(), ImageGatewayError> {
    let (provider_id, operation_id, command_schema): (String, String, String) = sqlx::query_as(
        r#"
        SELECT provider_id, operation_id, command_schema
        FROM provider_routes WHERE route_id = $1 AND revision = $2
        "#,
    )
    .bind(route_id)
    .bind(route_revision)
    .fetch_one(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    let member_ids = sqlx::query_scalar(
        r#"
        SELECT provider_account_id FROM provider_route_members
        WHERE route_id = $1 AND route_revision = $2 AND state = 'enabled'
        "#,
    )
    .bind(route_id)
    .bind(route_revision)
    .fetch_all(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    let mappings =
        validated_route_model_mappings(tx, &provider_id, &operation_id, &member_ids, None).await?;
    insert_route_model_mappings(
        tx,
        route_id,
        route_revision,
        &provider_id,
        &operation_id,
        &command_schema,
        &mappings,
        created_at_ms,
    )
    .await
}

async fn insert_managed_account_route(
    tx: &mut Transaction<'_, Postgres>,
    provider_account_id: Uuid,
    execution_profile_id: Uuid,
    route_key: String,
    display_name: &str,
    created_at_ms: i64,
) -> Result<Uuid, ImageGatewayError> {
    let route_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_routes
          (route_id, revision, route_key, display_name, provider_id, operation_id,
           command_schema, route_kind, selection_strategy, state, created_at_ms)
        SELECT $1, 1, $2, $3, profile.provider_id, profile.operation_id,
               profile.command_schema, 'account', 'quota_aware_least_loaded',
               'enabled', $4
        FROM provider_execution_profiles profile
        WHERE profile.execution_profile_id = $5
          AND profile.provider_account_id = $6
          AND profile.state = 'enabled'
        "#,
    )
    .bind(route_id)
    .bind(route_key)
    .bind(display_name)
    .bind(created_at_ms)
    .bind(execution_profile_id)
    .bind(provider_account_id)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_heads
          (route_id, route_key, provider_id, operation_id, command_schema,
           route_kind, current_revision, state, created_at_ms, updated_at_ms)
        SELECT route_id, route_key, provider_id, operation_id, command_schema,
               route_kind, revision, 'enabled', created_at_ms, created_at_ms
        FROM provider_routes WHERE route_id = $1 AND revision = 1
        "#,
    )
    .bind(route_id)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_members
          (route_id, route_revision, provider_id, operation_id, command_schema,
           provider_account_id, execution_profile_id, priority, weight, state,
           created_at_ms)
        SELECT $1, 1, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, 0, 100, 'enabled', $2
        FROM provider_execution_profiles
        WHERE execution_profile_id = $3 AND provider_account_id = $4
        "#,
    )
    .bind(route_id)
    .bind(created_at_ms)
    .bind(execution_profile_id)
    .bind(provider_account_id)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    insert_default_route_model_mappings(tx, route_id, 1, created_at_ms).await?;
    Ok(route_id)
}

async fn reconcile_route_model_mappings(pool: &PgPool) -> Result<(), ImageGatewayError> {
    let mut tx = pool.begin().await.map_err(store_unavailable)?;
    let routes: Vec<(Uuid, i64, i64)> = sqlx::query_as(
        r#"
        SELECT route.route_id, route.revision, route.created_at_ms
        FROM provider_routes route
        WHERE NOT EXISTS (
          SELECT 1 FROM provider_route_model_mappings mapping
          WHERE mapping.route_id = route.route_id
            AND mapping.route_revision = route.revision
        )
        ORDER BY route.route_id, route.revision
        FOR SHARE OF route
        "#,
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(store_unavailable)?;
    for (route_id, revision, created_at_ms) in routes {
        insert_default_route_model_mappings(&mut tx, route_id, revision, created_at_ms).await?;
    }
    tx.commit().await.map_err(store_unavailable)
}

fn route_model_mapping_view(mapping: ValidatedRouteModelMapping) -> ProviderRouteModelMappingView {
    ProviderRouteModelMappingView {
        api_profile: mapping.api_profile,
        public_model_id: mapping.public_model_id,
        provider_model_id: mapping.provider_model_id,
        execution_model_id: mapping.execution_model_id,
        provider_model_display_name: mapping.provider_model_display_name,
        media_kind: mapping.media_kind,
    }
}

fn default_model_aliases(
    provider_id: &str,
    operation_id: &str,
    model_id: &str,
) -> Vec<(String, String)> {
    let mut aliases = Vec::with_capacity(2);
    let native_profile = match (provider_id, operation_id) {
        ("openai-codex", "images.generations" | "images.edits") => "openai-images-v1",
        ("grok-cli", "images.generations" | "images.edits") => "xai-images-v1",
        ("grok-cli", "videos.generations") => "xai-videos-v1",
        ("dreamina-cli", "images.generations") => "dreamina-cli-images-v1",
        ("dreamina-cli", "videos.generations") => "dreamina-cli-videos-v1",
        _ => return aliases,
    };
    aliases.push((native_profile.to_owned(), model_id.to_owned()));
    if provider_id == "dreamina-cli" {
        let ark = match (operation_id, model_id) {
            ("images.generations", "5.0") => {
                Some(("volcengine-ark-images-v3", "doubao-seedream-5-0-lite"))
            }
            ("images.generations", "5.0Pro") => {
                Some(("volcengine-ark-images-v3", "doubao-seedream-5-0-260128"))
            }
            ("videos.generations", "seedance2.0") => Some((
                "volcengine-ark-content-generation-v3",
                "doubao-seedance-2-0-260128",
            )),
            ("videos.generations", "seedance2.0fast") => Some((
                "volcengine-ark-content-generation-v3",
                "doubao-seedance-2-0-fast-260128",
            )),
            ("videos.generations", "seedance2.0mini") => Some((
                "volcengine-ark-content-generation-v3",
                "doubao-seedance-2-0-mini-260128",
            )),
            _ => None,
        };
        if let Some((profile, public_model_id)) = ark {
            aliases.push((profile.to_owned(), public_model_id.to_owned()));
        }
    }
    aliases
}

fn supported_api_profile(provider_id: &str, operation_id: &str, api_profile: &str) -> bool {
    matches!(
        (provider_id, operation_id, api_profile),
        (
            "openai-codex",
            "images.generations" | "images.edits",
            "openai-images-v1"
        ) | (
            "grok-cli",
            "images.generations" | "images.edits",
            "xai-images-v1"
        ) | ("grok-cli", "videos.generations", "xai-videos-v1")
            | (
                "dreamina-cli",
                "images.generations",
                "dreamina-cli-images-v1" | "volcengine-ark-images-v3"
            )
            | (
                "dreamina-cli",
                "videos.generations",
                "dreamina-cli-videos-v1" | "volcengine-ark-content-generation-v3"
            )
    )
}

fn valid_public_model_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
}

fn invalid_route_models() -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        "Provider route model mappings are invalid",
        Some("model_mappings".to_owned()),
        "invalid_provider_route_models",
    )
}

fn validate_route_request(request: &CreateProviderRouteRequest) -> Result<(), ImageGatewayError> {
    let unique_members = request
        .members
        .iter()
        .map(|member| member.provider_account_id)
        .collect::<HashSet<_>>();
    if !valid_simple_key(&request.route_key)
        || request.display_name.trim().is_empty()
        || request.display_name.chars().count() > MAX_DISPLAY_NAME_CHARS
        || !valid_simple_identifier(&request.provider_id)
        || request.operation_id.is_empty()
        || request.operation_id.len() > 128
        || !matches!(
            request.selection_strategy.as_str(),
            "quota_aware_least_loaded" | "priority_weighted"
        )
        || !(60_000..=86_400_000).contains(&request.quota_freshness_ms)
        || !matches!(request.unknown_quota_policy.as_str(), "allow" | "block")
        || request.members.is_empty()
        || request.members.len() > 100
        || unique_members.len() != request.members.len()
        || request.members.iter().any(|member| {
            !(-1000..=1000).contains(&member.priority)
                || !(1..=1_000_000).contains(&member.weight)
                || !(0..=100).contains(&member.minimum_remaining_percent)
        })
    {
        return Err(ImageGatewayError::invalid_request(
            "Provider route configuration is invalid",
            Some("route_key".to_string()),
            "invalid_provider_route",
        ));
    }
    Ok(())
}

async fn update_account_route_model_mappings_in_transaction(
    tx: &mut Transaction<'_, Postgres>,
    provider_account_id: Uuid,
    route_id: Uuid,
    expected_revision: i64,
    requested_mappings: &[ProviderRouteModelMappingRequest],
) -> Result<i64, ImageGatewayError> {
    if expected_revision <= 0 {
        return Err(ImageGatewayError::invalid_request(
            "Provider route revision is invalid",
            Some("expected_route_revision".to_owned()),
            "invalid_provider_route",
        ));
    }
    let current = sqlx::query_as::<_, RouteRow>(
        r#"
        SELECT route.route_id, route.revision, route.route_key, route.display_name,
               route.provider_id, route.operation_id, route.command_schema,
               route.route_kind, route.selection_strategy,
               route.quota_freshness_ms, route.unknown_quota_policy,
               head.state, route.created_at_ms
        FROM provider_route_heads head
        JOIN provider_routes route
          ON route.route_id = head.route_id
         AND route.revision = head.current_revision
         AND route.provider_id = head.provider_id
         AND route.operation_id = head.operation_id
         AND route.command_schema = head.command_schema
        WHERE head.route_id = $1 AND head.state = 'enabled'
        FOR UPDATE OF head
        "#,
    )
    .bind(route_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(store_unavailable)?
    .ok_or_else(|| {
        ImageGatewayError::not_found(
            "Provider route not found",
            Some("route_id".to_owned()),
            "provider_route_not_found",
        )
    })?;
    if current.revision != expected_revision {
        return Err(ImageGatewayError::conflict(
            "Provider route changed since it was loaded",
            Some("expected_route_revision".to_owned()),
            "provider_route_revision_conflict",
        ));
    }
    if current.route_kind != "account" {
        return Err(ImageGatewayError::invalid_request(
            "Account model configuration requires a single-account route",
            Some("route_id".to_owned()),
            "provider_account_route_required",
        ));
    }
    let member_ids: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT provider_account_id
        FROM provider_route_members
        WHERE route_id = $1 AND route_revision = $2 AND state = 'enabled'
        ORDER BY provider_account_id
        FOR SHARE
        "#,
    )
    .bind(route_id)
    .bind(current.revision)
    .fetch_all(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    if member_ids.as_slice() != [provider_account_id] {
        return Err(ImageGatewayError::invalid_request(
            "Provider route does not belong to this account",
            Some("route_id".to_owned()),
            "provider_account_route_mismatch",
        ));
    }
    let model_mappings = validated_route_model_mappings(
        tx,
        &current.provider_id,
        &current.operation_id,
        &member_ids,
        Some(requested_mappings),
    )
    .await?;
    let revision = current.revision.checked_add(1).ok_or_else(|| {
        ImageGatewayError::service_unavailable("provider route revision exhausted")
    })?;
    let now = database_now(tx).await?;
    sqlx::query(
        r#"
        INSERT INTO provider_routes
          (route_id, revision, route_key, display_name, provider_id, operation_id,
           command_schema, route_kind, selection_strategy, quota_freshness_ms,
           unknown_quota_policy, state, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'account', $8, $9, $10,
                'enabled', $11)
        "#,
    )
    .bind(route_id)
    .bind(revision)
    .bind(&current.route_key)
    .bind(&current.display_name)
    .bind(&current.provider_id)
    .bind(&current.operation_id)
    .bind(&current.command_schema)
    .bind(&current.selection_strategy)
    .bind(current.quota_freshness_ms)
    .bind(&current.unknown_quota_policy)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_route_insert)?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_members
          (route_id, route_revision, provider_id, operation_id, command_schema,
           provider_account_id, execution_profile_id, priority, weight, state,
           minimum_remaining_percent, created_at_ms)
        SELECT route_id, $3, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, priority, weight, state,
               minimum_remaining_percent, $4
        FROM provider_route_members
        WHERE route_id = $1 AND route_revision = $2 AND state = 'enabled'
        "#,
    )
    .bind(route_id)
    .bind(current.revision)
    .bind(revision)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    insert_route_model_mappings(
        tx,
        route_id,
        revision,
        &current.provider_id,
        &current.operation_id,
        &current.command_schema,
        &model_mappings,
        now,
    )
    .await?;
    let updated = sqlx::query(
        r#"
        UPDATE provider_route_heads
        SET current_revision = $2, updated_at_ms = $3
        WHERE route_id = $1 AND current_revision = $4
        "#,
    )
    .bind(route_id)
    .bind(revision)
    .bind(now)
    .bind(current.revision)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    if updated.rows_affected() != 1 {
        return Err(ImageGatewayError::conflict(
            "Provider route changed since it was loaded",
            Some("expected_route_revision".to_owned()),
            "provider_route_revision_conflict",
        ));
    }
    Ok(revision)
}

fn validate_route_update_request(
    request: &UpdateProviderRouteRequest,
) -> Result<(), ImageGatewayError> {
    let unique_members = request
        .members
        .iter()
        .map(|member| member.provider_account_id)
        .collect::<HashSet<_>>();
    if request.expected_revision <= 0
        || request.display_name.trim().is_empty()
        || request.display_name.chars().count() > MAX_DISPLAY_NAME_CHARS
        || !matches!(
            request.selection_strategy.as_str(),
            "quota_aware_least_loaded" | "priority_weighted"
        )
        || !(60_000..=86_400_000).contains(&request.quota_freshness_ms)
        || !matches!(request.unknown_quota_policy.as_str(), "allow" | "block")
        || request.members.is_empty()
        || request.members.len() > 100
        || unique_members.len() != request.members.len()
        || request.members.iter().any(|member| {
            !(-1000..=1000).contains(&member.priority)
                || !(1..=1_000_000).contains(&member.weight)
                || !(0..=100).contains(&member.minimum_remaining_percent)
        })
    {
        return Err(ImageGatewayError::invalid_request(
            "Provider route configuration is invalid",
            Some("expected_revision".to_string()),
            "invalid_provider_route",
        ));
    }
    Ok(())
}

fn validate_external_id(value: &str, param: &str) -> Result<(), ImageGatewayError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(ImageGatewayError::invalid_request(
            format!("{param} is invalid"),
            Some(param.to_string()),
            "invalid_identifier",
        ));
    }
    Ok(())
}

fn valid_simple_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ACCOUNT_KEY_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn valid_simple_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn create_private_directory(path: &Path) -> Result<(), ImageGatewayError> {
    fs::create_dir_all(path)
        .map_err(|_| ImageGatewayError::config("provider home root is unavailable"))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|_| ImageGatewayError::config("provider home root is unavailable"))?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| ImageGatewayError::config("provider home root is unavailable"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(ImageGatewayError::config(
            "provider home root must be a private directory",
        ));
    }
    Ok(())
}

fn resolve_optional_executable(
    primary_env: &str,
    fallback_env: &str,
    default_name: &str,
) -> Result<Option<PathBuf>, ImageGatewayError> {
    let configured = std::env::var_os(primary_env).or_else(|| std::env::var_os(fallback_env));
    let explicitly_configured = configured.is_some();
    let candidate = configured
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default_name));
    match resolve_executable(candidate) {
        Ok(path) => Ok(Some(path)),
        Err(_) if !explicitly_configured => Ok(None),
        Err(_) => Err(ImageGatewayError::config(format!(
            "{primary_env} executable is unavailable"
        ))),
    }
}

fn upstream_identity_sha256(home: &Path) -> Result<String, ImageGatewayError> {
    let bytes = fs::read(home.join("auth.json")).map_err(|_| {
        ImageGatewayError::service_unavailable("Codex account identity unavailable")
    })?;
    if bytes.is_empty() || bytes.len() > MAX_AUTH_BYTES {
        return Err(ImageGatewayError::service_unavailable(
            "Codex account identity unavailable",
        ));
    }
    let auth: Value = serde_json::from_slice(&bytes).map_err(|_| {
        ImageGatewayError::service_unavailable("Codex account identity unavailable")
    })?;
    let account_id = auth
        .pointer("/tokens/account_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ImageGatewayError::service_unavailable("Codex account identity unavailable")
        })?;
    Ok(hex::encode(Sha256::digest(account_id.as_bytes())))
}

fn codex_access_expires_at_ms(home: &Path) -> Option<i64> {
    let bytes = fs::read(home.join("auth.json")).ok()?;
    if bytes.is_empty() || bytes.len() > MAX_AUTH_BYTES {
        return None;
    }
    let auth: Value = serde_json::from_slice(&bytes).ok()?;
    let token = auth.pointer("/tokens/access_token")?.as_str()?;
    let payload = token.split('.').nth(1)?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let claims: Value = serde_json::from_slice(&decoded).ok()?;
    claims.get("exp")?.as_i64()?.checked_mul(1_000)
}

struct GrokIdentity {
    upstream_identity_sha256: String,
    email: Option<String>,
}

fn grok_identity(home: &Path) -> Result<GrokIdentity, ImageGatewayError> {
    let bytes = fs::read(home.join("auth.json"))
        .map_err(|_| ImageGatewayError::service_unavailable("Grok account identity unavailable"))?;
    if bytes.is_empty() || bytes.len() > MAX_AUTH_BYTES {
        return Err(ImageGatewayError::service_unavailable(
            "Grok account identity unavailable",
        ));
    }
    let auth: Value = serde_json::from_slice(&bytes)
        .map_err(|_| ImageGatewayError::service_unavailable("Grok account identity unavailable"))?;
    let entries = auth.as_object().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Grok account identity unavailable")
    })?;
    let (issuer, account) = entries
        .iter()
        .find_map(|(issuer, value)| {
            let account = value.as_object()?;
            let principal = account
                .get("principal_id")
                .or_else(|| account.get("user_id"))?
                .as_str()?;
            (!principal.is_empty()
                && (issuer.starts_with("https://auth.x.ai")
                    || issuer.starts_with("https://accounts.x.ai")))
            .then_some((issuer.as_str(), account))
        })
        .ok_or_else(|| {
            ImageGatewayError::service_unavailable("Grok account identity unavailable")
        })?;
    let principal = account
        .get("principal_id")
        .or_else(|| account.get("user_id"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let team_id = account
        .get("team_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut digest = Sha256::new();
    digest.update(b"ai-image-factory/grok-account-identity/v1\0");
    digest.update(issuer.as_bytes());
    digest.update(b"\0");
    digest.update(principal.as_bytes());
    digest.update(b"\0");
    digest.update(team_id.as_bytes());
    let email = account
        .get("email")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 320)
        .map(str::to_owned);
    Ok(GrokIdentity {
        upstream_identity_sha256: hex::encode(digest.finalize()),
        email,
    })
}

fn grok_access_expires_at_ms(home: &Path) -> Option<i64> {
    let bytes = fs::read(home.join("auth.json")).ok()?;
    if bytes.is_empty() || bytes.len() > MAX_AUTH_BYTES {
        return None;
    }
    let auth: Value = serde_json::from_slice(&bytes).ok()?;
    let expires_at = auth
        .as_object()?
        .values()
        .filter_map(Value::as_object)
        .find_map(|entry| entry.get("expires_at").and_then(Value::as_str))?;
    let timestamp = OffsetDateTime::parse(expires_at, &Rfc3339).ok()?;
    i64::try_from(timestamp.unix_timestamp_nanos() / 1_000_000).ok()
}

fn credential_fingerprint(provider_id: &str, home: &Path) -> Result<String, ImageGatewayError> {
    match provider_id {
        openai_codex::PROVIDER_ID => codex_auth_file_sha256(home),
        GROK_PROVIDER_ID => grok_auth_file_sha256(home),
        _ => Err(ImageGatewayError::service_unavailable(
            "Provider credential refresh is unsupported",
        )),
    }
}

fn credential_identity(provider_id: &str, home: &Path) -> Result<String, ImageGatewayError> {
    match provider_id {
        openai_codex::PROVIDER_ID => upstream_identity_sha256(home),
        GROK_PROVIDER_ID => grok_identity(home).map(|identity| identity.upstream_identity_sha256),
        _ => Err(ImageGatewayError::service_unavailable(
            "Provider credential refresh is unsupported",
        )),
    }
}

fn credential_expires_at_ms(provider_id: &str, home: &Path) -> Option<i64> {
    match provider_id {
        openai_codex::PROVIDER_ID => codex_access_expires_at_ms(home),
        GROK_PROVIDER_ID => grok_access_expires_at_ms(home),
        _ => None,
    }
}

struct AuthFileReplacement {
    destination: PathBuf,
    original: Vec<u8>,
}

impl AuthFileReplacement {
    fn install(
        source_home: &Path,
        destination_home: &Path,
        login_session_id: Uuid,
    ) -> Result<Self, ImageGatewayError> {
        validate_private_home(source_home)?;
        validate_private_home(destination_home)?;
        let source = source_home.join("auth.json");
        let destination = destination_home.join("auth.json");
        validate_regular_auth_file(&source)?;
        validate_regular_auth_file(&destination)?;
        let fresh = read_bounded_auth(&source)?;
        let original = read_bounded_auth(&destination)?;
        atomic_write_auth(destination_home, &destination, &fresh, login_session_id)?;
        Ok(Self {
            destination,
            original,
        })
    }

    fn rollback(self) -> Result<(), ImageGatewayError> {
        let home = self.destination.parent().ok_or_else(|| {
            ImageGatewayError::internal("Provider credential destination is invalid")
        })?;
        atomic_write_auth(home, &self.destination, &self.original, Uuid::new_v4())
    }
}

fn validate_private_home(path: &Path) -> Result<(), ImageGatewayError> {
    if !path.is_absolute() {
        return Err(ImageGatewayError::service_unavailable(
            "Provider credential environment is invalid",
        ));
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        ImageGatewayError::service_unavailable("Provider credential environment is unavailable")
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(ImageGatewayError::service_unavailable(
            "Provider credential environment is invalid",
        ));
    }
    Ok(())
}

fn validate_regular_auth_file(path: &Path) -> Result<(), ImageGatewayError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        ImageGatewayError::service_unavailable("Provider credential material is unavailable")
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(ImageGatewayError::service_unavailable(
            "Provider credential material is invalid",
        ));
    }
    Ok(())
}

fn read_bounded_auth(path: &Path) -> Result<Vec<u8>, ImageGatewayError> {
    let bytes = fs::read(path).map_err(|_| {
        ImageGatewayError::service_unavailable("Provider credential material is unavailable")
    })?;
    if bytes.is_empty() || bytes.len() > MAX_AUTH_BYTES {
        return Err(ImageGatewayError::service_unavailable(
            "Provider credential material is invalid",
        ));
    }
    Ok(bytes)
}

fn atomic_write_auth(
    home: &Path,
    destination: &Path,
    bytes: &[u8],
    operation_id: Uuid,
) -> Result<(), ImageGatewayError> {
    let temporary = home.join(format!(".auth-{}.tmp", operation_id.simple()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|_| {
            ImageGatewayError::service_unavailable("Provider credential update is unavailable")
        })?;
    let write_result = (|| -> std::io::Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, destination)?;
        fs::File::open(home)?.sync_all()
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
        return Err(ImageGatewayError::service_unavailable(
            "Provider credential update is unavailable",
        ));
    }
    Ok(())
}

fn credential_refresh_deadline(access_expires_at_ms: Option<i64>, now_ms: i64) -> i64 {
    access_expires_at_ms
        .map(|expires| {
            expires
                .saturating_sub(CREDENTIAL_REFRESH_SKEW_MS)
                .max(now_ms)
        })
        .unwrap_or_else(|| now_ms.saturating_add(CREDENTIAL_REFRESH_INTERVAL_MS))
}

fn map_credential_store_error(error: CredentialResolveError) -> ImageGatewayError {
    match error {
        CredentialResolveError::Invalid => {
            ImageGatewayError::internal("Provider credential state is invalid")
        }
        CredentialResolveError::ReauthorizationRequired => ImageGatewayError::service_unavailable(
            "Provider account authorization expired; reauthorize the account",
        ),
        CredentialResolveError::Unsupported => {
            ImageGatewayError::service_unavailable("Provider credential refresh is unsupported")
        }
        CredentialResolveError::Unavailable => {
            ImageGatewayError::service_unavailable("Provider credential service is unavailable")
        }
    }
}

async fn insert_account_route(
    tx: &mut Transaction<'_, Postgres>,
    execution_profile_id: Uuid,
    display_name: &str,
    route_suffix: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let route_id = Uuid::new_v4();
    let route_key = format!("account.{}.{}", route_id.simple(), route_suffix);
    sqlx::query(
        r#"
        INSERT INTO provider_routes
          (route_id, revision, route_key, display_name, provider_id, operation_id,
           command_schema, route_kind, selection_strategy, state, created_at_ms)
        SELECT $1, 1, $2, $3, profile.provider_id, profile.operation_id,
               profile.command_schema, 'account', 'quota_aware_least_loaded',
               'enabled', $4
        FROM provider_execution_profiles profile
        WHERE profile.execution_profile_id = $5
        "#,
    )
    .bind(route_id)
    .bind(route_key)
    .bind(display_name.trim())
    .bind(now)
    .bind(execution_profile_id)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_heads
          (route_id, route_key, provider_id, operation_id, command_schema,
           route_kind, current_revision, state, created_at_ms, updated_at_ms)
        SELECT route_id, route_key, provider_id, operation_id, command_schema,
               route_kind, revision, 'enabled', created_at_ms, created_at_ms
        FROM provider_routes WHERE route_id = $1 AND revision = 1
        "#,
    )
    .bind(route_id)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_members
          (route_id, route_revision, provider_id, operation_id, command_schema,
           provider_account_id, execution_profile_id, priority, weight, state,
           created_at_ms)
        SELECT $1, 1, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, 0, 100, 'enabled', $2
        FROM provider_execution_profiles WHERE execution_profile_id = $3
        "#,
    )
    .bind(route_id)
    .bind(now)
    .bind(execution_profile_id)
    .execute(&mut **tx)
    .await
    .map_err(store_unavailable)?;
    insert_default_route_model_mappings(tx, route_id, 1, now).await?;
    Ok(())
}

fn map_profile_provisioning(error: crate::ExecutionProfileProvisioningError) -> ImageGatewayError {
    match error {
        crate::ExecutionProfileProvisioningError::InvalidInput => {
            ImageGatewayError::invalid_request(
                "Managed provider account configuration is invalid",
                Some("provider_id".to_owned()),
                "invalid_provider_account",
            )
        }
        crate::ExecutionProfileProvisioningError::Conflict => ImageGatewayError::invalid_request(
            "Managed provider account conflicts with existing configuration",
            Some("provider_id".to_owned()),
            "provider_account_conflict",
        ),
        crate::ExecutionProfileProvisioningError::Unavailable => {
            ImageGatewayError::service_unavailable("provider account provisioning unavailable")
        }
    }
}

async fn executable_version(executable: &Path) -> Option<String> {
    let output = timeout(
        Duration::from_secs(3),
        tokio::process::Command::new(executable)
            .arg("--version")
            .env("GROK_DISABLE_AUTOUPDATER", "1")
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    if !output.status.success() || output.stdout.len() > 1024 {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ImageGatewayError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(store_unavailable)
}

fn now_ms() -> Result<i64, ImageGatewayError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ImageGatewayError::internal("system clock is invalid"))?
        .as_millis();
    i64::try_from(millis).map_err(|_| ImageGatewayError::internal("system clock is invalid"))
}

fn now_seconds() -> Result<i64, ImageGatewayError> {
    Ok(now_ms()? / 1_000)
}

fn store_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("provider management state unavailable")
}

fn map_login_session_insert(error: sqlx::Error) -> ImageGatewayError {
    if let sqlx::Error::Database(database) = &error
        && database.code().as_deref() == Some("23505")
        && database
            .constraint()
            .is_some_and(|name| name == "provider_account_login_sessions_active_reauth_idx")
    {
        return ImageGatewayError::conflict(
            "This provider account already has an active reauthorization session",
            Some("provider_account_id".to_owned()),
            "provider_account_reauthorization_in_progress",
        );
    }
    store_unavailable(error)
}

fn map_identity_insert(error: sqlx::Error) -> ImageGatewayError {
    if let sqlx::Error::Database(database) = &error
        && database.code().as_deref() == Some("23505")
    {
        return ImageGatewayError::invalid_request(
            "This Codex account or account key is already managed",
            Some("account_key".to_string()),
            "duplicate_provider_account",
        );
    }
    store_unavailable(error)
}

fn map_route_insert(error: sqlx::Error) -> ImageGatewayError {
    if let sqlx::Error::Database(database) = &error
        && database.code().as_deref() == Some("23505")
    {
        return ImageGatewayError::invalid_request(
            "Provider route key already exists",
            Some("route_key".to_string()),
            "provider_route_conflict",
        );
    }
    store_unavailable(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_key_validation_rejects_path_material() {
        assert!(valid_simple_key("codex-team-a"));
        assert!(!valid_simple_key("../shared"));
        assert!(!valid_simple_key("account with spaces"));
    }

    #[test]
    fn managed_codex_account_key_is_internal_stable_and_valid() {
        let login_session_id =
            Uuid::parse_str("9be138ac-61c6-410f-81fd-496235ef5897").expect("valid fixture UUID");
        let key = managed_codex_account_key(login_session_id);
        assert_eq!(key, "codex-9be138ac61c6410f81fd496235ef5897");
        assert!(valid_simple_key(&key));
    }

    #[test]
    fn codex_quota_refresh_guard_is_single_flight_per_account() {
        let provider_account_id = Uuid::new_v4();
        let guard = CodexQuotaRefreshGuard::acquire(provider_account_id).expect("first refresh");
        let duplicate = CodexQuotaRefreshGuard::acquire(provider_account_id)
            .expect_err("duplicate refresh must be rejected");
        assert_eq!(duplicate.status_code(), axum::http::StatusCode::CONFLICT);
        assert_eq!(duplicate.error_code(), Some("quota_refresh_in_progress"));

        drop(guard);
        CodexQuotaRefreshGuard::acquire(provider_account_id).expect("guard released");
    }

    #[test]
    fn provider_login_operations_default_and_deduplicate_per_provider() {
        let mut request = StartProviderLoginRequest {
            provider_id: GROK_PROVIDER_ID.to_owned(),
            display_name: "Grok primary".to_owned(),
            operation_ids: vec![
                "videos.generations".to_owned(),
                "images.generations".to_owned(),
                "videos.generations".to_owned(),
            ],
            provider_account_id: None,
            login_method: CodexLoginMethod::DeviceCode,
            max_concurrency: 1,
        };

        normalize_provider_login_operations(&mut request).expect("supported operations");
        assert_eq!(
            request.operation_ids,
            ["images.edits", "images.generations", "videos.generations"]
        );

        request.operation_ids.clear();
        normalize_provider_login_operations(&mut request).expect("legacy default operations");
        assert_eq!(
            request.operation_ids,
            ["images.generations", "images.edits", "videos.generations"]
        );
    }

    #[test]
    fn provider_login_operations_reject_unsupported_codex_video() {
        let mut request = StartProviderLoginRequest {
            provider_id: openai_codex::PROVIDER_ID.to_owned(),
            display_name: "Codex primary".to_owned(),
            operation_ids: vec!["videos.generations".to_owned()],
            provider_account_id: None,
            login_method: CodexLoginMethod::BrowserOauth,
            max_concurrency: 1,
        };

        let error = normalize_provider_login_operations(&mut request)
            .expect_err("Codex video must be rejected");
        assert_eq!(error.error_code(), Some("unsupported_provider_operation"));
    }

    #[test]
    fn reauthorization_auth_replacement_can_roll_back() {
        let root = tempfile::tempdir().expect("temporary root");
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        create_private_directory(&source).expect("source home");
        create_private_directory(&destination).expect("destination home");
        fs::write(source.join("auth.json"), br#"{"token":"fresh"}"#).expect("fresh auth");
        fs::write(destination.join("auth.json"), br#"{"token":"original"}"#)
            .expect("original auth");
        fs::set_permissions(source.join("auth.json"), fs::Permissions::from_mode(0o600))
            .expect("fresh permissions");
        fs::set_permissions(
            destination.join("auth.json"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("original permissions");

        let replacement = AuthFileReplacement::install(
            &source,
            &destination,
            Uuid::parse_str("f675ce2e-2fee-46f4-a913-72a7e50f95ef").expect("fixture UUID"),
        )
        .expect("install replacement");
        assert_eq!(
            fs::read(destination.join("auth.json")).expect("installed auth"),
            br#"{"token":"fresh"}"#
        );
        replacement.rollback().expect("rollback replacement");
        assert_eq!(
            fs::read(destination.join("auth.json")).expect("restored auth"),
            br#"{"token":"original"}"#
        );
    }
}
