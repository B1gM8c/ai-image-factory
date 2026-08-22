use std::{
    env, fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ImageGatewayError;

const RECENT_COMMAND_LIMIT: i64 = 20;
const MAX_RELEASE_METADATA_BYTES: u64 = 16 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SystemUpdateActor {
    pub user_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SystemUpdateAction {
    Check,
    Apply,
}

impl SystemUpdateAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Check => "check",
            Self::Apply => "apply",
        }
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplySystemUpdateRequest {
    pub target_version: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct SystemUpdateCommandView {
    pub object: &'static str,
    pub command_id: String,
    pub action: String,
    pub target_version: Option<String>,
    pub status: String,
    pub phase: String,
    pub progress: serde_json::Value,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
    pub requested_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct SystemUpdateSnapshot {
    pub object: &'static str,
    pub configured: bool,
    pub apply_enabled: bool,
    pub repository: Option<String>,
    pub target_triple: String,
    pub current_version: String,
    pub current_commit_sha: Option<String>,
    pub previous_version: Option<String>,
    pub latest_version: Option<String>,
    pub latest_commit_sha: Option<String>,
    pub latest_verified: bool,
    pub update_available: bool,
    pub last_checked_at_ms: Option<i64>,
    pub last_applied_at_ms: Option<i64>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub active_command: Option<SystemUpdateCommandView>,
    pub recent_commands: Vec<SystemUpdateCommandView>,
}

#[async_trait]
pub trait SystemUpdateService: Send + Sync + 'static {
    async fn snapshot(&self) -> Result<SystemUpdateSnapshot, ImageGatewayError>;

    async fn enqueue(
        &self,
        actor: SystemUpdateActor,
        idempotency_key: &str,
        action: SystemUpdateAction,
        target_version: Option<String>,
    ) -> Result<SystemUpdateCommandView, ImageGatewayError>;
}

#[derive(Clone, Debug)]
struct SystemUpdateConfiguration {
    repository: Option<String>,
    apply_enabled: bool,
    target_triple: String,
    current_version: String,
    current_commit_sha: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseIdentity {
    schema_version: u32,
    release_version: String,
    commit_sha: String,
    target_triple: String,
}

impl ReleaseIdentity {
    fn from_path(path: &Path) -> Result<Self, ImageGatewayError> {
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            ImageGatewayError::config(format!("AIF_RELEASE_METADATA_PATH cannot be read: {error}"))
        })?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > MAX_RELEASE_METADATA_BYTES
        {
            return Err(ImageGatewayError::config(
                "AIF_RELEASE_METADATA_PATH must be a bounded regular file",
            ));
        }
        let bytes = fs::read(path).map_err(|error| {
            ImageGatewayError::config(format!("AIF_RELEASE_METADATA_PATH cannot be read: {error}"))
        })?;
        Self::from_slice(&bytes)
    }

    fn from_slice(bytes: &[u8]) -> Result<Self, ImageGatewayError> {
        let identity: Self = serde_json::from_slice(bytes).map_err(|error| {
            ImageGatewayError::config(format!("release metadata is invalid: {error}"))
        })?;
        if identity.schema_version != 1 {
            return Err(ImageGatewayError::config(
                "release metadata schema_version is unsupported",
            ));
        }
        validate_version(&identity.release_version)?;
        validate_commit_sha(&identity.commit_sha)?;
        validate_token(
            &identity.target_triple,
            "release metadata target_triple",
            100,
        )?;
        Ok(identity)
    }
}

impl SystemUpdateConfiguration {
    fn from_env() -> Result<Self, ImageGatewayError> {
        let repository = optional_env("AIF_UPDATE_GITHUB_REPOSITORY")
            .map(validate_repository)
            .transpose()?;
        let apply_enabled = boolean_env("AIF_UPDATE_APPLY_ENABLED", false)?;
        let (target_triple, current_version, current_commit_sha) =
            if let Some(path) = optional_env("AIF_RELEASE_METADATA_PATH") {
                let identity = ReleaseIdentity::from_path(Path::new(&path))?;
                (
                    identity.target_triple,
                    identity.release_version,
                    Some(identity.commit_sha),
                )
            } else {
                let target_triple =
                    optional_env("AIF_RELEASE_TARGET").unwrap_or_else(default_target_triple);
                validate_token(&target_triple, "AIF_RELEASE_TARGET", 100)?;
                let current_version = optional_env("AIF_RELEASE_VERSION")
                    .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
                validate_version(&current_version)?;
                let current_commit_sha = optional_env("AIF_RELEASE_COMMIT");
                if let Some(commit) = &current_commit_sha {
                    validate_commit_sha(commit)?;
                }
                (target_triple, current_version, current_commit_sha)
            };
        Ok(Self {
            repository,
            apply_enabled,
            target_triple,
            current_version,
            current_commit_sha,
        })
    }
}

#[derive(Clone)]
pub struct PostgresSystemUpdateService {
    pool: PgPool,
    configuration: SystemUpdateConfiguration,
}

impl PostgresSystemUpdateService {
    pub fn from_env(pool: PgPool) -> Result<Self, ImageGatewayError> {
        Ok(Self {
            pool,
            configuration: SystemUpdateConfiguration::from_env()?,
        })
    }

    async fn ensure_release_state(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        now_ms: i64,
    ) -> Result<(), ImageGatewayError> {
        sqlx::query(
            r#"
            INSERT INTO platform_release_state(
                singleton, repository, target_triple, current_version,
                current_commit_sha, updated_at_ms
            )
            VALUES (TRUE, $1, $2, $3, $4, $5)
            ON CONFLICT (singleton) DO UPDATE
            SET latest_version = CASE
                    WHEN platform_release_state.repository IS NOT DISTINCT FROM EXCLUDED.repository
                     AND platform_release_state.target_triple = EXCLUDED.target_triple
                     AND platform_release_state.current_version = EXCLUDED.current_version
                     AND platform_release_state.current_commit_sha IS NOT DISTINCT FROM EXCLUDED.current_commit_sha
                    THEN platform_release_state.latest_version
                    ELSE NULL
                END,
                latest_commit_sha = CASE
                    WHEN platform_release_state.repository IS NOT DISTINCT FROM EXCLUDED.repository
                     AND platform_release_state.target_triple = EXCLUDED.target_triple
                     AND platform_release_state.current_version = EXCLUDED.current_version
                     AND platform_release_state.current_commit_sha IS NOT DISTINCT FROM EXCLUDED.current_commit_sha
                    THEN platform_release_state.latest_commit_sha
                    ELSE NULL
                END,
                latest_verified = CASE
                    WHEN platform_release_state.repository IS NOT DISTINCT FROM EXCLUDED.repository
                     AND platform_release_state.target_triple = EXCLUDED.target_triple
                     AND platform_release_state.current_version = EXCLUDED.current_version
                     AND platform_release_state.current_commit_sha IS NOT DISTINCT FROM EXCLUDED.current_commit_sha
                    THEN platform_release_state.latest_verified
                    ELSE FALSE
                END,
                repository = EXCLUDED.repository,
                target_triple = EXCLUDED.target_triple,
                current_version = EXCLUDED.current_version,
                current_commit_sha = EXCLUDED.current_commit_sha,
                updated_at_ms = GREATEST(
                    platform_release_state.updated_at_ms,
                    EXCLUDED.updated_at_ms
                )
            "#,
        )
        .bind(&self.configuration.repository)
        .bind(&self.configuration.target_triple)
        .bind(&self.configuration.current_version)
        .bind(&self.configuration.current_commit_sha)
        .bind(now_ms)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
        Ok(())
    }
}

#[async_trait]
impl SystemUpdateService for PostgresSystemUpdateService {
    async fn snapshot(&self) -> Result<SystemUpdateSnapshot, ImageGatewayError> {
        let now_ms = now_ms()?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        self.ensure_release_state(&mut tx, now_ms).await?;
        let state = sqlx::query_as::<_, ReleaseStateRow>(
            r#"
            SELECT repository, target_triple, current_version,
                   current_commit_sha, previous_version, latest_version,
                   latest_commit_sha, latest_verified, last_checked_at_ms,
                   last_applied_at_ms, last_error_code, last_error_message
            FROM platform_release_state
            WHERE singleton = TRUE
            "#,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        let commands = sqlx::query_as::<_, CommandRow>(
            r#"
            SELECT command_id, action, target_version, status, phase,
                   progress, failure_code, failure_message,
                   requested_at_ms, started_at_ms, completed_at_ms, updated_at_ms
            FROM platform_update_commands
            ORDER BY requested_at_ms DESC, command_id DESC
            LIMIT $1
            "#,
        )
        .bind(RECENT_COMMAND_LIMIT)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;

        let recent_commands: Vec<_> = commands.into_iter().map(CommandRow::into_view).collect();
        let active_command = recent_commands
            .iter()
            .find(|command| {
                matches!(
                    command.status.as_str(),
                    "queued" | "running" | "restoring" | "restore_required"
                )
            })
            .cloned();
        let update_available = verified_update_available(
            state.latest_verified,
            state.latest_version.as_deref(),
            state.latest_commit_sha.as_deref(),
            &state.current_version,
            state.current_commit_sha.as_deref(),
        );
        Ok(SystemUpdateSnapshot {
            object: "system.update",
            configured: state.repository.is_some(),
            apply_enabled: self.configuration.apply_enabled,
            repository: state.repository,
            target_triple: state.target_triple,
            current_version: state.current_version,
            current_commit_sha: state.current_commit_sha,
            previous_version: state.previous_version,
            latest_version: state.latest_version,
            latest_commit_sha: state.latest_commit_sha,
            latest_verified: state.latest_verified,
            update_available,
            last_checked_at_ms: state.last_checked_at_ms,
            last_applied_at_ms: state.last_applied_at_ms,
            last_error_code: state.last_error_code,
            last_error_message: state.last_error_message,
            active_command,
            recent_commands,
        })
    }

    async fn enqueue(
        &self,
        actor: SystemUpdateActor,
        idempotency_key: &str,
        action: SystemUpdateAction,
        target_version: Option<String>,
    ) -> Result<SystemUpdateCommandView, ImageGatewayError> {
        validate_idempotency_key(idempotency_key)?;
        let target_version = normalize_target(action, target_version)?;
        let request_digest = request_digest(action, target_version.as_deref());
        let idempotency_key_digest = idempotency_digest(actor.user_id, idempotency_key);
        let now_ms = now_ms()?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        self.ensure_release_state(&mut tx, now_ms).await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('platform-system-update', 0))")
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;

        if let Some(existing) = command_by_idempotency(&mut tx, &idempotency_key_digest).await? {
            if existing.request_digest != request_digest {
                return Err(ImageGatewayError::idempotency_conflict());
            }
            tx.commit().await.map_err(unavailable)?;
            return Ok(existing.into_view());
        }
        if self.configuration.repository.is_none() {
            return Err(ImageGatewayError::service_unavailable(
                "System updates are not configured",
            ));
        }
        if action == SystemUpdateAction::Apply && !self.configuration.apply_enabled {
            return Err(ImageGatewayError::conflict(
                "Automatic system updates are disabled",
                None,
                "system_update_apply_disabled",
            ));
        }
        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM platform_update_commands
                WHERE status = 'restore_required'
            )
            "#,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?
        {
            return Err(ImageGatewayError::conflict(
                "A previous system update requires recovery",
                None,
                "system_update_recovery_required",
            ));
        }
        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM platform_update_commands
                WHERE status IN ('queued', 'running', 'restoring')
            )
            "#,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?
        {
            return Err(ImageGatewayError::conflict(
                "Another system update command is already active",
                None,
                "system_update_in_progress",
            ));
        }

        if action == SystemUpdateAction::Apply {
            let expected = sqlx::query_as::<_, LatestReleaseRow>(
                r#"
                SELECT current_version, current_commit_sha,
                       latest_version, latest_commit_sha, latest_verified
                FROM platform_release_state
                WHERE singleton = TRUE
                "#,
            )
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
            if expected.latest_version.as_deref() != target_version.as_deref()
                || !verified_update_available(
                    expected.latest_verified,
                    expected.latest_version.as_deref(),
                    expected.latest_commit_sha.as_deref(),
                    &expected.current_version,
                    expected.current_commit_sha.as_deref(),
                )
            {
                return Err(ImageGatewayError::conflict(
                    "The requested release is not the latest verified release",
                    Some("target_version".to_string()),
                    "system_update_release_not_verified",
                ));
            }
        }

        let command_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO platform_update_commands(
                command_id, action, target_version, status, phase,
                idempotency_key_digest, request_digest,
                requested_by_user_id, requested_by_session_id,
                requested_at_ms, updated_at_ms
            )
            VALUES (
                $1, $2, $3, 'queued', 'queued',
                $4, $5, $6, $7, $8, $8
            )
            "#,
        )
        .bind(command_id)
        .bind(action.as_str())
        .bind(&target_version)
        .bind(&idempotency_key_digest)
        .bind(&request_digest)
        .bind(actor.user_id)
        .bind(actor.session_id)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            r#"
            INSERT INTO platform_update_events(
                event_id, command_id, phase, outcome, details, created_at_ms
            )
            VALUES ($1, $2, 'queued', 'info', $3, $4)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(command_id)
        .bind(json!({ "action": action.as_str(), "target_version": target_version }))
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            r#"
            INSERT INTO identity_audit_events(
                event_id, actor_user_id, session_id, action,
                resource_type, resource_id, outcome, metadata, created_at_ms
            )
            VALUES (
                $1, $2, $3, $4, 'system_update', $5,
                'success', $6, $7
            )
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(actor.user_id)
        .bind(actor.session_id)
        .bind(format!("system.update.{}", action.as_str()))
        .bind(command_id.to_string())
        .bind(json!({ "target_version": target_version }))
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        let command = command_by_id(&mut tx, command_id).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(command.into_view())
    }
}

#[derive(Debug, FromRow)]
struct ReleaseStateRow {
    repository: Option<String>,
    target_triple: String,
    current_version: String,
    current_commit_sha: Option<String>,
    previous_version: Option<String>,
    latest_version: Option<String>,
    latest_commit_sha: Option<String>,
    latest_verified: bool,
    last_checked_at_ms: Option<i64>,
    last_applied_at_ms: Option<i64>,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
}

#[derive(Clone, Debug, FromRow)]
struct CommandRow {
    command_id: Uuid,
    action: String,
    target_version: Option<String>,
    status: String,
    phase: String,
    progress: serde_json::Value,
    failure_code: Option<String>,
    failure_message: Option<String>,
    requested_at_ms: i64,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
    updated_at_ms: i64,
}

impl CommandRow {
    fn into_view(self) -> SystemUpdateCommandView {
        SystemUpdateCommandView {
            object: "system.update_command",
            command_id: self.command_id.to_string(),
            action: self.action,
            target_version: self.target_version,
            status: self.status,
            phase: self.phase,
            progress: self.progress,
            failure_code: self.failure_code,
            failure_message: self.failure_message,
            requested_at_ms: self.requested_at_ms,
            started_at_ms: self.started_at_ms,
            completed_at_ms: self.completed_at_ms,
            updated_at_ms: self.updated_at_ms,
        }
    }
}

#[derive(Debug, FromRow)]
struct IdempotentCommandRow {
    #[sqlx(flatten)]
    command: CommandRow,
    request_digest: String,
}

impl IdempotentCommandRow {
    fn into_view(self) -> SystemUpdateCommandView {
        self.command.into_view()
    }
}

#[derive(Debug, FromRow)]
struct LatestReleaseRow {
    current_version: String,
    current_commit_sha: Option<String>,
    latest_version: Option<String>,
    latest_commit_sha: Option<String>,
    latest_verified: bool,
}

fn verified_update_available(
    latest_verified: bool,
    latest_version: Option<&str>,
    latest_commit_sha: Option<&str>,
    current_version: &str,
    current_commit_sha: Option<&str>,
) -> bool {
    if !latest_verified || latest_version.is_none_or(|version| version == current_version) {
        return false;
    }
    !matches!(
        (latest_commit_sha, current_commit_sha),
        (Some(latest), Some(current)) if latest.eq_ignore_ascii_case(current)
    )
}

async fn command_by_idempotency(
    tx: &mut Transaction<'_, Postgres>,
    idempotency_key_digest: &str,
) -> Result<Option<IdempotentCommandRow>, ImageGatewayError> {
    sqlx::query_as::<_, IdempotentCommandRow>(
        r#"
        SELECT command_id, action, target_version, status, phase,
               progress, failure_code, failure_message,
               requested_at_ms, started_at_ms, completed_at_ms, updated_at_ms,
               request_digest
        FROM platform_update_commands
        WHERE idempotency_key_digest = $1
        "#,
    )
    .bind(idempotency_key_digest)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn command_by_id(
    tx: &mut Transaction<'_, Postgres>,
    command_id: Uuid,
) -> Result<CommandRow, ImageGatewayError> {
    sqlx::query_as::<_, CommandRow>(
        r#"
        SELECT command_id, action, target_version, status, phase,
               progress, failure_code, failure_message,
               requested_at_ms, started_at_ms, completed_at_ms, updated_at_ms
        FROM platform_update_commands
        WHERE command_id = $1
        "#,
    )
    .bind(command_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)
}

fn normalize_target(
    action: SystemUpdateAction,
    target_version: Option<String>,
) -> Result<Option<String>, ImageGatewayError> {
    match (action, target_version) {
        (SystemUpdateAction::Check, None) => Ok(None),
        (SystemUpdateAction::Check, Some(_)) => Err(ImageGatewayError::invalid_request(
            "Check commands do not accept target_version",
            Some("target_version".to_string()),
            "system_update_unexpected_target",
        )),
        (SystemUpdateAction::Apply, Some(value)) => {
            let normalized = value.trim().to_string();
            validate_version(&normalized)?;
            Ok(Some(normalized))
        }
        (SystemUpdateAction::Apply, None) => Err(ImageGatewayError::invalid_request(
            "Apply commands require target_version",
            Some("target_version".to_string()),
            "system_update_target_required",
        )),
    }
}

fn validate_idempotency_key(value: &str) -> Result<(), ImageGatewayError> {
    if value.is_empty()
        || value.len() > 255
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(ImageGatewayError::invalid_idempotency_key());
    }
    Ok(())
}

fn validate_repository(value: String) -> Result<String, ImageGatewayError> {
    let parts: Vec<_> = value.split('/').collect();
    if parts.len() != 2
        || parts.iter().any(|part| {
            part.is_empty()
                || matches!(*part, "." | "..")
                || !part.bytes().all(is_safe_release_byte)
        })
    {
        return Err(ImageGatewayError::config(
            "AIF_UPDATE_GITHUB_REPOSITORY must use the fixed owner/repository form",
        ));
    }
    Ok(value)
}

fn validate_version(value: &str) -> Result<(), ImageGatewayError> {
    validate_token(value, "target_version", 100).map_err(|_| {
        ImageGatewayError::invalid_request(
            "target_version must contain only release-safe ASCII characters",
            Some("target_version".to_string()),
            "invalid_system_update_version",
        )
    })
}

fn validate_commit_sha(value: &str) -> Result<(), ImageGatewayError> {
    if value.len() != 40 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ImageGatewayError::config(
            "release commit must contain exactly 40 hexadecimal characters",
        ));
    }
    Ok(())
}

fn validate_token(value: &str, name: &str, max_len: usize) -> Result<(), ImageGatewayError> {
    if value.is_empty()
        || value.len() > max_len
        || !value.bytes().all(is_safe_release_byte)
        || value == "."
        || value == ".."
    {
        return Err(ImageGatewayError::config(format!(
            "{name} contains unsupported characters"
        )));
    }
    Ok(())
}

fn is_safe_release_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
}

fn request_digest(action: SystemUpdateAction, target_version: Option<&str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"ai-image-factory:system-update:request:v1");
    hasher.update([0]);
    hasher.update(action.as_str().as_bytes());
    hasher.update([0]);
    if let Some(value) = target_version {
        hasher.update(value.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn idempotency_digest(user_id: Uuid, value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"ai-image-factory:system-update:idempotency:v2");
    hasher.update([0]);
    hasher.update(user_id.as_bytes());
    hasher.update([0]);
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

fn default_target_triple() -> String {
    format!("{}-{}", env::consts::ARCH, env::consts::OS)
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn boolean_env(name: &str, default: bool) -> Result<bool, ImageGatewayError> {
    let Some(value) = optional_env(name) else {
        return Ok(default);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ImageGatewayError::config(format!(
            "{name} must be a boolean"
        ))),
    }
}

fn now_ms() -> Result<i64, ImageGatewayError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ImageGatewayError::internal("system clock is before unix epoch"))?
        .as_millis();
    i64::try_from(millis)
        .map_err(|_| ImageGatewayError::internal("system clock exceeds supported range"))
}

fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::warn!(?error, "system update storage unavailable");
    ImageGatewayError::service_unavailable("System update storage is unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_is_pinned_to_owner_and_name() {
        assert_eq!(
            validate_repository("owner/repository".to_string()).unwrap(),
            "owner/repository"
        );
        assert!(validate_repository("https://github.com/owner/repository".to_string()).is_err());
        assert!(validate_repository("owner/repository/extra".to_string()).is_err());
        assert!(validate_repository("../repository".to_string()).is_err());
    }

    #[test]
    fn release_versions_reject_paths_and_shell_syntax() {
        for invalid in ["", "..", "../v1", "v1/asset", "v1;rm", "v1 release"] {
            assert!(validate_version(invalid).is_err(), "{invalid}");
        }
        for valid in ["v1.2.3", "2026.07.29-rc.1", "release_42"] {
            validate_version(valid).unwrap();
        }
    }

    #[test]
    fn request_digest_binds_action_and_target() {
        assert_ne!(
            request_digest(SystemUpdateAction::Check, None),
            request_digest(SystemUpdateAction::Apply, Some("v1"))
        );
        assert_ne!(
            request_digest(SystemUpdateAction::Apply, Some("v1")),
            request_digest(SystemUpdateAction::Apply, Some("v2"))
        );
    }

    #[test]
    fn idempotency_digest_is_scoped_to_the_actor() {
        let first = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let second = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        assert_eq!(
            idempotency_digest(first, "retry-1"),
            idempotency_digest(first, "retry-1")
        );
        assert_ne!(
            idempotency_digest(first, "retry-1"),
            idempotency_digest(second, "retry-1")
        );
    }

    #[test]
    fn release_identity_is_strict_and_complete() {
        let identity = ReleaseIdentity::from_slice(
            br#"{
                "schema_version": 1,
                "release_version": "v1.2.3",
                "commit_sha": "0123456789abcdef0123456789abcdef01234567",
                "target_triple": "x86_64-unknown-linux-gnu"
            }"#,
        )
        .unwrap();
        assert_eq!(identity.release_version, "v1.2.3");

        assert!(
            ReleaseIdentity::from_slice(
                br#"{
                    "schema_version": 1,
                    "release_version": "v1.2.3",
                    "commit_sha": "0123456",
                    "target_triple": "x86_64-unknown-linux-gnu"
                }"#,
            )
            .is_err()
        );
        assert!(
            ReleaseIdentity::from_slice(
                br#"{
                    "schema_version": 1,
                    "release_version": "v1.2.3",
                    "commit_sha": "0123456789abcdef0123456789abcdef01234567",
                    "target_triple": "x86_64-unknown-linux-gnu",
                    "unexpected": true
                }"#,
            )
            .is_err()
        );
    }

    #[test]
    fn update_requires_a_distinct_verified_release_identity() {
        let current = "0123456789abcdef0123456789abcdef01234567";
        let latest = "89abcdef0123456789abcdef0123456789abcdef";

        assert!(!verified_update_available(
            false,
            Some("v2"),
            Some(latest),
            "v1",
            Some(current),
        ));
        assert!(!verified_update_available(
            true,
            Some("v1"),
            Some(latest),
            "v1",
            Some(current),
        ));
        assert!(!verified_update_available(
            true,
            Some("v2"),
            Some(current),
            "v1",
            Some(current),
        ));
        assert!(verified_update_available(
            true,
            Some("v2"),
            Some(latest),
            "v1",
            Some(current),
        ));
    }
}
