use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, LazyLock},
    time::Duration,
};

use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use tokio::{process::Command, sync::Semaphore, time::timeout};
use uuid::Uuid;

use crate::ImageGatewayError;

use super::{UpdateProviderAccountModelsRequest, grok_login::copy_proxy_environment};

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_DISCOVERY_OUTPUT_BYTES: usize = 256 * 1024;
const CODEX_PROVIDER_ID: &str = "openai-codex";
const GROK_PROVIDER_ID: &str = "grok-cli";
const DREAMINA_PROVIDER_ID: &str = "dreamina-cli";
static MODEL_DISCOVERY_LIMIT: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(4));

#[derive(Clone)]
pub(super) struct ProviderModelExecutables {
    pub codex: Arc<PathBuf>,
    pub grok: Option<Arc<PathBuf>>,
    pub dreamina: Option<Arc<PathBuf>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderModelView {
    pub provider_id: String,
    pub provider_display_name: String,
    pub model_id: String,
    pub display_name: String,
    pub media_kind: String,
    pub operation_ids: Vec<String>,
    pub discovery_source: String,
    pub adapter_state: String,
    pub lifecycle_state: String,
    pub observed_account_count: i64,
    pub routable_account_count: i64,
    pub latest_cli_version: Option<String>,
    pub last_observed_at_ms: Option<i64>,
    pub last_successful_refresh_at_ms: Option<i64>,
    pub availability: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderModelsSnapshot {
    pub as_of_ms: i64,
    pub models: Vec<ProviderModelView>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderAccountModelView {
    pub model_id: String,
    pub display_name: String,
    pub media_kind: String,
    pub operation_ids: Vec<String>,
    pub enabled: bool,
    pub configurable: bool,
    pub observed: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderAccountModelsView {
    pub provider_account_id: Uuid,
    pub provider_id: String,
    pub mode: String,
    pub version: i64,
    pub models: Vec<ProviderAccountModelView>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderModelRefreshView {
    pub refresh_id: Uuid,
    pub provider_account_id: Uuid,
    pub provider_id: String,
    pub status: String,
    pub discovered_count: i32,
    pub error_code: Option<String>,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Copy)]
struct AdapterModel {
    provider_id: &'static str,
    model_id: &'static str,
    display_name: &'static str,
    media_kind: &'static str,
    operation_ids: &'static [&'static str],
}

const ADAPTER_MODELS: &[AdapterModel] = &[
    adapter_model(
        CODEX_PROVIDER_ID,
        "gpt-image-2",
        "GPT Image 2",
        "image",
        &["images.generations", "images.edits"],
    ),
    adapter_model(
        CODEX_PROVIDER_ID,
        "gpt-image-2-2026-04-21",
        "GPT Image 2 (2026-04-21)",
        "image",
        &["images.generations", "images.edits"],
    ),
    adapter_model(
        GROK_PROVIDER_ID,
        "grok-imagine-image",
        "Grok Imagine Image",
        "image",
        &["images.generations"],
    ),
    adapter_model(
        GROK_PROVIDER_ID,
        "grok-imagine-image-quality",
        "Grok Imagine Image Quality",
        "image",
        &["images.generations", "images.edits"],
    ),
    adapter_model(
        GROK_PROVIDER_ID,
        "grok-imagine-video-1.5-preview",
        "Grok Imagine Video 1.5 Preview",
        "video",
        &["videos.generations"],
    ),
    adapter_model(
        GROK_PROVIDER_ID,
        "grok-imagine-video",
        "Grok Imagine Video",
        "video",
        &["videos.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "3.0",
        "即梦图片 3.0",
        "image",
        &["images.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "3.1",
        "即梦图片 3.1",
        "image",
        &["images.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "4.0",
        "即梦图片 4.0",
        "image",
        &["images.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "4.1",
        "即梦图片 4.1",
        "image",
        &["images.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "4.5",
        "即梦图片 4.5",
        "image",
        &["images.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "4.6",
        "即梦图片 4.6",
        "image",
        &["images.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "4.7",
        "即梦图片 4.7",
        "image",
        &["images.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "5.0",
        "即梦图片 5.0",
        "image",
        &["images.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "5.0Pro",
        "即梦图片 5.0 Pro",
        "image",
        &["images.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "seedance2.0",
        "Seedance 2.0",
        "video",
        &["videos.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "seedance2.0fast",
        "Seedance 2.0 Fast",
        "video",
        &["videos.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "seedance2.0_vip",
        "Seedance 2.0 VIP",
        "video",
        &["videos.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "seedance2.0fast_vip",
        "Seedance 2.0 Fast VIP",
        "video",
        &["videos.generations"],
    ),
    adapter_model(
        DREAMINA_PROVIDER_ID,
        "seedance2.0mini",
        "Seedance 2.0 Mini",
        "video",
        &["videos.generations"],
    ),
];

const fn adapter_model(
    provider_id: &'static str,
    model_id: &'static str,
    display_name: &'static str,
    media_kind: &'static str,
    operation_ids: &'static [&'static str],
) -> AdapterModel {
    AdapterModel {
        provider_id,
        model_id,
        display_name,
        media_kind,
        operation_ids,
    }
}

#[derive(FromRow)]
struct ProviderModelRow {
    provider_id: String,
    model_id: String,
    display_name: String,
    media_kind: String,
    operation_ids: Vec<String>,
    source_kind: String,
    adapter_state: String,
    lifecycle_state: String,
    observed_account_count: i64,
    routable_account_count: i64,
    latest_cli_version: Option<String>,
    last_observed_at_ms: Option<i64>,
    last_successful_refresh_at_ms: Option<i64>,
}

#[derive(FromRow)]
struct RefreshRow {
    refresh_id: Uuid,
    provider_account_id: Uuid,
    provider_id: String,
    status: String,
    discovered_count: i32,
    error_code: Option<String>,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(FromRow)]
struct AccountEnvironmentRow {
    provider_id: String,
    environment_ref: String,
    environment_state: String,
    account_state: String,
}

#[derive(FromRow)]
struct AccountModelRow {
    model_id: String,
    display_name: String,
    media_kind: String,
    operation_ids: Vec<String>,
    configurable: bool,
    observed: bool,
    explicitly_enabled: bool,
}

struct DiscoveredModel {
    model_id: String,
    media_kind: &'static str,
    source_kind: &'static str,
    metadata: Value,
}

struct DiscoveryResult {
    models: Vec<DiscoveredModel>,
    cli_version: Option<String>,
}

#[derive(Debug)]
struct DiscoveryFailure(&'static str);

pub(super) async fn reconcile_adapter_models(pool: &PgPool) -> Result<(), ImageGatewayError> {
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await.map_err(store_unavailable)?;
    for model in ADAPTER_MODELS {
        let operations = model
            .operation_ids
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        sqlx::query(
            r#"
            INSERT INTO provider_models
              (provider_id, model_id, execution_model_id, media_kind, display_name, adapter_state,
               lifecycle_state, operation_ids, source_kind, first_seen_at_ms,
               last_seen_at_ms, metadata_json)
            VALUES ($1, $2, $3, $4, $5, 'supported', 'enabled', $6,
                    'adapter_contract', $7, $7, '{}'::JSONB)
            ON CONFLICT (provider_id, model_id, media_kind) DO UPDATE SET
              display_name = EXCLUDED.display_name,
              execution_model_id = EXCLUDED.execution_model_id,
              adapter_state = 'supported',
              operation_ids = EXCLUDED.operation_ids
            "#,
        )
        .bind(model.provider_id)
        .bind(model.model_id)
        .bind(execution_model_id(
            model.provider_id,
            model.model_id,
            model.media_kind,
        ))
        .bind(model.media_kind)
        .bind(model.display_name)
        .bind(operations)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
    }
    tx.commit().await.map_err(store_unavailable)
}

pub(super) async fn fail_interrupted_refreshes(pool: &PgPool) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        UPDATE provider_model_refreshes
        SET status = 'failed', error_code = 'gateway_restarted',
            completed_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
            updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
        WHERE status IN ('queued', 'running')
        "#,
    )
    .execute(pool)
    .await
    .map_err(store_unavailable)?;
    Ok(())
}

pub(super) async fn list_models(
    pool: &PgPool,
) -> Result<ProviderModelsSnapshot, ImageGatewayError> {
    let rows = sqlx::query_as::<_, ProviderModelRow>(
        r#"
        SELECT model.provider_id, model.model_id, model.display_name, model.media_kind,
               model.operation_ids, model.source_kind, model.adapter_state,
               model.lifecycle_state,
               COALESCE((
                 SELECT count(DISTINCT observation.provider_account_id)
                 FROM provider_account_model_observations observation
                 WHERE observation.provider_id = model.provider_id
                   AND observation.model_id = model.model_id
                   AND observation.media_kind = model.media_kind
                   AND observation.available
               ), 0)::BIGINT AS observed_account_count,
               COALESCE((
                 SELECT count(DISTINCT profile.provider_account_id)
                 FROM provider_execution_profiles profile
                 JOIN provider_accounts account
                   ON account.provider_account_id = profile.provider_account_id
                 JOIN provider_account_environments environment
                   ON environment.provider_account_id = profile.provider_account_id
                 JOIN executor_resource_policies policy
                   ON policy.provider_account_id = profile.provider_account_id
                 WHERE profile.provider_id = model.provider_id
                   AND profile.operation_id = ANY(model.operation_ids)
                   AND profile.state = 'enabled'
                   AND account.state = 'enabled'
                   AND environment.state = 'active'
                   AND policy.state = 'enabled'
                   AND (
                     NOT EXISTS (
                       SELECT 1 FROM provider_account_model_configurations configuration
                       WHERE configuration.provider_account_id = profile.provider_account_id
                         AND configuration.provider_id = profile.provider_id
                         AND configuration.mode = 'allowlist'
                     )
                     OR EXISTS (
                       SELECT 1 FROM provider_account_model_bindings binding
                       WHERE binding.provider_account_id = profile.provider_account_id
                         AND binding.provider_id = model.provider_id
                         AND binding.model_id = model.model_id
                         AND binding.media_kind = model.media_kind
                     )
                   )
               ), 0)::BIGINT AS routable_account_count,
               (
                 SELECT observation.cli_version
                 FROM provider_account_model_observations observation
                 WHERE observation.provider_id = model.provider_id
                   AND observation.model_id = model.model_id
                   AND observation.media_kind = model.media_kind
                   AND observation.cli_version IS NOT NULL
                 ORDER BY observation.observed_at_ms DESC
                 LIMIT 1
               ) AS latest_cli_version,
               (
                 SELECT max(observation.observed_at_ms)
                 FROM provider_account_model_observations observation
                 WHERE observation.provider_id = model.provider_id
                   AND observation.model_id = model.model_id
                   AND observation.media_kind = model.media_kind
               ) AS last_observed_at_ms,
               model.last_successful_refresh_at_ms
        FROM provider_models model
        ORDER BY CASE model.provider_id
                   WHEN 'openai-codex' THEN 0
                   WHEN 'grok-cli' THEN 1
                   WHEN 'dreamina-cli' THEN 2
                   ELSE 3
                 END,
                 model.media_kind, model.model_id
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(store_unavailable)?;
    let as_of_ms = database_now(pool).await?;
    Ok(ProviderModelsSnapshot {
        as_of_ms,
        models: rows.into_iter().map(model_view).collect(),
    })
}

pub(super) async fn account_models(
    pool: &PgPool,
    provider_account_id: Uuid,
) -> Result<ProviderAccountModelsView, ImageGatewayError> {
    let account = sqlx::query_as::<_, (String, Option<String>, Option<i64>)>(
        r#"
        SELECT environment.provider_id, configuration.mode, configuration.version
        FROM provider_account_environments environment
        LEFT JOIN provider_account_model_configurations configuration
          ON configuration.provider_account_id = environment.provider_account_id
         AND configuration.provider_id = environment.provider_id
        WHERE environment.provider_account_id = $1
        "#,
    )
    .bind(provider_account_id)
    .fetch_optional(pool)
    .await
    .map_err(store_unavailable)?
    .ok_or_else(provider_account_not_found)?;
    let mode = account.1.unwrap_or_else(|| "automatic".to_owned());
    let rows = sqlx::query_as::<_, AccountModelRow>(
        r#"
        SELECT model.model_id, model.display_name, model.media_kind, model.operation_ids,
               (
                 model.adapter_state = 'supported'
                 AND model.lifecycle_state = 'enabled'
                 AND EXISTS (
                   SELECT 1 FROM provider_execution_profiles profile
                   WHERE profile.provider_account_id = $1
                     AND profile.provider_id = model.provider_id
                     AND profile.operation_id = ANY(model.operation_ids)
                     AND profile.state = 'enabled'
                 )
               ) AS configurable,
               EXISTS (
                 SELECT 1 FROM provider_account_model_observations observation
                 WHERE observation.provider_account_id = $1
                   AND observation.provider_id = model.provider_id
                   AND observation.model_id = model.model_id
                   AND observation.media_kind = model.media_kind
                   AND observation.available
               ) AS observed,
               EXISTS (
                 SELECT 1 FROM provider_account_model_bindings binding
                 WHERE binding.provider_account_id = $1
                   AND binding.provider_id = model.provider_id
                   AND binding.model_id = model.model_id
                   AND binding.media_kind = model.media_kind
               ) AS explicitly_enabled
        FROM provider_models model
        WHERE model.provider_id = $2
        ORDER BY model.media_kind, model.model_id
        "#,
    )
    .bind(provider_account_id)
    .bind(&account.0)
    .fetch_all(pool)
    .await
    .map_err(store_unavailable)?;
    Ok(ProviderAccountModelsView {
        provider_account_id,
        provider_id: account.0,
        mode: mode.clone(),
        version: account.2.unwrap_or(0),
        models: rows
            .into_iter()
            .map(|row| ProviderAccountModelView {
                enabled: row.configurable && (mode == "automatic" || row.explicitly_enabled),
                model_id: row.model_id,
                display_name: row.display_name,
                media_kind: row.media_kind,
                operation_ids: row.operation_ids,
                configurable: row.configurable,
                observed: row.observed,
            })
            .collect(),
    })
}

pub(super) async fn update_account_models(
    pool: &PgPool,
    provider_account_id: Uuid,
    request: UpdateProviderAccountModelsRequest,
) -> Result<ProviderAccountModelsView, ImageGatewayError> {
    let mut tx = pool.begin().await.map_err(store_unavailable)?;
    update_account_models_in_transaction(&mut tx, provider_account_id, &request).await?;
    tx.commit().await.map_err(store_unavailable)?;
    account_models(pool, provider_account_id).await
}

pub(super) async fn update_account_models_in_transaction(
    tx: &mut Transaction<'_, Postgres>,
    provider_account_id: Uuid,
    request: &UpdateProviderAccountModelsRequest,
) -> Result<i64, ImageGatewayError> {
    if !matches!(request.mode.as_str(), "automatic" | "allowlist") {
        return Err(ImageGatewayError::invalid_request(
            "Model configuration mode must be automatic or allowlist",
            Some("mode".to_owned()),
            "invalid_model_configuration_mode",
        ));
    }
    if request.mode == "automatic" && !request.enabled_models.is_empty() {
        return Err(ImageGatewayError::invalid_request(
            "Automatic model configuration cannot include an allowlist",
            Some("enabled_models".to_owned()),
            "invalid_model_allowlist",
        ));
    }
    let mut selections = HashSet::new();
    for selection in &request.enabled_models {
        if !matches!(selection.media_kind.as_str(), "image" | "video")
            || !valid_model_id(&selection.model_id)
            || !selections.insert((selection.model_id.clone(), selection.media_kind.clone()))
        {
            return Err(ImageGatewayError::invalid_request(
                "Model allowlist contains an invalid or duplicate entry",
                Some("enabled_models".to_owned()),
                "invalid_model_allowlist",
            ));
        }
    }

    let tx = &mut **tx;
    let provider_id = sqlx::query_scalar::<_, String>(
        "SELECT provider_id FROM provider_account_environments WHERE provider_account_id = $1 FOR SHARE",
    )
    .bind(provider_account_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(store_unavailable)?
    .ok_or_else(provider_account_not_found)?;
    let current_version = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM provider_account_model_configurations WHERE provider_account_id = $1 FOR UPDATE",
    )
    .bind(provider_account_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(store_unavailable)?
    .unwrap_or(0);
    if current_version != request.expected_version {
        return Err(ImageGatewayError::conflict(
            "Provider account model configuration changed; reload and retry",
            Some("expected_version".to_owned()),
            "provider_account_model_version_conflict",
        ));
    }
    if request.mode == "allowlist" {
        for (model_id, media_kind) in &selections {
            let selectable = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                  SELECT 1 FROM provider_models model
                  WHERE model.provider_id = $1 AND model.model_id = $2
                    AND model.media_kind = $3 AND model.adapter_state = 'supported'
                    AND model.lifecycle_state = 'enabled'
                    AND EXISTS (
                      SELECT 1 FROM provider_execution_profiles profile
                      WHERE profile.provider_account_id = $4
                        AND profile.provider_id = model.provider_id
                        AND profile.operation_id = ANY(model.operation_ids)
                        AND profile.state = 'enabled'
                    )
                )
                "#,
            )
            .bind(&provider_id)
            .bind(model_id)
            .bind(media_kind)
            .bind(provider_account_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(store_unavailable)?;
            if !selectable {
                return Err(ImageGatewayError::invalid_request(
                    "Model allowlist contains a model unavailable to this account",
                    Some("enabled_models".to_owned()),
                    "model_unavailable_for_account",
                ));
            }
        }
    }
    let now = sqlx::query_scalar::<_, i64>(
        "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(store_unavailable)?;
    let next_version = current_version + 1;
    sqlx::query(
        r#"
        INSERT INTO provider_account_model_configurations
          (provider_account_id, provider_id, mode, version, updated_at_ms)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (provider_account_id) DO UPDATE SET
          mode = EXCLUDED.mode, version = EXCLUDED.version,
          updated_at_ms = EXCLUDED.updated_at_ms
        "#,
    )
    .bind(provider_account_id)
    .bind(&provider_id)
    .bind(&request.mode)
    .bind(next_version)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(store_unavailable)?;
    sqlx::query("DELETE FROM provider_account_model_bindings WHERE provider_account_id = $1")
        .bind(provider_account_id)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
    if request.mode == "allowlist" {
        for (model_id, media_kind) in selections {
            sqlx::query(
                r#"
                INSERT INTO provider_account_model_bindings
                  (provider_account_id, provider_id, model_id, media_kind, configured_at_ms)
                VALUES ($1, $2, $3, $4, $5)
                "#,
            )
            .bind(provider_account_id)
            .bind(&provider_id)
            .bind(model_id)
            .bind(media_kind)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(store_unavailable)?;
        }
    }
    Ok(next_version)
}

pub(super) async fn start_refresh(
    pool: PgPool,
    executables: ProviderModelExecutables,
    provider_account_id: Uuid,
) -> Result<ProviderModelRefreshView, ImageGatewayError> {
    reconcile_adapter_models(&pool).await?;
    let account = sqlx::query_as::<_, AccountEnvironmentRow>(
        r#"
        SELECT environment.provider_id, environment.environment_ref,
               environment.state AS environment_state, account.state AS account_state
        FROM provider_account_environments environment
        JOIN provider_accounts account
          ON account.provider_account_id = environment.provider_account_id
        WHERE environment.provider_account_id = $1
        "#,
    )
    .bind(provider_account_id)
    .fetch_optional(&pool)
    .await
    .map_err(store_unavailable)?
    .ok_or_else(|| {
        ImageGatewayError::not_found(
            "Provider account was not found",
            Some("provider_account_id".to_owned()),
            "provider_account_not_found",
        )
    })?;
    if account.environment_state != "active" || account.account_state != "enabled" {
        return Err(ImageGatewayError::conflict(
            "Provider account is not active",
            Some("provider_account_id".to_owned()),
            "provider_account_inactive",
        ));
    }
    ensure_executable(&executables, &account.provider_id)?;

    let refresh_id = Uuid::new_v4();
    let now = database_now(&pool).await?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO provider_model_refreshes
          (refresh_id, provider_account_id, provider_id, status,
           discovered_count, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 'queued', 0, $4, $4)
        "#,
    )
    .bind(refresh_id)
    .bind(provider_account_id)
    .bind(&account.provider_id)
    .bind(now)
    .execute(&pool)
    .await;
    if let Err(error) = inserted {
        if let sqlx::Error::Database(database) = &error
            && database.constraint() == Some("provider_model_refreshes_active_account_idx")
        {
            return Err(ImageGatewayError::conflict(
                "A model refresh is already in progress for this account",
                Some("provider_account_id".to_owned()),
                "model_refresh_in_progress",
            ));
        }
        return Err(store_unavailable(error));
    }

    let task_pool = pool.clone();
    tokio::spawn(async move {
        run_refresh(
            task_pool,
            executables,
            refresh_id,
            provider_account_id,
            account,
        )
        .await;
    });
    refresh(&pool, refresh_id).await
}

pub(super) async fn refresh(
    pool: &PgPool,
    refresh_id: Uuid,
) -> Result<ProviderModelRefreshView, ImageGatewayError> {
    let row = sqlx::query_as::<_, RefreshRow>(
        r#"
        SELECT refresh_id, provider_account_id, provider_id, status, discovered_count,
               error_code, started_at_ms, completed_at_ms, created_at_ms, updated_at_ms
        FROM provider_model_refreshes
        WHERE refresh_id = $1
        "#,
    )
    .bind(refresh_id)
    .fetch_optional(pool)
    .await
    .map_err(store_unavailable)?
    .ok_or_else(|| {
        ImageGatewayError::not_found(
            "Provider model refresh was not found",
            Some("refresh_id".to_owned()),
            "model_refresh_not_found",
        )
    })?;
    Ok(row.into())
}

async fn run_refresh(
    pool: PgPool,
    executables: ProviderModelExecutables,
    refresh_id: Uuid,
    provider_account_id: Uuid,
    account: AccountEnvironmentRow,
) {
    let Ok(_permit) = MODEL_DISCOVERY_LIMIT.acquire().await else {
        mark_failed(&pool, refresh_id, "model_discovery_unavailable").await;
        return;
    };
    let started_at_ms = match database_now(&pool).await {
        Ok(value) => value,
        Err(_) => return,
    };
    if sqlx::query(
        "UPDATE provider_model_refreshes SET status = 'running', started_at_ms = $2, updated_at_ms = $2 WHERE refresh_id = $1 AND status = 'queued'",
    )
    .bind(refresh_id)
    .bind(started_at_ms)
    .execute(&pool)
    .await
    .ok()
    .is_none_or(|result| result.rows_affected() != 1)
    {
        return;
    }

    let result = discover(
        &executables,
        &account.provider_id,
        Path::new(&account.environment_ref),
    )
    .await;
    match result {
        Ok(discovery) => {
            if persist_discovery(
                &pool,
                refresh_id,
                provider_account_id,
                &account.provider_id,
                discovery,
            )
            .await
            .is_err()
            {
                mark_failed(&pool, refresh_id, "model_catalog_store_unavailable").await;
            }
        }
        Err(error) => mark_failed(&pool, refresh_id, error.0).await,
    }
}

async fn persist_discovery(
    pool: &PgPool,
    refresh_id: Uuid,
    provider_account_id: Uuid,
    provider_id: &str,
    discovery: DiscoveryResult,
) -> Result<(), ImageGatewayError> {
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await.map_err(store_unavailable)?;
    for discovered in &discovery.models {
        let adapter = find_adapter_model(provider_id, &discovered.model_id, discovered.media_kind);
        let display_name = adapter.map_or_else(
            || discovered.model_id.clone(),
            |model| model.display_name.to_owned(),
        );
        let adapter_state = if adapter.is_some() {
            "supported"
        } else {
            "discovered"
        };
        let lifecycle_state = if adapter.is_some() {
            "enabled"
        } else {
            "disabled"
        };
        let operations = adapter
            .map(|model| {
                model
                    .operation_ids
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        sqlx::query(
            r#"
            INSERT INTO provider_models
              (provider_id, model_id, execution_model_id, media_kind, display_name, adapter_state,
               lifecycle_state, operation_ids, source_kind, first_seen_at_ms,
               last_seen_at_ms, last_successful_refresh_at_ms, metadata_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, $10, $11)
            ON CONFLICT (provider_id, model_id, media_kind) DO UPDATE SET
              display_name = EXCLUDED.display_name,
              execution_model_id = EXCLUDED.execution_model_id,
              adapter_state = CASE WHEN EXCLUDED.adapter_state = 'supported'
                                   THEN 'supported' ELSE provider_models.adapter_state END,
              lifecycle_state = CASE WHEN EXCLUDED.adapter_state = 'supported'
                                     THEN 'enabled' ELSE provider_models.lifecycle_state END,
              operation_ids = CASE WHEN EXCLUDED.adapter_state = 'supported'
                                   THEN EXCLUDED.operation_ids ELSE provider_models.operation_ids END,
              source_kind = EXCLUDED.source_kind,
              last_seen_at_ms = EXCLUDED.last_seen_at_ms,
              last_successful_refresh_at_ms = EXCLUDED.last_successful_refresh_at_ms,
              metadata_json = EXCLUDED.metadata_json
            "#,
        )
        .bind(provider_id)
        .bind(&discovered.model_id)
        .bind(execution_model_id(
            provider_id,
            &discovered.model_id,
            discovered.media_kind,
        ))
        .bind(discovered.media_kind)
        .bind(display_name)
        .bind(adapter_state)
        .bind(lifecycle_state)
        .bind(operations)
        .bind(discovered.source_kind)
        .bind(now)
        .bind(&discovered.metadata)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_model_observations
              (provider_account_id, provider_id, model_id, media_kind, available,
               source_kind, cli_version, observed_at_ms, refresh_id, metadata_json)
            VALUES ($1, $2, $3, $4, TRUE, $5, $6, $7, $8, $9)
            ON CONFLICT (provider_account_id, provider_id, model_id, media_kind)
            DO UPDATE SET available = TRUE, source_kind = EXCLUDED.source_kind,
                          cli_version = EXCLUDED.cli_version,
                          observed_at_ms = EXCLUDED.observed_at_ms,
                          refresh_id = EXCLUDED.refresh_id,
                          metadata_json = EXCLUDED.metadata_json
            "#,
        )
        .bind(provider_account_id)
        .bind(provider_id)
        .bind(&discovered.model_id)
        .bind(discovered.media_kind)
        .bind(discovered.source_kind)
        .bind(&discovery.cli_version)
        .bind(now)
        .bind(refresh_id)
        .bind(&discovered.metadata)
        .execute(&mut *tx)
        .await
        .map_err(store_unavailable)?;
    }
    sqlx::query(
        r#"
        UPDATE provider_account_model_observations
        SET available = FALSE
        WHERE provider_account_id = $1 AND provider_id = $2 AND refresh_id <> $3
        "#,
    )
    .bind(provider_account_id)
    .bind(provider_id)
    .bind(refresh_id)
    .execute(&mut *tx)
    .await
    .map_err(store_unavailable)?;
    sqlx::query(
        r#"
        UPDATE provider_model_refreshes
        SET status = 'succeeded', discovered_count = $2, error_code = NULL,
            completed_at_ms = $3, updated_at_ms = $3
        WHERE refresh_id = $1 AND status = 'running'
        "#,
    )
    .bind(refresh_id)
    .bind(i32::try_from(discovery.models.len()).unwrap_or(i32::MAX))
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(store_unavailable)?;
    tx.commit().await.map_err(store_unavailable)
}

async fn discover(
    executables: &ProviderModelExecutables,
    provider_id: &str,
    home: &Path,
) -> Result<DiscoveryResult, DiscoveryFailure> {
    match provider_id {
        CODEX_PROVIDER_ID => {
            let version =
                command_text(&executables.codex, home, provider_id, &["--version"]).await?;
            Ok(contract_discovery(provider_id, Some(first_line(&version))))
        }
        GROK_PROVIDER_ID => {
            let executable = executables
                .grok
                .as_ref()
                .ok_or(DiscoveryFailure("grok_cli_unavailable"))?;
            let _agent_models = command_text(executable, home, provider_id, &["models"]).await?;
            let version = command_text(executable, home, provider_id, &["--version"])
                .await
                .ok();
            Ok(contract_discovery(
                provider_id,
                version.as_deref().map(first_line),
            ))
        }
        DREAMINA_PROVIDER_ID => {
            let executable = executables
                .dreamina
                .as_ref()
                .ok_or(DiscoveryFailure("dreamina_cli_unavailable"))?;
            let image_help =
                command_text(executable, home, provider_id, &["text2image", "--help"]).await?;
            let video_help =
                command_text(executable, home, provider_id, &["text2video", "--help"]).await?;
            let mut models = parse_model_versions(&image_help)
                .into_iter()
                .map(|model_id| DiscoveredModel {
                    model_id,
                    media_kind: "image",
                    source_kind: "cli_help",
                    metadata: json!({"command": "text2image"}),
                })
                .collect::<Vec<_>>();
            models.extend(
                parse_model_versions(&video_help)
                    .into_iter()
                    .map(|model_id| DiscoveredModel {
                        model_id,
                        media_kind: "video",
                        source_kind: "cli_help",
                        metadata: json!({"command": "text2video"}),
                    }),
            );
            if models.is_empty() {
                return Err(DiscoveryFailure("dreamina_models_unavailable"));
            }
            let version = command_text(executable, home, provider_id, &["--version"])
                .await
                .ok();
            Ok(DiscoveryResult {
                models,
                cli_version: version.as_deref().map(first_line),
            })
        }
        _ => Err(DiscoveryFailure("provider_model_discovery_unsupported")),
    }
}

fn contract_discovery(provider_id: &str, cli_version: Option<String>) -> DiscoveryResult {
    let models = ADAPTER_MODELS.iter().filter(|model| model.provider_id == provider_id).map(|model| DiscoveredModel {
        model_id: model.model_id.to_owned(),
        media_kind: model.media_kind,
        source_kind: "adapter_contract",
        metadata: json!({"discovery_note": "CLI does not expose a media-model catalog; adapter contract is authoritative"}),
    }).collect();
    DiscoveryResult {
        models,
        cli_version,
    }
}

async fn command_text(
    executable: &Path,
    home: &Path,
    provider_id: &str,
    args: &[&str],
) -> Result<String, DiscoveryFailure> {
    let temporary = home.join(".tmp");
    std::fs::create_dir_all(&temporary)
        .map_err(|_| DiscoveryFailure("provider_home_unavailable"))?;
    let mut command = Command::new(executable);
    command
        .args(args)
        .env_clear()
        .env("HOME", home)
        .env("TMPDIR", temporary)
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .current_dir(home)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if provider_id == GROK_PROVIDER_ID {
        command
            .env("GROK_HOME", home)
            .env("GROK_DISABLE_AUTOUPDATER", "1");
    }
    copy_proxy_environment(&mut command);
    let output = timeout(DISCOVERY_TIMEOUT, command.output())
        .await
        .map_err(|_| DiscoveryFailure("model_discovery_timeout"))?
        .map_err(|_| DiscoveryFailure("model_discovery_process_failed"))?;
    if output.stdout.len().saturating_add(output.stderr.len()) > MAX_DISCOVERY_OUTPUT_BYTES {
        return Err(DiscoveryFailure("model_discovery_output_too_large"));
    }
    if !output.status.success() {
        return Err(DiscoveryFailure("model_discovery_command_failed"));
    }
    String::from_utf8(output.stdout).map_err(|_| DiscoveryFailure("model_discovery_output_invalid"))
}

async fn mark_failed(pool: &PgPool, refresh_id: Uuid, code: &str) {
    let _ = sqlx::query(
        r#"
        UPDATE provider_model_refreshes
        SET status = 'failed', error_code = $2,
            completed_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
            updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
        WHERE refresh_id = $1 AND status IN ('queued', 'running')
        "#,
    )
    .bind(refresh_id)
    .bind(code)
    .execute(pool)
    .await;
}

fn parse_model_versions(output: &str) -> Vec<String> {
    let Some(values) = output
        .lines()
        .find_map(|line| line.trim().strip_prefix("- model_version:"))
    else {
        return Vec::new();
    };
    values
        .split(',')
        .map(str::trim)
        .filter(|value| valid_model_id(value))
        .map(str::to_owned)
        .collect()
}

fn valid_model_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn execution_model_id(provider_id: &str, model_id: &str, media_kind: &str) -> String {
    if provider_id == DREAMINA_PROVIDER_ID && media_kind == "image" {
        format!("dreamina-image-{model_id}")
    } else {
        model_id.to_owned()
    }
}

fn first_line(value: &str) -> String {
    value
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .chars()
        .take(255)
        .collect()
}

fn find_adapter_model(
    provider_id: &str,
    model_id: &str,
    media_kind: &str,
) -> Option<&'static AdapterModel> {
    ADAPTER_MODELS.iter().find(|model| {
        model.provider_id == provider_id
            && model.model_id == model_id
            && model.media_kind == media_kind
    })
}

fn ensure_executable(
    executables: &ProviderModelExecutables,
    provider_id: &str,
) -> Result<(), ImageGatewayError> {
    let available = match provider_id {
        CODEX_PROVIDER_ID => true,
        GROK_PROVIDER_ID => executables.grok.is_some(),
        DREAMINA_PROVIDER_ID => executables.dreamina.is_some(),
        _ => false,
    };
    if available {
        Ok(())
    } else {
        Err(ImageGatewayError::service_unavailable(
            "Provider model discovery is unavailable",
        ))
    }
}

fn provider_display_name(provider_id: &str) -> &str {
    match provider_id {
        CODEX_PROVIDER_ID => "Codex",
        GROK_PROVIDER_ID => "Grok",
        DREAMINA_PROVIDER_ID => "即梦",
        _ => provider_id,
    }
}

fn provider_account_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Provider account was not found",
        Some("provider_account_id".to_owned()),
        "provider_account_not_found",
    )
}

fn model_view(row: ProviderModelRow) -> ProviderModelView {
    let availability = if row.adapter_state != "supported" || row.lifecycle_state != "enabled" {
        "not_supported"
    } else if row.routable_account_count > 0 {
        "routable"
    } else if row.observed_account_count > 0 {
        "observed"
    } else {
        "unobserved"
    };
    ProviderModelView {
        provider_display_name: provider_display_name(&row.provider_id).to_owned(),
        provider_id: row.provider_id,
        model_id: row.model_id,
        display_name: row.display_name,
        media_kind: row.media_kind,
        operation_ids: row.operation_ids,
        discovery_source: row.source_kind,
        adapter_state: row.adapter_state,
        lifecycle_state: row.lifecycle_state,
        observed_account_count: row.observed_account_count,
        routable_account_count: row.routable_account_count,
        latest_cli_version: row.latest_cli_version,
        last_observed_at_ms: row.last_observed_at_ms,
        last_successful_refresh_at_ms: row.last_successful_refresh_at_ms,
        availability: availability.to_owned(),
    }
}

impl From<RefreshRow> for ProviderModelRefreshView {
    fn from(row: RefreshRow) -> Self {
        Self {
            refresh_id: row.refresh_id,
            provider_account_id: row.provider_account_id,
            provider_id: row.provider_id,
            status: row.status,
            discovered_count: row.discovered_count,
            error_code: row.error_code,
            started_at_ms: row.started_at_ms,
            completed_at_ms: row.completed_at_ms,
            created_at_ms: row.created_at_ms,
            updated_at_ms: row.updated_at_ms,
        }
    }
}

async fn database_now(pool: &PgPool) -> Result<i64, ImageGatewayError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(store_unavailable)
}

fn store_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("provider model catalog unavailable")
}

#[cfg(test)]
mod tests {
    use super::parse_model_versions;

    #[test]
    fn parses_only_the_declared_model_version_line() {
        let output =
            "Supported combinations:\n- model_version: 5.0, 5.0Pro, future-1\n- ratio: 1:1\n";
        assert_eq!(parse_model_versions(output), ["5.0", "5.0Pro", "future-1"]);
    }

    #[test]
    fn rejects_missing_or_invalid_model_lists() {
        assert!(parse_model_versions("--model_version string").is_empty());
        assert_eq!(
            parse_model_versions("- model_version: ok, bad/model"),
            ["ok"]
        );
    }
}
