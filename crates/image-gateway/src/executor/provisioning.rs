use image_provider_contracts::openai_codex;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::admission::GENERATION_COMMAND_SCHEMA;

use super::CODEX_GENERATION_ADAPTER_REVISION;

const EXECUTION_CLASS: &str = "agentic-cli";
const MAX_KEY_BYTES: usize = 128;
const MAX_CREDENTIAL_REF_BYTES: usize = 1_024;
const MAX_CONCURRENCY: i32 = 1_000_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexExecutionProfileProvisioning {
    pub profile_key: String,
    pub credential_pool_key: String,
    pub provider_account_key: String,
    pub credential_ref: String,
    pub credential_revision: i64,
    pub credential_auth_sha256: String,
    pub max_concurrency: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvisionedCodexExecutionProfile {
    pub execution_profile_id: Uuid,
    pub credential_pool_id: Uuid,
    pub provider_account_id: Uuid,
    pub resource_policy_id: Uuid,
    pub resource_policy_revision: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CodexProfileProvisioningError {
    #[error("Codex execution profile provisioning input is invalid")]
    InvalidInput,
    #[error("Codex execution profile provisioning conflicts with durable identity")]
    Conflict,
    #[error("Codex execution profile provisioning storage is unavailable")]
    Unavailable,
}

#[derive(sqlx::FromRow)]
struct CredentialPoolRow {
    credential_pool_id: Uuid,
    provider_id: String,
    state: String,
}

#[derive(sqlx::FromRow)]
struct ProviderAccountRow {
    provider_account_id: Uuid,
    provider_id: String,
    credential_ref: String,
    credential_revision: i64,
    credential_auth_sha256: String,
    state: String,
}

#[derive(sqlx::FromRow)]
struct ResourcePolicyRow {
    resource_policy_id: Uuid,
    revision: i64,
    credential_pool_id: Uuid,
    provider_id: String,
    execution_class: String,
    max_concurrency: i32,
    state: String,
}

#[derive(sqlx::FromRow)]
struct ExecutionProfileRow {
    execution_profile_id: Uuid,
    provider_id: String,
    command_schema: String,
    operation_id: String,
    operation_descriptor_revision: String,
    operation_descriptor_sha256_v1: String,
    completion_mode: String,
    idempotency_mode: String,
    adapter_revision: String,
    credential_pool_id: Uuid,
    provider_account_id: Uuid,
    credential_ref: String,
    credential_revision: i64,
    resource_policy_id: Uuid,
    resource_policy_revision: i64,
    state: String,
}

pub async fn provision_codex_execution_profile(
    pool: &PgPool,
    provisioning: &CodexExecutionProfileProvisioning,
) -> Result<ProvisionedCodexExecutionProfile, CodexProfileProvisioningError> {
    validate(provisioning)?;
    let mut tx = pool.begin().await.map_err(map_sql_error)?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('factoryctl.provision-codex-profile', 0))",
    )
    .execute(&mut *tx)
    .await
    .map_err(map_sql_error)?;
    let now = database_now(&mut tx).await?;
    let credential_pool_id = ensure_pool(&mut tx, provisioning, now).await?;
    let provider_account_id =
        ensure_account(&mut tx, provisioning, credential_pool_id, now).await?;
    let (resource_policy_id, resource_policy_revision) = ensure_policy(
        &mut tx,
        provisioning,
        credential_pool_id,
        provider_account_id,
        now,
    )
    .await?;
    let execution_profile_id = ensure_profile(
        &mut tx,
        provisioning,
        credential_pool_id,
        provider_account_id,
        resource_policy_id,
        resource_policy_revision,
        now,
    )
    .await?;
    tx.commit().await.map_err(map_sql_error)?;
    Ok(ProvisionedCodexExecutionProfile {
        execution_profile_id,
        credential_pool_id,
        provider_account_id,
        resource_policy_id,
        resource_policy_revision,
    })
}

async fn ensure_pool(
    tx: &mut Transaction<'_, Postgres>,
    provisioning: &CodexExecutionProfileProvisioning,
    now: i64,
) -> Result<Uuid, CodexProfileProvisioningError> {
    let existing: Option<CredentialPoolRow> = sqlx::query_as(
        r#"
        SELECT credential_pool_id, provider_id, state
        FROM provider_credential_pools
        WHERE pool_key = $1
        FOR UPDATE
        "#,
    )
    .bind(&provisioning.credential_pool_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sql_error)?;
    let credential_pool_id = if let Some(existing) = existing {
        if existing.provider_id != openai_codex::PROVIDER_ID || existing.state != "enabled" {
            return Err(CodexProfileProvisioningError::Conflict);
        }
        existing.credential_pool_id
    } else {
        let credential_pool_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO provider_credential_pools
              (credential_pool_id, pool_key, provider_id, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, 'enabled', $4, $4)
            "#,
        )
        .bind(credential_pool_id)
        .bind(&provisioning.credential_pool_key)
        .bind(openai_codex::PROVIDER_ID)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(map_sql_error)?;
        credential_pool_id
    };
    Ok(credential_pool_id)
}

async fn ensure_account(
    tx: &mut Transaction<'_, Postgres>,
    provisioning: &CodexExecutionProfileProvisioning,
    credential_pool_id: Uuid,
    now: i64,
) -> Result<Uuid, CodexProfileProvisioningError> {
    let existing: Option<ProviderAccountRow> = sqlx::query_as(
        r#"
        SELECT provider_account_id, provider_id, credential_ref,
               credential_revision, credential_auth_sha256, state
        FROM provider_accounts
        WHERE credential_pool_id = $1 AND account_key = $2
        FOR UPDATE
        "#,
    )
    .bind(credential_pool_id)
    .bind(&provisioning.provider_account_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sql_error)?;
    let provider_account_id = if let Some(existing) = existing {
        if existing.provider_id != openai_codex::PROVIDER_ID
            || existing.credential_ref != provisioning.credential_ref
            || existing.credential_revision != provisioning.credential_revision
            || existing.credential_auth_sha256 != provisioning.credential_auth_sha256
            || existing.state != "enabled"
        {
            return Err(CodexProfileProvisioningError::Conflict);
        }
        existing.provider_account_id
    } else {
        let provider_account_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO provider_accounts
              (provider_account_id, credential_pool_id, provider_id, account_key,
               credential_ref, credential_revision, credential_auth_sha256,
               state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'enabled', $8, $8)
            "#,
        )
        .bind(provider_account_id)
        .bind(credential_pool_id)
        .bind(openai_codex::PROVIDER_ID)
        .bind(&provisioning.provider_account_key)
        .bind(&provisioning.credential_ref)
        .bind(provisioning.credential_revision)
        .bind(&provisioning.credential_auth_sha256)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(map_sql_error)?;
        provider_account_id
    };
    Ok(provider_account_id)
}

async fn ensure_policy(
    tx: &mut Transaction<'_, Postgres>,
    provisioning: &CodexExecutionProfileProvisioning,
    credential_pool_id: Uuid,
    provider_account_id: Uuid,
    now: i64,
) -> Result<(Uuid, i64), CodexProfileProvisioningError> {
    let policies: Vec<ResourcePolicyRow> = sqlx::query_as(
        r#"
        SELECT resource_policy_id, revision, credential_pool_id, provider_id,
               execution_class, max_concurrency, state
        FROM executor_resource_policies
        WHERE provider_account_id = $1
        ORDER BY revision, resource_policy_id
        FOR UPDATE
        "#,
    )
    .bind(provider_account_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sql_error)?;
    let matching = policies
        .iter()
        .filter(|policy| {
            policy.credential_pool_id == credential_pool_id
                && policy.provider_id == openai_codex::PROVIDER_ID
                && policy.execution_class == EXECUTION_CLASS
                && policy.max_concurrency == provisioning.max_concurrency
        })
        .collect::<Vec<_>>();
    let (resource_policy_id, revision) = match matching.as_slice() {
        [policy] => {
            if policy.state != "enabled"
                || policies.iter().any(|candidate| {
                    candidate.state == "enabled"
                        && (candidate.resource_policy_id, candidate.revision)
                            != (policy.resource_policy_id, policy.revision)
                })
            {
                return Err(CodexProfileProvisioningError::Conflict);
            }
            (policy.resource_policy_id, policy.revision)
        }
        [] if policies.is_empty() => {
            let resource_policy_id = Uuid::new_v4();
            sqlx::query(
                r#"
                INSERT INTO executor_resource_policies
                  (resource_policy_id, revision, credential_pool_id,
                   provider_account_id, provider_id, execution_class,
                   max_concurrency, state, created_at_ms)
                VALUES ($1, 1, $2, $3, $4, $5, $6, 'enabled', $7)
                "#,
            )
            .bind(resource_policy_id)
            .bind(credential_pool_id)
            .bind(provider_account_id)
            .bind(openai_codex::PROVIDER_ID)
            .bind(EXECUTION_CLASS)
            .bind(provisioning.max_concurrency)
            .bind(now)
            .execute(&mut **tx)
            .await
            .map_err(map_sql_error)?;
            (resource_policy_id, 1)
        }
        _ => return Err(CodexProfileProvisioningError::Conflict),
    };
    Ok((resource_policy_id, revision))
}

#[allow(clippy::too_many_arguments)]
async fn ensure_profile(
    tx: &mut Transaction<'_, Postgres>,
    provisioning: &CodexExecutionProfileProvisioning,
    credential_pool_id: Uuid,
    provider_account_id: Uuid,
    resource_policy_id: Uuid,
    resource_policy_revision: i64,
    now: i64,
) -> Result<Uuid, CodexProfileProvisioningError> {
    let operation = openai_codex::operation("images.generations")
        .ok_or(CodexProfileProvisioningError::Conflict)?;
    let operation_descriptor_sha256_v1 = operation.canonical_sha256_v1_hex();
    let existing: Option<ExecutionProfileRow> = sqlx::query_as(
        r#"
        SELECT execution_profile_id, provider_id, command_schema,
               operation_id, operation_descriptor_revision,
               operation_descriptor_sha256_v1, completion_mode, idempotency_mode,
               adapter_revision,
               credential_pool_id, provider_account_id, credential_ref,
               credential_revision, resource_policy_id, resource_policy_revision,
               state
        FROM provider_execution_profiles
        WHERE profile_key = $1
        FOR UPDATE
        "#,
    )
    .bind(&provisioning.profile_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sql_error)?;
    let execution_profile_id = if let Some(existing) = existing {
        if existing.provider_id != openai_codex::PROVIDER_ID
            || existing.command_schema != GENERATION_COMMAND_SCHEMA
            || existing.operation_id != operation.id
            || existing.operation_descriptor_revision != operation.descriptor_revision
            || existing.operation_descriptor_sha256_v1 != operation_descriptor_sha256_v1
            || existing.completion_mode != operation.completion.as_str()
            || existing.idempotency_mode != operation.idempotency.as_str()
            || existing.adapter_revision != CODEX_GENERATION_ADAPTER_REVISION
            || existing.credential_pool_id != credential_pool_id
            || existing.provider_account_id != provider_account_id
            || existing.credential_ref != provisioning.credential_ref
            || existing.credential_revision != provisioning.credential_revision
            || existing.resource_policy_id != resource_policy_id
            || existing.resource_policy_revision != resource_policy_revision
            || existing.state != "enabled"
        {
            return Err(CodexProfileProvisioningError::Conflict);
        }
        existing.execution_profile_id
    } else {
        let execution_profile_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO provider_execution_profiles
              (execution_profile_id, profile_key, provider_id, command_schema,
               operation_id, operation_descriptor_revision,
               operation_descriptor_sha256_v1, completion_mode, idempotency_mode,
               adapter_revision, credential_pool_id, provider_account_id,
               credential_ref, credential_revision, resource_policy_id,
               resource_policy_revision, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, 'enabled', $17, $17)
            "#,
        )
        .bind(execution_profile_id)
        .bind(&provisioning.profile_key)
        .bind(openai_codex::PROVIDER_ID)
        .bind(GENERATION_COMMAND_SCHEMA)
        .bind(operation.id)
        .bind(operation.descriptor_revision)
        .bind(&operation_descriptor_sha256_v1)
        .bind(operation.completion.as_str())
        .bind(operation.idempotency.as_str())
        .bind(CODEX_GENERATION_ADAPTER_REVISION)
        .bind(credential_pool_id)
        .bind(provider_account_id)
        .bind(&provisioning.credential_ref)
        .bind(provisioning.credential_revision)
        .bind(resource_policy_id)
        .bind(resource_policy_revision)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(map_sql_error)?;
        execution_profile_id
    };
    Ok(execution_profile_id)
}

fn validate(
    provisioning: &CodexExecutionProfileProvisioning,
) -> Result<(), CodexProfileProvisioningError> {
    if !valid_key(&provisioning.profile_key)
        || !valid_key(&provisioning.credential_pool_key)
        || !valid_key(&provisioning.provider_account_key)
        || provisioning.credential_ref.is_empty()
        || provisioning.credential_ref.len() > MAX_CREDENTIAL_REF_BYTES
        || provisioning
            .credential_ref
            .bytes()
            .any(|byte| byte.is_ascii_control())
        || provisioning.credential_revision <= 0
        || !valid_sha256(&provisioning.credential_auth_sha256)
        || !(1..=MAX_CONCURRENCY).contains(&provisioning.max_concurrency)
    {
        Err(CodexProfileProvisioningError::InvalidInput)
    } else {
        Ok(())
    }
}

fn valid_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_KEY_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

async fn database_now(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<i64, CodexProfileProvisioningError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(map_sql_error)
}

fn map_sql_error(error: sqlx::Error) -> CodexProfileProvisioningError {
    if let sqlx::Error::Database(database) = &error {
        let code = database.code();
        if code.as_deref().is_some_and(|value| value.starts_with("23"))
            || code.as_deref() == Some("P0001")
        {
            return CodexProfileProvisioningError::Conflict;
        }
    }
    CodexProfileProvisioningError::Unavailable
}
