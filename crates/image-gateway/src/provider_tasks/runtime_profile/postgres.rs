use sqlx::FromRow;
use uuid::Uuid;

use crate::{
    executor::ExecutorExecutionProfile,
    provider_tasks::{PostgresProviderTaskStore, ProviderTaskStoreError},
};

use super::{ProviderRuntimeProfile, ProviderRuntimeProfileStore, valid_simple_identifier};

#[derive(FromRow)]
struct RuntimeProfileRow {
    execution_profile_id: Uuid,
    profile_key: String,
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
    credential_auth_sha256: String,
    resource_policy_id: Uuid,
    resource_policy_revision: i64,
    max_concurrency: i32,
}

impl From<RuntimeProfileRow> for ExecutorExecutionProfile {
    fn from(row: RuntimeProfileRow) -> Self {
        Self {
            execution_profile_id: row.execution_profile_id,
            profile_key: row.profile_key,
            provider_id: row.provider_id,
            command_schema: row.command_schema,
            operation_id: row.operation_id,
            operation_descriptor_revision: row.operation_descriptor_revision,
            operation_descriptor_sha256_v1: row.operation_descriptor_sha256_v1,
            completion_mode: row.completion_mode,
            idempotency_mode: row.idempotency_mode,
            adapter_revision: row.adapter_revision,
            credential_pool_id: row.credential_pool_id,
            provider_account_id: row.provider_account_id,
            credential_ref: row.credential_ref,
            credential_revision: row.credential_revision,
            credential_auth_sha256: row.credential_auth_sha256,
            resource_policy_id: row.resource_policy_id,
            resource_policy_revision: row.resource_policy_revision,
            max_concurrency: row.max_concurrency,
        }
    }
}

impl ProviderRuntimeProfileStore for PostgresProviderTaskStore {
    async fn load_active_runtime_profile(
        &self,
        profile_key: &str,
    ) -> Result<ProviderRuntimeProfile, ProviderTaskStoreError> {
        if !valid_simple_identifier(profile_key) {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let row: RuntimeProfileRow = sqlx::query_as(
            r#"
            SELECT profile.execution_profile_id, profile.profile_key,
                   profile.provider_id, profile.command_schema,
                   profile.operation_id, profile.operation_descriptor_revision,
                   profile.operation_descriptor_sha256_v1, profile.completion_mode,
                   profile.idempotency_mode, profile.adapter_revision,
                   profile.credential_pool_id, profile.provider_account_id,
                   profile.credential_ref, profile.credential_revision,
                   account.credential_auth_sha256,
                   profile.resource_policy_id, profile.resource_policy_revision,
                   policy.max_concurrency
            FROM provider_execution_profiles profile
            JOIN provider_credential_pools pool
              ON pool.credential_pool_id = profile.credential_pool_id
             AND pool.provider_id = profile.provider_id
             AND pool.state = 'enabled'
            JOIN provider_accounts account
              ON account.provider_account_id = profile.provider_account_id
             AND account.credential_pool_id = profile.credential_pool_id
             AND account.provider_id = profile.provider_id
             AND account.credential_ref = profile.credential_ref
             AND account.credential_revision = profile.credential_revision
             AND account.state = 'enabled'
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = profile.resource_policy_id
             AND policy.revision = profile.resource_policy_revision
             AND policy.credential_pool_id = profile.credential_pool_id
             AND policy.provider_account_id = profile.provider_account_id
             AND policy.provider_id = profile.provider_id
             AND policy.state = 'enabled'
            WHERE profile.profile_key = $1
              AND profile.state = 'enabled'
              AND profile.completion_mode = 'remote_task'
            "#,
        )
        .bind(profile_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?
        .ok_or(ProviderTaskStoreError::NotFound)?;
        ProviderRuntimeProfile::new(row.into()).map_err(|_| ProviderTaskStoreError::Conflict)
    }
}

fn unavailable(_: sqlx::Error) -> ProviderTaskStoreError {
    ProviderTaskStoreError::Unavailable
}
