use async_trait::async_trait;
use sqlx::FromRow;

use crate::provider_tasks::{
    MAX_PROVIDER_RUNTIME_LANES, PostgresProviderTaskStore, ProviderTaskStoreError,
};

use super::{
    ProviderProfileReadiness, ProviderProfileReadinessStatus, ProviderProfileReadinessStore,
    ProviderProfileReadinessSummary, ProviderRuntimeLease, ProviderRuntimeLeaseState,
    ProviderRuntimeReadinessStore, ProviderRuntimeRegistration, ProviderRuntimeRole,
    validate_lease, validate_registration,
};

#[derive(FromRow)]
struct RuntimeLeaseRow {
    runtime_id: uuid::Uuid,
    execution_profile_id: uuid::Uuid,
    runtime_role: String,
    runtime_owner: String,
    state: String,
    heartbeat_at_ms: i64,
    lease_expires_at_ms: i64,
}

impl TryFrom<RuntimeLeaseRow> for ProviderRuntimeLease {
    type Error = ProviderTaskStoreError;

    fn try_from(row: RuntimeLeaseRow) -> Result<Self, Self::Error> {
        Ok(Self {
            runtime_id: row.runtime_id,
            execution_profile_id: row.execution_profile_id,
            role: ProviderRuntimeRole::parse(&row.runtime_role)?,
            runtime_owner: row.runtime_owner,
            state: ProviderRuntimeLeaseState::parse(&row.state)?,
            heartbeat_at_ms: row.heartbeat_at_ms,
            lease_expires_at_ms: row.lease_expires_at_ms,
        })
    }
}

#[derive(FromRow)]
struct ProfileReadinessRow {
    execution_profile_id: uuid::Uuid,
    profile_key: String,
    provider_id: String,
    status: String,
    active_submitters: i64,
    active_pollers: i64,
    draining_submitters: i64,
    draining_pollers: i64,
}

#[derive(FromRow)]
struct ProfileReadinessSummaryRow {
    configured: i64,
    active: i64,
    draining: i64,
    blocked: i64,
}

impl TryFrom<ProfileReadinessRow> for ProviderProfileReadiness {
    type Error = ProviderTaskStoreError;

    fn try_from(row: ProfileReadinessRow) -> Result<Self, Self::Error> {
        Ok(Self {
            execution_profile_id: row.execution_profile_id,
            profile_key: row.profile_key,
            provider_id: row.provider_id,
            status: ProviderProfileReadinessStatus::parse(&row.status)?,
            active_submitters: row.active_submitters,
            active_pollers: row.active_pollers,
            draining_submitters: row.draining_submitters,
            draining_pollers: row.draining_pollers,
        })
    }
}

#[async_trait]
impl ProviderProfileReadinessStore for PostgresProviderTaskStore {
    async fn summarize_profile_readiness(
        &self,
    ) -> Result<ProviderProfileReadinessSummary, ProviderTaskStoreError> {
        let row: ProfileReadinessSummaryRow = sqlx::query_as(
            r#"
            SELECT COUNT(*) FILTER (WHERE status = 'configured')::BIGINT AS configured,
                   COUNT(*) FILTER (WHERE status = 'active')::BIGINT AS active,
                   COUNT(*) FILTER (WHERE status = 'draining')::BIGINT AS draining,
                   COUNT(*) FILTER (WHERE status = 'blocked')::BIGINT AS blocked
            FROM provider_profile_readiness
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(ProviderProfileReadinessSummary {
            configured: row.configured,
            active: row.active,
            draining: row.draining,
            blocked: row.blocked,
        })
    }
}

#[async_trait]
impl ProviderRuntimeReadinessStore for PostgresProviderTaskStore {
    async fn register_runtime(
        &self,
        registration: &ProviderRuntimeRegistration,
        lease_ms: i64,
    ) -> Result<ProviderRuntimeLease, ProviderTaskStoreError> {
        validate_registration(registration, lease_ms)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        sqlx::query(
            r#"
            DELETE FROM provider_runtime_leases
            WHERE execution_profile_id = $1 AND runtime_role = $2
              AND lease_expires_at_ms <= $3
            "#,
        )
        .bind(registration.execution_profile_id)
        .bind(registration.role.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        let runnable: Option<bool> = sqlx::query_scalar(
            r#"
            SELECT TRUE
            FROM provider_execution_profiles profile
            JOIN provider_credential_pools pool
              ON pool.credential_pool_id = profile.credential_pool_id
             AND pool.provider_id = profile.provider_id
            JOIN provider_accounts account
              ON account.provider_account_id = profile.provider_account_id
             AND account.credential_pool_id = profile.credential_pool_id
             AND account.provider_id = profile.provider_id
             AND account.credential_ref = profile.credential_ref
             AND account.credential_revision = profile.credential_revision
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = profile.resource_policy_id
             AND policy.revision = profile.resource_policy_revision
             AND policy.credential_pool_id = profile.credential_pool_id
             AND policy.provider_account_id = profile.provider_account_id
             AND policy.provider_id = profile.provider_id
            JOIN provider_account_execution_controls control
              ON control.provider_account_id = profile.provider_account_id
            WHERE profile.execution_profile_id = $1
              AND profile.state = 'enabled'
              AND pool.state = 'enabled'
              AND account.state = 'enabled'
              AND policy.state = 'enabled'
              AND control.lifecycle_state IN ('active', 'draining')
              AND profile.completion_mode = 'remote_task'
              AND control.desired_max_concurrency BETWEEN 1 AND $2
            FOR SHARE OF profile, pool, account, policy, control
            "#,
        )
        .bind(registration.execution_profile_id)
        .bind(i32::try_from(MAX_PROVIDER_RUNTIME_LANES).expect("runtime lane limit fits i32"))
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if runnable.is_none() {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let row: RuntimeLeaseRow = sqlx::query_as(
            r#"
            INSERT INTO provider_runtime_leases
              (runtime_id, execution_profile_id, runtime_role, runtime_owner,
               state, heartbeat_at_ms, lease_expires_at_ms,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, 'active', $5, $5 + $6, $5, $5)
            ON CONFLICT (runtime_id) DO UPDATE
            SET heartbeat_at_ms = EXCLUDED.heartbeat_at_ms,
                lease_expires_at_ms = EXCLUDED.lease_expires_at_ms,
                updated_at_ms = EXCLUDED.updated_at_ms
            WHERE provider_runtime_leases.execution_profile_id =
                      EXCLUDED.execution_profile_id
              AND provider_runtime_leases.runtime_role = EXCLUDED.runtime_role
              AND provider_runtime_leases.runtime_owner = EXCLUDED.runtime_owner
              AND provider_runtime_leases.state = 'active'
              AND provider_runtime_leases.lease_expires_at_ms > $5
            RETURNING runtime_id, execution_profile_id, runtime_role,
                      runtime_owner, state, heartbeat_at_ms, lease_expires_at_ms
            "#,
        )
        .bind(registration.runtime_id)
        .bind(registration.execution_profile_id)
        .bind(registration.role.as_str())
        .bind(&registration.runtime_owner)
        .bind(now)
        .bind(lease_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_conflict)?
        .ok_or(ProviderTaskStoreError::Conflict)?;
        tx.commit().await.map_err(unavailable)?;
        row.try_into()
    }

    async fn heartbeat_runtime(
        &self,
        lease: &ProviderRuntimeLease,
        lease_ms: i64,
    ) -> Result<ProviderRuntimeLease, ProviderTaskStoreError> {
        update_runtime_lease(self, lease, lease_ms, false).await
    }

    async fn begin_runtime_drain(
        &self,
        lease: &ProviderRuntimeLease,
        lease_ms: i64,
    ) -> Result<ProviderRuntimeLease, ProviderTaskStoreError> {
        update_runtime_lease(self, lease, lease_ms, true).await
    }

    async fn withdraw_runtime(
        &self,
        lease: &ProviderRuntimeLease,
    ) -> Result<(), ProviderTaskStoreError> {
        validate_lease(lease, None)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let drained = sqlx::query(
            r#"
            UPDATE provider_runtime_leases
            SET state = 'draining', heartbeat_at_ms = $6,
                lease_expires_at_ms = GREATEST(lease_expires_at_ms, $6 + 1),
                updated_at_ms = $6
            WHERE runtime_id = $1 AND execution_profile_id = $2
              AND runtime_role = $3 AND runtime_owner = $4
              AND state IN ('active', 'draining')
              AND lease_expires_at_ms > $5
            "#,
        )
        .bind(lease.runtime_id)
        .bind(lease.execution_profile_id)
        .bind(lease.role.as_str())
        .bind(&lease.runtime_owner)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?
        .rows_affected();
        if drained != 1 {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let deleted = sqlx::query(
            r#"
            DELETE FROM provider_runtime_leases
            WHERE runtime_id = $1 AND execution_profile_id = $2
              AND runtime_role = $3 AND runtime_owner = $4
              AND state = 'draining'
            "#,
        )
        .bind(lease.runtime_id)
        .bind(lease.execution_profile_id)
        .bind(lease.role.as_str())
        .bind(&lease.runtime_owner)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?
        .rows_affected();
        if deleted != 1 {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        tx.commit().await.map_err(unavailable)
    }

    async fn list_profile_readiness(
        &self,
    ) -> Result<Vec<ProviderProfileReadiness>, ProviderTaskStoreError> {
        let rows: Vec<ProfileReadinessRow> = sqlx::query_as(
            r#"
            SELECT execution_profile_id, profile_key, provider_id, status,
                   active_submitters, active_pollers,
                   draining_submitters, draining_pollers
            FROM provider_profile_readiness
            ORDER BY profile_key
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        rows.into_iter().map(TryInto::try_into).collect()
    }
}

async fn update_runtime_lease(
    store: &PostgresProviderTaskStore,
    lease: &ProviderRuntimeLease,
    lease_ms: i64,
    draining: bool,
) -> Result<ProviderRuntimeLease, ProviderTaskStoreError> {
    validate_lease(lease, Some(lease_ms))?;
    let row: RuntimeLeaseRow = sqlx::query_as(
        r#"
        WITH db_clock AS MATERIALIZED (
          SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
        )
        UPDATE provider_runtime_leases
        SET state = CASE WHEN $5 THEN 'draining' ELSE state END,
            heartbeat_at_ms = db_clock.now_ms,
            lease_expires_at_ms = db_clock.now_ms + $6,
            updated_at_ms = db_clock.now_ms
        FROM db_clock
        WHERE runtime_id = $1 AND execution_profile_id = $2
          AND runtime_role = $3 AND runtime_owner = $4
          AND state IN ('active', 'draining')
          AND lease_expires_at_ms > db_clock.now_ms
        RETURNING runtime_id, execution_profile_id, runtime_role,
                  runtime_owner, state, heartbeat_at_ms, lease_expires_at_ms
        "#,
    )
    .bind(lease.runtime_id)
    .bind(lease.execution_profile_id)
    .bind(lease.role.as_str())
    .bind(&lease.runtime_owner)
    .bind(draining)
    .bind(lease_ms)
    .fetch_optional(&store.pool)
    .await
    .map_err(storage_conflict)?
    .ok_or(ProviderTaskStoreError::StaleLease)?;
    row.try_into()
}

async fn database_now(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<i64, ProviderTaskStoreError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn unavailable(_: sqlx::Error) -> ProviderTaskStoreError {
    ProviderTaskStoreError::Unavailable
}

fn storage_conflict(error: sqlx::Error) -> ProviderTaskStoreError {
    match error.as_database_error().and_then(|error| error.code()) {
        Some(code) if matches!(code.as_ref(), "23503" | "23505" | "23514" | "P0001") => {
            ProviderTaskStoreError::Conflict
        }
        _ => ProviderTaskStoreError::Unavailable,
    }
}
