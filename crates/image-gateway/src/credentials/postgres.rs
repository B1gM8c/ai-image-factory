use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{CredentialResolveError, OperationalCredential, OperationalCredentialResolver};

const DEFAULT_REFRESH_INTERVAL_MS: i64 = 6 * 60 * 60 * 1_000;
const MAX_BACKOFF_MS: i64 = 60 * 60 * 1_000;
const REFRESH_SKEW_MS: i64 = 15 * 60 * 1_000;

#[derive(Clone)]
pub struct PostgresCredentialStore {
    pool: PgPool,
}

#[derive(Clone, Debug)]
pub struct CredentialRefreshLease {
    pub provider_account_id: Uuid,
    pub provider_id: String,
    pub revision: i64,
    pub material_fingerprint_sha256: String,
    pub environment_ref: PathBuf,
    pub access_expires_at_ms: Option<i64>,
    pub lease_owner: String,
    pub lease_epoch: i64,
    pub lease_expires_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct CredentialRow {
    provider_account_id: Uuid,
    provider_id: String,
    revision: i64,
    material_kind: String,
    material_fingerprint_sha256: String,
    environment_ref: String,
    access_expires_at_ms: Option<i64>,
    lifecycle_state: String,
    now_ms: i64,
}

#[derive(sqlx::FromRow)]
struct RefreshRow {
    provider_account_id: Uuid,
    provider_id: String,
    revision: i64,
    material_fingerprint_sha256: String,
    environment_ref: String,
    access_expires_at_ms: Option<i64>,
    lease_owner: String,
    lease_epoch: i64,
    lease_expires_at_ms: i64,
}

impl PostgresCredentialStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn claim_refresh(
        &self,
        provider_account_id: Uuid,
        owner: &str,
        lease_ms: i64,
        force: bool,
    ) -> Result<Option<CredentialRefreshLease>, CredentialResolveError> {
        if provider_account_id.is_nil()
            || owner.is_empty()
            || owner.len() > 128
            || owner.chars().any(char::is_control)
            || !(1_000..=5 * 60_000).contains(&lease_ms)
        {
            return Err(CredentialResolveError::Invalid);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let row = sqlx::query_as::<_, RefreshRow>(
            r#"
            WITH db_clock AS (
                SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            ), candidate AS (
                SELECT account.provider_account_id, account.provider_id,
                       head.active_revision AS revision,
                       revision.material_fingerprint_sha256,
                       environment.environment_ref, revision.access_expires_at_ms,
                       head.lease_epoch, db_clock.now_ms
                FROM provider_account_credential_heads head
                JOIN provider_accounts account USING (provider_account_id)
                JOIN provider_account_credential_revisions revision
                  ON revision.provider_account_id = head.provider_account_id
                 AND revision.revision = head.active_revision
                JOIN provider_account_environments environment
                  ON environment.provider_account_id = account.provider_account_id
                 AND environment.provider_id = account.provider_id
                CROSS JOIN db_clock
                WHERE head.provider_account_id = $1
                  AND head.refresh_strategy IN ('broker_managed', 'cli_managed')
                  AND head.lifecycle_state <> 'reauth_required'
                  AND ($4 OR head.next_refresh_at_ms IS NULL
                       OR head.next_refresh_at_ms <= db_clock.now_ms
                       OR head.lifecycle_state = 'refreshing')
                  AND (head.lease_owner IS NULL
                       OR head.lease_expires_at_ms <= db_clock.now_ms)
                FOR UPDATE OF head
            ), claimed AS (
                UPDATE provider_account_credential_heads head
                SET lifecycle_state = 'refreshing', lease_owner = $2,
                    lease_epoch = candidate.lease_epoch + 1,
                    lease_expires_at_ms = candidate.now_ms + $3,
                    last_attempt_at_ms = candidate.now_ms,
                    updated_at_ms = candidate.now_ms,
                    control_version = head.control_version + 1
                FROM candidate
                WHERE head.provider_account_id = candidate.provider_account_id
                RETURNING candidate.provider_account_id, candidate.provider_id,
                          candidate.revision, candidate.material_fingerprint_sha256,
                          candidate.environment_ref, candidate.access_expires_at_ms,
                          head.lease_owner, head.lease_epoch, head.lease_expires_at_ms
            )
            SELECT * FROM claimed
            "#,
        )
        .bind(provider_account_id)
        .bind(owner)
        .bind(lease_ms)
        .bind(force)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| CredentialResolveError::Unavailable)?;
        let lease = row.map(refresh_row).transpose()?;
        if let Some(lease) = lease.as_ref() {
            let claimed_at_ms = database_now(&mut tx).await?;
            insert_event(&mut tx, lease, "refresh_claimed", None, None, claimed_at_ms).await?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(lease)
    }

    pub async fn promote_auth_file(
        &self,
        lease: &CredentialRefreshLease,
        fingerprint: &str,
        access_expires_at_ms: Option<i64>,
    ) -> Result<i64, CredentialResolveError> {
        if !valid_sha256(fingerprint) {
            return Err(CredentialResolveError::Invalid);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now_ms = database_now(&mut tx).await?;
        let current: Option<(i64, String)> = sqlx::query_as(
            r#"
            SELECT active_revision, lifecycle_state
            FROM provider_account_credential_heads
            WHERE provider_account_id = $1 AND lease_owner = $2
              AND lease_epoch = $3 AND lease_expires_at_ms > $4
            FOR UPDATE
            "#,
        )
        .bind(lease.provider_account_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_epoch)
        .bind(now_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some((active_revision, lifecycle)) = current else {
            return Err(CredentialResolveError::Unavailable);
        };
        if active_revision != lease.revision || lifecycle != "refreshing" {
            return Err(CredentialResolveError::Unavailable);
        }
        let next_revision = if fingerprint == lease.material_fingerprint_sha256
            && access_expires_at_ms == lease.access_expires_at_ms
        {
            active_revision
        } else {
            let next = active_revision
                .checked_add(1)
                .ok_or(CredentialResolveError::Invalid)?;
            sqlx::query(
                r#"
                INSERT INTO provider_account_credential_revisions
                  (provider_account_id, revision, material_kind,
                   material_fingerprint_sha256, access_expires_at_ms, created_at_ms)
                VALUES ($1, $2, 'auth_file', $3, $4, $5)
                "#,
            )
            .bind(lease.provider_account_id)
            .bind(next)
            .bind(fingerprint)
            .bind(access_expires_at_ms)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            next
        };
        let refresh_at = refresh_deadline(access_expires_at_ms, now_ms);
        sqlx::query(
            r#"
            UPDATE provider_account_credential_heads
            SET active_revision = $2, lifecycle_state = 'active',
                refresh_after_ms = $3, next_refresh_at_ms = $3,
                last_success_at_ms = $4, consecutive_failures = 0,
                last_error_code = NULL, lease_owner = NULL,
                lease_expires_at_ms = NULL, updated_at_ms = $4,
                control_version = control_version + 1
            WHERE provider_account_id = $1
            "#,
        )
        .bind(lease.provider_account_id)
        .bind(next_revision)
        .bind(refresh_at)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        insert_event(
            &mut tx,
            lease,
            "refresh_succeeded",
            Some(next_revision),
            None,
            now_ms,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(next_revision)
    }

    pub async fn complete_cli_managed_refresh(
        &self,
        lease: &CredentialRefreshLease,
    ) -> Result<(), CredentialResolveError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        self.complete_cli_managed_refresh_in_transaction(&mut tx, lease)
            .await?;
        tx.commit().await.map_err(unavailable)
    }

    pub(crate) async fn complete_cli_managed_refresh_in_transaction(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        lease: &CredentialRefreshLease,
    ) -> Result<(), CredentialResolveError> {
        let now_ms = database_now(tx).await?;
        let updated = sqlx::query(
            r#"
            UPDATE provider_account_credential_heads head
            SET lifecycle_state = 'active',
                refresh_after_ms = $4, next_refresh_at_ms = $4,
                last_success_at_ms = $5, consecutive_failures = 0,
                last_error_code = NULL, lease_owner = NULL,
                lease_expires_at_ms = NULL, updated_at_ms = $5,
                control_version = control_version + 1
            FROM provider_account_credential_revisions revision
            WHERE head.provider_account_id = $1
              AND head.active_revision = $2
              AND head.lease_owner = $3
              AND head.lease_epoch = $6
              AND head.lease_expires_at_ms > $5
              AND head.lifecycle_state = 'refreshing'
              AND head.refresh_strategy = 'cli_managed'
              AND revision.provider_account_id = head.provider_account_id
              AND revision.revision = head.active_revision
              AND revision.material_kind = 'system_keyring'
            "#,
        )
        .bind(lease.provider_account_id)
        .bind(lease.revision)
        .bind(&lease.lease_owner)
        .bind(now_ms.saturating_add(DEFAULT_REFRESH_INTERVAL_MS))
        .bind(now_ms)
        .bind(lease.lease_epoch)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(CredentialResolveError::Unavailable);
        }
        insert_event(
            tx,
            lease,
            "refresh_succeeded",
            Some(lease.revision),
            None,
            now_ms,
        )
        .await?;
        Ok(())
    }

    pub async fn fail_refresh(
        &self,
        lease: &CredentialRefreshLease,
        error_code: &str,
        reauthorization_required: bool,
    ) -> Result<(), CredentialResolveError> {
        if !valid_error_code(error_code) {
            return Err(CredentialResolveError::Invalid);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let row: Option<(i64, i32)> = sqlx::query_as(
            r#"
            SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
                   consecutive_failures
            FROM provider_account_credential_heads
            WHERE provider_account_id = $1 AND lease_owner = $2 AND lease_epoch = $3
            FOR UPDATE
            "#,
        )
        .bind(lease.provider_account_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_epoch)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some((now_ms, failures)) = row else {
            return Err(CredentialResolveError::Unavailable);
        };
        let failures = failures.saturating_add(1);
        let backoff = (1_i64 << failures.min(10))
            .saturating_mul(1_000)
            .min(MAX_BACKOFF_MS);
        let lifecycle = if reauthorization_required {
            "reauth_required"
        } else {
            "active"
        };
        sqlx::query(
            r#"
            UPDATE provider_account_credential_heads
            SET lifecycle_state = $2, next_refresh_at_ms = $3,
                consecutive_failures = $4, last_error_code = $5,
                lease_owner = NULL, lease_expires_at_ms = NULL,
                updated_at_ms = $6, control_version = control_version + 1
            WHERE provider_account_id = $1
            "#,
        )
        .bind(lease.provider_account_id)
        .bind(lifecycle)
        .bind(now_ms.saturating_add(backoff))
        .bind(failures)
        .bind(error_code)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        insert_event(
            &mut tx,
            lease,
            if reauthorization_required {
                "reauth_required"
            } else {
                "refresh_failed"
            },
            None,
            Some(error_code),
            now_ms,
        )
        .await?;
        tx.commit().await.map_err(unavailable)
    }
}

#[async_trait]
impl OperationalCredentialResolver for PostgresCredentialStore {
    async fn resolve(
        &self,
        provider_account_id: Uuid,
    ) -> Result<OperationalCredential, CredentialResolveError> {
        if provider_account_id.is_nil() {
            return Err(CredentialResolveError::Invalid);
        }
        let row = sqlx::query_as::<_, CredentialRow>(
            r#"
            SELECT account.provider_account_id, account.provider_id,
                   revision.revision, revision.material_kind,
                   revision.material_fingerprint_sha256,
                   environment.environment_ref, revision.access_expires_at_ms,
                   head.lifecycle_state,
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            FROM provider_accounts account
            JOIN provider_account_credential_heads head USING (provider_account_id)
            JOIN provider_account_credential_revisions revision
              ON revision.provider_account_id = head.provider_account_id
             AND revision.revision = head.active_revision
            JOIN provider_account_environments environment
              ON environment.provider_account_id = account.provider_account_id
             AND environment.provider_id = account.provider_id
            WHERE account.provider_account_id = $1
              AND account.state = 'enabled' AND environment.state = 'active'
            "#,
        )
        .bind(provider_account_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?
        .ok_or(CredentialResolveError::Unavailable)?;
        match row.lifecycle_state.as_str() {
            "active" | "refresh_due" => {}
            "reauth_required" => return Err(CredentialResolveError::ReauthorizationRequired),
            "unsupported" => return Err(CredentialResolveError::Unsupported),
            "refreshing" => return Err(CredentialResolveError::Unavailable),
            _ => return Err(CredentialResolveError::Invalid),
        }
        if !matches!(row.material_kind.as_str(), "auth_file" | "system_keyring")
            || !valid_sha256(&row.material_fingerprint_sha256)
        {
            return Err(CredentialResolveError::Unsupported);
        }
        if row
            .access_expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms <= row.now_ms)
        {
            return Err(CredentialResolveError::Unavailable);
        }
        let environment_ref = PathBuf::from(row.environment_ref);
        if !environment_ref.is_absolute() {
            return Err(CredentialResolveError::Invalid);
        }
        Ok(OperationalCredential {
            provider_account_id: row.provider_account_id,
            provider_id: row.provider_id,
            revision: row.revision,
            material_kind: row.material_kind,
            material_fingerprint_sha256: row.material_fingerprint_sha256,
            environment_ref: Arc::new(environment_ref),
            access_expires_at_ms: row.access_expires_at_ms,
        })
    }
}

async fn insert_event(
    tx: &mut Transaction<'_, Postgres>,
    lease: &CredentialRefreshLease,
    event_type: &str,
    to_revision: Option<i64>,
    error_code: Option<&str>,
    now_ms: i64,
) -> Result<(), CredentialResolveError> {
    sqlx::query(
        r#"
        INSERT INTO provider_account_credential_events
          (credential_event_id, provider_account_id, event_type, from_revision,
           to_revision, lease_epoch, executor_execution_id, error_code, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(lease.provider_account_id)
    .bind(event_type)
    .bind(lease.revision)
    .bind(to_revision)
    .bind(lease.lease_epoch)
    .bind(error_code)
    .bind(now_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, CredentialResolveError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn refresh_row(row: RefreshRow) -> Result<CredentialRefreshLease, CredentialResolveError> {
    let environment_ref = PathBuf::from(row.environment_ref);
    if !environment_ref.is_absolute()
        || !valid_sha256(&row.material_fingerprint_sha256)
        || row.lease_epoch <= 0
        || row.lease_expires_at_ms <= 0
    {
        return Err(CredentialResolveError::Invalid);
    }
    Ok(CredentialRefreshLease {
        provider_account_id: row.provider_account_id,
        provider_id: row.provider_id,
        revision: row.revision,
        material_fingerprint_sha256: row.material_fingerprint_sha256,
        environment_ref,
        access_expires_at_ms: row.access_expires_at_ms,
        lease_owner: row.lease_owner,
        lease_epoch: row.lease_epoch,
        lease_expires_at_ms: row.lease_expires_at_ms,
    })
}

fn refresh_deadline(expires_at_ms: Option<i64>, now_ms: i64) -> i64 {
    expires_at_ms
        .map(|expires| expires.saturating_sub(REFRESH_SKEW_MS).max(now_ms))
        .unwrap_or_else(|| now_ms.saturating_add(DEFAULT_REFRESH_INTERVAL_MS))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn valid_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn unavailable(error: sqlx::Error) -> CredentialResolveError {
    tracing::warn!(error = ?error, "provider credential database operation failed");
    CredentialResolveError::Unavailable
}
