use std::time::Duration;

use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, pool::PoolConnection};

use super::ExecutorClaimScope;

const OWNER_GUARD_DOMAIN: &[u8] = b"ai-image-factory:executor-owner-guard:v1";

pub struct PostgresExecutorOwnerGuard {
    connection: PoolConnection<Postgres>,
    backend_pid: i32,
    check_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExecutorOwnerGuardError {
    #[error("another executord already owns this owner and scope")]
    AlreadyActive,
    #[error("executor owner guard is unavailable")]
    Unavailable,
}

impl PostgresExecutorOwnerGuard {
    pub async fn acquire(
        pool: &PgPool,
        owner: &str,
        scope: &ExecutorClaimScope,
        timeout: Duration,
    ) -> Result<Self, ExecutorOwnerGuardError> {
        if owner.is_empty()
            || scope.execution_profile_id.is_nil()
            || scope.provider_id.is_empty()
            || scope.command_schema.is_empty()
            || scope.adapter_revision.is_empty()
            || timeout.is_zero()
        {
            return Err(ExecutorOwnerGuardError::Unavailable);
        }
        let mut connection = tokio::time::timeout(timeout, pool.acquire())
            .await
            .map_err(|_| ExecutorOwnerGuardError::Unavailable)?
            .map_err(|_| ExecutorOwnerGuardError::Unavailable)?;
        connection.close_on_drop();
        let lock_key = lock_key(owner, scope);
        let acquired: bool = tokio::time::timeout(
            timeout,
            sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(lock_key)
                .fetch_one(&mut *connection),
        )
        .await
        .map_err(|_| ExecutorOwnerGuardError::Unavailable)?
        .map_err(|_| ExecutorOwnerGuardError::Unavailable)?;
        if !acquired {
            return Err(ExecutorOwnerGuardError::AlreadyActive);
        }
        let backend_pid = tokio::time::timeout(
            timeout,
            sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()").fetch_one(&mut *connection),
        )
        .await
        .map_err(|_| ExecutorOwnerGuardError::Unavailable)?
        .map_err(|_| ExecutorOwnerGuardError::Unavailable)?;
        Ok(Self {
            connection,
            backend_pid,
            check_timeout: timeout,
        })
    }

    pub async fn verify(&mut self) -> Result<(), ExecutorOwnerGuardError> {
        let backend_pid = tokio::time::timeout(
            self.check_timeout,
            sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
                .fetch_one(&mut *self.connection),
        )
        .await
        .map_err(|_| ExecutorOwnerGuardError::Unavailable)?
        .map_err(|_| ExecutorOwnerGuardError::Unavailable)?;
        if backend_pid == self.backend_pid {
            Ok(())
        } else {
            Err(ExecutorOwnerGuardError::Unavailable)
        }
    }

    pub fn backend_pid(&self) -> i32 {
        self.backend_pid
    }
}

fn lock_key(owner: &str, scope: &ExecutorClaimScope) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(OWNER_GUARD_DOMAIN);
    let profile_id = scope.execution_profile_id.to_string();
    for value in [
        owner,
        &profile_id,
        &scope.provider_id,
        &scope.command_schema,
        &scope.adapter_revision,
    ] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    let digest = hasher.finalize();
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_key_is_stable_and_fenced_by_every_identity_component() {
        let scope = ExecutorClaimScope {
            execution_profile_id: uuid::Uuid::from_u128(1),
            provider_id: "openai-codex".to_string(),
            command_schema: "openai.images.generation.v1".to_string(),
            adapter_revision: "openai-codex-generation-v1".to_string(),
        };
        let base = lock_key("owner-a", &scope);

        assert_eq!(base, lock_key("owner-a", &scope));
        assert_ne!(base, lock_key("owner-b", &scope));
        assert_ne!(
            base,
            lock_key(
                "owner-a",
                &ExecutorClaimScope {
                    execution_profile_id: uuid::Uuid::from_u128(2),
                    ..scope.clone()
                }
            )
        );
        assert_ne!(
            base,
            lock_key(
                "owner-a",
                &ExecutorClaimScope {
                    provider_id: "other-provider".to_string(),
                    ..scope.clone()
                }
            )
        );
        assert_ne!(
            base,
            lock_key(
                "owner-a",
                &ExecutorClaimScope {
                    command_schema: "openai.images.edit.v1".to_string(),
                    ..scope.clone()
                }
            )
        );
        assert_ne!(
            base,
            lock_key(
                "owner-a",
                &ExecutorClaimScope {
                    adapter_revision: "openai-codex-generation-v2".to_string(),
                    ..scope
                }
            )
        );
    }
}
