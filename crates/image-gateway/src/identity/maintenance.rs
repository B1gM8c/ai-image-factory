use sqlx::PgPool;

use crate::ImageGatewayError;

#[derive(Clone)]
pub struct PostgresIdentityMaintenanceStore {
    pool: PgPool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IdentityMaintenanceOutcome {
    pub session_families: u64,
    pub login_throttles: u64,
    pub audit_events: u64,
}

impl PostgresIdentityMaintenanceStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn purge_expired(
        &self,
        session_retention_ms: u64,
        throttle_retention_ms: u64,
        audit_retention_ms: u64,
        batch_size: u32,
    ) -> Result<IdentityMaintenanceOutcome, ImageGatewayError> {
        if batch_size == 0 {
            return Err(ImageGatewayError::config(
                "identity maintenance batch size must be greater than zero",
            ));
        }
        let session_retention_ms = bounded_ms(session_retention_ms)?;
        let throttle_retention_ms = bounded_ms(throttle_retention_ms)?;
        let audit_retention_ms = bounded_ms(audit_retention_ms)?;
        let batch_size = i64::from(batch_size);
        let mut tx = self.pool.begin().await.map_err(maintenance_unavailable)?;
        let now_ms: i64 =
            sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT")
                .fetch_one(&mut *tx)
                .await
                .map_err(maintenance_unavailable)?;

        let session_families = sqlx::query(
            r#"
            WITH doomed AS (
                SELECT session_id
                FROM identity_session_families
                WHERE absolute_expires_at_ms <= $1
                   OR (revoked_at_ms IS NOT NULL AND revoked_at_ms <= $1)
                ORDER BY LEAST(absolute_expires_at_ms, COALESCE(revoked_at_ms, absolute_expires_at_ms)),
                         session_id
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            DELETE FROM identity_session_families family
            USING doomed
            WHERE family.session_id = doomed.session_id
            "#,
        )
        .bind(now_ms.saturating_sub(session_retention_ms))
        .bind(batch_size)
        .execute(&mut *tx)
        .await
        .map_err(maintenance_unavailable)?
        .rows_affected();

        let login_throttles = sqlx::query(
            r#"
            WITH doomed AS (
                SELECT throttle_key
                FROM identity_login_throttles
                WHERE updated_at_ms <= $1
                  AND (blocked_until_ms IS NULL OR blocked_until_ms <= $2)
                ORDER BY updated_at_ms, throttle_key
                FOR UPDATE SKIP LOCKED
                LIMIT $3
            )
            DELETE FROM identity_login_throttles throttle
            USING doomed
            WHERE throttle.throttle_key = doomed.throttle_key
            "#,
        )
        .bind(now_ms.saturating_sub(throttle_retention_ms))
        .bind(now_ms)
        .bind(batch_size)
        .execute(&mut *tx)
        .await
        .map_err(maintenance_unavailable)?
        .rows_affected();

        let audit_events = sqlx::query(
            r#"
            WITH doomed AS (
                SELECT event_id
                FROM identity_audit_events
                WHERE created_at_ms <= $1
                ORDER BY created_at_ms, event_id
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            DELETE FROM identity_audit_events audit
            USING doomed
            WHERE audit.event_id = doomed.event_id
            "#,
        )
        .bind(now_ms.saturating_sub(audit_retention_ms))
        .bind(batch_size)
        .execute(&mut *tx)
        .await
        .map_err(maintenance_unavailable)?
        .rows_affected();

        tx.commit().await.map_err(maintenance_unavailable)?;
        Ok(IdentityMaintenanceOutcome {
            session_families,
            login_throttles,
            audit_events,
        })
    }
}

fn bounded_ms(value: u64) -> Result<i64, ImageGatewayError> {
    i64::try_from(value)
        .map_err(|_| ImageGatewayError::config("identity maintenance retention is too large"))
}

fn maintenance_unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(error = %error, "identity maintenance storage unavailable");
    ImageGatewayError::service_unavailable("identity maintenance unavailable")
}
