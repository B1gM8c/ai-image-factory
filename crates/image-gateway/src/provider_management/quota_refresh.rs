//! Optional, bounded quota observations. No model turns or image jobs are created.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sqlx::{PgPool, Postgres, Transaction};
use tokio::{
    task::{JoinHandle, JoinSet},
    time::{MissedTickBehavior, timeout},
};
use uuid::Uuid;

use super::{
    PostgresProviderManagementService,
    codex_app_server::{CodexAccountSnapshot, CodexQuotaSnapshot},
    postgres::persist_quota,
};
use crate::ImageGatewayError;

const DATABASE_TIMEOUT: Duration = Duration::from_secs(5);
const OBSERVER_TIMEOUT: Duration = Duration::from_secs(90);
const LEASE_MS: i64 = 105_000;
const BATCH_LIMIT: i64 = 16;

#[derive(Clone, Debug, Serialize, utoipa::ToSchema)]
pub struct CodexQuotaRefreshRuntimeView {
    pub enabled: bool,
    pub running: bool,
    pub healthy: bool,
    pub last_pass_at_ms: Option<i64>,
    pub in_flight: u64,
    pub attempts: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub timed_out: u64,
}

#[derive(Clone, Debug)]
pub(super) struct QuotaRefreshConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub concurrency: usize,
}

impl Default for QuotaRefreshConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: 15,
            concurrency: 2,
        }
    }
}

impl QuotaRefreshConfig {
    pub fn from_env() -> Result<Self, ImageGatewayError> {
        let enabled = match std::env::var("GATEWAY_CODEX_QUOTA_AUTO_REFRESH_ENABLED").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("false" | "0") => false,
            Ok("true" | "1") => true,
            _ => {
                return Err(ImageGatewayError::config(
                    "GATEWAY_CODEX_QUOTA_AUTO_REFRESH_ENABLED must be true or false",
                ));
            }
        };
        Ok(Self {
            enabled,
            interval_seconds: env_number(
                "GATEWAY_CODEX_QUOTA_AUTO_REFRESH_INTERVAL_SECONDS",
                15,
                5,
                300,
            )?,
            concurrency: env_number("GATEWAY_CODEX_QUOTA_AUTO_REFRESH_CONCURRENCY", 2, 1, 4)?
                as usize,
        })
    }
}

fn env_number(name: &str, default: u64, min: u64, max: u64) -> Result<u64, ImageGatewayError> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (min..=max).contains(n))
            .ok_or_else(|| {
                ImageGatewayError::config(format!("{name} must be between {min} and {max}"))
            }),
        _ => Err(ImageGatewayError::config(format!("{name} is invalid"))),
    }
}

pub(super) struct QuotaRefreshRuntime {
    config: QuotaRefreshConfig,
    running: AtomicBool,
    last_pass: AtomicI64,
    in_flight: AtomicU64,
    attempts: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    timed_out: AtomicU64,
}

impl QuotaRefreshRuntime {
    pub fn new(config: QuotaRefreshConfig) -> Self {
        Self {
            config,
            running: AtomicBool::new(false),
            last_pass: AtomicI64::new(0),
            in_flight: AtomicU64::new(0),
            attempts: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            timed_out: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> CodexQuotaRefreshRuntimeView {
        let running = self.running.load(Ordering::Relaxed);
        let last = self.last_pass.load(Ordering::Relaxed);
        CodexQuotaRefreshRuntimeView {
            enabled: self.config.enabled,
            running,
            healthy: !self.config.enabled
                || (running
                    && last > 0
                    && wall_now_ms().saturating_sub(last)
                        <= (self.config.interval_seconds * 3).max(45) as i64 * 1000),
            last_pass_at_ms: (last > 0).then_some(last),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            attempts: self.attempts.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
        }
    }
}

struct RunningGuard(Arc<QuotaRefreshRuntime>);
impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::Relaxed);
    }
}
struct FlightGuard(Arc<QuotaRefreshRuntime>);
impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl PostgresProviderManagementService {
    /// Start at most once per service. Disabled means no task, DB scans, or CLI calls.
    pub fn spawn_codex_quota_refresh(self: &Arc<Self>) -> Option<JoinHandle<()>> {
        if !self.quota_refresh.config.enabled
            || self.quota_refresh.running.swap(true, Ordering::Relaxed)
        {
            return None;
        }
        let service = self.clone();
        let guard = RunningGuard(service.quota_refresh.clone());
        Some(tokio::spawn(async move {
            let _guard = guard;
            let mut workers = JoinSet::new();
            let mut interval = tokio::time::interval(Duration::from_secs(
                service.quota_refresh.config.interval_seconds,
            ));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Even at capacity, a bounded DB pass proves the loop is alive.
                        match timeout(DATABASE_TIMEOUT, due_accounts(&service.pool)).await {
                            Ok(Ok(accounts)) => {
                                service.quota_refresh.last_pass.store(wall_now_ms(), Ordering::Relaxed);
                                let capacity = service.quota_refresh.config.concurrency.saturating_sub(workers.len());
                                for account in accounts.into_iter().take(capacity) {
                                    let service = service.clone();
                                    workers.spawn(async move {
                                        if let Err(error) = service.refresh_codex_quota_bounded(account, false).await {
                                            tracing::warn!(%account, code = ?error.error_code(), "automatic Codex quota refresh failed");
                                        }
                                    });
                                }
                            }
                            _ => tracing::warn!("automatic Codex quota candidate scan failed or timed out"),
                        }
                    }
                    Some(result) = workers.join_next(), if !workers.is_empty() => {
                        if result.is_err() { tracing::warn!("automatic Codex quota worker stopped unexpectedly"); }
                    }
                }
            }
        }))
    }

    pub(super) async fn refresh_codex_quota_bounded(
        &self,
        account: Uuid,
        manual: bool,
    ) -> Result<(), ImageGatewayError> {
        self.refresh_codex_quota_observation(
            account,
            manual,
            OBSERVER_TIMEOUT,
            self.observe_codex_account_quota(account),
        )
        .await
    }

    async fn refresh_codex_quota_observation(
        &self,
        account: Uuid,
        manual: bool,
        deadline: Duration,
        observation: impl std::future::Future<
            Output = Result<(CodexAccountSnapshot, CodexQuotaSnapshot), ImageGatewayError>,
        >,
    ) -> Result<(), ImageGatewayError> {
        let lease = timeout(DATABASE_TIMEOUT, claim(&self.pool, account, manual))
            .await
            .map_err(|_| unavailable())??
            .ok_or_else(|| {
                ImageGatewayError::conflict(
                    "A Codex quota refresh is already in progress or not due",
                    Some("provider_account_id".to_owned()),
                    "quota_refresh_in_progress",
                )
            })?;
        self.quota_refresh.attempts.fetch_add(1, Ordering::Relaxed);
        self.quota_refresh.in_flight.fetch_add(1, Ordering::Relaxed);
        let _flight = FlightGuard(self.quota_refresh.clone());
        let observation = timeout(deadline, observation).await;
        let (result, code) = match observation {
            Ok(Ok(value))
                if value
                    .1
                    .windows
                    .iter()
                    .any(|w| w.resets_at_ms.is_none_or(|reset| reset > wall_now_ms())) =>
            {
                (Ok(value), None)
            }
            Ok(Ok(_)) => (
                Err(ImageGatewayError::service_unavailable(
                    "Codex quota has no usable windows",
                )),
                Some("quota_windows_unavailable"),
            ),
            Ok(Err(error)) => (Err(error), Some("quota_observer_failed")),
            Err(_) => {
                self.quota_refresh.timed_out.fetch_add(1, Ordering::Relaxed);
                (
                    Err(ImageGatewayError::service_unavailable(
                        "Codex quota refresh timed out",
                    )),
                    Some("quota_observer_timeout"),
                )
            }
        };
        let published = timeout(
            DATABASE_TIMEOUT,
            finish(&self.pool, &lease, result.as_ref().ok(), code),
        )
        .await
        .unwrap_or_else(|_| Err(unavailable()));
        if result.is_ok() && published.is_ok() {
            self.quota_refresh.succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.quota_refresh.failed.fetch_add(1, Ordering::Relaxed);
        }
        published?;
        result.map(|_| ())
    }
}

// Use the strictest active route TTL. Failed observations are never fresh, and
// a reset is due only if it happened after the last successful observation.
async fn due_accounts(pool: &PgPool) -> Result<Vec<Uuid>, ImageGatewayError> {
    let mut tx = pool.begin().await.map_err(|_| unavailable())?;
    sqlx::query("SET LOCAL statement_timeout = '4s'")
        .execute(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
    let accounts = select_due(&mut *tx, None).await?;
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(accounts)
}

async fn select_due<'e>(
    executor: impl sqlx::Executor<'e, Database = Postgres>,
    account: Option<Uuid>,
) -> Result<Vec<Uuid>, ImageGatewayError> {
    sqlx::query_scalar(r#"
        WITH eligible AS (
            SELECT a.provider_account_id, MIN(r.quota_freshness_ms) AS ttl
            FROM provider_route_heads h
            JOIN provider_routes r ON r.route_id=h.route_id AND r.revision=h.current_revision
            JOIN provider_route_members m ON m.route_id=r.route_id AND m.route_revision=r.revision
            JOIN provider_execution_profiles p ON p.execution_profile_id=m.execution_profile_id
            JOIN provider_accounts a ON a.provider_account_id=m.provider_account_id
            JOIN provider_credential_pools pool ON pool.credential_pool_id=a.credential_pool_id
            JOIN provider_account_environments e ON e.provider_account_id=a.provider_account_id
            JOIN provider_account_execution_controls c ON c.provider_account_id=a.provider_account_id
            JOIN provider_account_credential_heads credential ON credential.provider_account_id=a.provider_account_id
            JOIN provider_account_operations operation ON operation.provider_account_id=a.provider_account_id AND operation.operation_id=r.operation_id
            JOIN executor_resource_policies policy ON policy.resource_policy_id=p.resource_policy_id AND policy.revision=p.resource_policy_revision
            WHERE h.state='enabled' AND r.state='enabled' AND m.state='enabled'
                AND a.provider_id='openai-codex' AND a.state='enabled' AND p.state='enabled'
                AND pool.state='enabled' AND e.state='active' AND c.lifecycle_state='active'
                AND credential.lifecycle_state IN ('active','refresh_due') AND policy.state='enabled'
                AND operation.state='enabled'
            GROUP BY a.provider_account_id
        )
        SELECT e.provider_account_id FROM eligible e
        LEFT JOIN provider_account_quota_snapshots q USING(provider_account_id)
        LEFT JOIN provider_account_quota_refreshes f USING(provider_account_id)
        WHERE ($2::UUID IS NULL OR e.provider_account_id=$2)
          AND COALESCE(f.next_attempt_at_ms,0) <= (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint
          AND COALESCE(f.lease_expires_at_ms,0) <= (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint
          AND (q.status IS DISTINCT FROM 'observed'
            OR q.observed_at_ms + e.ttl - LEAST(60000,e.ttl/2) <= (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint
            OR NOT EXISTS (SELECT 1 FROM provider_account_quota_windows w WHERE w.provider_account_id=e.provider_account_id
                AND w.observed_at_ms=q.observed_at_ms AND (w.resets_at_ms IS NULL OR w.resets_at_ms>(EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint))
            OR EXISTS (SELECT 1 FROM provider_account_quota_windows w WHERE w.provider_account_id=e.provider_account_id
                AND w.resets_at_ms > q.observed_at_ms AND w.resets_at_ms <= (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint))
        ORDER BY COALESCE(f.last_attempt_at_ms,0),e.provider_account_id LIMIT $1
    "#).bind(BATCH_LIMIT).bind(account).fetch_all(executor).await.map_err(|_| unavailable())
}

#[derive(Debug, sqlx::FromRow)]
struct Lease {
    provider_account_id: Uuid,
    lease_token: Uuid,
    lease_epoch: i64,
}

async fn claim(
    pool: &PgPool,
    account: Uuid,
    manual: bool,
) -> Result<Option<Lease>, ImageGatewayError> {
    let mut tx = pool.begin().await.map_err(|_| unavailable())?;
    sqlx::query("SET LOCAL statement_timeout = '4s'")
        .execute(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
    sqlx::query(
        r#"
        INSERT INTO provider_account_quota_refreshes(provider_account_id)
        SELECT a.provider_account_id
        FROM provider_accounts a JOIN provider_account_environments e USING(provider_account_id)
        WHERE a.provider_account_id=$1 AND a.provider_id='openai-codex'
        ON CONFLICT(provider_account_id) DO NOTHING
    "#,
    )
    .bind(account)
    .execute(&mut *tx)
    .await
    .map_err(|_| unavailable())?;
    let row = sqlx::query("SELECT provider_account_id FROM provider_account_quota_refreshes WHERE provider_account_id=$1 FOR UPDATE")
        .bind(account).fetch_optional(&mut *tx).await.map_err(|_| unavailable())?;
    if row.is_none() {
        return Err(ImageGatewayError::not_found(
            "Managed Codex account not found",
            Some("provider_account_id".to_owned()),
            "provider_account_not_found",
        ));
    }
    // Recheck after the same row lock used by publication. A manual refresh may
    // have completed since the scheduler scan; that must not start another CLI.
    if !manual && select_due(&mut *tx, Some(account)).await?.is_empty() {
        tx.commit().await.map_err(|_| unavailable())?;
        return Ok(None);
    }
    let lease = sqlx::query_as(r#"
        UPDATE provider_account_quota_refreshes SET lease_token=$2,lease_epoch=lease_epoch+1,
            lease_expires_at_ms=(EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint+$3,
            last_attempt_at_ms=(EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint
        WHERE provider_account_id=$1 AND COALESCE(lease_expires_at_ms,0)<=(EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint
        RETURNING provider_account_id,lease_token,lease_epoch
    "#).bind(account).bind(Uuid::new_v4()).bind(LEASE_MS)
        .fetch_optional(&mut *tx).await.map_err(|_| unavailable())?;
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(lease)
}

async fn finish(
    pool: &PgPool,
    lease: &Lease,
    observation: Option<&(CodexAccountSnapshot, CodexQuotaSnapshot)>,
    error: Option<&str>,
) -> Result<(), ImageGatewayError> {
    let mut tx = pool.begin().await.map_err(|_| unavailable())?;
    // Also bounds a lock wait on publication, independently of Rust cancellation.
    sqlx::query("SET LOCAL statement_timeout = '4s'")
        .execute(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
    let now: i64 =
        sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| unavailable())?;
    let failures: Option<i32> = sqlx::query_scalar(r#"
        SELECT consecutive_failures FROM provider_account_quota_refreshes
        WHERE provider_account_id=$1 AND lease_token=$2 AND lease_epoch=$3 AND lease_expires_at_ms>$4
        FOR UPDATE
    "#).bind(lease.provider_account_id).bind(lease.lease_token).bind(lease.lease_epoch).bind(now)
        .fetch_optional(&mut *tx).await.map_err(|_| unavailable())?;
    let failures = failures.ok_or_else(|| {
        ImageGatewayError::conflict(
            "Quota observation lease expired",
            None,
            "quota_refresh_lease_lost",
        )
    })?;
    if let Some((account, quota)) = observation {
        sqlx::query("UPDATE provider_account_environments SET account_email=COALESCE($2,account_email),updated_at_ms=$3 WHERE provider_account_id=$1")
            .bind(lease.provider_account_id).bind(&account.email).bind(now).execute(&mut *tx).await.map_err(|_| unavailable())?;
        persist_quota(&mut tx, lease.provider_account_id, quota, now).await?;
    } else {
        mark_unavailable(
            &mut tx,
            lease.provider_account_id,
            error.unwrap_or("quota_observer_failed"),
            now,
        )
        .await?;
    }
    let failures = if observation.is_some() {
        0
    } else {
        failures.saturating_add(1)
    };
    let delay = backoff_ms(failures);
    let updated = sqlx::query(
        r#"UPDATE provider_account_quota_refreshes SET lease_token=NULL,lease_expires_at_ms=NULL,
        next_attempt_at_ms=$4,consecutive_failures=$5,last_completed_at_ms=$6,last_error_code=$7
        WHERE provider_account_id=$1 AND lease_token=$2 AND lease_epoch=$3
            AND lease_expires_at_ms>(EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint"#,
    )
    .bind(lease.provider_account_id)
    .bind(lease.lease_token)
    .bind(lease.lease_epoch)
    .bind(now + delay)
    .bind(failures)
    .bind(now)
    .bind(error)
    .execute(&mut *tx)
    .await
    .map_err(|_| unavailable())?;
    if updated.rows_affected() != 1 {
        return Err(ImageGatewayError::conflict(
            "Quota observation lease expired",
            None,
            "quota_refresh_lease_lost",
        ));
    }
    tx.commit().await.map_err(|_| unavailable())
}

fn backoff_ms(failures: i32) -> i64 {
    if failures == 0 {
        0
    } else {
        (30_000_i64 * (1_i64 << (failures - 1).clamp(0, 4))).min(300_000)
    }
}

async fn mark_unavailable(
    tx: &mut Transaction<'_, Postgres>,
    account: Uuid,
    code: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"INSERT INTO provider_account_quota_snapshots
        (provider_account_id,provider_id,status,observed_at_ms,last_error_code)
        VALUES($1,'openai-codex','unavailable',$2,$3)
        ON CONFLICT(provider_account_id) DO UPDATE SET status='unavailable',last_error_code=$3
        -- Preserve the last successful observation time; a failure is not freshness.
    "#,
    )
    .bind(account)
    .bind(now)
    .bind(code)
    .execute(&mut **tx)
    .await
    .map_err(|_| unavailable())?;
    Ok(())
}

fn unavailable() -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Quota refresh storage unavailable")
}
fn wall_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
#[path = "quota_refresh_tests.rs"]
mod tests;
