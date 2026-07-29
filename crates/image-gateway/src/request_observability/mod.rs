use std::{
    env,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{ImageGatewayError, auth::AuthContext};

const DEFAULT_CHANNEL_CAPACITY: usize = 4_096;
const MAX_BATCH_SIZE: usize = 128;
const DEFAULT_RETENTION_DAYS: u64 = 90;
const MAX_RETENTION_DAYS: u64 = 3_650;
const RETENTION_BATCH_SIZE: i64 = 10_000;

tokio::task_local! {
    pub(crate) static ACTIVE_REQUEST_OBSERVATION: RequestObservationContext;
}

#[derive(Clone, Debug)]
pub(crate) struct RequestObservationContext {
    actor: Arc<OnceLock<RequestObservationActor>>,
}

impl RequestObservationContext {
    pub(crate) fn new() -> Self {
        Self {
            actor: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn actor(&self) -> Option<RequestObservationActor> {
        self.actor.get().cloned()
    }

    fn capture_auth(&self, auth: &AuthContext) {
        let auth_kind = if auth.api_key_id.is_some() {
            "api_key"
        } else if auth.actor_user_id.is_some() {
            "user_session"
        } else {
            "legacy"
        };
        let _ = self.actor.set(RequestObservationActor {
            tenant_id: auth.tenant_id.clone(),
            project_id: auth.project_id.clone(),
            service_account_id: auth.service_account_id.clone(),
            api_key_id: auth.api_key_id.clone(),
            credential_owner_user_id: auth.credential_owner_user_id,
            actor_user_id: auth.actor_user_id,
            auth_kind: auth_kind.to_owned(),
        });
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RequestObservationActor {
    pub(crate) tenant_id: String,
    pub(crate) project_id: String,
    pub(crate) service_account_id: Option<String>,
    pub(crate) api_key_id: Option<String>,
    pub(crate) credential_owner_user_id: Option<Uuid>,
    pub(crate) actor_user_id: Option<Uuid>,
    pub(crate) auth_kind: String,
}

pub(crate) fn capture_auth(auth: &AuthContext) {
    let _ = ACTIVE_REQUEST_OBSERVATION.try_with(|context| context.capture_auth(auth));
}

#[derive(Clone, Debug)]
pub(crate) struct ResponseErrorCode(pub(crate) Option<String>);

#[derive(Clone, Debug)]
pub struct RequestObservationRecord {
    pub request_id: String,
    pub source: String,
    pub method: String,
    pub route_pattern: String,
    pub request_path: String,
    pub status_code: u16,
    pub duration_ms: i64,
    pub error_code: Option<String>,
    pub idempotency_key_digest: Option<String>,
    pub tenant_id: Option<String>,
    pub project_id: Option<String>,
    pub service_account_id: Option<String>,
    pub api_key_id: Option<String>,
    pub credential_owner_user_id: Option<Uuid>,
    pub actor_user_id: Option<Uuid>,
    pub auth_kind: Option<String>,
    pub created_at_ms: i64,
    pub completed_at_ms: i64,
}

#[derive(Clone, Default)]
pub struct RequestObservationSink {
    sender: Option<mpsc::Sender<RequestObservationRecord>>,
    dropped: Arc<AtomicU64>,
}

impl RequestObservationSink {
    pub fn from_env(pool: PgPool) -> Result<Self, ImageGatewayError> {
        let retention_days = retention_days_from_env()?;
        let (sender, receiver) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY);
        tokio::spawn(run_writer(pool.clone(), receiver));
        tokio::spawn(run_retention(pool, retention_days));
        Ok(Self {
            sender: Some(sender),
            dropped: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn submit(&self, record: RequestObservationRecord) {
        let Some(sender) = &self.sender else {
            return;
        };
        if sender.try_send(record).is_err() {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped == 1 || dropped.is_multiple_of(100) {
                tracing::warn!(
                    dropped,
                    "request observation queue is full; metadata log was dropped"
                );
            }
        }
    }
}

pub(crate) fn digest_idempotency_key(value: Option<&str>) -> Option<String> {
    value.map(|value| hex::encode(Sha256::digest(value.as_bytes())))
}

async fn run_writer(pool: PgPool, mut receiver: mpsc::Receiver<RequestObservationRecord>) {
    while let Some(first) = receiver.recv().await {
        let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
        batch.push(first);
        while batch.len() < MAX_BATCH_SIZE {
            match receiver.try_recv() {
                Ok(record) => batch.push(record),
                Err(_) => break,
            }
        }
        if let Err(error) = persist_batch(&pool, &batch).await {
            tracing::warn!(
                ?error,
                request_count = batch.len(),
                "request observation batch could not be persisted"
            );
        }
    }
}

async fn persist_batch(
    pool: &PgPool,
    records: &[RequestObservationRecord],
) -> Result<(), sqlx::Error> {
    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO gateway_request_observations (
          request_id, source, method, route_pattern, request_path,
          status_code, duration_ms, error_code, idempotency_key_digest,
          tenant_id, project_id, service_account_id, api_key_id,
          credential_owner_user_id, actor_user_id, auth_kind,
          created_at_ms, completed_at_ms
        )
        "#,
    );
    query.push_values(records, |mut row, record| {
        row.push_bind(&record.request_id)
            .push_bind(&record.source)
            .push_bind(&record.method)
            .push_bind(&record.route_pattern)
            .push_bind(&record.request_path)
            .push_bind(i32::from(record.status_code))
            .push_bind(record.duration_ms)
            .push_bind(&record.error_code)
            .push_bind(&record.idempotency_key_digest)
            .push_bind(&record.tenant_id)
            .push_bind(&record.project_id)
            .push_bind(&record.service_account_id)
            .push_bind(&record.api_key_id)
            .push_bind(record.credential_owner_user_id)
            .push_bind(record.actor_user_id)
            .push_bind(&record.auth_kind)
            .push_bind(record.created_at_ms)
            .push_bind(record.completed_at_ms);
    });
    query.push(
        r#"
        ON CONFLICT (request_id) DO UPDATE SET
          source = EXCLUDED.source,
          method = EXCLUDED.method,
          route_pattern = EXCLUDED.route_pattern,
          request_path = EXCLUDED.request_path,
          status_code = EXCLUDED.status_code,
          duration_ms = EXCLUDED.duration_ms,
          error_code = EXCLUDED.error_code,
          idempotency_key_digest = EXCLUDED.idempotency_key_digest,
          tenant_id = COALESCE(EXCLUDED.tenant_id, gateway_request_observations.tenant_id),
          project_id = COALESCE(EXCLUDED.project_id, gateway_request_observations.project_id),
          service_account_id = COALESCE(
            EXCLUDED.service_account_id,
            gateway_request_observations.service_account_id
          ),
          api_key_id = COALESCE(
            EXCLUDED.api_key_id,
            gateway_request_observations.api_key_id
          ),
          credential_owner_user_id = COALESCE(
            EXCLUDED.credential_owner_user_id,
            gateway_request_observations.credential_owner_user_id
          ),
          actor_user_id = COALESCE(
            EXCLUDED.actor_user_id,
            gateway_request_observations.actor_user_id
          ),
          auth_kind = COALESCE(
            EXCLUDED.auth_kind,
            gateway_request_observations.auth_kind
          ),
          created_at_ms = LEAST(
            EXCLUDED.created_at_ms,
            gateway_request_observations.created_at_ms
          ),
          completed_at_ms = GREATEST(
            EXCLUDED.completed_at_ms,
            gateway_request_observations.completed_at_ms
          )
        "#,
    );
    query.build().execute(pool).await?;
    Ok(())
}

async fn run_retention(pool: PgPool, retention_days: u64) {
    let retention_ms = retention_days
        .saturating_mul(24)
        .saturating_mul(60)
        .saturating_mul(60)
        .saturating_mul(1_000)
        .min(i64::MAX as u64) as i64;
    let mut interval = tokio::time::interval(Duration::from_secs(6 * 60 * 60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let cutoff_ms = now_ms().saturating_sub(retention_ms);
        match delete_expired_batch(&pool, cutoff_ms).await {
            Ok(deleted) if deleted > 0 => {
                tracing::info!(deleted, retention_days, "expired request logs deleted")
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(?error, "request log retention pass failed"),
        }
    }
}

async fn delete_expired_batch(pool: &PgPool, cutoff_ms: i64) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        r#"
        WITH expired AS (
          SELECT request_id
          FROM gateway_request_observations
          WHERE completed_at_ms < $1
          ORDER BY completed_at_ms, request_id
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        DELETE FROM gateway_request_observations observation
        USING expired
        WHERE observation.request_id = expired.request_id
        "#,
    )
    .bind(cutoff_ms)
    .bind(RETENTION_BATCH_SIZE)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

fn retention_days_from_env() -> Result<u64, ImageGatewayError> {
    let Some(value) = env::var("AIF_REQUEST_LOG_RETENTION_DAYS").ok() else {
        return Ok(DEFAULT_RETENTION_DAYS);
    };
    let days = value.parse::<u64>().map_err(|_| {
        ImageGatewayError::config("AIF_REQUEST_LOG_RETENTION_DAYS must be an integer")
    })?;
    if days == 0 || days > MAX_RETENTION_DAYS {
        return Err(ImageGatewayError::config(format!(
            "AIF_REQUEST_LOG_RETENTION_DAYS must be between 1 and {MAX_RETENTION_DAYS}"
        )));
    }
    Ok(days)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
