use std::{sync::Arc, time::Duration};

use reqwest::{Client, redirect::Policy};
use sqlx::PgPool;
use tokio::task::JoinSet;
use uuid::Uuid;

use super::{
    WebhookAttemptResult, WebhookDeliveryLease, WebhookDestinationPolicy, WebhookSigningKeyring,
    now_millis,
};
use crate::ImageGatewayError;

const RETRY_WINDOW_MS: i64 = 72 * 60 * 60 * 1_000;
const MAX_BACKOFF_MS: i64 = 60 * 60 * 1_000;
const HTTP_TIMEOUT: Duration = Duration::from_secs(20);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct PostgresWebhookRelay {
    pool: PgPool,
}

#[derive(Clone)]
pub struct WebhookDeliveryWorker {
    relay: PostgresWebhookRelay,
    keyring: WebhookSigningKeyring,
    destination_policy: WebhookDestinationPolicy,
}

#[derive(sqlx::FromRow)]
struct OutboxRow {
    outbox_event_id: Uuid,
    job_id: Uuid,
    outbox_event_type: String,
    created_at_ms: i64,
    project_id: String,
    organization_id: String,
    request_id: String,
    operation: String,
    provider_id: String,
    model: String,
    state: String,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
}

#[derive(sqlx::FromRow)]
struct LeaseRow {
    delivery_id: Uuid,
    lease_owner: String,
    lease_epoch: i64,
    attempt_number: i32,
    endpoint_id: String,
    project_id: String,
    url: String,
    signing_key_version: i32,
    secret_revision: i64,
    event_id: String,
    event_type: String,
    payload_body: Vec<u8>,
    retry_deadline_at_ms: i64,
}

impl PostgresWebhookRelay {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn fan_out_once(&self, limit: usize) -> Result<usize, ImageGatewayError> {
        let mut tx = self.pool.begin().await.map_err(webhook_relay_unavailable)?;
        let rows = sqlx::query_as::<_, OutboxRow>(
            r#"
            SELECT outbox.event_id AS outbox_event_id,
                   outbox.job_id,
                   outbox.event_type AS outbox_event_type,
                   outbox.created_at_ms,
                   attribution.project_id,
                   attribution.tenant_id AS organization_id,
                   job.request_id,
                   job.operation,
                   job.provider_id,
                   job.model,
                   job.state,
                   job.last_error_code,
                   job.last_error_message
            FROM outbox_events outbox
            JOIN jobs job ON job.job_id = outbox.job_id
            JOIN job_auth_attributions attribution
              ON attribution.job_id = outbox.job_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM project_webhook_outbox_receipts receipt
                WHERE receipt.outbox_event_id = outbox.event_id
            )
            ORDER BY outbox.created_at_ms, outbox.event_id
            FOR UPDATE OF outbox SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(limit.clamp(1, 500) as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(webhook_relay_unavailable)?;
        let published_at = now_millis();
        for row in &rows {
            if let Some(event_type) = public_event_type(&row.operation, &row.outbox_event_type) {
                let event_id = format!("evt_{}", row.outbox_event_id.simple());
                let payload = public_payload(&event_id, event_type, row);
                let payload_body = serde_json::to_vec(&payload)
                    .map_err(|_| ImageGatewayError::internal("Webhook event encoding failed"))?;
                sqlx::query(
                    r#"
                    INSERT INTO project_webhook_events
                        (event_id, project_id, organization_id, source_kind,
                         outbox_event_id, event_type, payload_json, payload_body,
                         created_at_ms)
                    VALUES ($1, $2, $3, 'outbox', $4, $5, $6, $7, $8)
                    ON CONFLICT (outbox_event_id) DO NOTHING
                    "#,
                )
                .bind(&event_id)
                .bind(&row.project_id)
                .bind(&row.organization_id)
                .bind(row.outbox_event_id)
                .bind(event_type)
                .bind(&payload)
                .bind(&payload_body)
                .bind(row.created_at_ms)
                .execute(&mut *tx)
                .await
                .map_err(webhook_relay_unavailable)?;
                sqlx::query(
                    r#"
                    INSERT INTO project_webhook_deliveries
                        (delivery_id, event_id, endpoint_id, project_id,
                         organization_id, state, attempt_count,
                         next_attempt_at_ms, retry_deadline_at_ms, lease_epoch,
                         created_at_ms, updated_at_ms)
                    SELECT gen_random_uuid(), $1, endpoint.endpoint_id,
                           endpoint.project_id, endpoint.organization_id,
                           'pending', 0, $2, $3, 0, $2, $2
                    FROM project_webhook_endpoints endpoint
                    WHERE endpoint.project_id = $4
                      AND endpoint.organization_id = $5
                      AND endpoint.state = 'active'
                      AND endpoint.event_types @> ARRAY[$6]::TEXT[]
                    ON CONFLICT (event_id, endpoint_id) DO NOTHING
                    "#,
                )
                .bind(&event_id)
                .bind(published_at)
                .bind(row.created_at_ms.saturating_add(RETRY_WINDOW_MS))
                .bind(&row.project_id)
                .bind(&row.organization_id)
                .bind(event_type)
                .execute(&mut *tx)
                .await
                .map_err(webhook_relay_unavailable)?;
            }
            sqlx::query(
                r#"
                INSERT INTO project_webhook_outbox_receipts
                    (outbox_event_id, processed_at_ms)
                VALUES ($1, $2)
                ON CONFLICT (outbox_event_id) DO NOTHING
                "#,
            )
            .bind(row.outbox_event_id)
            .bind(published_at)
            .execute(&mut *tx)
            .await
            .map_err(webhook_relay_unavailable)?;
        }
        tx.commit().await.map_err(webhook_relay_unavailable)?;
        Ok(rows.len())
    }

    pub async fn claim_deliveries(
        &self,
        worker_id: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<WebhookDeliveryLease>, ImageGatewayError> {
        let lease_ms = i64::try_from(lease_duration.as_millis()).unwrap_or(i64::MAX);
        let rows = sqlx::query_as::<_, LeaseRow>(
            r#"
            WITH database_clock AS (
                SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            ),
            expired AS (
                UPDATE project_webhook_deliveries delivery
                SET state = 'dead_lettered',
                    lease_owner = NULL,
                    lease_expires_at_ms = NULL,
                    last_error_code = 'retry_window_expired',
                    updated_at_ms = database_clock.now_ms
                FROM database_clock
                WHERE delivery.state IN ('pending', 'retry_wait', 'leased')
                  AND delivery.retry_deadline_at_ms <= database_clock.now_ms
                RETURNING delivery.delivery_id
            ),
            picked AS (
                SELECT delivery.delivery_id
                FROM project_webhook_deliveries delivery
                JOIN project_webhook_endpoints endpoint
                  ON endpoint.endpoint_id = delivery.endpoint_id
                 AND endpoint.project_id = delivery.project_id
                 AND endpoint.organization_id = delivery.organization_id
                JOIN project_webhook_endpoint_runtime runtime
                  ON runtime.endpoint_id = endpoint.endpoint_id
                CROSS JOIN database_clock
                WHERE endpoint.state = 'active'
                  AND delivery.retry_deadline_at_ms > database_clock.now_ms
                  AND (
                    runtime.paused_until_ms IS NULL
                    OR runtime.paused_until_ms <= database_clock.now_ms
                  )
                  AND (
                    (
                      delivery.state IN ('pending', 'retry_wait')
                      AND delivery.next_attempt_at_ms <= database_clock.now_ms
                    )
                    OR
                    (
                      delivery.state = 'leased'
                      AND delivery.lease_expires_at_ms <= database_clock.now_ms
                    )
                  )
                ORDER BY delivery.next_attempt_at_ms, delivery.delivery_id
                FOR UPDATE OF delivery SKIP LOCKED
                LIMIT $2
            ),
            claimed AS (
                UPDATE project_webhook_deliveries delivery
                SET state = 'leased',
                    lease_owner = $1,
                    lease_epoch = delivery.lease_epoch + 1,
                    lease_expires_at_ms = database_clock.now_ms + $3,
                    attempt_count = delivery.attempt_count + 1,
                    last_attempt_at_ms = database_clock.now_ms,
                    updated_at_ms = database_clock.now_ms
                FROM picked, database_clock
                WHERE delivery.delivery_id = picked.delivery_id
                RETURNING delivery.*
            )
            SELECT claimed.delivery_id,
                   claimed.lease_owner,
                   claimed.lease_epoch,
                   claimed.attempt_count AS attempt_number,
                   endpoint.endpoint_id,
                   endpoint.project_id,
                   endpoint.url,
                   endpoint.signing_key_version,
                   endpoint.secret_revision,
                   event.event_id,
                   event.event_type,
                   event.payload_body,
                   claimed.retry_deadline_at_ms
            FROM claimed
            JOIN project_webhook_endpoints endpoint
              ON endpoint.endpoint_id = claimed.endpoint_id
            JOIN project_webhook_events event
              ON event.event_id = claimed.event_id
            ORDER BY claimed.next_attempt_at_ms, claimed.delivery_id
            "#,
        )
        .bind(worker_id)
        .bind(limit.clamp(1, 100) as i64)
        .bind(lease_ms)
        .fetch_all(&self.pool)
        .await
        .map_err(webhook_relay_unavailable)?;
        rows.into_iter().map(lease_from_row).collect()
    }

    pub async fn finish_attempt(
        &self,
        lease: &WebhookDeliveryLease,
        result: WebhookAttemptResult,
    ) -> Result<(), ImageGatewayError> {
        let now = now_millis();
        let status = result.http_status;
        let succeeded = status.is_some_and(|status| (200..=299).contains(&status));
        let gone = status == Some(410);
        let retry_at = if succeeded || gone {
            None
        } else {
            Some(
                result
                    .retry_after_ms
                    .map(|delay| now.saturating_add(delay.max(0)))
                    .unwrap_or_else(|| {
                        now.saturating_add(equal_jitter_backoff_ms(lease.attempt_number))
                    })
                    .min(lease.retry_deadline_at_ms),
            )
        };
        let retry = retry_at.is_some_and(|at| at < lease.retry_deadline_at_ms);
        let (next_state, attempt_outcome, error_code) = if succeeded {
            ("succeeded", "succeeded", None)
        } else if gone {
            (
                "dead_lettered",
                "dead_lettered",
                Some("endpoint_gone".to_string()),
            )
        } else if retry {
            (
                "retry_wait",
                "retry",
                result
                    .error_code
                    .clone()
                    .or_else(|| result.http_status.map(|status| format!("http_{status}"))),
            )
        } else {
            (
                "dead_lettered",
                "dead_lettered",
                result
                    .error_code
                    .clone()
                    .or_else(|| Some("retry_window_expired".to_string())),
            )
        };
        let mut tx = self.pool.begin().await.map_err(webhook_relay_unavailable)?;
        let updated = sqlx::query(
            r#"
            UPDATE project_webhook_deliveries
            SET state = $5,
                next_attempt_at_ms = COALESCE($6, next_attempt_at_ms),
                lease_owner = NULL,
                lease_expires_at_ms = NULL,
                last_http_status = $7,
                last_error_code = $8,
                delivered_at_ms = CASE WHEN $5 = 'succeeded' THEN $9 ELSE NULL END,
                updated_at_ms = $9
            WHERE delivery_id = $1
              AND state = 'leased'
              AND lease_owner = $2
              AND lease_epoch = $3
              AND lease_expires_at_ms > $4
            "#,
        )
        .bind(lease.delivery_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_epoch)
        .bind(now)
        .bind(next_state)
        .bind(retry_at)
        .bind(status.map(i32::from))
        .bind(&error_code)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(webhook_relay_unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(ImageGatewayError::conflict(
                "Webhook delivery lease expired or was superseded",
                Some("delivery_id".to_string()),
                "webhook_lease_conflict",
            ));
        }
        sqlx::query(
            r#"
            INSERT INTO project_webhook_attempts
                (attempt_id, delivery_id, attempt_number, outcome,
                 webhook_timestamp, http_status, error_code, duration_ms,
                 next_attempt_at_ms, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(lease.delivery_id)
        .bind(lease.attempt_number)
        .bind(attempt_outcome)
        .bind(result.webhook_timestamp)
        .bind(status.map(i32::from))
        .bind(&error_code)
        .bind(result.duration_ms.max(0))
        .bind(if retry { retry_at } else { None })
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(webhook_relay_unavailable)?;
        if succeeded {
            sqlx::query(
                r#"
                UPDATE project_webhook_endpoint_runtime
                SET paused_until_ms = NULL,
                    consecutive_failures = 0,
                    updated_at_ms = $2
                WHERE endpoint_id = $1
                "#,
            )
            .bind(&lease.endpoint_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(webhook_relay_unavailable)?;
        } else if gone {
            sqlx::query(
                r#"
                UPDATE project_webhook_endpoints
                SET state = 'disabled',
                    disabled_at_ms = $2,
                    updated_at_ms = $2,
                    control_version = control_version + 1
                WHERE endpoint_id = $1 AND state = 'active'
                "#,
            )
            .bind(&lease.endpoint_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(webhook_relay_unavailable)?;
            sqlx::query(
                r#"
                UPDATE project_webhook_deliveries
                SET state = 'canceled',
                    lease_owner = NULL,
                    lease_expires_at_ms = NULL,
                    last_error_code = 'endpoint_disabled',
                    updated_at_ms = $2
                WHERE endpoint_id = $1
                  AND delivery_id <> $3
                  AND state IN ('pending', 'retry_wait')
                "#,
            )
            .bind(&lease.endpoint_id)
            .bind(now)
            .bind(lease.delivery_id)
            .execute(&mut *tx)
            .await
            .map_err(webhook_relay_unavailable)?;
        } else {
            let paused_until = if status == Some(429) { retry_at } else { None };
            sqlx::query(
                r#"
                UPDATE project_webhook_endpoint_runtime
                SET paused_until_ms = COALESCE($2, paused_until_ms),
                    consecutive_failures = consecutive_failures + 1,
                    updated_at_ms = $3
                WHERE endpoint_id = $1
                "#,
            )
            .bind(&lease.endpoint_id)
            .bind(paused_until)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(webhook_relay_unavailable)?;
        }
        tx.commit().await.map_err(webhook_relay_unavailable)?;
        Ok(())
    }
}

impl WebhookDeliveryWorker {
    pub fn new(
        relay: PostgresWebhookRelay,
        keyring: WebhookSigningKeyring,
        destination_policy: WebhookDestinationPolicy,
    ) -> Self {
        Self {
            relay,
            keyring,
            destination_policy,
        }
    }

    pub async fn run(self, worker_id: String, concurrency: usize) -> Result<(), ImageGatewayError> {
        let worker = Arc::new(self);
        loop {
            let fanout = worker.relay.fan_out_once(100).await?;
            let leases = worker
                .relay
                .claim_deliveries(&worker_id, concurrency.max(1), Duration::from_secs(90))
                .await?;
            let delivery_count = leases.len();
            let mut tasks = JoinSet::new();
            for lease in leases {
                let worker = worker.clone();
                tasks.spawn(async move {
                    let result = worker.deliver(&lease).await;
                    worker.relay.finish_attempt(&lease, result).await
                });
            }
            while let Some(joined) = tasks.join_next().await {
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::warn!(?error, "webhook attempt finalize failed"),
                    Err(error) => tracing::warn!(?error, "webhook delivery task failed"),
                }
            }
            if fanout == 0 && delivery_count == 0 {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    async fn deliver(&self, lease: &WebhookDeliveryLease) -> WebhookAttemptResult {
        let started = std::time::Instant::now();
        let timestamp = now_millis() / 1_000;
        let destination = match self.destination_policy.resolve(&lease.url).await {
            Ok(destination) => destination,
            Err(_) => {
                return WebhookAttemptResult {
                    http_status: None,
                    error_code: Some("invalid_destination".to_string()),
                    retry_after_ms: None,
                    duration_ms: elapsed_millis(started),
                    webhook_timestamp: timestamp,
                };
            }
        };
        let signature = match self.keyring.signature_header(
            &lease.project_id,
            &lease.endpoint_id,
            lease.signing_key_version,
            lease.secret_revision,
            &lease.event_id,
            timestamp,
            &lease.payload_body,
        ) {
            Ok(signature) => signature,
            Err(_) => {
                return WebhookAttemptResult {
                    http_status: None,
                    error_code: Some("signing_key_unavailable".to_string()),
                    retry_after_ms: None,
                    duration_ms: elapsed_millis(started),
                    webhook_timestamp: timestamp,
                };
            }
        };
        let client = match pinned_client(&destination) {
            Ok(client) => client,
            Err(_) => {
                return WebhookAttemptResult {
                    http_status: None,
                    error_code: Some("http_client_unavailable".to_string()),
                    retry_after_ms: None,
                    duration_ms: elapsed_millis(started),
                    webhook_timestamp: timestamp,
                };
            }
        };
        match client
            .post(destination.url)
            .header("content-type", "application/json")
            .header("user-agent", "ai-image-factory-webhooks/1.0")
            .header("webhook-id", &lease.event_id)
            .header("webhook-timestamp", timestamp.to_string())
            .header("webhook-signature", signature)
            .body(lease.payload_body.clone())
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status().as_u16();
                let retry_after_ms = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(parse_retry_after_ms);
                WebhookAttemptResult {
                    http_status: Some(status),
                    error_code: (!(200..=299).contains(&status)).then(|| format!("http_{status}")),
                    retry_after_ms,
                    duration_ms: elapsed_millis(started),
                    webhook_timestamp: timestamp,
                }
            }
            Err(error) => WebhookAttemptResult {
                http_status: None,
                error_code: Some(if error.is_timeout() {
                    "timeout".to_string()
                } else {
                    "network_error".to_string()
                }),
                retry_after_ms: None,
                duration_ms: elapsed_millis(started),
                webhook_timestamp: timestamp,
            },
        }
    }
}

fn public_event_type(operation: &str, outbox_event_type: &str) -> Option<&'static str> {
    let outcome = match outbox_event_type {
        "job.succeeded" => "completed",
        "job.failed" | "job.uncertain" => "failed",
        _ => return None,
    };
    match (operation, outcome) {
        ("generation", "completed") => Some("image.generation.completed"),
        ("generation", "failed") => Some("image.generation.failed"),
        ("edit", "completed") => Some("image.edit.completed"),
        ("edit", "failed") => Some("image.edit.failed"),
        ("video_generation", "completed") => Some("video.generation.completed"),
        ("video_generation", "failed") => Some("video.generation.failed"),
        _ => None,
    }
}

fn public_payload(event_id: &str, event_type: &str, row: &OutboxRow) -> serde_json::Value {
    serde_json::json!({
        "object": "event",
        "id": event_id,
        "type": event_type,
        "created_at": row.created_at_ms / 1_000,
        "data": {
            "id": row.job_id,
            "project_id": row.project_id,
            "request_id": row.request_id,
            "operation": row.operation,
            "provider": row.provider_id,
            "model": row.model,
            "outcome": row.state,
            "error": row.last_error_code.as_ref().map(|code| serde_json::json!({
                "code": code,
                "message": row.last_error_message,
            })),
        }
    })
}

fn lease_from_row(row: LeaseRow) -> Result<WebhookDeliveryLease, ImageGatewayError> {
    Ok(WebhookDeliveryLease {
        delivery_id: row.delivery_id,
        lease_owner: row.lease_owner,
        lease_epoch: row.lease_epoch,
        attempt_number: row.attempt_number,
        endpoint_id: row.endpoint_id,
        project_id: row.project_id,
        url: row.url,
        signing_key_version: u16::try_from(row.signing_key_version)
            .map_err(|_| ImageGatewayError::internal("stored signing key version is invalid"))?,
        secret_revision: row.secret_revision,
        event_id: row.event_id,
        event_type: row.event_type,
        payload_body: row.payload_body,
        retry_deadline_at_ms: row.retry_deadline_at_ms,
    })
}

fn pinned_client(
    destination: &super::ResolvedWebhookDestination,
) -> Result<Client, reqwest::Error> {
    Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .timeout(HTTP_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT)
        .resolve_to_addrs(&destination.host, &destination.addresses)
        .build()
}

fn parse_retry_after_ms(value: &str) -> Option<i64> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return i64::try_from(seconds.saturating_mul(1_000)).ok();
    }
    let deadline = httpdate::parse_http_date(value).ok()?;
    let delay = deadline
        .duration_since(std::time::SystemTime::now())
        .unwrap_or_default();
    i64::try_from(delay.as_millis()).ok()
}

fn equal_jitter_backoff_ms(attempt_number: i32) -> i64 {
    let exponent = u32::try_from(attempt_number.saturating_sub(1).clamp(0, 20)).unwrap_or(0);
    let ceiling = 1_000_i64
        .saturating_mul(2_i64.saturating_pow(exponent))
        .min(MAX_BACKOFF_MS);
    let floor = (ceiling / 2).max(500);
    let window = ceiling.saturating_sub(floor).saturating_add(1);
    let bytes = Uuid::new_v4().into_bytes();
    let sample = u64::from_le_bytes(bytes[..8].try_into().unwrap_or([0; 8]));
    floor.saturating_add(
        i64::try_from(sample % u64::try_from(window.max(1)).unwrap_or(1)).unwrap_or(0),
    )
}

fn elapsed_millis(started: std::time::Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}

fn webhook_relay_unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(error = ?error, "webhook relay storage operation failed");
    ImageGatewayError::service_unavailable("Webhook relay unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_only_stable_public_terminal_events() {
        assert_eq!(
            public_event_type("generation", "job.succeeded"),
            Some("image.generation.completed")
        );
        assert_eq!(
            public_event_type("video_generation", "job.uncertain"),
            Some("video.generation.failed")
        );
        assert_eq!(public_event_type("generation", "job.accepted"), None);
        assert_eq!(public_event_type("generation", "work.requeued"), None);
    }

    #[test]
    fn retry_after_supports_delta_seconds() {
        assert_eq!(parse_retry_after_ms("15"), Some(15_000));
    }

    #[test]
    fn backoff_is_bounded() {
        for attempt in [1, 2, 10, 100] {
            let delay = equal_jitter_backoff_ms(attempt);
            assert!((500..=MAX_BACKOFF_MS).contains(&delay));
        }
    }
}
