use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    CreateProjectWebhookRequest, CreatedProjectWebhook, DeletedProjectWebhook,
    ProjectWebhookDelivery, ProjectWebhookDeliveryList, ProjectWebhookEndpoint, ProjectWebhookList,
    ProjectWebhookService, ProjectWebhookTestEvent, RotatedProjectWebhookSecret,
    UpdateProjectWebhookRequest, WebhookDeliveryState, WebhookDestinationPolicy,
    WebhookEndpointState, WebhookSigningKeyring, now_millis, validate_event_types, validate_name,
};
use crate::ImageGatewayError;

const WEBHOOK_OBJECT: &str = "organization.project.webhook";
const DELIVERY_OBJECT: &str = "organization.project.webhook.delivery";
const RETRY_WINDOW_MS: i64 = 72 * 60 * 60 * 1_000;

#[derive(Clone)]
pub struct PostgresProjectWebhookService {
    pool: PgPool,
    keyring: WebhookSigningKeyring,
    destination_policy: WebhookDestinationPolicy,
}

#[derive(sqlx::FromRow)]
struct EndpointRow {
    endpoint_id: String,
    project_id: String,
    name: Option<String>,
    url: String,
    event_types: Vec<String>,
    state: String,
    signing_key_version: i32,
    secret_revision: i64,
    control_version: i64,
    last_delivery_state: Option<String>,
    last_delivery_at_ms: Option<i64>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct DeliveryRow {
    delivery_id: Uuid,
    event_id: String,
    event_type: String,
    endpoint_id: String,
    state: String,
    attempt_count: i32,
    next_attempt_at_ms: i64,
    retry_deadline_at_ms: i64,
    last_http_status: Option<i32>,
    last_error_code: Option<String>,
    last_attempt_at_ms: Option<i64>,
    delivered_at_ms: Option<i64>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl PostgresProjectWebhookService {
    pub fn new(
        pool: PgPool,
        keyring: WebhookSigningKeyring,
        destination_policy: WebhookDestinationPolicy,
    ) -> Self {
        Self {
            pool,
            keyring,
            destination_policy,
        }
    }
}

#[async_trait]
impl ProjectWebhookService for PostgresProjectWebhookService {
    async fn list_endpoints(
        &self,
        project_id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectWebhookList, ImageGatewayError> {
        let fetch_limit = limit.clamp(1, 100).saturating_add(1) as i64;
        let rows = sqlx::query_as::<_, EndpointRow>(
            r#"
            SELECT endpoint.endpoint_id, endpoint.project_id, endpoint.name,
                   endpoint.url, endpoint.event_types, endpoint.state,
                   endpoint.signing_key_version, endpoint.secret_revision,
                   endpoint.control_version,
                   latest.state AS last_delivery_state,
                   latest.updated_at_ms AS last_delivery_at_ms,
                   endpoint.created_at_ms, endpoint.updated_at_ms
            FROM project_webhook_endpoints endpoint
            LEFT JOIN LATERAL (
                SELECT delivery.state, delivery.updated_at_ms
                FROM project_webhook_deliveries delivery
                WHERE delivery.endpoint_id = endpoint.endpoint_id
                ORDER BY delivery.created_at_ms DESC, delivery.delivery_id DESC
                LIMIT 1
            ) latest ON TRUE
            WHERE endpoint.project_id = $1
              AND endpoint.state <> 'deleted'
              AND ($2::TEXT IS NULL OR endpoint.endpoint_id < $2)
            ORDER BY endpoint.endpoint_id DESC
            LIMIT $3
            "#,
        )
        .bind(project_id)
        .bind(after)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(webhook_store_unavailable)?;
        let has_more = rows.len() > limit.clamp(1, 100);
        let data = rows
            .into_iter()
            .take(limit.clamp(1, 100))
            .map(endpoint_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProjectWebhookList {
            object: "list",
            last_id: data.last().map(|endpoint| endpoint.id.clone()),
            data,
            has_more,
        })
    }

    async fn create_endpoint(
        &self,
        project_id: &str,
        actor_user_id: Uuid,
        request: CreateProjectWebhookRequest,
    ) -> Result<CreatedProjectWebhook, ImageGatewayError> {
        let name = validate_name(request.name)?;
        let event_types = validate_event_types(request.event_types)?;
        let destination = self.destination_policy.resolve(&request.url).await?;
        let endpoint_id = format!("we_{}", Uuid::new_v4().simple());
        let now = now_millis();
        let signing_key_version = self.keyring.current_version();
        let mut tx = self.pool.begin().await.map_err(webhook_store_unavailable)?;
        let organization_id = project_organization(&mut tx, project_id).await?;
        let row = sqlx::query_as::<_, EndpointRow>(
            r#"
            INSERT INTO project_webhook_endpoints
                (endpoint_id, project_id, organization_id, name, url,
                 event_types, state, signing_key_version, secret_revision,
                 created_by_user_id, created_at_ms, updated_at_ms,
                 disabled_at_ms, deleted_at_ms, control_version)
            VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, 1,
                    $8, $9, $9, NULL, NULL, 1)
            RETURNING endpoint_id, project_id, name, url, event_types, state,
                      signing_key_version, secret_revision, control_version,
                      NULL::TEXT AS last_delivery_state,
                      NULL::BIGINT AS last_delivery_at_ms,
                      created_at_ms, updated_at_ms
            "#,
        )
        .bind(&endpoint_id)
        .bind(project_id)
        .bind(&organization_id)
        .bind(&name)
        .bind(destination.url.as_str())
        .bind(&event_types)
        .bind(i32::from(signing_key_version))
        .bind(actor_user_id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await
        .map_err(webhook_store_unavailable)?;
        sqlx::query(
            r#"
            INSERT INTO project_webhook_endpoint_runtime
                (endpoint_id, paused_until_ms, consecutive_failures, updated_at_ms)
            VALUES ($1, NULL, 0, $2)
            "#,
        )
        .bind(&endpoint_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(webhook_store_unavailable)?;
        append_audit(
            &mut tx,
            actor_user_id,
            "webhook.endpoint.create",
            &endpoint_id,
            serde_json::json!({
                "project_id": project_id,
                "name": name,
                "url": destination.url.as_str(),
                "event_types": event_types,
            }),
            now,
        )
        .await?;
        tx.commit().await.map_err(webhook_store_unavailable)?;
        let endpoint = endpoint_from_row(row)?;
        let signing_secret =
            self.keyring
                .signing_secret(project_id, &endpoint_id, signing_key_version, 1)?;
        Ok(CreatedProjectWebhook {
            object: "organization.project.webhook.created",
            endpoint,
            signing_secret,
        })
    }

    async fn update_endpoint(
        &self,
        project_id: &str,
        endpoint_id: &str,
        actor_user_id: Uuid,
        request: UpdateProjectWebhookRequest,
    ) -> Result<ProjectWebhookEndpoint, ImageGatewayError> {
        if request.expected_control_version <= 0 {
            return Err(ImageGatewayError::invalid_request(
                "expected_control_version must be greater than zero",
                Some("expected_control_version".to_string()),
                "invalid_control_version",
            ));
        }
        let name = validate_name(request.name)?;
        let event_types = validate_event_types(request.event_types)?;
        let destination = self.destination_policy.resolve(&request.url).await?;
        let now = now_millis();
        let state = request.state.as_str();
        let mut tx = self.pool.begin().await.map_err(webhook_store_unavailable)?;
        let row = sqlx::query_as::<_, EndpointRow>(
            r#"
            UPDATE project_webhook_endpoints
            SET name = $3,
                url = $4,
                event_types = $5,
                state = $6,
                disabled_at_ms = CASE WHEN $6 = 'disabled' THEN $7 ELSE NULL END,
                updated_at_ms = $7,
                control_version = control_version + 1
            WHERE project_id = $1
              AND endpoint_id = $2
              AND state <> 'deleted'
              AND control_version = $8
            RETURNING endpoint_id, project_id, name, url, event_types, state,
                      signing_key_version, secret_revision, control_version,
                      NULL::TEXT AS last_delivery_state,
                      NULL::BIGINT AS last_delivery_at_ms,
                      created_at_ms, updated_at_ms
            "#,
        )
        .bind(project_id)
        .bind(endpoint_id)
        .bind(&name)
        .bind(destination.url.as_str())
        .bind(&event_types)
        .bind(state)
        .bind(now)
        .bind(request.expected_control_version)
        .fetch_optional(&mut *tx)
        .await
        .map_err(webhook_store_unavailable)?
        .ok_or_else(|| webhook_conflict_or_not_found(endpoint_id))?;
        append_audit(
            &mut tx,
            actor_user_id,
            "webhook.endpoint.update",
            endpoint_id,
            serde_json::json!({
                "project_id": project_id,
                "name": name,
                "url": destination.url.as_str(),
                "event_types": event_types,
                "state": state,
                "control_version": row.control_version,
            }),
            now,
        )
        .await?;
        tx.commit().await.map_err(webhook_store_unavailable)?;
        endpoint_from_row(row)
    }

    async fn delete_endpoint(
        &self,
        project_id: &str,
        endpoint_id: &str,
        actor_user_id: Uuid,
    ) -> Result<DeletedProjectWebhook, ImageGatewayError> {
        let now = now_millis();
        let mut tx = self.pool.begin().await.map_err(webhook_store_unavailable)?;
        let updated = sqlx::query(
            r#"
            UPDATE project_webhook_endpoints
            SET state = 'deleted',
                disabled_at_ms = COALESCE(disabled_at_ms, $3),
                deleted_at_ms = $3,
                updated_at_ms = $3,
                control_version = control_version + 1
            WHERE project_id = $1
              AND endpoint_id = $2
              AND state <> 'deleted'
            "#,
        )
        .bind(project_id)
        .bind(endpoint_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(webhook_store_unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(webhook_not_found(endpoint_id));
        }
        sqlx::query(
            r#"
            UPDATE project_webhook_deliveries
            SET state = 'canceled',
                lease_owner = NULL,
                lease_expires_at_ms = NULL,
                updated_at_ms = $3
            WHERE project_id = $1
              AND endpoint_id = $2
              AND state IN ('pending', 'retry_wait')
            "#,
        )
        .bind(project_id)
        .bind(endpoint_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(webhook_store_unavailable)?;
        append_audit(
            &mut tx,
            actor_user_id,
            "webhook.endpoint.delete",
            endpoint_id,
            serde_json::json!({"project_id": project_id}),
            now,
        )
        .await?;
        tx.commit().await.map_err(webhook_store_unavailable)?;
        Ok(DeletedProjectWebhook {
            object: "organization.project.webhook.deleted",
            id: endpoint_id.to_string(),
            deleted: true,
        })
    }

    async fn rotate_secret(
        &self,
        project_id: &str,
        endpoint_id: &str,
        actor_user_id: Uuid,
    ) -> Result<RotatedProjectWebhookSecret, ImageGatewayError> {
        let now = now_millis();
        let signing_key_version = self.keyring.current_version();
        let mut tx = self.pool.begin().await.map_err(webhook_store_unavailable)?;
        let row = sqlx::query_as::<_, (i64, i64)>(
            r#"
            UPDATE project_webhook_endpoints
            SET signing_key_version = $3,
                secret_revision = secret_revision + 1,
                control_version = control_version + 1,
                updated_at_ms = $4
            WHERE project_id = $1
              AND endpoint_id = $2
              AND state <> 'deleted'
            RETURNING secret_revision, control_version
            "#,
        )
        .bind(project_id)
        .bind(endpoint_id)
        .bind(i32::from(signing_key_version))
        .bind(now)
        .fetch_optional(&mut *tx)
        .await
        .map_err(webhook_store_unavailable)?
        .ok_or_else(|| webhook_not_found(endpoint_id))?;
        append_audit(
            &mut tx,
            actor_user_id,
            "webhook.secret.rotate",
            endpoint_id,
            serde_json::json!({
                "project_id": project_id,
                "signing_key_version": signing_key_version,
                "secret_revision": row.0,
            }),
            now,
        )
        .await?;
        tx.commit().await.map_err(webhook_store_unavailable)?;
        let signing_secret =
            self.keyring
                .signing_secret(project_id, endpoint_id, signing_key_version, row.0)?;
        Ok(RotatedProjectWebhookSecret {
            object: "organization.project.webhook.secret",
            endpoint_id: endpoint_id.to_string(),
            signing_key_version,
            secret_revision: row.0,
            control_version: row.1,
            signing_secret,
        })
    }

    async fn enqueue_test(
        &self,
        project_id: &str,
        endpoint_id: &str,
        actor_user_id: Uuid,
    ) -> Result<ProjectWebhookTestEvent, ImageGatewayError> {
        let now = now_millis();
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let delivery_id = Uuid::new_v4();
        let mut tx = self.pool.begin().await.map_err(webhook_store_unavailable)?;
        let organization_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT organization_id
            FROM project_webhook_endpoints
            WHERE project_id = $1 AND endpoint_id = $2 AND state = 'active'
            "#,
        )
        .bind(project_id)
        .bind(endpoint_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(webhook_store_unavailable)?
        .ok_or_else(|| webhook_not_found(endpoint_id))?;
        let payload = serde_json::json!({
            "object": "event",
            "id": event_id,
            "type": "webhook.test",
            "created_at": now / 1_000,
            "data": {
                "project_id": project_id,
                "endpoint_id": endpoint_id,
            }
        });
        let payload_body = serde_json::to_vec(&payload)
            .map_err(|_| ImageGatewayError::internal("Webhook test event encoding failed"))?;
        sqlx::query(
            r#"
            INSERT INTO project_webhook_events
                (event_id, project_id, organization_id, source_kind,
                 outbox_event_id, event_type, payload_json, payload_body, created_at_ms)
            VALUES ($1, $2, $3, 'test', NULL, 'webhook.test', $4, $5, $6)
            "#,
        )
        .bind(&event_id)
        .bind(project_id)
        .bind(&organization_id)
        .bind(&payload)
        .bind(&payload_body)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(webhook_store_unavailable)?;
        sqlx::query(
            r#"
            INSERT INTO project_webhook_deliveries
                (delivery_id, event_id, endpoint_id, project_id, organization_id,
                 state, attempt_count, next_attempt_at_ms, retry_deadline_at_ms,
                 lease_epoch, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, 'pending', 0, $6, $7, 0, $6, $6)
            "#,
        )
        .bind(delivery_id)
        .bind(&event_id)
        .bind(endpoint_id)
        .bind(project_id)
        .bind(&organization_id)
        .bind(now)
        .bind(now.saturating_add(RETRY_WINDOW_MS))
        .execute(&mut *tx)
        .await
        .map_err(webhook_store_unavailable)?;
        append_audit(
            &mut tx,
            actor_user_id,
            "webhook.test.enqueue",
            endpoint_id,
            serde_json::json!({
                "project_id": project_id,
                "event_id": event_id,
                "delivery_id": delivery_id,
            }),
            now,
        )
        .await?;
        tx.commit().await.map_err(webhook_store_unavailable)?;
        Ok(ProjectWebhookTestEvent {
            object: "organization.project.webhook.test",
            event_id,
            endpoint_id: endpoint_id.to_string(),
            delivery_id,
        })
    }

    async fn list_deliveries(
        &self,
        project_id: &str,
        endpoint_id: &str,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<ProjectWebhookDeliveryList, ImageGatewayError> {
        let limit = limit.clamp(1, 100);
        let rows = sqlx::query_as::<_, DeliveryRow>(
            r#"
            SELECT delivery.delivery_id, delivery.event_id, event.event_type,
                   delivery.endpoint_id, delivery.state, delivery.attempt_count,
                   delivery.next_attempt_at_ms, delivery.retry_deadline_at_ms,
                   delivery.last_http_status, delivery.last_error_code,
                   delivery.last_attempt_at_ms, delivery.delivered_at_ms,
                   delivery.created_at_ms, delivery.updated_at_ms
            FROM project_webhook_deliveries delivery
            JOIN project_webhook_events event ON event.event_id = delivery.event_id
            WHERE delivery.project_id = $1
              AND delivery.endpoint_id = $2
              AND ($3::UUID IS NULL OR delivery.delivery_id < $3)
            ORDER BY delivery.delivery_id DESC
            LIMIT $4
            "#,
        )
        .bind(project_id)
        .bind(endpoint_id)
        .bind(after)
        .bind(limit.saturating_add(1) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(webhook_store_unavailable)?;
        let has_more = rows.len() > limit;
        let data = rows
            .into_iter()
            .take(limit)
            .map(delivery_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProjectWebhookDeliveryList {
            object: "list",
            last_id: data.last().map(|delivery| delivery.id),
            data,
            has_more,
        })
    }
}

async fn project_organization(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
) -> Result<String, ImageGatewayError> {
    sqlx::query_scalar(
        "SELECT tenant_id FROM gateway_projects WHERE id = $1 AND archived_at IS NULL",
    )
    .bind(project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(webhook_store_unavailable)?
    .ok_or_else(|| {
        ImageGatewayError::not_found(
            "Project was not found",
            Some("project_id".to_string()),
            "project_not_found",
        )
    })
}

async fn append_audit(
    tx: &mut Transaction<'_, Postgres>,
    actor_user_id: Uuid,
    action: &str,
    resource_id: &str,
    metadata: serde_json::Value,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO identity_audit_events
            (event_id, actor_user_id, action, resource_type, resource_id,
             outcome, metadata, created_at_ms)
        VALUES ($1, $2, $3, 'project_webhook', $4, 'success', $5, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(actor_user_id)
    .bind(action)
    .bind(resource_id)
    .bind(metadata)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(|_| ImageGatewayError::service_unavailable("audit state unavailable"))?;
    Ok(())
}

fn endpoint_from_row(row: EndpointRow) -> Result<ProjectWebhookEndpoint, ImageGatewayError> {
    Ok(ProjectWebhookEndpoint {
        object: WEBHOOK_OBJECT,
        id: row.endpoint_id,
        project_id: row.project_id,
        name: row.name,
        url: row.url,
        event_types: row.event_types,
        state: WebhookEndpointState::parse(&row.state)?,
        signing_key_version: u16::try_from(row.signing_key_version)
            .map_err(|_| ImageGatewayError::internal("stored signing key version is invalid"))?,
        secret_revision: row.secret_revision,
        control_version: row.control_version,
        last_delivery_state: row
            .last_delivery_state
            .as_deref()
            .map(WebhookDeliveryState::parse)
            .transpose()?,
        last_delivery_at_ms: row.last_delivery_at_ms,
        created_at: row.created_at_ms / 1_000,
        updated_at: row.updated_at_ms / 1_000,
    })
}

fn delivery_from_row(row: DeliveryRow) -> Result<ProjectWebhookDelivery, ImageGatewayError> {
    Ok(ProjectWebhookDelivery {
        object: DELIVERY_OBJECT,
        id: row.delivery_id,
        event_id: row.event_id,
        event_type: row.event_type,
        endpoint_id: row.endpoint_id,
        state: WebhookDeliveryState::parse(&row.state)?,
        attempt_count: row.attempt_count,
        next_attempt_at_ms: row.next_attempt_at_ms,
        retry_deadline_at_ms: row.retry_deadline_at_ms,
        last_http_status: row.last_http_status,
        last_error_code: row.last_error_code,
        last_attempt_at_ms: row.last_attempt_at_ms,
        delivered_at_ms: row.delivered_at_ms,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn webhook_store_unavailable(_: sqlx::Error) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Webhook store unavailable")
}

fn webhook_not_found(endpoint_id: &str) -> ImageGatewayError {
    ImageGatewayError::not_found(
        format!("Webhook endpoint '{endpoint_id}' was not found"),
        Some("endpoint_id".to_string()),
        "webhook_not_found",
    )
}

fn webhook_conflict_or_not_found(endpoint_id: &str) -> ImageGatewayError {
    ImageGatewayError::conflict(
        format!(
            "Webhook endpoint '{endpoint_id}' changed or no longer exists; refresh and try again"
        ),
        Some("expected_control_version".to_string()),
        "webhook_control_version_conflict",
    )
}
