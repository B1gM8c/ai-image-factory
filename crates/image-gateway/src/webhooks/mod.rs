mod destination;
mod postgres;
mod signing;
mod worker;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ImageGatewayError;

pub use destination::{ResolvedWebhookDestination, WebhookDestinationPolicy};
pub use postgres::PostgresProjectWebhookService;
pub use signing::WebhookSigningKeyring;
pub use worker::{PostgresWebhookRelay, WebhookDeliveryWorker};

pub const SUPPORTED_WEBHOOK_EVENT_TYPES: [&str; 6] = [
    "image.edit.completed",
    "image.edit.failed",
    "image.generation.completed",
    "image.generation.failed",
    "video.generation.completed",
    "video.generation.failed",
];

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateProjectWebhookRequest {
    pub name: Option<String>,
    pub url: String,
    pub event_types: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateProjectWebhookRequest {
    pub name: Option<String>,
    pub url: String,
    pub event_types: Vec<String>,
    pub state: WebhookEndpointState,
    pub expected_control_version: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEndpointState {
    Active,
    Disabled,
}

impl WebhookEndpointState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
        }
    }

    fn parse(value: &str) -> Result<Self, ImageGatewayError> {
        match value {
            "active" => Ok(Self::Active),
            "disabled" => Ok(Self::Disabled),
            _ => Err(ImageGatewayError::internal(
                "stored webhook endpoint state is invalid",
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectWebhookEndpoint {
    pub object: &'static str,
    pub id: String,
    pub project_id: String,
    pub name: Option<String>,
    pub url: String,
    pub event_types: Vec<String>,
    pub state: WebhookEndpointState,
    pub signing_key_version: u16,
    pub secret_revision: i64,
    pub control_version: i64,
    pub last_delivery_state: Option<WebhookDeliveryState>,
    pub last_delivery_at_ms: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct CreatedProjectWebhook {
    pub object: &'static str,
    pub endpoint: ProjectWebhookEndpoint,
    pub signing_secret: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RotatedProjectWebhookSecret {
    pub object: &'static str,
    pub endpoint_id: String,
    pub signing_key_version: u16,
    pub secret_revision: i64,
    pub control_version: i64,
    pub signing_secret: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectWebhookList {
    pub object: &'static str,
    pub data: Vec<ProjectWebhookEndpoint>,
    pub has_more: bool,
    pub last_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct DeletedProjectWebhook {
    pub object: &'static str,
    pub id: String,
    pub deleted: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectWebhookTestEvent {
    pub object: &'static str,
    pub event_id: String,
    pub endpoint_id: String,
    #[schema(value_type = String)]
    pub delivery_id: Uuid,
}

#[derive(Clone, Copy, Debug, Serialize, ToSchema, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WebhookDeliveryState {
    Pending,
    Leased,
    RetryWait,
    Succeeded,
    DeadLettered,
    Canceled,
}

impl WebhookDeliveryState {
    fn parse(value: &str) -> Result<Self, ImageGatewayError> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "retry_wait" => Ok(Self::RetryWait),
            "succeeded" => Ok(Self::Succeeded),
            "dead_lettered" => Ok(Self::DeadLettered),
            "canceled" => Ok(Self::Canceled),
            _ => Err(ImageGatewayError::internal(
                "stored webhook delivery state is invalid",
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectWebhookDelivery {
    pub object: &'static str,
    #[schema(value_type = String)]
    pub id: Uuid,
    pub event_id: String,
    pub event_type: String,
    pub endpoint_id: String,
    pub state: WebhookDeliveryState,
    pub attempt_count: i32,
    pub next_attempt_at_ms: i64,
    pub retry_deadline_at_ms: i64,
    pub last_http_status: Option<i32>,
    pub last_error_code: Option<String>,
    pub last_attempt_at_ms: Option<i64>,
    pub delivered_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ProjectWebhookDeliveryList {
    pub object: &'static str,
    pub data: Vec<ProjectWebhookDelivery>,
    pub has_more: bool,
    #[schema(value_type = Option<String>)]
    pub last_id: Option<Uuid>,
}

#[async_trait]
pub trait ProjectWebhookService: Send + Sync {
    async fn list_endpoints(
        &self,
        project_id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectWebhookList, ImageGatewayError>;

    async fn create_endpoint(
        &self,
        project_id: &str,
        actor_user_id: Uuid,
        request: CreateProjectWebhookRequest,
    ) -> Result<CreatedProjectWebhook, ImageGatewayError>;

    async fn update_endpoint(
        &self,
        project_id: &str,
        endpoint_id: &str,
        actor_user_id: Uuid,
        request: UpdateProjectWebhookRequest,
    ) -> Result<ProjectWebhookEndpoint, ImageGatewayError>;

    async fn delete_endpoint(
        &self,
        project_id: &str,
        endpoint_id: &str,
        actor_user_id: Uuid,
    ) -> Result<DeletedProjectWebhook, ImageGatewayError>;

    async fn rotate_secret(
        &self,
        project_id: &str,
        endpoint_id: &str,
        actor_user_id: Uuid,
    ) -> Result<RotatedProjectWebhookSecret, ImageGatewayError>;

    async fn enqueue_test(
        &self,
        project_id: &str,
        endpoint_id: &str,
        actor_user_id: Uuid,
    ) -> Result<ProjectWebhookTestEvent, ImageGatewayError>;

    async fn list_deliveries(
        &self,
        project_id: &str,
        endpoint_id: &str,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<ProjectWebhookDeliveryList, ImageGatewayError>;
}

#[derive(Clone, Debug)]
pub struct WebhookDeliveryLease {
    pub delivery_id: Uuid,
    pub lease_owner: String,
    pub lease_epoch: i64,
    pub attempt_number: i32,
    pub endpoint_id: String,
    pub project_id: String,
    pub url: String,
    pub signing_key_version: u16,
    pub secret_revision: i64,
    pub event_id: String,
    pub event_type: String,
    pub payload_body: Vec<u8>,
    pub retry_deadline_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct WebhookAttemptResult {
    pub http_status: Option<u16>,
    pub error_code: Option<String>,
    pub retry_after_ms: Option<i64>,
    pub duration_ms: i64,
    pub webhook_timestamp: i64,
}

pub(crate) fn validate_event_types(
    event_types: Vec<String>,
) -> Result<Vec<String>, ImageGatewayError> {
    let mut normalized = event_types
        .into_iter()
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    if normalized.is_empty()
        || normalized.len() > SUPPORTED_WEBHOOK_EVENT_TYPES.len()
        || normalized
            .iter()
            .any(|event_type| !SUPPORTED_WEBHOOK_EVENT_TYPES.contains(&event_type.as_str()))
    {
        return Err(ImageGatewayError::invalid_request(
            "event_types must contain one or more supported webhook event types",
            Some("event_types".to_string()),
            "invalid_webhook_event_type",
        ));
    }
    Ok(normalized)
}

pub(crate) fn validate_name(name: Option<String>) -> Result<Option<String>, ImageGatewayError> {
    let Some(name) = name else {
        return Ok(None);
    };
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 128 {
        return Err(ImageGatewayError::invalid_request(
            "name must be between 1 and 128 characters",
            Some("name".to_string()),
            "invalid_webhook_name",
        ));
    }
    Ok(Some(name.to_string()))
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
