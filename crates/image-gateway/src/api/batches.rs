use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    auth::{
        ApiKeyCapability, ApiKeyPermissionMode, ApiKeyPermissions, AuthContext,
        RequestRouteAttribution,
    },
    batches::{
        CreateProjectBatch, DEFAULT_BATCH_RETENTION_SECONDS, MAX_BATCH_REQUESTS, ProjectBatch,
        ProjectBatchPage, ProjectFilePurpose, ProjectScope, ValidatedBatchLine,
    },
    model_routing::ResolvedModelRoute,
    service_tiers::ProjectServiceTier,
};

use super::{
    AppState, IMAGE_GENERATION_ROUTE_OPERATION,
    admin::authorize_project,
    authenticate_image_request,
    files::{batch_service, console_project_scope},
    resolve_surface_model,
    sessions::private_json,
};

const OPENAI_IMAGES_API_PROFILE: &str = "openai-images-v1";
const OPENAI_CODEX_PROVIDER_ID: &str = "openai-codex";
const SUPPORTED_ENDPOINT: &str = "/v1/images/generations";
const COMPLETION_WINDOW: &str = "24h";
const DEFAULT_LIST_LIMIT: usize = 20;
const MAX_LIST_LIMIT: usize = 100;
const MAX_METADATA_ENTRIES: usize = 16;
const MAX_METADATA_KEY_BYTES: usize = 64;
const MAX_METADATA_VALUE_BYTES: usize = 512;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateBatchRequest {
    input_file_id: String,
    endpoint: String,
    completion_window: String,
    #[serde(default)]
    metadata: Option<BTreeMap<String, String>>,
    #[serde(default)]
    output_expires_after: Option<OutputExpiresAfter>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputExpiresAfter {
    anchor: String,
    seconds: u32,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct ListBatchesQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchLineWire {
    custom_id: String,
    method: String,
    url: String,
    body: Value,
}

#[derive(Debug, Serialize)]
struct BatchList {
    object: &'static str,
    data: Vec<BatchObject>,
    first_id: Option<String>,
    last_id: Option<String>,
    has_more: bool,
}

#[derive(Debug, Serialize)]
struct BatchObject {
    id: String,
    object: &'static str,
    endpoint: String,
    errors: Option<Value>,
    input_file_id: String,
    completion_window: String,
    status: crate::batches::BatchStatus,
    output_file_id: Option<String>,
    error_file_id: Option<String>,
    created_at: i64,
    in_progress_at: Option<i64>,
    expires_at: Option<i64>,
    finalizing_at: Option<i64>,
    completed_at: Option<i64>,
    failed_at: Option<i64>,
    expired_at: Option<i64>,
    cancelling_at: Option<i64>,
    cancelled_at: Option<i64>,
    request_counts: BatchRequestCountsObject,
    metadata: Value,
}

#[derive(Debug, Serialize)]
struct BatchRequestCountsObject {
    total: u32,
    completed: u32,
    failed: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct BatchAuthSnapshot {
    tenant_id: String,
    project_id: String,
    project_service_tier: ProjectServiceTier,
    service_account_id: Option<String>,
    api_key_id: Option<String>,
    credential_authz_version: Option<i64>,
    credential_owner_user_id: Option<Uuid>,
    actor_user_id: Option<Uuid>,
    actor_session_id: Option<Uuid>,
    actor_authz_version: Option<i64>,
    api_key_permission_mode: ApiKeyPermissionMode,
    api_key_permissions: ApiKeyPermissions,
    is_admin: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct BatchRouteSnapshot {
    public_model_id: String,
    api_profile: String,
    provider_id: String,
    operation_id: String,
    command_schema: String,
    provider_model_id: String,
    execution_model_id: String,
    media_kind: String,
    route_id: Uuid,
    route_revision: i64,
}

pub(super) async fn create_batch(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<CreateBatchRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let mut auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::BatchesWrite)?;
    auth.require_api_key_capability(ApiKeyCapability::FilesRead)?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesWrite)?;
    let Json(request) = parse_body(body)?;
    create_batch_for_auth(&state, &mut auth, request).await
}

pub(super) async fn create_console_batch(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<CreateBatchRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project(&headers, &state, &project_id, "workspace:write").await?;
    let Json(request) = parse_body(body)?;
    let project_defaults = state
        .api_key_store
        .project_runtime_defaults(&project_id)
        .await?
        .ok_or_else(|| project_not_found())?;
    let mut auth = AuthContext {
        tenant_id: project_defaults.tenant_id,
        project_id,
        project_service_tier: project_defaults.service_tier,
        service_account_id: None,
        api_key_id: None,
        credential_authz_version: None,
        credential_owner_user_id: None,
        actor_user_id: Some(principal.user_id),
        actor_session_id: Some(principal.session_id),
        actor_authz_version: Some(principal.authz_version),
        api_key_permission_mode: ApiKeyPermissionMode::All,
        api_key_permissions: ApiKeyPermissions::default(),
        route: None,
        is_admin: principal.roles.iter().any(|role| role == "platform_owner")
            && principal.scopes.iter().any(|scope| scope == "admin:*"),
    };
    create_batch_for_auth(&state, &mut auth, request).await
}

async fn create_batch_for_auth(
    state: &Arc<AppState>,
    auth: &mut AuthContext,
    request: CreateBatchRequest,
) -> Result<Response, ImageGatewayError> {
    validate_create_request(&request)?;
    let scope = ProjectScope::new(auth.tenant_id.clone(), auth.project_id.clone());
    let service = batch_service(state)?;
    let input = service.get_file(&scope, &request.input_file_id).await?;
    if input.purpose != ProjectFilePurpose::Batch {
        return Err(ImageGatewayError::invalid_request(
            "The input file must have purpose 'batch'",
            Some("input_file_id".to_string()),
            "invalid_batch_input_file",
        ));
    }
    let bytes = service.read_file(&scope, &request.input_file_id).await?;
    let lines = validate_jsonl(&bytes, &request.endpoint)?;
    let model = lines
        .first()
        .map(|line| line.model.as_str())
        .ok_or_else(|| invalid_jsonl(1, "The input file must contain at least one request"))?;
    let resolved = resolve_surface_model(
        state,
        auth,
        IMAGE_GENERATION_ROUTE_OPERATION,
        &[OPENAI_IMAGES_API_PROFILE],
        model,
    )
    .await?
    .ok_or_else(|| ImageGatewayError::model_not_found(model))?;
    if resolved.provider_id != OPENAI_CODEX_PROVIDER_ID
        || resolved.api_profile != OPENAI_IMAGES_API_PROFILE
    {
        return Err(ImageGatewayError::invalid_request(
            "This model does not support the Batch API",
            Some("model".to_string()),
            "batch_model_unsupported",
        ));
    }

    let output_retention = output_retention(&request)?;
    let metadata = serde_json::to_value(request.metadata.unwrap_or_default())
        .map_err(|_| ImageGatewayError::internal("failed to encode batch metadata"))?;
    let safe_auth_snapshot = serde_json::to_value(BatchAuthSnapshot::from(&*auth))
        .map_err(|_| ImageGatewayError::internal("failed to encode batch authorization"))?;
    let route_snapshot = serde_json::to_value(BatchRouteSnapshot::from(&resolved))
        .map_err(|_| ImageGatewayError::internal("failed to encode batch route"))?;
    let batch = service
        .create_batch(
            &scope,
            CreateProjectBatch {
                input_file_id: request.input_file_id,
                endpoint: request.endpoint,
                completion_window: request.completion_window,
                metadata,
                safe_auth_snapshot,
                route_snapshot,
                output_retention,
                lines,
            },
        )
        .await?;
    Ok(private_json(batch_object(batch)))
}

pub(super) async fn list_batches(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListBatchesQuery>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::BatchesRead)?;
    list_batches_for_scope(
        &state,
        ProjectScope::new(auth.tenant_id, auth.project_id),
        query,
    )
    .await
}

pub(super) async fn list_console_batches(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    Query(query): Query<ListBatchesQuery>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    let scope = console_project_scope(&state, &project_id).await?;
    list_batches_for_scope(&state, scope, query).await
}

async fn list_batches_for_scope(
    state: &Arc<AppState>,
    scope: ProjectScope,
    query: ListBatchesQuery,
) -> Result<Response, ImageGatewayError> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let page = batch_service(state)?
        .list_batches(&scope, query.after.as_deref(), limit)
        .await?;
    Ok(private_json(batch_list(page)))
}

pub(super) async fn get_batch(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(batch_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::BatchesRead)?;
    get_batch_for_scope(
        &state,
        ProjectScope::new(auth.tenant_id, auth.project_id),
        &batch_id,
    )
    .await
}

pub(super) async fn get_console_batch(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, batch_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    let scope = console_project_scope(&state, &project_id).await?;
    get_batch_for_scope(&state, scope, &batch_id).await
}

async fn get_batch_for_scope(
    state: &Arc<AppState>,
    scope: ProjectScope,
    batch_id: &str,
) -> Result<Response, ImageGatewayError> {
    let batch = batch_service(state)?.get_batch(&scope, batch_id).await?;
    Ok(private_json(batch_object(batch)))
}

pub(super) async fn cancel_batch(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(batch_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::BatchesWrite)?;
    cancel_batch_for_scope(
        &state,
        ProjectScope::new(auth.tenant_id, auth.project_id),
        &batch_id,
    )
    .await
}

pub(super) async fn cancel_console_batch(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, batch_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:write").await?;
    let scope = console_project_scope(&state, &project_id).await?;
    cancel_batch_for_scope(&state, scope, &batch_id).await
}

async fn cancel_batch_for_scope(
    state: &Arc<AppState>,
    scope: ProjectScope,
    batch_id: &str,
) -> Result<Response, ImageGatewayError> {
    let batch = batch_service(state)?.cancel_batch(&scope, batch_id).await?;
    Ok(private_json(batch_object(batch)))
}

fn validate_create_request(request: &CreateBatchRequest) -> Result<(), ImageGatewayError> {
    if request.input_file_id.trim().is_empty() {
        return Err(missing("input_file_id"));
    }
    if request.endpoint != SUPPORTED_ENDPOINT {
        return Err(ImageGatewayError::invalid_request(
            format!("Only {SUPPORTED_ENDPOINT} is currently supported"),
            Some("endpoint".to_string()),
            "unsupported_batch_endpoint",
        ));
    }
    if request.completion_window != COMPLETION_WINDOW {
        return Err(ImageGatewayError::invalid_request(
            "completion_window must be '24h'",
            Some("completion_window".to_string()),
            "invalid_completion_window",
        ));
    }
    if let Some(metadata) = request.metadata.as_ref() {
        if metadata.len() > MAX_METADATA_ENTRIES {
            return Err(ImageGatewayError::invalid_request(
                format!("metadata may contain at most {MAX_METADATA_ENTRIES} entries"),
                Some("metadata".to_string()),
                "invalid_metadata",
            ));
        }
        for (key, value) in metadata {
            if key.is_empty()
                || key.len() > MAX_METADATA_KEY_BYTES
                || key.chars().any(char::is_control)
            {
                return Err(ImageGatewayError::invalid_request(
                    "metadata keys must contain 1 to 64 non-control UTF-8 bytes",
                    Some("metadata".to_string()),
                    "invalid_metadata",
                ));
            }
            if value.len() > MAX_METADATA_VALUE_BYTES || value.chars().any(char::is_control) {
                return Err(ImageGatewayError::invalid_request(
                    "metadata values must contain at most 512 non-control UTF-8 bytes",
                    Some("metadata".to_string()),
                    "invalid_metadata",
                ));
            }
        }
    }
    if let Some(expiration) = request.output_expires_after.as_ref() {
        if expiration.anchor != "created_at" {
            return Err(ImageGatewayError::invalid_request(
                "output_expires_after.anchor must be 'created_at'",
                Some("output_expires_after.anchor".to_string()),
                "invalid_output_expiration",
            ));
        }
        if !(crate::batches::MIN_FILE_RETENTION_SECONDS
            ..=crate::batches::MAX_FILE_RETENTION_SECONDS)
            .contains(&expiration.seconds)
        {
            return Err(ImageGatewayError::invalid_request(
                "output_expires_after.seconds must be between 3600 and 2592000",
                Some("output_expires_after.seconds".to_string()),
                "invalid_output_expiration",
            ));
        }
    }
    Ok(())
}

fn output_retention(request: &CreateBatchRequest) -> Result<Duration, ImageGatewayError> {
    let seconds = request
        .output_expires_after
        .as_ref()
        .map(|value| value.seconds)
        .unwrap_or(DEFAULT_BATCH_RETENTION_SECONDS);
    Ok(Duration::from_secs(u64::from(seconds)))
}

fn validate_jsonl(
    bytes: &[u8],
    endpoint: &str,
) -> Result<Vec<ValidatedBatchLine>, ImageGatewayError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| invalid_jsonl(1, "The input file must be UTF-8 encoded JSONL"))?;
    let mut lines = Vec::new();
    let mut custom_ids = BTreeSet::new();
    let mut expected_model = None::<String>;
    for (index, raw_line) in text.lines().enumerate() {
        let line_number = index + 1;
        if raw_line.trim().is_empty() {
            return Err(invalid_jsonl(
                line_number,
                "Blank lines are not allowed in Batch input files",
            ));
        }
        if lines.len() >= MAX_BATCH_REQUESTS {
            return Err(invalid_jsonl(
                line_number,
                "A batch may contain at most 50000 requests",
            ));
        }
        let wire = serde_json::from_str::<BatchLineWire>(raw_line)
            .map_err(|_| invalid_jsonl(line_number, "Each line must be a valid Batch request"))?;
        if wire.custom_id.is_empty()
            || wire.custom_id.len() > 256
            || wire.custom_id.chars().any(char::is_control)
        {
            return Err(invalid_jsonl(
                line_number,
                "custom_id must contain 1 to 256 non-control UTF-8 bytes",
            ));
        }
        if !custom_ids.insert(wire.custom_id.clone()) {
            return Err(invalid_jsonl(
                line_number,
                "custom_id values must be unique within a batch",
            ));
        }
        if wire.method != "POST" {
            return Err(invalid_jsonl(line_number, "method must be 'POST'"));
        }
        if wire.url != endpoint {
            return Err(invalid_jsonl(
                line_number,
                "Every request URL must match the batch endpoint",
            ));
        }
        let body = wire
            .body
            .as_object()
            .ok_or_else(|| invalid_jsonl(line_number, "The request body must be a JSON object"))?;
        let model = body
            .get("model")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .ok_or_else(|| invalid_jsonl(line_number, "body.model is required"))?
            .to_string();
        if expected_model
            .as_ref()
            .is_some_and(|expected| expected != &model)
        {
            return Err(invalid_jsonl(
                line_number,
                "All requests in a batch must use the same model",
            ));
        }
        expected_model.get_or_insert_with(|| model.clone());
        lines.push(ValidatedBatchLine {
            ordinal: (line_number - 1) as u32,
            custom_id: wire.custom_id,
            method: wire.method,
            url: wire.url,
            model,
            body: Value::Object(body.clone()),
        });
    }
    if lines.is_empty() {
        return Err(invalid_jsonl(
            1,
            "The input file must contain at least one request",
        ));
    }
    Ok(lines)
}

fn batch_list(page: ProjectBatchPage) -> BatchList {
    let data = page.data.into_iter().map(batch_object).collect::<Vec<_>>();
    BatchList {
        object: "list",
        first_id: data.first().map(|batch| batch.id.clone()),
        last_id: data.last().map(|batch| batch.id.clone()),
        data,
        has_more: page.has_more,
    }
}

fn batch_object(batch: ProjectBatch) -> BatchObject {
    let expired_at = (batch.status == crate::batches::BatchStatus::Expired)
        .then_some(batch.expires_at_ms.div_euclid(1_000));
    BatchObject {
        id: batch.id,
        object: "batch",
        endpoint: batch.endpoint,
        errors: batch.errors,
        input_file_id: batch.input_file_id,
        completion_window: batch.completion_window,
        status: batch.status,
        output_file_id: batch.output_file_id,
        error_file_id: batch.error_file_id,
        created_at: batch.created_at_ms.div_euclid(1_000),
        in_progress_at: batch.in_progress_at_ms.map(to_seconds),
        expires_at: Some(batch.expires_at_ms.div_euclid(1_000)),
        finalizing_at: batch.finalizing_at_ms.map(to_seconds),
        completed_at: batch.completed_at_ms.map(to_seconds),
        failed_at: batch.failed_at_ms.map(to_seconds),
        expired_at,
        cancelling_at: batch.cancel_requested_at_ms.map(to_seconds),
        cancelled_at: batch.cancelled_at_ms.map(to_seconds),
        request_counts: BatchRequestCountsObject {
            total: batch.request_counts.total,
            completed: batch.request_counts.completed,
            failed: batch.request_counts.failed,
        },
        metadata: batch.metadata,
    }
}

impl From<&AuthContext> for BatchAuthSnapshot {
    fn from(auth: &AuthContext) -> Self {
        Self {
            tenant_id: auth.tenant_id.clone(),
            project_id: auth.project_id.clone(),
            project_service_tier: auth.project_service_tier,
            service_account_id: auth.service_account_id.clone(),
            api_key_id: auth.api_key_id.clone(),
            credential_authz_version: auth.credential_authz_version,
            credential_owner_user_id: auth.credential_owner_user_id,
            actor_user_id: auth.actor_user_id,
            actor_session_id: auth.actor_session_id,
            actor_authz_version: auth.actor_authz_version,
            api_key_permission_mode: auth.api_key_permission_mode,
            api_key_permissions: auth.api_key_permissions.clone(),
            is_admin: auth.is_admin,
        }
    }
}

impl BatchAuthSnapshot {
    pub(super) fn matches_scope(&self, scope: &ProjectScope) -> bool {
        self.tenant_id == scope.tenant_id && self.project_id == scope.project_id
    }

    pub(super) fn into_auth(self, route: &BatchRouteSnapshot) -> AuthContext {
        AuthContext {
            tenant_id: self.tenant_id,
            project_id: self.project_id,
            project_service_tier: self.project_service_tier,
            service_account_id: self.service_account_id,
            api_key_id: self.api_key_id,
            credential_authz_version: self.credential_authz_version,
            credential_owner_user_id: self.credential_owner_user_id,
            actor_user_id: self.actor_user_id,
            actor_session_id: self.actor_session_id,
            actor_authz_version: self.actor_authz_version,
            api_key_permission_mode: self.api_key_permission_mode,
            api_key_permissions: self.api_key_permissions,
            route: Some(route.attribution()),
            is_admin: self.is_admin,
        }
    }
}

impl From<&ResolvedModelRoute> for BatchRouteSnapshot {
    fn from(route: &ResolvedModelRoute) -> Self {
        Self {
            public_model_id: route.public_model_id.clone(),
            api_profile: route.api_profile.clone(),
            provider_id: route.provider_id.clone(),
            operation_id: route.operation_id.clone(),
            command_schema: route.command_schema.clone(),
            provider_model_id: route.provider_model_id.clone(),
            execution_model_id: route.execution_model_id.clone(),
            media_kind: route.media_kind.clone(),
            route_id: route.route_id,
            route_revision: route.route_revision,
        }
    }
}

impl BatchRouteSnapshot {
    pub(super) fn resolved(&self) -> ResolvedModelRoute {
        ResolvedModelRoute {
            public_model_id: self.public_model_id.clone(),
            api_profile: self.api_profile.clone(),
            provider_id: self.provider_id.clone(),
            operation_id: self.operation_id.clone(),
            command_schema: self.command_schema.clone(),
            provider_model_id: self.provider_model_id.clone(),
            execution_model_id: self.execution_model_id.clone(),
            media_kind: self.media_kind.clone(),
            route_id: self.route_id,
            route_revision: self.route_revision,
        }
    }

    fn attribution(&self) -> RequestRouteAttribution {
        RequestRouteAttribution {
            public_model_id: self.public_model_id.clone(),
            api_profile: self.api_profile.clone(),
            provider_id: self.provider_id.clone(),
            operation_id: self.operation_id.clone(),
            command_schema: self.command_schema.clone(),
            media_kind: self.media_kind.clone(),
            route_id: self.route_id,
            route_revision: self.route_revision,
        }
    }
}

fn parse_body(
    body: Result<Json<CreateBatchRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<CreateBatchRequest>, ImageGatewayError> {
    body.map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })
}

fn missing(param: &str) -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        format!("{param} is required"),
        Some(param.to_string()),
        "missing_required_parameter",
    )
}

fn invalid_jsonl(line: usize, message: &str) -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        format!("Invalid Batch input at line {line}: {message}"),
        Some("input_file_id".to_string()),
        "invalid_batch_input_file",
    )
}

fn project_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Project was not found",
        Some("project_id".to_string()),
        "project_not_found",
    )
}

fn to_seconds(value: i64) -> i64 {
    value.div_euclid(1_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonl_validation_requires_unique_ids_and_one_model() {
        let valid = br#"{"custom_id":"one","method":"POST","url":"/v1/images/generations","body":{"model":"gpt-image-2","prompt":"one"}}
{"custom_id":"two","method":"POST","url":"/v1/images/generations","body":{"model":"gpt-image-2","prompt":"two"}}"#;
        let lines = validate_jsonl(valid, SUPPORTED_ENDPOINT).expect("valid JSONL");
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1].ordinal, 1);

        let duplicate = br#"{"custom_id":"one","method":"POST","url":"/v1/images/generations","body":{"model":"gpt-image-2","prompt":"one"}}
{"custom_id":"one","method":"POST","url":"/v1/images/generations","body":{"model":"gpt-image-2","prompt":"two"}}"#;
        assert!(validate_jsonl(duplicate, SUPPORTED_ENDPOINT).is_err());

        let mixed_model = br#"{"custom_id":"one","method":"POST","url":"/v1/images/generations","body":{"model":"gpt-image-2","prompt":"one"}}
{"custom_id":"two","method":"POST","url":"/v1/images/generations","body":{"model":"other","prompt":"two"}}"#;
        assert!(validate_jsonl(mixed_model, SUPPORTED_ENDPOINT).is_err());
    }
}
