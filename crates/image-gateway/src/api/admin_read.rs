use std::{convert::Infallible, future::Future, sync::Arc, time::Duration};

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use serde::Deserialize;
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    admin_read::{
        AdminReadError, AdminReadScope, AuditLogsQuery, ConsoleBillingSnapshot,
        ConsoleJobEconomicsSnapshot, JobCursor, JobsQuery, MAX_AUDIT_LOG_WINDOW_MS,
        MAX_REQUEST_LOG_WINDOW_MS, ProviderAccountRuntimeEvent, RequestLogCursor,
        RequestLogVisibility, RequestLogsQuery, UsageAnalysisQuery, UsageGroupBy, UsageInterval,
    },
};

use super::{
    AppState,
    sessions::{authenticate_identity, authorize_platform_owner, identity_service, private_json},
};

const ADMIN_QUERY_TIMEOUT: Duration = Duration::from_millis(750);
const HOUR_MS: i64 = 60 * 60 * 1_000;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WindowQuery {
    window: Option<String>,
    user_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BillingWindowQuery {
    window: Option<String>,
    user_id: Option<Uuid>,
    project_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UsageAnalysisWindowQuery {
    window: Option<String>,
    interval: Option<String>,
    group_by: Option<String>,
    user_id: Option<Uuid>,
    project_id: Option<String>,
    api_key_id: Option<String>,
    provider_id: Option<String>,
    model: Option<String>,
    operation: Option<String>,
    service_tier: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JobsListQuery {
    window: Option<String>,
    to_ms: Option<i64>,
    limit: Option<u32>,
    cursor_created_at_ms: Option<i64>,
    cursor_job_id: Option<Uuid>,
    provider_id: Option<String>,
    state: Option<String>,
    operation: Option<String>,
    model: Option<String>,
    project_id: Option<String>,
    api_key_id: Option<String>,
    q: Option<String>,
    user_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RequestLogsListQuery {
    window: Option<String>,
    to_ms: Option<i64>,
    limit: Option<u32>,
    cursor_created_at_ms: Option<i64>,
    cursor_request_id: Option<String>,
    visibility: Option<String>,
    source: Option<String>,
    status: Option<String>,
    provider_id: Option<String>,
    model: Option<String>,
    project_id: Option<String>,
    api_key_id: Option<String>,
    q: Option<String>,
    user_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JobEconomicsQuery {
    user_id: Option<Uuid>,
    project_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AuditLogsListQuery {
    window: Option<String>,
    to_ms: Option<i64>,
    limit: Option<u32>,
    after: Option<Uuid>,
    event_type: Option<String>,
    outcome: Option<String>,
    actor_user_id: Option<Uuid>,
    project_id: Option<String>,
    resource_type: Option<String>,
    request_id: Option<String>,
    q: Option<String>,
}

pub(super) async fn overview(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<WindowQuery>,
) -> Result<Response, ImageGatewayError> {
    reject_console_scope(query.user_id)?;
    let principal = authorize_platform_owner(&headers, &state).await?;
    let window_ms = parse_window(query.window.as_deref(), 7 * 24 * HOUR_MS)?;
    let store = admin_read_store(&state)?;
    let snapshot = timeout_query(store.overview(window_ms)).await?;
    tracing::info!(
        admin.user_id = %principal.user_id,
        admin.query = "overview",
        admin.window_ms = window_ms,
        "admin read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn billing_summary(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<BillingWindowQuery>,
) -> Result<Response, ImageGatewayError> {
    reject_console_scope(query.user_id)?;
    let principal = authorize_platform_owner(&headers, &state).await?;
    let window_ms = parse_window(query.window.as_deref(), 31 * 24 * HOUR_MS)?;
    let project_id = normalized_filter(query.project_id, "project_id", 128)?;
    let store = admin_read_store(&state)?;
    let snapshot = timeout_query(store.billing_scoped(
        &AdminReadScope::Platform,
        window_ms,
        project_id.as_deref(),
    ))
    .await?;
    tracing::info!(
        admin.user_id = %principal.user_id,
        admin.query = "billing_summary",
        admin.window_ms = window_ms,
        "admin read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn usage_analysis(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<UsageAnalysisWindowQuery>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let usage_query = parse_usage_query(query)?;
    let window_ms = usage_query.window_ms;
    let snapshot = timeout_query(
        admin_read_store(&state)?.usage_analysis_scoped(&AdminReadScope::Platform, usage_query),
    )
    .await?;
    tracing::info!(
        admin.user_id = %principal.user_id,
        admin.query = "usage_analysis",
        admin.window_ms = window_ms,
        "admin read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn provider_accounts(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let store = admin_read_store(&state)?;
    let snapshot = timeout_query(store.provider_accounts()).await?;
    tracing::info!(
        admin.user_id = %principal.user_id,
        admin.query = "provider_accounts",
        "admin read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn console_overview(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<WindowQuery>,
) -> Result<Response, ImageGatewayError> {
    let (principal, scope) = console_read_scope(&headers, &state, query.user_id).await?;
    let window_ms = parse_window(query.window.as_deref(), 7 * 24 * HOUR_MS)?;
    let snapshot =
        timeout_query(admin_read_store(&state)?.overview_scoped(&scope, window_ms)).await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        console.query = "overview",
        console.scope = ?scope,
        "console read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn console_billing_summary(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<BillingWindowQuery>,
) -> Result<Response, ImageGatewayError> {
    let (principal, scope) = console_read_scope(&headers, &state, query.user_id).await?;
    let window_ms = parse_window(query.window.as_deref(), 31 * 24 * HOUR_MS)?;
    let project_id = normalized_filter(query.project_id, "project_id", 128)?;
    let mut snapshot = timeout_query(admin_read_store(&state)?.billing_scoped(
        &scope,
        window_ms,
        project_id.as_deref(),
    ))
    .await?;
    let platform_owner = principal.roles.iter().any(|role| role == "platform_owner")
        && principal.scopes.iter().any(|scope| scope == "admin:*");
    if !platform_owner {
        snapshot.account_snapshots.retain(|account| {
            principal.organizations.iter().any(|membership| {
                membership.organization_id == account.tenant_id && membership.role == "owner"
            })
        });
    }
    tracing::info!(
        actor.user_id = %principal.user_id,
        console.query = "billing_summary",
        console.scope = ?scope,
        "console read completed"
    );
    Ok(private_json(ConsoleBillingSnapshot::from(snapshot)))
}

pub(super) async fn console_usage_analysis(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<UsageAnalysisWindowQuery>,
) -> Result<Response, ImageGatewayError> {
    let requested_user_id = query.user_id;
    let (principal, scope) = console_read_scope(&headers, &state, requested_user_id).await?;
    let usage_query = parse_usage_query(query)?;
    let window_ms = usage_query.window_ms;
    let snapshot =
        timeout_query(admin_read_store(&state)?.usage_analysis_scoped(&scope, usage_query)).await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        console.query = "usage_analysis",
        console.scope = ?scope,
        console.window_ms = window_ms,
        "console read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn provider_account_runtime_events(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let store = admin_read_store(&state)?;
    let hub = state
        .provider_account_runtime_events
        .as_ref()
        .ok_or_else(|| {
            ImageGatewayError::service_unavailable(
                "Provider account runtime events are not enabled",
            )
        })?;
    let (receiver, baseline_sequence) = hub.subscribe();
    let snapshot = timeout_query(store.provider_account_concurrency(None)).await?;
    let initial = hub.snapshot_event(baseline_sequence, snapshot);
    let hub = Arc::clone(hub);

    let initial_stream = tokio_stream::once(Ok::<_, Infallible>(sse_event(initial)));
    let update_stream = BroadcastStream::new(receiver).map(move |event| {
        let event = match event {
            Ok(event) => event,
            Err(_) => hub.resync_required_event(),
        };
        Ok::<_, Infallible>(sse_event(event))
    });
    let mut response = Sse::new(initial_stream.chain(update_stream))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    tracing::info!(
        admin.user_id = %principal.user_id,
        admin.query = "provider_account_runtime_events",
        "admin event stream opened"
    );
    Ok(response)
}

fn sse_event(event: ProviderAccountRuntimeEvent) -> Event {
    let sequence = event.sequence.to_string();
    let data =
        serde_json::to_string(&event).expect("provider account runtime events must serialize");
    Event::default().id(sequence).data(data)
}

pub(super) async fn scheduler_queues(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<WindowQuery>,
) -> Result<Response, ImageGatewayError> {
    reject_console_scope(query.user_id)?;
    let principal = authorize_platform_owner(&headers, &state).await?;
    let window_ms = parse_window(query.window.as_deref(), 7 * 24 * HOUR_MS)?;
    let store = admin_read_store(&state)?;
    let snapshot = timeout_query(store.scheduler(window_ms)).await?;
    tracing::info!(
        admin.user_id = %principal.user_id,
        admin.query = "scheduler_queues",
        admin.window_ms = window_ms,
        "admin read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn list_jobs(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<JobsListQuery>,
) -> Result<Response, ImageGatewayError> {
    reject_console_scope(query.user_id)?;
    let principal = authorize_platform_owner(&headers, &state).await?;
    let jobs_query = parse_jobs_query(query)?;
    let store = admin_read_store(&state)?;
    let snapshot = timeout_query(store.jobs(jobs_query)).await?;
    tracing::info!(
        admin.user_id = %principal.user_id,
        admin.query = "jobs",
        admin.result_count = snapshot.items.len(),
        "admin read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn console_jobs(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<JobsListQuery>,
) -> Result<Response, ImageGatewayError> {
    let requested_user_id = query.user_id;
    let (principal, scope) = console_read_scope(&headers, &state, requested_user_id).await?;
    let jobs_query = parse_jobs_query(query)?;
    let snapshot = timeout_query(admin_read_store(&state)?.jobs_scoped(&scope, jobs_query)).await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        console.query = "jobs",
        console.scope = ?scope,
        console.result_count = snapshot.items.len(),
        "console read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn request_logs(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<RequestLogsListQuery>,
) -> Result<Response, ImageGatewayError> {
    reject_console_scope(query.user_id)?;
    let principal = authorize_platform_owner(&headers, &state).await?;
    let logs_query = parse_request_logs_query(query)?;
    let snapshot = timeout_query(admin_read_store(&state)?.request_logs(logs_query)).await?;
    tracing::info!(
        admin.user_id = %principal.user_id,
        admin.query = "request_logs",
        admin.result_count = snapshot.items.len(),
        "admin read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn audit_logs(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<AuditLogsListQuery>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let query = parse_audit_logs_query(query)?;
    let snapshot = timeout_query(admin_read_store(&state)?.audit_logs(query)).await?;
    tracing::info!(
        admin.user_id = %principal.user_id,
        admin.query = "audit_logs",
        admin.result_count = snapshot.data.len(),
        "admin audit log read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn console_request_logs(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<RequestLogsListQuery>,
) -> Result<Response, ImageGatewayError> {
    let requested_user_id = query.user_id;
    let (principal, scope) = console_read_scope(&headers, &state, requested_user_id).await?;
    let logs_query = parse_request_logs_query(query)?;
    let snapshot =
        timeout_query(admin_read_store(&state)?.request_logs_scoped(&scope, logs_query)).await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        console.query = "request_logs",
        console.scope = ?scope,
        console.result_count = snapshot.items.len(),
        "console read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn job_economics(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner(&headers, &state).await?;
    let snapshot = timeout_query(admin_read_store(&state)?.job_economics(job_id)).await?;
    tracing::info!(
        admin.user_id = %principal.user_id,
        admin.query = "job_economics",
        admin.job_id = %job_id,
        "admin read completed"
    );
    Ok(private_json(snapshot))
}

pub(super) async fn console_job_economics(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<Uuid>,
    Query(query): Query<JobEconomicsQuery>,
) -> Result<Response, ImageGatewayError> {
    let (principal, scope) = console_read_scope(&headers, &state, query.user_id).await?;
    let project_id = normalized_filter(query.project_id, "project_id", 128)?;
    let snapshot =
        timeout_query(admin_read_store(&state)?.job_economics_scoped(&scope, job_id, project_id))
            .await?;
    tracing::info!(
        actor.user_id = %principal.user_id,
        console.query = "job_economics",
        console.scope = ?scope,
        console.job_id = %job_id,
        "console read completed"
    );
    Ok(private_json(ConsoleJobEconomicsSnapshot::from(snapshot)))
}

fn parse_jobs_query(query: JobsListQuery) -> Result<JobsQuery, ImageGatewayError> {
    let cursor = match (query.cursor_created_at_ms, query.cursor_job_id) {
        (None, None) => None,
        (Some(created_at_ms), Some(job_id)) => Some(JobCursor {
            created_at_ms,
            job_id,
        }),
        _ => return Err(invalid_query("both cursor fields are required")),
    };
    let jobs_query = JobsQuery {
        window_ms: parse_window(query.window.as_deref(), 31 * 24 * HOUR_MS)?,
        to_ms: query.to_ms,
        limit: query.limit.unwrap_or(50),
        cursor,
        provider_id: normalized_filter(query.provider_id, "provider_id", 128)?,
        state: normalized_filter(query.state, "state", 64)?,
        operation: normalized_filter(query.operation, "operation", 64)?,
        model: normalized_filter(query.model, "model", 255)?,
        project_id: normalized_filter(query.project_id, "project_id", 128)?,
        api_key_id: normalized_filter(query.api_key_id, "api_key_id", 128)?,
        request_or_job_id: normalized_filter(query.q, "q", 255)?,
    };
    Ok(jobs_query)
}

fn parse_request_logs_query(
    query: RequestLogsListQuery,
) -> Result<RequestLogsQuery, ImageGatewayError> {
    let cursor = match (query.cursor_created_at_ms, query.cursor_request_id) {
        (None, None) => None,
        (Some(created_at_ms), Some(request_id)) => Some(RequestLogCursor {
            created_at_ms,
            request_id: normalized_filter(Some(request_id), "cursor_request_id", 255)?
                .ok_or_else(|| invalid_query("cursor_request_id is required"))?,
        }),
        _ => return Err(invalid_query("both request log cursor fields are required")),
    };
    let project_id = normalized_filter(query.project_id, "project_id", 128)?;
    let visibility = match query.visibility.as_deref() {
        None if project_id.is_some() => RequestLogVisibility::Project,
        None => RequestLogVisibility::Mine,
        Some("mine") => RequestLogVisibility::Mine,
        Some("project") => RequestLogVisibility::Project,
        Some(_) => return Err(invalid_query("visibility must be mine or project")),
    };
    Ok(RequestLogsQuery {
        window_ms: parse_window(query.window.as_deref(), MAX_REQUEST_LOG_WINDOW_MS)?,
        to_ms: query.to_ms,
        limit: query.limit.unwrap_or(50),
        cursor,
        visibility,
        source: normalized_filter(query.source, "source", 32)?,
        status: normalized_filter(query.status, "status", 32)?,
        provider_id: normalized_filter(query.provider_id, "provider_id", 128)?,
        model: normalized_filter(query.model, "model", 255)?,
        project_id,
        api_key_id: normalized_filter(query.api_key_id, "api_key_id", 128)?,
        request_or_job_id: normalized_filter(query.q, "q", 255)?,
    })
}

fn parse_audit_logs_query(query: AuditLogsListQuery) -> Result<AuditLogsQuery, ImageGatewayError> {
    Ok(AuditLogsQuery {
        window_ms: parse_window(query.window.as_deref(), MAX_AUDIT_LOG_WINDOW_MS)?,
        to_ms: query.to_ms,
        limit: query.limit.unwrap_or(50),
        after: query.after,
        event_type: normalized_filter(query.event_type, "event_type", 128)?,
        outcome: normalized_filter(query.outcome, "outcome", 16)?,
        actor_user_id: query.actor_user_id,
        project_id: normalized_filter(query.project_id, "project_id", 128)?,
        resource_type: normalized_filter(query.resource_type, "resource_type", 64)?,
        request_id: normalized_filter(query.request_id, "request_id", 255)?,
        query: normalized_filter(query.q, "q", 255)?,
    })
}

fn parse_usage_query(
    query: UsageAnalysisWindowQuery,
) -> Result<UsageAnalysisQuery, ImageGatewayError> {
    let window_ms = parse_window(query.window.as_deref(), 31 * 24 * HOUR_MS)?;
    let interval = match query.interval.as_deref() {
        None if window_ms <= 24 * HOUR_MS => UsageInterval::Hour,
        None => UsageInterval::Day,
        Some("1m") => UsageInterval::Minute,
        Some("1h") => UsageInterval::Hour,
        Some("1d") => UsageInterval::Day,
        Some(_) => return Err(invalid_query("interval must be one of 1m, 1h, or 1d")),
    };
    let group_by = match query.group_by.as_deref().unwrap_or("line_item") {
        "none" => UsageGroupBy::None,
        "line_item" => UsageGroupBy::LineItem,
        "project" => UsageGroupBy::Project,
        "api_key" => UsageGroupBy::ApiKey,
        "user" => UsageGroupBy::User,
        "provider" => UsageGroupBy::Provider,
        "model" => UsageGroupBy::Model,
        "operation" => UsageGroupBy::Operation,
        "service_tier" => UsageGroupBy::ServiceTier,
        _ => {
            return Err(invalid_query(
                "group_by must be one of none, line_item, project, api_key, user, provider, model, operation, or service_tier",
            ));
        }
    };
    Ok(UsageAnalysisQuery {
        window_ms,
        interval,
        group_by,
        project_id: normalized_filter(query.project_id, "project_id", 128)?,
        api_key_id: normalized_filter(query.api_key_id, "api_key_id", 128)?,
        filter_user_id: query.user_id,
        provider_id: normalized_filter(query.provider_id, "provider_id", 128)?,
        model: normalized_filter(query.model, "model", 255)?,
        operation: normalized_filter(query.operation, "operation", 64)?,
        service_tier: normalized_service_tier_filter(query.service_tier)?,
    })
}

fn normalized_service_tier_filter(
    value: Option<String>,
) -> Result<Option<String>, ImageGatewayError> {
    let value = normalized_filter(value, "service_tier", 16)?;
    if value
        .as_deref()
        .is_some_and(|value| !matches!(value, "default" | "flex" | "priority"))
    {
        return Err(invalid_query(
            "service_tier must be default, flex, or priority",
        ));
    }
    Ok(value)
}

async fn console_read_scope(
    headers: &HeaderMap,
    state: &Arc<AppState>,
    requested_user_id: Option<Uuid>,
) -> Result<(factory_identity::AuthenticatedPrincipal, AdminReadScope), ImageGatewayError> {
    let principal = authenticate_identity(headers, state).await?;
    let platform_owner = principal.roles.iter().any(|role| role == "platform_owner")
        && principal.scopes.iter().any(|scope| scope == "admin:*");
    if !platform_owner && !principal.has_scope("workspace:read") {
        return Err(ImageGatewayError::forbidden(
            "Workspace read permission is required",
        ));
    }

    let Some(target_user_id) = requested_user_id else {
        if platform_owner {
            return Ok((principal, AdminReadScope::Platform));
        }
        let tenant_ids = principal
            .organizations
            .iter()
            .map(|membership| membership.organization_id.clone())
            .collect::<Vec<_>>();
        let project_ids = principal
            .projects
            .iter()
            .map(|membership| membership.project_id.clone())
            .collect::<Vec<_>>();
        let user_id = principal.user_id;
        return member_scope(principal, user_id, tenant_ids, project_ids);
    };

    if target_user_id == principal.user_id {
        let tenant_ids = principal
            .organizations
            .iter()
            .map(|membership| membership.organization_id.clone())
            .collect::<Vec<_>>();
        let project_ids = principal
            .projects
            .iter()
            .map(|membership| membership.project_id.clone())
            .collect::<Vec<_>>();
        return member_scope(principal, target_user_id, tenant_ids, project_ids);
    }
    if !platform_owner {
        return Err(ImageGatewayError::forbidden(
            "The requested user scope is not available",
        ));
    }
    let target = identity_service(state)?
        .get_user_access(target_user_id)
        .await
        .map_err(super::sessions::map_identity_error)?
        .ok_or_else(|| {
            ImageGatewayError::not_found("User was not found", None, "user_not_found")
        })?;
    let tenant_ids = target
        .organizations
        .iter()
        .map(|membership| membership.organization_id.clone())
        .collect::<Vec<_>>();
    let project_ids = target
        .projects
        .into_iter()
        .map(|membership| membership.project_id)
        .collect::<Vec<_>>();
    member_scope(principal, target_user_id, tenant_ids, project_ids)
}

fn member_scope(
    principal: factory_identity::AuthenticatedPrincipal,
    user_id: Uuid,
    tenant_ids: Vec<String>,
    project_ids: Vec<String>,
) -> Result<(factory_identity::AuthenticatedPrincipal, AdminReadScope), ImageGatewayError> {
    if tenant_ids.is_empty() {
        return Err(ImageGatewayError::forbidden(
            "No active workspace membership is available",
        ));
    }
    Ok((
        principal,
        AdminReadScope::User {
            user_id,
            tenant_ids,
            project_ids,
        },
    ))
}

fn reject_console_scope(user_id: Option<Uuid>) -> Result<(), ImageGatewayError> {
    if user_id.is_some() {
        return Err(invalid_query(
            "user_id is supported only by the console read endpoints",
        ));
    }
    Ok(())
}

fn admin_read_store(
    state: &Arc<AppState>,
) -> Result<&dyn crate::admin_read::AdminReadStore, ImageGatewayError> {
    state
        .admin_read_store
        .as_deref()
        .ok_or_else(|| ImageGatewayError::service_unavailable("Admin read service is not enabled"))
}

async fn timeout_query<T>(
    future: impl Future<Output = Result<T, AdminReadError>>,
) -> Result<T, ImageGatewayError> {
    tokio::time::timeout(ADMIN_QUERY_TIMEOUT, future)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("Admin read query timed out"))?
        .map_err(map_read_error)
}

fn parse_window(value: Option<&str>, maximum_ms: i64) -> Result<i64, ImageGatewayError> {
    let window_ms = match value.unwrap_or("24h") {
        "1h" => HOUR_MS,
        "6h" => 6 * HOUR_MS,
        "24h" => 24 * HOUR_MS,
        "7d" => 7 * 24 * HOUR_MS,
        "30d" => 30 * 24 * HOUR_MS,
        "90d" => 90 * 24 * HOUR_MS,
        _ => {
            return Err(invalid_query(
                "window must be one of 1h, 6h, 24h, 7d, 30d, or 90d",
            ));
        }
    };
    if window_ms > maximum_ms {
        return Err(invalid_query("window exceeds the endpoint limit"));
    }
    Ok(window_ms)
}

fn normalized_filter(
    value: Option<String>,
    field: &'static str,
    maximum_len: usize,
) -> Result<Option<String>, ImageGatewayError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() || value.len() > maximum_len || value.chars().any(char::is_control) {
        return Err(invalid_query(&format!("{field} is invalid")));
    }
    Ok(Some(value.to_owned()))
}

fn map_read_error(error: AdminReadError) -> ImageGatewayError {
    match error {
        AdminReadError::InvalidQuery(message) => invalid_query(&message),
        AdminReadError::NotFound => ImageGatewayError::not_found(
            "The requested resource was not found",
            None,
            "resource_not_found",
        ),
        AdminReadError::Unavailable => {
            ImageGatewayError::service_unavailable("Admin read store is unavailable")
        }
    }
}

fn invalid_query(message: &str) -> ImageGatewayError {
    ImageGatewayError::invalid_request(message, None, "invalid_admin_query")
}

#[cfg(test)]
mod tests {
    use super::{UsageAnalysisWindowQuery, parse_usage_query, parse_window};

    #[test]
    fn accepts_only_bounded_window_presets() {
        const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
        assert_eq!(parse_window(None, 7 * DAY_MS).unwrap(), DAY_MS);
        assert_eq!(parse_window(Some("7d"), 7 * DAY_MS).unwrap(), 7 * DAY_MS);
        assert!(parse_window(Some("30d"), 7 * DAY_MS).is_err());
        assert!(parse_window(Some("2h"), 7 * DAY_MS).is_err());
    }

    #[test]
    fn usage_query_defaults_interval_and_rejects_unknown_dimensions() {
        let query = parse_usage_query(UsageAnalysisWindowQuery {
            window: Some("24h".to_string()),
            interval: None,
            group_by: Some("model".to_string()),
            user_id: None,
            project_id: None,
            api_key_id: None,
            provider_id: None,
            model: None,
            operation: None,
            service_tier: None,
        })
        .unwrap();
        assert_eq!(query.interval.as_str(), "1h");
        assert_eq!(query.group_by.as_str(), "model");

        assert!(
            parse_usage_query(UsageAnalysisWindowQuery {
                window: Some("24h".to_string()),
                interval: Some("15m".to_string()),
                group_by: None,
                user_id: None,
                project_id: None,
                api_key_id: None,
                provider_id: None,
                model: None,
                operation: None,
                service_tier: None,
            })
            .is_err()
        );
    }
}
