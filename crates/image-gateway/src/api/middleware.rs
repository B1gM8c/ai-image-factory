use std::{sync::Arc, time::Instant};

use axum::{
    extract::{MatchedPath, Request, State},
    http::HeaderValue,
    middleware::Next,
    response::Response,
};
use uuid::Uuid;

use super::{AppState, RequestId};
use crate::request_observability::{
    ACTIVE_REQUEST_OBSERVATION, RequestObservationContext, RequestObservationRecord,
    ResponseErrorCode, digest_idempotency_key,
};

pub(super) async fn add_request_id(mut request: Request, next: Next) -> Response {
    let request_id = RequestId(new_request_id());
    request.extensions_mut().insert(request_id.clone());

    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&request_id.0) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
        .headers_mut()
        .insert("openai-version", HeaderValue::from_static("2020-10-01"));
    response
}

pub(super) async fn observe_request(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let route_pattern = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .map(str::to_owned);
    let Some((route_pattern, source)) = route_pattern
        .and_then(|route| request_source(&route).map(|source| (route, source.to_owned())))
    else {
        return next.run(request).await;
    };

    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map(|request_id| request_id.0.clone())
        .unwrap_or_else(new_request_id);
    let method = request.method().as_str().to_owned();
    let request_path = request.uri().path().to_owned();
    let idempotency_key_digest = digest_idempotency_key(
        request
            .headers()
            .get("idempotency-key")
            .and_then(|value| value.to_str().ok()),
    );
    let context = RequestObservationContext::new();
    let created_at_ms = now_ms();
    let started = Instant::now();
    let response = ACTIVE_REQUEST_OBSERVATION
        .scope(context.clone(), next.run(request))
        .await;
    let completed_at_ms = now_ms().max(created_at_ms);
    let actor = context.actor();
    let error_code = response
        .extensions()
        .get::<ResponseErrorCode>()
        .and_then(|error| error.0.clone());
    state
        .request_observation_sink
        .submit(RequestObservationRecord {
            request_id,
            source,
            method,
            route_pattern,
            request_path,
            status_code: response.status().as_u16(),
            duration_ms: started.elapsed().as_millis().min(i64::MAX as u128) as i64,
            error_code,
            idempotency_key_digest,
            tenant_id: actor.as_ref().map(|actor| actor.tenant_id.clone()),
            project_id: actor.as_ref().map(|actor| actor.project_id.clone()),
            service_account_id: actor
                .as_ref()
                .and_then(|actor| actor.service_account_id.clone()),
            api_key_id: actor.as_ref().and_then(|actor| actor.api_key_id.clone()),
            credential_owner_user_id: actor
                .as_ref()
                .and_then(|actor| actor.credential_owner_user_id),
            actor_user_id: actor.as_ref().and_then(|actor| actor.actor_user_id),
            auth_kind: actor.map(|actor| actor.auth_kind),
            created_at_ms,
            completed_at_ms,
        });
    response
}

pub(super) fn new_request_id() -> String {
    format!("req_{}", Uuid::new_v4().simple())
}

fn request_source(route: &str) -> Option<&'static str> {
    if route == "/v1/models" {
        return Some("models");
    }
    if route.ends_with("/models") {
        return None;
    }
    if route == "/v1/batches" || route.starts_with("/v1/batches/") {
        return Some("batches");
    }
    if route == "/v1/files" || route.starts_with("/v1/files/") {
        return Some("files");
    }
    if route.contains("/images/") {
        return Some("images");
    }
    if route.contains("/videos/") || route.contains("/contents/generations/tasks") {
        return Some("videos");
    }
    None
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
