use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};

use axum::{
    body::to_bytes,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use serde_json::{Value, json};
use tokio::task::{JoinHandle, JoinSet};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    batches::{
        BatchExecutionSnapshot, BatchRequestLease, BatchRequestSuccess, BatchService, BatchStatus,
        BatchWorkTarget, ProjectFileCleanupLease,
    },
};

use super::{
    AppState,
    batches::{BatchAuthSnapshot, BatchRouteSnapshot},
    images::generate_batch_with_resolved_auth,
};

const SCAN_LIMIT: usize = 4;
const CLAIM_LIMIT: usize = 4;
const SCAN_INTERVAL: Duration = Duration::from_millis(500);
const LEASE_GRACE: Duration = Duration::from_secs(90);
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_REQUEST_ATTEMPTS: u32 = 8;
const MIN_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(64);
const IDEMPOTENCY_RECOVERY_GRACE: Duration = Duration::from_secs(6);
const FINALIZATION_LEASE_DURATION: Duration = Duration::from_secs(10 * 60);
const FILE_CLEANUP_SCAN_INTERVAL: Duration = Duration::from_secs(30);
const FILE_CLEANUP_LEASE_DURATION: Duration = Duration::from_secs(60);
const FILE_CLEANUP_LIMIT: usize = 16;

pub(super) fn spawn(state: Arc<AppState>) {
    if state.batch_service.is_none() {
        return;
    }
    supervise_task(
        "Batch file cleanup worker",
        spawn_file_cleanup(Arc::clone(&state)),
    );
    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(SCAN_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut tasks = JoinSet::new();
        let mut active = HashSet::new();
        let mut task_keys = HashMap::new();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match service(&state).and_then(|service| {
                        Ok(Arc::clone(service))
                    }) {
                        Ok(service) => match service.list_runnable_batches(SCAN_LIMIT).await {
                            Ok(targets) => {
                                for target in targets {
                                    let key = (
                                        target.scope.tenant_id.clone(),
                                        target.scope.project_id.clone(),
                                        target.batch_id.clone(),
                                    );
                                    if active.len() >= SCAN_LIMIT || !active.insert(key.clone()) {
                                        continue;
                                    }
                                    let state = Arc::clone(&state);
                                    let handle = tasks.spawn(async move {
                                        process_target(&state, target).await
                                    });
                                    task_keys.insert(handle.id(), key);
                                }
                            }
                            Err(error) => tracing::warn!(
                                error.code = error.error_code().unwrap_or("unknown"),
                                "Batch worker scan failed"
                            ),
                        },
                        Err(error) => tracing::warn!(
                            error.code = error.error_code().unwrap_or("unknown"),
                            "Batch worker service is unavailable"
                        ),
                    }
                }
                joined = tasks.join_next_with_id(), if !tasks.is_empty() => {
                    match joined {
                        Some(Ok((task_id, Ok(())))) => {
                            remove_active_task(&mut active, &mut task_keys, task_id);
                        }
                        Some(Ok((task_id, Err(error)))) => {
                            remove_active_task(&mut active, &mut task_keys, task_id);
                            tracing::warn!(
                                error.code = error.error_code().unwrap_or("unknown"),
                                "Batch processing pass failed"
                            );
                        }
                        Some(Err(error)) => {
                            remove_active_task(&mut active, &mut task_keys, error.id());
                            tracing::warn!(?error, "Batch processing task stopped unexpectedly");
                        }
                        None => {}
                    }
                }
            }
        }
    });
    supervise_task("Batch worker", handle);
}

fn spawn_file_cleanup(state: Arc<AppState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(FILE_CLEANUP_SCAN_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Ok(service) = service(&state).map(Arc::clone) else {
                tracing::warn!("Batch file cleanup service is unavailable");
                continue;
            };
            let leases = match service
                .claim_file_cleanup(
                    &state.worker_id,
                    FILE_CLEANUP_LIMIT,
                    FILE_CLEANUP_LEASE_DURATION,
                )
                .await
            {
                Ok(leases) => leases,
                Err(error) => {
                    tracing::warn!(
                        error.code = error.error_code().unwrap_or("unknown"),
                        "Batch file cleanup scan failed"
                    );
                    continue;
                }
            };
            for lease in leases {
                process_file_cleanup(&service, lease).await;
            }
        }
    })
}

fn supervise_task(name: &'static str, handle: JoinHandle<()>) {
    tokio::spawn(async move {
        match handle.await {
            Ok(()) => tracing::error!(worker.name = name, "Background worker stopped"),
            Err(error) => {
                tracing::error!(worker.name = name, ?error, "Background worker crashed");
            }
        }
        std::process::abort();
    });
}

async fn process_file_cleanup(service: &Arc<dyn BatchService>, lease: ProjectFileCleanupLease) {
    if let Err(error) = service.delete_file_blob(&lease).await {
        tracing::warn!(
            file.id = lease.file_id,
            error.code = error.error_code().unwrap_or("unknown"),
            "Batch file blob cleanup failed"
        );
        if let Err(release_error) = service.release_file_cleanup(&lease).await {
            tracing::warn!(
                error.code = release_error.error_code().unwrap_or("unknown"),
                "Batch file cleanup lease release failed"
            );
        }
        return;
    }
    if let Err(error) = service.complete_file_cleanup(&lease).await {
        tracing::warn!(
            file.id = lease.file_id,
            error.code = error.error_code().unwrap_or("unknown"),
            "Batch file cleanup completion failed"
        );
    }
}

async fn process_target(
    state: &Arc<AppState>,
    mut target: BatchWorkTarget,
) -> Result<(), ImageGatewayError> {
    let service = service(state)?;
    if now_ms() >= target.expires_at_ms {
        let batch = service
            .expire_batch(&target.scope, &target.batch_id)
            .await?;
        target.status = batch.status;
        if target.status.is_terminal() {
            return Ok(());
        }
    }
    if target.status == BatchStatus::Validating {
        let batch = service
            .mark_batch_validated(&target.scope, &target.batch_id)
            .await?;
        target.status = batch.status;
    }

    if target.status == BatchStatus::InProgress {
        process_requests(state, service, &target).await?;
    }
    finalize_if_ready(state, service, &target).await
}

async fn process_requests(
    state: &Arc<AppState>,
    service: &Arc<dyn BatchService>,
    target: &BatchWorkTarget,
) -> Result<(), ImageGatewayError> {
    let snapshot = service
        .load_execution_snapshot(&target.scope, &target.batch_id)
        .await?;
    let auth = decode_auth_snapshot(&snapshot)?;
    let route = decode_route_snapshot(&snapshot)?;
    let lease_duration = state.config.request_timeout.saturating_add(LEASE_GRACE);
    let leases = service
        .claim_requests(
            &target.scope,
            &target.batch_id,
            &state.worker_id,
            CLAIM_LIMIT,
            lease_duration,
        )
        .await?;
    let mut tasks = JoinSet::new();
    let batch_expires_at_ms = target.expires_at_ms;
    for lease in leases {
        let state = Arc::clone(state);
        let service = Arc::clone(service);
        let auth = auth.clone();
        let route = route.clone();
        tasks.spawn(async move {
            execute_request(&state, &service, lease, auth, route, batch_expires_at_ms).await;
        });
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            tracing::warn!(?error, "Batch request execution task stopped unexpectedly");
        }
    }
    Ok(())
}

async fn execute_request(
    state: &Arc<AppState>,
    service: &Arc<dyn BatchService>,
    lease: BatchRequestLease,
    auth_snapshot: BatchAuthSnapshot,
    route_snapshot: BatchRouteSnapshot,
    batch_expires_at_ms: i64,
) {
    let mut headers = HeaderMap::new();
    let idempotency_key =
        batch_idempotency_key(&lease.batch_id, lease.request_id, &lease.request_hash);
    let Ok(idempotency_value) = HeaderValue::from_str(&idempotency_key) else {
        let _ = service
            .fail_request(
                &lease,
                batch_error(
                    "invalid_batch_request",
                    "The batch request idempotency key is invalid",
                ),
            )
            .await;
        return;
    };
    headers.insert("idempotency-key", idempotency_value);
    let request_id = format!("batch-{}", lease.request_id.simple());
    let auth = auth_snapshot.into_auth(&route_snapshot);
    let route = route_snapshot.resolved();
    let response = generate_batch_with_resolved_auth(
        state,
        auth,
        &headers,
        request_id,
        lease.body.clone(),
        route,
    )
    .await;

    match response {
        Ok(response) => {
            let status = response.status();
            let retry_after = retry_after_delay(response.headers());
            let response_request_id = response
                .headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let bytes = match to_bytes(response.into_body(), MAX_RESPONSE_BYTES).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    let _ = service
                        .fail_request(
                            &lease,
                            batch_error(
                                "batch_response_too_large",
                                "The image response exceeded the batch response limit",
                            ),
                        )
                        .await;
                    return;
                }
            };
            let body = match serde_json::from_slice::<Value>(&bytes) {
                Ok(body) => body,
                Err(_) => {
                    let _ = service
                        .fail_request(
                            &lease,
                            batch_error(
                                "invalid_upstream_response",
                                "The image response was not valid JSON",
                            ),
                        )
                        .await;
                    return;
                }
            };
            if should_retry_status(status) {
                schedule_retry(
                    service,
                    &lease,
                    batch_expires_at_ms,
                    retry_after,
                    "transient_provider_error",
                    "The provider returned a transient response",
                )
                .await;
            } else if status.is_success() {
                if let Err(error) = service
                    .complete_request(
                        &lease,
                        BatchRequestSuccess {
                            status_code: status.as_u16(),
                            request_id: response_request_id,
                            body,
                        },
                    )
                    .await
                {
                    tracing::warn!(
                        error.code = error.error_code().unwrap_or("unknown"),
                        "Batch request completion was rejected"
                    );
                }
            } else {
                let error = body
                    .get("error")
                    .filter(|value| value.is_object())
                    .cloned()
                    .unwrap_or_else(|| {
                        batch_error("image_request_failed", "The image request failed")
                    });
                let _ = service.fail_request(&lease, error).await;
            }
        }
        Err(error) if error.error_code() == Some("idempotency_in_progress") => {
            schedule_retry(
                service,
                &lease,
                batch_expires_at_ms,
                Some(idempotency_recovery_delay(state.config.queue_timeout)),
                "idempotency_in_progress",
                "The previous execution attempt is still being reconciled",
            )
            .await;
        }
        Err(error) if should_retry_status(error.status_code()) => {
            schedule_retry(
                service,
                &lease,
                batch_expires_at_ms,
                None,
                error.error_code().unwrap_or("transient_provider_error"),
                "The image request encountered a transient error",
            )
            .await;
        }
        Err(error) => {
            let response = error.into_response();
            let body = to_bytes(response.into_body(), 1024 * 1024)
                .await
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
            let error = body
                .as_ref()
                .and_then(|body| body.get("error"))
                .filter(|value| value.is_object())
                .cloned()
                .unwrap_or_else(|| batch_error("image_request_failed", "The image request failed"));
            let _ = service.fail_request(&lease, error).await;
        }
    }
}

async fn finalize_if_ready(
    state: &Arc<AppState>,
    service: &Arc<dyn BatchService>,
    target: &BatchWorkTarget,
) -> Result<(), ImageGatewayError> {
    let Some(lease) = service
        .claim_finalization(
            &target.scope,
            &target.batch_id,
            &state.worker_id,
            FINALIZATION_LEASE_DURATION,
        )
        .await?
    else {
        return Ok(());
    };
    service.materialize_result_files(&lease).await?;
    let batch = service.finalize_batch(&lease).await?;
    tracing::info!(
        batch.id = %batch.id,
        batch.status = batch.status.as_str(),
        batch.completed = batch.request_counts.completed,
        batch.failed = batch.request_counts.failed,
        "Batch reached a terminal state"
    );
    Ok(())
}

fn decode_auth_snapshot(
    snapshot: &BatchExecutionSnapshot,
) -> Result<BatchAuthSnapshot, ImageGatewayError> {
    let auth = serde_json::from_value::<BatchAuthSnapshot>(snapshot.safe_auth_snapshot.clone())
        .map_err(|_| ImageGatewayError::internal("stored batch authorization is invalid"))?;
    if !auth.matches_scope(&snapshot.scope) {
        return Err(ImageGatewayError::internal(
            "stored batch authorization does not match its project",
        ));
    }
    Ok(auth)
}

fn decode_route_snapshot(
    snapshot: &BatchExecutionSnapshot,
) -> Result<BatchRouteSnapshot, ImageGatewayError> {
    serde_json::from_value::<BatchRouteSnapshot>(snapshot.route_snapshot.clone())
        .map_err(|_| ImageGatewayError::internal("stored batch route is invalid"))
}

fn service(state: &Arc<AppState>) -> Result<&Arc<dyn BatchService>, ImageGatewayError> {
    state
        .batch_service
        .as_ref()
        .ok_or_else(|| ImageGatewayError::service_unavailable("Batch API is unavailable"))
}

fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::BAD_GATEWAY
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::GATEWAY_TIMEOUT
        || status.is_server_error()
}

fn batch_error(code: &str, message: &str) -> Value {
    json!({
        "code": code,
        "message": message,
        "param": Value::Null,
        "type": "invalid_request_error",
    })
}

async fn schedule_retry(
    service: &Arc<dyn BatchService>,
    lease: &BatchRequestLease,
    batch_expires_at_ms: i64,
    retry_after: Option<Duration>,
    code: &str,
    message: &str,
) {
    let now = now_ms();
    let Some(delay) = retry_delay(
        lease.attempt_count,
        retry_after,
        retry_jitter_seed(),
        now,
        batch_expires_at_ms,
    ) else {
        let _ = service
            .fail_request(
                lease,
                json!({
                    "code": "batch_retry_exhausted",
                    "message": "The batch request exhausted its retry budget",
                    "param": Value::Null,
                    "type": "server_error",
                    "retryable": false,
                    "attempt": lease.attempt_count,
                    "last_error": code,
                }),
            )
            .await;
        return;
    };
    let error = json!({
        "code": code,
        "message": message,
        "param": Value::Null,
        "type": "server_error",
        "retryable": true,
        "attempt": lease.attempt_count,
    });
    match service.retry_request(lease, error, delay).await {
        Ok(()) => tracing::warn!(
            batch.id = %lease.batch_id,
            batch.request.id = %lease.request_id,
            retry_after_ms = delay.as_millis(),
            attempt = lease.attempt_count,
            last_error = code,
            "Batch request scheduled for retry"
        ),
        Err(error) => tracing::warn!(
            batch.id = %lease.batch_id,
            batch.request.id = %lease.request_id,
            error.code = error.error_code().unwrap_or("unknown"),
            "Batch request retry scheduling was rejected"
        ),
    }
}

fn retry_delay(
    attempt_count: u32,
    retry_after: Option<Duration>,
    jitter_seed: u64,
    now_ms: i64,
    batch_expires_at_ms: i64,
) -> Option<Duration> {
    if attempt_count >= MAX_REQUEST_ATTEMPTS || now_ms >= batch_expires_at_ms {
        return None;
    }
    let exponent = attempt_count.saturating_sub(1).min(6);
    let cap_ms = Duration::from_secs(1_u64 << exponent)
        .min(MAX_RETRY_DELAY)
        .as_millis() as u64;
    let jitter_ms = jitter_seed % cap_ms.saturating_add(1);
    let retry_after_ms = retry_after
        .map(|delay| delay.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or_default();
    let delay = Duration::from_millis(
        jitter_ms
            .max(retry_after_ms)
            .max(MIN_RETRY_DELAY.as_millis() as u64),
    );
    let delay_ms = i64::try_from(delay.as_millis()).ok()?;
    (now_ms.saturating_add(delay_ms) < batch_expires_at_ms).then_some(delay)
}

fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get("retry-after")?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let deadline = httpdate::parse_http_date(value).ok()?;
    Some(
        deadline
            .duration_since(SystemTime::now())
            .unwrap_or_default(),
    )
}

fn idempotency_recovery_delay(queue_timeout: Duration) -> Duration {
    queue_timeout.saturating_add(IDEMPOTENCY_RECOVERY_GRACE)
}

fn retry_jitter_seed() -> u64 {
    Uuid::new_v4().as_u128() as u64
}

fn batch_idempotency_key(batch_id: &str, request_id: Uuid, request_hash: &str) -> String {
    format!("batch:{batch_id}:{}:{request_hash}", request_id.simple())
}

fn remove_active_task(
    active: &mut HashSet<(String, String, String)>,
    task_keys: &mut HashMap<tokio::task::Id, (String, String, String)>,
    task_id: tokio::task::Id,
) {
    if let Some(key) = task_keys.remove(&task_id) {
        active.remove(&key);
    }
}

fn now_ms() -> i64 {
    use std::time::UNIX_EPOCH;
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_is_bounded_by_attempts_and_batch_deadline() {
        let now = 1_000_000;
        assert!(retry_delay(1, None, 500, now, now + 60_000).is_some());
        assert!(retry_delay(MAX_REQUEST_ATTEMPTS, None, 0, now, now + 60_000).is_none());
        assert!(retry_delay(1, None, 0, now, now + 50).is_none());
    }

    #[test]
    fn retry_after_is_respected_when_it_fits_the_batch_deadline() {
        let now = 1_000_000;
        let delay = retry_delay(2, Some(Duration::from_secs(7)), 0, now, now + 10_000)
            .expect("retry should fit");
        assert_eq!(delay, Duration::from_secs(7));
    }

    #[test]
    fn batch_idempotency_is_stable_per_line_and_distinct_for_duplicate_bodies() {
        let batch_id = "batch-duplicate-body";
        let request_hash = "same-request-hash";
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);

        assert_eq!(
            batch_idempotency_key(batch_id, first, request_hash),
            batch_idempotency_key(batch_id, first, request_hash)
        );
        assert_ne!(
            batch_idempotency_key(batch_id, first, request_hash),
            batch_idempotency_key(batch_id, second, request_hash)
        );
    }

    #[test]
    fn idempotency_recovery_waits_past_the_admission_deadline() {
        assert_eq!(
            idempotency_recovery_delay(Duration::from_secs(120)),
            Duration::from_secs(126)
        );
    }

    #[tokio::test]
    async fn panicked_batch_pass_releases_its_active_key() {
        let key = (
            "tenant".to_string(),
            "project".to_string(),
            "batch".to_string(),
        );
        let mut active = HashSet::from([key.clone()]);
        let mut task_keys = HashMap::new();
        let mut tasks = JoinSet::new();
        let handle = tasks.spawn(async {
            panic!("simulated batch pass failure");
        });
        task_keys.insert(handle.id(), key);

        let error = tasks
            .join_next_with_id()
            .await
            .expect("task should finish")
            .expect_err("task should panic");
        remove_active_task(&mut active, &mut task_keys, error.id());

        assert!(active.is_empty());
        assert!(task_keys.is_empty());
    }
}
