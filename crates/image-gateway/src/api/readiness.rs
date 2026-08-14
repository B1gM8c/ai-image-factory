use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

use crate::{
    models::{ExecutionQueueReadinessCounts, ProviderProfileReadinessCounts, ReadinessResponse},
    provider_tasks::{
        ExecutionQueueReadinessSummary, ProviderProfileReadinessStore,
        ProviderProfileReadinessSummary, ProviderTaskStoreError,
    },
};

use super::AppState;

pub(super) struct EmptyProviderProfileReadinessStore;

#[async_trait]
impl ProviderProfileReadinessStore for EmptyProviderProfileReadinessStore {
    async fn summarize_profile_readiness(
        &self,
    ) -> Result<ProviderProfileReadinessSummary, ProviderTaskStoreError> {
        Ok(ProviderProfileReadinessSummary::default())
    }
}

pub(super) async fn readyz(State(state): State<Arc<AppState>>) -> Response {
    readiness_response(
        state.provider_readiness_store.as_ref(),
        state.config.readiness_timeout,
        state.config.readiness_stall_threshold,
    )
    .await
}

async fn readiness_response(
    store: &dyn ProviderProfileReadinessStore,
    readiness_timeout: Duration,
    readiness_stall_threshold: Duration,
) -> Response {
    let stalled_after_ms = readiness_stall_threshold.as_millis().min(i64::MAX as u128) as i64;
    let probe = tokio::time::timeout(readiness_timeout, async {
        tokio::try_join!(
            store.summarize_profile_readiness(),
            store.summarize_execution_queue_readiness(stalled_after_ms),
        )
    })
    .await;
    let (status_code, response) = match probe {
        Ok(Ok((profile_summary, queue_summary))) => {
            let stalled = queue_summary.is_stalled(stalled_after_ms);
            if stalled {
                tracing::warn!(
                    ready_work_items = queue_summary.ready_work_items,
                    active_work_leases = queue_summary.active_work_leases,
                    oldest_ready_work_age_ms = queue_summary.oldest_ready_work_age_ms,
                    stalled_work_profiles = queue_summary.stalled_work_profiles,
                    prepared_executions = queue_summary.prepared_executions,
                    active_executor_leases = queue_summary.active_executor_leases,
                    oldest_prepared_execution_age_ms =
                        queue_summary.oldest_prepared_execution_age_ms,
                    stalled_executor_profiles = queue_summary.stalled_executor_profiles,
                    ready_reductions = queue_summary.ready_reductions,
                    active_reducer_leases = queue_summary.active_reducer_leases,
                    oldest_ready_reduction_age_ms = queue_summary.oldest_ready_reduction_age_ms,
                    stalled_after_ms,
                    "execution queue has aged work without an active consumer lease"
                );
            }
            (
                if stalled {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::OK
                },
                ReadinessResponse {
                    status: if stalled { "not_ready" } else { "ready" },
                    provider_profiles: Some(profile_summary.into()),
                    execution_queue: Some(queue_summary.into()),
                },
            )
        }
        Ok(Err(_)) | Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            ReadinessResponse {
                status: "not_ready",
                provider_profiles: None,
                execution_queue: None,
            },
        ),
    };

    (
        status_code,
        [(header::CACHE_CONTROL, "no-store")],
        Json(response),
    )
        .into_response()
}

impl From<ExecutionQueueReadinessSummary> for ExecutionQueueReadinessCounts {
    fn from(summary: ExecutionQueueReadinessSummary) -> Self {
        Self {
            ready_work_items: summary.ready_work_items,
            active_work_leases: summary.active_work_leases,
            oldest_ready_work_age_ms: summary.oldest_ready_work_age_ms,
            stalled_work_profiles: summary.stalled_work_profiles,
            prepared_executions: summary.prepared_executions,
            active_executor_leases: summary.active_executor_leases,
            oldest_prepared_execution_age_ms: summary.oldest_prepared_execution_age_ms,
            stalled_executor_profiles: summary.stalled_executor_profiles,
            ready_reductions: summary.ready_reductions,
            active_reducer_leases: summary.active_reducer_leases,
            oldest_ready_reduction_age_ms: summary.oldest_ready_reduction_age_ms,
        }
    }
}

impl From<ProviderProfileReadinessSummary> for ProviderProfileReadinessCounts {
    fn from(summary: ProviderProfileReadinessSummary) -> Self {
        Self {
            configured: summary.configured,
            active: summary.active,
            draining: summary.draining,
            blocked: summary.blocked,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use axum::body::to_bytes;
    use serde_json::{Value, json};

    use super::*;

    struct FixedReadinessStore {
        result: Result<ProviderProfileReadinessSummary, ProviderTaskStoreError>,
    }

    #[async_trait]
    impl ProviderProfileReadinessStore for FixedReadinessStore {
        async fn summarize_profile_readiness(
            &self,
        ) -> Result<ProviderProfileReadinessSummary, ProviderTaskStoreError> {
            self.result
        }
    }

    struct PendingReadinessStore;

    #[async_trait]
    impl ProviderProfileReadinessStore for PendingReadinessStore {
        async fn summarize_profile_readiness(
            &self,
        ) -> Result<ProviderProfileReadinessSummary, ProviderTaskStoreError> {
            pending().await
        }
    }

    #[tokio::test]
    async fn ready_probe_returns_only_bounded_aggregate_counts() {
        let response = readiness_response(
            &FixedReadinessStore {
                result: Ok(ProviderProfileReadinessSummary {
                    configured: 1,
                    active: 2,
                    draining: 3,
                    blocked: 4,
                }),
            },
            Duration::from_secs(1),
            Duration::from_secs(60),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(
            json_body(response).await,
            json!({
                "status": "ready",
                "provider_profiles": {
                    "configured": 1,
                    "active": 2,
                    "draining": 3,
                    "blocked": 4
                },
                "execution_queue": {
                    "ready_work_items": 0,
                    "active_work_leases": 0,
                    "oldest_ready_work_age_ms": 0,
                    "stalled_work_profiles": 0,
                    "prepared_executions": 0,
                    "active_executor_leases": 0,
                    "oldest_prepared_execution_age_ms": 0,
                    "stalled_executor_profiles": 0,
                    "ready_reductions": 0,
                    "active_reducer_leases": 0,
                    "oldest_ready_reduction_age_ms": 0
                }
            })
        );
    }

    #[tokio::test]
    async fn store_failure_is_not_ready_without_internal_details() {
        let response = readiness_response(
            &FixedReadinessStore {
                result: Err(ProviderTaskStoreError::Unavailable),
            },
            Duration::from_secs(1),
            Duration::from_secs(60),
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            json_body(response).await,
            json!({
                "status": "not_ready",
                "provider_profiles": null,
                "execution_queue": null
            })
        );
    }

    struct StalledQueueReadinessStore;

    #[async_trait]
    impl ProviderProfileReadinessStore for StalledQueueReadinessStore {
        async fn summarize_profile_readiness(
            &self,
        ) -> Result<ProviderProfileReadinessSummary, ProviderTaskStoreError> {
            Ok(ProviderProfileReadinessSummary::default())
        }

        async fn summarize_execution_queue_readiness(
            &self,
            _stalled_after_ms: i64,
        ) -> Result<ExecutionQueueReadinessSummary, ProviderTaskStoreError> {
            Ok(ExecutionQueueReadinessSummary {
                ready_work_items: 1,
                active_work_leases: 1,
                oldest_ready_work_age_ms: 60_000,
                stalled_work_profiles: 1,
                ..ExecutionQueueReadinessSummary::default()
            })
        }
    }

    #[tokio::test]
    async fn aged_ready_work_without_a_consumer_lease_is_not_ready() {
        let response = readiness_response(
            &StalledQueueReadinessStore,
            Duration::from_secs(1),
            Duration::from_secs(60),
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = json_body(response).await;
        assert_eq!(body["status"], "not_ready");
        assert_eq!(body["execution_queue"]["ready_work_items"], 1);
        assert_eq!(body["execution_queue"]["active_work_leases"], 1);
        assert_eq!(body["execution_queue"]["stalled_work_profiles"], 1);
    }

    #[tokio::test]
    async fn stalled_store_is_bounded_by_the_probe_timeout() {
        let response = readiness_response(
            &PendingReadinessStore,
            Duration::from_millis(1),
            Duration::from_secs(60),
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            json_body(response).await,
            json!({
                "status": "not_ready",
                "provider_profiles": null,
                "execution_queue": null
            })
        );
    }

    async fn json_body(response: Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}
