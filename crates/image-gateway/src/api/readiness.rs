use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

use crate::{
    models::{ProviderProfileReadinessCounts, ReadinessResponse},
    provider_tasks::{
        ProviderProfileReadinessStore, ProviderProfileReadinessSummary, ProviderTaskStoreError,
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
    )
    .await
}

async fn readiness_response(
    store: &dyn ProviderProfileReadinessStore,
    readiness_timeout: Duration,
) -> Response {
    let probe = tokio::time::timeout(readiness_timeout, store.summarize_profile_readiness()).await;
    let (status_code, response) = match probe {
        Ok(Ok(summary)) => (
            StatusCode::OK,
            ReadinessResponse {
                status: "ready",
                provider_profiles: Some(summary.into()),
            },
        ),
        Ok(Err(_)) | Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            ReadinessResponse {
                status: "not_ready",
                provider_profiles: None,
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
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            json_body(response).await,
            json!({"status": "not_ready", "provider_profiles": null})
        );
    }

    #[tokio::test]
    async fn stalled_store_is_bounded_by_the_probe_timeout() {
        let response = readiness_response(&PendingReadinessStore, Duration::from_millis(1)).await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            json_body(response).await,
            json!({"status": "not_ready", "provider_profiles": null})
        );
    }

    async fn json_body(response: Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}
