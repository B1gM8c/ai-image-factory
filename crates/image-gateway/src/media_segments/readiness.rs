use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{MediaSegmentsService, analyzer_key, now_ms};

pub const WORKER_HEARTBEAT_TTL_MS: i64 = 150_000;

#[derive(Clone, Debug, Deserialize, sqlx::FromRow)]
pub struct SegmentWorkerHeartbeat {
    pub observed_at_ms: i64,
    pub release_terminal_sources: bool,
    pub configuration_mismatch: bool,
}

#[derive(Debug, Serialize)]
pub struct SegmentReadiness {
    pub object: &'static str,
    pub status: &'static str,
    pub analyzer_key: String,
    pub last_heartbeat_at_ms: Option<i64>,
    pub heartbeat_age_ms: Option<i64>,
    pub heartbeat_ttl_ms: i64,
    pub source_release_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
}

impl MediaSegmentsService {
    /// Independent readiness: a stopped sidecar never changes image readiness.
    pub async fn readiness(&self) -> SegmentReadiness {
        let key = analyzer_key(&self.config);
        match tokio::time::timeout(Duration::from_secs(2), self.store.worker_heartbeat(&key)).await
        {
            Ok(Ok(heartbeat)) => project_readiness(key, heartbeat, now_ms()),
            _ => {
                let mut view = project_readiness(key, None, now_ms());
                view.reason = Some("worker_heartbeat_unavailable");
                view
            }
        }
    }
}

fn project_readiness(
    analyzer_key: String,
    heartbeat: Option<SegmentWorkerHeartbeat>,
    now: i64,
) -> SegmentReadiness {
    let last = heartbeat.as_ref().map(|heartbeat| heartbeat.observed_at_ms);
    let age = last.map(|last| now.saturating_sub(last));
    let mixed = heartbeat
        .as_ref()
        .is_some_and(|heartbeat| heartbeat.configuration_mismatch);
    let fresh = !mixed && age.is_some_and(|age| (0..=WORKER_HEARTBEAT_TTL_MS).contains(&age));
    SegmentReadiness {
        object: "media.readiness",
        status: if fresh { "ready" } else { "not_ready" },
        analyzer_key,
        last_heartbeat_at_ms: last,
        heartbeat_age_ms: age,
        heartbeat_ttl_ms: WORKER_HEARTBEAT_TTL_MS,
        source_release_enabled: heartbeat
            .and_then(|heartbeat| (!mixed).then_some(heartbeat.release_terminal_sources)),
        reason: if mixed {
            Some("worker_configuration_mismatch")
        } else if fresh {
            None
        } else if last.is_none() {
            Some("worker_heartbeat_missing")
        } else {
            Some("worker_heartbeat_stale")
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_requires_fresh_matching_worker_not_configuration() {
        let absent = project_readiness("config-a".into(), None, 500_000);
        assert_eq!(absent.status, "not_ready");
        assert_eq!(absent.reason, Some("worker_heartbeat_missing"));
        for (observed, expected) in [
            (500_000, "ready"),
            (350_000, "ready"),
            (349_999, "not_ready"),
            (500_001, "not_ready"),
        ] {
            let view = project_readiness(
                "config-a".into(),
                Some(SegmentWorkerHeartbeat {
                    observed_at_ms: observed,
                    release_terminal_sources: true,
                    configuration_mismatch: false,
                }),
                500_000,
            );
            assert_eq!(view.status, expected);
            assert_eq!(view.source_release_enabled, Some(true));
            assert_eq!(view.analyzer_key, "config-a");
            assert_eq!(view.last_heartbeat_at_ms, Some(observed));
        }
    }

    #[test]
    fn mixed_worker_modes_cannot_pass_a_rollout_or_rollback() {
        let view = project_readiness(
            "config-a".into(),
            Some(SegmentWorkerHeartbeat {
                observed_at_ms: 500_000,
                release_terminal_sources: false,
                configuration_mismatch: true,
            }),
            500_001,
        );
        assert_eq!(view.status, "not_ready");
        assert_eq!(view.reason, Some("worker_configuration_mismatch"));
        assert_eq!(view.source_release_enabled, None);
    }
}
