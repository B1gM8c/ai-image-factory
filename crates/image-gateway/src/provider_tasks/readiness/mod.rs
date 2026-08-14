use async_trait::async_trait;
use serde::Serialize;
use uuid::Uuid;

use super::ProviderTaskStoreError;

mod postgres;
mod supervisor;

pub use supervisor::{
    ProviderRuntimeShutdown, ProviderRuntimeSupervisor, ProviderRuntimeSupervisorConfig,
    ProviderRuntimeSupervisorError,
};

const MAX_RUNTIME_LEASE_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRuntimeRole {
    Submit,
    Poll,
}

impl ProviderRuntimeRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Submit => "submit",
            Self::Poll => "poll",
        }
    }

    fn parse(value: &str) -> Result<Self, ProviderTaskStoreError> {
        match value {
            "submit" => Ok(Self::Submit),
            "poll" => Ok(Self::Poll),
            _ => Err(ProviderTaskStoreError::Conflict),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRuntimeRegistration {
    pub runtime_id: Uuid,
    pub execution_profile_id: Uuid,
    pub role: ProviderRuntimeRole,
    pub runtime_owner: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRuntimeLeaseState {
    Active,
    Draining,
}

impl ProviderRuntimeLeaseState {
    fn parse(value: &str) -> Result<Self, ProviderTaskStoreError> {
        match value {
            "active" => Ok(Self::Active),
            "draining" => Ok(Self::Draining),
            _ => Err(ProviderTaskStoreError::Conflict),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRuntimeLease {
    pub runtime_id: Uuid,
    pub execution_profile_id: Uuid,
    pub role: ProviderRuntimeRole,
    pub runtime_owner: String,
    pub state: ProviderRuntimeLeaseState,
    pub heartbeat_at_ms: i64,
    pub lease_expires_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderProfileReadinessStatus {
    Configured,
    Active,
    Draining,
    Blocked,
}

impl ProviderProfileReadinessStatus {
    fn parse(value: &str) -> Result<Self, ProviderTaskStoreError> {
        match value {
            "configured" => Ok(Self::Configured),
            "active" => Ok(Self::Active),
            "draining" => Ok(Self::Draining),
            "blocked" => Ok(Self::Blocked),
            _ => Err(ProviderTaskStoreError::Conflict),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProviderProfileReadiness {
    pub execution_profile_id: Uuid,
    pub profile_key: String,
    pub provider_id: String,
    pub status: ProviderProfileReadinessStatus,
    pub active_submitters: i64,
    pub active_pollers: i64,
    pub draining_submitters: i64,
    pub draining_pollers: i64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ProviderProfileReadinessSummary {
    pub configured: i64,
    pub active: i64,
    pub draining: i64,
    pub blocked: i64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ExecutionQueueReadinessSummary {
    pub ready_work_items: i64,
    pub active_work_leases: i64,
    pub oldest_ready_work_age_ms: i64,
    pub stalled_work_profiles: i64,
    pub prepared_executions: i64,
    pub active_executor_leases: i64,
    pub oldest_prepared_execution_age_ms: i64,
    pub stalled_executor_profiles: i64,
    pub ready_reductions: i64,
    pub active_reducer_leases: i64,
    pub oldest_ready_reduction_age_ms: i64,
}

impl ExecutionQueueReadinessSummary {
    pub fn is_stalled(self, stalled_after_ms: i64) -> bool {
        self.stalled_work_profiles > 0
            || self.stalled_executor_profiles > 0
            || (self.ready_reductions > 0
                && self.active_reducer_leases == 0
                && self.oldest_ready_reduction_age_ms >= stalled_after_ms)
    }
}

#[async_trait]
pub trait ProviderProfileReadinessStore: Send + Sync + 'static {
    async fn summarize_profile_readiness(
        &self,
    ) -> Result<ProviderProfileReadinessSummary, ProviderTaskStoreError>;

    async fn summarize_execution_queue_readiness(
        &self,
        _stalled_after_ms: i64,
    ) -> Result<ExecutionQueueReadinessSummary, ProviderTaskStoreError> {
        Ok(ExecutionQueueReadinessSummary::default())
    }
}

#[async_trait]
pub trait ProviderRuntimeReadinessStore: Send + Sync + 'static {
    async fn register_runtime(
        &self,
        registration: &ProviderRuntimeRegistration,
        lease_ms: i64,
    ) -> Result<ProviderRuntimeLease, ProviderTaskStoreError>;

    async fn heartbeat_runtime(
        &self,
        lease: &ProviderRuntimeLease,
        lease_ms: i64,
    ) -> Result<ProviderRuntimeLease, ProviderTaskStoreError>;

    async fn begin_runtime_drain(
        &self,
        lease: &ProviderRuntimeLease,
        lease_ms: i64,
    ) -> Result<ProviderRuntimeLease, ProviderTaskStoreError>;

    async fn withdraw_runtime(
        &self,
        lease: &ProviderRuntimeLease,
    ) -> Result<(), ProviderTaskStoreError>;

    async fn list_profile_readiness(
        &self,
    ) -> Result<Vec<ProviderProfileReadiness>, ProviderTaskStoreError>;
}

fn validate_registration(
    registration: &ProviderRuntimeRegistration,
    lease_ms: i64,
) -> Result<(), ProviderTaskStoreError> {
    if registration.runtime_id.is_nil()
        || registration.execution_profile_id.is_nil()
        || !valid_owner(&registration.runtime_owner)
        || !(1..=MAX_RUNTIME_LEASE_MS).contains(&lease_ms)
    {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    Ok(())
}

fn validate_lease(
    lease: &ProviderRuntimeLease,
    lease_ms: Option<i64>,
) -> Result<(), ProviderTaskStoreError> {
    if lease.runtime_id.is_nil()
        || lease.execution_profile_id.is_nil()
        || !valid_owner(&lease.runtime_owner)
        || lease.heartbeat_at_ms <= 0
        || lease.lease_expires_at_ms <= lease.heartbeat_at_ms
        || lease_ms.is_some_and(|value| !(1..=MAX_RUNTIME_LEASE_MS).contains(&value))
    {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    Ok(())
}

fn valid_owner(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && value.bytes().all(|byte| byte.is_ascii_graphic())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration() -> ProviderRuntimeRegistration {
        ProviderRuntimeRegistration {
            runtime_id: Uuid::new_v4(),
            execution_profile_id: Uuid::new_v4(),
            role: ProviderRuntimeRole::Submit,
            runtime_owner: "provider-submitd-a".to_string(),
        }
    }

    #[test]
    fn profile_scoped_stall_is_not_hidden_by_other_active_leases() {
        let summary = ExecutionQueueReadinessSummary {
            ready_work_items: 2,
            active_work_leases: 1,
            oldest_ready_work_age_ms: 60_000,
            stalled_work_profiles: 1,
            ..ExecutionQueueReadinessSummary::default()
        };

        assert!(summary.is_stalled(60_000));
    }

    #[test]
    fn young_or_consumed_profile_backlog_is_not_stalled() {
        let summary = ExecutionQueueReadinessSummary {
            ready_work_items: 1,
            active_work_leases: 1,
            oldest_ready_work_age_ms: 59_999,
            ..ExecutionQueueReadinessSummary::default()
        };

        assert!(!summary.is_stalled(60_000));
    }

    #[test]
    fn registration_validation_rejects_ambiguous_identity_and_duration() {
        let mut value = registration();
        assert_eq!(validate_registration(&value, 1), Ok(()));
        value.runtime_id = Uuid::nil();
        assert_eq!(
            validate_registration(&value, 1),
            Err(ProviderTaskStoreError::InvalidInput)
        );
        value = registration();
        value.runtime_owner = "owner with spaces".to_string();
        assert_eq!(
            validate_registration(&value, 1),
            Err(ProviderTaskStoreError::InvalidInput)
        );
        assert_eq!(
            validate_registration(&registration(), 0),
            Err(ProviderTaskStoreError::InvalidInput)
        );
    }
}
