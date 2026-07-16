use std::future::Future;

use uuid::Uuid;

use crate::provider_tasks::{
    ProviderRemoteTask, ProviderSubmitAcquire, ProviderSubmitIntent, ProviderSubmitRecoveryLease,
    ProviderTaskClaimScope, ProviderTaskStore, ProviderTaskStoreError, RemoteTaskAttach,
    RemoteTaskQuarantinedReceipt, RemoteTaskSubmitFailure, RemoteTaskSubmitReceipt,
    RemoteTaskSubmitReservation,
};

pub trait ProviderSubmitOrchestrationStore: Send + Sync + 'static {
    fn acquire_submit(
        &self,
        request: &RemoteTaskSubmitReservation,
    ) -> impl Future<Output = Result<ProviderSubmitAcquire, ProviderTaskStoreError>> + Send;

    fn record_submit_failure(
        &self,
        request: &RemoteTaskSubmitFailure,
    ) -> impl Future<Output = Result<ProviderSubmitIntent, ProviderTaskStoreError>> + Send;

    fn record_submit_receipt(
        &self,
        request: &RemoteTaskSubmitReceipt,
    ) -> impl Future<Output = Result<ProviderSubmitIntent, ProviderTaskStoreError>> + Send;

    fn quarantine_submit_receipt(
        &self,
        request: &RemoteTaskQuarantinedReceipt,
    ) -> impl Future<Output = Result<ProviderSubmitIntent, ProviderTaskStoreError>> + Send;

    fn attach(
        &self,
        request: &RemoteTaskAttach,
    ) -> impl Future<Output = Result<ProviderRemoteTask, ProviderTaskStoreError>> + Send;

    fn load(
        &self,
        submission_id: Uuid,
    ) -> impl Future<Output = Result<Option<ProviderRemoteTask>, ProviderTaskStoreError>> + Send;
}

impl<S> ProviderSubmitOrchestrationStore for S
where
    S: ProviderTaskStore,
{
    async fn acquire_submit(
        &self,
        request: &RemoteTaskSubmitReservation,
    ) -> Result<ProviderSubmitAcquire, ProviderTaskStoreError> {
        ProviderTaskStore::acquire_submit(self, request).await
    }

    async fn record_submit_failure(
        &self,
        request: &RemoteTaskSubmitFailure,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
        ProviderTaskStore::record_submit_failure(self, request).await
    }

    async fn record_submit_receipt(
        &self,
        request: &RemoteTaskSubmitReceipt,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
        ProviderTaskStore::record_submit_receipt(self, request).await
    }

    async fn quarantine_submit_receipt(
        &self,
        request: &RemoteTaskQuarantinedReceipt,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
        ProviderTaskStore::quarantine_submit_receipt(self, request).await
    }

    async fn attach(
        &self,
        request: &RemoteTaskAttach,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        ProviderTaskStore::attach(self, request).await
    }

    async fn load(
        &self,
        submission_id: Uuid,
    ) -> Result<Option<ProviderRemoteTask>, ProviderTaskStoreError> {
        ProviderTaskStore::load(self, submission_id).await
    }
}

pub trait ProviderSubmitSchedulingStore: Send + Sync + 'static {
    fn resolve_due_submit_deadline(
        &self,
        scope: &ProviderTaskClaimScope,
    ) -> impl Future<Output = Result<Option<ProviderSubmitIntent>, ProviderTaskStoreError>> + Send;

    fn claim_submit_recovery(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        command_id: &str,
        lease_ms: i64,
    ) -> impl Future<Output = Result<Option<ProviderSubmitRecoveryLease>, ProviderTaskStoreError>> + Send;

    fn heartbeat_submit_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
        lease_ms: i64,
    ) -> impl Future<Output = Result<ProviderSubmitRecoveryLease, ProviderTaskStoreError>> + Send;

    fn defer_submit_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
        command_id: &str,
        retry_after_ms: i64,
    ) -> impl Future<Output = Result<(), ProviderTaskStoreError>> + Send;
}

impl<S> ProviderSubmitSchedulingStore for S
where
    S: ProviderTaskStore,
{
    async fn resolve_due_submit_deadline(
        &self,
        scope: &ProviderTaskClaimScope,
    ) -> Result<Option<ProviderSubmitIntent>, ProviderTaskStoreError> {
        ProviderTaskStore::resolve_due_submit_deadline(self, scope).await
    }

    async fn claim_submit_recovery(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        command_id: &str,
        lease_ms: i64,
    ) -> Result<Option<ProviderSubmitRecoveryLease>, ProviderTaskStoreError> {
        ProviderTaskStore::claim_submit_recovery(self, scope, owner, command_id, lease_ms).await
    }

    async fn heartbeat_submit_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
        lease_ms: i64,
    ) -> Result<ProviderSubmitRecoveryLease, ProviderTaskStoreError> {
        ProviderTaskStore::heartbeat_submit_recovery(self, lease, lease_ms).await
    }

    async fn defer_submit_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
        command_id: &str,
        retry_after_ms: i64,
    ) -> Result<(), ProviderTaskStoreError> {
        ProviderTaskStore::defer_submit_recovery(self, lease, command_id, retry_after_ms).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SubmitOnlyStore;

    impl ProviderSubmitOrchestrationStore for SubmitOnlyStore {
        async fn acquire_submit(
            &self,
            _: &RemoteTaskSubmitReservation,
        ) -> Result<ProviderSubmitAcquire, ProviderTaskStoreError> {
            Err(ProviderTaskStoreError::Unavailable)
        }

        async fn record_submit_failure(
            &self,
            _: &RemoteTaskSubmitFailure,
        ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
            Err(ProviderTaskStoreError::Unavailable)
        }

        async fn record_submit_receipt(
            &self,
            _: &RemoteTaskSubmitReceipt,
        ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
            Err(ProviderTaskStoreError::Unavailable)
        }

        async fn quarantine_submit_receipt(
            &self,
            _: &RemoteTaskQuarantinedReceipt,
        ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
            Err(ProviderTaskStoreError::Unavailable)
        }

        async fn attach(
            &self,
            _: &RemoteTaskAttach,
        ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
            Err(ProviderTaskStoreError::Unavailable)
        }

        async fn load(
            &self,
            _: Uuid,
        ) -> Result<Option<ProviderRemoteTask>, ProviderTaskStoreError> {
            Err(ProviderTaskStoreError::Unavailable)
        }
    }

    struct SchedulingOnlyStore;

    impl ProviderSubmitSchedulingStore for SchedulingOnlyStore {
        async fn resolve_due_submit_deadline(
            &self,
            _: &ProviderTaskClaimScope,
        ) -> Result<Option<ProviderSubmitIntent>, ProviderTaskStoreError> {
            Err(ProviderTaskStoreError::Unavailable)
        }

        async fn claim_submit_recovery(
            &self,
            _: &ProviderTaskClaimScope,
            _: &str,
            _: &str,
            _: i64,
        ) -> Result<Option<ProviderSubmitRecoveryLease>, ProviderTaskStoreError> {
            Err(ProviderTaskStoreError::Unavailable)
        }

        async fn heartbeat_submit_recovery(
            &self,
            _: &ProviderSubmitRecoveryLease,
            _: i64,
        ) -> Result<ProviderSubmitRecoveryLease, ProviderTaskStoreError> {
            Err(ProviderTaskStoreError::Unavailable)
        }

        async fn defer_submit_recovery(
            &self,
            _: &ProviderSubmitRecoveryLease,
            _: &str,
            _: i64,
        ) -> Result<(), ProviderTaskStoreError> {
            Err(ProviderTaskStoreError::Unavailable)
        }
    }

    #[test]
    fn submit_ports_do_not_require_the_wide_provider_task_store() {
        fn accepts_orchestration<T: ProviderSubmitOrchestrationStore>() {}
        fn accepts_scheduling<T: ProviderSubmitSchedulingStore>() {}

        accepts_orchestration::<SubmitOnlyStore>();
        accepts_scheduling::<SchedulingOnlyStore>();
    }
}
