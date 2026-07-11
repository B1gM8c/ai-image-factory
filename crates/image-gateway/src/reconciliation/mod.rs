use async_trait::async_trait;
use uuid::Uuid;

use crate::{ImageGatewayError, input_blobs::InputBlobStore};

mod postgres;

pub use postgres::PostgresReconciliationStore;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconciliationOutcome {
    pub requeued: u32,
    pub uncertain: u32,
    pub orphaned: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InputCleanupOutcome {
    pub claimed: u32,
    pub completed: u32,
    pub failed: u32,
}

#[async_trait]
pub trait ReconciliationStore: Send + Sync + 'static {
    async fn reconcile_expired_work(
        &self,
        limit: u32,
    ) -> Result<ReconciliationOutcome, ImageGatewayError>;

    async fn reconcile_orphan_reservations(
        &self,
        grace_ms: u64,
        limit: u32,
    ) -> Result<ReconciliationOutcome, ImageGatewayError>;

    async fn claim_input_cleanup(
        &self,
        owner: &str,
        grace_ms: u64,
        lease_ms: u64,
        limit: u32,
    ) -> Result<Vec<Uuid>, ImageGatewayError>;

    async fn complete_input_cleanup(
        &self,
        owner: &str,
        session_id: Uuid,
    ) -> Result<(), ImageGatewayError>;
}

pub async fn reconcile_input_cleanup(
    store: &dyn ReconciliationStore,
    blobs: &dyn InputBlobStore,
    owner: &str,
    grace_ms: u64,
    lease_ms: u64,
    limit: u32,
) -> Result<InputCleanupOutcome, ImageGatewayError> {
    let sessions = store
        .claim_input_cleanup(owner, grace_ms, lease_ms, limit)
        .await?;
    let mut outcome = InputCleanupOutcome {
        claimed: sessions.len().try_into().unwrap_or(u32::MAX),
        ..InputCleanupOutcome::default()
    };
    for session_id in sessions {
        if blobs.delete_session(session_id).await.is_err() {
            outcome.failed = outcome.failed.saturating_add(1);
            continue;
        }
        if store
            .complete_input_cleanup(owner, session_id)
            .await
            .is_err()
        {
            outcome.failed = outcome.failed.saturating_add(1);
            continue;
        }
        outcome.completed = outcome.completed.saturating_add(1);
    }
    Ok(outcome)
}
