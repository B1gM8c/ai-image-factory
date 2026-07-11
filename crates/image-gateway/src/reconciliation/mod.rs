use async_trait::async_trait;

use crate::ImageGatewayError;

mod postgres;

pub use postgres::PostgresReconciliationStore;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconciliationOutcome {
    pub requeued: u32,
    pub uncertain: u32,
    pub orphaned: u32,
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
}
