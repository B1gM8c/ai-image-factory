use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    artifacts::{
        ArtifactBlobStore, ArtifactMetadata, ArtifactWriteError, ExecutorArtifactReference,
        FilesystemArtifactBlobStore,
    },
};

mod postgres;

pub use postgres::PostgresArtifactRetentionStore;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedExecutorArtifact {
    pub authority_id: Uuid,
    pub storage_backend: String,
    pub storage_namespace: String,
    pub object_key: String,
    pub sha256_hex: String,
    pub byte_size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedArtifactPair {
    pub customer: ArtifactMetadata,
    pub executor: Option<RetainedExecutorArtifact>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactRetentionLease {
    pub job_id: Uuid,
    pub owner: String,
    pub epoch: i64,
    pub artifacts: Vec<RetainedArtifactPair>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactRetentionClaim {
    Lease(ArtifactRetentionLease),
    Deferred,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArtifactRetentionOutcome {
    pub expired: u32,
    pub claimed: u32,
    pub deleted: u32,
    pub failed: u32,
}

#[async_trait]
pub trait ArtifactRetentionStore: Send + Sync + 'static {
    async fn expire_due(&self, limit: u32) -> Result<u32, ImageGatewayError>;

    async fn claim_due(
        &self,
        owner: &str,
        lease_ms: u64,
    ) -> Result<Option<ArtifactRetentionClaim>, ImageGatewayError>;

    async fn complete(&self, lease: &ArtifactRetentionLease) -> Result<(), ImageGatewayError>;

    async fn retry(
        &self,
        lease: &ArtifactRetentionLease,
        error_code: &'static str,
    ) -> Result<(), ImageGatewayError>;
}

pub async fn reconcile_artifact_retention(
    store: &dyn ArtifactRetentionStore,
    blobs: &FilesystemArtifactBlobStore,
    owner: &str,
    lease_ms: u64,
    limit: u32,
) -> Result<ArtifactRetentionOutcome, ImageGatewayError> {
    let mut outcome = ArtifactRetentionOutcome {
        expired: store.expire_due(limit).await?,
        ..ArtifactRetentionOutcome::default()
    };
    for _ in 0..limit {
        let Some(claim) = store.claim_due(owner, lease_ms).await? else {
            break;
        };
        let ArtifactRetentionClaim::Lease(lease) = claim else {
            outcome.failed = outcome.failed.saturating_add(1);
            continue;
        };
        outcome.claimed = outcome.claimed.saturating_add(1);
        if delete_lease(blobs, &lease).await.is_err() {
            store.retry(&lease, "artifact_delete_failed").await?;
            outcome.failed = outcome.failed.saturating_add(1);
            continue;
        }
        store.complete(&lease).await?;
        outcome.deleted = outcome.deleted.saturating_add(1);
    }
    Ok(outcome)
}

async fn delete_lease(
    blobs: &FilesystemArtifactBlobStore,
    lease: &ArtifactRetentionLease,
) -> Result<(), ArtifactWriteError> {
    for pair in &lease.artifacts {
        ArtifactBlobStore::delete(blobs, &pair.customer).await?;
        if let Some(executor) = &pair.executor {
            blobs
                .delete_executor_reference(&ExecutorArtifactReference {
                    authority_id: executor.authority_id,
                    storage_backend: &executor.storage_backend,
                    storage_namespace: &executor.storage_namespace,
                    object_key: &executor.object_key,
                    sha256_hex: &executor.sha256_hex,
                    byte_size: executor.byte_size,
                })
                .await?;
        }
    }
    Ok(())
}
