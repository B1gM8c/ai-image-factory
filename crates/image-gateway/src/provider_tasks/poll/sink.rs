use std::sync::Arc;

use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, ArtifactSinkError, ArtifactSinkErrorKind,
    DurableArtifactManifest,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use super::super::{ProviderArtifactAuthority, ProviderTaskLease};

pub struct StagedProviderArtifact {
    manifest: DurableArtifactManifest,
    authority: ProviderArtifactAuthority,
}

impl StagedProviderArtifact {
    pub fn new(
        manifest: DurableArtifactManifest,
        authority: ProviderArtifactAuthority,
    ) -> Result<Self, ProviderArtifactSinkContractError> {
        let mut authority_sha256 = [0_u8; 32];
        if hex::decode_to_slice(&authority.sha256_hex, &mut authority_sha256).is_err()
            || authority.byte_size != manifest.byte_size()
            || authority.media_type != manifest.media_type()
            || authority_sha256 != *manifest.sha256()
        {
            return Err(ProviderArtifactSinkContractError::ManifestMismatch);
        }
        Ok(Self {
            manifest,
            authority,
        })
    }

    pub fn manifest(&self) -> &DurableArtifactManifest {
        &self.manifest
    }

    pub(crate) fn authority(&self) -> &ProviderArtifactAuthority {
        &self.authority
    }
}

pub trait ProviderArtifactStager: Send + 'static {
    fn write_chunk(
        &mut self,
        chunk: &[u8],
    ) -> impl Future<Output = Result<(), ArtifactSinkError>> + Send;

    fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> impl Future<Output = Result<StagedProviderArtifact, ArtifactSinkError>> + Send;
}

pub trait ProviderArtifactStagerFactory: Send + Sync + 'static {
    type Stager: ProviderArtifactStager;

    fn begin(
        &self,
        context: &ProviderArtifactStageContext,
    ) -> impl Future<Output = Result<Self::Stager, ArtifactSinkError>> + Send;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderArtifactStageContext {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    poll_lease_epoch: i64,
}

impl ProviderArtifactStageContext {
    pub(crate) fn from_lease(lease: &ProviderTaskLease) -> Self {
        Self {
            submission_id: lease.task.submission_id,
            executor_execution_id: lease.task.executor_execution_id,
            poll_lease_epoch: lease.poll_lease_epoch,
        }
    }

    pub fn submission_id(&self) -> Uuid {
        self.submission_id
    }

    pub fn executor_execution_id(&self) -> Uuid {
        self.executor_execution_id
    }

    pub fn poll_lease_epoch(&self) -> i64 {
        self.poll_lease_epoch
    }
}

enum SinkState {
    Pristine,
    Streaming,
    Finalized(Box<StagedProviderArtifact>),
    Failed,
}

pub(crate) struct ControlledProviderArtifactSink<'a, F>
where
    F: ProviderArtifactStagerFactory,
{
    factory: &'a F,
    context: ProviderArtifactStageContext,
    stager: Option<F::Stager>,
    state: SinkState,
    materialization_limit: Arc<Semaphore>,
    materialization_permit: Option<OwnedSemaphorePermit>,
}

impl<'a, F> ControlledProviderArtifactSink<'a, F>
where
    F: ProviderArtifactStagerFactory,
{
    pub(crate) fn new(
        factory: &'a F,
        context: ProviderArtifactStageContext,
        materialization_limit: Arc<Semaphore>,
    ) -> Self {
        Self {
            factory,
            context,
            stager: None,
            state: SinkState::Pristine,
            materialization_limit,
            materialization_permit: None,
        }
    }

    pub(crate) fn into_pristine(self) -> Result<(), ProviderArtifactSinkContractError> {
        match self.state {
            SinkState::Pristine => Ok(()),
            SinkState::Streaming | SinkState::Finalized(_) | SinkState::Failed => {
                Err(ProviderArtifactSinkContractError::NotPristine)
            }
        }
    }

    pub(crate) fn into_finalized(
        self,
        expected: &DurableArtifactManifest,
    ) -> Result<StagedProviderArtifact, ProviderArtifactSinkContractError> {
        match self.state {
            SinkState::Finalized(staged) if staged.manifest == *expected => Ok(*staged),
            SinkState::Finalized(_) => Err(ProviderArtifactSinkContractError::ManifestMismatch),
            SinkState::Pristine | SinkState::Streaming | SinkState::Failed => {
                Err(ProviderArtifactSinkContractError::NotFinalized)
            }
        }
    }
}

impl<F> ArtifactSink for ControlledProviderArtifactSink<'_, F>
where
    F: ProviderArtifactStagerFactory,
{
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactSinkError> {
        if chunk.is_empty() || !matches!(self.state, SinkState::Pristine | SinkState::Streaming) {
            return Err(sink_error(
                ArtifactSinkErrorKind::InvalidArtifact,
                "provider_artifact_chunk_invalid",
            ));
        }
        if self.materialization_permit.is_none() {
            self.materialization_permit = Some(
                Arc::clone(&self.materialization_limit)
                    .acquire_owned()
                    .await
                    .map_err(|_| {
                        sink_error(
                            ArtifactSinkErrorKind::Storage,
                            "provider_materialization_limit_closed",
                        )
                    })?,
            );
        }
        if self.stager.is_none() {
            match self.factory.begin(&self.context).await {
                Ok(stager) => self.stager = Some(stager),
                Err(error) => {
                    self.state = SinkState::Failed;
                    return Err(error);
                }
            }
        }
        self.state = SinkState::Streaming;
        let Some(stager) = self.stager.as_mut() else {
            self.state = SinkState::Failed;
            return Err(sink_error(
                ArtifactSinkErrorKind::Storage,
                "provider_artifact_stager_unavailable",
            ));
        };
        if let Err(error) = stager.write_chunk(chunk).await {
            self.state = SinkState::Failed;
            return Err(error);
        }
        Ok(())
    }

    async fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> Result<DurableArtifactManifest, ArtifactSinkError> {
        if !matches!(self.state, SinkState::Streaming) {
            return Err(sink_error(
                ArtifactSinkErrorKind::AlreadyFinalized,
                "provider_artifact_finalize_invalid",
            ));
        }
        let Some(stager) = self.stager.as_mut() else {
            self.state = SinkState::Failed;
            return Err(sink_error(
                ArtifactSinkErrorKind::Storage,
                "provider_artifact_stager_unavailable",
            ));
        };
        match stager.finalize(metadata).await {
            Ok(staged) => {
                let manifest = staged.manifest.clone();
                self.state = SinkState::Finalized(Box::new(staged));
                Ok(manifest)
            }
            Err(error) => {
                self.state = SinkState::Failed;
                Err(error)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderArtifactSinkContractError {
    #[error("provider poll returned without a pristine artifact sink")]
    NotPristine,
    #[error("provider poll completed without exactly one finalized artifact")]
    NotFinalized,
    #[error("provider poll artifact manifest does not match staged bytes")]
    ManifestMismatch,
}

fn sink_error(kind: ArtifactSinkErrorKind, code: &'static str) -> ArtifactSinkError {
    ArtifactSinkError::new(kind, code)
}
