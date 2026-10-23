use std::sync::Arc;

use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, ArtifactSinkError, ArtifactSinkErrorKind,
    DurableArtifactManifest, DurableArtifactRef,
};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use super::{
    ControlledProviderArtifactSink, ProviderArtifactStageContext, ProviderArtifactStagerFactory,
    ProviderNativeBatchArtifactSink, StagedProviderArtifact,
};
use crate::provider_tasks::{NativeBatchMember, ProviderTaskLease};

enum CurrentOutput<'a, F: ProviderArtifactStagerFactory> {
    Staging(ControlledProviderArtifactSink<'a, F>),
    Verifying { hasher: Sha256, byte_size: u64 },
}

pub(super) struct ControlledNativeBatchSink<'a, F: ProviderArtifactStagerFactory> {
    factory: &'a F,
    members: &'a [NativeBatchMember],
    lease: &'a ProviderTaskLease,
    limit: Arc<Semaphore>,
    current: Option<CurrentOutput<'a, F>>,
    completed: usize,
    staged: Vec<(usize, StagedProviderArtifact)>,
    touched: bool,
}

impl<'a, F: ProviderArtifactStagerFactory> ControlledNativeBatchSink<'a, F> {
    pub(super) fn new(
        factory: &'a F,
        members: &'a [NativeBatchMember],
        lease: &'a ProviderTaskLease,
        limit: Arc<Semaphore>,
    ) -> Self {
        Self {
            factory,
            members,
            lease,
            limit,
            current: None,
            completed: 0,
            staged: Vec::new(),
            touched: false,
        }
    }

    pub(super) fn is_pristine(&self) -> bool {
        !self.touched
    }

    pub(super) fn finish(
        self,
        output_count: usize,
    ) -> Result<Vec<(usize, StagedProviderArtifact)>, ArtifactSinkError> {
        if self.current.is_some()
            || self.completed != output_count
            || output_count == 0
            || output_count > self.members.len()
        {
            return Err(batch_contract_error());
        }
        Ok(self.staged)
    }
}

impl<F: ProviderArtifactStagerFactory> ProviderNativeBatchArtifactSink
    for ControlledNativeBatchSink<'_, F>
{
    fn select_output(&mut self, output_index: usize) -> Result<(), ArtifactSinkError> {
        if self.current.is_some() || output_index != self.completed {
            return Err(batch_contract_error());
        }
        let member = self
            .members
            .get(output_index)
            .ok_or_else(batch_contract_error)?;
        if member.committed_failure_code.is_some() {
            return Err(batch_contract_error());
        }
        self.touched = true;
        self.current = Some(if member.committed_artifact.is_some() {
            CurrentOutput::Verifying {
                hasher: Sha256::new(),
                byte_size: 0,
            }
        } else {
            CurrentOutput::Staging(ControlledProviderArtifactSink::new(
                self.factory,
                ProviderArtifactStageContext::for_native_batch_member(self.lease, member),
                Arc::clone(&self.limit),
            ))
        });
        Ok(())
    }
}

impl<F: ProviderArtifactStagerFactory> ArtifactSink for ControlledNativeBatchSink<'_, F> {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactSinkError> {
        match self.current.as_mut().ok_or_else(batch_contract_error)? {
            CurrentOutput::Staging(sink) => sink.write_chunk(chunk).await,
            CurrentOutput::Verifying { hasher, byte_size } => {
                if chunk.is_empty() {
                    return Err(batch_contract_error());
                }
                *byte_size = byte_size
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(batch_contract_error)?;
                hasher.update(chunk);
                Ok(())
            }
        }
    }

    async fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> Result<DurableArtifactManifest, ArtifactSinkError> {
        let current = self.current.take().ok_or_else(batch_contract_error)?;
        let manifest = match current {
            CurrentOutput::Staging(mut sink) => {
                let manifest = sink.finalize(metadata).await?;
                let artifact = sink
                    .into_finalized(&manifest)
                    .map_err(|_| batch_contract_error())?;
                self.staged.push((self.completed, artifact));
                manifest
            }
            CurrentOutput::Verifying { hasher, byte_size } => {
                let member = &self.members[self.completed];
                let manifest = DurableArtifactManifest::new(
                    DurableArtifactRef::new(
                        "native-batch",
                        member.executor_execution_id.simple().to_string(),
                    )
                    .map_err(|_| batch_contract_error())?,
                    metadata.media_type,
                    byte_size,
                    hasher.finalize().into(),
                )
                .map_err(|_| batch_contract_error())?;
                if !member
                    .committed_artifact
                    .as_ref()
                    .is_some_and(|publication| publication.matches_durable_manifest(&manifest))
                {
                    return Err(batch_contract_error());
                }
                manifest
            }
        };
        self.completed += 1;
        Ok(manifest)
    }
}

fn batch_contract_error() -> ArtifactSinkError {
    ArtifactSinkError::new(
        ArtifactSinkErrorKind::InvalidArtifact,
        "provider_native_batch_artifact_contract",
    )
}
