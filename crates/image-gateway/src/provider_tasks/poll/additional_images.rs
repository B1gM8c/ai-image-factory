use std::sync::Arc;

use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, ArtifactSinkError, ArtifactSinkErrorKind,
    DurableArtifactManifest,
};
use tokio::sync::Semaphore;

use super::{
    ControlledProviderArtifactSink, ProviderAdditionalImageArtifactSink,
    ProviderArtifactStageContext, ProviderArtifactStagerFactory, StagedProviderArtifact,
};
use crate::provider_tasks::ProviderTaskLease;

/// Holds physical results independently of the one paid executor output. Each
/// image releases its materialization permit before the next image is selected.
pub(super) struct ControlledAdditionalImageSink<'a, F: ProviderArtifactStagerFactory> {
    factory: &'a F,
    lease: &'a ProviderTaskLease,
    limit: Arc<Semaphore>,
    current: Option<ControlledProviderArtifactSink<'a, F>>,
    staged: Vec<StagedProviderArtifact>,
    touched: bool,
    total_byte_size: u64,
}

impl<'a, F: ProviderArtifactStagerFactory> ControlledAdditionalImageSink<'a, F> {
    pub(super) fn new(factory: &'a F, lease: &'a ProviderTaskLease, limit: Arc<Semaphore>) -> Self {
        Self {
            factory,
            lease,
            limit,
            current: None,
            staged: Vec::new(),
            touched: false,
            total_byte_size: 0,
        }
    }

    pub(super) fn is_pristine(&self) -> bool {
        !self.touched
    }

    pub(super) fn finish(
        self,
        primary: &DurableArtifactManifest,
    ) -> Result<Vec<StagedProviderArtifact>, ArtifactSinkError> {
        if self.current.is_some()
            || self.staged.is_empty()
            || self.staged.len() > 10
            || self.staged[0].manifest() != primary
        {
            return Err(contract_error());
        }
        Ok(self.staged)
    }
}

impl<F: ProviderArtifactStagerFactory> ProviderAdditionalImageArtifactSink
    for ControlledAdditionalImageSink<'_, F>
{
    fn select_image(&mut self, source_index: usize) -> Result<(), ArtifactSinkError> {
        if self.current.is_some() || source_index != self.staged.len() || source_index >= 10 {
            return Err(contract_error());
        }
        let context = if source_index == 0 {
            ProviderArtifactStageContext::from_lease(self.lease)
        } else {
            ProviderArtifactStageContext::for_additional_image(self.lease, source_index)
                .ok_or_else(contract_error)?
        };
        self.touched = true;
        self.current = Some(ControlledProviderArtifactSink::new(
            self.factory,
            context,
            Arc::clone(&self.limit),
        ));
        Ok(())
    }
}

impl<F: ProviderArtifactStagerFactory> ArtifactSink for ControlledAdditionalImageSink<'_, F> {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactSinkError> {
        self.total_byte_size = self
            .total_byte_size
            .checked_add(chunk.len() as u64)
            .filter(|size| *size <= crate::artifacts::MAX_ARTIFACT_BYTES)
            .ok_or_else(contract_error)?;
        // Ordinary single-output drivers need not know about the opt-in channel.
        if self.current.is_none() && self.staged.is_empty() {
            self.select_image(0)?;
        }
        self.current
            .as_mut()
            .ok_or_else(contract_error)?
            .write_chunk(chunk)
            .await
    }

    async fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> Result<DurableArtifactManifest, ArtifactSinkError> {
        if !matches!(
            metadata.media_type,
            "image/png" | "image/jpeg" | "image/webp"
        ) {
            return Err(contract_error());
        }
        let mut current = self.current.take().ok_or_else(contract_error)?;
        let manifest = current.finalize(metadata).await?;
        let staged = current
            .into_finalized(&manifest)
            .map_err(|_| contract_error())?;
        self.staged.push(staged);
        Ok(manifest)
    }
}

fn contract_error() -> ArtifactSinkError {
    ArtifactSinkError::new(
        ArtifactSinkErrorKind::InvalidArtifact,
        "provider_additional_image_contract",
    )
}

#[cfg(test)]
mod tests {
    use super::super::tests::{TestStagerFactory, lease};
    use super::*;

    #[tokio::test]
    async fn total_budget_is_shared_across_images_and_rejected_before_a_chunk_is_staged() {
        let factory = TestStagerFactory::default();
        let lease = lease(None);
        let mut sink =
            ControlledAdditionalImageSink::new(&factory, &lease, Arc::new(Semaphore::new(1)));
        sink.write_chunk(b"primary").await.unwrap();
        let primary = sink
            .finalize(ArtifactMetadata {
                media_type: "image/png",
            })
            .await
            .unwrap();
        // Set the accumulated budget near its boundary without allocating 256MiB.
        sink.total_byte_size = crate::artifacts::MAX_ARTIFACT_BYTES - 2;
        sink.select_image(1).unwrap();
        sink.write_chunk(b"xy").await.unwrap();
        assert!(sink.write_chunk(b"z").await.is_err());
        assert_eq!(sink.total_byte_size, crate::artifacts::MAX_ARTIFACT_BYTES);
        assert!(
            sink.finish(&primary).is_err(),
            "an over-budget unfinished sibling cannot become deliverable"
        );
    }
}
