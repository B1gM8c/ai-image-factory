use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, ArtifactSinkError, ArtifactSinkErrorKind,
    DurableArtifactManifest, DurableArtifactRef,
};

#[derive(Debug)]
pub struct RecordingArtifactSink {
    artifact: DurableArtifactRef,
    sha256: [u8; 32],
    bytes: Vec<u8>,
    chunk_sizes: Vec<usize>,
    finalize_count: usize,
}

impl RecordingArtifactSink {
    pub fn new(artifact: DurableArtifactRef, sha256: [u8; 32]) -> Self {
        Self {
            artifact,
            sha256,
            bytes: Vec::new(),
            chunk_sizes: Vec::new(),
            finalize_count: 0,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn chunk_sizes(&self) -> &[usize] {
        &self.chunk_sizes
    }

    pub fn finalize_count(&self) -> usize {
        self.finalize_count
    }
}

impl ArtifactSink for RecordingArtifactSink {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactSinkError> {
        if self.finalize_count != 0 {
            return Err(ArtifactSinkError::new(
                ArtifactSinkErrorKind::AlreadyFinalized,
                "write_after_finalize",
            ));
        }
        self.chunk_sizes.push(chunk.len());
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }

    async fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> Result<DurableArtifactManifest, ArtifactSinkError> {
        if self.finalize_count != 0 {
            return Err(ArtifactSinkError::new(
                ArtifactSinkErrorKind::AlreadyFinalized,
                "duplicate_finalize",
            ));
        }
        self.finalize_count += 1;
        DurableArtifactManifest::new(
            self.artifact.clone(),
            metadata.media_type,
            self.bytes.len() as u64,
            self.sha256,
        )
        .map_err(|_| {
            ArtifactSinkError::new(ArtifactSinkErrorKind::InvalidArtifact, "invalid_manifest")
        })
    }
}
