use std::{error::Error, fmt};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DurableArtifactRef {
    namespace: String,
    artifact_id: String,
}

impl DurableArtifactRef {
    pub fn new(
        namespace: impl Into<String>,
        artifact_id: impl Into<String>,
    ) -> Result<Self, DurableArtifactRefError> {
        let namespace = namespace.into();
        let artifact_id = artifact_id.into();
        validate_component("namespace", &namespace)?;
        validate_component("artifact_id", &artifact_id)?;

        Ok(Self {
            namespace,
            artifact_id,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn artifact_id(&self) -> &str {
        &self.artifact_id
    }
}

fn validate_component(field: &'static str, value: &str) -> Result<(), DurableArtifactRefError> {
    if value.is_empty() {
        return Err(DurableArtifactRefError::Empty(field));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(DurableArtifactRefError::InvalidCharacter(field));
    }
    if value.len() > 255 {
        return Err(DurableArtifactRefError::TooLong(field));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableArtifactRefError {
    Empty(&'static str),
    InvalidCharacter(&'static str),
    TooLong(&'static str),
}

impl fmt::Display for DurableArtifactRefError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty(field) => write!(formatter, "{field} must not be empty"),
            Self::InvalidCharacter(field) => {
                write!(formatter, "{field} is not a durable opaque identifier")
            }
            Self::TooLong(field) => write!(formatter, "{field} exceeds 255 bytes"),
        }
    }
}

impl Error for DurableArtifactRefError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableArtifactManifest {
    artifact: DurableArtifactRef,
    media_type: String,
    byte_size: u64,
    sha256: [u8; 32],
}

impl DurableArtifactManifest {
    pub fn new(
        artifact: DurableArtifactRef,
        media_type: impl Into<String>,
        byte_size: u64,
        sha256: [u8; 32],
    ) -> Result<Self, DurableArtifactManifestError> {
        let media_type = media_type.into();
        if byte_size == 0 {
            return Err(DurableArtifactManifestError::Empty);
        }
        if media_type.len() > 128
            || !matches!(media_type.split_once('/'), Some(("image" | "video", subtype)) if !subtype.is_empty())
            || !media_type.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(DurableArtifactManifestError::InvalidMediaType);
        }
        Ok(Self {
            artifact,
            media_type,
            byte_size,
            sha256,
        })
    }

    pub fn artifact(&self) -> &DurableArtifactRef {
        &self.artifact
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub fn byte_size(&self) -> u64 {
        self.byte_size
    }

    pub fn sha256(&self) -> &[u8; 32] {
        &self.sha256
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableArtifactManifestError {
    Empty,
    InvalidMediaType,
}

impl fmt::Display for DurableArtifactManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("durable artifact must not be empty"),
            Self::InvalidMediaType => formatter.write_str("durable artifact media type is invalid"),
        }
    }
}

impl Error for DurableArtifactManifestError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactMetadata<'a> {
    pub media_type: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactSinkErrorKind {
    AlreadyFinalized,
    InvalidArtifact,
    Storage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactSinkError {
    kind: ArtifactSinkErrorKind,
    code: String,
}

impl ArtifactSinkError {
    pub fn new(kind: ArtifactSinkErrorKind, code: impl Into<String>) -> Self {
        Self {
            kind,
            code: code.into(),
        }
    }

    pub fn kind(&self) -> ArtifactSinkErrorKind {
        self.kind
    }

    pub fn code(&self) -> &str {
        &self.code
    }
}

impl fmt::Display for ArtifactSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "artifact sink error: {}", self.code)
    }
}

impl Error for ArtifactSinkError {}

pub trait ArtifactSink: Send {
    fn write_chunk(
        &mut self,
        chunk: &[u8],
    ) -> impl std::future::Future<Output = Result<(), ArtifactSinkError>> + Send;

    fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> impl std::future::Future<Output = Result<DurableArtifactManifest, ArtifactSinkError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_refs_reject_urls_and_local_paths() {
        assert!(DurableArtifactRef::new("artifacts", "output-1").is_ok());
        assert!(DurableArtifactRef::new("artifacts", "https://example.test/file").is_err());
        assert!(DurableArtifactRef::new("artifacts", "/tmp/output.png").is_err());
        assert!(DurableArtifactRef::new("artifacts", r"C:\\output.png").is_err());
    }

    #[test]
    fn manifests_require_nonempty_image_or_video_media() {
        let artifact = DurableArtifactRef::new("artifacts", "output-1").unwrap();
        assert!(DurableArtifactManifest::new(artifact.clone(), "image/png", 1, [0; 32]).is_ok());
        assert!(DurableArtifactManifest::new(artifact.clone(), "video/mp4", 1, [0; 32]).is_ok());
        assert!(DurableArtifactManifest::new(artifact.clone(), "text/plain", 1, [0; 32]).is_err());
        assert!(DurableArtifactManifest::new(artifact, "image/png", 0, [0; 32]).is_err());
    }
}
