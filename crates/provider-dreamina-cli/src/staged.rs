use crate::RequestValidationError;

/// A server-assigned basename in the execution workspace, never a caller path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalInputName(String);

impl LocalInputName {
    pub fn new(value: impl Into<String>) -> Result<Self, RequestValidationError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 255
            || !value.as_bytes()[0].is_ascii_alphanumeric()
            || !value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
        {
            return Err(RequestValidationError::InvalidStagedFilename);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StagedInputKindV1 {
    Image,
    Video,
    Audio,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedInputRefV1<'a> {
    pub kind: StagedInputKindV1,
    /// Zero-based order within this media category.
    pub index: usize,
    pub filename: &'a str,
    pub sha256: &'a str,
    pub dimensions: Option<(u32, u32)>,
    pub duration_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedImageV1 {
    filename: LocalInputName,
    sha256: String,
    width: u32,
    height: u32,
}

impl StagedImageV1 {
    /// Dimensions and digest must come from probing the frozen input bytes.
    pub fn new(
        filename: impl Into<String>,
        sha256: impl Into<String>,
        width: u32,
        height: u32,
    ) -> Result<Self, RequestValidationError> {
        if width == 0 || height == 0 {
            return Err(RequestValidationError::InvalidStagedImageDimensions);
        }
        Ok(Self {
            filename: LocalInputName::new(filename)?,
            sha256: validate_sha256(sha256.into())?,
            width,
            height,
        })
    }
    pub fn filename(&self) -> &str {
        self.filename.as_str()
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
    pub fn width(&self) -> u32 {
        self.width
    }
    pub fn height(&self) -> u32 {
        self.height
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedTimedMediaV1 {
    filename: LocalInputName,
    sha256: String,
    duration_ms: u64,
}

impl StagedTimedMediaV1 {
    /// Duration and digest must come from probing the frozen input bytes.
    pub fn new(
        filename: impl Into<String>,
        sha256: impl Into<String>,
        duration_ms: u64,
    ) -> Result<Self, RequestValidationError> {
        if duration_ms == 0 {
            return Err(RequestValidationError::InvalidStagedMediaDuration);
        }
        Ok(Self {
            filename: LocalInputName::new(filename)?,
            sha256: validate_sha256(sha256.into())?,
            duration_ms,
        })
    }
    pub fn filename(&self) -> &str {
        self.filename.as_str()
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
    pub fn duration_ms(&self) -> u64 {
        self.duration_ms
    }
}

fn validate_sha256(value: String) -> Result<String, RequestValidationError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(RequestValidationError::InvalidStagedSha256);
    }
    Ok(value)
}

pub(crate) fn validate_unique_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> Result<(), RequestValidationError> {
    let mut seen = std::collections::HashSet::new();
    if names.into_iter().any(|name| !seen.insert(name)) {
        return Err(RequestValidationError::DuplicateStagedFilename);
    }
    Ok(())
}
