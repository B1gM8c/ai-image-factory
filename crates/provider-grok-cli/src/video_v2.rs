use image_api_contracts::xai::{
    XaiVideoAspectRatio, XaiVideoAudioReference, XaiVideoGenerationCommandV2, XaiVideoKeyframe,
    XaiVideoResolution, XaiVideoWorkflow,
};
use image_provider_sdk::{CanonicalCommandPayload, OutputSlot};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{GrokCommandError, RequestValidationError, StagedImageV1, VideoResolution};

pub const GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2: &str = "grok-cli.videos.generate.v2";
pub const VIDEO_ADAPTER_REVISION_V2: &str = "grok-cli-1.0.34.agentic-video.v1";
const STAGED_INPUT_URL_PREFIX: &str = "factory-staged-sha256:";
const MAX_VOICE_ID_BYTES: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VideoAspectRatioV2 {
    R1x1,
    R16x9,
    R9x16,
    R4x3,
    R3x4,
    R3x2,
    R2x3,
}

impl VideoAspectRatioV2 {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::R1x1 => "1:1",
            Self::R16x9 => "16:9",
            Self::R9x16 => "9:16",
            Self::R4x3 => "4:3",
            Self::R3x4 => "3:4",
            Self::R3x2 => "3:2",
            Self::R2x3 => "2:3",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReferenceVideoDurationV2(u8);

impl ReferenceVideoDurationV2 {
    pub fn new(seconds: u8) -> Result<Self, XaiGrokVideoProjectionErrorV2> {
        if (1..=15).contains(&seconds) {
            Ok(Self(seconds))
        } else {
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedDuration)
        }
    }

    pub const fn seconds(self) -> u8 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrokVideoGenerationInputsV2 {
    first_frame: Option<StagedImageV1>,
    last_frame: Option<StagedImageV1>,
    reference_images: Vec<StagedImageV1>,
    keyframes: Vec<KeyframeV2>,
}

impl GrokVideoGenerationInputsV2 {
    fn new(
        first_frame: Option<StagedImageV1>,
        last_frame: Option<StagedImageV1>,
        reference_images: Vec<StagedImageV1>,
        keyframes: Vec<KeyframeV2>,
    ) -> Self {
        Self {
            first_frame,
            last_frame,
            reference_images,
            keyframes,
        }
    }

    pub fn first_frame(&self) -> Option<&StagedImageV1> {
        self.first_frame.as_ref()
    }

    pub fn last_frame(&self) -> Option<&StagedImageV1> {
        self.last_frame.as_ref()
    }

    pub fn reference_images(&self) -> &[StagedImageV1] {
        &self.reference_images
    }

    pub fn ordered(&self) -> impl Iterator<Item = (&'static str, usize, &StagedImageV1)> {
        self.first_frame
            .iter()
            .map(|image| ("first_frame", 0, image))
            .chain(self.last_frame.iter().map(|image| ("last_frame", 0, image)))
            .chain(
                self.reference_images
                    .iter()
                    .enumerate()
                    .map(|(index, image)| ("reference", index, image)),
            )
            .chain(
                self.keyframes
                    .iter()
                    .enumerate()
                    .map(|(index, keyframe)| ("keyframe", index, keyframe.image())),
            )
    }

    fn from_staged(
        command: &XaiVideoGenerationCommandV2,
        staged_images: Vec<StagedImageV1>,
    ) -> Result<Self, XaiGrokVideoProjectionErrorV2> {
        let expected = usize::from(command.image.is_some())
            + usize::from(command.last_frame.is_some())
            + command.reference_images.len()
            + command.keyframes.len();
        if staged_images.len() != expected {
            return Err(XaiGrokVideoProjectionErrorV2::InputManifestMismatch);
        }
        let mut iter = staged_images.into_iter();
        let first_frame = command
            .image
            .as_ref()
            .map(|_| iter.next().expect("count checked"));
        let last_frame = command
            .last_frame
            .as_ref()
            .map(|_| iter.next().expect("count checked"));
        let reference_images = (0..command.reference_images.len())
            .map(|_| iter.next().expect("count checked"))
            .collect();
        let keyframes = command
            .keyframes
            .iter()
            .map(|keyframe| {
                KeyframeV2::new(iter.next().expect("count checked"), keyframe.timestamp_s)
            })
            .collect();
        Ok(Self::new(
            first_frame,
            last_frame,
            reference_images,
            keyframes,
        ))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KeyframeV2 {
    image: StagedImageV1,
    timestamp_s: f64,
}

impl KeyframeV2 {
    fn new(image: StagedImageV1, timestamp_s: f64) -> Self {
        Self { image, timestamp_s }
    }

    pub(crate) fn image(&self) -> &StagedImageV1 {
        &self.image
    }

    pub(crate) const fn timestamp_s(&self) -> f64 {
        self.timestamp_s
    }
}

impl PartialEq for KeyframeV2 {
    fn eq(&self, other: &Self) -> bool {
        self.image == other.image && self.timestamp_s.to_bits() == other.timestamp_s.to_bits()
    }
}

impl Eq for KeyframeV2 {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceAudioV2 {
    original: String,
    normalized: String,
}

impl ReferenceAudioV2 {
    pub fn original(&self) -> &str {
        &self.original
    }

    pub fn normalized(&self) -> &str {
        &self.normalized
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextToVideoRequestV2 {
    prompt: String,
    aspect_ratio: VideoAspectRatioV2,
    duration: u8,
    resolution: VideoResolution,
}

impl TextToVideoRequestV2 {
    pub fn prompt(&self) -> &str {
        &self.prompt
    }
    pub const fn aspect_ratio(&self) -> VideoAspectRatioV2 {
        self.aspect_ratio
    }
    pub const fn duration(&self) -> u8 {
        self.duration
    }
    pub const fn resolution(&self) -> VideoResolution {
        self.resolution
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageToVideoRequestV2 {
    prompt: Option<String>,
    image: StagedImageV1,
    duration: u8,
    resolution: VideoResolution,
}

impl ImageToVideoRequestV2 {
    pub fn prompt(&self) -> Option<&str> {
        self.prompt.as_deref()
    }
    pub fn image(&self) -> &StagedImageV1 {
        &self.image
    }
    pub const fn duration(&self) -> u8 {
        self.duration
    }
    pub const fn resolution(&self) -> VideoResolution {
        self.resolution
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceToVideoRequestV2 {
    prompt: Option<String>,
    first_frame: Option<StagedImageV1>,
    last_frame: Option<StagedImageV1>,
    reference_images: Vec<StagedImageV1>,
    keyframes: Vec<KeyframeV2>,
    voices: Vec<ReferenceAudioV2>,
    normalized_voices: Vec<String>,
    aspect_ratio: VideoAspectRatioV2,
    duration: ReferenceVideoDurationV2,
    resolution: VideoResolution,
}

impl ReferenceToVideoRequestV2 {
    pub fn prompt(&self) -> Option<&str> {
        self.prompt.as_deref()
    }
    pub fn first_frame(&self) -> Option<&StagedImageV1> {
        self.first_frame.as_ref()
    }
    pub fn last_frame(&self) -> Option<&StagedImageV1> {
        self.last_frame.as_ref()
    }
    pub fn reference_images(&self) -> &[StagedImageV1] {
        &self.reference_images
    }
    pub(crate) fn keyframes(&self) -> &[KeyframeV2] {
        &self.keyframes
    }
    pub fn voices(&self) -> &[String] {
        &self.normalized_voices
    }
    pub fn voice_bindings(&self) -> &[ReferenceAudioV2] {
        &self.voices
    }
    pub const fn aspect_ratio(&self) -> VideoAspectRatioV2 {
        self.aspect_ratio
    }
    pub const fn duration(&self) -> ReferenceVideoDurationV2 {
        self.duration
    }
    pub const fn resolution(&self) -> VideoResolution {
        self.resolution
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrokVideoGenerationRequestV2 {
    TextToVideo(TextToVideoRequestV2),
    ImageToVideo(ImageToVideoRequestV2),
    ReferenceToVideo(ReferenceToVideoRequestV2),
}

impl GrokVideoGenerationRequestV2 {
    pub fn as_text(&self) -> Option<&TextToVideoRequestV2> {
        match self {
            Self::TextToVideo(value) => Some(value),
            _ => None,
        }
    }
    pub fn as_image(&self) -> Option<&ImageToVideoRequestV2> {
        match self {
            Self::ImageToVideo(value) => Some(value),
            _ => None,
        }
    }
    pub fn as_reference(&self) -> Option<&ReferenceToVideoRequestV2> {
        match self {
            Self::ReferenceToVideo(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrokVideoGenerationPayloadV2 {
    source_command: XaiVideoGenerationCommandV2,
    source_command_sha256: String,
    inputs: GrokVideoGenerationInputsV2,
    request: GrokVideoGenerationRequestV2,
}

impl GrokVideoGenerationPayloadV2 {
    pub fn preflight(
        command: &XaiVideoGenerationCommandV2,
    ) -> Result<(), XaiGrokVideoProjectionErrorV2> {
        if command.schema_version != 2 || command.operation != "videos.generations" {
            return Err(XaiGrokVideoProjectionErrorV2::InvalidSourceCommand);
        }
        if command.output.is_some() {
            return Err(XaiGrokVideoProjectionErrorV2::UnsupportedOutput);
        }
        if command.storage_options.is_some() {
            return Err(XaiGrokVideoProjectionErrorV2::UnsupportedStorageOptions);
        }
        if !command.generate_audio {
            return Err(XaiGrokVideoProjectionErrorV2::UnsupportedGenerateAudio);
        }
        if matches!(command.resolution, XaiVideoResolution::P1080) {
            return Err(XaiGrokVideoProjectionErrorV2::UnsupportedResolution);
        }
        for image in command
            .image
            .iter()
            .chain(command.last_frame.iter())
            .chain(command.reference_images.iter())
            .chain(command.keyframes.iter().map(|keyframe| &keyframe.image))
        {
            if image.file_id.is_some() {
                return Err(XaiGrokVideoProjectionErrorV2::UnsupportedFileId);
            }
            if image.url.is_none() {
                return Err(XaiGrokVideoProjectionErrorV2::InputManifestMismatch);
            }
        }
        for audio in &command.reference_audios {
            if audio.url.is_some() {
                return Err(XaiGrokVideoProjectionErrorV2::UnsupportedReferenceAudioUrl);
            }
            let voice = audio
                .voice_id
                .as_deref()
                .ok_or(XaiGrokVideoProjectionErrorV2::InvalidVoiceId)?;
            normalize_voice(voice)?;
        }
        if command.reference_audios.len() > 3
            || command.reference_images.len() > 7
            || command.keyframes.len() > 4
            || usize::from(command.image.is_some())
                + usize::from(command.last_frame.is_some())
                + command.reference_images.len()
                + command.keyframes.len()
                > 9
        {
            return Err(XaiGrokVideoProjectionErrorV2::InputCountExceeded);
        }
        let model = command
            .model
            .as_deref()
            .ok_or(XaiGrokVideoProjectionErrorV2::ModelRequired)?;
        if !is_supported_model(model) {
            return Err(XaiGrokVideoProjectionErrorV2::UnsupportedModel);
        }
        match command.workflow() {
            XaiVideoWorkflow::TextToVideo if !matches!(command.duration, 6 | 10) => {
                return Err(XaiGrokVideoProjectionErrorV2::UnsupportedDuration);
            }
            XaiVideoWorkflow::ImageToVideo => {
                if command.aspect_ratio.is_some() {
                    return Err(XaiGrokVideoProjectionErrorV2::UnsupportedAspectRatio);
                }
                if !matches!(command.duration, 6 | 10) {
                    return Err(XaiGrokVideoProjectionErrorV2::UnsupportedDuration);
                }
            }
            XaiVideoWorkflow::ReferenceToVideo if !(1..=15).contains(&command.duration) => {
                return Err(XaiGrokVideoProjectionErrorV2::UnsupportedDuration);
            }
            XaiVideoWorkflow::ReferenceToVideo => {}
            XaiVideoWorkflow::TextToVideo => {}
        }
        Ok(())
    }

    pub fn from_xai_command(
        source_command: XaiVideoGenerationCommandV2,
        staged_images: Vec<StagedImageV1>,
    ) -> Result<Self, XaiGrokVideoProjectionErrorV2> {
        Self::preflight(&source_command)?;
        let inputs = GrokVideoGenerationInputsV2::from_staged(&source_command, staged_images)?;
        validate_staged_bindings(&source_command, &inputs)?;
        let source_command_sha256 = source_command.canonical_sha256_hex();
        let request = project_request(&source_command, &inputs)?;
        let source_command = redact_inputs(source_command, &inputs)?;
        Ok(Self {
            source_command,
            source_command_sha256,
            inputs,
            request,
        })
    }

    pub fn source_command(&self) -> &XaiVideoGenerationCommandV2 {
        &self.source_command
    }
    pub fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }
    pub fn inputs(&self) -> &GrokVideoGenerationInputsV2 {
        &self.inputs
    }
    pub fn request(&self) -> &GrokVideoGenerationRequestV2 {
        &self.request
    }

    pub fn into_request(self) -> GrokVideoGenerationRequestV2 {
        self.request
    }

    pub fn into_canonical_bytes(self, output: OutputSlot) -> Vec<u8> {
        <Self as CanonicalCommandPayload>::into_canonical_bytes(self, output)
    }
}

impl CanonicalCommandPayload for GrokVideoGenerationPayloadV2 {
    const SCHEMA_ID: &'static str = GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2;
    const ADAPTER_REVISION: &'static str = VIDEO_ADAPTER_REVISION_V2;

    fn source_command_sha256(&self) -> &str {
        &self.source_command_sha256
    }

    fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
        let mut canonical = CanonicalVideoGenerationV2::from_payload(self);
        let unsigned = serde_json::to_vec(&canonical).expect("V2 canonical serialization");
        canonical.integrity_sha256 = Some(hex_sha256(&unsigned));
        serde_json::to_vec(&canonical).expect("V2 canonical serialization")
    }
}

pub fn parse_video_generation_payload_v2(
    input: &[u8],
) -> Result<GrokVideoGenerationPayloadV2, GrokCommandError> {
    if input.is_empty() || input.len() > crate::MAX_CANONICAL_COMMAND_BYTES {
        return Err(GrokCommandError::InvalidCanonicalCommand);
    }
    let canonical: CanonicalVideoGenerationV2 =
        serde_json::from_slice(input).map_err(|_| GrokCommandError::InvalidCanonicalCommand)?;
    if canonical.schema_version != 2
        || canonical.operation != "videos.generations"
        || canonical.adapter_revision != VIDEO_ADAPTER_REVISION_V2
        || !valid_sha256(&canonical.source_command_sha256)
    {
        return Err(GrokCommandError::InvalidCanonicalCommand);
    }
    let expected_integrity = canonical.integrity_sha256.clone();
    let mut unsigned = canonical.clone();
    unsigned.integrity_sha256 = None;
    let actual = hex_sha256(
        &serde_json::to_vec(&unsigned).map_err(|_| GrokCommandError::InvalidCanonicalCommand)?,
    );
    if expected_integrity.as_deref() != Some(actual.as_str()) {
        return Err(GrokCommandError::InvalidCanonicalCommand);
    }
    let staged = canonical
        .inputs
        .iter()
        .map(|input| StagedImageV1::new(input.filename.clone(), input.sha256.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| GrokCommandError::InvalidCanonicalCommand)?;
    let canonical_inputs =
        canonical_inputs(&canonical.inputs, &canonical.source_command.keyframes)?;
    validate_redacted_staged_bindings(&canonical.source_command, &canonical_inputs)?;
    let mut payload =
        GrokVideoGenerationPayloadV2::from_xai_command(canonical.source_command.clone(), staged)
            .map_err(|_| GrokCommandError::InvalidCanonicalCommand)?;
    // The canonical source is intentionally redacted to staged digests. Preserve the
    // digest of the original public command captured before redaction.
    payload.source_command_sha256 = canonical.source_command_sha256.clone();
    if payload.request.workflow_name() != canonical.workflow
        || payload.source_command.model.as_deref() != Some(canonical.model.as_str())
        || payload.inputs != canonical_inputs
        || payload.request.controls() != canonical.controls()
        || canonical.generate_audio != payload.source_command.generate_audio
        || canonical.voices != canonical_voices(payload.request.voice_bindings())
        || canonical.keyframes != canonical_keyframes(payload.request.reference_keyframes())
    {
        return Err(GrokCommandError::InvalidCanonicalCommand);
    }
    Ok(payload)
}

pub fn parse_video_generation_command_v2(
    input: &[u8],
) -> Result<GrokVideoGenerationRequestV2, GrokCommandError> {
    parse_video_generation_payload_v2(input).map(|payload| payload.request)
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum XaiGrokVideoProjectionErrorV2 {
    #[error("xAI video source command is invalid")]
    InvalidSourceCommand,
    #[error("xAI video model is required")]
    ModelRequired,
    #[error("xAI video model is not supported by Grok CLI 1.0.34")]
    UnsupportedModel,
    #[error("Grok CLI 1.0.34 requires generated audio")]
    UnsupportedGenerateAudio,
    #[error("output delivery is not supported by Grok CLI 1.0.34")]
    UnsupportedOutput,
    #[error("storage options are not supported by Grok CLI 1.0.34")]
    UnsupportedStorageOptions,
    #[error("reference audio URLs are not supported by Grok CLI 1.0.34")]
    UnsupportedReferenceAudioUrl,
    #[error("xAI file_id inputs are not supported by Grok CLI")]
    UnsupportedFileId,
    #[error("xAI video duration is not supported by this workflow")]
    UnsupportedDuration,
    #[error("xAI video resolution is not supported by Grok CLI")]
    UnsupportedResolution,
    #[error("xAI image-to-video aspect ratio is not supported by Grok CLI")]
    UnsupportedAspectRatio,
    #[error("reference input or voice count exceeds the CLI limit")]
    InputCountExceeded,
    #[error("voice identifier is invalid")]
    InvalidVoiceId,
    #[error("staged input manifest does not match the source command")]
    InputManifestMismatch,
    #[error(transparent)]
    InvalidRequest(#[from] RequestValidationError),
}

fn project_request(
    command: &XaiVideoGenerationCommandV2,
    inputs: &GrokVideoGenerationInputsV2,
) -> Result<GrokVideoGenerationRequestV2, XaiGrokVideoProjectionErrorV2> {
    let resolution = match command.resolution {
        XaiVideoResolution::P480 => VideoResolution::P480,
        XaiVideoResolution::P720 => VideoResolution::P720,
        XaiVideoResolution::P1080 => {
            return Err(XaiGrokVideoProjectionErrorV2::UnsupportedResolution);
        }
    };
    match command.workflow() {
        XaiVideoWorkflow::TextToVideo => {
            if inputs.ordered().next().is_some() {
                return Err(XaiGrokVideoProjectionErrorV2::InputManifestMismatch);
            }
            let prompt = command
                .prompt
                .clone()
                .ok_or(XaiGrokVideoProjectionErrorV2::InvalidSourceCommand)?;
            let aspect_ratio =
                map_ratio(command.aspect_ratio.unwrap_or(XaiVideoAspectRatio::R16x9));
            let duration = image_duration(command.duration)?;
            Ok(GrokVideoGenerationRequestV2::TextToVideo(
                TextToVideoRequestV2 {
                    prompt,
                    aspect_ratio,
                    duration,
                    resolution,
                },
            ))
        }
        XaiVideoWorkflow::ImageToVideo => {
            if command.aspect_ratio.is_some()
                || command.last_frame.is_some()
                || !command.reference_audios.is_empty()
                || !command.reference_images.is_empty()
            {
                return Err(XaiGrokVideoProjectionErrorV2::UnsupportedAspectRatio);
            }
            let image = inputs
                .first_frame
                .clone()
                .ok_or(XaiGrokVideoProjectionErrorV2::InputManifestMismatch)?;
            let duration = image_duration(command.duration)?;
            Ok(GrokVideoGenerationRequestV2::ImageToVideo(
                ImageToVideoRequestV2 {
                    prompt: command.prompt.clone(),
                    image,
                    duration,
                    resolution,
                },
            ))
        }
        XaiVideoWorkflow::ReferenceToVideo => {
            let duration = ReferenceVideoDurationV2::new(command.duration)?;
            let aspect_ratio =
                map_ratio(command.aspect_ratio.unwrap_or(XaiVideoAspectRatio::R16x9));
            let voices = command
                .reference_audios
                .iter()
                .map(normalize_audio)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(GrokVideoGenerationRequestV2::ReferenceToVideo(
                ReferenceToVideoRequestV2 {
                    prompt: command.prompt.clone(),
                    first_frame: inputs.first_frame.clone(),
                    last_frame: inputs.last_frame.clone(),
                    reference_images: inputs.reference_images.clone(),
                    keyframes: inputs.keyframes.clone(),
                    normalized_voices: voices
                        .iter()
                        .map(|voice| voice.normalized.clone())
                        .collect(),
                    voices,
                    aspect_ratio,
                    duration,
                    resolution,
                },
            ))
        }
    }
}

fn image_duration(seconds: u8) -> Result<u8, XaiGrokVideoProjectionErrorV2> {
    match seconds {
        6 | 10 => Ok(seconds),
        _ => Err(XaiGrokVideoProjectionErrorV2::UnsupportedDuration),
    }
}

fn map_ratio(value: XaiVideoAspectRatio) -> VideoAspectRatioV2 {
    match value {
        XaiVideoAspectRatio::R1x1 => VideoAspectRatioV2::R1x1,
        XaiVideoAspectRatio::R16x9 => VideoAspectRatioV2::R16x9,
        XaiVideoAspectRatio::R9x16 => VideoAspectRatioV2::R9x16,
        XaiVideoAspectRatio::R4x3 => VideoAspectRatioV2::R4x3,
        XaiVideoAspectRatio::R3x4 => VideoAspectRatioV2::R3x4,
        XaiVideoAspectRatio::R3x2 => VideoAspectRatioV2::R3x2,
        XaiVideoAspectRatio::R2x3 => VideoAspectRatioV2::R2x3,
    }
}

fn is_supported_model(value: &str) -> bool {
    matches!(
        value,
        "grok-imagine-video-1.5"
            | "grok-imagine-video-1.5-preview"
            | "grok-imagine-video-1.5-2026-05-30"
    )
}

fn normalize_voice(value: &str) -> Result<String, XaiGrokVideoProjectionErrorV2> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed.len() > MAX_VOICE_ID_BYTES
        || trimmed.chars().any(char::is_control)
    {
        return Err(XaiGrokVideoProjectionErrorV2::InvalidVoiceId);
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn normalize_audio(
    audio: &XaiVideoAudioReference,
) -> Result<ReferenceAudioV2, XaiGrokVideoProjectionErrorV2> {
    let original = audio
        .voice_id
        .as_deref()
        .ok_or(XaiGrokVideoProjectionErrorV2::InvalidVoiceId)?;
    Ok(ReferenceAudioV2 {
        original: original.trim().to_owned(),
        normalized: normalize_voice(original)?,
    })
}

fn redact_inputs(
    mut command: XaiVideoGenerationCommandV2,
    inputs: &GrokVideoGenerationInputsV2,
) -> Result<XaiVideoGenerationCommandV2, XaiGrokVideoProjectionErrorV2> {
    let staged: Vec<_> = inputs.ordered().map(|(_, _, image)| image).collect();
    let refs: Vec<_> = command
        .image
        .iter_mut()
        .chain(command.last_frame.iter_mut())
        .chain(command.reference_images.iter_mut())
        .chain(
            command
                .keyframes
                .iter_mut()
                .map(|keyframe| &mut keyframe.image),
        )
        .collect();
    if refs.len() != staged.len() {
        return Err(XaiGrokVideoProjectionErrorV2::InputManifestMismatch);
    }
    for (reference, image) in refs.into_iter().zip(staged) {
        reference.file_id = None;
        reference.url = Some(format!("{STAGED_INPUT_URL_PREFIX}{}", image.sha256()));
    }
    Ok(command)
}

fn validate_staged_bindings(
    command: &XaiVideoGenerationCommandV2,
    inputs: &GrokVideoGenerationInputsV2,
) -> Result<(), XaiGrokVideoProjectionErrorV2> {
    let staged: Vec<_> = inputs.ordered().map(|(_, _, image)| image).collect();
    let references: Vec<_> = command
        .image
        .iter()
        .chain(command.last_frame.iter())
        .chain(command.reference_images.iter())
        .chain(command.keyframes.iter().map(|keyframe| &keyframe.image))
        .collect();
    if references.len() != staged.len() {
        return Err(XaiGrokVideoProjectionErrorV2::InputManifestMismatch);
    }
    for (reference, image) in references.into_iter().zip(staged) {
        if let Some(url) = reference.url.as_deref()
            && let Some(hash) = url.strip_prefix(STAGED_INPUT_URL_PREFIX)
            && hash != image.sha256()
        {
            return Err(XaiGrokVideoProjectionErrorV2::InputManifestMismatch);
        }
    }
    Ok(())
}

fn validate_redacted_staged_bindings(
    command: &XaiVideoGenerationCommandV2,
    inputs: &GrokVideoGenerationInputsV2,
) -> Result<(), GrokCommandError> {
    let staged: Vec<_> = inputs.ordered().map(|(_, _, image)| image).collect();
    let references: Vec<_> = command
        .image
        .iter()
        .chain(command.last_frame.iter())
        .chain(command.reference_images.iter())
        .chain(command.keyframes.iter().map(|keyframe| &keyframe.image))
        .collect();
    if references.len() != staged.len()
        || references.iter().zip(staged).any(|(reference, image)| {
            reference.file_id.is_some()
                || reference.url.as_deref()
                    != Some(format!("{STAGED_INPUT_URL_PREFIX}{}", image.sha256()).as_str())
        })
    {
        return Err(GrokCommandError::InvalidCanonicalCommand);
    }
    Ok(())
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn canonical_voices(voices: &[ReferenceAudioV2]) -> Vec<CanonicalVoiceV2> {
    voices
        .iter()
        .map(|voice| CanonicalVoiceV2 {
            original: voice.original.clone(),
            normalized: voice.normalized.clone(),
        })
        .collect()
}

fn canonical_keyframes(keyframes: &[KeyframeV2]) -> Vec<CanonicalKeyframeV2> {
    keyframes
        .iter()
        .map(|keyframe| CanonicalKeyframeV2 {
            timestamp_s: keyframe.timestamp_s(),
        })
        .collect()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalVideoGenerationV2 {
    schema_version: u16,
    operation: String,
    adapter_revision: String,
    workflow: String,
    source_command: XaiVideoGenerationCommandV2,
    source_command_sha256: String,
    model: String,
    prompt: Option<String>,
    duration: u8,
    resolution: String,
    aspect_ratio: Option<String>,
    generate_audio: bool,
    voices: Vec<CanonicalVoiceV2>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    keyframes: Vec<CanonicalKeyframeV2>,
    inputs: Vec<CanonicalInputV2>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    integrity_sha256: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalVoiceV2 {
    original: String,
    normalized: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct CanonicalKeyframeV2 {
    timestamp_s: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalInputV2 {
    role: String,
    index: usize,
    filename: String,
    sha256: String,
}

impl CanonicalVideoGenerationV2 {
    fn from_payload(payload: GrokVideoGenerationPayloadV2) -> Self {
        let model = payload
            .source_command
            .model
            .clone()
            .expect("preflight model");
        let workflow = payload.request.workflow_name().to_owned();
        let prompt = payload.request.prompt().map(str::to_owned);
        let duration = payload.request.duration();
        let resolution = payload.request.resolution().as_str().to_owned();
        let aspect_ratio = payload.request.aspect_ratio().map(str::to_owned);
        let voices = canonical_voices(payload.request.voice_bindings());
        let keyframes = canonical_keyframes(payload.request.reference_keyframes());
        let inputs = payload
            .inputs
            .ordered()
            .map(|(role, index, image)| CanonicalInputV2 {
                role: role.to_owned(),
                index,
                filename: image.filename().to_owned(),
                sha256: image.sha256().to_owned(),
            })
            .collect();
        Self {
            schema_version: 2,
            operation: "videos.generations".to_owned(),
            adapter_revision: VIDEO_ADAPTER_REVISION_V2.to_owned(),
            workflow,
            source_command: payload.source_command,
            source_command_sha256: payload.source_command_sha256,
            model,
            prompt,
            duration,
            resolution,
            aspect_ratio,
            generate_audio: true,
            voices,
            keyframes,
            inputs,
            integrity_sha256: None,
        }
    }

    fn controls(&self) -> (Option<&str>, u8, &str, Option<&str>, bool) {
        (
            self.aspect_ratio.as_deref(),
            self.duration,
            self.resolution.as_str(),
            self.prompt.as_deref(),
            self.generate_audio,
        )
    }
}

fn canonical_inputs(
    items: &[CanonicalInputV2],
    keyframes: &[XaiVideoKeyframe],
) -> Result<GrokVideoGenerationInputsV2, GrokCommandError> {
    let mut first = None;
    let mut last = None;
    let mut refs = Vec::new();
    let mut canonical_keyframes = Vec::new();
    for item in items {
        let image = StagedImageV1::new(item.filename.clone(), item.sha256.clone())
            .map_err(|_| GrokCommandError::InvalidCanonicalCommand)?;
        match (item.role.as_str(), item.index) {
            ("first_frame", 0) if first.is_none() => first = Some(image),
            ("last_frame", 0) if last.is_none() => last = Some(image),
            ("reference", index) if index == refs.len() => refs.push(image),
            ("keyframe", index) if index == canonical_keyframes.len() => {
                let timestamp_s = keyframes
                    .get(index)
                    .ok_or(GrokCommandError::InvalidCanonicalCommand)?
                    .timestamp_s;
                canonical_keyframes.push(KeyframeV2::new(image, timestamp_s));
            }
            _ => return Err(GrokCommandError::InvalidCanonicalCommand),
        }
    }
    if canonical_keyframes.len() != keyframes.len() {
        return Err(GrokCommandError::InvalidCanonicalCommand);
    }
    Ok(GrokVideoGenerationInputsV2::new(
        first,
        last,
        refs,
        canonical_keyframes,
    ))
}

impl GrokVideoGenerationRequestV2 {
    fn workflow_name(&self) -> &'static str {
        match self {
            Self::TextToVideo(_) => "text_to_video",
            Self::ImageToVideo(_) => "image_to_video",
            Self::ReferenceToVideo(_) => "reference_to_video",
        }
    }
    fn prompt(&self) -> Option<&str> {
        match self {
            Self::TextToVideo(v) => Some(v.prompt()),
            Self::ImageToVideo(v) => v.prompt(),
            Self::ReferenceToVideo(v) => v.prompt(),
        }
    }
    fn duration(&self) -> u8 {
        match self {
            Self::TextToVideo(v) => v.duration(),
            Self::ImageToVideo(v) => v.duration(),
            Self::ReferenceToVideo(v) => v.duration().seconds(),
        }
    }
    fn resolution(&self) -> VideoResolution {
        match self {
            Self::TextToVideo(v) => v.resolution(),
            Self::ImageToVideo(v) => v.resolution(),
            Self::ReferenceToVideo(v) => v.resolution(),
        }
    }
    fn aspect_ratio(&self) -> Option<&'static str> {
        match self {
            Self::TextToVideo(v) => Some(v.aspect_ratio().as_str()),
            Self::ImageToVideo(_) => None,
            Self::ReferenceToVideo(v) => Some(v.aspect_ratio().as_str()),
        }
    }
    fn voice_bindings(&self) -> &[ReferenceAudioV2] {
        match self {
            Self::ReferenceToVideo(v) => &v.voices,
            _ => &[],
        }
    }
    fn reference_keyframes(&self) -> &[KeyframeV2] {
        match self {
            Self::ReferenceToVideo(v) => &v.keyframes,
            _ => &[],
        }
    }
    fn controls(&self) -> (Option<&str>, u8, &str, Option<&str>, bool) {
        (
            self.aspect_ratio(),
            self.duration(),
            self.resolution().as_str(),
            self.prompt(),
            true,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use image_api_contracts::xai::{
        XaiVideoGenerationRequest, XaiVideoImageUrl, XaiVideoKeyframe, XaiVideoOutput,
        XaiVideoStorageOptions,
    };

    const SHA: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn staged(name: &str) -> StagedImageV1 {
        StagedImageV1::new(name, SHA).unwrap()
    }
    fn command() -> XaiVideoGenerationCommandV2 {
        XaiVideoGenerationCommandV2::from_request(XaiVideoGenerationRequest {
            aspect_ratio: Some(XaiVideoAspectRatio::R16x9),
            duration: Some(6),
            generate_audio: Some(true),
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AA==".into()),
            }),
            keyframes: Vec::new(),
            last_frame: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AQ==".into()),
            }),
            model: Some("grok-imagine-video-1.5".into()),
            output: None,
            prompt: None,
            reference_audios: vec![XaiVideoAudioReference {
                url: None,
                voice_id: Some(" Eve ".into()),
            }],
            reference_images: vec![XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,Ag==".into()),
            }],
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: None,
        })
        .unwrap()
    }

    fn text_command() -> XaiVideoGenerationCommandV2 {
        XaiVideoGenerationCommandV2::from_request(XaiVideoGenerationRequest {
            aspect_ratio: Some(XaiVideoAspectRatio::R16x9),
            duration: Some(10),
            generate_audio: Some(true),
            image: None,
            keyframes: Vec::new(),
            last_frame: None,
            model: Some("grok-imagine-video-1.5".into()),
            output: None,
            prompt: Some("moonlit lake".into()),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P720),
            storage_options: None,
            user: None,
        })
        .unwrap()
    }

    fn image_command() -> XaiVideoGenerationCommandV2 {
        XaiVideoGenerationCommandV2::from_request(XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(6),
            generate_audio: Some(true),
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AA==".into()),
            }),
            keyframes: Vec::new(),
            last_frame: None,
            model: Some("grok-imagine-video-1.5".into()),
            output: None,
            prompt: Some("camera pan".into()),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: None,
        })
        .unwrap()
    }

    fn recompute_integrity(value: serde_json::Value) -> Vec<u8> {
        let mut canonical: CanonicalVideoGenerationV2 = serde_json::from_value(value).unwrap();
        canonical.integrity_sha256 = None;
        let digest = super::hex_sha256(&serde_json::to_vec(&canonical).unwrap());
        canonical.integrity_sha256 = Some(digest);
        serde_json::to_vec(&canonical).unwrap()
    }

    #[test]
    fn v2_projects_inputs_in_semantic_order() {
        let payload = GrokVideoGenerationPayloadV2::from_xai_command(
            command(),
            vec![
                staged("first.png"),
                staged("last.png"),
                staged("reference-0.png"),
            ],
        )
        .unwrap();
        let request = payload.request().as_reference().unwrap();
        assert_eq!(request.first_frame().unwrap().filename(), "first.png");
        assert_eq!(request.last_frame().unwrap().filename(), "last.png");
        assert_eq!(request.reference_images()[0].filename(), "reference-0.png");
        assert_eq!(request.voice_bindings()[0].normalized(), "eve");
    }

    #[test]
    fn v2_empty_keyframes_do_not_change_the_canonical_wire_shape() {
        let payload = GrokVideoGenerationPayloadV2::from_xai_command(
            command(),
            vec![
                staged("first.png"),
                staged("last.png"),
                staged("reference-0.png"),
            ],
        )
        .unwrap();
        let canonical: serde_json::Value =
            serde_json::from_slice(&payload.into_canonical_bytes(OutputSlot::new(0, 1).unwrap()))
                .unwrap();

        assert!(canonical.get("keyframes").is_none());
        assert!(canonical["source_command"].get("keyframes").is_none());
    }

    #[test]
    fn v2_canonical_round_trip_and_tamper_check_keyframes() {
        let mut source = command();
        source.keyframes.push(XaiVideoKeyframe {
            image: XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,Aw==".into()),
            },
            timestamp_s: 2.0,
        });
        let payload = GrokVideoGenerationPayloadV2::from_xai_command(
            source,
            vec![
                staged("first.png"),
                staged("last.png"),
                staged("reference-0.png"),
                staged("keyframe-0.png"),
            ],
        )
        .unwrap();
        let canonical = payload
            .clone()
            .into_canonical_bytes(OutputSlot::new(0, 1).unwrap());
        let parsed = parse_video_generation_payload_v2(&canonical).unwrap();
        let request = parsed.request().as_reference().unwrap();
        assert_eq!(request.keyframes()[0].image().filename(), "keyframe-0.png");
        assert_eq!(request.keyframes()[0].timestamp_s(), 2.0);

        let mut tampered: serde_json::Value = serde_json::from_slice(&canonical).unwrap();
        tampered["source_command"]["keyframes"][0]["timestamp_s"] = serde_json::json!(1.0);
        let source_command: XaiVideoGenerationCommandV2 =
            serde_json::from_value(tampered["source_command"].clone()).unwrap();
        tampered["source_command_sha256"] =
            serde_json::json!(source_command.canonical_sha256_hex());
        assert_eq!(
            parse_video_generation_payload_v2(&recompute_integrity(tampered)),
            Err(GrokCommandError::InvalidCanonicalCommand)
        );
    }

    #[test]
    fn v2_duration_domains_are_bounded() {
        assert!(ReferenceVideoDurationV2::new(1).is_ok());
        assert!(ReferenceVideoDurationV2::new(15).is_ok());
        assert!(ReferenceVideoDurationV2::new(0).is_err());
        assert!(ReferenceVideoDurationV2::new(16).is_err());
    }

    #[test]
    fn v2_rejects_silent_and_1080_before_inputs() {
        let mut silent = command();
        silent.generate_audio = false;
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&silent),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedGenerateAudio)
        );
        let mut hd = command();
        hd.resolution = XaiVideoResolution::P1080;
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&hd),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedResolution)
        );
    }

    #[test]
    fn v2_rejects_provider_unsupported_delivery_options_before_inputs() {
        let mut output = command();
        output.output = Some(XaiVideoOutput {
            upload_url: "https://upload.example/video".into(),
        });
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&output),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedOutput)
        );

        let mut storage = command();
        storage.storage_options = Some(XaiVideoStorageOptions {
            expires_after: Some(3_600),
            filename: "video.mp4".into(),
            public_url: None,
        });
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&storage),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedStorageOptions)
        );
    }

    #[test]
    fn v2_preflight_rejects_reference_duration_outside_one_through_fifteen() {
        let mut zero = command();
        zero.duration = 0;
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&zero),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedDuration)
        );
        let mut sixteen = command();
        sixteen.duration = 16;
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&sixteen),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedDuration)
        );
        let mut one = command();
        one.duration = 1;
        assert!(GrokVideoGenerationPayloadV2::preflight(&one).is_ok());
        let mut fifteen = command();
        fifteen.duration = 15;
        assert!(GrokVideoGenerationPayloadV2::preflight(&fifteen).is_ok());
    }

    #[test]
    fn v2_parser_rejects_raw_source_urls_even_with_recomputed_integrity() {
        let payload = GrokVideoGenerationPayloadV2::from_xai_command(
            command(),
            vec![
                staged("first.png"),
                staged("last.png"),
                staged("reference-0.png"),
            ],
        )
        .unwrap();
        let bytes = payload.into_canonical_bytes(OutputSlot::new(0, 1).unwrap());
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["source_command"]["image"]["url"] = serde_json::json!("https://example.test/raw.png");
        let source_command: XaiVideoGenerationCommandV2 =
            serde_json::from_value(value["source_command"].clone()).unwrap();
        value["source_command_sha256"] = serde_json::json!(source_command.canonical_sha256_hex());
        assert_eq!(
            parse_video_generation_payload_v2(&recompute_integrity(value)),
            Err(GrokCommandError::InvalidCanonicalCommand)
        );
    }

    #[test]
    fn v2_accepts_text_and_image_workflows_and_rejects_i2v_boundaries() {
        let text =
            GrokVideoGenerationPayloadV2::from_xai_command(text_command(), Vec::new()).unwrap();
        assert_eq!(text.request().as_text().unwrap().duration(), 10);

        let image = GrokVideoGenerationPayloadV2::from_xai_command(
            image_command(),
            vec![staged("first.png")],
        )
        .unwrap();
        assert_eq!(
            image.request().as_image().unwrap().image().filename(),
            "first.png"
        );

        let mut duration = image_command();
        duration.duration = 8;
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&duration),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedDuration)
        );

        let mut ratio = image_command();
        ratio.aspect_ratio = Some(XaiVideoAspectRatio::R16x9);
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&ratio),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedAspectRatio)
        );

        let mut near_model = image_command();
        near_model.model = Some("grok-imagine-video-1.5x".into());
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&near_model),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedModel)
        );

        let mut file_id = image_command();
        file_id.image.as_mut().unwrap().file_id = Some("file-1".into());
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&file_id),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedFileId)
        );

        let mut audio_url = command();
        audio_url.reference_audios[0].url = Some("https://example.test/audio.wav".into());
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&audio_url),
            Err(XaiGrokVideoProjectionErrorV2::UnsupportedReferenceAudioUrl)
        );
    }

    #[test]
    fn v2_enforces_reference_and_voice_limits_and_supports_audio_only_without_prompt() {
        let mut too_many_refs = command();
        too_many_refs.reference_images = (0..8)
            .map(|index| XaiVideoImageUrl {
                file_id: None,
                url: Some(format!("data:image/png;base64,{index}")),
            })
            .collect();
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&too_many_refs),
            Err(XaiGrokVideoProjectionErrorV2::InputCountExceeded)
        );
        let mut seven_refs = too_many_refs.clone();
        seven_refs.reference_images.truncate(7);
        assert!(GrokVideoGenerationPayloadV2::preflight(&seven_refs).is_ok());

        let mut too_many_voices = command();
        too_many_voices.reference_audios = (0..4)
            .map(|index| XaiVideoAudioReference {
                url: None,
                voice_id: Some(format!("voice-{index}")),
            })
            .collect();
        assert_eq!(
            GrokVideoGenerationPayloadV2::preflight(&too_many_voices),
            Err(XaiGrokVideoProjectionErrorV2::InputCountExceeded)
        );
        let mut three_voices = too_many_voices.clone();
        three_voices.reference_audios.truncate(3);
        assert!(GrokVideoGenerationPayloadV2::preflight(&three_voices).is_ok());

        let mut audio_only = command();
        audio_only.image = None;
        audio_only.last_frame = None;
        audio_only.reference_images.clear();
        audio_only.prompt = None;
        let payload =
            GrokVideoGenerationPayloadV2::from_xai_command(audio_only, Vec::new()).unwrap();
        assert!(
            payload
                .request()
                .as_reference()
                .unwrap()
                .first_frame()
                .is_none()
        );
        assert!(payload.request().as_reference().unwrap().prompt().is_none());
    }

    #[test]
    fn v2_canonical_rejects_role_tampering_and_v1_fixture_is_unchanged() {
        let payload = GrokVideoGenerationPayloadV2::from_xai_command(
            command(),
            vec![
                staged("first.png"),
                staged("last.png"),
                staged("reference-0.png"),
            ],
        )
        .unwrap();
        let bytes = payload.into_canonical_bytes(OutputSlot::new(0, 1).unwrap());
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["inputs"][0]["role"] = serde_json::json!("last_frame");
        assert_eq!(
            parse_video_generation_payload_v2(&serde_json::to_vec(&value).unwrap()),
            Err(GrokCommandError::InvalidCanonicalCommand)
        );

        let fixture = include_bytes!("../tests/fixtures/grok-video-v1-reference.json");
        assert_eq!(
            super::hex_sha256(fixture),
            "e6feac603ac16e3aa2a797de87834a9b0a1d726ccbb58da89f3a8bebcf6aaaa8"
        );
        let parsed = crate::parse_video_generation_payload(fixture).unwrap();
        let crate::GrokVideoGenerationRequestV1::ReferenceToVideo(request) = parsed.request()
        else {
            panic!("expected reference fixture")
        };
        assert_eq!(request.prompt(), "cinematic motion");
        assert_eq!(request.images()[0].filename(), "one.png");
        assert_eq!(request.images()[0].sha256(), SHA);
        assert_eq!(request.images()[1].filename(), "two.png");
        assert_eq!(request.images()[1].sha256(), SHA);
        assert_eq!(request.aspect_ratio(), crate::VideoAspectRatio::R2x3);
        assert_eq!(request.duration().seconds(), 6);
        assert_eq!(request.resolution(), crate::VideoResolution::P480);
    }

    #[test]
    fn v2_canonical_round_trip_preserves_controls_and_voice_integrity() {
        let payload = GrokVideoGenerationPayloadV2::from_xai_command(
            command(),
            vec![
                staged("first.png"),
                staged("last.png"),
                staged("reference-0.png"),
            ],
        )
        .unwrap();
        let bytes = payload
            .clone()
            .into_canonical_bytes(OutputSlot::new(0, 1).unwrap());
        let parsed = parse_video_generation_payload_v2(&bytes).unwrap();
        let request = parsed.request().as_reference().unwrap();
        assert_eq!(request.voices(), &["eve".to_owned()]);
        assert_eq!(request.first_frame().unwrap().filename(), "first.png");
        assert_eq!(request.duration().seconds(), 6);
    }
}
