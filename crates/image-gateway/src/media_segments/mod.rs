//! Optional, durable bbox analysis. Image generation never calls this module.
mod codex;
mod postgres;
mod types;
mod validation;

pub use codex::CodexBboxAnalyzer;
pub use postgres::PostgresSegmentStore;
pub use types::*;
pub use validation::validate_candidate;

use crate::{
    ImageGatewayError,
    input_blobs::{InputBlobKey, InputBlobStore},
};
use image::{DynamicImage, ImageDecoder, ImageFormat, ImageReader, Limits, metadata::Orientation};
use sha2::{Digest, Sha256};
use std::{
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub fn analyzer_key(config: &AnalyzerConfig) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(config).expect("analyzer config is serializable"),
    ))
}

fn cache_key(asset: &MediaAsset, config: &AnalyzerConfig, request: &SegmentRequest) -> String {
    let material = (
        &asset.digest,
        config,
        SCHEMA_VERSION,
        &request.language,
        &request.detail,
        &request.mask_format,
    );
    hex::encode(Sha256::digest(
        serde_json::to_vec(&material).expect("cache key is serializable"),
    ))
}

fn parse_id(value: &str, prefix: &str) -> Result<Uuid, ImageGatewayError> {
    value
        .strip_prefix(prefix)
        .filter(|id| id.len() == 32)
        .and_then(|id| Uuid::parse_str(id).ok())
        .ok_or_else(|| {
            ImageGatewayError::invalid_request("Invalid media identifier", None, "invalid_media_id")
        })
}

pub fn analyzer_config_from_env() -> Result<AnalyzerConfig, ImageGatewayError> {
    let mut config = AnalyzerConfig::default();
    if let Ok(model) = std::env::var("GATEWAY_BBOX_MODEL") {
        config.model = model;
    }
    if let Ok(effort) = std::env::var("GATEWAY_BBOX_REASONING_EFFORT") {
        config.reasoning_effort = effort;
    }
    if config.model.is_empty()
        || config.model.len() > 128
        || !config
            .model
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-._".contains(&c))
        || !matches!(
            config.reasoning_effort.as_str(),
            "none" | "low" | "medium" | "high"
        )
    {
        return Err(ImageGatewayError::config(
            "Invalid bbox analyzer model or reasoning effort",
        ));
    }
    Ok(config)
}

pub struct MediaSegmentsService {
    store: Arc<dyn SegmentStore>,
    blobs: Arc<dyn InputBlobStore>,
    config: AnalyzerConfig,
    uploads: tokio::sync::Semaphore,
}

/// A fail-fast admission slot held across multipart buffering, decoding, and storage.
pub struct AssetRegistrationPermit<'a> {
    _permit: tokio::sync::SemaphorePermit<'a>,
}

fn acquire_registration_slot(
    uploads: &tokio::sync::Semaphore,
) -> Result<AssetRegistrationPermit<'_>, ImageGatewayError> {
    uploads
        .try_acquire()
        .map(|permit| AssetRegistrationPermit { _permit: permit })
        .map_err(|_| ImageGatewayError::service_unavailable("Media asset registration is busy"))
}

impl MediaSegmentsService {
    pub fn new(
        store: Arc<dyn SegmentStore>,
        blobs: Arc<dyn InputBlobStore>,
        config: AnalyzerConfig,
    ) -> Self {
        Self {
            store,
            blobs,
            config,
            uploads: tokio::sync::Semaphore::new(2),
        }
    }

    pub fn capabilities(&self) -> SidecarCapabilities {
        SidecarCapabilities {
            supports_bbox_sidecar: true,
            supports_mask_sidecar: false,
        }
    }

    pub fn try_acquire_registration(
        &self,
    ) -> Result<AssetRegistrationPermit<'_>, ImageGatewayError> {
        acquire_registration_slot(&self.uploads)
    }

    pub async fn register_asset(
        &self,
        scope: &MediaScope,
        bytes: Vec<u8>,
    ) -> Result<AssetResponse, ImageGatewayError> {
        let permit = self.try_acquire_registration()?;
        self.register_asset_with_permit(scope, bytes, permit).await
    }

    pub async fn register_asset_with_permit(
        &self,
        scope: &MediaScope,
        bytes: Vec<u8>,
        permit: AssetRegistrationPermit<'_>,
    ) -> Result<AssetResponse, ImageGatewayError> {
        let result = self.register_asset_inner(scope, bytes).await;
        drop(permit);
        result
    }

    async fn register_asset_inner(
        &self,
        scope: &MediaScope,
        bytes: Vec<u8>,
    ) -> Result<AssetResponse, ImageGatewayError> {
        if bytes.is_empty() || bytes.len() > MAX_ASSET_BYTES {
            return Err(ImageGatewayError::payload_too_large(
                "image must contain 1 byte to 20 MiB",
            ));
        }
        // Duplicate registrations hash the bytes but skip the full pixel decode and file write.
        let (bytes, digest) = tokio::task::spawn_blocking(move || {
            let digest = hex::encode(Sha256::digest(&bytes));
            (bytes, digest)
        })
        .await
        .map_err(|_| ImageGatewayError::internal("Image hash task failed"))?;
        if let Some(asset) = self.store.find_asset(scope, &digest).await? {
            return Ok(asset.response());
        }
        let (bytes, image) = tokio::task::spawn_blocking(move || decode_asset(bytes))
            .await
            .map_err(|_| ImageGatewayError::internal("Image validation task failed"))??;
        let id = Uuid::new_v4();
        let blob = self
            .blobs
            .put(
                InputBlobKey {
                    admission_session_id: id,
                    input_id: id,
                },
                &bytes,
            )
            .await
            .map_err(|_| {
                ImageGatewayError::service_unavailable("Media asset storage unavailable")
            })?;
        let asset = MediaAsset {
            id,
            scope: scope.clone(),
            digest,
            image,
            blob,
            expires_at_ms: now_ms() + ASSET_TTL_MS,
        };
        match self.store.insert_asset(&asset).await {
            Ok(winner) => {
                if winner.id != id {
                    let _ = self.blobs.delete_session(id).await;
                }
                Ok(winner.response())
            }
            Err(error) => {
                // A lost COMMIT acknowledgement must never delete a committed asset's pixels.
                match self.store.get_asset(scope, id).await {
                    Ok(Some(committed)) => return Ok(committed.response()),
                    Ok(None) => {
                        let _ = self.blobs.delete_session(id).await;
                    }
                    Err(_) => {} // Leave the blob intact until database authority is available.
                }
                Err(error)
            }
        }
    }

    pub async fn request(
        &self,
        scope: &MediaScope,
        request: SegmentRequest,
    ) -> Result<Segmentation, ImageGatewayError> {
        if request.language != "zh-CN" || request.detail != "bbox" || request.mask_format != "none"
        {
            return Err(ImageGatewayError::invalid_request(
                "Only zh-CN, bbox and mask_format=none are supported",
                None,
                "unsupported_segmentation_options",
            ));
        }
        let id = parse_id(&request.asset_id, "img_")?;
        let asset = self.store.get_asset(scope, id).await?.ok_or_else(|| {
            ImageGatewayError::not_found(
                "Media asset was not found or expired",
                Some("asset_id".into()),
                "asset_not_found",
            )
        })?;
        let key = cache_key(&asset, &self.config, &request);
        if let Some(result) = self.store.cached(scope, &key).await? {
            return Ok(result);
        }
        if request.cached_only {
            return Err(ImageGatewayError::not_found(
                "Segmentation is not cached",
                Some("asset_id".into()),
                "segments_not_cached",
            ));
        }
        self.store
            .enqueue(&asset, &key, &analyzer_key(&self.config))
            .await
    }

    pub async fn get(
        &self,
        scope: &MediaScope,
        id: &str,
    ) -> Result<Segmentation, ImageGatewayError> {
        let id = parse_id(id, "seg_")?;
        self.store.get(scope, id).await?.ok_or_else(|| {
            ImageGatewayError::not_found(
                "Segmentation was not found",
                None,
                "segmentation_not_found",
            )
        })
    }
}

fn decode_asset(mut bytes: Vec<u8>) -> Result<(Vec<u8>, ImageSize), ImageGatewayError> {
    let bad = || {
        ImageGatewayError::invalid_request(
            "image must be a valid PNG, JPEG or WebP within 8192px and 16 megapixels",
            Some("image".into()),
            "invalid_image",
        )
    };
    let format = image::guess_format(&bytes).map_err(|_| bad())?;
    if !matches!(
        format,
        ImageFormat::Png | ImageFormat::Jpeg | ImageFormat::WebP
    ) {
        return Err(bad());
    }
    let dims = ImageReader::with_format(Cursor::new(&bytes), format)
        .into_dimensions()
        .map_err(|_| bad())?;
    if dims.0 == 0 || dims.1 == 0 || !crate::core::image_bytes::dimensions_within_input_budget(dims)
    {
        return Err(bad());
    }
    let mut reader = ImageReader::with_format(Cursor::new(&bytes), format);
    let mut limits = Limits::default();
    limits.max_image_width = Some(8192);
    limits.max_image_height = Some(8192);
    limits.max_alloc = Some(128 * 1024 * 1024);
    reader.limits(limits);
    let mut decoder = reader.into_decoder().map_err(|_| bad())?;
    let orientation = decoder.orientation().map_err(|_| bad())?;
    let mut decoded = DynamicImage::from_decoder(decoder).map_err(|_| bad())?;
    if orientation != Orientation::NoTransforms {
        // Browsers display EXIF-oriented pixels; give the analyzer those same pixels.
        decoded.apply_orientation(orientation);
        let mut output = Cursor::new(Vec::new());
        decoded
            .write_to(&mut output, ImageFormat::Png)
            .map_err(|_| bad())?;
        bytes = output.into_inner();
        if bytes.len() > MAX_ASSET_BYTES {
            return Err(bad());
        }
    }
    let size = ImageSize {
        width: decoded.width(),
        height: decoded.height(),
        coordinate_system: "pixel_xyxy".into(),
    };
    Ok((bytes, size))
}

pub struct SegmentWorker {
    store: Arc<dyn SegmentStore>,
    blobs: Arc<dyn InputBlobStore>,
    analyzer: Arc<dyn BboxAnalyzer>,
    analyzer_key: String,
}

impl SegmentWorker {
    pub fn new(
        store: Arc<dyn SegmentStore>,
        blobs: Arc<dyn InputBlobStore>,
        analyzer: Arc<dyn BboxAnalyzer>,
        config: &AnalyzerConfig,
    ) -> Self {
        Self {
            store,
            blobs,
            analyzer,
            analyzer_key: analyzer_key(config),
        }
    }

    pub async fn run_once(&self) -> Result<bool, ImageGatewayError> {
        let Some(work) = self.store.claim(&self.analyzer_key, 120_000).await? else {
            return Ok(false);
        };
        let mut result = work.result.clone();
        let mut timings = SegmentTimings::default();
        let analysis_started = Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(95), async {
            let bytes = self.blobs.get(&work.asset.blob).await.map_err(|_| {
                ImageGatewayError::service_unavailable("Media asset could not be loaded")
            })?;
            let analysis = self.analyzer.analyze(&bytes, &work.asset.image).await?;
            timings = analysis.timings;
            let started = Instant::now();
            validate_candidate(analysis.candidate, &mut result)?;
            timings.validation_ms = started.elapsed().as_millis() as u64;
            Ok::<_, ImageGatewayError>(())
        })
        .await;
        let analysis_ms = analysis_started.elapsed().as_millis() as u64;
        let failure = match outcome {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(if error.error_code() == Some("bbox_invalid") {
                "bbox_invalid"
            } else if error.error_code() == Some("timeout") {
                "bbox_timeout"
            } else {
                "bbox_failed"
            }),
            Err(_) => Some("bbox_timeout"),
        };
        if let Some(code) = failure {
            result.status = SegmentStatus::Failed;
            result.groups.clear();
            result.error = Some(SegmentError {
                code: code.into(),
                message: "图片分析暂未完成，请稍后再试".into(),
            });
        }
        let started = Instant::now();
        let published = self.store.finish(&work, &result, &timings).await?;
        timings.storage_ms = started.elapsed().as_millis() as u64;
        tracing::info!(stage="bbox_sidecar", status=?result.status, published, analysis_ms, cli_start_ms=timings.cli_start_ms, bbox_ms=timings.bbox_ms, validation_ms=timings.validation_ms, storage_ms=timings.storage_ms, retries=timings.retries, "media segmentation finished");
        Ok(true)
    }

    pub async fn maintain(&self) -> Result<(), ImageGatewayError> {
        self.store.expire_leases().await?;
        for asset in self.store.expired_assets(32).await? {
            self.blobs.delete_session(asset.id).await.map_err(|_| {
                ImageGatewayError::service_unavailable("Media asset cleanup failed")
            })?;
            self.store.delete_expired_asset(asset.id).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
