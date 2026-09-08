use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ImageGatewayError, input_blobs::InputBlobRef};

pub const SCHEMA_VERSION: &str = "1.0";
pub const PROMPT_REVISION: &str = "bbox-compact-v1";
pub const MAX_ASSET_BYTES: usize = 20 * 1024 * 1024;
pub const ASSET_TTL_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MediaScope {
    pub tenant_id: String,
    pub project_id: String,
    /// Empty for project service credentials; personal credentials retain owner isolation.
    pub owner_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MediaAsset {
    pub id: Uuid,
    pub scope: MediaScope,
    pub digest: String,
    pub image: ImageSize,
    pub blob: InputBlobRef,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ImageSize {
    pub width: u32,
    pub height: u32,
    pub coordinate_system: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetResponse {
    pub object: String,
    pub asset_id: String,
    pub image: ImageSize,
    pub expires_at: i64,
}

impl MediaAsset {
    pub fn response(&self) -> AssetResponse {
        AssetResponse {
            object: "media.asset".into(),
            asset_id: format!("img_{}", self.id.simple()),
            image: self.image.clone(),
            expires_at: self.expires_at_ms / 1000,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentRequest {
    pub asset_id: String,
    #[serde(default = "default_cached_only")]
    pub cached_only: bool,
    #[serde(default = "default_detail")]
    pub detail: String,
    #[serde(default = "default_language")]
    pub language: String,
    #[serde(default = "default_mask_format")]
    pub mask_format: String,
}

fn default_cached_only() -> bool {
    true
}
fn default_detail() -> String {
    "bbox".into()
}
fn default_language() -> String {
    "zh-CN".into()
}
fn default_mask_format() -> String {
    "none".into()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SegmentStatus {
    Processing,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentError {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Segmentation {
    pub object: String,
    pub id: String,
    pub asset_id: String,
    pub status: SegmentStatus,
    pub schema_version: String,
    pub image: ImageSize,
    pub groups: Vec<SegmentGroup>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<SegmentError>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentGroup {
    pub group_id: String,
    pub parent_group_id: Option<String>,
    #[serde(rename = "名称")]
    pub name: String,
    pub mask_key: String,
    #[serde(rename = "类别")]
    pub category: String,
    pub ui_rank: usize,
    pub bbox_xyxy: [u32; 4],
    pub confidence: f64,
    pub segments: Vec<SegmentItem>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentItem {
    pub segment_id: String,
    #[serde(rename = "名称")]
    pub name: String,
    pub mask_key: String,
    pub bbox_xyxy: [u32; 4],
    pub confidence: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BboxCandidate {
    pub groups: Vec<CandidateGroup>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateGroup {
    pub parent_index: Option<usize>,
    #[serde(rename = "名称")]
    pub name: String,
    #[serde(rename = "类别")]
    pub category: String,
    pub bbox_xyxy: [u32; 4],
    pub confidence: f64,
    pub segments: Vec<CandidateItem>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateItem {
    #[serde(rename = "名称")]
    pub name: String,
    pub bbox_xyxy: [u32; 4],
    pub confidence: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnalyzerConfig {
    pub provider: String,
    pub model: String,
    pub reasoning_effort: String,
    pub revision: String,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            provider: "openai_codex".into(),
            model: "gpt-5.6-luna".into(),
            reasoning_effort: "none".into(),
            revision: PROMPT_REVISION.into(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SegmentTimings {
    pub cli_start_ms: u64,
    pub bbox_ms: u64,
    pub validation_ms: u64,
    // Known only after the terminal database write; emitted in the worker log.
    #[serde(skip)]
    pub storage_ms: u64,
    pub retries: u32,
}

#[derive(Debug)]
pub struct Analysis {
    pub candidate: BboxCandidate,
    pub timings: SegmentTimings,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct SidecarCapabilities {
    pub supports_bbox_sidecar: bool,
    pub supports_mask_sidecar: bool,
}

#[async_trait]
pub trait BboxAnalyzer: Send + Sync {
    fn capabilities(&self) -> SidecarCapabilities {
        SidecarCapabilities {
            supports_bbox_sidecar: true,
            supports_mask_sidecar: false,
        }
    }
    async fn analyze(&self, bytes: &[u8], image: &ImageSize)
    -> Result<Analysis, ImageGatewayError>;
}

#[derive(Clone, Debug)]
pub struct SegmentWork {
    pub id: Uuid,
    pub lease_token: Uuid,
    pub asset: MediaAsset,
    pub result: Segmentation,
}

#[async_trait]
pub trait SegmentStore: Send + Sync {
    async fn find_asset(
        &self,
        scope: &MediaScope,
        digest: &str,
    ) -> Result<Option<MediaAsset>, ImageGatewayError>;
    /// Returns the winner of the per-scope digest uniqueness constraint.
    async fn insert_asset(&self, asset: &MediaAsset) -> Result<MediaAsset, ImageGatewayError>;
    async fn get_asset(
        &self,
        scope: &MediaScope,
        id: Uuid,
    ) -> Result<Option<MediaAsset>, ImageGatewayError>;
    async fn cached(
        &self,
        scope: &MediaScope,
        cache_key: &str,
    ) -> Result<Option<Segmentation>, ImageGatewayError>;
    async fn get(
        &self,
        scope: &MediaScope,
        id: Uuid,
    ) -> Result<Option<Segmentation>, ImageGatewayError>;
    async fn enqueue(
        &self,
        asset: &MediaAsset,
        cache_key: &str,
        analyzer_key: &str,
    ) -> Result<Segmentation, ImageGatewayError>;
    async fn claim(
        &self,
        analyzer_key: &str,
        lease_ms: i64,
    ) -> Result<Option<SegmentWork>, ImageGatewayError>;
    /// Fenced terminal write. False means this worker lost its lease.
    async fn finish(
        &self,
        work: &SegmentWork,
        result: &Segmentation,
        timings: &SegmentTimings,
    ) -> Result<bool, ImageGatewayError>;
    /// Fail expired active leases without rerunning a possibly charged model call.
    async fn expire_leases(&self) -> Result<u64, ImageGatewayError>;
    async fn expired_assets(&self, limit: i64) -> Result<Vec<MediaAsset>, ImageGatewayError>;
    async fn delete_expired_asset(&self, id: Uuid) -> Result<(), ImageGatewayError>;
}
