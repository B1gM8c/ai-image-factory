//! Version 2 binds an original query-only execution to a separately recorded
//! HTTP retrieval. It cannot reinterpret a CLI download or a four-image receipt.
use std::fs::File;

use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::{
    DreaminaImageSuccessResolutionRequest, conflict,
    evidence::{MAX_IMAGE_BYTES, MAX_RECEIPT_BYTES, VerifiedEvidence, read},
};
use crate::{ImageGatewayError, artifacts::sha256_hex};

pub(super) const MODE: &str = "query_only_http_retrieval_v2";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryAudit {
    schema_version: u16,
    evidence_mode: String,
    job_id: Uuid,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    remote_operation_id: String,
    argv: Vec<String>,
    stdout_sha256: String,
    exit_code: i32,
    executed_at_ms: i64,
    downloaded_images: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetrievalAudit {
    schema_version: u16,
    evidence_mode: String,
    job_id: Uuid,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    remote_operation_id: String,
    source_index: u32,
    item_id: String,
    image_url_sha256: String,
    method: String,
    http_status: u16,
    redirect_count: u16,
    retrieved_at_ms: i64,
    media_type: String,
    sha256: String,
    byte_size: u64,
    width: u32,
    height: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Review {
    schema_version: u16,
    evidence_mode: String,
    stdout_sha256: String,
    execution_audit_sha256: String,
    retrieval_audit_sha256: String,
    image_url_sha256: String,
    selected_sha256: String,
    selected_source_index: u32,
    selected_item_id: String,
    quote_hash: String,
    provider_command_sha256: String,
    tenant_id: String,
    auth_attribution: Value,
}

pub(super) fn load(
    directory: &File,
    request: &DreaminaImageSuccessResolutionRequest,
) -> Result<VerifiedEvidence, ImageGatewayError> {
    let stdout = read(directory, "reviewed-query.json", MAX_RECEIPT_BYTES)?;
    let audit_bytes = read(directory, "execution-audit.json", MAX_RECEIPT_BYTES)?;
    let retrieval_bytes = read(directory, "retrieval-audit.json", MAX_RECEIPT_BYTES)?;
    let review_bytes = read(directory, "review.json", MAX_RECEIPT_BYTES)?;
    let bytes = read(directory, "selected.png", MAX_IMAGE_BYTES)?;
    let review: Review = serde_json::from_slice(&review_bytes).map_err(|_| conflict())?;
    let audit: QueryAudit = serde_json::from_slice(&audit_bytes).map_err(|_| conflict())?;
    let retrieval: RetrievalAudit =
        serde_json::from_slice(&retrieval_bytes).map_err(|_| conflict())?;
    let stdout_sha256 = sha256_hex(&stdout);
    let selected_sha256 = sha256_hex(&bytes);
    let execution_audit_sha256 = sha256_hex(&audit_bytes);
    let retrieval_audit_sha256 = sha256_hex(&retrieval_bytes);
    if review.schema_version != 2
        || audit.schema_version != 2
        || retrieval.schema_version != 2
        || review.evidence_mode != MODE
        || audit.evidence_mode != MODE
        || retrieval.evidence_mode != MODE
        || request.reviewed_stdout_sha256 != stdout_sha256
        || review.stdout_sha256 != stdout_sha256
        || audit.stdout_sha256 != stdout_sha256
        || review.execution_audit_sha256 != execution_audit_sha256
        || review.retrieval_audit_sha256 != retrieval_audit_sha256
        || review.selected_sha256 != selected_sha256
        || retrieval.sha256 != selected_sha256
        || retrieval.byte_size != bytes.len() as u64
        || review.selected_source_index != 0
        || retrieval.source_index != 0
        || review.selected_item_id != retrieval.item_id
        || audit.job_id != request.job_id
        || retrieval.job_id != request.job_id
        || audit.submission_id != request.submission_id
        || retrieval.submission_id != request.submission_id
        || audit.executor_execution_id != request.executor_execution_id
        || retrieval.executor_execution_id != request.executor_execution_id
        || retrieval.remote_operation_id != audit.remote_operation_id
        || audit.argv != ["query_result", "--submit_id", &audit.remote_operation_id]
        || !audit.downloaded_images.is_empty()
        || audit.exit_code != 0
        || audit.executed_at_ms <= 0
        || retrieval.retrieved_at_ms < audit.executed_at_ms
        || retrieval.method != "GET"
        || retrieval.http_status != 200
        || retrieval.redirect_count != 0
        || retrieval.media_type != "image/png"
        || !super::valid_sha256(&review.quote_hash)
        || !super::valid_sha256(&review.provider_command_sha256)
        || review.tenant_id.trim().is_empty()
        || !review.auth_attribution.is_object()
    {
        return Err(conflict());
    }
    let receipt: Value = serde_json::from_slice(&stdout).map_err(|_| conflict())?;
    if receipt.get("submit_id").and_then(Value::as_str) != Some(audit.remote_operation_id.as_str())
        || receipt.get("gen_status").and_then(Value::as_str) != Some("success")
    {
        return Err(conflict());
    }
    // This audited incident used the object result shape. Do not silently accept
    // a replacement serialization or widen the historical four-image path.
    let result = receipt
        .get("result_json")
        .filter(|v| v.is_object())
        .ok_or_else(conflict)?;
    let images = result
        .get("images")
        .and_then(Value::as_array)
        .ok_or_else(conflict)?;
    if images.len() != 1
        || result
            .get("videos")
            .and_then(Value::as_array)
            .is_none_or(|v| !v.is_empty())
    {
        return Err(conflict());
    }
    let selected = &images[0];
    let item = selected
        .get("item_id")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty() && v.len() <= 255 && !v.chars().any(char::is_control))
        .ok_or_else(conflict)?;
    let url = selected
        .get("image_url")
        .and_then(Value::as_str)
        .ok_or_else(conflict)?;
    let parsed_url = reqwest::Url::parse(url).map_err(|_| conflict())?;
    if item != review.selected_item_id
        || selected.get("path").is_some()
        || parsed_url.scheme() != "https"
        || parsed_url.host_str().is_none()
        || !parsed_url.username().is_empty()
        || parsed_url.password().is_some()
        || parsed_url.fragment().is_some()
        || url.chars().any(char::is_control)
        || sha256_hex(url.as_bytes()) != review.image_url_sha256
        || review.image_url_sha256 != retrieval.image_url_sha256
    {
        return Err(conflict());
    }
    let width = selected
        .get("width")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(conflict)?;
    let height = selected
        .get("height")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(conflict)?;
    if width == 0
        || height == 0
        || width > 8192
        || height > 8192
        || u64::from(width) * u64::from(height) > 16 * 1024 * 1024
        || retrieval.width != width
        || retrieval.height != height
        || image::guess_format(&bytes).ok() != Some(image::ImageFormat::Png)
        || image::ImageReader::with_format(std::io::Cursor::new(&bytes), image::ImageFormat::Png)
            .into_dimensions()
            .ok()
            != Some((width, height))
    {
        return Err(conflict());
    }
    let decoded = image::load_from_memory_with_format(&bytes, image::ImageFormat::Png)
        .map_err(|_| conflict())?;
    if decoded.width() != width || decoded.height() != height {
        return Err(conflict());
    }
    Ok(VerifiedEvidence {
        mode: MODE,
        upstream_result_count: 1,
        retrieval_audit_sha256: Some(retrieval_audit_sha256),
        image_url_sha256: Some(review.image_url_sha256),
        tenant_id: Some(review.tenant_id),
        auth_attribution: Some(review.auth_attribution),
        stdout_sha256,
        execution_audit_sha256,
        review_sha256: sha256_hex(&review_bytes),
        selected_sha256,
        selected_item_id: review.selected_item_id,
        remote_operation_id: audit.remote_operation_id,
        quote_hash: review.quote_hash,
        provider_command_sha256: review.provider_command_sha256,
        width,
        height,
        bytes,
    })
}
