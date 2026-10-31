//! Read-only evidence validation. A supplement receipt is never an original receipt.
use super::{DreaminaImageSupplementPublishRequest, conflict};
use crate::{ImageGatewayError, artifacts::sha256_hex};
use rustix::fs::{Mode, OFlags, open, openat};
use serde::Deserialize;
use serde_json::Value;
use std::{fs::File, io::Read, os::unix::fs::MetadataExt, path::Path};
use uuid::Uuid;
const MAX_JSON: u64 = 1024 * 1024;
const MAX_IMAGE: u64 = 32 * 1024 * 1024;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Audit {
    schema_version: u16,
    authorization_id: Uuid,
    dispatch_nonce: Uuid,
    job_id: Uuid,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    remote_operation_id: String,
    argv: Vec<String>,
    stdout_sha256: String,
    exit_code: i32,
    executed_at_ms: i64,
    downloaded_images: Vec<Download>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Download {
    source_index: usize,
    filename: String,
    evidence_filename: String,
    sha256: String,
    byte_size: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Review {
    schema_version: u16,
    authority: String,
    stdout_sha256: String,
    execution_audit_sha256: String,
    quote_hash: String,
    provider_command_sha256: String,
    submit_stdout_sha256: String,
    authorization_id: Uuid,
    dispatch_nonce: Uuid,
}
pub(super) struct Image {
    pub bytes: Vec<u8>,
    pub sha256: String,
    pub width: u32,
    pub height: u32,
}
pub(super) struct VerifiedEvidence {
    pub job_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub submit_stdout_sha256: String,
    pub stdout_sha256: String,
    pub execution_audit_sha256: String,
    pub review_sha256: String,
    pub remote_operation_id: String,
    pub quote_hash: String,
    pub provider_command_sha256: String,
    pub images: Vec<Image>,
}
pub(super) fn load(
    root: &Path,
    request: &DreaminaImageSupplementPublishRequest,
) -> Result<VerifiedEvidence, ImageGatewayError> {
    if !root.is_absolute() || std::fs::canonicalize(root).ok().as_deref() != Some(root) {
        return Err(conflict());
    }
    let dir: File = open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| conflict())?;
    private(&dir, true)?;
    let stdout = read(&dir, "reviewed-query.json", MAX_JSON)?;
    let ab = read(&dir, "execution-audit.json", MAX_JSON)?;
    let rb = read(&dir, "review.json", MAX_JSON)?;
    let submit = read(&dir, "submit-stdout.json", MAX_JSON)?;
    let audit: Audit = serde_json::from_slice(&ab).map_err(|_| conflict())?;
    let review: Review = serde_json::from_slice(&rb).map_err(|_| conflict())?;
    let sh = sha256_hex(&stdout);
    let ah = sha256_hex(&ab);
    if review.schema_version != 1
        || audit.schema_version != 1
        || review.authority != "dreamina-image-manual-supplement/v1"
        || review.authorization_id != request.authorization_id
        || review.dispatch_nonce != request.dispatch_nonce
        || audit.authorization_id != request.authorization_id
        || audit.dispatch_nonce != request.dispatch_nonce
        || audit.job_id.is_nil()
        || audit.submission_id.is_nil()
        || audit.executor_execution_id.is_nil()
        || request.reviewed_stdout_sha256 != sh
        || review.stdout_sha256 != sh
        || audit.stdout_sha256 != sh
        || review.execution_audit_sha256 != ah
        || review.submit_stdout_sha256 != sha256_hex(&submit)
        || audit.exit_code != 0
        || audit.executed_at_ms <= 0
        || !super::valid_sha256(&review.quote_hash)
        || !super::valid_sha256(&review.provider_command_sha256)
    {
        return Err(conflict());
    }
    let accepted = image_provider_dreamina_cli::parse_receipt(&submit).map_err(|_| conflict())?;
    if accepted.submit_id() != audit.remote_operation_id
        || audit.argv.len() != 5
        || audit.argv[..3] != ["query_result", "--submit_id", &audit.remote_operation_id]
        || audit.argv[3] != "--download_dir"
    {
        return Err(conflict());
    }
    let download_dir = &audit.argv[4];
    if !Path::new(download_dir).is_absolute()
        || download_dir.chars().any(char::is_control)
        || Path::new(download_dir).components().any(|p| {
            matches!(
                p,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(conflict());
    }
    let receipt: Value = serde_json::from_slice(&stdout).map_err(|_| conflict())?;
    if receipt.get("submit_id").and_then(Value::as_str) != Some(audit.remote_operation_id.as_str())
        || receipt.get("gen_status").and_then(Value::as_str) != Some("success")
    {
        return Err(conflict());
    }
    let result = match receipt.get("result_json") {
        Some(Value::String(v)) => serde_json::from_str::<Value>(v).map_err(|_| conflict())?,
        Some(Value::Object(v)) => Value::Object(v.clone()),
        _ => return Err(conflict()),
    };
    let candidates = result
        .get("images")
        .and_then(Value::as_array)
        .ok_or_else(conflict)?;
    if !(1..=4).contains(&candidates.len())
        || candidates.len() != audit.downloaded_images.len()
        || result
            .get("videos")
            .and_then(Value::as_array)
            .is_none_or(|v| !v.is_empty())
    {
        return Err(conflict());
    }
    let mut ids = std::collections::HashSet::new();
    let mut images = Vec::new();
    let mut total = 0usize;
    for (index, (item, d)) in candidates.iter().zip(&audit.downloaded_images).enumerate() {
        let ef = if index == 0 {
            "selected.png".into()
        } else {
            format!("image-{index}.png")
        };
        let expected = format!("{}_image_{}.png", audit.remote_operation_id, index + 1);
        let item_id = item
            .get("item_id")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty() && v.len() <= 255 && !v.chars().any(char::is_control))
            .ok_or_else(conflict)?;
        if !ids.insert(item_id)
            || d.source_index != index
            || d.filename != expected
            || d.evidence_filename != ef
            || !super::valid_sha256(&d.sha256)
            || d.byte_size == 0
            || d.byte_size > MAX_IMAGE
            || item.get("path").and_then(Value::as_str)
                != Some(format!("{}/{}", download_dir.trim_end_matches('/'), expected).as_str())
        {
            return Err(conflict());
        }
        let bytes = read(&dir, &ef, MAX_IMAGE)?;
        total = total.checked_add(bytes.len()).ok_or_else(conflict)?;
        if total > crate::artifacts::MAX_ARTIFACT_BYTES as usize
            || sha256_hex(&bytes) != d.sha256
            || bytes.len() as u64 != d.byte_size
            || image::guess_format(&bytes).ok() != Some(image::ImageFormat::Png)
        {
            return Err(conflict());
        }
        let width = item
            .get("width")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(conflict)?;
        let height = item
            .get("height")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(conflict)?;
        if width == 0
            || height == 0
            || width > 8192
            || height > 8192
            || u64::from(width) * u64::from(height) > 16 * 1024 * 1024
        {
            return Err(conflict());
        }
        // Bound the decoder independently of potentially incorrect CLI metadata.
        let mut reader =
            image::ImageReader::with_format(std::io::Cursor::new(&bytes), image::ImageFormat::Png);
        let mut limits = image::Limits::default();
        limits.max_image_width = Some(8192);
        limits.max_image_height = Some(8192);
        limits.max_alloc = Some(64 * 1024 * 1024);
        reader.limits(limits);
        let img = reader.decode().map_err(|_| conflict())?;
        if img.width() != width || img.height() != height {
            return Err(conflict());
        }
        images.push(Image {
            bytes,
            sha256: d.sha256.clone(),
            width,
            height,
        });
    }
    Ok(VerifiedEvidence {
        job_id: audit.job_id,
        submission_id: audit.submission_id,
        executor_execution_id: audit.executor_execution_id,
        submit_stdout_sha256: review.submit_stdout_sha256,
        stdout_sha256: sh,
        execution_audit_sha256: ah,
        review_sha256: sha256_hex(&rb),
        remote_operation_id: audit.remote_operation_id,
        quote_hash: review.quote_hash,
        provider_command_sha256: review.provider_command_sha256,
        images,
    })
}
fn private(file: &File, directory: bool) -> Result<(), ImageGatewayError> {
    let m = file.metadata().map_err(|_| conflict())?;
    if m.uid() != unsafe { libc::geteuid() }
        || m.mode() & 0o077 != 0
        || (directory && !m.is_dir())
        || (!directory && (!m.is_file() || m.nlink() != 1))
    {
        return Err(conflict());
    }
    Ok(())
}
fn read(dir: &File, name: &str, max: u64) -> Result<Vec<u8>, ImageGatewayError> {
    let mut f: File = openat(
        dir,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| conflict())?;
    private(&f, false)?;
    let n = f.metadata().map_err(|_| conflict())?.len();
    if n == 0 || n > max {
        return Err(conflict());
    }
    let mut b = Vec::new();
    f.by_ref()
        .take(max + 1)
        .read_to_end(&mut b)
        .map_err(|_| conflict())?;
    if b.len() as u64 != n {
        return Err(conflict());
    }
    Ok(b)
}
