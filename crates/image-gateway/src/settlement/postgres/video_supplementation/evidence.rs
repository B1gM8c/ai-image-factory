use std::{fs::File, io::Read, os::unix::fs::MetadataExt, path::Path};

use rustix::fs::{Mode, OFlags, open, openat};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::{DreaminaVideoSupplementPublishRequest, conflict};
use crate::{ImageGatewayError, artifacts::sha256_hex};

const MAX_RECEIPT_BYTES: u64 = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionAudit {
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
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Review {
    schema_version: u16,
    stdout_sha256: String,
    execution_audit_sha256: String,
    video_url_sha256: String,
    quote_hash: String,
    provider_command_sha256: String,
    submit_stdout_sha256: String,
    retrieved_video_sha256: String,
    retrieved_video_byte_size: i64,
    #[serde(default)]
    acceptance_recovery_sha256: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveredAcceptance {
    schema_version: u16,
    evidence_mode: String,
    authorization_id: Uuid,
    dispatch_nonce: Uuid,
    command_hash: String,
    quote_hash: String,
    remote_operation_id: String,
    query_stdout_sha256: String,
    reviewed_recovery_sha256: String,
}

pub(super) struct VerifiedEvidence {
    pub job_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub submit_stdout_sha256: String,
    pub retrieved_video_sha256: String,
    pub retrieved_video_byte_size: i64,
    pub video_bytes: Vec<u8>,
    pub video_width: u32,
    pub video_height: u32,
    pub video_duration_ms: u64,
    pub stdout_sha256: String,
    pub execution_audit_sha256: String,
    pub review_sha256: String,
    pub video_url_sha256: String,
    pub remote_operation_id: String,
    pub quote_hash: String,
    pub provider_command_sha256: String,
    pub result: image_provider_sdk::RemoteVideoResult,
}

/// These files are operator-reviewed evidence, not a cryptographic provider
/// attestation. No command, network request, or download is performed here.
pub(super) fn load(
    root: &Path,
    request: &DreaminaVideoSupplementPublishRequest,
) -> Result<VerifiedEvidence, ImageGatewayError> {
    if !root.is_absolute() || std::fs::canonicalize(root).ok().as_deref() != Some(root) {
        return Err(conflict());
    }
    let directory: File = open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| conflict())?;
    private_metadata(&directory, true)?;
    let stdout = read(&directory, "reviewed-query.json", MAX_RECEIPT_BYTES)?;
    let audit_bytes = read(&directory, "execution-audit.json", MAX_RECEIPT_BYTES)?;
    let review_bytes = read(&directory, "review.json", MAX_RECEIPT_BYTES)?;
    let review: Review = serde_json::from_slice(&review_bytes).map_err(|_| conflict())?;
    let audit: ExecutionAudit = serde_json::from_slice(&audit_bytes).map_err(|_| conflict())?;
    let submit_stdout = match &review.acceptance_recovery_sha256 {
        None => read(&directory, "submit-stdout.json", MAX_RECEIPT_BYTES)?,
        Some(expected) => {
            let marker = read(&directory, "acceptance-recovery.json", MAX_RECEIPT_BYTES)?;
            let source: RecoveredAcceptance =
                serde_json::from_slice(&marker).map_err(|_| conflict())?;
            let query = read(&directory, "acceptance-query.json", MAX_RECEIPT_BYTES)?;
            if sha256_hex(&marker) != *expected
                || source.schema_version != 1
                || source.evidence_mode != "native_log_acceptance_query_v1"
                || source.authorization_id != request.authorization_id
                || source.dispatch_nonce != request.dispatch_nonce
                || source.command_hash != review.provider_command_sha256
                || source.quote_hash != review.quote_hash
                || source.remote_operation_id != audit.remote_operation_id
                || source.query_stdout_sha256 != sha256_hex(&query)
                || !super::valid_sha256(&source.reviewed_recovery_sha256)
            {
                return Err(conflict());
            }
            query
        }
    };
    let stdout_sha256 = sha256_hex(&stdout);
    let execution_audit_sha256 = sha256_hex(&audit_bytes);
    if review.schema_version != 1
        || audit.schema_version != 1
        || stdout_sha256 != request.reviewed_stdout_sha256
        || review.stdout_sha256 != stdout_sha256
        || audit.stdout_sha256 != stdout_sha256
        || review.execution_audit_sha256 != execution_audit_sha256
        || audit.authorization_id != request.authorization_id
        || audit.dispatch_nonce != request.dispatch_nonce
        || audit.job_id.is_nil()
        || audit.submission_id.is_nil()
        || audit.executor_execution_id.is_nil()
        || review.submit_stdout_sha256 != sha256_hex(&submit_stdout)
        || audit.exit_code != 0
        || audit.executed_at_ms <= 0
        || audit.argv != ["query_result", "--submit_id", &audit.remote_operation_id]
        || !super::valid_sha256(&review.quote_hash)
        || !super::valid_sha256(&review.provider_command_sha256)
    {
        return Err(conflict());
    }
    // The original receipt or explicitly reviewed native-log recovery receipt
    // must name this operation. A URL alone is never acceptance evidence.
    let submit =
        image_provider_dreamina_cli::parse_receipt(&submit_stdout).map_err(|_| conflict())?;
    if submit.submit_id() != audit.remote_operation_id {
        return Err(conflict());
    }
    let (retrieved_video_sha256, retrieved_video_byte_size, video_bytes) = video_bytes(&directory)?;
    if retrieved_video_sha256 != review.retrieved_video_sha256
        || retrieved_video_byte_size != review.retrieved_video_byte_size
    {
        return Err(conflict());
    }
    let metadata =
        crate::media_inputs::probe_input(&video_bytes, crate::media_inputs::MediaInputKind::Video)
            .map_err(|_| conflict())?;
    let (video_width, video_height) = metadata.dimensions().ok_or_else(conflict)?;
    let video_duration_ms = metadata.duration_ms().ok_or_else(conflict)?;
    let receipt: Value = serde_json::from_slice(&stdout).map_err(|_| conflict())?;
    if receipt.get("submit_id").and_then(Value::as_str) != Some(audit.remote_operation_id.as_str())
        || receipt.get("gen_status").and_then(Value::as_str) != Some("success")
    {
        return Err(conflict());
    }
    let result = match receipt.get("result_json") {
        Some(Value::String(value)) => {
            serde_json::from_str::<Value>(value).map_err(|_| conflict())?
        }
        Some(Value::Object(value)) => Value::Object(value.clone()),
        _ => return Err(conflict()),
    };
    if result
        .get("images")
        .and_then(Value::as_array)
        .is_none_or(|v| !v.is_empty())
    {
        return Err(conflict());
    }
    let videos = result
        .get("videos")
        .and_then(Value::as_array)
        .ok_or_else(conflict)?;
    let [video] = videos.as_slice() else {
        return Err(conflict());
    };
    let url = video
        .get("video_url")
        .and_then(Value::as_str)
        .ok_or_else(conflict)?;
    // CLI queryResultMediaOutput has no TTL field. Preserve the exact original
    // URL and do not infer an expiry from signed query parameters or metadata.
    let result =
        image_provider_sdk::RemoteVideoResult::new(url.to_owned(), None).map_err(|_| conflict())?;
    let video_url_sha256 = sha256_hex(result.video_url().as_bytes());
    if review.video_url_sha256 != video_url_sha256 {
        return Err(conflict());
    }
    Ok(VerifiedEvidence {
        job_id: audit.job_id,
        submission_id: audit.submission_id,
        executor_execution_id: audit.executor_execution_id,
        submit_stdout_sha256: review.submit_stdout_sha256,
        retrieved_video_sha256,
        retrieved_video_byte_size,
        video_bytes,
        video_width,
        video_height,
        video_duration_ms,
        stdout_sha256,
        execution_audit_sha256,
        review_sha256: sha256_hex(&review_bytes),
        video_url_sha256,
        remote_operation_id: audit.remote_operation_id,
        quote_hash: review.quote_hash,
        provider_command_sha256: review.provider_command_sha256,
        result,
    })
}

fn private_metadata(file: &File, directory: bool) -> Result<(), ImageGatewayError> {
    let metadata = file.metadata().map_err(|_| conflict())?;
    // SAFETY: geteuid has no preconditions.
    if metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
        || (directory && !metadata.is_dir())
        || (!directory && (!metadata.is_file() || metadata.nlink() != 1))
    {
        return Err(conflict());
    }
    Ok(())
}

fn read(directory: &File, name: &str, maximum: u64) -> Result<Vec<u8>, ImageGatewayError> {
    let mut file: File = openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| conflict())?;
    private_metadata(&file, false)?;
    let size = file.metadata().map_err(|_| conflict())?.len();
    if size == 0 || size > maximum {
        return Err(conflict());
    }
    let mut bytes = Vec::with_capacity(size as usize);
    file.by_ref()
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| conflict())?;
    if bytes.len() as u64 != size {
        return Err(conflict());
    }
    Ok(bytes)
}

fn video_bytes(directory: &File) -> Result<(String, i64, Vec<u8>), ImageGatewayError> {
    use sha2::{Digest, Sha256};
    let mut file: File = openat(
        directory,
        "retrieved-video.mp4",
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| conflict())?;
    private_metadata(&file, false)?;
    let size = file.metadata().map_err(|_| conflict())?.len();
    if size < 12 || size > 128 * 1024 * 1024 {
        return Err(conflict());
    }
    let mut prefix = [0u8; 12];
    file.read_exact(&mut prefix).map_err(|_| conflict())?;
    if &prefix[4..8] != b"ftyp" {
        return Err(conflict());
    }
    let mut bytes = Vec::with_capacity(size as usize);
    bytes.extend_from_slice(&prefix);
    let mut hash = Sha256::new();
    hash.update(prefix);
    let mut count = 12u64;
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer).map_err(|_| conflict())?;
        if n == 0 {
            break;
        }
        count += n as u64;
        if count > size {
            return Err(conflict());
        }
        hash.update(&buffer[..n]);
        bytes.extend_from_slice(&buffer[..n]);
    }
    if count != size {
        return Err(conflict());
    }
    Ok((hex::encode(hash.finalize()), size as i64, bytes))
}
