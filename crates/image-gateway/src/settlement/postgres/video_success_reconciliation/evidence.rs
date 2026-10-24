use std::{fs::File, io::Read, os::unix::fs::MetadataExt, path::Path};

use rustix::fs::{Mode, OFlags, open, openat};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::{DreaminaVideoSuccessResolutionRequest, conflict};
use crate::{ImageGatewayError, artifacts::sha256_hex};

const MAX_RECEIPT_BYTES: u64 = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionAudit {
    schema_version: u16,
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
}

pub(super) struct VerifiedEvidence {
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
    request: &DreaminaVideoSuccessResolutionRequest,
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
    let stdout_sha256 = sha256_hex(&stdout);
    let execution_audit_sha256 = sha256_hex(&audit_bytes);
    if review.schema_version != 1
        || audit.schema_version != 1
        || stdout_sha256 != request.reviewed_stdout_sha256
        || review.stdout_sha256 != stdout_sha256
        || audit.stdout_sha256 != stdout_sha256
        || review.execution_audit_sha256 != execution_audit_sha256
        || audit.job_id != request.job_id
        || audit.submission_id != request.submission_id
        || audit.executor_execution_id != request.executor_execution_id
        || audit.exit_code != 0
        || audit.executed_at_ms <= 0
        || audit.argv != ["query_result", "--submit_id", &audit.remote_operation_id]
        || !super::valid_sha256(&review.quote_hash)
        || !super::valid_sha256(&review.provider_command_sha256)
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::fs::{PermissionsExt, symlink};

    fn fixture(result: Value) -> (tempfile::TempDir, DreaminaVideoSuccessResolutionRequest) {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let request = DreaminaVideoSuccessResolutionRequest {
            job_id: Uuid::new_v4(),
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            reviewed_stdout_sha256: String::new(),
            operator_reference: "offline:reviewed-video".into(),
        };
        let stdout = serde_json::to_vec(
            &json!({"submit_id":"original-video-1","gen_status":"success","result_json":result}),
        )
        .unwrap();
        let request = DreaminaVideoSuccessResolutionRequest {
            reviewed_stdout_sha256: sha256_hex(&stdout),
            ..request
        };
        let audit = serde_json::to_vec(&json!({"schema_version":1,"job_id":request.job_id,"submission_id":request.submission_id,"executor_execution_id":request.executor_execution_id,
            "remote_operation_id":"original-video-1","argv":["query_result","--submit_id","original-video-1"],"stdout_sha256":request.reviewed_stdout_sha256,"exit_code":0,"executed_at_ms":1})).unwrap();
        let review = serde_json::to_vec(&json!({"schema_version":1,"stdout_sha256":request.reviewed_stdout_sha256,"execution_audit_sha256":sha256_hex(&audit),
            "video_url_sha256":sha256_hex(b"https://media.example.test/video%2Fone.mp4?token=A%2Bb"),"quote_hash":"a".repeat(64),"provider_command_sha256":"b".repeat(64)})).unwrap();
        for (name, bytes) in [
            ("reviewed-query.json", stdout),
            ("execution-audit.json", audit),
            ("review.json", review),
        ] {
            std::fs::write(directory.path().join(name), bytes).unwrap();
            std::fs::set_permissions(
                directory.path().join(name),
                std::fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        (directory, request)
    }
    fn result() -> Value {
        json!({"images":[],"videos":[{"video_url":"https://media.example.test/video%2Fone.mp4?token=A%2Bb","duration":10}]})
    }

    #[test]
    fn accepts_one_reviewed_original_video_and_preserves_url_without_inventing_expiry() {
        for result in [result(), json!(result().to_string())] {
            let (directory, request) = fixture(result);
            let root = std::fs::canonicalize(directory.path()).unwrap();
            let verified = load(&root, &request).unwrap();
            assert_eq!(
                verified.result.video_url(),
                "https://media.example.test/video%2Fone.mp4?token=A%2Bb"
            );
            assert_eq!(verified.result.expires_at_ms(), None);
        }
    }

    #[test]
    fn rejects_ambiguous_or_invalid_receipts_even_with_consistent_reviewed_hashes() {
        for result in [
            json!({"images":[],"videos":[]}),
            json!({"images":[],"videos":[{"video_url":"https://example.test/1.mp4"},{"video_url":"https://example.test/2.mp4"}]}),
            json!({"images":[{"image_url":"https://example.test/1.png"}],"videos":[{"video_url":"https://example.test/1.mp4"}]}),
            json!({"images":[],"videos":[{"video_url":"http://example.test/1.mp4"}]}),
            json!({"images":[],"videos":[{"video_url":"https://name:secret@example.test/1.mp4"}]}),
            json!({"images":[],"videos":[{"video_url":"https://example.test/1.mp4#fragment"}]}),
            json!({"images":[],"videos":[{"video_url":"https://example.test/1.mp4\n"}]}),
        ] {
            let (directory, request) = fixture(result);
            assert!(load(&std::fs::canonicalize(directory.path()).unwrap(), &request).is_err());
        }
    }

    #[test]
    fn rejects_unprotected_or_symlinked_review_files() {
        let (directory, request) = fixture(result());
        let root = std::fs::canonicalize(directory.path()).unwrap();
        let review = root.join("review.json");
        std::fs::set_permissions(&review, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load(&root, &request).is_err());
        std::fs::set_permissions(&review, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::rename(&review, root.join("other.json")).unwrap();
        symlink(root.join("other.json"), &review).unwrap();
        assert!(load(&root, &request).is_err());
    }
}
