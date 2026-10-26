use std::{
    fs::File,
    io::{Cursor, Read},
    os::unix::fs::MetadataExt,
    path::Path,
};

use image_provider_dreamina_cli::{DreaminaQueryStatusV1, parse_query_receipt};
use rustix::fs::{Mode, OFlags, open, openat};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::{ImageGatewayError, artifacts::sha256_hex};

const MAX_JSON_BYTES: u64 = 64 * 1024;
const MAX_SCREENSHOT_BYTES: u64 = 10 * 1024 * 1024;

pub(in crate::settlement::postgres) struct ExpectedIdentity {
    pub job_id: Uuid,
    pub request_id: String,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub provider_account_id: Uuid,
    pub upstream_task_id: String,
    pub operator_reference: String,
    pub query_stdout_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionAudit {
    schema_version: u16,
    job_id: Uuid,
    request_id: String,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_account_id: Uuid,
    upstream_task_id: String,
    upstream_identity_sha256: String,
    account_home_sha256: String,
    argv: Vec<String>,
    stdout_sha256: String,
    exit_code: i32,
    executed_at_ms: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Review {
    schema: String,
    job_id: Uuid,
    request_id: String,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_account_id: Uuid,
    upstream_task_id: String,
    operator_reference: String,
    website_cancelled: bool,
    query_stdout_sha256: String,
    execution_audit_sha256: String,
    website_confirmation_sha256: String,
    confirmed_at_ms: i64,
}

pub(in crate::settlement::postgres) struct VerifiedEvidence {
    pub query_stdout_sha256: String,
    pub execution_audit_sha256: String,
    pub review_sha256: String,
    pub website_confirmation_sha256: String,
    pub upstream_identity_sha256: String,
    pub account_home_sha256: String,
    pub queried_at_ms: i64,
    pub website_confirmed_at_ms: i64,
}

/// Operator files are reviewed evidence, not a provider-signed attestation. The
/// production operator owns this private directory; an API client cannot mint
/// authority by supplying a cancellation checkbox or its own evidence hashes.
pub(in crate::settlement::postgres) fn load(
    root: &Path,
    expected: &ExpectedIdentity,
) -> Result<VerifiedEvidence, ImageGatewayError> {
    if !root.is_absolute()
        || std::fs::canonicalize(root).ok().as_deref() != Some(root)
        || expected.job_id.is_nil()
        || expected.submission_id.is_nil()
        || expected.executor_execution_id.is_nil()
        || expected.provider_account_id.is_nil()
        || expected.submission_id == expected.executor_execution_id
        || !valid_text(&expected.request_id, 512)
        || !valid_text(&expected.operator_reference, 255)
        || !valid_sha256(&expected.query_stdout_sha256)
    {
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
    let stdout = read(&directory, "query-stdout.json", MAX_JSON_BYTES)?;
    let audit_bytes = read(&directory, "execution-audit.json", MAX_JSON_BYTES)?;
    let review_bytes = read(&directory, "review.json", MAX_JSON_BYTES)?;
    let screenshot = read(&directory, "website-confirmation.png", MAX_SCREENSHOT_BYTES)?;
    validate_screenshot(&screenshot)?;

    let audit: ExecutionAudit = serde_json::from_slice(&audit_bytes).map_err(|_| conflict())?;
    let review: Review = serde_json::from_slice(&review_bytes).map_err(|_| conflict())?;
    let stdout_sha256 = sha256_hex(&stdout);
    let execution_audit_sha256 = sha256_hex(&audit_bytes);
    let website_confirmation_sha256 = sha256_hex(&screenshot);
    if audit.schema_version != 1
        || review.schema != "factory.manual_cancellation.review.v1"
        || audit.job_id != expected.job_id
        || review.job_id != expected.job_id
        || audit.request_id != expected.request_id
        || review.request_id != expected.request_id
        || audit.submission_id != expected.submission_id
        || review.submission_id != expected.submission_id
        || audit.executor_execution_id != expected.executor_execution_id
        || review.executor_execution_id != expected.executor_execution_id
        || audit.provider_account_id != expected.provider_account_id
        || review.provider_account_id != expected.provider_account_id
        || audit.upstream_task_id != expected.upstream_task_id
        || review.upstream_task_id != expected.upstream_task_id
        || review.operator_reference != expected.operator_reference
        || !review.website_cancelled
        || stdout_sha256 != expected.query_stdout_sha256
        || audit.stdout_sha256 != stdout_sha256
        || review.query_stdout_sha256 != stdout_sha256
        || review.execution_audit_sha256 != execution_audit_sha256
        || review.website_confirmation_sha256 != website_confirmation_sha256
        || !valid_sha256(&audit.upstream_identity_sha256)
        || !valid_sha256(&audit.account_home_sha256)
        || audit.exit_code != 0
        || review.confirmed_at_ms <= 0
        || audit.executed_at_ms < review.confirmed_at_ms
        || audit.argv != ["query_result", "--submit_id", &expected.upstream_task_id]
    {
        return Err(conflict());
    }

    let receipt = parse_query_receipt(&stdout).map_err(|_| conflict())?;
    let raw: Value = serde_json::from_slice(&stdout).map_err(|_| conflict())?;
    if receipt.status() != DreaminaQueryStatusV1::Failed
        || receipt.submit_id() != expected.upstream_task_id
        || !no_results(raw.get("result_json"))
        || raw.as_object().is_none_or(|fields| {
            fields.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "submit_id"
                        | "gen_status"
                        | "fail_reason"
                        | "result_json"
                        | "queue_info"
                        | "estimated_remaining_seconds"
                )
            })
        })
    {
        return Err(conflict());
    }
    Ok(VerifiedEvidence {
        query_stdout_sha256: stdout_sha256,
        execution_audit_sha256,
        review_sha256: sha256_hex(&review_bytes),
        website_confirmation_sha256,
        upstream_identity_sha256: audit.upstream_identity_sha256,
        account_home_sha256: audit.account_home_sha256,
        queried_at_ms: audit.executed_at_ms,
        website_confirmed_at_ms: review.confirmed_at_ms,
    })
}

fn no_results(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::String(encoded)) => serde_json::from_str::<Value>(encoded)
            .ok()
            .is_some_and(|decoded| no_results(Some(&decoded))),
        Some(Value::Array(values)) => values.is_empty(),
        Some(Value::Object(values)) => values.iter().all(|(key, value)| {
            matches!(key.as_str(), "images" | "videos")
                && value.as_array().is_some_and(Vec::is_empty)
        }),
        _ => false,
    }
}

fn validate_screenshot(bytes: &[u8]) -> Result<(), ImageGatewayError> {
    // Require a real bounded PNG, rather than a renamed text file or its magic.
    if image::guess_format(bytes).ok() != Some(image::ImageFormat::Png) {
        return Err(conflict());
    }
    let mut reader = image::ImageReader::with_format(Cursor::new(bytes), image::ImageFormat::Png);
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(16_384);
    limits.max_image_height = Some(16_384);
    limits.max_alloc = Some(128 * 1024 * 1024);
    reader.limits(limits);
    let decoded = reader.decode().map_err(|_| conflict())?;
    if decoded.width() == 0 || decoded.height() == 0 {
        return Err(conflict());
    }
    Ok(())
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(in crate::settlement::postgres) fn private_metadata(
    file: &File,
    directory: bool,
) -> Result<(), ImageGatewayError> {
    let metadata = file.metadata().map_err(|_| conflict())?;
    // SAFETY: geteuid has no preconditions. Production runs the root operator;
    // the same owner check permits an isolated unprivileged offline fixture.
    if metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o7777 != if directory { 0o700 } else { 0o600 }
        || (directory && !metadata.is_dir())
        || (!directory && (!metadata.is_file() || metadata.nlink() != 1))
    {
        return Err(conflict());
    }
    Ok(())
}

pub(in crate::settlement::postgres) fn read(
    directory: &File,
    name: &str,
    maximum: u64,
) -> Result<Vec<u8>, ImageGatewayError> {
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

fn conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Dreamina manual cancellation conflicts with private operator evidence",
        None,
        "dreamina_manual_video_cancellation_conflict",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::fs::{PermissionsExt, symlink};

    struct Fixture {
        directory: tempfile::TempDir,
        expected: ExpectedIdentity,
    }

    impl Fixture {
        fn new(stdout: Value) -> Self {
            let directory = tempfile::tempdir().unwrap();
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
            let mut screenshot = Cursor::new(Vec::new());
            image::RgbaImage::new(2, 2)
                .write_to(&mut screenshot, image::ImageFormat::Png)
                .unwrap();
            let screenshot = screenshot.into_inner();
            let stdout = serde_json::to_vec(&stdout).unwrap();
            let expected = ExpectedIdentity {
                job_id: Uuid::new_v4(),
                request_id: "original-request-1".into(),
                submission_id: Uuid::new_v4(),
                executor_execution_id: Uuid::new_v4(),
                provider_account_id: Uuid::new_v4(),
                upstream_task_id: "original-upstream-1".into(),
                operator_reference: "offline:website-cancellation".into(),
                query_stdout_sha256: sha256_hex(&stdout),
            };
            let audit = serde_json::to_vec(&json!({
                "schema_version":1,"job_id":expected.job_id,"request_id":expected.request_id,
                "submission_id":expected.submission_id,"executor_execution_id":expected.executor_execution_id,
                "provider_account_id":expected.provider_account_id,"upstream_task_id":expected.upstream_task_id,
                "upstream_identity_sha256":"a".repeat(64),"account_home_sha256":"b".repeat(64),
                "argv":["query_result","--submit_id",expected.upstream_task_id],
                "stdout_sha256":expected.query_stdout_sha256,"exit_code":0,"executed_at_ms":2000
            })).unwrap();
            let review = serde_json::to_vec(&json!({
                "schema":"factory.manual_cancellation.review.v1","job_id":expected.job_id,"request_id":expected.request_id,
                "submission_id":expected.submission_id,"executor_execution_id":expected.executor_execution_id,
                "provider_account_id":expected.provider_account_id,"upstream_task_id":expected.upstream_task_id,
                "operator_reference":expected.operator_reference,"website_cancelled":true,
                "query_stdout_sha256":expected.query_stdout_sha256,"execution_audit_sha256":sha256_hex(&audit),
                "website_confirmation_sha256":sha256_hex(&screenshot),"confirmed_at_ms":1000
            })).unwrap();
            for (name, bytes) in [
                ("query-stdout.json", stdout),
                ("execution-audit.json", audit),
                ("review.json", review),
                ("website-confirmation.png", screenshot),
            ] {
                Self::write(directory.path(), name, &bytes);
            }
            Self {
                directory,
                expected,
            }
        }

        fn write(root: &Path, name: &str, bytes: &[u8]) {
            std::fs::write(root.join(name), bytes).unwrap();
            std::fs::set_permissions(root.join(name), std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }

        fn root(&self) -> std::path::PathBuf {
            std::fs::canonicalize(self.directory.path()).unwrap()
        }

        fn modify_review(&self, modify: impl FnOnce(&mut Value)) {
            let path = self.root().join("review.json");
            let mut value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            modify(&mut value);
            Self::write(
                &self.root(),
                "review.json",
                &serde_json::to_vec(&value).unwrap(),
            );
        }

        fn modify_audit(&self, modify: impl FnOnce(&mut Value)) {
            let path = self.root().join("execution-audit.json");
            let mut value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            modify(&mut value);
            let bytes = serde_json::to_vec(&value).unwrap();
            Self::write(&self.root(), "execution-audit.json", &bytes);
            self.modify_review(|review| {
                review["execution_audit_sha256"] = json!(sha256_hex(&bytes));
            });
        }
    }

    fn failed() -> Value {
        json!({"submit_id":"original-upstream-1","gen_status":"fail","fail_reason":"Canceled by user"})
    }

    #[test]
    fn accepts_private_same_task_failed_query_and_reviewed_website_cancellation() {
        for result in [
            None,
            Some(Value::Null),
            Some(json!({"images":[],"videos":[]})),
            Some(json!("{\"images\":[],\"videos\":[]}")),
        ] {
            let mut stdout = failed();
            if let Some(result) = result {
                stdout["result_json"] = result;
            }
            let fixture = Fixture::new(stdout);
            let evidence = load(&fixture.root(), &fixture.expected).unwrap();
            assert_eq!(
                evidence.query_stdout_sha256,
                fixture.expected.query_stdout_sha256
            );
            assert_eq!(evidence.queried_at_ms, 2000);
            assert_eq!(evidence.website_confirmed_at_ms, 1000);
            assert_eq!(evidence.upstream_identity_sha256, "a".repeat(64));
            assert_eq!(evidence.account_home_sha256, "b".repeat(64));
            assert_eq!(evidence.review_sha256.len(), 64);
            assert_eq!(evidence.execution_audit_sha256.len(), 64);
            assert_eq!(evidence.website_confirmation_sha256.len(), 64);
        }
    }

    #[test]
    fn rejects_success_unknown_task_and_nonempty_output_even_with_matching_hashes() {
        for stdout in [
            json!({"submit_id":"original-upstream-1","gen_status":"success"}),
            json!({"submit_id":"original-upstream-1","gen_status":"querying"}),
            json!({"submit_id":"other-upstream","gen_status":"fail"}),
            json!({"submit_id":"original-upstream-1","gen_status":"fail","result_json":{"images":[],"videos":[{"video_url":"https://example.test/one.mp4"}]}}),
            json!({"submit_id":"original-upstream-1","gen_status":"fail","video_url":"https://example.test/one.mp4"}),
            json!({"submit_id":"original-upstream-1","gen_status":"fail","result":{"url":"https://example.test/one.mp4"}}),
            json!({"submit_id":"original-upstream-1","gen_status":"fail","result_json":"not json"}),
            json!({"submit_id":"original-upstream-1","gen_status":"fail","result_json":{"outputs":[]}}),
        ] {
            let fixture = Fixture::new(stdout);
            assert!(load(&fixture.root(), &fixture.expected).is_err());
        }
    }

    #[test]
    fn rejects_client_claim_mismatched_identity_hash_and_confirmation_order() {
        for (key, value) in [
            ("website_cancelled", json!(false)),
            ("job_id", json!(Uuid::new_v4())),
            ("request_id", json!("other-request")),
            ("submission_id", json!(Uuid::new_v4())),
            ("executor_execution_id", json!(Uuid::new_v4())),
            ("provider_account_id", json!(Uuid::new_v4())),
            ("upstream_task_id", json!("another-upstream")),
            ("operator_reference", json!("client-claim")),
            ("website_confirmation_sha256", json!("c".repeat(64))),
            ("query_stdout_sha256", json!("d".repeat(64))),
            ("execution_audit_sha256", json!("e".repeat(64))),
            ("confirmed_at_ms", json!(3000)),
            ("schema", json!("client-cancel.v1")),
        ] {
            let fixture = Fixture::new(failed());
            fixture.modify_review(|review| review[key] = value);
            assert!(
                load(&fixture.root(), &fixture.expected).is_err(),
                "accepted changed {key}"
            );
        }
    }

    #[test]
    fn rejects_mismatched_failed_query_audit_even_with_reviewed_audit_hash() {
        for (key, value) in [
            ("schema_version", json!(2)),
            ("job_id", json!(Uuid::new_v4())),
            ("request_id", json!("another-request")),
            ("submission_id", json!(Uuid::new_v4())),
            ("executor_execution_id", json!(Uuid::new_v4())),
            ("provider_account_id", json!(Uuid::new_v4())),
            ("upstream_task_id", json!("another-task")),
            ("argv", json!(["submit", "--prompt", "must-not-run"])),
            ("stdout_sha256", json!("f".repeat(64))),
            ("exit_code", json!(1)),
            ("executed_at_ms", json!(0)),
            ("upstream_identity_sha256", json!("invalid")),
            ("account_home_sha256", json!("invalid")),
        ] {
            let fixture = Fixture::new(failed());
            fixture.modify_audit(|audit| audit[key] = value);
            assert!(
                load(&fixture.root(), &fixture.expected).is_err(),
                "accepted changed audit {key}"
            );
        }
    }

    #[test]
    fn rejects_relative_or_symlink_root_and_oversized_evidence() {
        let fixture = Fixture::new(failed());
        assert!(load(Path::new("."), &fixture.expected).is_err());
        let alias = fixture.root().join("root-alias");
        symlink(fixture.root(), &alias).unwrap();
        assert!(load(&alias, &fixture.expected).is_err());
        std::fs::remove_file(alias).unwrap();
        Fixture::write(
            &fixture.root(),
            "query-stdout.json",
            &vec![b' '; (MAX_JSON_BYTES + 1) as usize],
        );
        assert!(load(&fixture.root(), &fixture.expected).is_err());
    }

    #[test]
    fn rejects_symlink_hardlink_public_modes_and_invalid_screenshot() {
        let fixture = Fixture::new(failed());
        let root = fixture.root();
        for name in [
            "query-stdout.json",
            "execution-audit.json",
            "review.json",
            "website-confirmation.png",
        ] {
            let path = root.join(name);
            let bytes = std::fs::read(&path).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(load(&root, &fixture.expected).is_err());
            Fixture::write(&root, name, &bytes);
            let other = root.join("hardlink");
            std::fs::hard_link(&path, &other).unwrap();
            assert!(load(&root, &fixture.expected).is_err());
            std::fs::remove_file(&other).unwrap();
            std::fs::rename(&path, &other).unwrap();
            symlink(&other, &path).unwrap();
            assert!(load(&root, &fixture.expected).is_err());
            std::fs::remove_file(&path).unwrap();
            std::fs::rename(&other, &path).unwrap();
        }
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(load(&root, &fixture.expected).is_err());
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        Fixture::write(
            &root,
            "website-confirmation.png",
            b"\x89PNG\r\n\x1a\ninvalid",
        );
        fixture.modify_review(|review| {
            review["website_confirmation_sha256"] = json!(sha256_hex(b"\x89PNG\r\n\x1a\ninvalid"))
        });
        assert!(load(&root, &fixture.expected).is_err());
    }
}
