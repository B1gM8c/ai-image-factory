//! Read-only verification of a saved original-ID query; never invokes a CLI.
use super::journal::valid_sha256;
use crate::{ImageGatewayError, artifacts::sha256_hex};
use base64::{Engine, engine::general_purpose::STANDARD};
use image_provider_dreamina_cli::{DreaminaQueryStatusV1, parse_query_receipt};
use rustix::fs::{Mode, OFlags, open, openat};
use serde_json::Value;
use std::{fs::File, io::Read, os::unix::fs::MetadataExt, path::Path};
use uuid::Uuid;

pub(super) struct Identity<'a> {
    pub job_id: Uuid,
    pub submission_id: Uuid,
    pub execution_id: Uuid,
    pub account_id: Uuid,
    pub remote_id: &'a str,
    pub binding_sha256: &'a str,
    pub provider_command_sha256: &'a str,
    pub reviewed_stdout_sha256: &'a str,
    pub reviewed_query_audit_sha256: &'a str,
    pub credential_auth_sha256: &'a str,
    pub execution_profile_id: Uuid,
    pub credential_pool_id: Uuid,
    pub credential_revision: i64,
    pub prompt: &'a str,
    pub quarantine_at_ms: i64,
}
pub(super) struct Verified {
    pub stdout_sha256: String,
    pub audit_sha256: String,
    pub process_request_sha256: String,
    pub process_terminal_sha256: String,
    pub prompt_sha256: String,
    pub queried_at_ms: i64,
}

/// The private operator audit is evidence, not a provider-signed attestation.
/// Its captured original files must still match the immutable DB binding.
pub(super) fn load(
    root: &Path,
    original_entry: &Path,
    e: &Identity<'_>,
) -> Result<Verified, ImageGatewayError> {
    let query = directory(root, true)?;
    let original = directory(original_entry, false)?;
    let stdout = read(&query, "query-stdout.json", true)?;
    let audit_bytes = read(&query, "execution-audit.json", true)?;
    let request_bytes = read(&original, "process-request.json", false)?;
    let terminal_bytes = read(&original, "process-terminal.json", false)?;
    let spec = json(&read(&original, "spec.json", false)?)?;
    let audit = json(&audit_bytes)?;
    let request = json(&request_bytes)?;
    let terminal = json(&terminal_bytes)?;
    verify(&stdout, &audit, &request, &terminal, &spec, e)?;
    if sha256_hex(&audit_bytes) != e.reviewed_query_audit_sha256
        || audit["request_sha256"] != sha256_hex(&request_bytes)
        || audit["terminal_sha256"] != sha256_hex(&terminal_bytes)
    {
        return Err(conflict());
    }
    Ok(Verified {
        stdout_sha256: sha256_hex(&stdout),
        audit_sha256: sha256_hex(&audit_bytes),
        process_request_sha256: sha256_hex(&request_bytes),
        process_terminal_sha256: sha256_hex(&terminal_bytes),
        prompt_sha256: sha256_hex(e.prompt.as_bytes()),
        queried_at_ms: timestamp(&audit["finished_at"])?,
    })
}
fn verify(
    stdout: &[u8],
    audit: &Value,
    request: &Value,
    terminal: &Value,
    spec: &Value,
    e: &Identity<'_>,
) -> Result<(), ImageGatewayError> {
    let receipt = parse_query_receipt(stdout).map_err(|_| conflict())?;
    let raw = json(stdout)?;
    let started = timestamp(&audit["started_at"])?;
    let finished = timestamp(&audit["finished_at"])?;
    let argv = serde_json::json!([
        request["executable"],
        "query_result",
        "--submit_id",
        e.remote_id
    ]);
    let original_stdout = STANDARD
        .decode(terminal["stdout_base64"].as_str().ok_or_else(conflict)?)
        .map_err(|_| conflict())?;
    let original_receipt = json(&original_stdout)?;
    let arguments = request["arguments"].as_array().ok_or_else(conflict)?;
    let prompt_index = arguments
        .iter()
        .position(|v| v == "--prompt")
        .ok_or_else(conflict)?;
    if !valid_sha256(e.reviewed_stdout_sha256)
        || receipt.status() != DreaminaQueryStatusV1::Failed
        || receipt.failure_code() != Some("dreamina_copyright_rejected")
        || receipt.submit_id() != e.remote_id
        || !no_results(raw.get("result_json"))
        || raw["prompt"] != e.prompt
        || original_receipt["prompt"] != e.prompt
        || original_receipt["submit_id"] != e.remote_id
        || original_receipt["gen_status"] != "querying"
        || arguments.get(prompt_index + 1).and_then(Value::as_str) != Some(e.prompt)
        || arguments.iter().filter(|v| *v == "--prompt").count() != 1
        || spec["submission_id"] != e.submission_id.to_string()
        || spec["executor_execution_id"] != e.execution_id.to_string()
        || spec["provider_account_id"] != e.account_id.to_string()
        || spec["execution_profile_id"] != e.execution_profile_id.to_string()
        || spec["credential_pool_id"] != e.credential_pool_id.to_string()
        || spec["credential_revision"] != e.credential_revision
        || spec["credential_auth_sha256"] != e.credential_auth_sha256
        || spec["provider_id"] != "dreamina-cli"
        || spec["provider_command_sha256"] != e.provider_command_sha256
        || spec["execution_binding_sha256"] != e.binding_sha256
        || request["submission_id"] != e.submission_id.to_string()
        || request["execution_binding_sha256"] != e.binding_sha256
        || terminal["execution_binding_sha256"] != e.binding_sha256
        || request["launch_nonce"] != terminal["launch_nonce"]
        || request["launch_nonce"]
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .is_none_or(|id| id.is_nil())
        || terminal["released"] != true
        || terminal["exec_started"] != true
        || terminal["stdout_truncated"] != false
        || terminal["stderr_truncated"] != false
        || terminal["outcome"] != serde_json::json!({"state":"exited","exit_code":0,"signal":null})
        || audit["job_id"] != e.job_id.to_string()
        || audit["submission_id"] != e.submission_id.to_string()
        || audit["execution_id"] != e.execution_id.to_string()
        || audit["native_id"] != e.remote_id
        || audit["argv"] != argv
        || audit["binding_sha256"] != e.binding_sha256
        || audit["provider_command_sha256"] != e.provider_command_sha256
        || audit["executable_sha256"] != request["executable_sha256"]
        || audit["prompt_sha256"] != sha256_hex(e.prompt.as_bytes())
        || audit["query_only"] != true
        || audit["query_calls"] != 1
        || audit["generation_calls"] != 0
        || audit["downloads"] != 0
        || audit["exit_code"] != 0
        || audit["timed_out"] != false
        || audit["output_limit_exceeded"] != false
        || audit["original_evidence_unchanged"] != true
        || audit["stdout_bytes"] != stdout.len() as u64
        || audit["stderr_bytes"] != 0
        || audit["stdout_sha256"] != e.reviewed_stdout_sha256
        || sha256_hex(stdout) != e.reviewed_stdout_sha256
        || started < e.quarantine_at_ms
        || finished < started
        || finished - started > 65_000
    {
        return Err(conflict());
    }
    Ok(())
}
fn no_results(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => serde_json::from_str::<Value>(s)
            .ok()
            .is_some_and(|v| no_results(Some(&v))),
        Some(Value::Object(o)) => o.iter().all(|(k, v)| {
            matches!(k.as_str(), "images" | "videos") && v.as_array().is_some_and(Vec::is_empty)
        }),
        Some(Value::Array(a)) => a.is_empty(),
        _ => false,
    }
}
fn timestamp(v: &Value) -> Result<i64, ImageGatewayError> {
    time::OffsetDateTime::parse(
        v.as_str().ok_or_else(conflict)?,
        &time::format_description::well_known::Rfc3339,
    )
    .map_err(|_| conflict())
    .and_then(|v| i64::try_from(v.unix_timestamp_nanos() / 1_000_000).map_err(|_| conflict()))
}
fn json(bytes: &[u8]) -> Result<Value, ImageGatewayError> {
    serde_json::from_slice(bytes).map_err(|_| conflict())
}
fn directory(path: &Path, private: bool) -> Result<File, ImageGatewayError> {
    if !path.is_absolute() || std::fs::canonicalize(path).ok().as_deref() != Some(path) {
        return Err(conflict());
    }
    let file = File::from(
        open(
            path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| conflict())?,
    );
    let m = file.metadata().map_err(|_| conflict())?;
    if !m.is_dir()
        || m.mode() & 0o022 != 0
        || (private && (m.uid() != unsafe { libc::geteuid() } || m.mode() & 0o7777 != 0o700))
    {
        return Err(conflict());
    }
    Ok(file)
}
fn read(dir: &File, name: &str, private: bool) -> Result<Vec<u8>, ImageGatewayError> {
    let mut f = File::from(
        openat(
            dir,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| conflict())?,
    );
    let m = f.metadata().map_err(|_| conflict())?;
    if !m.is_file()
        || m.nlink() != 1
        || m.mode() & 0o022 != 0
        || m.len() == 0
        || m.len() > 65_536
        || m.uid() != dir.metadata().map_err(|_| conflict())?.uid()
        || (private && m.mode() & 0o7777 != 0o600)
    {
        return Err(conflict());
    }
    let mut bytes = Vec::new();
    f.by_ref()
        .take(65_537)
        .read_to_end(&mut bytes)
        .map_err(|_| conflict())?;
    if bytes.len() as u64 != m.len() {
        return Err(conflict());
    }
    Ok(bytes)
}
fn conflict() -> ImageGatewayError {
    super::accepted_terminal_recovery::conflict()
}
