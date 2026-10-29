//! Operator-reviewed original execution and query evidence; no provider calls.
use super::{
    super::{conflict, valid_sha256},
    DreaminaVideoArchiveResolutionRequest,
};
use crate::{
    ImageGatewayError,
    artifacts::sha256_hex,
    media_inputs::{MediaInputKind, probe_input},
};
use base64::Engine;
use image_provider_dreamina_cli::{
    DreaminaQueryStatusV1, DreaminaSubmitPayloadV1, DreaminaSubmitRequestV1, parse_query_receipt,
    parse_submit_command,
};
use image_provider_sdk::{OutputSlot, SingleOutputCommand};
use rustix::fs::{Mode, OFlags, open, openat};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs::File, io::Read, os::unix::fs::MetadataExt, path::Path};
use uuid::Uuid;

pub(super) const MODE: &str = "normal_submit_archive_v1";
const MAX_JSON: u64 = 1024 * 1024;
const MAX_VIDEO: u64 = 256 * 1024 * 1024;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Review {
    pub schema_version: u16,
    pub evidence_mode: String,
    pub job_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub tenant_id: String,
    pub provider_account_id: Uuid,
    pub account_home_sha256: String,
    pub quote_hash: String,
    pub source_command_sha256: String,
    pub provider_command_sha256: String,
    pub execution_binding_sha256: String,
    pub remote_operation_id: String,
    pub operator_reference: String,
    pub original_started_at_ms: i64,
    pub original_finished_at_ms: i64,
    pub source_trace_id: String,
    pub source_start_line_sha256: String,
    pub source_candidate_line_sha256: String,
    pub files: BTreeMap<String, String>,
}
pub(super) struct Evidence {
    pub review: Review,
    pub command: DreaminaSubmitRequestV1,
    pub query_sha256: String,
    pub audit_sha256: String,
    pub video_url: String,
    pub video_url_sha256: String,
    pub video_sha256: String,
    pub video: Vec<u8>,
}

pub(super) fn load(
    root: &Path,
    request: &DreaminaVideoArchiveResolutionRequest,
) -> Result<Evidence, ImageGatewayError> {
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
    let review_bytes = read(&dir, "review.json", MAX_JSON, false)?;
    if sha256_hex(&review_bytes) != request.reviewed_recovery_sha256 {
        return Err(conflict());
    }
    let review: Review = serde_json::from_slice(&review_bytes).map_err(|_| conflict())?;
    if review.schema_version != 1
        || review.evidence_mode != MODE
        || review.job_id != request.job_id
        || review.submission_id != request.submission_id
        || review.executor_execution_id != request.executor_execution_id
        || review.operator_reference != request.operator_reference
        || review.tenant_id.is_empty()
        || review.provider_account_id.is_nil()
        || review.original_started_at_ms <= 0
        || review.original_finished_at_ms < review.original_started_at_ms
        || review.source_trace_id.is_empty()
        || review.source_trace_id.len() > 128
        || !review
            .source_trace_id
            .bytes()
            .all(|v| v.is_ascii_alphanumeric())
        || Uuid::parse_str(&review.remote_operation_id).is_err()
        || [
            &review.account_home_sha256,
            &review.quote_hash,
            &review.source_command_sha256,
            &review.provider_command_sha256,
            &review.execution_binding_sha256,
            &review.source_start_line_sha256,
            &review.source_candidate_line_sha256,
        ]
        .iter()
        .any(|v| !valid_sha256(v))
    {
        return Err(conflict());
    }
    let bound = |name: &str, max: u64, empty: bool| -> Result<Vec<u8>, ImageGatewayError> {
        let data = read(&dir, name, max, empty)?;
        if review
            .files
            .get(name)
            .is_none_or(|v| v != &sha256_hex(&data))
        {
            return Err(conflict());
        }
        Ok(data)
    };
    let value = |data: &[u8]| serde_json::from_slice::<Value>(data).map_err(|_| conflict());
    let command_bytes = bound("command.bin", MAX_JSON, false)?;
    let command = parse_submit_command(&command_bytes).map_err(|_| conflict())?;
    let DreaminaSubmitRequestV1::MultimodalToVideo(video_command) = &command else {
        return Err(conflict());
    };
    if video_command.duration_seconds() != 10 || video_command.resolution().as_str() != "720p" {
        return Err(conflict());
    }
    let identity = SingleOutputCommand::new(
        OutputSlot::new(0, 1).map_err(|_| conflict())?,
        DreaminaSubmitPayloadV1::new(&review.source_command_sha256, command.clone())
            .map_err(|_| conflict())?,
    )
    .map_err(|_| conflict())?;
    if hex::encode(identity.canonical_sha256()) != review.provider_command_sha256 {
        return Err(conflict());
    }
    let spec = value(&bound("spec.json", MAX_JSON, false)?)?;
    let gate = value(&bound("process-request.json", MAX_JSON, false)?)?;
    let terminal = value(&bound("process-terminal.json", MAX_JSON, false)?)?;
    for (key, expected) in [
        ("submission_id", request.submission_id.to_string()),
        (
            "provider_account_id",
            review.provider_account_id.to_string(),
        ),
        ("provider_id", "dreamina-cli".into()),
        ("command_schema", "dreamina-cli.submit.v1".into()),
        ("adapter_revision", "dreamina-cli.remote-task.v1".into()),
        (
            "executor_execution_id",
            request.executor_execution_id.to_string(),
        ),
        (
            "provider_command_sha256",
            review.provider_command_sha256.clone(),
        ),
        (
            "execution_binding_sha256",
            review.execution_binding_sha256.clone(),
        ),
        ("command_bytes_sha256", sha256_hex(&command_bytes)),
    ] {
        if spec[key].as_str() != Some(&expected) {
            return Err(conflict());
        }
    }
    if spec["output_index"] != 0
        || spec["output_total"] != 1
        || gate["environment"]["HOME"]
            .as_str()
            .is_none_or(|home| sha256_hex(home.as_bytes()) != review.account_home_sha256)
        || spec["command_byte_size"].as_u64() != Some(command_bytes.len() as u64)
        || gate["submission_id"] != spec["submission_id"]
        || gate["execution_binding_sha256"] != spec["execution_binding_sha256"]
        || terminal["execution_binding_sha256"] != spec["execution_binding_sha256"]
        || gate["launch_nonce"] != terminal["launch_nonce"]
        || gate["launch_nonce"]
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .is_none()
        || terminal["released"] != true
        || terminal["exec_started"] != true
        || terminal["outcome"]["state"] != "exited"
        || terminal["outcome"]["exit_code"] != 0
        || !terminal["outcome"]["signal"].is_null()
        || terminal["stdout_truncated"] != false
        || terminal["stderr_truncated"] != false
    {
        return Err(conflict());
    }
    for key in ["stdout_base64", "stderr_base64"] {
        if !base64::engine::general_purpose::STANDARD
            .decode(terminal[key].as_str().ok_or_else(conflict)?)
            .map_err(|_| conflict())?
            .is_empty()
        {
            return Err(conflict());
        }
    }
    verify_success_terminal_payload(&terminal)?;
    let argv: Vec<String> =
        serde_json::from_value(gate["arguments"].clone()).map_err(|_| conflict())?;
    if argv.first().map(String::as_str) != Some("multimodal2video") {
        return Err(conflict());
    }
    if argv.iter().filter(|a| a.as_str() == "--poll=0").count() != 1 {
        return Err(conflict());
    }
    let mut flags: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut index = 1;
    while index < argv.len() {
        if argv[index] == "--poll=0" {
            index += 1;
            continue;
        }
        if index + 1 >= argv.len() {
            return Err(conflict());
        }
        flags
            .entry(&argv[index])
            .or_default()
            .push(&argv[index + 1]);
        index += 2;
    }
    for (kind, flag) in [
        (
            image_provider_dreamina_cli::StagedInputKindV1::Image,
            "--image",
        ),
        (
            image_provider_dreamina_cli::StagedInputKindV1::Video,
            "--video",
        ),
        (
            image_provider_dreamina_cli::StagedInputKindV1::Audio,
            "--audio",
        ),
    ] {
        let expected: Vec<_> = command
            .staged_inputs()
            .into_iter()
            .filter(|i| i.kind == kind)
            .map(|i| i.filename)
            .collect();
        let actual: Vec<_> = flags
            .get(flag)
            .into_iter()
            .flatten()
            .map(|p| {
                Path::new(p)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(conflict)
            })
            .collect::<Result<_, _>>()?;
        if actual != expected {
            return Err(conflict());
        }
    }
    if flags.keys().any(|key| {
        ![
            "--image",
            "--video",
            "--audio",
            "--prompt",
            "--model_version",
            "--ratio",
            "--video_resolution",
            "--duration",
        ]
        .contains(key)
    }) {
        return Err(conflict());
    }
    let canonical: Value = value(&command_bytes)?;
    for (key, flag) in [
        ("prompt", "--prompt"),
        ("model_version", "--model_version"),
        ("ratio", "--ratio"),
        ("video_resolution", "--video_resolution"),
    ] {
        if flags.get(flag).map(Vec::as_slice)
            != Some([canonical[key].as_str().ok_or_else(conflict)?].as_slice())
        {
            return Err(conflict());
        }
    }
    if flags.get("--duration").map(Vec::as_slice) != Some(["10"].as_slice()) {
        return Err(conflict());
    }
    let log = String::from_utf8(bound("source-account-log.bin", 4 * MAX_JSON, false)?)
        .map_err(|_| conflict())?;
    let lines: Vec<&str> = log
        .lines()
        .filter(|line| {
            line.split(" input=")
                .next()
                .unwrap_or("")
                .split_whitespace()
                .any(|v| v == review.source_trace_id)
        })
        .collect();
    let starts: Vec<_> = lines
        .iter()
        .filter(|line| line.contains("[SubmitTask] start submit generation task"))
        .collect();
    if starts.len() != 1 || sha256_hex(starts[0].as_bytes()) != review.source_start_line_sha256 {
        return Err(conflict());
    }
    let (prefix, source) = starts[0].split_once(" input=").ok_or_else(conflict)?;
    let source: Value = serde_json::from_str(source).map_err(|_| conflict())?;
    let fields: Vec<_> = prefix.split_whitespace().collect();
    if fields.len() < 3 {
        return Err(conflict());
    }
    let timestamp = format!("{}T{}+08:00", fields[1], fields[2].replace(',', "."));
    let at =
        time::OffsetDateTime::parse(&timestamp, &time::format_description::well_known::Rfc3339)
            .map_err(|_| conflict())?
            .unix_timestamp_nanos()
            / 1_000_000;
    if at < i128::from(review.original_started_at_ms)
        || at > i128::from(review.original_finished_at_ms)
    {
        return Err(conflict());
    }
    for key in [
        "prompt",
        "model_version",
        "ratio",
        "duration",
        "video_resolution",
    ] {
        if source[key] != canonical[key] {
            return Err(conflict());
        }
    }
    for (key, flag) in [
        ("image_paths", "--image"),
        ("video_paths", "--video"),
        ("audio_paths", "--audio"),
    ] {
        let expected = flags.get(flag).cloned().unwrap_or_default();
        if !(source[key].is_null() && expected.is_empty())
            && source[key] != serde_json::json!(expected)
        {
            return Err(conflict());
        }
    }
    let candidate = format!("submit_id={}", review.remote_operation_id);
    if !lines.iter().any(|line| {
        sha256_hex(line.as_bytes()) == review.source_candidate_line_sha256
            && line
                .split(" input=")
                .next()
                .unwrap_or("")
                .split_whitespace()
                .any(|v| v == candidate)
            && line.to_ascii_lowercase().contains("upload")
    }) {
        return Err(conflict());
    }
    let query = bound("query-stdout.json", MAX_JSON, false)?;
    let audit_bytes = bound("execution-audit.json", MAX_JSON, false)?;
    let audit = value(&audit_bytes)?;
    let script = bound("capture-query-only.py", MAX_JSON, false)?;
    if script.is_empty() || !bound("query-stderr.bin", MAX_JSON, true)?.is_empty() {
        return Err(conflict());
    }
    let query_value = value(&query)?;
    let parsed = parse_query_receipt(&query).map_err(|_| conflict())?;
    if parsed.submit_id() != review.remote_operation_id
        || parsed.status() != DreaminaQueryStatusV1::Success
        || query_value["prompt"] != canonical["prompt"]
        || audit["native_id"] != review.remote_operation_id
        || audit["exit_code"] != 0
        || audit["timed_out"] != false
        || audit["query_only"] != true
        || audit["generation_calls"] != 0
        || audit["business_writes"] != 0
        || audit["stdout_sha256"] != sha256_hex(&query)
        || audit["stdout_bytes"].as_u64() != Some(query.len() as u64)
        || audit["stderr_bytes"] != 0
        || audit["executable_sha256"] != gate["executable_sha256"]
        || audit["argv"]
            != serde_json::json!([
                gate["executable"],
                "query_result",
                "--submit_id",
                review.remote_operation_id
            ])
    {
        return Err(conflict());
    }
    let mut previous = i128::from(review.original_finished_at_ms);
    for key in ["started_at", "finished_at"] {
        let timestamp = time::OffsetDateTime::parse(
            audit[key].as_str().ok_or_else(conflict)?,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|_| conflict())?
        .unix_timestamp_nanos()
            / 1_000_000;
        if timestamp < previous {
            return Err(conflict());
        }
        previous = timestamp;
    }
    let remote = parsed.remote_video_result().ok_or_else(conflict)?;
    let video = bound("original-video.mp4", MAX_VIDEO, false)?;
    let archive = value(&bound("archive-audit.json", MAX_JSON, false)?)?;
    let video_sha256 = sha256_hex(&video);
    let video_url_sha256 = sha256_hex(remote.video_url().as_bytes());
    if archive["native_id"] != review.remote_operation_id
        || archive["query_sha256"] != sha256_hex(&query)
        || archive["original_sha256"] != video_sha256
        || archive["original_bytes"].as_u64() != Some(video.len() as u64)
        || archive["public_dns_pinned"] != true
        || archive["tls_hostname_verified"] != true
        || archive["transcoded"] != false
        || archive["generation_calls"] != 0
        || archive["business_writes"] != 0
    {
        return Err(conflict());
    }
    let hops = archive["hops"].as_array().ok_or_else(conflict)?;
    let query_host = reqwest::Url::parse(remote.video_url())
        .map_err(|_| conflict())?
        .host_str()
        .ok_or_else(conflict)?
        .to_owned();
    if hops.is_empty()
        || hops.len() > 4
        || hops[0]["host"] != query_host
        || hops.last().is_none_or(|hop| hop["status"] != 200)
        || hops[..hops.len() - 1].iter().any(|hop| {
            hop["status"]
                .as_u64()
                .is_none_or(|status| ![301, 302, 303, 307, 308].contains(&status))
        })
    {
        return Err(conflict());
    }
    let mut download_start = None;
    for key in ["started_at", "finished_at"] {
        let at = time::OffsetDateTime::parse(
            archive[key].as_str().ok_or_else(conflict)?,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|_| conflict())?
        .unix_timestamp_nanos()
            / 1_000_000;
        if at < previous {
            return Err(conflict());
        }
        if let Some(start) = download_start {
            if at - start > 180_000 {
                return Err(conflict());
            }
        } else {
            download_start = Some(at);
        }
        previous = at;
    }
    let metadata = probe_input(&video, MediaInputKind::Video).map_err(|_| conflict())?;
    let (width, height) = metadata.dimensions().ok_or_else(conflict)?;
    if width != 1280
        || height != 720
        || metadata
            .duration_ms()
            .is_none_or(|ms| !(9000..=11000).contains(&ms))
    {
        return Err(conflict());
    }
    Ok(Evidence {
        review,
        command,
        query_sha256: sha256_hex(&query),
        audit_sha256: sha256_hex(&audit_bytes),
        video_url: remote.video_url().into(),
        video_url_sha256,
        video_sha256,
        video,
    })
}

// DiskTerminal has no submission_id: the reviewed request binds that ID, while
// the terminal binds its execution and launch nonce plus its own payload hash.
// Match process::protocol::DiskTerminal's fixed successful, empty-stream form.
fn verify_success_terminal_payload(terminal: &Value) -> Result<(), ImageGatewayError> {
    let mut digest = Sha256::new();
    digest.update(b"ai-image-factory/gated-cli-terminal/v1\0");
    let binding = terminal["execution_binding_sha256"]
        .as_str()
        .ok_or_else(conflict)?;
    digest.update((binding.len() as u64).to_be_bytes());
    digest.update(binding.as_bytes());
    for key in ["launch_nonce", "helper_nonce", "child_nonce"] {
        let nonce = terminal[key]
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(conflict)?;
        digest.update(nonce.as_bytes());
    }
    digest.update([1, 1]); // released, exec_started
    digest.update([0, 1, 0, 0, 0, 0, 0]); // Exited(Some(0), None)
    for _ in 0..2 {
        digest.update(0_u64.to_be_bytes()); // empty base64 stream
        digest.update([0]); // not truncated
    }
    if terminal["payload_sha256"].as_str() != Some(hex::encode(digest.finalize()).as_str()) {
        return Err(conflict());
    }
    Ok(())
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
fn read(dir: &File, name: &str, max: u64, empty: bool) -> Result<Vec<u8>, ImageGatewayError> {
    let mut f: File = openat(
        dir,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| conflict())?;
    private(&f, false)?;
    let n = f.metadata().map_err(|_| conflict())?.len();
    if (!empty && n == 0) || n > max {
        return Err(conflict());
    }
    let mut bytes = Vec::with_capacity(n as usize);
    f.by_ref()
        .take(max + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| conflict())?;
    if bytes.len() as u64 != n {
        return Err(conflict());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successful_terminal_matches_producer_hash_without_submission_id() {
        let mut terminal = serde_json::json!({
            "execution_binding_sha256": "a".repeat(64),
            "launch_nonce": "00000000-0000-4000-8000-000000000001",
            "helper_nonce": "00000000-0000-4000-8000-000000000002",
            "child_nonce": "00000000-0000-4000-8000-000000000003",
            "released": true,
            "exec_started": true,
            "outcome": { "state": "exited", "exit_code": 0, "signal": null },
            "stdout_base64": "", "stdout_truncated": false,
            "stderr_base64": "", "stderr_truncated": false,
            "payload_sha256": "debefb249766fa313739359f83cb49472d91e2b4ae9664eaf907669bea2e36b6"
        });
        assert!(terminal.get("submission_id").is_none());
        assert!(verify_success_terminal_payload(&terminal).is_ok());
        for field in [
            "payload_sha256",
            "execution_binding_sha256",
            "launch_nonce",
            "helper_nonce",
            "child_nonce",
        ] {
            let mut changed = terminal.clone();
            changed[field] = serde_json::json!("changed");
            assert!(
                verify_success_terminal_payload(&changed).is_err(),
                "{field}"
            );
        }
        terminal.as_object_mut().unwrap().remove("payload_sha256");
        assert!(verify_success_terminal_payload(&terminal).is_err());
    }
}
