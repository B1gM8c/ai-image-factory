//! Same-attempt recovery of a missing native submit receipt. No generation runs
//! here: a unique, bound acceptance log is followed only by query_result(ID).
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
    sync::Arc,
};

use image_cli_runtime::{NoopSpawnObserver, RecoverableAttemptWorkspace, TokioProcessBackend};
use image_provider_dreamina_cli::{
    DreaminaQueryStatusV1, QueryResultRequestV1, parse_query_receipt,
};
use rustix::fs::{self as rfs, Mode, OFlags};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

use super::*;
use crate::provider_tasks::{GatedCliReceiptEvidence, ProviderSubmitDriverCall};

const SOURCE_FILE: &str = "process-receipt-recovery-native-source.json";
const ACCEPTED_FILE: &str = "process-receipt-recovery-accepted.json";
const MAX_LOG_BYTES: u64 = 4 * 1024 * 1024;
const MAX_SOURCE_BYTES: u64 = 1024 * 1024;
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_GRACE: Duration = Duration::from_secs(2);
const MODE: &str = "same_attempt_native_acceptance_query_v1";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Source {
    schema_version: u16,
    evidence_mode: String,
    gate_request_sha256: String,
    gate_terminal_sha256: String,
    execution_binding_sha256: String,
    launch_nonce: Uuid,
    remote_operation_id: String,
    trace_id: String,
    utc_offset_seconds: i32,
    started_line: String,
    accepted_line: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QueryEvidence {
    gate_request_sha256: String,
    remote_operation_id: String,
    started_at_ms: i64,
    finished_at_ms: i64,
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_truncated: bool,
    stderr_truncated: bool,
    query_only: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Accepted {
    schema_version: u16,
    evidence_mode: String,
    gate_request_sha256: String,
    source_sha256: String,
    query_file: String,
    query_sha256: String,
}

/// Only the normal driver calls this. `command` and the operator's historical
/// projection remain byte-for-byte unchanged. Absolute media paths bind a
/// native log to this submission+nonce, including concurrent identical prompts.
pub(super) fn project_normal_material_paths(
    projected: GatedCliCommand,
    command: &SingleOutputCommand<DreaminaSubmitPayloadV1>,
    workspace: &WorkingDirectory,
) -> Result<GatedCliCommand, ProviderFailure> {
    let request = parse_submit_command(command.canonical_payload()).map_err(|_| invalid())?;
    if !matches!(request, DreaminaSubmitRequestV1::MultimodalToVideo(_)) {
        return Ok(projected);
    }
    let argv = absolute_media_arguments(projected.arguments(), workspace.path(), true)?;
    projected.with_arguments(argv).map_err(|_| invalid())
}

fn absolute_media_arguments(
    argv: &[String],
    workspace: &Path,
    verify_files: bool,
) -> Result<Vec<String>, ProviderFailure> {
    let mut result = argv.to_vec();
    let mut count = 0;
    let mut index = 1;
    while index < argv.len() {
        if argv[index] == "--poll=0" {
            index += 1;
            continue;
        }
        if index + 1 >= argv.len() {
            return Err(invalid());
        }
        let media = matches!(argv[index].as_str(), "--image" | "--video" | "--audio");
        if !media {
            index += 2;
            continue;
        }
        index += 1;
        let name = Path::new(&argv[index]);
        if name.components().count() != 1
            || !matches!(
                name.components().next(),
                Some(std::path::Component::Normal(_))
            )
        {
            return Err(invalid());
        }
        let absolute = workspace.join(name);
        if verify_files {
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&absolute)
                .map_err(|_| invalid())?;
            let metadata = file.metadata().map_err(|_| invalid())?;
            if !metadata.is_file()
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.nlink() != 1
                || metadata.mode() & 0o077 != 0
                || fs::canonicalize(&absolute).map_err(|_| invalid())? != absolute
            {
                return Err(invalid());
            }
        }
        result[index] = absolute.to_str().ok_or_else(invalid)?.to_owned();
        count += 1;
        index += 1;
    }
    if count == 0 {
        return Err(invalid());
    }
    Ok(result)
}

pub(super) async fn recover(
    codec: &DreaminaCliSubmitCodecV1,
    call: &ProviderSubmitDriverCall<DreaminaSubmitPayloadV1>,
    evidence: Arc<GatedCliReceiptEvidence>,
) -> Result<Option<PendingOperation>, ProviderFailure> {
    if !codec
        .binding
        .matches_submit(call.intent(), call.execution_context())
        || evidence.execution_binding_sha256()
            != call.execution_context().execution_binding_sha256()
        || evidence.command().executable_sha256() != hex::encode(codec.policy.executable_sha256())
        || evidence.command().environment().get("HOME").map(Path::new)
            != Some(codec.policy.account_home_path())
    {
        return Err(invalid());
    }
    let request =
        parse_submit_command(call.command().canonical_payload()).map_err(|_| invalid())?;
    // Other workflows have no verified native log contract yet. Never guess an
    // acceptance from a cache row or by selecting the newest same-prompt task.
    let DreaminaSubmitRequestV1::MultimodalToVideo(multimodal) = &request else {
        return Ok(None);
    };
    let expected = multimodal
        .to_argv()
        .iter()
        .map(|arg| arg.to_str().map(str::to_owned).ok_or_else(invalid))
        .collect::<Result<Vec<_>, _>>()?;
    if evidence.command().working_directory().parent() != Some(codec.policy.workspace_root_path())
        || absolute_media_arguments(&expected, evidence.command().working_directory(), false)?
            != evidence.command().arguments()
    {
        return Err(invalid());
    }
    // A prior invocation may have persisted acceptance then crashed after
    // staging cleanup but before the outer Accepted journal. Replay its actual
    // log+query source first, without requiring the removed staging directory.
    if let Some(pending) = replay_accepted(call, &evidence, multimodal.prompt())? {
        return Ok(Some(pending));
    }

    let source_bytes = if let Some(bytes) = evidence
        .read(SOURCE_FILE, MAX_SOURCE_BYTES)
        .map_err(|_| invalid())?
    {
        bytes
    } else {
        let workspace = WorkingDirectory::new_private(evidence.command().working_directory())
            .map_err(|_| invalid())?;
        // Rebuild without executing. The policy hashes every original staged
        // input before any native ID is selected from the account log.
        let rebuilt = codec.normal_command(
            call.intent(),
            call.execution_context(),
            call.command(),
            &workspace,
        )?;
        if &rebuilt != evidence.command() {
            return Err(invalid());
        }
        let owned = Arc::clone(&evidence);
        let home = codec.policy.account_home_path().to_owned();
        let source = tokio::task::spawn_blocking(move || select_source(&home, &owned))
            .await
            .map_err(|_| invalid())??;
        let Some(source) = source else {
            tracing::warn!(submission_id = %call.intent().submission_id, error_code = "dreamina_submit_acceptance_source_missing", "submit receipt recovery has no unique acceptance evidence; original inputs retained");
            return Ok(None);
        };
        let bytes = serde_json::to_vec(&source).map_err(|_| invalid())?;
        evidence
            .publish(SOURCE_FILE, &bytes)
            .map_err(|_| invalid())?;
        bytes
    };
    let source: Source = serde_json::from_slice(&source_bytes).map_err(|_| invalid())?;
    validate_source(&source, &evidence)?;
    let root =
        WorkingDirectory::new_private(codec.policy.workspace_root_path()).map_err(|_| invalid())?;
    let workspace =
        RecoverableAttemptWorkspace::new(&root, ".receipt-query-").map_err(|_| invalid())?;
    let query_key = Uuid::new_v4().simple().to_string();
    let attempt = workspace
        .open_or_create(&query_key)
        .map_err(|_| invalid())?;
    let policy = codec
        .policy
        .query_policy(QUERY_TIMEOUT, QUERY_GRACE)
        .map_err(|_| invalid())?;
    let query = QueryResultRequestV1::query_only(&source.remote_operation_id, attempt.path())
        .map_err(|_| invalid())?;
    let command = policy.command_spec(&query).map_err(|_| invalid())?;
    let credential_guard = call.guard_credentials().await.map_err(|_| awaiting())?;
    let started_at_ms = now_ms()?;
    let result = TokioProcessBackend
        .execute_process(&command, &mut NoopSpawnObserver)
        .await;
    drop(credential_guard);
    // Only temporary files from this query are removed; original submit
    // staging remains until the accepted marker and outer journal are durable.
    let _ = workspace.remove(&query_key);
    let completion = match result {
        Ok(value) => value,
        Err(_) => {
            let audit = serde_json::to_vec(&json!({"gate_request_sha256": evidence.request_sha256(), "remote_operation_id": source.remote_operation_id, "started_at_ms": started_at_ms, "finished_at_ms": now_ms()?, "error_code": "dreamina_receipt_query_process_failed", "query_only": true})).map_err(|_| invalid())?;
            evidence
                .publish(
                    &format!("process-receipt-recovery-query-error-{query_key}.json"),
                    &audit,
                )
                .map_err(|_| invalid())?;
            tracing::warn!(submission_id = %call.intent().submission_id, cause = "dreamina_receipt_query_process_failed", "same-ID receipt query will be observed again within the existing submit deadline");
            return Err(awaiting());
        }
    };
    let observed = QueryEvidence {
        gate_request_sha256: evidence.request_sha256().into(),
        remote_operation_id: source.remote_operation_id.clone(),
        started_at_ms,
        finished_at_ms: now_ms()?,
        exit_code: completion.status.code(),
        stdout: completion.stdout.bytes().to_vec(),
        stderr: completion.stderr.bytes().to_vec(),
        stdout_truncated: completion.stdout.is_truncated(),
        stderr_truncated: completion.stderr.is_truncated(),
        query_only: true,
    };
    let query_bytes = serde_json::to_vec(&observed).map_err(|_| invalid())?;
    let query_file = format!("process-receipt-recovery-query-{query_key}.json");
    evidence
        .publish(&query_file, &query_bytes)
        .map_err(|_| invalid())?;
    if validate_query(&observed, &source, &evidence, multimodal.prompt()).is_err() {
        tracing::warn!(submission_id = %call.intent().submission_id, cause = "dreamina_receipt_query_not_confirmed", "same-ID query has not confirmed the native acceptance; immutable observation retained");
        return Err(awaiting());
    }
    let accepted = Accepted {
        schema_version: 1,
        evidence_mode: MODE.into(),
        gate_request_sha256: evidence.request_sha256().into(),
        source_sha256: digest(&source_bytes),
        query_file,
        query_sha256: digest(&query_bytes),
    };
    evidence
        .publish(
            ACCEPTED_FILE,
            &serde_json::to_vec(&accepted).map_err(|_| invalid())?,
        )
        .map_err(|_| invalid())?;
    tracing::info!(submission_id = %call.intent().submission_id, "missing submit receipt recovered from same-attempt native acceptance and actual query evidence");
    Ok(Some(pending(
        call.intent().submission_id,
        &source.remote_operation_id,
    )?))
}

fn replay_accepted(
    call: &ProviderSubmitDriverCall<DreaminaSubmitPayloadV1>,
    evidence: &GatedCliReceiptEvidence,
    prompt: &str,
) -> Result<Option<PendingOperation>, ProviderFailure> {
    let Some(bytes) = evidence
        .read(ACCEPTED_FILE, MAX_SOURCE_BYTES)
        .map_err(|_| invalid())?
    else {
        return Ok(None);
    };
    let accepted: Accepted = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    let source_bytes = evidence
        .read(SOURCE_FILE, MAX_SOURCE_BYTES)
        .map_err(|_| invalid())?
        .ok_or_else(invalid)?;
    let query_bytes = evidence
        .read(&accepted.query_file, MAX_SOURCE_BYTES)
        .map_err(|_| invalid())?
        .ok_or_else(invalid)?;
    if accepted.schema_version != 1
        || accepted.evidence_mode != MODE
        || accepted.gate_request_sha256 != evidence.request_sha256()
        || accepted.source_sha256 != digest(&source_bytes)
        || accepted.query_sha256 != digest(&query_bytes)
    {
        return Err(invalid());
    }
    let source = serde_json::from_slice(&source_bytes).map_err(|_| invalid())?;
    let query = serde_json::from_slice(&query_bytes).map_err(|_| invalid())?;
    validate_source(&source, evidence)?;
    validate_query(&query, &source, evidence, prompt)?;
    Ok(Some(pending(
        call.intent().submission_id,
        &source.remote_operation_id,
    )?))
}

fn pending(submission_id: Uuid, remote: &str) -> Result<PendingOperation, ProviderFailure> {
    Ok(PendingOperation::new(
        RemoteOperationRef::new(PROVIDER_ID, submission_id.to_string(), remote)
            .map_err(|_| invalid())?,
        None,
        Some(DEFAULT_POLL_AFTER_MS),
    ))
}

fn validate_query(
    query: &QueryEvidence,
    source: &Source,
    evidence: &GatedCliReceiptEvidence,
    prompt: &str,
) -> Result<(), ProviderFailure> {
    if query.gate_request_sha256 != evidence.request_sha256()
        || query.remote_operation_id != source.remote_operation_id
        || !query.query_only
        || query.started_at_ms < evidence.finished_at_ms()
        || query.finished_at_ms < query.started_at_ms
        || query.exit_code != Some(0)
        || query.stdout_truncated
        || query.stderr_truncated
    {
        return Err(invalid());
    }
    validate_query_receipt(&query.stdout, &source.remote_operation_id, prompt)
}

fn validate_query_receipt(
    stdout: &[u8],
    remote: &str,
    prompt: &str,
) -> Result<(), ProviderFailure> {
    let parsed = parse_query_receipt(stdout).map_err(|_| invalid())?;
    let raw: Value = serde_json::from_slice(stdout).map_err(|_| invalid())?;
    if parsed.submit_id() != remote || raw["prompt"].as_str() != Some(prompt)
        // Native Failed can still be a local cache failure. Preserve the
        // accepted source and query only this ID again rather than publishing
        // an unproven remote failure or authorizing a new generation.
        || !matches!(parsed.status(), DreaminaQueryStatusV1::Querying | DreaminaQueryStatusV1::Success)
    {
        return Err(invalid());
    }
    Ok(())
}

fn select_source(
    home: &Path,
    evidence: &GatedCliReceiptEvidence,
) -> Result<Option<Source>, ProviderFailure> {
    let offset = local_offset(evidence.started_at_ms())?;
    if local_offset(evidence.finished_at_ms())? != offset
        || evidence.finished_at_ms() - evidence.started_at_ms() > 2 * 60 * 60 * 1000
    {
        return Err(invalid());
    }
    let logs = native_logs(
        home,
        evidence.started_at_ms(),
        evidence.finished_at_ms(),
        offset,
    )?;
    select_log_source(
        &logs,
        evidence.command().arguments(),
        evidence.started_at_ms(),
        evidence.finished_at_ms(),
        offset,
    )
    .map(|candidate| {
        candidate.map(
            |(remote_operation_id, trace_id, started_line, accepted_line)| Source {
                schema_version: 1,
                evidence_mode: MODE.into(),
                gate_request_sha256: evidence.request_sha256().into(),
                gate_terminal_sha256: evidence.terminal_sha256().into(),
                execution_binding_sha256: evidence.execution_binding_sha256().into(),
                launch_nonce: evidence.launch_nonce(),
                remote_operation_id,
                trace_id,
                utc_offset_seconds: offset,
                started_line,
                accepted_line,
            },
        )
    })
}

fn validate_source(
    source: &Source,
    evidence: &GatedCliReceiptEvidence,
) -> Result<(), ProviderFailure> {
    if source.schema_version != 1
        || source.evidence_mode != MODE
        || source.gate_request_sha256 != evidence.request_sha256()
        || source.gate_terminal_sha256 != evidence.terminal_sha256()
        || source.execution_binding_sha256 != evidence.execution_binding_sha256()
        || source.launch_nonce != evidence.launch_nonce()
    {
        return Err(invalid());
    }
    let logs = format!("{}\n{}\n", source.started_line, source.accepted_line);
    let Some((remote, trace, _, _)) = select_log_source(
        &logs,
        evidence.command().arguments(),
        evidence.started_at_ms(),
        evidence.finished_at_ms(),
        source.utc_offset_seconds,
    )?
    else {
        return Err(invalid());
    };
    if remote != source.remote_operation_id || trace != source.trace_id {
        return Err(invalid());
    }
    Ok(())
}

// Keep only the two exact raw lines; no unrelated account log/prompt is copied.
fn select_log_source(
    log: &str,
    argv: &[String],
    started: i64,
    finished: i64,
    offset: i32,
) -> Result<Option<(String, String, String, String)>, ProviderFailure> {
    if argv.first().map(String::as_str) != Some("multimodal2video") || started > finished {
        return Err(invalid());
    }
    let mut starts = Vec::new();
    let mut accepted = Vec::new();
    for line in log.lines().filter(|line| line.contains("[SubmitTask]")) {
        let Ok(at) = log_millis(line, offset) else {
            continue;
        };
        if at < started || at > finished {
            continue;
        }
        let Some((prefix, input)) = line.split_once(" input=") else {
            continue;
        };
        let Ok(input) = serde_json::from_str::<Value>(input) else {
            continue;
        };
        if !input_matches(&input, argv) {
            continue;
        }
        let fields = prefix.split_whitespace().collect::<Vec<_>>();
        let trace = native_log_trace(prefix)?.to_owned();
        if prefix.contains("[SubmitTask] start submit generation task") {
            starts.push((at, trace, input, line.to_owned()));
        } else if prefix.contains("[SubmitTask] submit generation task finished") {
            let remote = field(&fields, "submit_id=").ok_or_else(invalid)?;
            let status = field(&fields, "task_status=").ok_or_else(invalid)?;
            let remote_trace = field(&fields, "remote_logid=").ok_or_else(invalid)?;
            if Uuid::parse_str(remote).map_or(true, |id| id.is_nil())
                || !matches!(status, "querying" | "success")
                || remote_trace.is_empty()
                || remote_trace == "-"
            {
                return Err(invalid());
            }
            accepted.push((at, trace, input, line.to_owned(), remote.to_owned()));
        }
    }
    if starts.is_empty() && accepted.is_empty() {
        return Ok(None);
    }
    if starts.len() != 1 || accepted.len() != 1 {
        return Err(invalid());
    }
    let (start_at, start_trace, input, first) = starts.pop().unwrap();
    let (end_at, end_trace, last_input, last, remote) = accepted.pop().unwrap();
    if start_at > end_at || start_trace != end_trace || input != last_input {
        return Err(invalid());
    }
    Ok(Some((remote, start_trace, first, last)))
}

fn native_log_trace(prefix: &str) -> Result<&str, ProviderFailure> {
    // CLI 1.4.21 may omit the host field. Bind the unique logger metadata
    // anchor instead of a column, and never search the task message/input.
    let (metadata, _) = prefix.split_once("[SubmitTask]").ok_or_else(invalid)?;
    let fields = metadata.split_whitespace().collect::<Vec<_>>();
    let mut anchors = fields
        .windows(5)
        .filter(|parts| parts[0] == "-" && parts[2] == "default" && parts[3] == "-");
    let parts = anchors.next().ok_or_else(invalid)?;
    let trace = parts[1];
    if anchors.next().is_some()
        || parts[4].is_empty()
        || !parts[4].bytes().all(|byte| byte.is_ascii_digit())
        || trace.len() != 33
        || !trace.as_bytes()[..14].iter().all(u8::is_ascii_digit)
        || !trace
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'A'..=b'F').contains(&byte))
    {
        return Err(invalid());
    }
    Ok(trace)
}

fn field<'a>(fields: &[&'a str], prefix: &str) -> Option<&'a str> {
    let mut found = fields.iter().filter_map(|part| part.strip_prefix(prefix));
    let value = found.next()?;
    if found.next().is_some() {
        None
    } else {
        Some(value)
    }
}

fn input_matches(input: &Value, argv: &[String]) -> bool {
    let mut arguments = BTreeMap::<&str, Vec<&str>>::new();
    let mut index = 1;
    while index < argv.len() {
        let flag = argv[index].as_str();
        if flag == "--poll=0" {
            index += 1;
            continue;
        }
        if !matches!(
            flag,
            "--image"
                | "--video"
                | "--audio"
                | "--prompt"
                | "--model_version"
                | "--ratio"
                | "--duration"
                | "--video_resolution"
        ) || index + 1 >= argv.len()
        {
            return false;
        }
        arguments.entry(flag).or_default().push(&argv[index + 1]);
        index += 2;
    }
    for (name, flag) in [
        ("image_paths", "--image"),
        ("video_paths", "--video"),
        ("audio_paths", "--audio"),
    ] {
        let expected = arguments.get(flag).map(Vec::as_slice).unwrap_or_default();
        let Some(actual) = input.get(name) else {
            return false;
        };
        if *actual != json!(expected) && !(actual.is_null() && expected.is_empty()) {
            return false;
        }
        if expected.iter().any(|path| !Path::new(path).is_absolute()) {
            return false;
        }
    }
    for (name, flag) in [
        ("prompt", "--prompt"),
        ("model_version", "--model_version"),
        ("ratio", "--ratio"),
        ("video_resolution", "--video_resolution"),
    ] {
        let expected = arguments.get(flag).map(Vec::as_slice).unwrap_or_default();
        if flag == "--prompt"
            && expected.is_empty()
            && input.get(name).and_then(Value::as_str) == Some("")
        {
            continue;
        }
        if expected.len() != 1 || input.get(name).and_then(Value::as_str) != Some(expected[0]) {
            return false;
        }
    }
    let duration = arguments
        .get("--duration")
        .map(Vec::as_slice)
        .unwrap_or_default();
    duration.len() == 1
        && input["duration"]
            .as_u64()
            .is_some_and(|actual| Some(actual) == duration[0].parse::<u64>().ok())
        && input["workspace"].is_i64()
}

fn native_logs(
    home: &Path,
    started: i64,
    finished: i64,
    offset: i32,
) -> Result<String, ProviderFailure> {
    let mut directory = rfs::open(
        home,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| invalid())?;
    for name in [".dreamina_cli", "logs"] {
        directory = rfs::openat(
            &directory,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| invalid())?;
        let stat = rfs::fstat(&directory).map_err(|_| invalid())?;
        if stat.st_uid != unsafe { libc::geteuid() } || stat.st_mode & 0o022 != 0 {
            return Err(invalid());
        }
    }
    let first = (started + i64::from(offset) * 1000).div_euclid(3_600_000);
    let last = (finished + i64::from(offset) * 1000).div_euclid(3_600_000);
    let mut result = String::new();
    for hour in first..=last {
        let at = OffsetDateTime::from_unix_timestamp(hour * 3600).map_err(|_| invalid())?;
        let name = format!(
            "dreamina.log.{:04}-{:02}-{:02}_{:02}",
            at.year(),
            u8::from(at.month()),
            at.day(),
            at.hour()
        );
        let fd = match rfs::openat(
            &directory,
            &name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        ) {
            Ok(value) => value,
            Err(rustix::io::Errno::NOENT) => continue,
            Err(_) => return Err(invalid()),
        };
        let mut file = File::from(fd);
        let meta = file.metadata().map_err(|_| invalid())?;
        if !meta.is_file()
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.nlink() != 1
            || meta.mode() & 0o022 != 0
        {
            return Err(invalid());
        }
        // Account logs are append-only and may exceed the read budget. Read
        // only their bounded tail, never reject a healthy busy account merely
        // because earlier unrelated tasks made this hourly file large.
        let offset = meta.len().saturating_sub(MAX_LOG_BYTES);
        file.seek(SeekFrom::Start(offset)).map_err(|_| invalid())?;
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(MAX_LOG_BYTES)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid())?;
        if result.len() + bytes.len() > 3 * MAX_LOG_BYTES as usize {
            return Err(invalid());
        }
        if offset > 0 {
            // Drop the partial first line after seeking into the hourly log.
            let start = bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |at| at + 1);
            bytes.drain(..start);
        }
        result.push_str(std::str::from_utf8(&bytes).map_err(|_| invalid())?);
        result.push('\n');
    }
    Ok(result)
}

fn local_offset(millis: i64) -> Result<i32, ProviderFailure> {
    let seconds = libc::time_t::try_from(millis.div_euclid(1000)).map_err(|_| invalid())?;
    let mut local = std::mem::MaybeUninit::<libc::tm>::uninit();
    if unsafe { libc::localtime_r(&seconds, local.as_mut_ptr()) }.is_null() {
        return Err(invalid());
    }
    i32::try_from(unsafe { local.assume_init() }.tm_gmtoff).map_err(|_| invalid())
}

fn log_millis(line: &str, offset: i32) -> Result<i64, ProviderFailure> {
    let fields = line.split_whitespace().take(3).collect::<Vec<_>>();
    if fields.len() != 3 || !matches!(fields[0], "Info" | "Warn" | "Error") {
        return Err(invalid());
    }
    let date = fields[1]
        .split('-')
        .map(str::parse::<u16>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid())?;
    let clock = fields[2]
        .split([':', ','])
        .map(str::parse::<u16>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid())?;
    if date.len() != 3 || clock.len() != 4 {
        return Err(invalid());
    }
    let date = Date::from_calendar_date(
        i32::from(date[0]),
        Month::try_from(u8::try_from(date[1]).map_err(|_| invalid())?).map_err(|_| invalid())?,
        u8::try_from(date[2]).map_err(|_| invalid())?,
    )
    .map_err(|_| invalid())?;
    let clock = Time::from_hms_milli(
        u8::try_from(clock[0]).map_err(|_| invalid())?,
        u8::try_from(clock[1]).map_err(|_| invalid())?,
        u8::try_from(clock[2]).map_err(|_| invalid())?,
        clock[3],
    )
    .map_err(|_| invalid())?;
    i64::try_from(
        PrimitiveDateTime::new(date, clock)
            .assume_utc()
            .unix_timestamp_nanos()
            / 1_000_000
            - i128::from(offset) * 1000,
    )
    .map_err(|_| invalid())
}
fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn now_ms() -> Result<i64, ProviderFailure> {
    i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| invalid())
}
fn invalid() -> ProviderFailure {
    unknown_failure("dreamina_submit_recovery_evidence_invalid")
}
fn awaiting() -> ProviderFailure {
    unknown_failure(crate::provider_tasks::SUBMIT_RECEIPT_RECOVERY_PENDING)
}

#[cfg(test)]
mod tests;
