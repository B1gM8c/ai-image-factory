//! Explicit review of a real CLI acceptance whose captured submit stdout was empty.
//! This imports an independently captured query; it never launches a generation.
use super::*;
use serde_json::Value;
use time::{Date, Month, PrimitiveDateTime, Time, format_description::well_known::Rfc3339};

const MODE: &str = "native_log_acceptance_query_v1";
const MAX_LOG_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Review {
    schema_version: u16,
    evidence_mode: String,
    authorization_id: Uuid,
    dispatch_nonce: Uuid,
    command_hash: String,
    quote_hash: String,
    runtime_binding_sha256: String,
    gate_request_sha256: String,
    gate_terminal_sha256: String,
    source_log_sha256: String,
    source_trace_id: String,
    source_started_line_sha256: String,
    source_accepted_line_sha256: String,
    log_utc_offset_seconds: i32,
    remote_operation_id: String,
    query_stdout_sha256: String,
    execution_audit_sha256: String,
    query_stderr_sha256: String,
    capture_script_sha256: String,
    query_home_sha256: String,
    retrieved_video_sha256: String,
    retrieved_video_byte_size: u64,
    video_url_sha256: String,
    operator_reference: String,
}

/// The immutable marker identifies the new receipt's actual source. The legacy
/// DB column `submit_stdout_sha256` stores this real query receipt's digest;
/// no synthetic submit stdout is created or substituted into the old gate.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveredAcceptance {
    pub schema_version: u16,
    pub evidence_mode: String,
    pub authorization_id: Uuid,
    pub dispatch_nonce: Uuid,
    pub command_hash: String,
    pub quote_hash: String,
    pub remote_operation_id: String,
    pub query_stdout_sha256: String,
    pub reviewed_recovery_sha256: String,
}

pub async fn recover_dreamina_video_supplement_acceptance_operator(
    pool: &PgPool,
    evidence_root: &Path,
    authorization_id: Uuid,
    recovery_root: &Path,
    reviewed_recovery_sha256: &str,
) -> Result<DreaminaVideoSupplementOperatorReceipt, ImageGatewayError> {
    let (store, blobs) = stores(pool)?;
    let mut plan = store
        .load_dreamina_video_supplement(authorization_id)
        .await?;
    let directory = evidence_directory(evidence_root, authorization_id)?;
    if plan.state == "succeeded" {
        return Ok(receipt(&plan, &directory, "published", None));
    }
    if !matches!(
        plan.state.as_str(),
        "dispatching" | "uncertain" | "accepted"
    ) || store
        .is_dreamina_video_supplement_cancelled(authorization_id, plan.dispatch_nonce)
        .await?
    {
        return Err(failure());
    }
    let _lock = recovery_lock(&directory)?;
    let review_bytes = private_read(recovery_root, "review.json", MAX_PRIVATE_JSON_BYTES)?;
    if sha256_hex(&review_bytes) != reviewed_recovery_sha256 {
        return Err(failure());
    }
    let review: Review = serde_json::from_slice(&review_bytes).map_err(|_| failure())?;
    validate_review(&review, &plan)?;
    // An explicit recovery must not replace even a malformed nonempty submit
    // document. The original terminal, including its empty streams, stays intact.
    if !bound_terminal_stdout(&plan, &directory)?.is_empty()
        || private_read_optional(&directory, "submit-stdout.json", 64 * 1024)?.is_some()
    {
        return Err(failure());
    }
    let runtime = Runtime::load(pool, &plan).await?;
    if runtime.binding()? != plan.runtime_binding_json {
        return Err(failure());
    }
    let prepared: Value = serde_json::from_slice(&private_read(
        &directory,
        "plan.json",
        MAX_PRIVATE_JSON_BYTES,
    )?)
    .map_err(|_| failure())?;
    let budget: SubmitBudget =
        serde_json::from_value(prepared.get("submit_budget").cloned().ok_or_else(failure)?)
            .map_err(|_| failure())?;
    budget.validate()?;
    // Reconstruct but never execute the frozen command. This verifies every
    // staged input byte against admission's original hash before log matching.
    let (_, command) = prepare_command(pool, &plan, &runtime, blobs, budget).await?;
    let argv: Vec<String> = command
        .arguments()
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let gate_dir = directory
        .join("gate")
        .join(authorization_id.simple().to_string());
    let request_bytes = private_read(&gate_dir, "process-request.json", MAX_PRIVATE_JSON_BYTES)?;
    let terminal_bytes = private_read(&gate_dir, "process-terminal.json", MAX_PRIVATE_JSON_BYTES)?;
    let gate: Value = serde_json::from_slice(&request_bytes).map_err(|_| failure())?;
    if sha256_hex(&request_bytes) != review.gate_request_sha256
        || sha256_hex(&terminal_bytes) != review.gate_terminal_sha256
        || gate["arguments"] != json!(argv)
        || prepared["argv"] != json!(argv)
        || gate["working_directory"]
            != json!(
                command
                    .working_directory()
                    .path()
                    .to_str()
                    .ok_or_else(failure)?
            )
        || gate["environment"]["HOME"] != json!(runtime.home.path().to_str().ok_or_else(failure)?)
        || gate["executable_sha256"] != json!(hex::encode(runtime.executable_sha256))
    {
        return Err(failure());
    }
    let log = private_read(recovery_root, "source-account-log.bin", MAX_LOG_BYTES)?;
    if sha256_hex(&log) != review.source_log_sha256 {
        return Err(failure());
    }
    let started_at = file_millis(&gate_dir.join("process-exec-started.json"))?;
    let finished_at = file_millis(&gate_dir.join("process-terminal.json"))?;
    validate_log(&log, &review, &argv, started_at, finished_at)?;
    let query = private_read(recovery_root, "query-stdout.json", 64 * 1024)?;
    let audit = private_read(
        recovery_root,
        "execution-audit.json",
        MAX_PRIVATE_JSON_BYTES,
    )?;
    let stderr =
        private_read_optional(recovery_root, "query-stderr.bin", 64 * 1024)?.ok_or_else(failure)?;
    let script = private_read(
        recovery_root,
        "capture-query-only.py",
        MAX_PRIVATE_JSON_BYTES,
    )?;
    let video = private_read(recovery_root, "original-video.mp4", MAX_VIDEO_BYTES as u64)?;
    validate_query_capture(
        &review,
        &plan,
        &query,
        &audit,
        &stderr,
        &script,
        finished_at,
    )?;
    let parsed = validate_query(&query, &review.remote_operation_id)?;
    let remote_video = parsed.remote_video_result().ok_or_else(failure)?;
    if parsed.status() != DreaminaQueryStatusV1::Success
        || sha256_hex(remote_video.video_url().as_bytes()) != review.video_url_sha256
        || sha256_hex(&video) != review.retrieved_video_sha256
        || video.len() as u64 != review.retrieved_video_byte_size
    {
        return Err(failure());
    }
    probe_input(&video, MediaInputKind::Video).map_err(|_| failure())?;
    let acceptance = RecoveredAcceptance {
        schema_version: 1,
        evidence_mode: MODE.into(),
        authorization_id,
        dispatch_nonce: plan.dispatch_nonce,
        command_hash: plan.command_hash.clone(),
        quote_hash: plan.quote_hash.clone(),
        remote_operation_id: review.remote_operation_id.clone(),
        query_stdout_sha256: sha256_hex(&query),
        reviewed_recovery_sha256: reviewed_recovery_sha256.into(),
    };
    let recovered = private_subdirectory(&directory, "acceptance-recovery-source")?;
    for (name, bytes) in [
        ("review.json", review_bytes.as_slice()),
        ("source-account-log.bin", log.as_slice()),
        ("query-stdout.json", query.as_slice()),
        ("execution-audit.json", audit.as_slice()),
        ("capture-query-only.py", script.as_slice()),
        ("original-video.mp4", video.as_slice()),
    ] {
        private_write_once(&recovered, name, bytes)?;
    }
    private_write_once(&directory, "acceptance-query.json", &query)?;
    private_write_once(
        &directory,
        "acceptance-recovery.json",
        &serde_json::to_vec(&acceptance).map_err(|_| failure())?,
    )?;
    store
        .record_dreamina_video_supplement_recovered_acceptance(
            &DreaminaVideoSupplementAcceptedRequest {
                authorization_id,
                dispatch_nonce: plan.dispatch_nonce,
                remote_operation_id: review.remote_operation_id,
                submit_stdout_sha256: sha256_hex(&query),
            },
        )
        .await?;
    plan = store
        .load_dreamina_video_supplement(authorization_id)
        .await?;
    // Publication takes this same non-reentrant recovery flock. Release the
    // source-import lock before invoking its existing recovery/publish path.
    drop(_lock);
    if let Some(published) = recover_publication(&store, &plan, &directory).await? {
        return Ok(published);
    }
    // Publication still performs the normal original quote, video dimensions,
    // leaf fencing and unique hold/ledger transaction; no manual settlement.
    let audit_json: Value = serde_json::from_slice(&audit).map_err(|_| failure())?;
    let executed_at_ms = audit_time(&audit_json, "finished_at")?;
    publish_video_evidence(&store, &plan, &directory, &query, &video, executed_at_ms).await
}

fn validate_review(
    review: &Review,
    plan: &DreaminaVideoSupplementPlan,
) -> Result<(), ImageGatewayError> {
    if review.schema_version != 1
        || review.evidence_mode != MODE
        || review.authorization_id != plan.authorization_id
        || review.dispatch_nonce != plan.dispatch_nonce
        || review.command_hash != plan.command_hash
        || review.quote_hash != plan.quote_hash
        || review.runtime_binding_sha256
            != sha256_hex(&serde_json::to_vec(&plan.runtime_binding_json).map_err(|_| failure())?)
        || review.query_home_sha256
            != plan.runtime_binding_json["account_home_sha256"]
                .as_str()
                .ok_or_else(failure)?
        || review.source_trace_id.is_empty()
        || review.source_trace_id.len() > 128
        || !review
            .source_trace_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric())
        || review.operator_reference.trim().is_empty()
        || review.operator_reference.len() > 200
        || !(-14 * 60 * 60..=14 * 60 * 60).contains(&review.log_utc_offset_seconds)
        || review.retrieved_video_byte_size == 0
        || review.retrieved_video_byte_size > MAX_VIDEO_BYTES as u64
    {
        return Err(failure());
    }
    Ok(())
}

fn validate_query_capture(
    review: &Review,
    plan: &DreaminaVideoSupplementPlan,
    query: &[u8],
    audit: &[u8],
    stderr: &[u8],
    script: &[u8],
    terminal_at_ms: i64,
) -> Result<(), ImageGatewayError> {
    if sha256_hex(audit) != review.execution_audit_sha256 {
        return Err(failure());
    }
    let audit: Value = serde_json::from_slice(audit).map_err(|_| failure())?;
    let value: Value = serde_json::from_slice(query).map_err(|_| failure())?;
    let expected_prompt = plan.command_json["prompt"].as_str().ok_or_else(failure)?;
    if sha256_hex(query) != review.query_stdout_sha256
        || sha256_hex(stderr) != review.query_stderr_sha256
        || sha256_hex(script) != review.capture_script_sha256
        || audit["native_id"] != json!(review.remote_operation_id)
        || audit["argv"]
            != json!([
                plan.runtime_binding_json["cli_path"]
                    .as_str()
                    .ok_or_else(failure)?,
                "query_result",
                "--submit_id",
                &review.remote_operation_id
            ])
        || audit["executable_sha256"] != plan.runtime_binding_json["cli_sha256"]
        || audit["exit_code"] != json!(0)
        || audit["query_only"] != json!(true)
        || audit["generation_calls"] != json!(0)
        || audit["stdout_bytes"] != json!(query.len())
        || audit["stdout_sha256"] != json!(sha256_hex(query))
        || audit["stderr_bytes"] != json!(stderr.len())
        || audit["stderr_sha256"] != json!(sha256_hex(stderr))
        || value["prompt"].as_str() != Some(expected_prompt)
        || audit_time(&audit, "started_at")? < terminal_at_ms
        || audit_time(&audit, "finished_at")? < audit_time(&audit, "started_at")?
    {
        return Err(failure());
    }
    Ok(())
}

fn audit_time(value: &Value, name: &str) -> Result<i64, ImageGatewayError> {
    let timestamp =
        time::OffsetDateTime::parse(value[name].as_str().ok_or_else(failure)?, &Rfc3339)
            .map_err(|_| failure())?;
    i64::try_from(timestamp.unix_timestamp_nanos() / 1_000_000).map_err(|_| failure())
}

fn file_millis(path: &Path) -> Result<i64, ImageGatewayError> {
    let meta = fs::symlink_metadata(path).map_err(|_| failure())?;
    meta.mtime()
        .checked_mul(1000)
        .and_then(|v| v.checked_add(meta.mtime_nsec() / 1_000_000))
        .ok_or_else(failure)
}

fn validate_log(
    bytes: &[u8],
    review: &Review,
    argv: &[String],
    started: i64,
    finished: i64,
) -> Result<(), ImageGatewayError> {
    if argv.first().map(String::as_str) != Some("multimodal2video") || started > finished {
        return Err(failure());
    }
    let log = std::str::from_utf8(bytes).map_err(|_| failure())?;
    let mut first = None;
    let mut last = None;
    for line in log.lines() {
        if !line.contains("[SubmitTask]") {
            continue;
        }
        // The source snapshot can include unrelated failures from this hour.
        // Filter their time window before requiring this call's input shape.
        let at = log_millis(line, review.log_utc_offset_seconds)?;
        if at < started || at > finished {
            continue;
        }
        let (prefix, input) = line.split_once(" input=").ok_or_else(failure)?;
        if !prefix
            .split_whitespace()
            .any(|part| part == review.source_trace_id)
        {
            return Err(failure());
        }
        let value: Value = serde_json::from_str(input).map_err(|_| failure())?;
        if prefix.contains("[SubmitTask] start submit generation task") {
            if first.replace((line, value)).is_some() {
                return Err(failure());
            }
        } else if prefix.contains("[SubmitTask] submit generation task finished") {
            if !prefix
                .split_whitespace()
                .any(|part| part == format!("submit_id={}", review.remote_operation_id))
                || last.replace((line, value)).is_some()
            {
                return Err(failure());
            }
        } else {
            return Err(failure());
        }
    }
    let (first_line, first) = first.ok_or_else(failure)?;
    let (last_line, last) = last.ok_or_else(failure)?;
    if sha256_hex(first_line.as_bytes()) != review.source_started_line_sha256
        || sha256_hex(last_line.as_bytes()) != review.source_accepted_line_sha256
        || first != last
    {
        return Err(failure());
    }
    validate_log_input(&first, argv)
}

fn validate_log_input(input: &Value, argv: &[String]) -> Result<(), ImageGatewayError> {
    let values = |flag: &str| -> Vec<&str> {
        argv.windows(2)
            .filter(|pair| pair[0] == flag)
            .map(|pair| pair[1].as_str())
            .collect()
    };
    for (name, flag) in [
        ("image_paths", "--image"),
        ("video_paths", "--video"),
        ("audio_paths", "--audio"),
    ] {
        let expected = values(flag);
        let actual = input.get(name).ok_or_else(failure)?;
        // This native CLI logs an explicitly absent optional media list as
        // null. A missing field is different and never establishes a match.
        if *actual != json!(expected) && !(actual.is_null() && expected.is_empty()) {
            return Err(failure());
        }
    }
    for (name, flag) in [
        ("prompt", "--prompt"),
        ("model_version", "--model_version"),
        ("ratio", "--ratio"),
        ("video_resolution", "--video_resolution"),
    ] {
        let expected = values(flag);
        if expected.len() != 1 || input[name].as_str() != Some(expected[0]) {
            return Err(failure());
        }
    }
    let duration = values("--duration");
    if duration.len() != 1
        || input["duration"].as_u64() != duration[0].parse::<u64>().ok()
        || !input["workspace"].is_i64()
    {
        return Err(failure());
    }
    Ok(())
}

fn log_millis(prefix: &str, offset: i32) -> Result<i64, ImageGatewayError> {
    let mut fields = prefix.split_whitespace();
    if !matches!(fields.next(), Some("Info" | "Warn" | "Error")) {
        return Err(failure());
    }
    let date: Vec<_> = fields.next().ok_or_else(failure)?.split('-').collect();
    let clock: Vec<_> = fields
        .next()
        .ok_or_else(failure)?
        .split([':', ','])
        .collect();
    if date.len() != 3 || clock.len() != 4 {
        return Err(failure());
    }
    let number = |text: &str| text.parse::<u16>().map_err(|_| failure());
    let date = Date::from_calendar_date(
        i32::from(number(date[0])?),
        Month::try_from(u8::try_from(number(date[1])?).map_err(|_| failure())?)
            .map_err(|_| failure())?,
        u8::try_from(number(date[2])?).map_err(|_| failure())?,
    )
    .map_err(|_| failure())?;
    let clock = Time::from_hms_milli(
        u8::try_from(number(clock[0])?).map_err(|_| failure())?,
        u8::try_from(number(clock[1])?).map_err(|_| failure())?,
        u8::try_from(number(clock[2])?).map_err(|_| failure())?,
        number(clock[3])?,
    )
    .map_err(|_| failure())?;
    let at = PrimitiveDateTime::new(date, clock)
        .assume_utc()
        .unix_timestamp_nanos()
        / 1_000_000
        - i128::from(offset) * 1000;
    i64::try_from(at).map_err(|_| failure())
}

pub(super) fn recovered_receipt(
    plan: &DreaminaVideoSupplementPlan,
    directory: &Path,
) -> Result<Option<(Vec<u8>, Vec<u8>)>, ImageGatewayError> {
    let Some(marker) = private_read_optional(
        directory,
        "acceptance-recovery.json",
        MAX_PRIVATE_JSON_BYTES,
    )?
    else {
        return Ok(None);
    };
    let source: RecoveredAcceptance = serde_json::from_slice(&marker).map_err(|_| failure())?;
    let query = private_read(directory, "acceptance-query.json", 64 * 1024)?;
    let source_root = directory.join("acceptance-recovery-source");
    let reviewed = private_read(&source_root, "review.json", MAX_PRIVATE_JSON_BYTES)?;
    let review: Review = serde_json::from_slice(&reviewed).map_err(|_| failure())?;
    validate_review(&review, plan)?;
    if source.schema_version != 1
        || source.evidence_mode != MODE
        || source.authorization_id != plan.authorization_id
        || source.dispatch_nonce != plan.dispatch_nonce
        || source.command_hash != plan.command_hash
        || source.quote_hash != plan.quote_hash
        || Some(source.remote_operation_id.as_str()) != plan.remote_operation_id.as_deref()
        || source.query_stdout_sha256 != sha256_hex(&query)
        || plan.submit_stdout_sha256.as_deref() != Some(source.query_stdout_sha256.as_str())
        || sha256_hex(&reviewed) != source.reviewed_recovery_sha256
        || private_read(&source_root, "query-stdout.json", 64 * 1024)? != query
    {
        return Err(failure());
    }
    validate_query(&query, &source.remote_operation_id)?;
    Ok(Some((query, marker)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> DreaminaVideoSupplementPlan {
        DreaminaVideoSupplementPlan {
            authorization_id: Uuid::new_v4(),
            dispatch_nonce: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            output_id: Uuid::new_v4(),
            provider_account_id: Uuid::new_v4(),
            execution_profile_id: Uuid::new_v4(),
            runtime_binding_json: json!({"cli_path":"/offline/dreamina","cli_sha256":"e".repeat(64),"account_home_sha256":"d".repeat(64)}),
            command_json: json!({"prompt":"original prompt\r\nwith exact bytes"}),
            command_hash: "a".repeat(64),
            quote_hash: "b".repeat(64),
            execution_binding_sha256: "c".repeat(64),
            billable_units: 10,
            api_profile: "test".into(),
            state: "dispatching".into(),
            remote_operation_id: None,
            submit_stdout_sha256: None,
        }
    }

    fn review(plan: &DreaminaVideoSupplementPlan) -> Review {
        Review {
            schema_version: 1,
            evidence_mode: MODE.into(),
            authorization_id: plan.authorization_id,
            dispatch_nonce: plan.dispatch_nonce,
            command_hash: plan.command_hash.clone(),
            quote_hash: plan.quote_hash.clone(),
            runtime_binding_sha256: sha256_hex(
                &serde_json::to_vec(&plan.runtime_binding_json).unwrap(),
            ),
            gate_request_sha256: "1".repeat(64),
            gate_terminal_sha256: "2".repeat(64),
            source_log_sha256: "3".repeat(64),
            source_trace_id: "trace123".into(),
            source_started_line_sha256: String::new(),
            source_accepted_line_sha256: String::new(),
            log_utc_offset_seconds: 28800,
            remote_operation_id: "native-accepted-task".into(),
            query_stdout_sha256: String::new(),
            execution_audit_sha256: String::new(),
            query_stderr_sha256: sha256_hex(b""),
            capture_script_sha256: sha256_hex(b"reviewed capture source"),
            query_home_sha256: "d".repeat(64),
            retrieved_video_sha256: "f".repeat(64),
            retrieved_video_byte_size: 100,
            video_url_sha256: "4".repeat(64),
            operator_reference: "offline original acceptance review".into(),
        }
    }

    fn argv() -> Vec<String> {
        [
            "multimodal2video",
            "--image",
            "input-0.jpg",
            "--image",
            "input-1.png",
            "--prompt",
            "original prompt\r\nwith exact bytes",
            "--model_version",
            "seedance2.0fast_vip",
            "--ratio",
            "16:9",
            "--duration",
            "10",
            "--video_resolution",
            "720p",
            "--poll=0",
        ]
        .map(str::to_owned)
        .to_vec()
    }

    fn input() -> Value {
        json!({"image_paths":["input-0.jpg","input-1.png"],"video_paths":null,"audio_paths":null,"prompt":"original prompt\r\nwith exact bytes","model_version":"seedance2.0fast_vip","ratio":"16:9","duration":10,"video_resolution":"720p","workspace":0})
    }

    fn log_fixture(input: &Value, review: &mut Review) -> (String, i64, i64) {
        let start = format!(
            "Info 2026-10-27 22:26:05,901 v1(7) gen.go:181 host - trace123 default - 0 [SubmitTask] start submit generation task input={input}"
        );
        let end = format!(
            "Info 2026-10-27 22:26:09,701 v1(7) gen.go:257 host - trace123 default - 0 submit_id=native-accepted-task [SubmitTask] submit generation task finished remote_logid=upstream input={input}"
        );
        review.source_started_line_sha256 = sha256_hex(start.as_bytes());
        review.source_accepted_line_sha256 = sha256_hex(end.as_bytes());
        let lower = log_millis(&start, 28800).unwrap() - 1;
        let upper = log_millis(&end, 28800).unwrap() + 1;
        (
            format!(
                "Error 2026-10-27 22:25:00,001 v1(7) [SubmitTask] unrelated failed call with no input\n{start}\n{end}\n"
            ),
            lower,
            upper,
        )
    }

    #[test]
    fn native_log_binds_exact_prompt_parameters_media_order_and_single_call_window() {
        let plan = plan();
        let mut review = review(&plan);
        let (log, lower, upper) = log_fixture(&input(), &mut review);
        validate_review(&review, &plan).unwrap();
        validate_log(log.as_bytes(), &review, &argv(), lower, upper).unwrap();
        assert!(validate_log(log.as_bytes(), &review, &argv(), lower + 1_000, upper).is_err());
        assert!(validate_log(log.as_bytes(), &review, &argv(), lower, upper - 1_000).is_err());
        let ambiguous = log.replace("host - trace123", "host - anothertrace");
        assert!(validate_log(ambiguous.as_bytes(), &review, &argv(), lower, upper).is_err());
        let duplicate = format!("{log}{}\n", log.lines().nth(1).unwrap());
        assert!(validate_log(duplicate.as_bytes(), &review, &argv(), lower, upper).is_err());
        let wrong_id = log.replace("submit_id=native-accepted-task", "submit_id=other-native");
        assert!(validate_log(wrong_id.as_bytes(), &review, &argv(), lower, upper).is_err());
        for (name, value) in [
            ("prompt", json!("same-looking prompt with different bytes")),
            ("duration", json!(8)),
            ("model_version", json!("seedance2.0mini")),
            ("video_resolution", json!("1080p")),
            ("ratio", json!("1:1")),
            ("image_paths", json!(["input-1.png", "input-0.jpg"])),
            ("audio_paths", json!(["extra.wav"])),
        ] {
            let mut changed = input();
            changed[name] = value;
            let (changed_log, lo, hi) = log_fixture(&changed, &mut review);
            assert!(
                validate_log(changed_log.as_bytes(), &review, &argv(), lo, hi).is_err(),
                "accepted changed {name}"
            );
        }
    }

    #[test]
    fn explicit_null_optional_media_is_not_a_missing_or_nonempty_media_field() {
        validate_log_input(&input(), &argv()).unwrap();
        let mut with_empty_arrays = input();
        with_empty_arrays["video_paths"] = json!([]);
        with_empty_arrays["audio_paths"] = json!([]);
        validate_log_input(&with_empty_arrays, &argv()).unwrap();
        for name in [
            "image_paths",
            "video_paths",
            "audio_paths",
            "prompt",
            "duration",
            "workspace",
        ] {
            let mut missing = input();
            missing.as_object_mut().unwrap().remove(name);
            assert!(
                validate_log_input(&missing, &argv()).is_err(),
                "accepted absent {name}"
            );
        }
        let mut missing_images = input();
        missing_images["image_paths"] = Value::Null;
        assert!(validate_log_input(&missing_images, &argv()).is_err());
    }

    fn capture(plan: &DreaminaVideoSupplementPlan, review: &mut Review) -> (Vec<u8>, Vec<u8>) {
        let query = serde_json::to_vec(&json!({"submit_id":"native-accepted-task","prompt":plan.command_json["prompt"],"gen_status":"success","result_json":{"images":[],"videos":[{"video_url":"https://example.test/video.mp4"}]}})).unwrap();
        let audit = serde_json::to_vec(&json!({"native_id":"native-accepted-task","argv":["/offline/dreamina","query_result","--submit_id","native-accepted-task"],"executable_sha256":"e".repeat(64),"started_at":"2026-10-27T14:30:00Z","finished_at":"2026-10-27T14:30:01Z","exit_code":0,"query_only":true,"generation_calls":0,"stdout_bytes":query.len(),"stdout_sha256":sha256_hex(&query),"stderr_bytes":0,"stderr_sha256":sha256_hex(b"")})).unwrap();
        review.query_stdout_sha256 = sha256_hex(&query);
        review.execution_audit_sha256 = sha256_hex(&audit);
        (query, audit)
    }

    #[test]
    fn actual_query_requires_reviewed_audit_digest_original_prompt_and_query_only_execution() {
        let plan = plan();
        let mut review = review(&plan);
        let (query, audit) = capture(&plan, &mut review);
        validate_query_capture(
            &review,
            &plan,
            &query,
            &audit,
            b"",
            b"reviewed capture source",
            1,
        )
        .unwrap();
        validate_query(&query, &review.remote_operation_id).unwrap();
        review.execution_audit_sha256 = "0".repeat(64);
        assert!(
            validate_query_capture(
                &review,
                &plan,
                &query,
                &audit,
                b"",
                b"reviewed capture source",
                1
            )
            .is_err()
        );
        review.execution_audit_sha256 = sha256_hex(&audit);
        for (name, changed) in [
            ("generation_calls", json!(1)),
            ("query_only", json!(false)),
            ("exit_code", json!(1)),
            ("native_id", json!("other-task")),
            ("executable_sha256", json!("f".repeat(64))),
            ("stdout_bytes", json!(0)),
            ("started_at", json!("1970-01-01T00:00:00Z")),
        ] {
            let mut data: Value = serde_json::from_slice(&audit).unwrap();
            data[name] = changed;
            let bytes = serde_json::to_vec(&data).unwrap();
            review.execution_audit_sha256 = sha256_hex(&bytes);
            assert!(
                validate_query_capture(
                    &review,
                    &plan,
                    &query,
                    &bytes,
                    b"",
                    b"reviewed capture source",
                    1
                )
                .is_err(),
                "accepted altered {name}"
            );
        }
        review.execution_audit_sha256 = sha256_hex(&audit);
        for prompt in [Value::Null, json!("original prompt\nwith exact bytes")] {
            let mut value: Value = serde_json::from_slice(&query).unwrap();
            value["prompt"] = prompt;
            let bytes = serde_json::to_vec(&value).unwrap();
            let mut revised_audit: Value = serde_json::from_slice(&audit).unwrap();
            revised_audit["stdout_sha256"] = json!(sha256_hex(&bytes));
            revised_audit["stdout_bytes"] = json!(bytes.len());
            let revised_audit = serde_json::to_vec(&revised_audit).unwrap();
            review.query_stdout_sha256 = sha256_hex(&bytes);
            review.execution_audit_sha256 = sha256_hex(&revised_audit);
            assert!(
                validate_query_capture(
                    &review,
                    &plan,
                    &bytes,
                    &revised_audit,
                    b"",
                    b"reviewed capture source",
                    1
                )
                .is_err()
            );
        }
        review.query_stdout_sha256 = sha256_hex(&query);
        review.execution_audit_sha256 = sha256_hex(&audit);
        assert!(
            validate_query_capture(
                &review,
                &plan,
                &query,
                &audit,
                b"different stderr",
                b"reviewed capture source",
                1
            )
            .is_err()
        );
        assert!(
            validate_query_capture(
                &review,
                &plan,
                &query,
                &audit,
                b"",
                b"changed capture source",
                1
            )
            .is_err()
        );
    }

    #[test]
    fn wrong_authority_nonce_runtime_home_and_invalid_time_offset_are_rejected() {
        let plan = plan();
        let mut value = review(&plan);
        value.authorization_id = Uuid::new_v4();
        assert!(validate_review(&value, &plan).is_err());
        let mut value = review(&plan);
        value.dispatch_nonce = Uuid::new_v4();
        assert!(validate_review(&value, &plan).is_err());
        let mut value = review(&plan);
        value.runtime_binding_sha256 = "0".repeat(64);
        assert!(validate_review(&value, &plan).is_err());
        let mut value = review(&plan);
        value.query_home_sha256 = "0".repeat(64);
        assert!(validate_review(&value, &plan).is_err());
        let mut value = review(&plan);
        value.log_utc_offset_seconds = i32::MIN;
        assert!(validate_review(&value, &plan).is_err());
    }
}
