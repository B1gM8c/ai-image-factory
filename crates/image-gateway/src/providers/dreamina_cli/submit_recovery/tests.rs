use super::*;
use std::os::unix::fs::{PermissionsExt, symlink};
use tempfile::TempDir;

const REMOTE: &str = "7eabf607-22b0-4e1d-b693-d60569e51edf";
fn argv(workspace: &str) -> Vec<String> {
    [
        "multimodal2video",
        "--image",
        &format!("{workspace}/input-0.png"),
        "--prompt",
        "same prompt\r\nexact",
        "--model_version",
        "seedance2.0fast_vip",
        "--ratio",
        "16:9",
        "--duration",
        "6",
        "--video_resolution",
        "720p",
        "--poll=0",
    ]
    .map(str::to_owned)
    .to_vec()
}
fn input(argv: &[String]) -> Value {
    json!({"image_paths": [argv[2]], "video_paths": null, "audio_paths": null, "prompt": argv[4], "model_version": argv[6], "ratio": argv[8], "duration": 6, "video_resolution": argv[12], "workspace": 0})
}
fn log(value: &Value, trace: &str, remote: &str) -> String {
    format!(
        "Info 2026-10-27 22:26:05,901 v1(7) gen.go:181 host - {trace} default - 0 [SubmitTask] start submit generation task input={value}\nInfo 2026-10-27 22:26:09,701 v1(7) gen.go:200 host - {trace} default - 0 [SubmitTask] submit generation task finished submit_id={remote} task_status=querying remote_logid=upstream-trace input={value}\n"
    )
}
fn window() -> (i64, i64) {
    (
        log_millis("Info 2026-10-27 22:26:05,000", 28800).unwrap(),
        log_millis("Info 2026-10-27 22:26:10,000", 28800).unwrap(),
    )
}

#[test]
fn selects_the_same_absolute_attempt_among_identical_parallel_prompts() {
    let a = argv("/private/attempt-a");
    let b = argv("/private/attempt-b");
    let (start, end) = window();
    let source = log(
        &input(&b),
        "other-trace",
        "8137cf73-6698-4533-bf61-3c743aa536ff",
    ) + &log(&input(&a), "same-trace", REMOTE);
    let matched = select_log_source(&source, &a, start, end, 28800)
        .unwrap()
        .unwrap();
    assert_eq!(matched.0, REMOTE);
    assert_eq!(matched.1, "same-trace");
    assert!(!matched.2.contains("attempt-b"));
}

#[test]
fn rejects_ambiguous_candidates_and_cross_trace_pairs() {
    let a = argv("/private/attempt-a");
    let (start, end) = window();
    let source = log(&input(&a), "same-trace", REMOTE);
    assert!(select_log_source(&(source.clone() + &source), &a, start, end, 28800).is_err());
    let crossed = source.replacen(
        "gen.go:200 host - same-trace",
        "gen.go:200 host - other-trace",
        1,
    );
    assert!(select_log_source(&crossed, &a, start, end, 28800).is_err());
}

#[test]
fn relative_historical_arguments_or_missing_null_fields_do_not_match() {
    let mut a = argv("/private/attempt-a");
    let original = input(&a);
    a[2] = "input-0.png".into();
    assert!(!input_matches(&original, &a));
    let mut relative = original.clone();
    relative["image_paths"] = json!(["input-0.png"]);
    assert!(!input_matches(&relative, &a));
    let a = argv("/private/attempt-a");
    let mut missing = original.clone();
    missing.as_object_mut().unwrap().remove("audio_paths");
    assert!(!input_matches(&missing, &a));
    let mut changed = original.clone();
    changed["duration"] = json!(8);
    assert!(!input_matches(&changed, &a));
    changed = original.clone();
    changed["prompt"] = json!("same prompt\nexact");
    assert!(!input_matches(&changed, &a));
}

#[test]
fn failed_local_cache_or_outside_window_cannot_prove_acceptance() {
    let a = argv("/private/attempt-a");
    let (start, end) = window();
    let source = log(&input(&a), "same-trace", REMOTE);
    assert!(
        select_log_source(&source, &a, end + 1, end + 1000, 28800)
            .unwrap()
            .is_none()
    );
    for altered in [
        source.replace("task_status=querying", "task_status=fail"),
        source.replace("remote_logid=upstream-trace", "remote_logid="),
        source.replace(REMOTE, "not-a-native-id"),
    ] {
        assert!(select_log_source(&altered, &a, start, end, 28800).is_err());
    }
}

#[test]
fn normal_projection_uses_private_absolute_paths_and_preserves_input_argv() {
    let root = TempDir::new().unwrap();
    let root = fs::canonicalize(root.path()).unwrap();
    let media = root.join("input-0.png");
    fs::write(&media, b"frozen fixture media").unwrap();
    fs::set_permissions(&media, fs::Permissions::from_mode(0o600)).unwrap();
    let mut relative = argv("/unused");
    relative[2] = "input-0.png".into();
    let unchanged = relative.clone();
    let projected = absolute_media_arguments(&relative, &root, true).unwrap();
    assert_eq!(relative, unchanged); // Operator/historical argv is not mutated.
    assert_eq!(projected[2], media.to_str().unwrap());
    for index in 0..relative.len() {
        if index != 2 {
            assert_eq!(projected[index], relative[index]);
        }
    }
    fs::remove_file(&media).unwrap();
    symlink(root.join("outside"), &media).unwrap();
    assert!(absolute_media_arguments(&relative, &root, true).is_err());
}

#[test]
fn bounded_log_reader_rejects_symlink_and_reads_only_bound_hour() {
    let root = TempDir::new().unwrap();
    let home = fs::canonicalize(root.path()).unwrap();
    let dir = home.join(".dreamina_cli/logs");
    fs::create_dir_all(&dir).unwrap();
    let a = argv("/private/attempt-a");
    let (start, end) = window();
    let path = dir.join("dreamina.log.2026-10-27_22");
    fs::write(&path, log(&input(&a), "same-trace", REMOTE)).unwrap();
    assert!(
        native_logs(&home, start, end, 28800)
            .unwrap()
            .contains(REMOTE)
    );
    fs::write(
        dir.join("dreamina.log.2026-10-26_22"),
        b"unrelated account log",
    )
    .unwrap();
    assert!(
        !native_logs(&home, start, end, 28800)
            .unwrap()
            .contains("unrelated")
    );
    let mut large = vec![b'x'; MAX_LOG_BYTES as usize + 100];
    large.push(b'\n');
    large.extend(log(&input(&a), "same-trace", REMOTE).as_bytes());
    fs::write(&path, large).unwrap();
    let tail = native_logs(&home, start, end, 28800).unwrap();
    assert!(
        select_log_source(&tail, &a, start, end, 28800)
            .unwrap()
            .is_some()
    );
    fs::rename(&path, dir.join("original")).unwrap();
    symlink(dir.join("original"), &path).unwrap();
    assert!(native_logs(&home, start, end, 28800).is_err());
}

#[test]
fn timestamp_window_uses_explicit_host_offset() {
    assert_eq!(
        log_millis("Info 2026-10-27 22:26:05,901", 28800).unwrap() + 28_800_000,
        log_millis("Info 2026-10-27 22:26:05,901", 0).unwrap()
    );
}

#[test]
fn query_validation_requires_native_id_exact_prompt_and_positive_status() {
    let good = serde_json::to_vec(
        &json!({"submit_id": REMOTE, "gen_status": "querying", "prompt": "fixture\r\nexact"}),
    )
    .unwrap();
    validate_query_receipt(&good, REMOTE, "fixture\r\nexact").unwrap();
    for value in [
        json!({"submit_id": "other-native-id", "gen_status": "querying", "prompt": "fixture\r\nexact"}),
        json!({"submit_id": REMOTE, "gen_status": "querying", "prompt": "fixture\nexact"}),
        json!({"submit_id": REMOTE, "gen_status": "fail", "prompt": "fixture\r\nexact", "fail_reason": "local upload failed"}),
        json!({"submit_id": REMOTE, "gen_status": "unknown", "prompt": "fixture\r\nexact"}),
    ] {
        assert!(
            validate_query_receipt(
                &serde_json::to_vec(&value).unwrap(),
                REMOTE,
                "fixture\r\nexact"
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn recovery_query_policy_runs_only_the_original_id_and_captures_actual_stdout() {
    let root = TempDir::new().unwrap();
    let workspace = root.path().join("work");
    let home = root.path().join("home");
    let query_dir = workspace.join("query");
    for directory in [&workspace, &home] {
        fs::create_dir(directory).unwrap();
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::create_dir(&query_dir).unwrap();
    fs::set_permissions(&query_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let executable = root.path().join("fixture-cli");
    let response = serde_json::to_string(
        &json!({"submit_id": REMOTE, "gen_status": "querying", "prompt": "fixture"}),
    )
    .unwrap();
    let script = format!(
        "#!/bin/sh\n[ \"$#\" -eq 3 ] && [ \"$1\" = query_result ] && [ \"$2\" = --submit_id ] && [ \"$3\" = {REMOTE} ] || exit 9\nprintf 'query-only\\n' >> \"$HOME/query-calls\"\nprintf '%s' '{response}'\n"
    );
    fs::write(&executable, &script).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).unwrap();
    let policy = DreaminaCliPolicyV1::new(
        &executable,
        Sha256::digest(script.as_bytes()).into(),
        WorkingDirectory::new_private(&workspace).unwrap(),
        WorkingDirectory::new_private(&home).unwrap(),
        Duration::from_secs(5),
        Duration::from_millis(100),
    )
    .unwrap();
    let query_policy = policy
        .query_policy(Duration::from_secs(3), Duration::from_millis(100))
        .unwrap();
    let command = query_policy
        .command_spec(
            &QueryResultRequestV1::query_only(REMOTE, fs::canonicalize(&query_dir).unwrap())
                .unwrap(),
        )
        .unwrap();
    let completed = TokioProcessBackend
        .execute_process(&command, &mut NoopSpawnObserver)
        .await
        .unwrap();
    assert!(completed.status.success());
    assert!(!completed.stdout.is_truncated());
    assert_eq!(completed.stdout.bytes(), response.as_bytes());
    validate_query_receipt(completed.stdout.bytes(), REMOTE, "fixture").unwrap();
    assert_eq!(fs::read(home.join("query-calls")).unwrap(), b"query-only\n");
    assert!(
        !command
            .arguments()
            .iter()
            .any(|argument| argument == "--download_dir")
    );
}
