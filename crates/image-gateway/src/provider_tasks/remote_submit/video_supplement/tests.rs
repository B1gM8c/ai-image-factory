use super::*;
use std::io::Write;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn frozen_image_and_audio_input_rows_decode_postgres_smallint_indices() {
    let Ok(url) = env::var("TEST_DATABASE_URL") else {
        assert!(
            env::var_os("CI").is_none(),
            "TEST_DATABASE_URL must be set in CI"
        );
        return;
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    // job_input_objects.input_index is SMALLINT (migration 0007), even for
    // multimodal requests whose image and audio indices share one sequence.
    let sql = r#"
        SELECT '11111111-1111-4111-8111-111111111111'::uuid AS admission_session_id,
               input_id, role, input_index, media_type,
               'filesystem-v1'::text AS storage_backend,
               'inputs/frozen'::text AS object_key,
               repeat('a', 64) AS sha256_hex, 10::bigint AS byte_size
        FROM (VALUES
            ('22222222-2222-4222-8222-222222222222'::uuid, 'image'::text, 0::smallint, 'image/jpeg'::text),
            ('33333333-3333-4333-8333-333333333333'::uuid, 'audio'::text, 1::smallint, 'audio/mpeg'::text)
        ) AS inputs(input_id, role, input_index, media_type)
        ORDER BY input_index
    "#;
    let old_decode =
        sqlx::query_as::<_, (Uuid, Uuid, String, i32, String, String, String, String, i64)>(sql)
            .fetch_all(&pool)
            .await;
    assert!(matches!(old_decode, Err(sqlx::Error::ColumnDecode { .. })));
    let rows = sqlx::query_as::<_, InputRow>(sql)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!((rows[0].role.as_str(), rows[0].input_index), ("image", 0));
    assert_eq!((rows[1].role.as_str(), rows[1].input_index), ("audio", 1));
    pool.close().await;
}

#[test]
fn submit_budget_is_explicit_bounded_and_leaves_a_receipt_margin() {
    let budget = SubmitBudget::parse("540000", "600000").unwrap();
    assert_eq!(budget.wall_limit(), Duration::from_secs(540));
    assert_eq!(budget.launch_deadline(1_000).unwrap(), 541_000);
    assert!(budget.launch_deadline(u64::MAX).is_err());
    assert!(SubmitBudget::parse("1", "60001").is_ok());
    assert!(SubmitBudget::parse("86340000", "86400000").is_ok());
    for (wall, provider) in [
        ("", "600000"),
        ("0", "600000"),
        ("540001", "600000"),
        ("600000", "600000"),
        ("1", "60000"),
        ("1", "0"),
        ("1", "86400001"),
        ("1.5", "600000"),
        ("540000", "bad"),
        ("18446744073709551616", "600000"),
    ] {
        assert!(
            SubmitBudget::parse(wall, provider).is_err(),
            "accepted {wall}/{provider}"
        );
    }
}

#[test]
fn prepared_budget_drift_or_legacy_dispatch_is_rejected_before_any_claim() {
    let budget = SubmitBudget::parse("540000", "600000").unwrap();
    let reviewed = json!({"schema_version":2, "submit_budget": budget});
    assert_eq!(validate_prepared_budget(&reviewed, budget).unwrap(), budget);
    for current in [
        SubmitBudget::parse("60000", "600000").unwrap(),
        SubmitBudget::parse("540000", "900000").unwrap(),
    ] {
        assert!(validate_prepared_budget(&reviewed, current).is_err());
    }
    for altered in [
        json!({"schema_version":1}),
        json!({"schema_version":1,"submit_budget":budget}),
        json!({"schema_version":2,"submit_budget":{"cli_wall_timeout_ms":540000}}),
        json!({"schema_version":2,"submit_budget":{"cli_wall_timeout_ms":540000,"provider_timeout_ms":600000,"extra":1}}),
        json!({"schema_version":2,"submit_budget":{"cli_wall_timeout_ms":600000,"provider_timeout_ms":600000}}),
    ] {
        assert!(validate_prepared_budget(&altered, budget).is_err());
    }
}

#[test]
fn submit_command_and_launch_use_the_same_reviewed_budget_without_extending_poll() {
    let fixture = ProcessFixture::new("accepted");
    let budget = SubmitBudget::parse("540000", "600000").unwrap();
    let policy = DreaminaCliPolicyV1::new(
        &fixture.executable,
        fixture.executable_sha256,
        fixture.workspace.clone(),
        fixture.home.clone(),
        budget.wall_limit(),
        TERMINATION_GRACE,
    )
    .unwrap();
    let request = parse_submit_command(br#"{"operation":"text2video","schema_version":1,"prompt":"p","model_version":"seedance2.5","ratio":"16:9","duration":5,"video_resolution":"480p","poll":0}"#).unwrap();
    let command = policy.command_spec(&request).unwrap();
    let started = now_ms().unwrap();
    let launch = LaunchEvidence {
        authorization_id: fixture.plan.authorization_id,
        dispatch_nonce: fixture.plan.dispatch_nonce,
        execution_binding_sha256: supplement_binding(&fixture.plan).unwrap(),
        absolute_deadline_unix_ms: budget.launch_deadline(started).unwrap(),
    };
    assert_eq!(
        command.wall_timeout().as_millis() as u64,
        launch.absolute_deadline_unix_ms - started
    );
    assert!(
        command
            .arguments()
            .iter()
            .any(|argument| argument == "--poll=0")
    );
    assert_eq!(QUERY_WALL_LIMIT, Duration::from_secs(60));
    assert_eq!(
        fixture.plan.runtime_binding_json.as_object().unwrap().len(),
        11
    );
}

#[tokio::test]
async fn legacy_plan_still_recovers_and_polls_without_a_submit_budget() {
    let fixture = ProcessFixture::new("accepted");
    private_write_once(&fixture.directory, "plan.json", br#"{"schema_version":1}"#).unwrap();
    let store = DurableFakeStore {
        path: fixture.directory.join("claimed"),
        runtime_binding: fixture.plan.runtime_binding_json.clone(),
    };
    let stdout = fixture.dispatch(&store).await.unwrap().unwrap();
    let recovered = recover_submit_stdout(&fixture.plan, &fixture.directory).unwrap();
    assert_eq!(recovered, stdout);
    let remote = parse_receipt(&recovered).unwrap();
    fixture.poll(remote.submit_id()).await;
    assert_eq!(fixture.count("generation-count"), 1);
    assert_eq!(fixture.count("poll-count"), 1);
}

struct DurableFakeStore {
    path: PathBuf,
    runtime_binding: serde_json::Value,
}
#[async_trait::async_trait]
impl DispatchClaimStore for DurableFakeStore {
    async fn claim(
        &self,
        authorization_id: Uuid,
        nonce: Uuid,
        runtime_binding: &serde_json::Value,
    ) -> Result<bool, ImageGatewayError> {
        if runtime_binding != &self.runtime_binding {
            return Err(failure());
        }
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)
        {
            Ok(mut file) => {
                file.write_all(format!("{authorization_id}:{nonce}").as_bytes())
                    .unwrap();
                file.sync_all().unwrap();
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(_) => Err(failure()),
        }
    }
}

#[tokio::test]
async fn concurrent_dispatch_claimants_launch_exactly_once() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let store = DurableFakeStore {
        path: root.join("claimed"),
        runtime_binding: json!({}),
    };
    let calls = AtomicUsize::new(0);
    let id = Uuid::new_v4();
    let nonce = Uuid::new_v4();
    let run = || async {
        one_shot_dispatch(&store, id, nonce, &json!({}), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok(br#"{"submit_id":"replacement-1","gen_status":"querying"}"#.to_vec())
        })
        .await
        .unwrap()
    };
    let (first, second) = tokio::join!(run(), run());
    assert_eq!(
        usize::from(first.is_some()) + usize::from(second.is_some()),
        1
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn timeout_then_process_restart_cannot_generate_again() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.join("claimed");
    let id = Uuid::new_v4();
    let nonce = Uuid::new_v4();
    let generation_calls = AtomicUsize::new(0);
    {
        let store = DurableFakeStore {
            path: path.clone(),
            runtime_binding: json!({}),
        };
        let first = one_shot_dispatch(&store, id, nonce, &json!({}), || async {
            generation_calls.fetch_add(1, Ordering::SeqCst);
            // A timeout or unknown effect is never evidence authorizing another send.
            Err(failure())
        })
        .await
        .unwrap();
        assert!(first.unwrap().is_err());
    }
    let restarted_store = DurableFakeStore {
        path,
        runtime_binding: json!({}),
    };
    let second = one_shot_dispatch(&restarted_store, id, nonce, &json!({}), || async {
        generation_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    })
    .await
    .unwrap();
    assert!(second.is_none());
    assert_eq!(generation_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn accepted_restart_only_polls_saved_id_and_cannot_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.join("claimed");
    let id = Uuid::new_v4();
    let nonce = Uuid::new_v4();
    let generation_calls = AtomicUsize::new(0);
    let store = DurableFakeStore {
        path: path.clone(),
        runtime_binding: json!({}),
    };
    let raw = one_shot_dispatch(&store, id, nonce, &json!({}), || async {
        generation_calls.fetch_add(1, Ordering::SeqCst);
        Ok(br#"{"submit_id":"new-accepted-id","gen_status":"querying"}"#.to_vec())
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    private_write_once(&root, "submit-stdout.json", &raw).unwrap();
    drop(store);
    let restarted = DurableFakeStore {
        path,
        runtime_binding: json!({}),
    };
    let accepted =
        parse_receipt(&private_read(&root, "submit-stdout.json", 65536).unwrap()).unwrap();
    let query = json!({"submit_id":accepted.submit_id(),"gen_status":"querying"});
    let observed =
        validate_query(&serde_json::to_vec(&query).unwrap(), accepted.submit_id()).unwrap();
    assert_eq!(observed.submit_id(), "new-accepted-id");
    assert!(
        one_shot_dispatch(&restarted, id, nonce, &json!({}), || async {
            generation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        })
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(generation_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn query_requires_the_real_accepted_id_and_single_typed_video() {
    let valid = json!({"submit_id":"new-id","gen_status":"success","result_json":{"images":[],"videos":[{"video_url":"https://media.example.test/one.mp4"}]}});
    let bytes = serde_json::to_vec(&valid).unwrap();
    assert!(validate_query(&bytes, "new-id").is_ok());
    assert!(validate_query(&bytes, "original-unknown-id").is_err());
    for result in [
        json!({"images":[],"videos":[]}),
        json!({"images":[{"image_url":"https://media.example.test/a.png"}],"videos":[]}),
        json!({"images":[],"videos":[{"video_url":"file:///tmp/a.mp4"}]}),
        json!({"images":[],"videos":[{"video_url":"https://media.example.test/a.mp4"},{"video_url":"https://media.example.test/b.mp4"}]}),
    ] {
        let value = json!({"submit_id":"new-id","gen_status":"success","result_json":result});
        assert!(validate_query(&serde_json::to_vec(&value).unwrap(), "new-id").is_err());
    }
}

#[tokio::test]
async fn result_retrieval_has_its_own_bound_above_the_input_32_mib_default() {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let bytes = vec![0_u8; 32 * 1024 * 1024 + 1];
    let source = format!("data:video/mp4;base64,{}", STANDARD.encode(&bytes));
    let mut total = 0;
    let decoded = crate::api::video_inputs::decode_media_source(
        "video",
        &source,
        MAX_VIDEO_BYTES,
        MAX_VIDEO_BYTES,
        &["video/mp4"],
        &mut total,
    )
    .await
    .unwrap();
    assert_eq!(decoded.bytes, bytes);
    assert_eq!(total, 32 * 1024 * 1024 + 1);
    let mut total = 0;
    assert!(
        crate::api::video_inputs::decode_media_source(
            "video",
            &source,
            32 * 1024 * 1024,
            MAX_VIDEO_BYTES,
            &["video/mp4"],
            &mut total,
        )
        .await
        .is_err()
    );
}

fn frozen_runtime_binding() -> serde_json::Value {
    json!({
        "execution_profile_id": Uuid::new_v4(), "provider_account_id": Uuid::new_v4(),
        "upstream_identity_sha256": "a".repeat(64), "account_home_sha256": "b".repeat(64),
        "cli_path": "/opt/dreamina", "cli_sha256": "c".repeat(64),
        "runner_path": "/opt/runner", "runner_sha256": "d".repeat(64),
        "credential_revision": 1, "credential_ref_sha256": "e".repeat(64),
        "credential_auth_sha256": "f".repeat(64),
    })
}

#[tokio::test]
async fn runtime_drift_never_releases_generation_but_normal_credential_renewal_can_poll() {
    let temp = tempfile::tempdir().unwrap();
    let frozen = frozen_runtime_binding();
    let store = DurableFakeStore {
        path: temp.path().join("claimed"),
        runtime_binding: frozen.clone(),
    };
    let calls = AtomicUsize::new(0);
    let id = Uuid::new_v4();
    let nonce = Uuid::new_v4();
    for field in frozen.as_object().unwrap().keys() {
        let mut changed = frozen.clone();
        changed[field] = json!("changed");
        assert!(
            one_shot_dispatch(&store, id, nonce, &changed, || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            })
            .await
            .is_err()
        );
        if !field.starts_with("credential_") {
            assert!(validate_poll_binding(&frozen, &changed).is_err());
        }
    }
    let mut renewed = frozen.clone();
    renewed["credential_revision"] = json!(2);
    renewed["credential_ref_sha256"] = json!("1".repeat(64));
    renewed["credential_auth_sha256"] = json!("2".repeat(64));
    assert!(validate_poll_binding(&frozen, &renewed).is_ok());
    assert!(validate_poll_binding(&json!({}), &renewed).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!store.path.exists());
}

#[test]
fn private_evidence_is_immutable_and_rejects_links_or_public_permissions() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    private_write_once(&root, "receipt.json", b"first").unwrap();
    private_write_once(&root, "receipt.json", b"first").unwrap();
    assert!(private_write_once(&root, "receipt.json", b"changed").is_err());
    let meta = fs::metadata(root.join("receipt.json")).unwrap();
    assert_eq!(meta.mode() & 0o777, 0o600);
    symlink(root.join("receipt.json"), root.join("link.json")).unwrap();
    assert!(private_read(&root, "link.json", 1024).is_err());
    fs::hard_link(root.join("receipt.json"), root.join("hardlink.json")).unwrap();
    assert!(private_read(&root, "receipt.json", 1024).is_err());
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(private_read(&root, "receipt.json", 1024).is_err());
}

struct ProcessFixture {
    _temp: tempfile::TempDir,
    directory: PathBuf,
    workspace: WorkingDirectory,
    home: WorkingDirectory,
    executable: PathBuf,
    executable_sha256: [u8; 32],
    plan: DreaminaVideoSupplementPlan,
    launch: LaunchEvidence,
    command: super::super::GatedCliCommand,
}

impl ProcessFixture {
    fn new(mode: &str) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let home =
            WorkingDirectory::new_private(private_subdirectory(&root, "home").unwrap()).unwrap();
        let workspace =
            WorkingDirectory::new_private(private_subdirectory(&root, "workspace").unwrap())
                .unwrap();
        let executable = root.join("fake-cli");
        let body = match mode {
            "accepted" => {
                "printf '{\"submit_id\":\"replacement-process-id\",\"gen_status\":\"querying\"}'"
            }
            "bad_stdout" => "printf '{\"submit_id\":'",
            "timeout" => "exec /bin/sleep 30",
            _ => panic!("unknown fake CLI mode"),
        };
        fs::write(&executable, format!("#!/bin/sh\ncase \"$1\" in\ngenerate) printf x >> \"$HOME/generation-count\"; {body};;\nquery_result) [ \"$2\" = --submit_id ] && [ \"$3\" = replacement-process-id ] || exit 9; printf p >> \"$HOME/poll-count\"; printf '{{\"submit_id\":\"replacement-process-id\",\"gen_status\":\"querying\"}}';;\n*) exit 8;;\nesac\n")).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).unwrap();
        let executable_sha256 = hex::decode(sha256_hex(&fs::read(&executable).unwrap()))
            .unwrap()
            .try_into()
            .unwrap();
        let plan = DreaminaVideoSupplementPlan {
            runtime_binding_json: frozen_runtime_binding(),
            authorization_id: Uuid::new_v4(),
            dispatch_nonce: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            output_id: Uuid::new_v4(),
            provider_account_id: Uuid::new_v4(),
            execution_profile_id: Uuid::new_v4(),
            command_json: json!({}),
            command_hash: "a".repeat(64),
            quote_hash: "b".repeat(64),
            execution_binding_sha256: "c".repeat(64),
            billable_units: 1260,
            api_profile: "test".into(),
            state: "dispatching".into(),
            remote_operation_id: None,
            submit_stdout_sha256: None,
        };
        let directory = evidence_directory(&root, plan.authorization_id).unwrap();
        let launch = LaunchEvidence {
            authorization_id: plan.authorization_id,
            dispatch_nonce: plan.dispatch_nonce,
            execution_binding_sha256: supplement_binding(&plan).unwrap(),
            absolute_deadline_unix_ms: now_ms().unwrap() + 10_000,
        };
        private_write_once(
            &directory,
            "launch.json",
            &serde_json::to_vec(&launch).unwrap(),
        )
        .unwrap();
        let mut environment = std::collections::BTreeMap::new();
        environment.insert("HOME".into(), home.path().to_str().unwrap().into());
        let command = super::super::GatedCliCommand::new(
            &executable,
            hex::encode(executable_sha256),
            workspace.path(),
            vec!["generate".into()],
            environment,
            Vec::new(),
            if mode == "timeout" {
                Duration::from_secs(1)
            } else {
                Duration::from_secs(2)
            },
            Duration::from_millis(100),
        )
        .unwrap();
        Self {
            _temp: temp,
            directory,
            workspace,
            home,
            executable,
            executable_sha256,
            plan,
            launch,
            command,
        }
    }

    fn gate(&self) -> GatedCliSubmission {
        GatedCliSubmission::new(self.directory.join("gate"), self.plan.authorization_id).unwrap()
    }

    async fn dispatch(
        &self,
        store: &DurableFakeStore,
    ) -> Option<Result<Vec<u8>, ImageGatewayError>> {
        one_shot_dispatch(
            store,
            self.plan.authorization_id,
            self.plan.dispatch_nonce,
            &self.plan.runtime_binding_json,
            || async {
                let gate = self.gate();
                let binding = self.launch.binding()?;
                gate.prepare(&binding, &self.command)
                    .map_err(|_| failure())?;
                let runner = env::current_exe()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .join("remote-submit-runner");
                assert!(
                    runner.is_file(),
                    "build the local remote-submit-runner binary before running process tests"
                );
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.home.path().join("runner-count"))
                    .unwrap()
                    .write_all(b"r")
                    .unwrap();
                let mut child = tokio::process::Command::new(runner)
                    .arg(self.directory.join("gate"))
                    .arg(self.plan.authorization_id.to_string())
                    .env_clear()
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .spawn()
                    .unwrap();
                let result = tokio::time::timeout(Duration::from_secs(8), async {
                    let mut released = false;
                    loop {
                        match gate.observe(&binding).map_err(|_| failure())? {
                            GatedCliObservation::Ready(ready) if !released => {
                                gate.release(&binding, &ready).map_err(|_| failure())?;
                                fs::OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open(self.home.path().join("release-count"))
                                    .unwrap()
                                    .write_all(b"x")
                                    .unwrap();
                                released = true;
                            }
                            GatedCliObservation::Terminal(terminal) => {
                                return terminal_stdout(&terminal);
                            }
                            GatedCliObservation::Lost { .. } => return Err(failure()),
                            _ => tokio::time::sleep(Duration::from_millis(10)).await,
                        }
                    }
                })
                .await
                .map_err(|_| failure())?;
                assert!(child.wait().await.unwrap().success());
                result
            },
        )
        .await
        .unwrap()
    }

    async fn poll(&self, remote: &str) {
        let output = private_subdirectory(self.workspace.path(), "query-only").unwrap();
        let policy = DreaminaCliQueryPolicyV1::new(
            &self.executable,
            self.executable_sha256,
            self.workspace.clone(),
            self.home.clone(),
            Duration::from_secs(2),
            Duration::from_millis(100),
        )
        .unwrap();
        let command = policy
            .command_spec(&QueryResultRequestV1::query_only(remote, &output).unwrap())
            .unwrap();
        let result = TokioProcessBackend
            .execute_process(&command, &mut NoopSpawnObserver)
            .await
            .unwrap();
        assert!(result.status.success());
        validate_query(result.stdout.bytes(), remote).unwrap();
    }

    fn count(&self, name: &str) -> usize {
        fs::read(self.home.path().join(name))
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }
}

#[tokio::test]
async fn process_gate_concurrent_dispatch_restarts_recover_torn_receipt_and_only_poll() {
    for broken in [b"".as_slice(), b"{\"submit_id\":\"replace".as_slice()] {
        let fixture = ProcessFixture::new("accepted");
        let store = DurableFakeStore {
            path: fixture.directory.join("claimed"),
            runtime_binding: fixture.plan.runtime_binding_json.clone(),
        };
        let (first, second) = tokio::join!(fixture.dispatch(&store), fixture.dispatch(&store));
        let stdout = first.or(second).unwrap().unwrap();
        assert_eq!(fixture.count("runner-count"), 1);
        assert_eq!(fixture.count("release-count"), 1);
        assert_eq!(
            fixture.count("generation-count"),
            1,
            "fake CLI must have run before recovery"
        );
        fs::write(fixture.directory.join("submit-stdout.json"), broken).unwrap();
        fs::set_permissions(
            fixture.directory.join("submit-stdout.json"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let recovered = recover_submit_stdout(&fixture.plan, &fixture.directory).unwrap();
        assert_eq!(recovered, stdout);
        let accepted = parse_receipt(&recovered).unwrap();
        drop(store);
        let restarted = DurableFakeStore {
            path: fixture.directory.join("claimed"),
            runtime_binding: fixture.plan.runtime_binding_json.clone(),
        };
        assert!(fixture.dispatch(&restarted).await.is_none());
        fixture.poll(accepted.submit_id()).await;
        assert_eq!(fixture.count("runner-count"), 1);
        assert_eq!(fixture.count("generation-count"), 1);
        assert_eq!(fixture.count("poll-count"), 1);
        let quarantines: Vec<_> = fs::read_dir(&fixture.directory)
            .unwrap()
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".quarantined-submit-stdout.json-")
            })
            .collect();
        assert_eq!(quarantines.len(), 1);
        assert_eq!(fs::read(quarantines[0].path()).unwrap(), broken);
    }
}

#[tokio::test]
async fn process_gate_timeout_or_bad_stdout_never_relaunches_or_quarantines_without_valid_terminal()
{
    for mode in ["timeout", "bad_stdout"] {
        let fixture = ProcessFixture::new(mode);
        let store = DurableFakeStore {
            path: fixture.directory.join("claimed"),
            runtime_binding: fixture.plan.runtime_binding_json.clone(),
        };
        let first = fixture.dispatch(&store).await.unwrap();
        if mode == "timeout" {
            let GatedCliObservation::Terminal(terminal) = fixture
                .gate()
                .observe(&fixture.launch.binding().unwrap())
                .unwrap()
            else {
                panic!("expected durable timeout evidence");
            };
            assert_eq!(terminal.outcome(), &GatedCliProcessOutcome::TimedOut);
        }
        assert!(match first {
            Ok(ref stdout) => parse_receipt(stdout).is_err(),
            Err(_) => true,
        });
        fs::write(fixture.directory.join("submit-stdout.json"), b"{").unwrap();
        fs::set_permissions(
            fixture.directory.join("submit-stdout.json"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        assert!(recover_submit_stdout(&fixture.plan, &fixture.directory).is_err());
        assert_eq!(
            fs::read(fixture.directory.join("submit-stdout.json")).unwrap(),
            b"{"
        );
        let restarted = DurableFakeStore {
            path: fixture.directory.join("claimed"),
            runtime_binding: fixture.plan.runtime_binding_json.clone(),
        };
        assert!(fixture.dispatch(&restarted).await.is_none());
        assert_eq!(fixture.count("runner-count"), 1);
        assert_eq!(fixture.count("release-count"), 1);
        assert_eq!(fixture.count("generation-count"), 1);
        assert_eq!(fixture.count("poll-count"), 0);
    }
}

#[test]
fn atomic_private_publication_never_exposes_a_partial_winner_or_replaces_conflicts() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(root.join(".tmp-abandoned-crash"), b"{\"partial\":").unwrap();
    let barrier = std::sync::Barrier::new(2);
    let a = vec![b'a'; 128 * 1024];
    let b = vec![b'b'; 128 * 1024];
    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            barrier.wait();
            private_write_once(&root, "winner.json", &a)
        });
        let second = scope.spawn(|| {
            barrier.wait();
            private_write_once(&root, "winner.json", &b)
        });
        (first.join().unwrap(), second.join().unwrap())
    });
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let winner = private_read(&root, "winner.json", 128 * 1024).unwrap();
    assert!(winner == a || winner == b);
    assert_eq!(fs::metadata(root.join("winner.json")).unwrap().nlink(), 1);
    assert!(private_write_once(&root, "winner.json", b"different").is_err());
    assert_eq!(
        private_read(&root, "winner.json", 128 * 1024).unwrap(),
        winner
    );
}

#[tokio::test]
async fn process_gate_recovery_preserves_complete_conflicts_and_rejects_wrong_binding() {
    let fixture = ProcessFixture::new("accepted");
    let store = DurableFakeStore {
        path: fixture.directory.join("claimed"),
        runtime_binding: fixture.plan.runtime_binding_json.clone(),
    };
    fixture.dispatch(&store).await.unwrap().unwrap();
    for conflict in [
        b"{}".as_slice(),
        b"null".as_slice(),
        br#"{"submit_id":"different-complete-id","gen_status":"querying"}"#.as_slice(),
    ] {
        private_write_once(&fixture.directory, "submit-stdout.json", conflict).unwrap();
        assert!(recover_submit_stdout(&fixture.plan, &fixture.directory).is_err());
        assert_eq!(
            private_read(&fixture.directory, "submit-stdout.json", 65536).unwrap(),
            conflict
        );
        fs::remove_file(fixture.directory.join("submit-stdout.json")).unwrap();
    }
    fs::write(fixture.directory.join("submit-stdout.json"), b"{").unwrap();
    fs::set_permissions(
        fixture.directory.join("submit-stdout.json"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    for field in ["nonce", "runtime", "command", "quote", "execution"] {
        let mut wrong = fixture.plan.clone();
        match field {
            "nonce" => wrong.dispatch_nonce = Uuid::new_v4(),
            "runtime" => wrong.runtime_binding_json["cli_sha256"] = json!("e".repeat(64)),
            "command" => wrong.command_hash = "e".repeat(64),
            "quote" => wrong.quote_hash = "e".repeat(64),
            "execution" => wrong.execution_binding_sha256 = "e".repeat(64),
            _ => unreachable!(),
        }
        assert!(
            recover_submit_stdout(&wrong, &fixture.directory).is_err(),
            "recovery must reject a changed {field}"
        );
    }
    assert_eq!(
        fs::read(fixture.directory.join("submit-stdout.json")).unwrap(),
        b"{"
    );
    assert_eq!(fixture.count("runner-count"), 1);
    assert_eq!(fixture.count("release-count"), 1);
    assert_eq!(fixture.count("generation-count"), 1);
    assert_eq!(fixture.count("poll-count"), 0);
}

fn publication_bundle(fixture: &ProcessFixture, submit: &[u8]) -> DreaminaVideoSupplementPlan {
    let mut plan = fixture.plan.clone();
    plan.state = "accepted".into();
    plan.remote_operation_id = Some("replacement-process-id".into());
    plan.submit_stdout_sha256 = Some(sha256_hex(submit));
    let name = format!("success-{}", Uuid::new_v4().simple());
    let directory = private_subdirectory(&fixture.directory, &name).unwrap();
    let query = serde_json::to_vec(&json!({"submit_id":"replacement-process-id","gen_status":"success","result_json":{"images":[],"videos":[{"video_url":"https://media.example.test/result.mp4"}]}})).unwrap();
    let audit = serde_json::to_vec(&json!({
        "schema_version":1, "authorization_id":plan.authorization_id, "dispatch_nonce":plan.dispatch_nonce,
        "job_id":plan.job_id,"submission_id":plan.submission_id,"executor_execution_id":plan.executor_execution_id,
        "remote_operation_id":"replacement-process-id","argv":["query_result","--submit_id","replacement-process-id"],
        "stdout_sha256":sha256_hex(&query),"exit_code":0,"executed_at_ms":now_ms().unwrap()
    })).unwrap();
    let video = include_bytes!("../../../media_inputs/fixtures/blue.mp4");
    let review = serde_json::to_vec(&json!({
        "schema_version":1,"stdout_sha256":sha256_hex(&query),"execution_audit_sha256":sha256_hex(&audit),
        "video_url_sha256":sha256_hex(b"https://media.example.test/result.mp4"),"quote_hash":plan.quote_hash,
        "provider_command_sha256":plan.command_hash,"submit_stdout_sha256":sha256_hex(submit),
        "retrieved_video_sha256":sha256_hex(video),"retrieved_video_byte_size":video.len()
    })).unwrap();
    for (name, bytes) in [
        ("submit-stdout.json", submit),
        ("reviewed-query.json", query.as_slice()),
        ("execution-audit.json", audit.as_slice()),
        ("review.json", review.as_slice()),
        ("retrieved-video.mp4", video.as_slice()),
    ] {
        private_write_once(&directory, name, bytes).unwrap();
    }
    plan
}

#[tokio::test]
async fn process_gate_recovers_torn_publication_only_from_one_complete_bound_bundle() {
    for broken in [b"".as_slice(), b"{\"evidence_name\":\"success-".as_slice()] {
        let fixture = ProcessFixture::new("accepted");
        let store = DurableFakeStore {
            path: fixture.directory.join("claimed"),
            runtime_binding: fixture.plan.runtime_binding_json.clone(),
        };
        let submit = fixture.dispatch(&store).await.unwrap().unwrap();
        let plan = publication_bundle(&fixture, &submit);
        fs::write(fixture.directory.join("publication.json"), broken).unwrap();
        fs::set_permissions(
            fixture.directory.join("publication.json"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let recovered = recover_publication_pointer(&plan, &fixture.directory)
            .unwrap()
            .unwrap();
        assert_eq!(
            recovered.reviewed_stdout_sha256,
            complete_publication_evidence(
                &plan,
                &fixture.directory,
                &recovered.evidence_name,
                &submit
            )
            .unwrap()
            .reviewed_stdout_sha256
        );
        assert!(
            recover_publication_pointer(&plan, &fixture.directory)
                .unwrap()
                .is_some()
        );
        assert_eq!(fixture.count("generation-count"), 1);
        assert_eq!(
            fs::read_dir(&fixture.directory)
                .unwrap()
                .flatten()
                .filter(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".quarantined-publication.json-"))
                .count(),
            1
        );
    }
}
