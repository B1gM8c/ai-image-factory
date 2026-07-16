use std::{
    convert::Infallible,
    ffi::CString,
    fs,
    io::{self, Write},
    os::unix::{ffi::OsStrExt, fs::PermissionsExt},
    path::Path,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::{
    AsyncOutputSealError, AsyncOutputSink, CliPolicy, CliRuntime, CommandSpec, CommandSpecError,
    ExitClassification, FreshOutputDirectory, NoopSpawnObserver, OutputContract, OutputError,
    ProcessError, ReceiptCliPolicy, RuntimeError, SpawnEvidence, SpawnObserver, VerifiedExecutable,
    WorkingDirectory, default_exit_classification,
};

#[derive(Clone)]
struct StaticPolicy {
    command: CommandSpec,
}

impl CliPolicy for StaticPolicy {
    type Request = ();
    type Error = Infallible;

    fn command(&self, _request: &Self::Request) -> Result<CommandSpec, Self::Error> {
        Ok(self.command.clone())
    }

    fn classify_exit(&self, status: &std::process::ExitStatus) -> ExitClassification {
        default_exit_classification(status)
    }
}

#[derive(Clone)]
struct StaticReceiptPolicy {
    command: CommandSpec,
}

impl ReceiptCliPolicy for StaticReceiptPolicy {
    type Request = ();
    type Receipt = String;
    type Error = String;

    fn command(&self, _request: &Self::Request) -> Result<CommandSpec, Self::Error> {
        Ok(self.command.clone())
    }

    fn classify_exit(&self, status: &std::process::ExitStatus) -> ExitClassification {
        default_exit_classification(status)
    }

    fn parse_receipt(&self, stdout: &[u8]) -> Result<Self::Receipt, Self::Error> {
        std::str::from_utf8(stdout)
            .map(str::to_owned)
            .map_err(|_| "receipt is not UTF-8".to_string())
    }
}

#[derive(Default)]
struct ChunkRecordingSink {
    bytes: Vec<u8>,
    largest_write: usize,
}

#[derive(Debug)]
struct ReplacingSink {
    output: PathBuf,
    detached: PathBuf,
    replaced: bool,
}

#[derive(Default)]
struct AsyncChunkRecordingSink {
    bytes: Vec<u8>,
    largest_write: usize,
}

#[derive(Debug)]
struct AsyncReplacingSink {
    output: PathBuf,
    detached: PathBuf,
    replaced: bool,
}

impl AsyncOutputSink for AsyncChunkRecordingSink {
    type Error = io::Error;

    async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.largest_write = self.largest_write.max(bytes.len());
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

impl AsyncOutputSink for AsyncReplacingSink {
    type Error = io::Error;

    async fn write_chunk(&mut self, _bytes: &[u8]) -> Result<(), Self::Error> {
        if !self.replaced {
            fs::rename(&self.output, &self.detached)?;
            fs::write(&self.output, b"replacement")?;
            self.replaced = true;
        }
        Ok(())
    }
}

impl Write for ReplacingSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.replaced {
            fs::rename(&self.output, &self.detached)?;
            fs::write(&self.output, b"replacement")?;
            self.replaced = true;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Write for ChunkRecordingSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.largest_write = self.largest_write.max(bytes.len());
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct RejectingObserver {
    evidence: Arc<Mutex<Option<SpawnEvidence>>>,
}

impl SpawnObserver for RejectingObserver {
    type Error = &'static str;

    fn observe_spawn(&mut self, evidence: &SpawnEvidence) -> Result<(), Self::Error> {
        *self.evidence.lock().expect("observer mutex") = Some(*evidence);
        Err("persistence rejected")
    }
}

#[derive(Default)]
struct RecordingObserver {
    evidence: Arc<Mutex<Option<SpawnEvidence>>>,
}

impl SpawnObserver for RecordingObserver {
    type Error = Infallible;

    fn observe_spawn(&mut self, evidence: &SpawnEvidence) -> Result<(), Self::Error> {
        *self.evidence.lock().expect("observer mutex") = Some(*evidence);
        Ok(())
    }
}

#[test]
fn private_working_directory_requires_current_owner_and_mode_0700() {
    let directory = TempDir::new().expect("temp directory");
    make_private(directory.path());
    WorkingDirectory::new_private(directory.path()).expect("private working directory");

    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o750))
        .expect("relax directory mode");
    assert!(matches!(
        WorkingDirectory::new_private(directory.path()),
        Err(CommandSpecError::InvalidPrivateWorkingDirectory)
    ));
    assert!(WorkingDirectory::new(directory.path()).is_ok());
}

#[test]
fn command_debug_redacts_private_paths_and_runtime_payloads() {
    let directory = TempDir::new().expect("working directory");
    let account_home = TempDir::new().expect("account home");
    make_private(account_home.path());
    let secret_argument = "sensitive-provider-task";
    let secret_environment = "sensitive-environment-value";
    let secret_stdin = b"sensitive-stdin-value";
    let command = CommandSpec::new_receipt(
        VerifiedExecutable::new("/bin/sh").expect("verified shell"),
        WorkingDirectory::new(directory.path()).expect("working directory"),
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .expect("command spec")
    .arg(secret_argument)
    .expect("secret argument")
    .env("HOME", account_home.path())
    .expect("account home")
    .env("PROVIDER_SECRET", secret_environment)
    .expect("secret environment")
    .stdin(secret_stdin)
    .expect("secret stdin")
    .require_directory(
        WorkingDirectory::new_private(account_home.path()).expect("private account home"),
    );

    let debug = format!("{command:?}");
    assert!(debug.contains("PROVIDER_SECRET"));
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains(directory.path().to_str().unwrap()));
    assert!(!debug.contains(account_home.path().to_str().unwrap()));
    assert!(!debug.contains(secret_argument));
    assert!(!debug.contains(secret_environment));
    assert!(!debug.contains(std::str::from_utf8(secret_stdin).unwrap()));
}

#[tokio::test]
async fn private_working_directory_revalidates_permissions_before_spawn() {
    let directory = TempDir::new().expect("temp directory");
    let account_home = TempDir::new().expect("account home");
    make_private(account_home.path());
    let command = CommandSpec::new_receipt(
        VerifiedExecutable::new("/bin/sh").expect("verified shell"),
        WorkingDirectory::new(directory.path()).expect("working directory"),
        Duration::from_secs(3),
        Duration::from_millis(50),
    )
    .expect("command spec")
    .require_directory(
        WorkingDirectory::new_private(account_home.path()).expect("private account home"),
    )
    .arg("-c")
    .expect("shell flag")
    .arg("printf called > should-not-exist")
    .expect("shell script");
    fs::set_permissions(account_home.path(), fs::Permissions::from_mode(0o755))
        .expect("relax account-home mode");

    assert!(matches!(
        CliRuntime::new(StaticReceiptPolicy { command })
            .run_receipt(&(), &mut NoopSpawnObserver)
            .await,
        Err(RuntimeError::Process(ProcessError::InvalidCommand(
            CommandSpecError::WorkingDirectoryChanged
        )))
    ));
    assert!(!directory.path().join("should-not-exist").exists());
}

#[tokio::test]
async fn clears_environment_and_discards_artifact_process_output() {
    let directory = TempDir::new().expect("temp directory");
    let script = r#"
        i=0
        while [ "$i" -lt 20000 ]; do
            printf noise
            printf noise >&2
            i=$((i + 1))
        done
        printf '%s|%s' "${HOME-unset}" "${EXPLICIT-unset}" > result.bin
    "#;
    let command = command(directory.path(), script, 1024)
        .env("EXPLICIT", "present")
        .expect("explicit environment");
    let result = CliRuntime::new(StaticPolicy { command })
        .run_to_sink(&(), &mut NoopSpawnObserver, Vec::new())
        .await
        .expect("command succeeds");

    assert_eq!(result.sink, b"unset|present");
}

#[tokio::test]
async fn passes_argv_without_shell_interpolation_and_writes_stdin() {
    let directory = TempDir::new().expect("temp directory");
    let command = command(
        directory.path(),
        r#"IFS= read -r line; printf '%s|%s|%s' "$1" "$2" "$line" > result.bin"#,
        1024,
    )
    .arg("runtime-test")
    .expect("argv zero")
    .arg("alpha beta")
    .expect("argument one")
    .arg("$(touch should-not-exist)")
    .expect("argument two")
    .stdin(b"input payload\n".to_vec())
    .expect("bounded stdin");
    let result = CliRuntime::new(StaticPolicy { command })
        .run_to_sink(&(), &mut NoopSpawnObserver, Vec::new())
        .await
        .expect("command succeeds");

    assert_eq!(
        result.sink,
        b"alpha beta|$(touch should-not-exist)|input payload"
    );
    assert!(!directory.path().join("should-not-exist").exists());
}

#[tokio::test]
async fn seals_output_with_fixed_size_streaming_buffer() {
    let directory = TempDir::new().expect("temp directory");
    let payload = vec![b'x'; crate::STREAM_BUFFER_BYTES * 2 + 17];
    let command = command(
        directory.path(),
        "/bin/cat > result.bin",
        payload.len() as u64,
    )
    .stdin(payload.clone())
    .expect("bounded stdin");
    let result = CliRuntime::new(StaticPolicy { command })
        .run_to_sink(&(), &mut NoopSpawnObserver, ChunkRecordingSink::default())
        .await
        .expect("command succeeds");

    let expected_hash = Sha256::digest(&payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(result.output.byte_size, payload.len() as u64);
    assert_eq!(result.output.sha256_hex, expected_hash);
    assert_eq!(result.sink.bytes, payload);
    assert!(result.sink.largest_write <= crate::STREAM_BUFFER_BYTES);
}

#[tokio::test]
async fn fixed_output_rejects_same_name_replacement_during_read() {
    let directory = TempDir::new().expect("temp directory");
    let output_path = directory.path().join("result.bin");
    let command = command(directory.path(), "printf original > result.bin", 1024);
    let error = CliRuntime::new(StaticPolicy { command })
        .run_to_sink(
            &(),
            &mut NoopSpawnObserver,
            ReplacingSink {
                output: output_path,
                detached: directory.path().join("detached"),
                replaced: false,
            },
        )
        .await
        .expect_err("same-name replacement must fail");

    assert!(matches!(
        error,
        RuntimeError::Output(OutputError::ChangedDuringRead)
    ));
}

#[test]
fn fresh_output_directory_seals_one_bound_file_with_bounded_writes() {
    let root = TempDir::new().expect("temp directory");
    let directory = root.path().join("staging");
    fs::create_dir(&directory).expect("staging directory");
    make_private(&directory);
    let working = WorkingDirectory::new(&directory).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 512 * 1024).expect("fresh output");
    let payload = vec![b'x'; crate::STREAM_BUFFER_BYTES * 2 + 17];
    fs::write(directory.join("provider-output.bin"), &payload).expect("provider output");

    let (sealed, sink) = output
        .seal_single_file_to_sink(ChunkRecordingSink::default())
        .expect("single output seals");

    assert_eq!(sealed.relative_filename, Path::new("provider-output.bin"));
    assert_eq!(sealed.byte_size, payload.len() as u64);
    let expected_hash = Sha256::digest(&payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(sealed.sha256_hex, expected_hash);
    assert_eq!(sink.bytes, payload);
    assert!(sink.largest_write <= crate::STREAM_BUFFER_BYTES);
}

#[tokio::test]
async fn fresh_output_directory_streams_one_bound_file_to_an_async_sink() {
    let root = TempDir::new().expect("temp directory");
    let directory = root.path().join("staging");
    fs::create_dir(&directory).expect("staging directory");
    make_private(&directory);
    let working = WorkingDirectory::new(&directory).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 512 * 1024).expect("fresh output");
    let payload = vec![b'x'; crate::STREAM_BUFFER_BYTES * 2 + 17];
    fs::write(directory.join("provider-output.bin"), &payload).expect("provider output");

    let (sealed, sink) = output
        .seal_single_file_to_async_sink(AsyncChunkRecordingSink::default())
        .await
        .expect("single output seals asynchronously");

    assert_eq!(sealed.relative_filename, Path::new("provider-output.bin"));
    assert_eq!(sealed.byte_size, payload.len() as u64);
    let expected_hash = Sha256::digest(&payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(sealed.sha256_hex, expected_hash);
    assert_eq!(sink.bytes, payload);
    assert!(sink.largest_write <= crate::STREAM_BUFFER_BYTES);
}

#[tokio::test]
async fn async_output_sealing_rejects_same_name_replacement_during_read() {
    let root = TempDir::new().expect("temp directory");
    let staging = root.path().join("staging");
    fs::create_dir(&staging).expect("staging directory");
    make_private(&staging);
    let working = WorkingDirectory::new(&staging).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");
    let output_path = staging.join("output");
    fs::write(&output_path, b"original").expect("provider output");

    let error = output
        .seal_single_file_to_async_sink(AsyncReplacingSink {
            output: output_path,
            detached: root.path().join("detached"),
            replaced: false,
        })
        .await
        .expect_err("same-name replacement must fail");

    assert!(matches!(
        error,
        AsyncOutputSealError::Output(OutputError::ChangedDuringRead)
    ));
}

#[test]
fn fresh_output_directory_rejects_untrusted_directory_shapes() {
    let nonempty = TempDir::new().expect("temp directory");
    make_private(nonempty.path());
    fs::write(nonempty.path().join("existing"), b"data").expect("existing file");
    let working = WorkingDirectory::new(nonempty.path()).expect("working directory");
    assert!(matches!(
        FreshOutputDirectory::new(&working, 1024),
        Err(OutputError::DirectoryNotEmpty)
    ));

    let multiple = TempDir::new().expect("temp directory");
    make_private(multiple.path());
    let working = WorkingDirectory::new(multiple.path()).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");
    fs::write(multiple.path().join("one"), b"one").expect("first output");
    fs::write(multiple.path().join("two"), b"two").expect("second output");
    assert!(matches!(
        output.seal_single_file_to_sink(Vec::new()),
        Err(OutputError::MultipleEntries)
    ));

    let missing = TempDir::new().expect("temp directory");
    make_private(missing.path());
    let working = WorkingDirectory::new(missing.path()).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");
    assert!(matches!(
        output.seal_single_file_to_sink(Vec::new()),
        Err(OutputError::Missing)
    ));

    let empty = TempDir::new().expect("temp directory");
    make_private(empty.path());
    let working = WorkingDirectory::new(empty.path()).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");
    fs::write(empty.path().join("empty"), b"").expect("empty output");
    assert!(matches!(
        output.seal_single_file_to_sink(Vec::new()),
        Err(OutputError::Empty)
    ));

    let oversized = TempDir::new().expect("temp directory");
    make_private(oversized.path());
    let working = WorkingDirectory::new(oversized.path()).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 3).expect("fresh output");
    fs::write(oversized.path().join("large"), b"1234").expect("oversized output");
    assert!(matches!(
        output.seal_single_file_to_sink(Vec::new()),
        Err(OutputError::TooLarge)
    ));

    let nested = TempDir::new().expect("temp directory");
    make_private(nested.path());
    let working = WorkingDirectory::new(nested.path()).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");
    fs::create_dir(nested.path().join("nested")).expect("nested output");
    assert!(matches!(
        output.seal_single_file_to_sink(Vec::new()),
        Err(OutputError::NotRegular)
    ));

    let fifo = TempDir::new().expect("temp directory");
    make_private(fifo.path());
    let working = WorkingDirectory::new(fifo.path()).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");
    let fifo_path = fifo.path().join("output");
    let fifo_c = CString::new(fifo_path.as_os_str().as_bytes()).expect("FIFO path");
    assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
    assert!(matches!(
        output.seal_single_file_to_sink(Vec::new()),
        Err(OutputError::NotRegular)
    ));

    let symlink = TempDir::new().expect("temp directory");
    make_private(symlink.path());
    let target = TempDir::new().expect("target directory");
    fs::write(target.path().join("target"), b"data").expect("target output");
    let working = WorkingDirectory::new(symlink.path()).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");
    std::os::unix::fs::symlink(target.path().join("target"), symlink.path().join("output"))
        .expect("symlink output");
    assert!(output.seal_single_file_to_sink(Vec::new()).is_err());

    let hardlink = TempDir::new().expect("temp directory");
    make_private(hardlink.path());
    let outside = TempDir::new().expect("outside directory");
    fs::write(outside.path().join("target"), b"data").expect("hardlink target");
    let working = WorkingDirectory::new(hardlink.path()).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");
    fs::hard_link(
        outside.path().join("target"),
        hardlink.path().join("output"),
    )
    .expect("hardlink output");
    assert!(matches!(
        output.seal_single_file_to_sink(Vec::new()),
        Err(OutputError::NotRegular)
    ));

    let unsafe_permissions = TempDir::new().expect("temp directory");
    fs::set_permissions(unsafe_permissions.path(), fs::Permissions::from_mode(0o755))
        .expect("unsafe permissions");
    let working = WorkingDirectory::new(unsafe_permissions.path()).expect("working directory");
    assert!(matches!(
        FreshOutputDirectory::new(&working, 1024),
        Err(OutputError::UnsafeDirectory)
    ));
    assert!(matches!(
        FreshOutputDirectory::new(&working, 0),
        Err(OutputError::InvalidLimit)
    ));

    let special_permissions = TempDir::new().expect("temp directory");
    fs::set_permissions(
        special_permissions.path(),
        fs::Permissions::from_mode(0o2700),
    )
    .expect("setgid permissions");
    let working = WorkingDirectory::new(special_permissions.path()).expect("working directory");
    assert!(matches!(
        FreshOutputDirectory::new(&working, 1024),
        Err(OutputError::UnsafeDirectory)
    ));
}

#[test]
fn fresh_output_directory_stays_bound_after_path_replacement() {
    let root = TempDir::new().expect("temp directory");
    let staging = root.path().join("staging");
    let moved = root.path().join("moved");
    fs::create_dir(&staging).expect("staging directory");
    make_private(&staging);
    let working = WorkingDirectory::new(&staging).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");

    fs::rename(&staging, &moved).expect("move bound directory");
    fs::create_dir(&staging).expect("replacement directory");
    fs::write(staging.join("replacement"), b"wrong").expect("replacement output");
    fs::write(moved.join("expected"), b"right").expect("bound output");

    let (sealed, bytes) = output
        .seal_single_file_to_sink(Vec::new())
        .expect("bound directory output");
    assert_eq!(sealed.relative_filename, Path::new("expected"));
    assert_eq!(bytes, b"right");
}

#[test]
fn fresh_output_directory_rejects_same_name_replacement_during_read() {
    let root = TempDir::new().expect("temp directory");
    let staging = root.path().join("staging");
    fs::create_dir(&staging).expect("staging directory");
    make_private(&staging);
    let working = WorkingDirectory::new(&staging).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");
    let output_path = staging.join("output");
    fs::write(&output_path, b"original").expect("provider output");

    let error = output
        .seal_single_file_to_sink(ReplacingSink {
            output: output_path,
            detached: root.path().join("detached"),
            replaced: false,
        })
        .expect_err("same-name replacement must fail");

    assert!(matches!(error, OutputError::ChangedDuringRead));
}

#[test]
fn fresh_output_directory_rejects_permissions_changed_after_preflight() {
    let directory = TempDir::new().expect("temp directory");
    make_private(directory.path());
    let working = WorkingDirectory::new(directory.path()).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh output");
    fs::write(directory.path().join("output"), b"data").expect("provider output");
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755))
        .expect("unsafe permissions");

    assert!(matches!(
        output.seal_single_file_to_sink(Vec::new()),
        Err(OutputError::UnsafeDirectory)
    ));
}

fn make_private(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("private directory permissions");
}

#[tokio::test]
async fn timeout_terminates_the_entire_process_group_and_waits() {
    let directory = TempDir::new().expect("temp directory");
    let mut command = command(directory.path(), "trap '' TERM; while :; do :; done", 1024);
    command = CommandSpec::new(
        command.executable().clone(),
        command.working_directory().clone(),
        command.output().expect("output contract").clone(),
        Duration::from_millis(80),
        Duration::from_millis(30),
    )
    .expect("timeouts")
    .arg("-c")
    .expect("shell flag")
    .arg("trap '' TERM; while :; do :; done")
    .expect("shell script");
    let mut observer = RecordingObserver::default();
    let error = CliRuntime::new(StaticPolicy { command })
        .run_to_sink(&(), &mut observer, Vec::new())
        .await
        .expect_err("command times out");
    let RuntimeError::Process(ProcessError::TimedOut { evidence, .. }) = error else {
        panic!("unexpected error: {error}");
    };

    assert_eq!(
        Some(evidence),
        *observer.evidence.lock().expect("observer mutex")
    );
    assert_process_group_gone(evidence.process_group_id).await;
}

#[tokio::test]
async fn captures_a_bounded_receipt_without_an_artifact_contract() {
    let directory = TempDir::new().expect("temp directory");
    let command = receipt_command(directory.path(), "printf '{\"submit_id\":\"task-1\"}'");
    let result = CliRuntime::new(StaticReceiptPolicy { command })
        .run_receipt(&(), &mut NoopSpawnObserver)
        .await
        .expect("receipt succeeds");

    assert_eq!(result.receipt, r#"{"submit_id":"task-1"}"#);
}

#[tokio::test]
async fn rejects_truncated_success_output_after_draining_the_process() {
    let directory = TempDir::new().expect("temp directory");
    let command = receipt_command(
        directory.path(),
        "i=0; while [ \"$i\" -lt 70000 ]; do printf x >&2; i=$((i + 1)); done; printf ok",
    );
    let error = CliRuntime::new(StaticReceiptPolicy { command })
        .run_receipt(&(), &mut NoopSpawnObserver)
        .await
        .expect_err("oversized stderr fails closed");

    assert!(matches!(
        error,
        RuntimeError::CapturedOutputTooLarge { stream: "stderr" }
    ));
}

#[test]
fn verifies_an_executable_against_a_pinned_sha256() {
    let bytes = fs::read("/bin/sh").expect("read shell");
    let expected: [u8; 32] = Sha256::digest(bytes).into();
    VerifiedExecutable::new_with_sha256("/bin/sh", expected).expect("matching digest");

    assert!(matches!(
        VerifiedExecutable::new_with_sha256("/bin/sh", [0_u8; 32]),
        Err(CommandSpecError::ExecutableDigestMismatch)
    ));
}

#[tokio::test]
async fn aborting_run_kills_the_leader_and_its_original_process_group() {
    let directory = TempDir::new().expect("temp directory");
    let command = command(
        directory.path(),
        "/bin/sleep 30 & printf '%s' \"$!\" > descendant.pid; wait",
        1024,
    );
    let observed = Arc::new(Mutex::new(None));
    let task_observed = Arc::clone(&observed);
    let task = tokio::spawn(async move {
        let mut observer = RecordingObserver {
            evidence: task_observed,
        };
        CliRuntime::new(StaticPolicy { command })
            .run_to_sink(&(), &mut observer, Vec::new())
            .await
    });

    let descendant_path = directory.path().join("descendant.pid");
    let (evidence, descendant_pid) = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let evidence = *observed.lock().expect("observer mutex");
            let descendant_pid = fs::read_to_string(&descendant_path)
                .ok()
                .and_then(|pid| pid.parse::<u32>().ok());
            if let (Some(evidence), Some(descendant_pid)) = (evidence, descendant_pid) {
                break (evidence, descendant_pid);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("leader and descendant start before abort");

    task.abort();
    let join_error = task.await.expect_err("runtime task is cancelled");
    assert!(join_error.is_cancelled());
    assert_process_gone(evidence.pid).await;
    assert_process_gone(descendant_pid).await;
    assert_process_group_gone(evidence.process_group_id).await;
}

#[tokio::test]
async fn rejects_unsafe_and_invalid_outputs() {
    assert!(matches!(
        OutputContract::new("../escape", 10),
        Err(CommandSpecError::InvalidOutputFilename)
    ));
    assert!(matches!(
        OutputContract::new("nested/output", 10),
        Err(CommandSpecError::InvalidOutputFilename)
    ));

    let symlink_directory = TempDir::new().expect("temp directory");
    let symlink = command(
        symlink_directory.path(),
        "printf data > target; /bin/ln -s target result.bin",
        1024,
    );
    assert!(matches!(
        CliRuntime::new(StaticPolicy { command: symlink })
            .run_to_sink(&(), &mut NoopSpawnObserver, Vec::new())
            .await,
        Err(RuntimeError::Output(_))
    ));

    let directory_output = TempDir::new().expect("temp directory");
    let directory = command(directory_output.path(), "/bin/mkdir result.bin", 1024);
    assert!(matches!(
        CliRuntime::new(StaticPolicy { command: directory })
            .run_to_sink(&(), &mut NoopSpawnObserver, Vec::new())
            .await,
        Err(RuntimeError::Output(crate::OutputError::NotRegular))
    ));

    let oversized_output = TempDir::new().expect("temp directory");
    let oversized = command(oversized_output.path(), "printf 123456 > result.bin", 3);
    assert!(matches!(
        CliRuntime::new(StaticPolicy { command: oversized })
            .run_to_sink(&(), &mut NoopSpawnObserver, Vec::new())
            .await,
        Err(RuntimeError::Output(crate::OutputError::TooLarge))
    ));

    let hardlink_output = TempDir::new().expect("temp directory");
    let hardlink = command(
        hardlink_output.path(),
        "printf data > target; /bin/ln target result.bin",
        1024,
    );
    assert!(matches!(
        CliRuntime::new(StaticPolicy { command: hardlink })
            .run_to_sink(&(), &mut NoopSpawnObserver, Vec::new())
            .await,
        Err(RuntimeError::Output(crate::OutputError::NotRegular))
    ));
}

#[tokio::test]
async fn rejects_unbounded_stdin_before_spawn() {
    let directory = TempDir::new().expect("temp directory");
    let result = command(directory.path(), "printf data > result.bin", 1024).stdin(vec![
        0;
        crate::MAX_STDIN_BYTES
            + 1
    ]);
    assert!(matches!(result, Err(CommandSpecError::StdinTooLarge)));
}

#[tokio::test]
async fn successful_leader_with_a_live_descendant_fails_closed() {
    let directory = TempDir::new().expect("temp directory");
    let command = command(
        directory.path(),
        "sleep 30 & printf data > result.bin",
        1024,
    );
    let error = CliRuntime::new(StaticPolicy { command })
        .run_to_sink(&(), &mut NoopSpawnObserver, Vec::new())
        .await
        .expect_err("detached descendant violates the runtime contract");

    assert!(matches!(
        error,
        RuntimeError::Process(ProcessError::ResidualProcessGroup { .. })
    ));
}

#[tokio::test]
async fn observer_failure_terminates_and_waits_for_the_child() {
    let directory = TempDir::new().expect("temp directory");
    let command = command(directory.path(), "trap '' TERM; while :; do :; done", 1024);
    let evidence = Arc::new(Mutex::new(None));
    let mut observer = RejectingObserver {
        evidence: Arc::clone(&evidence),
    };
    let error = CliRuntime::new(StaticPolicy { command })
        .run_to_sink(&(), &mut observer, Vec::new())
        .await
        .expect_err("observer rejects spawn");
    assert!(matches!(
        error,
        RuntimeError::Process(ProcessError::Observer { .. })
    ));

    let evidence = evidence
        .lock()
        .expect("observer mutex")
        .expect("spawn evidence");
    assert_process_group_gone(evidence.process_group_id).await;
}

fn command(directory: &Path, script: &str, max_output_bytes: u64) -> CommandSpec {
    CommandSpec::new(
        VerifiedExecutable::new("/bin/sh").expect("verified shell"),
        WorkingDirectory::new(directory).expect("working directory"),
        OutputContract::new("result.bin", max_output_bytes).expect("output contract"),
        Duration::from_secs(3),
        Duration::from_millis(50),
    )
    .expect("command spec")
    .arg("-c")
    .expect("shell flag")
    .arg(script)
    .expect("shell script")
}

fn receipt_command(directory: &Path, script: &str) -> CommandSpec {
    CommandSpec::new_receipt(
        VerifiedExecutable::new("/bin/sh").expect("verified shell"),
        WorkingDirectory::new(directory).expect("working directory"),
        Duration::from_secs(3),
        Duration::from_millis(50),
    )
    .expect("command spec")
    .arg("-c")
    .expect("shell flag")
    .arg(script)
    .expect("shell script")
}

async fn assert_process_group_gone(process_group_id: u32) {
    for _ in 0..200 {
        let result = unsafe { libc::kill(-(process_group_id as libc::pid_t), 0) };
        if result == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("process group {process_group_id} is still alive");
}

async fn assert_process_gone(pid: u32) {
    for _ in 0..200 {
        let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
        if result == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("process {pid} is still alive");
}
