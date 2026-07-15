use std::{
    convert::Infallible,
    fs,
    io::{self, Write},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::{
    CliPolicy, CliRuntime, CommandSpec, CommandSpecError, ExitClassification, NoopSpawnObserver,
    OutputContract, ProcessError, ReceiptCliPolicy, RuntimeError, SpawnEvidence, SpawnObserver,
    VerifiedExecutable, WorkingDirectory, default_exit_classification,
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
