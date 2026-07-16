#![cfg(unix)]

use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use gpt_image_2_gateway::{
    GatedCliBinding, GatedCliCommand, GatedCliObservation, GatedCliProcessError,
    GatedCliProcessOutcome, GatedCliSubmission,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::{process::Child, time::timeout};
use uuid::Uuid;

const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn durable_release_is_required_before_the_cli_can_create_a_side_effect() {
    let fixture = Fixture::new(Duration::from_secs(10), "success").unwrap();
    let mut runner = fixture.spawn_runner().unwrap();
    let ready = fixture.wait_ready().await.unwrap();
    assert!(!fixture.side_effect.exists());

    fixture
        .submission
        .release(&fixture.binding, &ready)
        .unwrap();
    let terminal = fixture.wait_terminal().await.unwrap();
    let status = runner.wait().await.unwrap();

    assert!(status.success());
    assert!(fixture.side_effect.exists());
    assert!(terminal.released());
    assert!(terminal.exec_started());
    assert_eq!(
        terminal.outcome(),
        &GatedCliProcessOutcome::Exited {
            exit_code: Some(0),
            signal: None,
        }
    );
    assert_eq!(terminal.stdout(), b"{\"submit_id\":\"task-1\"}");
    assert_eq!(terminal.stderr(), b"provider-stderr");
    assert!(!terminal.stdout_truncated());
    assert!(!terminal.stderr_truncated());
}

#[tokio::test]
async fn killing_the_helper_before_release_never_invokes_the_cli_and_allows_orphan_cleanup() {
    let fixture = Fixture::new(Duration::from_secs(10), "success").unwrap();
    let mut runner = fixture.spawn_runner().unwrap();
    let _ready = fixture.wait_ready().await.unwrap();
    assert!(!fixture.side_effect.exists());

    runner.start_kill().unwrap();
    runner.wait().await.unwrap();
    timeout(OBSERVATION_TIMEOUT, async {
        loop {
            match fixture.submission.observe(&fixture.binding).unwrap() {
                GatedCliObservation::Lost {
                    released: false,
                    child_alive: false,
                } => return,
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap();

    assert!(
        !fixture
            .submission
            .terminate_orphan(&fixture.binding)
            .unwrap()
    );
    assert!(!fixture.side_effect.exists());
}

#[tokio::test]
async fn killing_the_gate_before_release_is_not_misclassified_as_a_deadline() {
    let fixture = Fixture::new(Duration::from_secs(10), "success").unwrap();
    let mut runner = fixture.spawn_runner().unwrap();
    let _ready = fixture.wait_ready().await.unwrap();
    let child_pid = fixture.child_pid().unwrap();

    assert_eq!(unsafe { libc::kill(child_pid, libc::SIGKILL) }, 0);
    let terminal = fixture.wait_terminal().await.unwrap();
    let status = runner.wait().await.unwrap();

    assert!(status.success());
    assert!(!fixture.side_effect.exists());
    assert!(!terminal.released());
    assert!(!terminal.exec_started());
    assert_eq!(
        terminal.outcome(),
        &GatedCliProcessOutcome::GateFailed {
            error_code: "gate_lost_before_release".to_owned(),
        }
    );
}

#[tokio::test]
async fn absolute_deadline_expiry_terminates_the_gate_without_invoking_the_cli() {
    let fixture = Fixture::new(Duration::from_secs(1), "success").unwrap();
    let mut runner = fixture.spawn_runner().unwrap();
    let ready = fixture.wait_ready().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(
        fixture.submission.release(&fixture.binding, &ready),
        Err(GatedCliProcessError::NotReady)
    );
    let terminal = fixture.wait_terminal().await.unwrap();
    let status = runner.wait().await.unwrap();

    assert!(status.success());
    assert!(!fixture.side_effect.exists());
    assert!(!terminal.released());
    assert!(!terminal.exec_started());
    assert_eq!(
        terminal.outcome(),
        &GatedCliProcessOutcome::AbsoluteDeadlineElapsed
    );
}

#[tokio::test]
async fn released_cli_timeout_is_bounded_and_records_that_exec_started() {
    let fixture = Fixture::new(Duration::from_secs(10), "timeout").unwrap();
    let mut runner = fixture.spawn_runner().unwrap();
    let ready = fixture.wait_ready().await.unwrap();
    fixture
        .submission
        .release(&fixture.binding, &ready)
        .unwrap();
    let terminal = fixture.wait_terminal().await.unwrap();
    let status = runner.wait().await.unwrap();

    assert!(status.success());
    assert!(fixture.side_effect.exists());
    assert!(terminal.released());
    assert!(terminal.exec_started());
    assert_eq!(terminal.outcome(), &GatedCliProcessOutcome::TimedOut);
}

#[tokio::test]
async fn absolute_deadline_hard_kills_a_running_cli_without_termination_grace() {
    let fixture = Fixture::new(Duration::from_secs(2), "absolute_deadline").unwrap();
    let mut runner = fixture.spawn_runner().unwrap();
    let ready = fixture.wait_ready().await.unwrap();
    let started = Instant::now();
    fixture
        .submission
        .release(&fixture.binding, &ready)
        .unwrap();
    fixture.wait_for_side_effect().await.unwrap();
    let terminal = fixture.wait_terminal().await.unwrap();
    let status = runner.wait().await.unwrap();

    assert!(status.success());
    assert!(terminal.released());
    assert!(terminal.exec_started());
    assert_eq!(
        terminal.outcome(),
        &GatedCliProcessOutcome::AbsoluteDeadlineElapsed
    );
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "absolute deadline incorrectly waited for the five-second termination grace"
    );
}

#[tokio::test]
async fn residual_process_group_is_killed_before_terminal_publication() {
    let fixture = Fixture::new(Duration::from_secs(10), "residual").unwrap();
    let mut runner = fixture.spawn_runner().unwrap();
    let ready = fixture.wait_ready().await.unwrap();
    fixture
        .submission
        .release(&fixture.binding, &ready)
        .unwrap();
    let terminal = fixture.wait_terminal().await.unwrap();
    let status = runner.wait().await.unwrap();

    assert!(status.success());
    assert!(terminal.released());
    assert!(terminal.exec_started());
    assert_eq!(
        terminal.outcome(),
        &GatedCliProcessOutcome::ResidualProcessGroup
    );
}

#[tokio::test]
async fn provider_output_is_drained_while_capture_remains_bounded() {
    let fixture = Fixture::new(Duration::from_secs(10), "large_output").unwrap();
    let mut runner = fixture.spawn_runner().unwrap();
    let ready = fixture.wait_ready().await.unwrap();
    fixture
        .submission
        .release(&fixture.binding, &ready)
        .unwrap();
    let terminal = fixture.wait_terminal().await.unwrap();
    let status = runner.wait().await.unwrap();

    assert!(status.success());
    assert_eq!(terminal.stdout().len(), 64 * 1024);
    assert!(terminal.stdout_truncated());
    assert_eq!(
        terminal.outcome(),
        &GatedCliProcessOutcome::Exited {
            exit_code: Some(0),
            signal: None,
        }
    );
}

#[tokio::test]
async fn killing_the_helper_after_release_exposes_one_cleanup_target_without_relaunch() {
    let fixture = Fixture::new(Duration::from_secs(10), "orphan").unwrap();
    let mut runner = fixture.spawn_runner().unwrap();
    let ready = fixture.wait_ready().await.unwrap();
    fixture
        .submission
        .release(&fixture.binding, &ready)
        .unwrap();
    fixture.wait_for_side_effect().await.unwrap();

    runner.start_kill().unwrap();
    runner.wait().await.unwrap();
    timeout(OBSERVATION_TIMEOUT, async {
        loop {
            match fixture.submission.observe(&fixture.binding).unwrap() {
                GatedCliObservation::Lost {
                    released: true,
                    child_alive: true,
                } => return,
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap();

    assert!(
        fixture
            .submission
            .terminate_orphan(&fixture.binding)
            .unwrap()
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!fixture.completed.exists());
    assert_eq!(fs::read_to_string(&fixture.side_effect).unwrap(), "invoked");
}

struct Fixture {
    _temp: TempDir,
    root: PathBuf,
    side_effect: PathBuf,
    completed: PathBuf,
    submission: GatedCliSubmission,
    binding: GatedCliBinding,
}

impl Fixture {
    fn new(release_after: Duration, mode: &str) -> Result<Self, String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700))
            .map_err(|error| error.to_string())?;
        let root = temp.path().join("remote-submit");
        let side_effect = temp.path().join("provider-invoked");
        let completed = temp.path().join("provider-completed");
        let submission_id = Uuid::new_v4();
        let submission =
            GatedCliSubmission::new(&root, submission_id).map_err(|error| error.to_string())?;
        let binding = GatedCliBinding::new(
            hex::encode(Sha256::digest(b"integration-binding")),
            Uuid::new_v4(),
            unix_time_ms() + release_after.as_millis() as u64,
        )
        .map_err(|error| error.to_string())?;
        let shell = Path::new("/bin/sh");
        let script = match mode {
            "success" => {
                "printf invoked > \"$1\"; printf '{\"submit_id\":\"task-1\"}'; printf provider-stderr >&2"
            }
            "timeout" => "printf invoked > \"$1\"; /bin/sleep 30",
            "absolute_deadline" => "printf invoked > \"$1\"; /bin/sleep 30",
            "orphan" => "printf invoked > \"$1\"; /bin/sleep 30; printf completed > \"$2\"",
            "residual" => "printf invoked > \"$1\"; /bin/sleep 30 &",
            "large_output" => "i=0; while [ \"$i\" -lt 70000 ]; do printf x; i=$((i + 1)); done",
            _ => return Err("unknown fixture mode".to_owned()),
        };
        let command = GatedCliCommand::new(
            shell,
            file_sha256(shell)?,
            temp.path(),
            vec![
                "-c".to_owned(),
                script.to_owned(),
                "gated-cli-test".to_owned(),
                side_effect.to_string_lossy().into_owned(),
                completed.to_string_lossy().into_owned(),
            ],
            BTreeMap::new(),
            Vec::new(),
            match mode {
                "timeout" => Duration::from_millis(250),
                "absolute_deadline" | "orphan" | "residual" => Duration::from_secs(30),
                "large_output" => Duration::from_secs(10),
                _ => Duration::from_secs(2),
            },
            if mode == "absolute_deadline" {
                Duration::from_secs(5)
            } else {
                Duration::from_millis(100)
            },
        )
        .map_err(|error| error.to_string())?;
        submission
            .prepare(&binding, &command)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            _temp: temp,
            root,
            side_effect,
            completed,
            submission,
            binding,
        })
    }

    fn spawn_runner(&self) -> Result<Child, String> {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
        command
            .arg(&self.root)
            .arg(self.submission.submission_id().to_string())
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        command.spawn().map_err(|error| error.to_string())
    }

    async fn wait_ready(&self) -> Result<gpt_image_2_gateway::GatedCliReady, String> {
        timeout(OBSERVATION_TIMEOUT, async {
            loop {
                match self
                    .submission
                    .observe(&self.binding)
                    .map_err(|error| error.to_string())?
                {
                    GatedCliObservation::Ready(ready) => return Ok(ready),
                    GatedCliObservation::AwaitingHelper | GatedCliObservation::Starting => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    observation => {
                        return Err(format!(
                            "unexpected pre-release observation: {observation:?}"
                        ));
                    }
                }
            }
        })
        .await
        .map_err(|_| "runner did not become ready".to_owned())?
    }

    async fn wait_terminal(&self) -> Result<gpt_image_2_gateway::GatedCliProcessTerminal, String> {
        timeout(OBSERVATION_TIMEOUT, async {
            loop {
                match self
                    .submission
                    .observe(&self.binding)
                    .map_err(|error| error.to_string())?
                {
                    GatedCliObservation::Terminal(terminal) => return Ok(terminal),
                    GatedCliObservation::AwaitingHelper
                    | GatedCliObservation::Starting
                    | GatedCliObservation::Ready(_)
                    | GatedCliObservation::Running => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    observation => {
                        return Err(format!("unexpected terminal observation: {observation:?}"));
                    }
                }
            }
        })
        .await
        .map_err(|_| "runner did not publish terminal evidence".to_owned())?
    }

    async fn wait_for_side_effect(&self) -> Result<(), String> {
        timeout(OBSERVATION_TIMEOUT, async {
            while !self.side_effect.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "fake provider did not start".to_owned())
    }

    fn child_pid(&self) -> Result<libc::pid_t, String> {
        let path = self
            .root
            .join(self.submission.submission_id().simple().to_string())
            .join("process-ready.json");
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(path).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        value
            .get("pid")
            .and_then(serde_json::Value::as_u64)
            .and_then(|pid| libc::pid_t::try_from(pid).ok())
            .ok_or_else(|| "process-ready.json did not contain a valid pid".to_owned())
    }
}

fn file_sha256(path: &Path) -> Result<String, String> {
    fs::read(path)
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
        .map_err(|error| error.to_string())
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
