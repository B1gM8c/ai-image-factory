use std::{
    env,
    fs::{self, File},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{process::Child, time::timeout};

use super::{ADMIN_TOKEN, API_TOKEN, TestDatabase, TestResult, combine_results, require};

const HEALTH_TIMEOUT: Duration = Duration::from_secs(10);
const HEALTH_TIMEOUT_ENV: &str = "PROCESS_SMOKE_HEALTH_TIMEOUT_SECS";
const EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const STARTUP_ATTEMPTS: usize = 3;

pub(crate) struct SmokeFiles {
    _root: TempDir,
    pub(crate) argv_log: PathBuf,
    pub(crate) stdin_log: PathBuf,
    pub(crate) fake_pid_log: PathBuf,
    pub(crate) invocation_log: PathBuf,
    pub(crate) artifact_root: PathBuf,
    fake_bin: PathBuf,
    codex_home: PathBuf,
    fake_active_pid: PathBuf,
    gateway_log: PathBuf,
}

impl SmokeFiles {
    pub(crate) fn new(fixture: &[u8]) -> TestResult<Self> {
        let root =
            tempfile::tempdir().map_err(|error| format!("failed to create temp root: {error}"))?;
        let fake_bin = root.path().join("fake-bin");
        let codex_home = root.path().join("codex-home");
        let argv_log = root.path().join("codex.argv");
        let stdin_log = root.path().join("codex.stdin");
        let fake_pid_log = root.path().join("codex.pid");
        let invocation_log = root.path().join("codex.invocations");
        let fake_active_pid = root.path().join("codex.active.pid");
        let fixture_path = root.path().join("opaque.png");
        let gateway_log = root.path().join("gateway.log");
        let artifact_root = root.path().join("artifacts");
        fs::create_dir_all(&fake_bin)
            .map_err(|error| format!("failed to create fake bin: {error}"))?;
        fs::create_dir_all(&codex_home)
            .map_err(|error| format!("failed to create Codex home: {error}"))?;
        fs::create_dir_all(&artifact_root)
            .map_err(|error| format!("failed to create artifact root: {error}"))?;
        fs::write(&fixture_path, fixture)
            .map_err(|error| format!("failed to write PNG fixture: {error}"))?;

        let script_path = fake_bin.join("codex");
        let script = fake_codex_script(
            &codex_home,
            &argv_log,
            &stdin_log,
            &fake_pid_log,
            &invocation_log,
            &fake_active_pid,
            &fixture_path,
        );
        fs::write(&script_path, script)
            .map_err(|error| format!("failed to write fake Codex: {error}"))?;
        let mut permissions = fs::metadata(&script_path)
            .map_err(|error| format!("failed to stat fake Codex: {error}"))?
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&script_path, permissions)
            .map_err(|error| format!("failed to make fake Codex executable: {error}"))?;

        Ok(Self {
            _root: root,
            argv_log,
            stdin_log,
            fake_pid_log,
            invocation_log,
            artifact_root,
            fake_bin,
            codex_home,
            fake_active_pid,
            gateway_log,
        })
    }
}

pub(crate) struct GatewayProcess {
    child: Child,
    pid: u32,
    log_path: PathBuf,
    fake_active_pid: PathBuf,
    exit_status: Option<ExitStatus>,
}

impl GatewayProcess {
    fn start(
        database: &TestDatabase,
        files: &SmokeFiles,
        reserved_listener: TcpListener,
        address: SocketAddr,
    ) -> TestResult<Self> {
        let log = File::create(&files.gateway_log)
            .map_err(|error| format!("failed to create gateway log: {error}"))?;
        let stderr = log
            .try_clone()
            .map_err(|error| format!("failed to clone gateway log: {error}"))?;
        let inherited_path = env::var_os("PATH").unwrap_or_default();
        let path = env::join_paths(
            std::iter::once(files.fake_bin.as_os_str()).chain(
                env::split_paths(&inherited_path)
                    .map(|path| path.as_os_str().to_owned())
                    .collect::<Vec<_>>()
                    .iter()
                    .map(|path| path.as_os_str()),
            ),
        )
        .map_err(|error| format!("failed to build fake-first PATH: {error}"))?;

        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_gpt-image-2-gateway"));
        command
            .env_clear()
            .env("PATH", path)
            .env("DATABASE_URL", database.database_url())
            .env("GATEWAY_DATABASE_SCHEMA", database.schema())
            .env("GATEWAY_BIND", address.to_string())
            .env("GATEWAY_API_TOKEN", API_TOKEN)
            .env("GATEWAY_ADMIN_TOKEN", ADMIN_TOKEN)
            .env(
                "GATEWAY_API_KEY_PEPPERS",
                "1:1111111111111111111111111111111111111111111111111111111111111111",
            )
            .env("GATEWAY_API_KEY_CURRENT_PEPPER_VERSION", "1")
            .env("GATEWAY_CODEX_HOME", &files.codex_home)
            .env("GATEWAY_ARTIFACT_ROOT", &files.artifact_root)
            .env("GATEWAY_CLEANUP_CODEX_OUTPUTS", "true")
            .env("GATEWAY_QUEUE_TIMEOUT_SECS", "1")
            .env("GATEWAY_REQUEST_TIMEOUT_SECS", "5")
            .env("GATEWAY_MAX_CONCURRENT_JOBS", "1")
            .env("GATEWAY_MAX_QUEUE_SIZE", "0")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        command.process_group(0);
        drop(reserved_listener);
        let child = command
            .spawn()
            .map_err(|error| format!("failed to start production gateway binary: {error}"))?;
        let pid = child
            .id()
            .ok_or_else(|| "gateway PID unavailable after spawn".to_string())?;
        Ok(Self {
            child,
            pid,
            log_path: files.gateway_log.clone(),
            fake_active_pid: files.fake_active_pid.clone(),
            exit_status: None,
        })
    }

    pub(crate) async fn terminate(&mut self) -> TestResult {
        if let Some(status) = self.poll_exit()? {
            return Err(format!(
                "gateway exited before SIGTERM with {status}: {}",
                self.logs()
            ));
        }
        if let Err(error) = signal_process_group(self.pid, libc::SIGTERM) {
            let cleanup = self.terminate_abnormally().await;
            return combine_results(Err(error), cleanup, "forced gateway cleanup");
        }

        match timeout(EXIT_TIMEOUT, self.child.wait()).await {
            Ok(Ok(status)) => {
                self.exit_status = Some(status);
                require(
                    status.success(),
                    format!("gateway SIGTERM exit was {status}: {}", self.logs()),
                )
            }
            Ok(Err(error)) => {
                let cleanup = self.terminate_abnormally().await;
                combine_results(
                    Err(format!("failed waiting for gateway SIGTERM exit: {error}")),
                    cleanup,
                    "forced gateway cleanup",
                )
            }
            Err(_) => {
                let cleanup = self.terminate_abnormally().await;
                combine_results(
                    Err(format!(
                        "gateway did not exit within {EXIT_TIMEOUT:?} after SIGTERM: {}",
                        self.logs()
                    )),
                    cleanup,
                    "forced gateway cleanup",
                )
            }
        }
    }

    async fn terminate_abnormally(&mut self) -> TestResult {
        kill_fake_codex_if_active(&self.fake_active_pid);
        if self.poll_exit()?.is_some() {
            return Ok(());
        }
        let _ = signal_process_group(self.pid, libc::SIGKILL);
        let _ = self.child.start_kill();
        match timeout(Duration::from_secs(2), self.child.wait()).await {
            Ok(Ok(status)) => {
                self.exit_status = Some(status);
                Ok(())
            }
            Ok(Err(error)) => Err(format!(
                "failed to reap gateway after forced cleanup: {error}"
            )),
            Err(_) => Err("timed out reaping gateway after forced cleanup".to_string()),
        }
    }

    fn poll_exit(&mut self) -> TestResult<Option<ExitStatus>> {
        if self.exit_status.is_some() {
            return Ok(self.exit_status);
        }
        let status = self
            .child
            .try_wait()
            .map_err(|error| format!("failed to inspect gateway process: {error}"))?;
        if status.is_some() {
            self.exit_status = status;
        }
        Ok(status)
    }

    fn logs(&self) -> String {
        fs::read_to_string(&self.log_path)
            .unwrap_or_else(|_| "<gateway log unavailable>".to_string())
    }
}

impl Drop for GatewayProcess {
    fn drop(&mut self) {
        kill_fake_codex_if_active(&self.fake_active_pid);
        if self.exit_status.is_none() {
            let _ = signal_process_group(self.pid, libc::SIGKILL);
            let _ = self.child.start_kill();
        }
    }
}

pub(crate) async fn start_gateway_with_retry(
    client: &reqwest::Client,
    database: &TestDatabase,
    files: &SmokeFiles,
) -> TestResult<(GatewayProcess, SocketAddr)> {
    let mut failures = Vec::new();
    for attempt in 1..=STARTUP_ATTEMPTS {
        let reserved_listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .map_err(|error| format!("failed to reserve loopback port: {error}"))?;
        let address = reserved_listener
            .local_addr()
            .map_err(|error| format!("failed to inspect reserved loopback port: {error}"))?;
        let mut gateway = GatewayProcess::start(database, files, reserved_listener, address)?;
        let health = poll_health(client, &format!("http://{address}"), &mut gateway).await;
        if health.is_ok() {
            return Ok((gateway, address));
        }

        let retryable = startup_failed_from_address_in_use(&gateway.logs());
        let cleanup = gateway.terminate_abnormally().await;
        let failure = match combine_results(
            health,
            cleanup,
            &format!("startup attempt {attempt} cleanup"),
        ) {
            Err(error) => error,
            Ok(()) => {
                "startup health unexpectedly succeeded after entering failure cleanup".to_string()
            }
        };
        if !retryable {
            return Err(failure);
        }
        failures.push(format!("attempt {attempt}: {failure}"));
    }

    Err(format!(
        "gateway exhausted {STARTUP_ATTEMPTS} startup attempts after loopback address-in-use failures:\n{}",
        failures.join("\n")
    ))
}

pub(crate) async fn poll_health(
    client: &reqwest::Client,
    base_url: &str,
    gateway: &mut GatewayProcess,
) -> TestResult {
    timeout(health_timeout(), async {
        loop {
            if let Some(status) = gateway.poll_exit()? {
                return Err(format!(
                    "gateway exited before health check with {status}: {}",
                    gateway.logs()
                ));
            }
            if let Ok(response) = client.get(format!("{base_url}/healthz")).send().await
                && response.status() == reqwest::StatusCode::OK
                && gateway.logs().contains("gpt-image-2 gateway listening")
            {
                let body: Value = response
                    .json()
                    .await
                    .map_err(|error| format!("health response was not JSON: {error}"))?;
                if body == json!({"status": "ok"}) {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| format!("gateway health check timed out: {}", gateway.logs()))?
}

fn health_timeout() -> Duration {
    env::var(HEALTH_TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
        .unwrap_or(HEALTH_TIMEOUT)
}

pub(crate) fn startup_failed_from_address_in_use(logs: &str) -> bool {
    logs.contains("failed to bind HTTP listener")
}

pub(crate) fn read_pid(path: &Path) -> TestResult<i32> {
    fs::read_to_string(path)
        .map_err(|error| format!("failed to read PID log {}: {error}", path.display()))?
        .trim()
        .parse::<i32>()
        .map_err(|error| format!("invalid PID log {}: {error}", path.display()))
}

fn fake_codex_script(
    codex_home: &Path,
    argv_log: &Path,
    stdin_log: &Path,
    fake_pid_log: &Path,
    invocation_log: &Path,
    fake_active_pid: &Path,
    fixture: &Path,
) -> String {
    format!(
        r#"#!/bin/sh
set -eu
[ "${{HOME-}}" = "${{CODEX_HOME-}}" ] || exit 20
[ "${{HOME-}}" = {codex_home} ] || exit 21
[ -z "${{GATEWAY_API_TOKEN+x}}" ] || exit 22
[ -z "${{GATEWAY_ADMIN_TOKEN+x}}" ] || exit 23
[ -z "${{DATABASE_URL+x}}" ] || exit 24
[ -z "${{GATEWAY_DATABASE_URL+x}}" ] || exit 25
[ -z "${{TEST_DATABASE_URL+x}}" ] || exit 26
[ -z "${{GATEWAY_DATABASE_SCHEMA+x}}" ] || exit 29
[ -z "${{GATEWAY_API_KEY_PEPPERS+x}}" ] || exit 30
[ -z "${{GATEWAY_API_KEY_CURRENT_PEPPER_VERSION+x}}" ] || exit 31
printf '%s\n' "$$" > {fake_pid_log}
printf 'invoked\n' >> {invocation_log}
printf '%s\n' "$$" > {fake_active_pid}
trap 'rm -f {fake_active_pid}' EXIT
printf '%s\0' "$@" > {argv_log}
cat > {stdin_log}
request_dir=
while [ "$#" -gt 0 ]; do
    if [ "$1" = "--cd" ]; then
        shift
        [ "$#" -gt 0 ] || exit 27
        request_dir=$1
        break
    fi
    shift
done
[ -n "$request_dir" ] || exit 28
cp {fixture} "$request_dir/final.png"
"#,
        codex_home = shell_quote(codex_home),
        argv_log = shell_quote(argv_log),
        stdin_log = shell_quote(stdin_log),
        fake_pid_log = shell_quote(fake_pid_log),
        invocation_log = shell_quote(invocation_log),
        fake_active_pid = shell_quote(fake_active_pid),
        fixture = shell_quote(fixture),
    )
}

fn signal_process_group(pid: u32, signal: libc::c_int) -> TestResult {
    let result = unsafe { libc::kill(-(pid as libc::pid_t), signal) };
    if result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(format!(
            "failed to send signal {signal} to gateway process group {pid}: {}",
            std::io::Error::last_os_error()
        ))
    }
}

fn kill_fake_codex_if_active(active_pid_path: &Path) {
    let Ok(pid) = read_pid(active_pid_path) else {
        return;
    };
    unsafe {
        libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        libc::kill(pid as libc::pid_t, libc::SIGKILL);
    }
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}
