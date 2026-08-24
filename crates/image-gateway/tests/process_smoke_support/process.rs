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
const MAX_DIAGNOSTIC_LOG_LINES: usize = 200;
const STARTUP_ATTEMPTS: usize = 3;

pub(crate) struct SmokeFiles {
    _root: TempDir,
    pub(crate) argv_log: PathBuf,
    pub(crate) stdin_log: PathBuf,
    pub(crate) fake_pid_log: PathBuf,
    pub(crate) fake_parent_pid_log: PathBuf,
    pub(crate) invocation_log: PathBuf,
    pub(crate) artifact_root: PathBuf,
    runner_root: PathBuf,
    fake_bin: PathBuf,
    codex_home: PathBuf,
    fake_active_pid: PathBuf,
    fake_delay: PathBuf,
    gateway_log: PathBuf,
    workerd_log: PathBuf,
    executord_log: PathBuf,
    reducerd_log: PathBuf,
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
        let fake_parent_pid_log = root.path().join("codex.ppid");
        let invocation_log = root.path().join("codex.invocations");
        let fake_active_pid = root.path().join("codex.active.pid");
        let fake_delay = root.path().join("codex.delay-seconds");
        let fixture_path = root.path().join("opaque.png");
        let second_fixture_path = root.path().join("opaque-second.png");
        let gateway_log = root.path().join("gateway.log");
        let workerd_log = root.path().join("workerd.log");
        let executord_log = root.path().join("executord.log");
        let reducerd_log = root.path().join("reducerd.log");
        let artifact_root = root.path().join("artifacts");
        let runner_root = root.path().join("runner");
        fs::create_dir_all(&fake_bin)
            .map_err(|error| format!("failed to create fake bin: {error}"))?;
        fs::create_dir_all(&codex_home)
            .map_err(|error| format!("failed to create Codex home: {error}"))?;
        fs::create_dir_all(&artifact_root)
            .map_err(|error| format!("failed to create artifact root: {error}"))?;
        fs::create_dir_all(&runner_root)
            .map_err(|error| format!("failed to create runner root: {error}"))?;
        for private_root in [&codex_home, &artifact_root, &runner_root] {
            fs::set_permissions(private_root, fs::Permissions::from_mode(0o700)).map_err(
                |error| {
                    format!(
                        "failed to protect private root {}: {error}",
                        private_root.display()
                    )
                },
            )?;
        }
        let auth_path = codex_home.join("auth.json");
        fs::write(
            &auth_path,
            br#"{"auth_mode":"chatgpt","OPENAI_API_KEY":null,"tokens":{"id_token":"process-smoke-id","access_token":"process-smoke-access","refresh_token":"process-smoke-refresh","account_id":"process-smoke-account"}}
"#,
        )
            .map_err(|error| format!("failed to write fake Codex credentials: {error}"))?;
        fs::set_permissions(&auth_path, fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("failed to protect fake Codex credentials: {error}"))?;
        fs::write(&fixture_path, fixture)
            .map_err(|error| format!("failed to write PNG fixture: {error}"))?;
        fs::write(&second_fixture_path, fixture)
            .map_err(|error| format!("failed to write second PNG fixture: {error}"))?;
        fs::write(&fake_delay, "0")
            .map_err(|error| format!("failed to initialize fake Codex delay: {error}"))?;

        let script_path = fake_bin.join("codex");
        let script = fake_codex_script(FakeCodexPaths {
            codex_home: &codex_home,
            argv_log: &argv_log,
            stdin_log: &stdin_log,
            fake_pid_log: &fake_pid_log,
            fake_parent_pid_log: &fake_parent_pid_log,
            invocation_log: &invocation_log,
            fake_active_pid: &fake_active_pid,
            fake_delay: &fake_delay,
            fixture: &fixture_path,
            second_fixture: &second_fixture_path,
        });
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
            fake_parent_pid_log,
            invocation_log,
            artifact_root,
            runner_root,
            fake_bin,
            codex_home,
            fake_active_pid,
            fake_delay,
            gateway_log,
            workerd_log,
            executord_log,
            reducerd_log,
        })
    }

    pub(crate) fn set_fake_codex_delay(&self, delay: Duration) -> TestResult {
        require(
            !delay.is_zero() && delay.subsec_nanos() == 0,
            "fake Codex delay must be a positive whole number of seconds",
        )?;
        fs::write(&self.fake_delay, delay.as_secs().to_string())
            .map_err(|error| format!("failed to configure fake Codex delay: {error}"))
    }

    pub(crate) fn set_second_fixture(&self, fixture: &[u8]) -> TestResult {
        fs::write(self._root.path().join("opaque-second.png"), fixture)
            .map_err(|error| format!("failed to write distinct second PNG fixture: {error}"))
    }

    pub(crate) async fn wait_for_fake_codex_active(&self) -> TestResult {
        timeout(HEALTH_TIMEOUT, async {
            loop {
                if read_pid(&self.fake_active_pid).is_ok() {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "fake Codex did not become active before timeout".to_string())?
    }

    pub(crate) fn codex_auth_file_sha256(&self) -> TestResult<String> {
        gpt_image_2_gateway::codex_auth_file_sha256(&self.codex_home)
            .map_err(|error| format!("failed to hash fake Codex auth.json: {error:?}"))
    }

    pub(crate) fn codex_credential_home(&self) -> &Path {
        &self.codex_home
    }

    pub(crate) fn process_diagnostics(&self) -> String {
        [
            ("gateway", &self.gateway_log),
            ("workerd", &self.workerd_log),
            ("executord", &self.executord_log),
            ("reducerd", &self.reducerd_log),
        ]
        .into_iter()
        .map(|(name, path)| {
            let log = fs::read_to_string(path).unwrap_or_else(|_| "<log unavailable>".to_string());
            let lines = log.lines().collect::<Vec<_>>();
            let omitted = lines.len().saturating_sub(MAX_DIAGNOSTIC_LOG_LINES);
            let tail = lines
                .into_iter()
                .skip(omitted)
                .collect::<Vec<_>>()
                .join("\n");
            let prefix = if omitted == 0 {
                String::new()
            } else {
                format!("<{omitted} earlier lines omitted>\n")
            };
            format!("--- {name} ---\n{prefix}{tail}")
        })
        .collect::<Vec<_>>()
        .join("\n")
    }
}

pub(crate) struct WorkerdProcess {
    child: Child,
    pid: u32,
    log_path: PathBuf,
    exit_status: Option<ExitStatus>,
}

impl WorkerdProcess {
    pub(crate) async fn start(database: &TestDatabase, files: &SmokeFiles) -> TestResult<Self> {
        Self::start_with_direct_edit_endpoint(database, files, None).await
    }

    pub(crate) async fn start_with_direct_edit_endpoint(
        database: &TestDatabase,
        files: &SmokeFiles,
        direct_edit_endpoint: Option<&str>,
    ) -> TestResult<Self> {
        let log = File::create(&files.workerd_log)
            .map_err(|error| format!("failed to create workerd log: {error}"))?;
        let stderr = log
            .try_clone()
            .map_err(|error| format!("failed to clone workerd log: {error}"))?;
        let inherited_path = env::var_os("PATH").unwrap_or_default();
        let path = env::join_paths(
            std::iter::once(files.fake_bin.clone()).chain(env::split_paths(&inherited_path)),
        )
        .map_err(|error| format!("failed to build workerd PATH: {error}"))?;
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_workerd"));
        command
            .env_clear()
            .env("PATH", path)
            .env("DATABASE_URL", database.database_url())
            .env("GATEWAY_DATABASE_SCHEMA", database.schema())
            .env("GATEWAY_IMAGES_GENERATION_CONTRACT", "legacy-v1")
            .env("GATEWAY_CODEX_HOME", &files.codex_home)
            .env("GATEWAY_ARTIFACT_ROOT", &files.artifact_root)
            .env("GATEWAY_CLEANUP_CODEX_OUTPUTS", "true")
            .env("GATEWAY_REQUEST_TIMEOUT_SECS", "15")
            .env("WORKER_ID", "process-smoke-workerd")
            .env("WORKER_POLL_INTERVAL_MS", "10")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        if let Some(endpoint) = direct_edit_endpoint {
            command.env("GATEWAY_TEST_CODEX_IMAGE_EDITS_URL", endpoint);
        }
        command.process_group(0);
        let child = command
            .spawn()
            .map_err(|error| format!("failed to start workerd binary: {error}"))?;
        let pid = child
            .id()
            .ok_or_else(|| "workerd PID unavailable after spawn".to_string())?;
        let mut process = Self {
            child,
            pid,
            log_path: files.workerd_log.clone(),
            exit_status: None,
        };
        timeout(HEALTH_TIMEOUT, async {
            loop {
                if let Some(status) = process.poll_exit()? {
                    return Err(format!(
                        "workerd exited during startup with {status}: {}",
                        process.logs()
                    ));
                }
                if process.logs().contains("workerd started") {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| format!("workerd startup timed out: {}", process.logs()))??;
        Ok(process)
    }

    pub(crate) async fn start_handoff(
        database: &TestDatabase,
        files: &SmokeFiles,
        profile_key: &str,
    ) -> TestResult<Self> {
        let log = File::create(&files.workerd_log)
            .map_err(|error| format!("failed to create V2 workerd log: {error}"))?;
        let stderr = log
            .try_clone()
            .map_err(|error| format!("failed to clone V2 workerd log: {error}"))?;
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_workerd"));
        command
            .env_clear()
            .env("DATABASE_URL", database.database_url())
            .env("GATEWAY_DATABASE_SCHEMA", database.schema())
            .env("GATEWAY_IMAGES_GENERATION_CONTRACT", "output-economics-v2")
            .env("WORKER_EXECUTION_MODE", "executor-handoff")
            .env("EXECUTOR_PROFILE_KEY", profile_key)
            .env("WORKER_ID", "process-smoke-v2-workerd")
            .env("WORKER_POLL_INTERVAL_MS", "10")
            .env("WORKER_HANDOFF_LEASE_MS", "5000")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        command.process_group(0);
        let child = command
            .spawn()
            .map_err(|error| format!("failed to start V2 workerd binary: {error}"))?;
        let pid = child
            .id()
            .ok_or_else(|| "V2 workerd PID unavailable after spawn".to_string())?;
        let mut process = Self {
            child,
            pid,
            log_path: files.workerd_log.clone(),
            exit_status: None,
        };
        timeout(HEALTH_TIMEOUT, async {
            loop {
                if let Some(status) = process.poll_exit()? {
                    return Err(format!(
                        "V2 workerd exited during startup with {status}: {}",
                        process.logs()
                    ));
                }
                if process.logs().contains("workerd started") {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| format!("V2 workerd startup timed out: {}", process.logs()))??;
        Ok(process)
    }

    pub(crate) fn pid(&self) -> u32 {
        self.pid
    }

    pub(crate) async fn terminate(&mut self) -> TestResult {
        if let Some(status) = self.poll_exit()? {
            return Err(format!(
                "workerd exited before SIGTERM with {status}: {}",
                self.logs()
            ));
        }
        signal_process_group(self.pid, libc::SIGTERM)?;
        match timeout(EXIT_TIMEOUT, self.child.wait()).await {
            Ok(Ok(status)) => {
                self.exit_status = Some(status);
                require(
                    status.success(),
                    format!("workerd SIGTERM exit was {status}: {}", self.logs()),
                )
            }
            Ok(Err(error)) => Err(format!("failed waiting for workerd exit: {error}")),
            Err(_) => Err(format!(
                "workerd did not exit within {EXIT_TIMEOUT:?}: {}",
                self.logs()
            )),
        }
    }

    fn poll_exit(&mut self) -> TestResult<Option<ExitStatus>> {
        if self.exit_status.is_some() {
            return Ok(self.exit_status);
        }
        let status = self
            .child
            .try_wait()
            .map_err(|error| format!("failed to inspect workerd process: {error}"))?;
        if status.is_some() {
            self.exit_status = status;
        }
        Ok(status)
    }

    fn logs(&self) -> String {
        fs::read_to_string(&self.log_path)
            .unwrap_or_else(|_| "<workerd log unavailable>".to_string())
    }
}

impl Drop for WorkerdProcess {
    fn drop(&mut self) {
        if self.exit_status.is_none() {
            let _ = signal_process_group(self.pid, libc::SIGKILL);
            let _ = self.child.start_kill();
        }
    }
}

pub(crate) struct ExecutordProcess(ManagedProcess);

impl ExecutordProcess {
    pub(crate) async fn start(
        database: &TestDatabase,
        files: &SmokeFiles,
        profile_key: &str,
        credential_ref: &str,
    ) -> TestResult<Self> {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_executord"));
        command
            .env_clear()
            .env("DATABASE_URL", database.database_url())
            .env("GATEWAY_DATABASE_SCHEMA", database.schema())
            .env("GATEWAY_ARTIFACT_ROOT", &files.artifact_root)
            .env("EXECUTOR_RUNNER_ROOT", &files.runner_root)
            .env(
                "EXECUTOR_HELPER_EXECUTABLE",
                env!("CARGO_BIN_EXE_codex-runner"),
            )
            .env("EXECUTOR_CODEX_EXECUTABLE", files.fake_bin.join("codex"))
            .env("EXECUTOR_CODEX_CREDENTIAL_HOME", &files.codex_home)
            .env("EXECUTOR_OWNER", "process-smoke-v2-executord")
            .env("EXECUTOR_PROFILE_KEY", profile_key)
            .env("EXECUTOR_CREDENTIAL_REF", credential_ref)
            .env("EXECUTOR_CREDENTIAL_REVISION", "1")
            .env("EXECUTOR_LEASE_MS", "10000")
            .env("EXECUTOR_HEARTBEAT_INTERVAL_MS", "250")
            .env("EXECUTOR_POLL_INTERVAL_MS", "10")
            .env("EXECUTOR_PROCESS_POLL_INTERVAL_MS", "10")
            .env("EXECUTOR_PROCESS_STARTUP_GRACE_MS", "10000")
            .env("EXECUTOR_REQUEST_TIMEOUT_MS", "15000")
            .env("EXECUTOR_OWNER_GUARD_TIMEOUT_MS", "1000")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .kill_on_drop(true);
        ManagedProcess::start(
            command,
            &files.executord_log,
            "executord",
            "executord started",
        )
        .await
        .map(Self)
    }

    pub(crate) async fn terminate(&mut self) -> TestResult {
        self.0.terminate().await
    }
}

pub(crate) struct ReducerdProcess(ManagedProcess);

impl ReducerdProcess {
    pub(crate) async fn start(database: &TestDatabase, files: &SmokeFiles) -> TestResult<Self> {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_reducerd"));
        command
            .env_clear()
            .env("DATABASE_URL", database.database_url())
            .env("GATEWAY_DATABASE_SCHEMA", database.schema())
            .env("GATEWAY_ARTIFACT_ROOT", &files.artifact_root)
            .env("REDUCER_OWNER", "process-smoke-v2-reducerd")
            .env("REDUCER_LEASE_MS", "10000")
            .env("REDUCER_HEARTBEAT_INTERVAL_MS", "250")
            .env("REDUCER_POLL_INTERVAL_MS", "10")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .kill_on_drop(true);
        ManagedProcess::start(command, &files.reducerd_log, "reducerd", "reducerd started")
            .await
            .map(Self)
    }

    pub(crate) async fn terminate(&mut self) -> TestResult {
        self.0.terminate().await
    }
}

struct ManagedProcess {
    child: Child,
    pid: u32,
    log_path: PathBuf,
    name: &'static str,
    exit_status: Option<ExitStatus>,
}

impl ManagedProcess {
    async fn start(
        mut command: tokio::process::Command,
        log_path: &Path,
        name: &'static str,
        startup_marker: &str,
    ) -> TestResult<Self> {
        let log = File::create(log_path)
            .map_err(|error| format!("failed to create {name} log: {error}"))?;
        let stderr = log
            .try_clone()
            .map_err(|error| format!("failed to clone {name} log: {error}"))?;
        command.stdout(Stdio::from(log)).stderr(Stdio::from(stderr));
        command.process_group(0);
        let child = command
            .spawn()
            .map_err(|error| format!("failed to start {name} binary: {error}"))?;
        let pid = child
            .id()
            .ok_or_else(|| format!("{name} PID unavailable after spawn"))?;
        let mut process = Self {
            child,
            pid,
            log_path: log_path.to_path_buf(),
            name,
            exit_status: None,
        };
        timeout(HEALTH_TIMEOUT, async {
            loop {
                if let Some(status) = process.poll_exit()? {
                    return Err(format!(
                        "{name} exited during startup with {status}: {}",
                        process.logs()
                    ));
                }
                if process.logs().contains(startup_marker) {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| format!("{name} startup timed out: {}", process.logs()))??;
        Ok(process)
    }

    async fn terminate(&mut self) -> TestResult {
        if let Some(status) = self.poll_exit()? {
            return Err(format!(
                "{} exited before SIGTERM with {status}: {}",
                self.name,
                self.logs()
            ));
        }
        signal_process_group(self.pid, libc::SIGTERM)?;
        match timeout(EXIT_TIMEOUT, self.child.wait()).await {
            Ok(Ok(status)) => {
                self.exit_status = Some(status);
                require(
                    status.success(),
                    format!("{} SIGTERM exit was {status}: {}", self.name, self.logs()),
                )
            }
            Ok(Err(error)) => Err(format!("failed waiting for {} exit: {error}", self.name)),
            Err(_) => Err(format!(
                "{} did not exit within {EXIT_TIMEOUT:?}: {}",
                self.name,
                self.logs()
            )),
        }
    }

    fn poll_exit(&mut self) -> TestResult<Option<ExitStatus>> {
        if self.exit_status.is_some() {
            return Ok(self.exit_status);
        }
        let status = self
            .child
            .try_wait()
            .map_err(|error| format!("failed to inspect {} process: {error}", self.name))?;
        if status.is_some() {
            self.exit_status = status;
        }
        Ok(status)
    }

    fn logs(&self) -> String {
        fs::read_to_string(&self.log_path)
            .unwrap_or_else(|_| format!("<{} log unavailable>", self.name))
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        if self.exit_status.is_none() {
            let _ = signal_process_group(self.pid, libc::SIGKILL);
            let _ = self.child.start_kill();
        }
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
        generation_contract: Option<&str>,
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
            .env("GATEWAY_LEGACY_ADMIN_AUTH_ENABLED", "true")
            .env(
                "GATEWAY_API_KEY_PEPPERS",
                "1:1111111111111111111111111111111111111111111111111111111111111111",
            )
            .env("GATEWAY_API_KEY_CURRENT_PEPPER_VERSION", "1")
            .env(
                "GATEWAY_WEBHOOK_SIGNING_KEYS",
                "1:2222222222222222222222222222222222222222222222222222222222222222",
            )
            .env("GATEWAY_WEBHOOK_CURRENT_SIGNING_KEY_VERSION", "1")
            .env("GATEWAY_ARTIFACT_ROOT", &files.artifact_root)
            .env("GATEWAY_QUEUE_TIMEOUT_SECS", "1")
            .env("GATEWAY_REQUEST_TIMEOUT_SECS", "15")
            .env("GATEWAY_MAX_CONCURRENT_JOBS", "1")
            .env("GATEWAY_MAX_QUEUE_SIZE", "0")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        if let Some(generation_contract) = generation_contract {
            command.env("GATEWAY_IMAGES_GENERATION_CONTRACT", generation_contract);
        }
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
    start_gateway_with_contract(client, database, files, Some("legacy-v1")).await
}

pub(crate) async fn start_v2_gateway_with_retry(
    client: &reqwest::Client,
    database: &TestDatabase,
    files: &SmokeFiles,
) -> TestResult<(GatewayProcess, SocketAddr)> {
    start_gateway_with_contract(client, database, files, Some("output-economics-v2")).await
}

async fn start_gateway_with_contract(
    client: &reqwest::Client,
    database: &TestDatabase,
    files: &SmokeFiles,
    generation_contract: Option<&str>,
) -> TestResult<(GatewayProcess, SocketAddr)> {
    let mut failures = Vec::new();
    for attempt in 1..=STARTUP_ATTEMPTS {
        let reserved_listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .map_err(|error| format!("failed to reserve loopback port: {error}"))?;
        let address = reserved_listener
            .local_addr()
            .map_err(|error| format!("failed to inspect reserved loopback port: {error}"))?;
        let mut gateway = GatewayProcess::start(
            database,
            files,
            reserved_listener,
            address,
            generation_contract,
        )?;
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
                    let ready = client
                        .get(format!("{base_url}/readyz"))
                        .send()
                        .await
                        .map_err(|error| format!("readiness request failed: {error}"))?;
                    let status = ready.status();
                    let body: Value = ready
                        .json()
                        .await
                        .map_err(|error| format!("readiness response was not JSON: {error}"))?;
                    if status == reqwest::StatusCode::OK
                        && body["status"] == "ready"
                        && body["provider_profiles"].is_object()
                    {
                        return Ok(());
                    }
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

struct FakeCodexPaths<'a> {
    codex_home: &'a Path,
    argv_log: &'a Path,
    stdin_log: &'a Path,
    fake_pid_log: &'a Path,
    fake_parent_pid_log: &'a Path,
    invocation_log: &'a Path,
    fake_active_pid: &'a Path,
    fake_delay: &'a Path,
    fixture: &'a Path,
    second_fixture: &'a Path,
}

fn fake_codex_script(paths: FakeCodexPaths<'_>) -> String {
    format!(
        r#"#!/bin/sh
set -eu
refresh_mode=false
if [ "$#" -eq 2 ] && [ "$1" = "app-server" ] && [ "$2" = "--stdio" ]; then
    refresh_mode=true
fi
if [ "$refresh_mode" = false ]; then
    [ "${{HOME-}}" = "${{CODEX_HOME-}}" ] || exit 20
    if [ "${{HOME-}}" != {codex_home} ]; then
        [ -f "${{HOME-}}/auth.json" ] || exit 21
        /usr/bin/grep -F '"auth_mode":"chatgptAuthTokens"' "${{HOME-}}/auth.json" >/dev/null || exit 21
        /usr/bin/grep -F '"id_token":"process-smoke-id"' "${{HOME-}}/auth.json" >/dev/null || exit 21
        /usr/bin/grep -F '"access_token":"process-smoke-access"' "${{HOME-}}/auth.json" >/dev/null || exit 21
        /usr/bin/grep -F '"refresh_token":""' "${{HOME-}}/auth.json" >/dev/null || exit 21
        ! /usr/bin/grep -F 'process-smoke-refresh' "${{HOME-}}/auth.json" >/dev/null || exit 21
    fi
fi
[ -z "${{GATEWAY_API_TOKEN+x}}" ] || exit 22
[ -z "${{GATEWAY_ADMIN_TOKEN+x}}" ] || exit 23
[ -z "${{DATABASE_URL+x}}" ] || exit 24
[ -z "${{GATEWAY_DATABASE_URL+x}}" ] || exit 25
[ -z "${{TEST_DATABASE_URL+x}}" ] || exit 26
[ -z "${{GATEWAY_DATABASE_SCHEMA+x}}" ] || exit 29
[ -z "${{GATEWAY_API_KEY_PEPPERS+x}}" ] || exit 30
[ -z "${{GATEWAY_API_KEY_CURRENT_PEPPER_VERSION+x}}" ] || exit 31
printf '%s\n' "$$" > {fake_pid_log}
printf '%s\n' "$PPID" > {fake_parent_pid_log}
printf 'invoked\n' >> {invocation_log}
printf '%s\n' "$$" > {fake_active_pid}
trap 'rm -f {fake_active_pid}' EXIT
printf '%s\0' "$@" > {argv_log}
: > {stdin_log}
read_and_log() {{
    IFS= read -r line || exit 32
    printf '%s\n' "$line" >> {stdin_log}
}}
read_and_log
printf '{{"id":1,"result":{{"codexHome":"%s"}}}}\n' "$CODEX_HOME"
read_and_log
read_and_log
if printf '%s' "$line" | /usr/bin/grep -F '"method":"account/read"' >/dev/null; then
    printf '%s' "$line" | /usr/bin/grep -F '"refreshToken":true' >/dev/null
    printf '%s' '{{"auth_mode":"chatgpt","OPENAI_API_KEY":null,"tokens":{{"id_token":"process-smoke-id-refreshed","access_token":"process-smoke-access-refreshed","refresh_token":"process-smoke-refresh-refreshed","account_id":"process-smoke-account"}}}}' > "$CODEX_HOME/auth.json.next"
    chmod 600 "$CODEX_HOME/auth.json.next"
    mv "$CODEX_HOME/auth.json.next" "$CODEX_HOME/auth.json"
    printf '{{"id":3,"result":{{"account":{{"email":"process-smoke@example.invalid","planType":"test"}}}}}}\n'
    while IFS= read -r ignored; do :; done
    exit 0
fi
thread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'
turn_id='019fd9f5-badb-7dd3-8903-28ffded0ef55'
printf '{{"method":"thread/started","params":{{"thread":{{"id":"%s"}}}}}}\n' "$thread_id"
printf '{{"id":2,"result":{{"thread":{{"id":"%s"}}}}}}\n' "$thread_id"
read_and_log
printf '{{"method":"turn/started","params":{{"threadId":"%s","turn":{{"id":"%s"}}}}}}\n' "$thread_id" "$turn_id"
printf '{{"id":3,"result":{{"turn":{{"id":"%s"}}}}}}\n' "$turn_id"
sleep "$(cat {fake_delay})"
if /usr/bin/grep -q '第 2/2 张候选图片' {stdin_log}; then
    selected_fixture={second_fixture}
else
    selected_fixture={fixture}
fi
call_id="call_process_smoke_$$"
output_dir="$CODEX_HOME/generated_images/$thread_id"
mkdir -p "$output_dir"
chmod 700 "$CODEX_HOME/generated_images" "$output_dir"
cp "$selected_fixture" "$output_dir/$call_id.png.partial"
chmod 600 "$output_dir/$call_id.png.partial"
mv "$output_dir/$call_id.png.partial" "$output_dir/$call_id.png"
printf '{{"method":"item/started","params":{{"threadId":"%s","turnId":"%s","item":{{"type":"imageGeneration","id":"%s","status":"inProgress"}}}}}}\n' "$thread_id" "$turn_id" "$call_id"
printf '{{"method":"item/completed","params":{{"threadId":"%s","turnId":"%s","item":{{"type":"imageGeneration","id":"%s","status":"completed","result":"fixture","savedPath":"%s"}}}}}}\n' "$thread_id" "$turn_id" "$call_id" "$output_dir/$call_id.png"
printf '{{"method":"turn/completed","params":{{"threadId":"%s","turn":{{"id":"%s","status":"completed"}}}}}}\n' "$thread_id" "$turn_id"
while IFS= read -r ignored; do :; done
"#,
        codex_home = shell_quote(paths.codex_home),
        argv_log = shell_quote(paths.argv_log),
        stdin_log = shell_quote(paths.stdin_log),
        fake_pid_log = shell_quote(paths.fake_pid_log),
        fake_parent_pid_log = shell_quote(paths.fake_parent_pid_log),
        invocation_log = shell_quote(paths.invocation_log),
        fake_active_pid = shell_quote(paths.fake_active_pid),
        fake_delay = shell_quote(paths.fake_delay),
        fixture = shell_quote(paths.fixture),
        second_fixture = shell_quote(paths.second_fixture),
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
