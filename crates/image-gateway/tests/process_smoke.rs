#![cfg(unix)]

use std::{
    env,
    fs::{self, File},
    io::Cursor,
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use gpt_image_2_gateway::database::{connect_pool_with_search_path, run_migrations};
use image::{ImageBuffer, ImageFormat, Rgb};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, PgPool};
use tempfile::TempDir;
use tokio::{process::Child, time::timeout};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const API_TOKEN: &str = "process-smoke-api-secret";
const ADMIN_TOKEN: &str = "process-smoke-admin-secret";
const DATABASE_ENV: &str = "TEST_DATABASE_URL";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const STARTUP_ATTEMPTS: usize = 3;

// Like the other PostgreSQL integration tests, local runs skip without TEST_DATABASE_URL while CI
// fails closed so the process composition cannot silently go untested there.
#[tokio::test]
async fn production_process_composition_succeeds_when_test_database_is_configured() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = run_process_smoke(&database).await;
    let cleanup = database.cleanup().await;
    combine_results(result, cleanup, "schema cleanup")
}

#[test]
fn startup_bind_failure_is_retryable() -> TestResult {
    require(
        startup_failed_from_address_in_use(
            "Error: Config { message: \"failed to bind HTTP listener\" }",
        ),
        "loopback bind startup failure should be retryable",
    )
}

#[test]
fn prompt_contract_is_semantic_not_full_text_equality() -> TestResult {
    let request_dir = "/tmp/process-smoke-request";
    let prompt = format!(
        "prefix may evolve\n用户原始需求：process smoke opaque fixture\n尺寸 auto；质量 low；输出格式 png。\n不要再启动 codex、openai 或其它 AI CLI 子进程来委托生成。\n不要用 sips、ImageMagick、Python、Rust、ffmpeg、canvas 或其他本地图像处理工具裁切、拉伸、重采样、扩边、转绘或修改像素。\n请保存为 {request_dir}/final.png\nsuffix may evolve"
    );
    assert_prompt_semantics(&prompt, request_dir)
}

async fn run_process_smoke(database: &TestDatabase) -> TestResult {
    let fixture = opaque_png()?;
    let files = SmokeFiles::new(&fixture)?;
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| format!("failed to build HTTP client: {error}"))?;
    let (mut gateway, address) = start_gateway_with_retry(&client, database, &files).await?;

    let result = exercise_gateway(&client, address, database, &files, &fixture, &mut gateway).await;
    let shutdown = gateway.terminate().await;
    combine_results(result, shutdown, "gateway shutdown")
}

async fn start_gateway_with_retry(
    client: &reqwest::Client,
    database: &TestDatabase,
    files: &SmokeFiles,
) -> TestResult<(GatewayProcess, std::net::SocketAddr)> {
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

        let logs = gateway.logs();
        let retryable = startup_failed_from_address_in_use(&logs);
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

async fn exercise_gateway(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    database: &TestDatabase,
    files: &SmokeFiles,
    fixture: &[u8],
    gateway: &mut GatewayProcess,
) -> TestResult {
    let base_url = format!("http://{address}");
    poll_health(client, &base_url, gateway).await?;

    let response = client
        .post(format!("{base_url}/v1/images/generations"))
        .bearer_auth(API_TOKEN)
        .json(&json!({
            "model": "gpt-image-2",
            "prompt": "process smoke opaque fixture",
            "n": 1,
            "size": "auto",
            "quality": "low",
            "output_format": "png"
        }))
        .send()
        .await
        .map_err(|error| format!("generation request failed: {error}"))?;

    let status = response.status();
    let headers = response.headers().clone();
    let request_id = header(&headers, "x-request-id")?;
    let body: Value = response
        .json()
        .await
        .map_err(|error| format!("generation response was not JSON: {error}"))?;
    require(
        status == reqwest::StatusCode::OK,
        format!("generation returned {status}: {body:#}"),
    )?;
    assert_response(&body, &headers, fixture)?;

    let argv = read_nul_strings(&files.argv_log)?;
    let request_dir = argv_value(&argv, "--cd")?;
    assert_codex_invocation(&argv, &request_dir)?;
    let prompt = fs::read_to_string(&files.stdin_log)
        .map_err(|error| format!("failed to read fake Codex stdin log: {error}"))?;
    assert_prompt_semantics(&prompt, &request_dir)?;
    let fake_pid = read_pid(&files.fake_pid_log)?;
    require(
        fake_pid > 0,
        "fake Codex PID log must contain a positive PID",
    )?;
    require(
        !Path::new(&request_dir).exists(),
        format!("cleaned request directory still exists: {request_dir}"),
    )?;

    assert_database_transitions(&database.pool, &request_id).await
}

fn assert_response(
    body: &Value,
    headers: &reqwest::header::HeaderMap,
    fixture: &[u8],
) -> TestResult {
    require(
        body["created"].as_i64().is_some_and(|value| value > 0),
        "missing created metadata",
    )?;
    require(
        body["output_format"] == "png",
        "output_format metadata was not png",
    )?;
    require(body["quality"] == "low", "quality metadata was not low")?;
    require(body["size"] == "2x1", "size metadata was not 2x1")?;
    require(
        body["background"] == "opaque",
        "background metadata was not opaque",
    )?;
    require(
        header(headers, "openai-project")? == "proj_default",
        "unexpected project metadata",
    )?;
    require(
        header(headers, "x-image-units-limit-5h")? == "40",
        "unexpected 5h limit metadata",
    )?;
    require(
        header(headers, "x-image-units-remaining-5h")? == "39",
        "unexpected 5h remaining metadata",
    )?;

    let encoded = body["data"]
        .as_array()
        .filter(|data| data.len() == 1)
        .and_then(|data| data[0]["b64_json"].as_str())
        .ok_or_else(|| "response must contain exactly one b64_json image".to_string())?;
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|error| format!("response image was not valid base64: {error}"))?;
    require(
        decoded == fixture,
        "decoded response did not exactly match the opaque PNG fixture",
    )
}

fn assert_codex_invocation(argv: &[String], request_dir: &str) -> TestResult {
    let expected = [
        "exec",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--disable",
        "plugins",
        "--disable",
        "apps",
        "--sandbox",
        "workspace-write",
        "--skip-git-repo-check",
        "--cd",
        request_dir,
        "-",
    ]
    .map(str::to_string);
    require(
        argv == expected,
        format!("unexpected exact Codex argv:\nactual: {argv:?}\nexpected: {expected:?}"),
    )
}

async fn assert_database_transitions(pool: &PgPool, request_id: &str) -> TestResult {
    let transition: TransitionRow = sqlx::query_as(
        r#"
        SELECT j.job_id, qr.reservation_id,
               j.state AS job_state,
               j.requested_units AS job_requested_units,
               j.charged_units,
               j.finished_at_ms IS NOT NULL AS job_finished,
               qr.state AS reservation_state,
               qr.requested_units AS reservation_requested_units,
               qr.committed_units,
               qr.released_units
        FROM jobs j
        JOIN quota_reservations qr
          ON qr.reservation_id = j.reservation_id
         AND qr.job_id = j.job_id
         AND qr.tenant_id = j.tenant_id
        WHERE j.request_id = $1 AND j.tenant_id = 'tenant_default'
        "#,
    )
    .bind(request_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to read job and reservation transition: {error}"))?;
    require(
        transition.job_state == "succeeded"
            && transition.job_requested_units == 1
            && transition.charged_units == 1
            && transition.job_finished
            && transition.reservation_state == "committed"
            && transition.reservation_requested_units == 1
            && transition.committed_units == 1
            && transition.released_units == 0,
        format!("unexpected succeeded/committed transition: {transition:?}"),
    )?;

    let charged: (i64, i64, Option<String>, Option<String>) = sqlx::query_as(
        r#"
        SELECT COUNT(*), COALESCE(SUM(units), 0)::BIGINT, MIN(outcome), MIN(operation)
        FROM usage_events
        WHERE request_id = $1 AND tenant_id = 'tenant_default'
        "#,
    )
    .bind(request_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to read charged usage transition: {error}"))?;
    require(
        charged
            == (
                1,
                1,
                Some("charged".to_string()),
                Some("generation".to_string()),
            ),
        format!("unexpected charged usage transition: {charged:?}"),
    )?;

    let metering: Vec<MeteringRow> = sqlx::query_as(
        r#"
        SELECT me.event_type, me.units, me.outcome, me.job_id, me.reservation_id
        FROM metering_events me
        JOIN jobs j
          ON j.job_id = me.job_id
         AND j.tenant_id = me.tenant_id
         AND j.request_id = me.request_id
        JOIN quota_reservations qr
          ON qr.reservation_id = me.reservation_id
         AND qr.job_id = j.job_id
         AND qr.tenant_id = me.tenant_id
         AND qr.request_id = me.request_id
        WHERE me.request_id = $1
          AND me.tenant_id = 'tenant_default'
          AND j.job_id = $2
          AND qr.reservation_id = $3
        ORDER BY me.event_type
        "#,
    )
    .bind(request_id)
    .bind(transition.job_id)
    .bind(transition.reservation_id)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("failed to read metering transitions: {error}"))?;
    let expected = vec![
        MeteringRow::expected(
            "job_succeeded",
            "succeeded",
            transition.job_id,
            transition.reservation_id,
        ),
        MeteringRow::expected(
            "quota_committed",
            "succeeded",
            transition.job_id,
            transition.reservation_id,
        ),
        MeteringRow::expected(
            "quota_reserved",
            "reserved",
            transition.job_id,
            transition.reservation_id,
        ),
    ];
    require(
        metering == expected,
        format!("unexpected authoritative metering transitions: {metering:?}"),
    )
}

#[derive(Debug, sqlx::FromRow)]
struct TransitionRow {
    job_id: Uuid,
    reservation_id: Uuid,
    job_state: String,
    job_requested_units: i32,
    charged_units: i32,
    job_finished: bool,
    reservation_state: String,
    reservation_requested_units: i32,
    committed_units: i32,
    released_units: i32,
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct MeteringRow {
    event_type: String,
    units: i32,
    outcome: String,
    job_id: Option<Uuid>,
    reservation_id: Option<Uuid>,
}

impl MeteringRow {
    fn expected(event_type: &str, outcome: &str, job_id: Uuid, reservation_id: Uuid) -> Self {
        Self {
            event_type: event_type.to_string(),
            units: 1,
            outcome: outcome.to_string(),
            job_id: Some(job_id),
            reservation_id: Some(reservation_id),
        }
    }
}

async fn poll_health(
    client: &reqwest::Client,
    base_url: &str,
    gateway: &mut GatewayProcess,
) -> TestResult {
    timeout(HEALTH_TIMEOUT, async {
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

struct TestDatabase {
    database_url: String,
    schema: String,
    pool: PgPool,
}

impl TestDatabase {
    async fn new() -> TestResult<Option<Self>> {
        let Ok(database_url) = env::var(DATABASE_ENV) else {
            if env::var_os("CI").is_some() {
                return Err(format!("{DATABASE_ENV} must be set when CI is present"));
            }
            eprintln!("skipping process smoke test: {DATABASE_ENV} is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_process_smoke_{}", Uuid::new_v4().simple());
        let pool = connect_pool_with_search_path(&database_url, 4, &schema)
            .await
            .map_err(|error| format!("test database should be reachable: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("failed to identify test database: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because current_database() is {database_name:?}, which does not contain 'test'"
            ));
        }

        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("failed to create isolated schema {schema}: {error}"))?;
        let setup = async {
            let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
                .fetch_one(&pool)
                .await
                .map_err(|error| format!("failed to inspect current schema: {error}"))?;
            require(
                current_schema == schema,
                format!("database helper resolved search_path to {current_schema:?}, expected {schema:?}"),
            )?;
            run_migrations(&pool)
                .await
                .map_err(|error| format!("failed to migrate isolated schema: {error:?}"))
        }
        .await;
        if let Err(error) = setup {
            let _ = drop_schema(&pool, &schema).await;
            pool.close().await;
            return Err(error);
        }

        Ok(Some(Self {
            database_url,
            schema,
            pool,
        }))
    }

    fn scoped_database_url(&self) -> String {
        let separator = if self.database_url.contains('?') {
            '&'
        } else {
            '?'
        };
        format!(
            "{}{separator}options=-csearch_path%3D{}",
            self.database_url, self.schema
        )
    }

    async fn cleanup(self) -> TestResult {
        let result = timeout(
            Duration::from_secs(5),
            drop_schema(&self.pool, &self.schema),
        )
        .await
        .map_err(|_| format!("timed out cleaning isolated schema {}", self.schema))?;
        self.pool.close().await;
        result
    }
}

async fn drop_schema(pool: &PgPool, schema: &str) -> TestResult {
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|error| format!("failed to clean isolated schema {schema}: {error}"))
}

struct SmokeFiles {
    _root: TempDir,
    fake_bin: PathBuf,
    codex_home: PathBuf,
    argv_log: PathBuf,
    stdin_log: PathBuf,
    fake_pid_log: PathBuf,
    fake_active_pid: PathBuf,
    gateway_log: PathBuf,
}

impl SmokeFiles {
    fn new(fixture: &[u8]) -> TestResult<Self> {
        let root =
            tempfile::tempdir().map_err(|error| format!("failed to create temp root: {error}"))?;
        let fake_bin = root.path().join("fake-bin");
        let codex_home = root.path().join("codex-home");
        let argv_log = root.path().join("codex.argv");
        let stdin_log = root.path().join("codex.stdin");
        let fake_pid_log = root.path().join("codex.pid");
        let fake_active_pid = root.path().join("codex.active.pid");
        let fixture_path = root.path().join("opaque.png");
        let gateway_log = root.path().join("gateway.log");
        fs::create_dir_all(&fake_bin)
            .map_err(|error| format!("failed to create fake bin: {error}"))?;
        fs::create_dir_all(&codex_home)
            .map_err(|error| format!("failed to create Codex home: {error}"))?;
        fs::write(&fixture_path, fixture)
            .map_err(|error| format!("failed to write PNG fixture: {error}"))?;

        let script_path = fake_bin.join("codex");
        let script = fake_codex_script(
            &codex_home,
            &argv_log,
            &stdin_log,
            &fake_pid_log,
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
            fake_bin,
            codex_home,
            argv_log,
            stdin_log,
            fake_pid_log,
            fake_active_pid,
            gateway_log,
        })
    }
}

fn fake_codex_script(
    codex_home: &Path,
    argv_log: &Path,
    stdin_log: &Path,
    fake_pid_log: &Path,
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
printf '%s\n' "$$" > {fake_pid_log}
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
        fake_active_pid = shell_quote(fake_active_pid),
        fixture = shell_quote(fixture),
    )
}

struct GatewayProcess {
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
        address: std::net::SocketAddr,
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
            .env("DATABASE_URL", database.scoped_database_url())
            .env("GATEWAY_BIND", address.to_string())
            .env("GATEWAY_API_TOKEN", API_TOKEN)
            .env("GATEWAY_ADMIN_TOKEN", ADMIN_TOKEN)
            .env("GATEWAY_CODEX_HOME", &files.codex_home)
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

    async fn terminate(&mut self) -> TestResult {
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

fn opaque_png() -> TestResult<Vec<u8>> {
    let image = ImageBuffer::from_fn(2, 1, |x, _| {
        if x == 0 {
            Rgb([12_u8, 34, 56])
        } else {
            Rgb([210_u8, 180, 90])
        }
    });
    let mut cursor = Cursor::new(Vec::new());
    image
        .write_to(&mut cursor, ImageFormat::Png)
        .map_err(|error| format!("failed to encode opaque PNG fixture: {error}"))?;
    Ok(cursor.into_inner())
}

fn assert_prompt_semantics(prompt: &str, request_dir: &str) -> TestResult {
    for (description, required) in [
        (
            "original prompt",
            "用户原始需求：process smoke opaque fixture".to_string(),
        ),
        ("auto size", "尺寸 auto".to_string()),
        ("low quality", "质量 low".to_string()),
        ("PNG output format", "输出格式 png".to_string()),
        (
            "request-local final image",
            format!("{request_dir}/final.png"),
        ),
        (
            "no delegated AI CLI",
            "不要再启动 codex、openai 或其它 AI CLI 子进程".to_string(),
        ),
        (
            "no local image manipulation tools",
            "不要用 sips、ImageMagick、Python、Rust、ffmpeg、canvas 或其他本地图像处理工具"
                .to_string(),
        ),
        (
            "no local pixel manipulation",
            "裁切、拉伸、重采样、扩边、转绘或修改像素".to_string(),
        ),
    ] {
        require(
            prompt.contains(&required),
            format!("Codex prompt is missing {description} semantics: {prompt}"),
        )?;
    }
    Ok(())
}

fn startup_failed_from_address_in_use(logs: &str) -> bool {
    logs.contains("failed to bind HTTP listener")
}

fn argv_value(argv: &[String], flag: &str) -> TestResult<String> {
    argv.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
        .ok_or_else(|| format!("fake Codex argv did not contain {flag}: {argv:?}"))
}

fn read_nul_strings(path: &Path) -> TestResult<Vec<String>> {
    let bytes =
        fs::read(path).map_err(|error| format!("failed to read fake Codex argv log: {error}"))?;
    bytes
        .split(|byte| *byte == 0)
        .filter(|value| !value.is_empty())
        .map(|value| {
            String::from_utf8(value.to_vec())
                .map_err(|error| format!("fake Codex argv was not UTF-8: {error}"))
        })
        .collect()
}

fn read_pid(path: &Path) -> TestResult<i32> {
    fs::read_to_string(path)
        .map_err(|error| format!("failed to read PID log {}: {error}", path.display()))?
        .trim()
        .parse::<i32>()
        .map_err(|error| format!("invalid PID log {}: {error}", path.display()))
}

fn header(headers: &reqwest::header::HeaderMap, name: &str) -> TestResult<String> {
    headers
        .get(name)
        .ok_or_else(|| format!("response header {name} is missing"))?
        .to_str()
        .map(str::to_string)
        .map_err(|error| format!("response header {name} was invalid: {error}"))
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn combine_results(primary: TestResult, cleanup: TestResult, cleanup_name: &str) -> TestResult {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup)) => Err(format!("{cleanup_name} failed: {cleanup}")),
        (Err(error), Err(cleanup)) => {
            Err(format!("{error}\n{cleanup_name} also failed: {cleanup}"))
        }
    }
}
