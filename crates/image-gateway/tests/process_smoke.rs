#![cfg(unix)]

mod process_smoke_support;

use std::{sync::Arc, time::Duration};

use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use process_smoke_support::{
    API_TOKEN, ExecutordProcess, GatewayProcess, ReducerdProcess, SmokeFiles, TestDatabase,
    TestResult, WorkerdProcess, alternate_opaque_png, assert_artifact_bytes,
    assert_codex_edit_outputs, assert_codex_outputs, assert_executor_codex_outputs,
    assert_prompt_semantics, assert_response, combine_results, header, opaque_png, poll_health,
    require, start_gateway_with_retry, start_v2_gateway_with_retry,
    startup_failed_from_address_in_use, tamper_artifact,
};

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const IDEMPOTENCY_KEY: &str = "process-smoke-key";
const DRAIN_IDEMPOTENCY_KEY: &str = "process-smoke-drain-key";
const QUEUE_IDEMPOTENCY_KEYS: [&str; 2] = ["process-smoke-queue-a", "process-smoke-queue-b"];
const EDIT_IDEMPOTENCY_KEY: &str = "process-smoke-edit-key";
const V2_IDEMPOTENCY_KEY: &str = "process-smoke-generation-v2-key";
const V2_OUTPUT_COUNT: usize = 2;
const V2_SUCCESS_PRICE_MICROS: i64 = 7_000;
static PROCESS_SMOKE_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// Like the other PostgreSQL integration tests, local runs skip without TEST_DATABASE_URL while CI
// fails closed so the process composition cannot silently go untested there.
#[tokio::test]
async fn production_process_composition_succeeds_when_test_database_is_configured() -> TestResult {
    let _serial = PROCESS_SMOKE_SERIAL.lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = run_process_smoke(&database).await;
    let cleanup = database.cleanup().await;
    combine_results(result, cleanup, "schema cleanup")
}

#[tokio::test]
async fn production_process_composition_executes_and_replays_generation_v2() -> TestResult {
    let _serial = PROCESS_SMOKE_SERIAL.lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = run_generation_v2_process_smoke(&database).await;
    let cleanup = database.cleanup().await;
    combine_results(result, cleanup, "schema cleanup")
}

#[tokio::test]
async fn workerd_sigterm_drains_in_flight_generation_before_successful_exit() -> TestResult {
    let _serial = PROCESS_SMOKE_SERIAL.lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = run_workerd_drain_smoke(&database).await;
    let cleanup = database.cleanup().await;
    combine_results(result, cleanup, "schema cleanup")
}

#[tokio::test]
async fn external_generation_queues_without_holding_gateway_scheduler_permits() -> TestResult {
    let _serial = PROCESS_SMOKE_SERIAL.lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = run_external_queue_smoke(&database).await;
    let cleanup = database.cleanup().await;
    combine_results(result, cleanup, "schema cleanup")
}

#[tokio::test]
async fn production_process_composition_executes_and_replays_durable_edit() -> TestResult {
    let _serial = PROCESS_SMOKE_SERIAL.lock().await;
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = run_edit_process_smoke(&database).await;
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
    let prompt = "prefix may evolve\n用户原始需求：process smoke opaque fixture\n尺寸 auto；质量 low；输出格式 png。\n不要再启动 codex、openai 或其它 AI CLI 子进程来委托生成。\n不要用 shell、sips、ImageMagick、Python、Rust、ffmpeg、canvas 或其他本地工具复制、移动、重命名、删除、裁切、拉伸、重采样、扩边、转绘或修改图像生成工具产物。必须只调用一次当前启用的 image_gen.imagegen 图像生成工具；工具成功后立即停止，由 Factory 从该工具的受控原生产物路径完成封存。\nsuffix may evolve";
    assert_prompt_semantics(&prompt, request_dir)
}

async fn run_process_smoke(database: &TestDatabase) -> TestResult {
    let fixture = opaque_png()?;
    let files = SmokeFiles::new(&fixture)?;
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| format!("failed to build HTTP client: {error}"))?;
    let mut workerd = WorkerdProcess::start(database, &files).await?;
    let (mut gateway, address) = start_gateway_with_retry(&client, database, &files).await?;

    let result = exercise_gateway(
        &client,
        address,
        database,
        &files,
        &fixture,
        workerd.pid(),
        &mut gateway,
    )
    .await;
    let gateway_shutdown = gateway.terminate().await;
    let result = combine_results(result, gateway_shutdown, "gateway shutdown");
    let worker_shutdown = workerd.terminate().await;
    combine_results(result, worker_shutdown, "workerd shutdown")
}

async fn run_generation_v2_process_smoke(database: &TestDatabase) -> TestResult {
    let fixtures = [opaque_png()?, alternate_opaque_png()?];
    let files = SmokeFiles::new(&fixtures[0])?;
    files.set_second_fixture(&fixtures[1])?;
    database
        .configure_v2_pricing(V2_OUTPUT_COUNT, V2_SUCCESS_PRICE_MICROS)
        .await?;
    let profile = database
        .provision_codex_execution_profile(
            files.codex_credential_home(),
            files.codex_auth_file_sha256()?,
        )
        .await?;
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| format!("failed to build HTTP client: {error}"))?;
    let mut workerd = WorkerdProcess::start_handoff(database, &files, &profile.profile_key).await?;
    let mut executord = ExecutordProcess::start(
        database,
        &files,
        &profile.profile_key,
        &profile.credential_ref,
    )
    .await?;
    let mut reducerd = ReducerdProcess::start(database, &files).await?;
    let (mut gateway, address) = start_v2_gateway_with_retry(&client, database, &files).await?;

    let result = exercise_generation_v2_gateway(
        &client,
        address,
        database,
        &files,
        &fixtures,
        &profile,
        &mut gateway,
    )
    .await;
    let gateway_shutdown = gateway.terminate().await;
    let result = combine_results(result, gateway_shutdown, "V2 gateway shutdown");
    let reducer_shutdown = reducerd.terminate().await;
    let result = combine_results(result, reducer_shutdown, "reducerd shutdown");
    let executor_shutdown = executord.terminate().await;
    let result = combine_results(result, executor_shutdown, "executord shutdown");
    let worker_shutdown = workerd.terminate().await;
    combine_results(result, worker_shutdown, "V2 workerd shutdown")
}

async fn run_edit_process_smoke(database: &TestDatabase) -> TestResult {
    let fixture = opaque_png()?;
    let files = SmokeFiles::new(&fixture)?;
    let direct_edit = DirectEditMock::start(&fixture).await?;
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| format!("failed to build HTTP client: {error}"))?;
    let mut workerd = WorkerdProcess::start_with_direct_edit_endpoint(
        database,
        &files,
        Some(&direct_edit.endpoint),
    )
    .await?;
    let (mut gateway, address) = start_gateway_with_retry(&client, database, &files).await?;
    let result = exercise_edit_gateway(
        &client,
        address,
        database,
        &files,
        &fixture,
        &mut gateway,
        &direct_edit,
    )
    .await;
    let gateway_shutdown = gateway.terminate().await;
    let result = combine_results(result, gateway_shutdown, "gateway shutdown");
    let worker_shutdown = workerd.terminate().await;
    combine_results(result, worker_shutdown, "workerd shutdown")
}

async fn run_workerd_drain_smoke(database: &TestDatabase) -> TestResult {
    let fixture = opaque_png()?;
    let files = SmokeFiles::new(&fixture)?;
    files.set_fake_codex_delay(Duration::from_secs(2))?;
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| format!("failed to build HTTP client: {error}"))?;
    let mut workerd = WorkerdProcess::start(database, &files).await?;
    let (mut gateway, address) = start_gateway_with_retry(&client, database, &files).await?;

    let request_client = client.clone();
    let request_fixture = fixture.clone();
    let request = tokio::spawn(async move {
        let response = request_client
            .post(format!("http://{address}/v1/images/generations"))
            .bearer_auth(API_TOKEN)
            .header("Idempotency-Key", DRAIN_IDEMPOTENCY_KEY)
            .json(&generation_request())
            .send()
            .await
            .map_err(|error| format!("in-flight generation request failed: {error}"))?;
        let status = response.status();
        let headers = response.headers().clone();
        let body: Value = response
            .json()
            .await
            .map_err(|error| format!("in-flight generation response was not JSON: {error}"))?;
        require(
            status == reqwest::StatusCode::OK,
            format!("in-flight generation returned {status}: {body:#}"),
        )?;
        assert_response(&body, &headers, &request_fixture)
    });

    let active = files.wait_for_fake_codex_active().await;
    let worker_shutdown = match active {
        Ok(()) => workerd.terminate().await,
        Err(error) => Err(error),
    };
    let request_result = request
        .await
        .map_err(|error| format!("in-flight request task failed: {error}"))?;
    let gateway_shutdown = gateway.terminate().await;
    let result = combine_results(
        request_result,
        worker_shutdown,
        "workerd drain and successful exit",
    );
    combine_results(result, gateway_shutdown, "gateway shutdown")
}

async fn run_external_queue_smoke(database: &TestDatabase) -> TestResult {
    let fixture = opaque_png()?;
    let files = SmokeFiles::new(&fixture)?;
    files.set_fake_codex_delay(Duration::from_secs(2))?;
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| format!("failed to build HTTP client: {error}"))?;
    let mut workerd = WorkerdProcess::start(database, &files).await?;
    let (mut gateway, address) = start_gateway_with_retry(&client, database, &files).await?;

    let first = spawn_generation_request(client.clone(), address, QUEUE_IDEMPOTENCY_KEYS[0]);
    files.wait_for_fake_codex_active().await?;
    let second = spawn_generation_request(client, address, QUEUE_IDEMPOTENCY_KEYS[1]);
    let queue_result = database.wait_for_generation_work_count(2).await;
    let first_result = first
        .await
        .map_err(|error| format!("first queued request task failed: {error}"))?;
    let second_result = second
        .await
        .map_err(|error| format!("second queued request task failed: {error}"))?;
    let gateway_shutdown = gateway.terminate().await;
    let worker_shutdown = workerd.terminate().await;

    let result = combine_results(queue_result, first_result, "first queued generation");
    let result = combine_results(result, second_result, "second queued generation");
    let result = combine_results(result, gateway_shutdown, "gateway shutdown");
    combine_results(result, worker_shutdown, "workerd shutdown")
}

fn spawn_generation_request(
    client: reqwest::Client,
    address: std::net::SocketAddr,
    idempotency_key: &'static str,
) -> tokio::task::JoinHandle<TestResult> {
    tokio::spawn(async move {
        let response = client
            .post(format!("http://{address}/v1/images/generations"))
            .bearer_auth(API_TOKEN)
            .header("Idempotency-Key", idempotency_key)
            .json(&generation_request())
            .send()
            .await
            .map_err(|error| format!("queued generation request failed: {error}"))?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .map_err(|error| format!("queued generation response was not JSON: {error}"))?;
        require(
            status == reqwest::StatusCode::OK,
            format!("queued generation returned {status}: {body:#}"),
        )
    })
}

async fn exercise_generation_v2_gateway(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    database: &TestDatabase,
    files: &SmokeFiles,
    fixtures: &[Vec<u8>; V2_OUTPUT_COUNT],
    profile: &process_smoke_support::ExecutionProfile,
    gateway: &mut GatewayProcess,
) -> TestResult {
    let request_body = generation_v2_request();
    let base_url = format!("http://{address}");
    poll_health(client, &base_url, gateway).await?;
    let response = client
        .post(format!("{base_url}/v1/images/generations"))
        .bearer_auth(API_TOKEN)
        .header("Idempotency-Key", V2_IDEMPOTENCY_KEY)
        .json(&request_body)
        .send()
        .await
        .map_err(|error| {
            format!(
                "V2 generation request failed: {error}\n{}",
                files.process_diagnostics()
            )
        })?;
    let status = response.status();
    let headers = response.headers().clone();
    let request_id = header(&headers, "x-request-id")?;
    let body: Value = response
        .json()
        .await
        .map_err(|error| format!("V2 generation response was not JSON: {error}"))?;
    if status != reqwest::StatusCode::OK {
        return Err(format!(
            "V2 generation returned {status}: {body:#}\n{}\n--- database ---\n{}",
            files.process_diagnostics(),
            database.process_state_diagnostics().await?
        ));
    }
    require(
        status == reqwest::StatusCode::OK,
        "V2 generation status changed after validation",
    )?;
    assert_generation_v2_response(&body, &headers, fixtures)?;
    assert_executor_codex_outputs(files, V2_OUTPUT_COUNT)?;
    database
        .assert_v2_generation_graph(
            &request_id,
            profile,
            fixtures,
            V2_OUTPUT_COUNT,
            V2_SUCCESS_PRICE_MICROS,
        )
        .await?;

    gateway.terminate().await?;
    let (restarted, restarted_address) =
        start_v2_gateway_with_retry(client, database, files).await?;
    *gateway = restarted;
    let replay = client
        .post(format!("http://{restarted_address}/v1/images/generations"))
        .bearer_auth(API_TOKEN)
        .header("Idempotency-Key", V2_IDEMPOTENCY_KEY)
        .json(&request_body)
        .send()
        .await
        .map_err(|error| {
            format!(
                "V2 idempotent replay failed: {error}\n{}",
                files.process_diagnostics()
            )
        })?;
    let replay_status = replay.status();
    let replay_headers = replay.headers().clone();
    let replay_request_id = header(&replay_headers, "x-request-id")?;
    let replay_body: Value = replay
        .json()
        .await
        .map_err(|error| format!("V2 idempotent replay was not JSON: {error}"))?;
    require(
        replay_status == reqwest::StatusCode::OK && replay_body == body,
        format!("unexpected V2 idempotent replay {replay_status}: {replay_body:#}"),
    )?;
    require(
        replay_request_id != request_id,
        "V2 replay must receive a fresh request id",
    )?;
    assert_generation_v2_response(&replay_body, &replay_headers, fixtures)?;
    assert_executor_codex_outputs(files, V2_OUTPUT_COUNT)?;
    database
        .assert_v2_generation_graph(
            &request_id,
            profile,
            fixtures,
            V2_OUTPUT_COUNT,
            V2_SUCCESS_PRICE_MICROS,
        )
        .await
}

fn assert_generation_v2_response(
    body: &Value,
    headers: &reqwest::header::HeaderMap,
    fixtures: &[Vec<u8>; V2_OUTPUT_COUNT],
) -> TestResult {
    require(
        body["created"].as_i64().is_some_and(|value| value > 0)
            && body["output_format"] == "png"
            && body["quality"] == "low"
            && body["size"] == "2x1"
            && body["background"] == "opaque",
        format!("unexpected V2 response metadata: {body:#}"),
    )?;
    require(
        header(headers, "openai-project")? == "proj_default"
            && header(headers, "x-image-units-limit-5h")? == "2147483647"
            && header(headers, "x-image-units-remaining-5h")? == "2147483645",
        "unexpected V2 response usage metadata",
    )?;
    let encoded = body["data"]
        .as_array()
        .filter(|data| data.len() == V2_OUTPUT_COUNT)
        .ok_or_else(|| format!("V2 response must contain exactly {V2_OUTPUT_COUNT} images"))?;
    for (index, image) in encoded.iter().enumerate() {
        let encoded = image["b64_json"]
            .as_str()
            .ok_or_else(|| format!("V2 response image {index} is missing b64_json"))?;
        let decoded = STANDARD
            .decode(encoded)
            .map_err(|error| format!("V2 response image {index} was not valid base64: {error}"))?;
        require(
            decoded.as_slice() == fixtures[index].as_slice(),
            format!("V2 response image {index} did not exactly match the fake Codex fixture"),
        )?;
    }
    Ok(())
}

async fn exercise_gateway(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    database: &TestDatabase,
    files: &SmokeFiles,
    fixture: &[u8],
    workerd_pid: u32,
    gateway: &mut GatewayProcess,
) -> TestResult {
    let base_url = format!("http://{address}");
    poll_health(client, &base_url, gateway).await?;

    let request_body = generation_request();
    let response = client
        .post(format!("{base_url}/v1/images/generations"))
        .bearer_auth(API_TOKEN)
        .header("Idempotency-Key", IDEMPOTENCY_KEY)
        .json(&request_body)
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
    assert_codex_outputs(files, workerd_pid)?;
    assert_artifact_bytes(files, fixture)?;

    gateway.terminate().await?;
    let (restarted, restarted_address) = start_gateway_with_retry(client, database, files).await?;
    *gateway = restarted;
    let base_url = format!("http://{restarted_address}");
    poll_health(client, &base_url, gateway).await?;

    let replay = client
        .post(format!("{base_url}/v1/images/generations"))
        .bearer_auth(API_TOKEN)
        .header("Idempotency-Key", IDEMPOTENCY_KEY)
        .json(&request_body)
        .send()
        .await
        .map_err(|error| format!("idempotent replay failed: {error}"))?;
    let replay_status = replay.status();
    let replay_headers = replay.headers().clone();
    let replay_request_id = header(&replay_headers, "x-request-id")?;
    let replay_body: Value = replay
        .json()
        .await
        .map_err(|error| format!("idempotent replay was not JSON: {error}"))?;
    require(
        replay_status == reqwest::StatusCode::OK && replay_body == body,
        format!("unexpected idempotent replay response {replay_status}: {replay_body:#}"),
    )?;
    require(
        replay_request_id != request_id,
        "replay must receive a fresh request id",
    )?;
    assert_response(&replay_body, &replay_headers, fixture)?;
    assert_codex_outputs(files, workerd_pid)?;
    assert_artifact_bytes(files, fixture)?;
    tamper_artifact(files)?;

    let corrupted = client
        .post(format!("{base_url}/v1/images/generations"))
        .bearer_auth(API_TOKEN)
        .header("Idempotency-Key", IDEMPOTENCY_KEY)
        .json(&request_body)
        .send()
        .await
        .map_err(|error| format!("corrupted artifact replay failed: {error}"))?;
    let corrupted_status = corrupted.status();
    let corrupted_body: Value = corrupted
        .json()
        .await
        .map_err(|error| format!("corrupted artifact response was not JSON: {error}"))?;
    require(
        corrupted_status == reqwest::StatusCode::INTERNAL_SERVER_ERROR
            && corrupted_body["error"]["code"] == "artifact_integrity_error"
            && corrupted_body["error"]["param"].is_null(),
        format!("unexpected corrupted artifact response {corrupted_status}: {corrupted_body:#}"),
    )?;
    require(
        !corrupted_body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains(&files.artifact_root.display().to_string()),
        "artifact integrity error leaked the storage path",
    )?;
    assert_codex_outputs(files, workerd_pid)?;
    database.assert_transitions(&request_id).await
}

async fn exercise_edit_gateway(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    database: &TestDatabase,
    files: &SmokeFiles,
    fixture: &[u8],
    gateway: &mut GatewayProcess,
    direct_edit: &DirectEditMock,
) -> TestResult {
    let base_url = format!("http://{address}");
    poll_health(client, &base_url, gateway).await?;
    let request_body = edit_request(fixture);
    let response = client
        .post(format!("{base_url}/v1/images/edits"))
        .bearer_auth(API_TOKEN)
        .header("Idempotency-Key", EDIT_IDEMPOTENCY_KEY)
        .json(&request_body)
        .send()
        .await
        .map_err(|error| format!("edit request failed: {error}"))?;
    let status = response.status();
    let headers = response.headers().clone();
    let request_id = header(&headers, "x-request-id")?;
    let body: Value = response
        .json()
        .await
        .map_err(|error| format!("edit response was not JSON: {error}"))?;
    require(
        status == reqwest::StatusCode::OK,
        format!(
            "edit returned {status}: {body:#}\n{}",
            files.process_diagnostics()
        ),
    )?;
    assert_response(&body, &headers, fixture)?;
    assert_codex_edit_outputs(files)?;
    direct_edit.assert_single_request().await?;
    assert_artifact_bytes(files, fixture)?;

    gateway.terminate().await?;
    let (restarted, restarted_address) = start_gateway_with_retry(client, database, files).await?;
    *gateway = restarted;
    let replay = client
        .post(format!("http://{restarted_address}/v1/images/edits"))
        .bearer_auth(API_TOKEN)
        .header("Idempotency-Key", EDIT_IDEMPOTENCY_KEY)
        .json(&request_body)
        .send()
        .await
        .map_err(|error| format!("edit replay failed: {error}"))?;
    let replay_status = replay.status();
    let replay_headers = replay.headers().clone();
    let replay_request_id = header(&replay_headers, "x-request-id")?;
    let replay_body: Value = replay
        .json()
        .await
        .map_err(|error| format!("edit replay was not JSON: {error}"))?;
    require(
        replay_status == reqwest::StatusCode::OK && replay_body == body,
        format!("unexpected edit replay {replay_status}: {replay_body:#}"),
    )?;
    require(
        replay_request_id != request_id,
        "edit replay must receive a fresh request id",
    )?;
    assert_codex_edit_outputs(files)?;
    direct_edit.assert_single_request().await?;
    database.assert_edit_transitions(&request_id).await
}

#[derive(Default)]
struct DirectEditObservation {
    headers: Option<HeaderMap>,
    bodies: Vec<Value>,
}

struct DirectEditMock {
    endpoint: String,
    observation: Arc<tokio::sync::Mutex<DirectEditObservation>>,
    task: tokio::task::JoinHandle<()>,
}

impl DirectEditMock {
    async fn start(fixture: &[u8]) -> TestResult<Self> {
        #[derive(Clone)]
        struct MockState {
            encoded_fixture: String,
            observation: Arc<tokio::sync::Mutex<DirectEditObservation>>,
        }

        async fn edit(
            State(state): State<MockState>,
            headers: HeaderMap,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            let mut observation = state.observation.lock().await;
            observation.headers = Some(headers);
            observation.bodies.push(body);
            Json(json!({"data": [{"b64_json": state.encoded_fixture}]}))
        }

        let observation = Arc::new(tokio::sync::Mutex::new(DirectEditObservation::default()));
        let state = MockState {
            encoded_fixture: STANDARD.encode(fixture),
            observation: Arc::clone(&observation),
        };
        let app = Router::new()
            .route("/backend-api/codex/images/edits", post(edit))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| format!("failed to bind fake Codex edit upstream: {error}"))?;
        let endpoint = format!(
            "http://{}/backend-api/codex/images/edits",
            listener
                .local_addr()
                .map_err(|error| format!("failed to read fake Codex edit address: {error}"))?
        );
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            endpoint,
            observation,
            task,
        })
    }

    async fn assert_single_request(&self) -> TestResult {
        let observation = self.observation.lock().await;
        require(
            observation.bodies.len() == 1,
            format!(
                "fake Codex edit upstream received {} requests, expected 1",
                observation.bodies.len()
            ),
        )?;
        let headers = observation
            .headers
            .as_ref()
            .ok_or_else(|| "fake Codex edit upstream received no headers".to_string())?;
        require(
            headers
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                == Some("Bearer process-smoke-access"),
            "direct edit did not use the broker-managed access token",
        )?;
        require(
            headers
                .get("ChatGPT-Account-ID")
                .and_then(|value| value.to_str().ok())
                == Some("process-smoke-account"),
            "direct edit did not preserve the broker-managed account binding",
        )?;
        let body = &observation.bodies[0];
        require(
            body["model"] == "gpt-image-2"
                && body["n"] == 1
                && body["images"].as_array().map(Vec::len) == Some(1),
            format!("unexpected direct edit request contract: {body:#}"),
        )?;
        require(
            body["prompt"] == "process smoke opaque fixture",
            "direct edit did not preserve the exact admitted prompt",
        )
    }
}

impl Drop for DirectEditMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn generation_request() -> Value {
    json!({
        "model": "gpt-image-2",
        "prompt": "process smoke opaque fixture",
        "n": 1,
        "size": "auto",
        "quality": "low",
        "output_format": "png"
    })
}

fn generation_v2_request() -> Value {
    let mut request = generation_request();
    request["n"] = json!(V2_OUTPUT_COUNT);
    request
}

fn edit_request(image: &[u8]) -> Value {
    json!({
        "model": "gpt-image-2",
        "prompt": "process smoke opaque fixture",
        "images": [{
            "image_url": format!("data:image/png;base64,{}", STANDARD.encode(image))
        }],
        "n": 1,
        "size": "auto",
        "quality": "low",
        "output_format": "png"
    })
}
