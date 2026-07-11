#![cfg(unix)]

mod process_smoke_support;

use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use process_smoke_support::{
    API_TOKEN, GatewayProcess, SmokeFiles, TestDatabase, TestResult, WorkerdProcess,
    assert_artifact_bytes, assert_codex_edit_outputs, assert_codex_outputs,
    assert_prompt_semantics, assert_response, combine_results, header, opaque_png, poll_health,
    require, start_gateway_with_retry, startup_failed_from_address_in_use, tamper_artifact,
};

const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const IDEMPOTENCY_KEY: &str = "process-smoke-key";
const DRAIN_IDEMPOTENCY_KEY: &str = "process-smoke-drain-key";
const QUEUE_IDEMPOTENCY_KEYS: [&str; 2] = ["process-smoke-queue-a", "process-smoke-queue-b"];
const EDIT_IDEMPOTENCY_KEY: &str = "process-smoke-edit-key";

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

#[tokio::test]
async fn workerd_sigterm_drains_in_flight_generation_before_successful_exit() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = run_workerd_drain_smoke(&database).await;
    let cleanup = database.cleanup().await;
    combine_results(result, cleanup, "schema cleanup")
}

#[tokio::test]
async fn external_generation_queues_without_holding_gateway_scheduler_permits() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = run_external_queue_smoke(&database).await;
    let cleanup = database.cleanup().await;
    combine_results(result, cleanup, "schema cleanup")
}

#[tokio::test]
async fn production_process_composition_executes_and_replays_durable_edit() -> TestResult {
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

async fn run_edit_process_smoke(database: &TestDatabase) -> TestResult {
    let fixture = opaque_png()?;
    let files = SmokeFiles::new(&fixture)?;
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|error| format!("failed to build HTTP client: {error}"))?;
    let mut workerd = WorkerdProcess::start(database, &files).await?;
    let (mut gateway, address) = start_gateway_with_retry(&client, database, &files).await?;
    let result = exercise_edit_gateway(
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
    workerd_pid: u32,
    gateway: &mut GatewayProcess,
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
        format!("edit returned {status}: {body:#}"),
    )?;
    assert_response(&body, &headers, fixture)?;
    assert_codex_edit_outputs(files, workerd_pid)?;
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
    assert_codex_edit_outputs(files, workerd_pid)?;
    database.assert_edit_transitions(&request_id).await
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
