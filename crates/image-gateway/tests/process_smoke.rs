#![cfg(unix)]

mod process_smoke_support;

use std::time::Duration;

use serde_json::{Value, json};

use process_smoke_support::{
    API_TOKEN, GatewayProcess, SmokeFiles, TestDatabase, TestResult, assert_artifact_bytes,
    assert_codex_outputs, assert_prompt_semantics, assert_response, combine_results, header,
    opaque_png, poll_health, require, start_gateway_with_retry, startup_failed_from_address_in_use,
    tamper_artifact,
};

const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const IDEMPOTENCY_KEY: &str = "process-smoke-key";

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

    let request_body = json!({
        "model": "gpt-image-2",
        "prompt": "process smoke opaque fixture",
        "n": 1,
        "size": "auto",
        "quality": "low",
        "output_format": "png"
    });
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
    assert_codex_outputs(files)?;
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
    assert_codex_outputs(files)?;
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
    assert_codex_outputs(files)?;
    database.assert_transitions(&request_id).await
}
