#![cfg(unix)]

mod process_smoke_support;

use std::time::Duration;

use serde_json::{Value, json};

use process_smoke_support::{
    API_TOKEN, GatewayProcess, SmokeFiles, TestDatabase, TestResult, assert_codex_outputs,
    assert_prompt_semantics, assert_response, combine_results, header, opaque_png, poll_health,
    require, start_gateway_with_retry, startup_failed_from_address_in_use,
};

const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

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
    assert_codex_outputs(files)?;
    database.assert_transitions(&request_id).await
}
