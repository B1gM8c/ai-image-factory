use std::{env, sync::Arc, time::Duration};

use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gpt_image_2_gateway::{
    AppConfig, CodexImageGenerator, InMemoryUsageStore, ProxyConfig, build_router,
};
use serde_json::{Value, json};
use tower::ServiceExt;

fn smoke_config() -> AppConfig {
    let codex_home = env::var("GATEWAY_CODEX_HOME").expect(
        "GATEWAY_CODEX_HOME must explicitly select the Codex credentials for this smoke test",
    );
    AppConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: Some("smoke-token".to_string()),
        admin_token: Some("admin-token".to_string()),
        database_url: None,
        generation_admission_contract: Default::default(),
        five_hour_image_limit: 3,
        seven_day_image_limit: 3,
        max_concurrent_jobs: 1,
        max_queue_size: 0,
        max_concurrent_jobs_per_tenant: 1,
        max_queue_size_per_tenant: 0,
        queue_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(900),
        readiness_timeout: Duration::from_millis(500),
        max_upload_bytes: 32 * 1024 * 1024,
        proxy: ProxyConfig::default(),
        codex_home: Some(codex_home),
        cleanup_codex_outputs: false,
    }
}

#[tokio::test]
#[ignore = "runs real Codex CLI image generation and may consume image quota"]
async fn images_generation_route_can_call_real_codex_cli() {
    let config = smoke_config();
    let app = build_router(
        config.clone(),
        Arc::new(CodexImageGenerator::new(config)),
        Arc::new(InMemoryUsageStore::default()),
    );

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/images/generations")
                .header(header::AUTHORIZATION, "Bearer smoke-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "model": "gpt-image-2",
                        "prompt": "smoke test: a single blue square icon on a white background",
                        "size": "auto",
                        "quality": "low",
                        "output_format": "png",
                        "n": 1
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(status, StatusCode::OK, "{body:#}");
    let encoded = body["data"][0]["b64_json"]
        .as_str()
        .expect("b64 image in response");
    let image_bytes = STANDARD.decode(encoded).expect("valid base64 image");
    assert!(
        image::load_from_memory(&image_bytes).is_ok(),
        "Codex response should decode as an image"
    );
    assert_eq!(body["output_format"], "png");
}
