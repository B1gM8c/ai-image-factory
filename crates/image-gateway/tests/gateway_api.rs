use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gpt_image_2_gateway::{
    AppConfig, EditJob, GeneratedImage, GenerationJob, ImageGatewayError, ImageGenerator,
    InMemoryApiKeyStore, InMemoryUsageStore, build_router, build_router_with_api_key_store,
};
use image::{ImageBuffer, ImageFormat, Rgba};
use serde_json::{Value, json};
use tower::ServiceExt;

const TINY_PNG: &[u8] = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR\0\0\0\x01\0\0\0\x01\0\0\0\0";
const RUNTIME_STABLE_FIXTURE: &str =
    include_str!("fixtures/openai_images/2026-07-10/runtime-stable.json");
const ERRORS_STABLE_FIXTURE: &str =
    include_str!("fixtures/openai_images/2026-07-10/errors-stable.json");

fn fixture_json(contents: &str) -> Value {
    serde_json::from_str(contents).expect("valid OpenAI Images contract fixture")
}

fn runtime_event_fixture(event_type: &str) -> Value {
    fixture_json(RUNTIME_STABLE_FIXTURE)["events"]
        .as_array()
        .expect("runtime events")
        .iter()
        .find(|event| event["type"] == event_type)
        .unwrap_or_else(|| panic!("missing runtime fixture for {event_type}"))
        .clone()
}

fn error_scenario_fixture(id: &str) -> Value {
    fixture_json(ERRORS_STABLE_FIXTURE)["scenarios"]
        .as_array()
        .expect("error scenarios")
        .iter()
        .find(|scenario| scenario["id"] == id)
        .unwrap_or_else(|| panic!("missing error fixture for {id}"))
        .clone()
}

fn stable_sse_projection(event: &Value) -> Value {
    let mut event = event.clone();
    let object = event.as_object_mut().expect("SSE event object");
    for dynamic in ["b64_json", "created_at", "usage"] {
        object.remove(dynamic);
    }
    event
}

fn assert_error_fixture(status: StatusCode, body: &Value, id: &str) {
    let fixture = error_scenario_fixture(id);
    assert_eq!(status.as_u16(), fixture["status"].as_u64().unwrap() as u16);
    assert_eq!(body["error"], fixture["error"]);
}

fn png_bytes(width: u32, height: u32) -> Vec<u8> {
    let image = ImageBuffer::from_pixel(width, height, Rgba([255u8, 255, 255, 255]));
    let mut cursor = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut cursor, ImageFormat::Png)
        .expect("encode test png");
    cursor.into_inner()
}

#[derive(Clone)]
struct FakeGenerator {
    calls: Arc<Mutex<Vec<FakeCall>>>,
    delay: Duration,
    image_bytes: Vec<u8>,
    failure: Option<FakeFailure>,
}

impl Default for FakeGenerator {
    fn default() -> Self {
        Self {
            calls: Arc::default(),
            delay: Duration::ZERO,
            image_bytes: png_bytes(1024, 1024),
            failure: None,
        }
    }
}

#[derive(Clone, Copy)]
enum FakeFailure {
    CodexCliFailed,
    CodexNoImageOutput,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FakeCall {
    Generate {
        prompt: String,
        n: u32,
        size: String,
        output_format: String,
    },
    Edit {
        prompt: String,
        images: usize,
        has_mask: bool,
        n: u32,
    },
}

#[async_trait]
impl ImageGenerator for FakeGenerator {
    async fn generate(&self, job: GenerationJob) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        if let Some(failure) = self.failure {
            return Err(match failure {
                FakeFailure::CodexCliFailed => ImageGatewayError::codex_cli_failed(),
                FakeFailure::CodexNoImageOutput => ImageGatewayError::codex_no_image_output(),
            });
        }

        self.calls.lock().unwrap().push(FakeCall::Generate {
            prompt: job.prompt,
            n: job.n,
            size: job.size,
            output_format: job.output_format,
        });

        let image_bytes = self.image_bytes.clone();
        Ok((0..job.n)
            .map(|idx| GeneratedImage {
                bytes: if image_bytes.is_empty() {
                    format!("fake-image-{idx}").into_bytes()
                } else {
                    image_bytes.clone()
                },
            })
            .collect())
    }

    async fn edit(&self, job: EditJob) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
        if let Some(failure) = self.failure {
            return Err(match failure {
                FakeFailure::CodexCliFailed => ImageGatewayError::codex_cli_failed(),
                FakeFailure::CodexNoImageOutput => ImageGatewayError::codex_no_image_output(),
            });
        }
        self.calls.lock().unwrap().push(FakeCall::Edit {
            prompt: job.prompt.clone(),
            images: job.images.len(),
            has_mask: job.mask.is_some(),
            n: job.n,
        });

        let image_bytes = self.image_bytes.clone();
        Ok((0..job.n)
            .map(|_| GeneratedImage {
                bytes: image_bytes.clone(),
            })
            .collect())
    }
}

fn config() -> AppConfig {
    AppConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: Some("test-token".to_string()),
        admin_token: Some("admin-token".to_string()),
        database_url: None,
        generation_admission_contract: Default::default(),
        five_hour_image_limit: 10,
        seven_day_image_limit: 50,
        max_concurrent_jobs: 2,
        max_queue_size: 4,
        max_concurrent_jobs_per_tenant: 2,
        max_queue_size_per_tenant: 4,
        queue_timeout: Duration::from_millis(100),
        request_timeout: Duration::from_secs(5),
        readiness_timeout: Duration::from_millis(500),
        max_upload_bytes: 50 * 1024 * 1024,
        proxy: Default::default(),
        codex_home: None,
        cleanup_codex_outputs: false,
    }
}

fn sse_body_events(body: &str) -> Vec<(&str, Value)> {
    body.trim()
        .split("\n\n")
        .map(|frame| {
            let mut lines = frame.lines();
            let event = lines
                .next()
                .and_then(|line| line.strip_prefix("event: "))
                .expect("event line");
            let data = lines
                .next()
                .and_then(|line| line.strip_prefix("data: "))
                .expect("data line");
            assert!(lines.next().is_none());
            (event, serde_json::from_str(data).expect("json event data"))
        })
        .collect()
}

fn usage_store() -> Arc<InMemoryUsageStore> {
    Arc::new(InMemoryUsageStore::default())
}

fn assert_request_id(headers: &axum::http::HeaderMap) {
    let request_id = headers
        .get("x-request-id")
        .expect("x-request-id header")
        .to_str()
        .expect("valid x-request-id");
    assert!(request_id.starts_with("req_"));
    assert!(request_id.len() > "req_".len());
    assert_eq!(headers["openai-version"], "2020-10-01");
}

async fn send_json(
    app: axum::Router,
    token: Option<&str>,
    body: Value,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri("/v1/images/generations")
        .header(header::CONTENT_TYPE, "application/json");

    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }

    let response = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap();

    (status, headers, json)
}

async fn send_edit_multipart(
    app: axum::Router,
    fields: &[(&str, &str)],
    include_image: bool,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let boundary = "x-test-boundary";
    let mut body = Vec::new();

    for (name, value) in fields {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }

    if include_image {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"input.png\"\r\nContent-Type: image/png\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(TINY_PNG);
        body.extend_from_slice(b"\r\n");
    }

    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/images/edits")
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap();

    (status, headers, json)
}

async fn send_edit_json(
    app: axum::Router,
    token: Option<&str>,
    body: Value,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri("/v1/images/edits")
        .header(header::CONTENT_TYPE, "application/json");

    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }

    let response = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap();

    (status, headers, json)
}

async fn send_json_raw(
    app: axum::Router,
    token: Option<&str>,
    body: Value,
) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri("/v1/images/generations")
        .header(header::CONTENT_TYPE, "application/json");

    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }

    let response = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();

    (status, headers, bytes)
}

async fn post_json_to(
    app: axum::Router,
    uri: &str,
    token: Option<&str>,
    body: Value,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");

    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }

    let response = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap();

    (status, headers, json)
}

async fn delete_with_token(
    app: axum::Router,
    uri: &str,
    token: Option<&str>,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut builder = Request::builder().method(Method::DELETE).uri(uri);

    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }

    let response = app
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap();

    (status, headers, json)
}

async fn get(app: axum::Router, uri: &str) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    get_with_token(app, uri, None).await
}

async fn get_with_token(
    app: axum::Router,
    uri: &str,
    token: Option<&str>,
) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let mut builder = Request::builder().method(Method::GET).uri(uri);

    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }

    let response = app
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, headers, bytes)
}

#[tokio::test]
async fn liveness_stays_dependency_free_and_in_memory_readiness_is_empty() {
    let fake = Arc::new(FakeGenerator::default());
    let (health_status, _, health_bytes) = get(
        build_router(config(), fake.clone(), usage_store()),
        "/healthz",
    )
    .await;
    let (ready_status, ready_headers, ready_bytes) =
        get(build_router(config(), fake, usage_store()), "/readyz").await;

    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&health_bytes).unwrap(),
        json!({"status": "ok"})
    );
    assert_eq!(ready_status, StatusCode::OK);
    assert_eq!(ready_headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        serde_json::from_slice::<Value>(&ready_bytes).unwrap(),
        json!({
            "status": "ready",
            "provider_profiles": {
                "configured": 0,
                "active": 0,
                "draining": 0,
                "blocked": 0
            }
        })
    );
}

#[tokio::test]
async fn generations_return_openai_style_base64_json() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "n": 2,
            "size": "1024x1024",
            "quality": "low",
            "output_format": "png"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_request_id(&_headers);
    assert!(body["created"].as_i64().unwrap() > 0);
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    let bytes = STANDARD
        .decode(body["data"][0]["b64_json"].as_str().unwrap())
        .unwrap();
    let decoded = image::load_from_memory_with_format(&bytes, ImageFormat::Png).unwrap();
    assert_eq!((decoded.width(), decoded.height()), (1024, 1024));
    assert!(body["data"][0].get("url").is_none());

    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Generate {
            prompt: "a tiny terminal icon".to_string(),
            n: 2,
            size: "1024x1024".to_string(),
            output_format: "png".to_string(),
        }]
    );
}

#[tokio::test]
async fn generation_auto_size_reports_actual_dimensions_and_opaque_background() {
    let fake = FakeGenerator {
        calls: Arc::default(),
        delay: Duration::ZERO,
        image_bytes: png_bytes(1448, 1086),
        failure: None,
    };
    let app = build_router(config(), Arc::new(fake), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "size": "auto",
            "background": "auto",
            "output_format": "png"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["size"], "1448x1086");
    assert_eq!(body["background"], "opaque");
}

#[tokio::test]
async fn generation_rejects_returned_png_with_wrong_dimensions() {
    let fake = FakeGenerator {
        calls: Arc::default(),
        delay: Duration::ZERO,
        image_bytes: png_bytes(1254, 1254),
        failure: None,
    };
    let app = build_router(config(), Arc::new(fake), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "size": "1024x1024",
            "output_format": "png"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["error"]["type"], "server_error");
    assert_eq!(body["error"]["code"], "image_generation_failed");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not the requested 1024x1024")
    );
}

#[tokio::test]
async fn generation_accepts_aspect_ratio_size_extension() {
    let fake = FakeGenerator {
        calls: Arc::default(),
        delay: Duration::ZERO,
        image_bytes: png_bytes(1448, 1086),
        failure: None,
    };
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "size": "4:3",
            "output_format": "png"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["size"], "1448x1086");
    assert!(body["data"][0]["b64_json"].as_str().is_some());
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Generate {
            prompt: "a tiny terminal icon".to_string(),
            n: 1,
            size: "4:3".to_string(),
            output_format: "png".to_string(),
        }]
    );
}

#[tokio::test]
async fn generation_passes_arbitrary_gpt_image_2_size_through_exactly() {
    let fake = FakeGenerator {
        calls: Arc::default(),
        delay: Duration::ZERO,
        image_bytes: png_bytes(1536, 864),
        failure: None,
    };
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a cinematic terminal icon",
            "size": "1536x864",
            "output_format": "png"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["size"], "1536x864");
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Generate {
            prompt: "a cinematic terminal icon".to_string(),
            n: 1,
            size: "1536x864".to_string(),
            output_format: "png".to_string(),
        }]
    );
}

#[tokio::test]
async fn generation_rejects_returned_png_with_wrong_aspect_ratio() {
    let fake = FakeGenerator {
        calls: Arc::default(),
        delay: Duration::ZERO,
        image_bytes: png_bytes(1254, 1254),
        failure: None,
    };
    let app = build_router(config(), Arc::new(fake), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "size": "16:9",
            "output_format": "png"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["error"]["type"], "server_error");
    assert_eq!(body["error"]["code"], "image_generation_failed");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not the requested 16:9 aspect ratio")
    );
}

#[tokio::test]
async fn bearer_token_is_required() {
    let app = build_router(config(), Arc::new(FakeGenerator::default()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        None,
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_request_id(&_headers);
    assert_eq!(body["error"]["type"], "authentication_error");
    assert_eq!(body["error"]["message"], "Invalid Authentication");
    assert_eq!(body["error"]["code"], "invalid_api_key");
}

#[tokio::test]
async fn manually_constructed_config_without_tokens_does_not_allow_images() {
    let mut cfg = config();
    cfg.auth_token = None;
    cfg.admin_token = None;
    let app = build_router(cfg, Arc::new(FakeGenerator::default()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        None,
        json!({
            "model": "gpt-image-2",
            "prompt": "must not use the legacy default tenant"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"]["code"], "invalid_api_key");
}

#[tokio::test]
async fn admin_token_alone_does_not_authorize_image_requests() {
    let mut cfg = config();
    cfg.auth_token = None;
    cfg.admin_token = Some("admin-token".to_string());
    let app = build_router_with_api_key_store(
        cfg,
        Arc::new(FakeGenerator::default()),
        usage_store(),
        Arc::new(InMemoryApiKeyStore::default()),
    );

    let (missing_status, _headers, missing_body) = send_json(
        app.clone(),
        None,
        json!({
            "model": "gpt-image-2",
            "prompt": "must not be public"
        }),
    )
    .await;
    assert_eq!(missing_status, StatusCode::UNAUTHORIZED);
    assert_eq!(missing_body["error"]["code"], "invalid_api_key");

    let (admin_status, _headers, admin_body) = send_json(
        app,
        Some("admin-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "admin is not an image key"
        }),
    )
    .await;
    assert_eq!(admin_status, StatusCode::UNAUTHORIZED);
    assert_eq!(admin_body["error"]["code"], "invalid_api_key");
}

#[tokio::test]
async fn legacy_image_token_without_admin_token_cannot_manage_api_keys() {
    let mut cfg = config();
    cfg.auth_token = Some("legacy-image-token".to_string());
    cfg.admin_token = None;
    let app = build_router_with_api_key_store(
        cfg,
        Arc::new(FakeGenerator::default()),
        usage_store(),
        Arc::new(InMemoryApiKeyStore::default()),
    );

    let (image_status, _headers, image_body) = send_json(
        app.clone(),
        Some("legacy-image-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "legacy token can still generate"
        }),
    )
    .await;
    assert_eq!(image_status, StatusCode::OK);
    assert!(image_body["data"][0]["b64_json"].as_str().is_some());

    let (admin_status, _headers, admin_body) = post_json_to(
        app,
        "/v1/organization/projects/proj_alpha/service_accounts",
        Some("legacy-image-token"),
        json!({ "name": "Must Not Be Admin" }),
    )
    .await;
    assert_eq!(admin_status, StatusCode::UNAUTHORIZED);
    assert_eq!(admin_body["error"]["type"], "authentication_error");
    assert_eq!(admin_body["error"]["code"], "invalid_api_key");
}

#[tokio::test]
async fn url_response_format_is_rejected_for_gpt_image_models() {
    let app = build_router(config(), Arc::new(FakeGenerator::default()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "response_format": "url"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_request_id(&_headers);
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(body["error"]["param"], "response_format");
    assert_eq!(body["error"]["code"], "unsupported_parameter");
}

#[tokio::test]
async fn generation_stream_true_returns_final_only_sse_event() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake), usage_store());

    let (status, headers, body) = send_json_raw(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "stream": true,
            "partial_images": 0
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    let body = String::from_utf8(body).unwrap();
    let events = sse_body_events(&body);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "image_generation.completed");
    assert_eq!(events[0].1["type"], "image_generation.completed");
    assert!(events[0].1["b64_json"].as_str().is_some());
    assert_eq!(events[0].1["background"], "opaque");
    assert_eq!(events[0].1["output_format"], "png");
    assert_eq!(events[0].1["quality"], "auto");
    assert_eq!(events[0].1["size"], "1024x1024");
    assert!(events[0].1["created_at"].as_i64().is_some());
    assert!(events[0].1.get("created").is_none());
    assert!(events[0].1.get("usage").is_none());
    assert!(events[0].1.get("partial_image_index").is_none());
    assert_eq!(
        stable_sse_projection(&events[0].1),
        runtime_event_fixture("image_generation.completed")
    );
}

#[tokio::test]
async fn generation_rejects_unknown_field_without_calling_provider() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "input_fidelity": "high"
        }),
    )
    .await;

    assert_error_fixture(status, &body, "unknown_generation_field");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn generation_rejects_true_partial_image_streaming() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "stream": true,
            "partial_images": 1
        }),
    )
    .await;

    assert_error_fixture(status, &body, "partial_images_gt_zero");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn generation_accepts_gpt_image_2_snapshot_model() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, _body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2-2026-04-21",
            "prompt": "a tiny terminal icon"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn generation_rejects_unsupported_moderation_low() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "moderation": "low"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "moderation");
    assert_eq!(body["error"]["code"], "unsupported_parameter");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn generation_rejects_output_compression_above_100() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "output_format": "webp",
            "output_compression": 101
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "output_compression");
    assert_eq!(body["error"]["code"], "invalid_value");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn invalid_gpt_image_2_size_is_rejected_before_generation() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "a tiny terminal icon",
            "size": "1000x1000"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "size");
    assert_eq!(body["error"]["code"], "invalid_image_size");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn usage_limits_apply_across_router_restarts() {
    let store = usage_store();
    let mut cfg = config();
    cfg.five_hour_image_limit = 2;
    cfg.seven_day_image_limit = 2;

    let first_app = build_router(
        cfg.clone(),
        Arc::new(FakeGenerator::default()),
        store.clone(),
    );
    let (first_status, _headers, _body) = send_json(
        first_app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "two images",
            "n": 2
        }),
    )
    .await;
    assert_eq!(first_status, StatusCode::OK);

    let restarted_app = build_router(cfg, Arc::new(FakeGenerator::default()), store);
    let (status, headers, body) = send_json(
        restarted_app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "one more image",
            "n": 1
        }),
    )
    .await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_request_id(&headers);
    assert_eq!(body["error"]["code"], "rate_limit_exceeded");
    assert_eq!(headers["x-ratelimit-limit-5h"], "2");
    assert_eq!(headers["x-ratelimit-remaining-5h"], "0");
}

#[tokio::test]
async fn failed_generation_releases_reserved_quota() {
    let store = usage_store();
    let mut cfg = config();
    cfg.five_hour_image_limit = 1;
    cfg.seven_day_image_limit = 1;

    let failing_app = build_router(
        cfg.clone(),
        Arc::new(FakeGenerator {
            failure: Some(FakeFailure::CodexCliFailed),
            ..Default::default()
        }),
        store.clone(),
    );
    let (failed_status, _headers, failed_body) = send_json(
        failing_app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "this provider call fails"
        }),
    )
    .await;
    assert_eq!(failed_status, StatusCode::BAD_GATEWAY);
    assert_eq!(failed_body["error"]["code"], "codex_cli_failed");

    let retry_app = build_router(cfg, Arc::new(FakeGenerator::default()), store);
    let (retry_status, retry_headers, _retry_body) = send_json(
        retry_app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "retry after provider failure"
        }),
    )
    .await;

    assert_eq!(retry_status, StatusCode::OK);
    assert_eq!(retry_headers["x-ratelimit-limit-5h"], "1");
    assert_eq!(retry_headers["x-ratelimit-remaining-5h"], "0");
}

#[tokio::test]
async fn models_response_lists_active_provider_models() {
    let app = build_router(config(), Arc::new(FakeGenerator::default()), usage_store());

    let (status, headers, body) = get_with_token(app, "/v1/models", Some("test-token")).await;
    let body: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_request_id(&headers);
    let ids = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|model| model["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["gpt-image-2", "gpt-image-2-2026-04-21"]);
}

#[tokio::test]
async fn admin_can_create_project_service_account_api_key_and_use_it() {
    let key_store = Arc::new(InMemoryApiKeyStore::default());
    let app = build_router_with_api_key_store(
        config(),
        Arc::new(FakeGenerator::default()),
        usage_store(),
        key_store,
    );

    let (create_status, create_headers, created) = post_json_to(
        app.clone(),
        "/v1/organization/projects/proj_alpha/service_accounts",
        Some("admin-token"),
        json!({ "name": "Production App" }),
    )
    .await;

    assert_eq!(create_status, StatusCode::OK);
    assert_request_id(&create_headers);
    assert_eq!(created["object"], "organization.project.service_account");
    assert_eq!(created["role"], "member");
    assert_eq!(
        created["api_key"]["object"],
        "organization.project.service_account.api_key"
    );
    let api_key = created["api_key"]["value"].as_str().unwrap();
    assert!(api_key.starts_with("sk-gw-"));

    let (status, headers, body) = send_json(
        app,
        Some(api_key),
        json!({
            "model": "gpt-image-2",
            "prompt": "tenant scoped image"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["openai-project"], "proj_alpha");
    assert_eq!(headers["x-ratelimit-limit-5h"], "10");
    assert_eq!(headers["x-ratelimit-remaining-5h"], "9");
    assert!(body["data"][0]["b64_json"].as_str().is_some());
}

#[tokio::test]
async fn image_api_token_does_not_authorize_admin_requests() {
    let app = build_router_with_api_key_store(
        config(),
        Arc::new(FakeGenerator::default()),
        usage_store(),
        Arc::new(InMemoryApiKeyStore::default()),
    );

    let (status, headers, body) = post_json_to(
        app,
        "/v1/organization/projects/proj_alpha/service_accounts",
        Some("test-token"),
        json!({ "name": "Not Admin" }),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_request_id(&headers);
    assert_eq!(body["error"]["type"], "authentication_error");
    assert_eq!(body["error"]["code"], "invalid_api_key");
}

#[tokio::test]
async fn deleted_project_api_key_cannot_be_used_again() {
    let key_store = Arc::new(InMemoryApiKeyStore::default());
    let app = build_router_with_api_key_store(
        config(),
        Arc::new(FakeGenerator::default()),
        usage_store(),
        key_store,
    );
    let (_create_status, _headers, created) = post_json_to(
        app.clone(),
        "/v1/organization/projects/proj_alpha/service_accounts",
        Some("admin-token"),
        json!({ "name": "Temporary App" }),
    )
    .await;
    let key_id = created["api_key"]["id"].as_str().unwrap();
    let api_key = created["api_key"]["value"].as_str().unwrap().to_string();

    let (delete_status, _headers, deleted) = delete_with_token(
        app.clone(),
        &format!("/v1/organization/projects/proj_alpha/api_keys/{key_id}"),
        Some("admin-token"),
    )
    .await;
    assert_eq!(delete_status, StatusCode::OK);
    assert_eq!(deleted["object"], "organization.project.api_key.deleted");
    assert_eq!(deleted["deleted"], true);

    let (status, _headers, body) = send_json(
        app,
        Some(&api_key),
        json!({
            "model": "gpt-image-2",
            "prompt": "should not authenticate"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"]["code"], "invalid_api_key");
}

#[tokio::test]
async fn project_api_key_list_redacts_secret_values() {
    let app = build_router_with_api_key_store(
        config(),
        Arc::new(FakeGenerator::default()),
        usage_store(),
        Arc::new(InMemoryApiKeyStore::default()),
    );
    let (_create_status, _headers, created) = post_json_to(
        app.clone(),
        "/v1/organization/projects/proj_alpha/service_accounts",
        Some("admin-token"),
        json!({ "name": "Listable App" }),
    )
    .await;
    let api_key = created["api_key"]["value"].as_str().unwrap();
    let key_id = created["api_key"]["id"].as_str().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/v1/organization/projects/proj_alpha/api_keys?limit=20")
                .header(header::AUTHORIZATION, "Bearer admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["object"], "list");
    assert_eq!(body["data"][0]["id"], key_id);
    assert!(
        body["data"][0]["redacted_value"]
            .as_str()
            .unwrap()
            .starts_with("sk-gw-...")
    );
    assert_ne!(body["data"][0]["redacted_value"], api_key);
    assert!(body["data"][0].get("value").is_none());
}

#[tokio::test]
async fn tenant_usage_limits_are_isolated_by_api_key_project() {
    let mut cfg = config();
    cfg.five_hour_image_limit = 1;
    cfg.seven_day_image_limit = 1;
    let key_store = Arc::new(InMemoryApiKeyStore::default());
    let app = build_router_with_api_key_store(
        cfg,
        Arc::new(FakeGenerator::default()),
        usage_store(),
        key_store,
    );

    let (_status, _headers, alpha) = post_json_to(
        app.clone(),
        "/v1/organization/projects/proj_alpha/service_accounts",
        Some("admin-token"),
        json!({ "name": "Alpha" }),
    )
    .await;
    let (_status, _headers, beta) = post_json_to(
        app.clone(),
        "/v1/organization/projects/proj_beta/service_accounts",
        Some("admin-token"),
        json!({ "name": "Beta" }),
    )
    .await;
    let alpha_key = alpha["api_key"]["value"].as_str().unwrap();
    let beta_key = beta["api_key"]["value"].as_str().unwrap();

    let (alpha_first, _headers, _body) = send_json(
        app.clone(),
        Some(alpha_key),
        json!({ "model": "gpt-image-2", "prompt": "alpha first" }),
    )
    .await;
    assert_eq!(alpha_first, StatusCode::OK);

    let (alpha_second, _headers, alpha_error) = send_json(
        app.clone(),
        Some(alpha_key),
        json!({ "model": "gpt-image-2", "prompt": "alpha second" }),
    )
    .await;
    assert_eq!(alpha_second, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(alpha_error["error"]["code"], "rate_limit_exceeded");

    let (beta_status, beta_headers, _body) = send_json(
        app,
        Some(beta_key),
        json!({ "model": "gpt-image-2", "prompt": "beta still has quota" }),
    )
    .await;
    assert_eq!(beta_status, StatusCode::OK);
    assert_eq!(beta_headers["openai-project"], "proj_beta");
}

#[tokio::test]
async fn concurrent_generation_over_queue_limit_is_rejected() {
    let mut cfg = config();
    cfg.max_concurrent_jobs = 1;
    cfg.max_queue_size = 0;
    cfg.queue_timeout = Duration::from_millis(20);

    let app = build_router(
        cfg,
        Arc::new(FakeGenerator {
            calls: Arc::default(),
            delay: Duration::from_millis(150),
            image_bytes: png_bytes(1024, 1024),
            failure: None,
        }),
        usage_store(),
    );

    let first = tokio::spawn(send_json(
        app.clone(),
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "slow image"
        }),
    ));

    tokio::time::sleep(Duration::from_millis(20)).await;

    let (status, _headers, body) = send_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "queued image"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"]["code"], "rate_limit_exceeded");

    let (first_status, _headers, _body) = first.await.unwrap();
    assert_eq!(first_status, StatusCode::OK);
}

#[tokio::test]
async fn tenant_concurrency_pool_is_isolated_from_other_tenants() {
    let mut cfg = config();
    cfg.max_concurrent_jobs = 2;
    cfg.max_queue_size = 0;
    cfg.max_concurrent_jobs_per_tenant = 1;
    cfg.max_queue_size_per_tenant = 0;
    cfg.queue_timeout = Duration::from_millis(20);

    let key_store = Arc::new(InMemoryApiKeyStore::default());
    let app = build_router_with_api_key_store(
        cfg,
        Arc::new(FakeGenerator {
            calls: Arc::default(),
            delay: Duration::from_millis(150),
            image_bytes: png_bytes(1024, 1024),
            failure: None,
        }),
        usage_store(),
        key_store,
    );

    let (_status, _headers, alpha) = post_json_to(
        app.clone(),
        "/v1/organization/projects/proj_alpha/service_accounts",
        Some("admin-token"),
        json!({ "name": "Alpha" }),
    )
    .await;
    let (_status, _headers, beta) = post_json_to(
        app.clone(),
        "/v1/organization/projects/proj_beta/service_accounts",
        Some("admin-token"),
        json!({ "name": "Beta" }),
    )
    .await;
    let alpha_key = alpha["api_key"]["value"].as_str().unwrap().to_string();
    let beta_key = beta["api_key"]["value"].as_str().unwrap().to_string();

    let first_alpha_app = app.clone();
    let first_alpha_key = alpha_key.clone();
    let first_alpha = tokio::spawn(async move {
        send_json(
            first_alpha_app,
            Some(&first_alpha_key),
            json!({ "model": "gpt-image-2", "prompt": "slow alpha image" }),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let (second_alpha_status, _headers, second_alpha_body) = send_json(
        app.clone(),
        Some(&alpha_key),
        json!({ "model": "gpt-image-2", "prompt": "blocked alpha image" }),
    )
    .await;
    assert_eq!(second_alpha_status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(second_alpha_body["error"]["code"], "rate_limit_exceeded");

    let (beta_status, beta_headers, _body) = send_json(
        app,
        Some(&beta_key),
        json!({ "model": "gpt-image-2", "prompt": "beta image" }),
    )
    .await;
    assert_eq!(beta_status, StatusCode::OK);
    assert_eq!(beta_headers["openai-project"], "proj_beta");

    let (first_status, _headers, _body) = first_alpha.await.unwrap();
    assert_eq!(first_status, StatusCode::OK);
}

#[tokio::test]
async fn edits_reject_unsupported_multipart_moderation_low() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_edit_multipart(
        app,
        &[
            ("model", "gpt-image-2"),
            ("prompt", "turn this into a product shot"),
            ("moderation", "low"),
        ],
        true,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "moderation");
    assert_eq!(body["error"]["code"], "unsupported_parameter");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_reject_true_partial_image_streaming() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_edit_multipart(
        app,
        &[
            ("model", "gpt-image-2"),
            ("prompt", "turn this into a product shot"),
            ("stream", "true"),
            ("partial_images", "1"),
        ],
        true,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "partial_images");
    assert_eq!(body["error"]["code"], "unsupported_parameter");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_accept_json_data_url_images_and_mask() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let image_url = format!("data:image/png;base64,{}", STANDARD.encode(png_bytes(1, 1)));

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "use the reference",
            "images": [{ "image_url": image_url }],
            "mask": { "image_url": image_url }
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["data"][0]["b64_json"].as_str().is_some());
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Edit {
            prompt: "use the reference".to_string(),
            images: 1,
            has_mask: true,
            n: 1,
        }]
    );
}

#[tokio::test]
async fn edits_accept_json_data_url_image_field() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let image_url = format!("data:image/png;base64,{}", STANDARD.encode(TINY_PNG));

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "use the single image field",
            "image": { "image_url": image_url }
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["data"][0]["b64_json"].as_str().is_some());
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Edit {
            prompt: "use the single image field".to_string(),
            images: 1,
            has_mask: false,
            n: 1,
        }]
    );
}

#[tokio::test]
async fn edits_accept_json_b64_json_images_and_mask() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let encoded = STANDARD.encode(png_bytes(1, 1));

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "use raw base64 references",
            "images": [
                { "b64_json": encoded },
                { "b64_json": encoded, "mime_type": "image/png" }
            ],
            "mask": { "b64_json": encoded, "mime_type": "image/png" },
            "n": 2
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Edit {
            prompt: "use raw base64 references".to_string(),
            images: 2,
            has_mask: true,
            n: 2,
        }]
    );
}

#[tokio::test]
async fn edits_accept_multiple_json_images_and_generate_requested_count() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let image_url = format!("data:image/png;base64,{}", STANDARD.encode(TINY_PNG));

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "combine both references into a clean product shot",
            "images": [
                { "image_url": image_url },
                { "image_url": image_url }
            ],
            "n": 3
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 3);
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Edit {
            prompt: "combine both references into a clean product shot".to_string(),
            images: 2,
            has_mask: false,
            n: 3,
        }]
    );
}

#[tokio::test]
async fn edits_reject_json_more_than_16_images() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let image_url = format!("data:image/png;base64,{}", STANDARD.encode(TINY_PNG));
    let images = (0..17)
        .map(|_| json!({ "image_url": image_url }))
        .collect::<Vec<_>>();

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "too many references",
            "images": images
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "image");
    assert_eq!(body["error"]["code"], "invalid_value");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_reject_n_outside_1_to_10() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let image_url = format!("data:image/png;base64,{}", STANDARD.encode(TINY_PNG));

    for n in [0, 11] {
        let (status, _headers, body) = send_edit_json(
            app.clone(),
            Some("test-token"),
            json!({
                "model": "gpt-image-2",
                "prompt": "bad n",
                "images": [{ "image_url": image_url }],
                "n": n
            }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["param"], "n");
        assert_eq!(body["error"]["code"], "invalid_value");
    }
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_accept_n_10() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let image_url = format!("data:image/png;base64,{}", STANDARD.encode(TINY_PNG));

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "ten candidates",
            "images": [{ "image_url": image_url }],
            "n": 10
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 10);
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Edit {
            prompt: "ten candidates".to_string(),
            images: 1,
            has_mask: false,
            n: 10,
        }]
    );
}

#[tokio::test]
async fn edits_reject_json_remote_image_url() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "use the reference",
            "images": [{ "image_url": "https://example.com/input.png" }]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "images");
    assert_eq!(body["error"]["code"], "unsupported_parameter");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_reject_json_data_url_above_limit_before_decode() {
    let fake = FakeGenerator::default();
    let mut cfg = config();
    cfg.max_upload_bytes = 8;
    let app = build_router(cfg, Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "use the reference",
            "images": [{ "image_url": format!("data:image/png;base64,{}", STANDARD.encode(png_bytes(32, 32))) }]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["error"]["code"], "request_too_large");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_reject_json_data_url_magic_mismatch() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let jpeg_bytes = b"\xff\xd8\xffnot-a-real-jpeg";

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "use the reference",
            "images": [{ "image_url": format!("data:image/png;base64,{}", STANDARD.encode(jpeg_bytes)) }]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "image");
    assert_eq!(body["error"]["code"], "invalid_image_format");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_reject_json_file_id_references() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "use the reference",
            "images": [{ "file_id": "file_123" }]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "images");
    assert_eq!(body["error"]["code"], "unsupported_parameter");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_reject_json_image_reference_with_both_file_id_and_image_url() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let image_url = format!("data:image/png;base64,{}", STANDARD.encode(TINY_PNG));

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "use the reference",
            "images": [{ "file_id": "file_123", "image_url": image_url }]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "images");
    assert_eq!(body["error"]["code"], "invalid_value");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_reject_json_image_reference_with_both_image_url_and_b64_json() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let encoded = STANDARD.encode(TINY_PNG);
    let image_url = format!("data:image/png;base64,{encoded}");

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "ambiguous reference",
            "images": [{ "image_url": image_url, "b64_json": encoded }]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "images");
    assert_eq!(body["error"]["code"], "invalid_value");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_reject_json_b64_json_magic_mismatch() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());
    let jpeg_bytes = b"\xff\xd8\xffnot-a-real-jpeg";

    let (status, _headers, body) = send_edit_json(
        app,
        Some("test-token"),
        json!({
            "model": "gpt-image-2",
            "prompt": "bad raw base64",
            "images": [{
                "b64_json": STANDARD.encode(jpeg_bytes),
                "mime_type": "image/png"
            }]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "image");
    assert_eq!(body["error"]["code"], "invalid_image_format");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_stream_true_returns_final_only_sse_event() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake), usage_store());
    let image_url = format!("data:image/png;base64,{}", STANDARD.encode(TINY_PNG));

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/images/edits")
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "model": "gpt-image-2",
                        "prompt": "use the reference",
                        "images": [{ "image_url": image_url }],
                        "stream": true,
                        "partial_images": 0
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    let events = sse_body_events(&body);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "image_edit.completed");
    assert_eq!(events[0].1["type"], "image_edit.completed");
    assert!(events[0].1["b64_json"].as_str().is_some());
    assert_eq!(events[0].1["background"], "opaque");
    assert_eq!(events[0].1["output_format"], "png");
    assert_eq!(events[0].1["quality"], "auto");
    assert_eq!(events[0].1["size"], "1024x1024");
    assert!(events[0].1["created_at"].as_i64().is_some());
    assert!(events[0].1.get("created").is_none());
    assert!(events[0].1.get("usage").is_none());
    assert!(events[0].1.get("partial_image_index").is_none());
    assert_eq!(
        stable_sse_projection(&events[0].1),
        runtime_event_fixture("image_edit.completed")
    );
}

#[tokio::test]
async fn edits_reject_partial_images_without_calling_generator() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_edit_multipart(
        app,
        &[
            ("model", "gpt-image-2"),
            ("prompt", "turn this into a product shot"),
            ("partial_images", "1"),
        ],
        true,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "partial_images");
    assert_eq!(body["error"]["code"], "unsupported_parameter");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_reject_input_fidelity_for_gpt_image_2() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_edit_multipart(
        app,
        &[
            ("model", "gpt-image-2"),
            ("prompt", "turn this into a product shot"),
            ("input_fidelity", "high"),
        ],
        true,
    )
    .await;

    assert_error_fixture(status, &body, "edit_input_fidelity");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_reject_output_compression_above_100() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let (status, _headers, body) = send_edit_multipart(
        app,
        &[
            ("model", "gpt-image-2"),
            ("prompt", "turn this into a product shot"),
            ("output_format", "jpeg"),
            ("output_compression", "101"),
        ],
        true,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "output_compression");
    assert_eq!(body["error"]["code"], "invalid_value");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn edits_accept_multipart_images_and_return_base64_json() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let boundary = "x-test-boundary";
    let mut body = Vec::new();
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngpt-image-2\r\n",
    );
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"prompt\"\r\n\r\nturn this into a product shot\r\n",
    );
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"input.png\"\r\nContent-Type: image/png\r\n\r\n",
    );
    body.extend_from_slice(TINY_PNG);
    body.extend_from_slice(b"\r\n--x-test-boundary--\r\n");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/images/edits")
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(status, StatusCode::OK);
    let bytes = STANDARD
        .decode(body["data"][0]["b64_json"].as_str().unwrap())
        .unwrap();
    let decoded = image::load_from_memory_with_format(&bytes, ImageFormat::Png).unwrap();
    assert_eq!((decoded.width(), decoded.height()), (1024, 1024));
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Edit {
            prompt: "turn this into a product shot".to_string(),
            images: 1,
            has_mask: false,
            n: 1,
        }]
    );
}

#[tokio::test]
async fn edits_reports_codex_cli_failed_with_specific_code() {
    let fake = FakeGenerator {
        failure: Some(FakeFailure::CodexCliFailed),
        ..Default::default()
    };
    let app = build_router(config(), Arc::new(fake), usage_store());

    let (status, _headers, body) = send_edit_multipart(
        app,
        &[
            ("model", "gpt-image-2"),
            ("prompt", "turn this into a product shot"),
        ],
        true,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["error"]["type"], "server_error");
    assert_eq!(body["error"]["code"], "codex_cli_failed");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Codex CLI exited")
    );
}

#[tokio::test]
async fn edits_reports_missing_codex_output_with_specific_code() {
    let fake = FakeGenerator {
        failure: Some(FakeFailure::CodexNoImageOutput),
        ..Default::default()
    };
    let app = build_router(config(), Arc::new(fake), usage_store());

    let (status, _headers, body) = send_edit_multipart(
        app,
        &[
            ("model", "gpt-image-2"),
            ("prompt", "turn this into a product shot"),
        ],
        true,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["error"]["type"], "server_error");
    assert_eq!(body["error"]["code"], "codex_no_image_output");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("did not save")
    );
}

#[tokio::test]
async fn edits_accept_multipart_bare_image_field() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let boundary = "x-test-boundary";
    let mut body = Vec::new();
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngpt-image-2\r\n",
    );
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"prompt\"\r\n\r\nuse the bare image field\r\n",
    );
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"image\"; filename=\"input.png\"\r\nContent-Type: image/png\r\n\r\n",
    );
    body.extend_from_slice(TINY_PNG);
    body.extend_from_slice(b"\r\n--x-test-boundary--\r\n");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/images/edits")
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert!(body["data"][0]["b64_json"].as_str().is_some());
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Edit {
            prompt: "use the bare image field".to_string(),
            images: 1,
            has_mask: false,
            n: 1,
        }]
    );
}

#[tokio::test]
async fn edits_accept_multipart_mask() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let boundary = "x-test-boundary";
    let rgba_png = png_bytes(1, 1);
    let mut body = Vec::new();
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngpt-image-2\r\n",
    );
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"prompt\"\r\n\r\nuse the mask\r\n",
    );
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"input.png\"\r\nContent-Type: image/png\r\n\r\n",
    );
    body.extend_from_slice(&rgba_png);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"mask\"; filename=\"mask.png\"\r\nContent-Type: image/png\r\n\r\n",
    );
    body.extend_from_slice(&rgba_png);
    body.extend_from_slice(b"\r\n--x-test-boundary--\r\n");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/images/edits")
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert!(body["data"][0]["b64_json"].as_str().is_some());
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Edit {
            prompt: "use the mask".to_string(),
            images: 1,
            has_mask: true,
            n: 1,
        }]
    );
}

#[tokio::test]
async fn edits_accept_multiple_multipart_images_and_generate_requested_count() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let boundary = "x-test-boundary";
    let mut body = Vec::new();
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngpt-image-2\r\n",
    );
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"prompt\"\r\n\r\nmerge these references\r\n",
    );
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"n\"\r\n\r\n2\r\n",
    );
    for name in ["first.png", "second.png"] {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"{name}\"\r\nContent-Type: image/png\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(TINY_PNG);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"--x-test-boundary--\r\n");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/images/edits")
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    assert_eq!(
        fake.calls.lock().unwrap().as_slice(),
        &[FakeCall::Edit {
            prompt: "merge these references".to_string(),
            images: 2,
            has_mask: false,
            n: 2,
        }]
    );
}

#[tokio::test]
async fn edits_reject_duplicate_multipart_mask() {
    let fake = FakeGenerator::default();
    let app = build_router(config(), Arc::new(fake.clone()), usage_store());

    let boundary = "x-test-boundary";
    let mut body = Vec::new();
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngpt-image-2\r\n",
    );
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"prompt\"\r\n\r\nuse the mask\r\n",
    );
    body.extend_from_slice(
        b"--x-test-boundary\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"input.png\"\r\nContent-Type: image/png\r\n\r\n",
    );
    body.extend_from_slice(TINY_PNG);
    body.extend_from_slice(b"\r\n");
    for name in ["first-mask.png", "second-mask.png"] {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"mask\"; filename=\"{name}\"\r\nContent-Type: image/png\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(TINY_PNG);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"--x-test-boundary--\r\n");

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/images/edits")
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "mask");
    assert_eq!(body["error"]["code"], "invalid_value");
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn openapi_json_documents_images_api() {
    let app = build_router(config(), Arc::new(FakeGenerator::default()), usage_store());

    let (status, headers, bytes) = get(app, "/openapi.json").await;
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert!(
        headers[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    assert_eq!(body["openapi"], "3.1.0");
    assert_eq!(body["info"]["title"], "AI Image Factory API");
    assert!(body["paths"]["/v1/images/generations"]["post"].is_object());
    assert!(
        body["paths"]["/v1/images/generations"]["post"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|parameter| parameter["name"] == "Idempotency-Key"
                && parameter["in"] == "header"
                && parameter["required"] == false)
    );
    assert!(body["paths"]["/v1/images/generations"]["post"]["responses"]["409"].is_object());
    assert!(body["paths"]["/v1/images/edits"]["post"].is_object());
    assert!(body["paths"]["/healthz"]["get"].is_object());
    assert!(body["paths"]["/readyz"]["get"]["responses"]["200"].is_object());
    assert!(body["paths"]["/readyz"]["get"]["responses"]["503"].is_object());
    assert!(
        body["paths"]["/v1/organization/projects/{project_id}/service_accounts"]["post"]
            .is_object()
    );
    assert!(body["paths"]["/v1/organization/projects/{project_id}/api_keys"]["get"].is_object());
    assert!(
        body["paths"]["/v1/organization/projects/{project_id}/api_keys/{api_key_id}"]["delete"]
            .is_object()
    );
    assert!(
        body["components"]["schemas"]["ImageGenerationRequest"]["properties"]["model"]["enum"]
            .as_array()
            .unwrap()
            .contains(&json!("gpt-image-2-2026-04-21"))
    );
    assert_eq!(
        body["components"]["schemas"]["ImageGenerationRequest"]["properties"]["moderation"]["enum"],
        json!(["auto"]),
    );
    assert!(
        body["components"]["schemas"]["ImageGenerationRequest"]["properties"]["stream"].is_object()
    );
    assert!(
        body["components"]["schemas"]["ImageStreamEvent"]["properties"]["created_at"].is_object()
    );
    assert!(
        body["components"]["schemas"]["ImageStreamEvent"]["properties"]
            .get("created")
            .is_none()
    );
    assert_eq!(
        body["components"]["schemas"]["ImageEditRequest"]["properties"]["moderation"]["enum"],
        json!(["auto"]),
    );
    assert!(body["components"]["schemas"]["ImageEditRequest"]["properties"]["stream"].is_object());
    assert!(
        body["components"]["schemas"]["ImageEditRequest"]["properties"]
            .get("input_fidelity")
            .is_none()
    );
    assert_eq!(
        body["components"]["schemas"]["ImageEditRequest"]["anyOf"][0]["required"][0],
        "image"
    );
    assert_eq!(
        body["components"]["securitySchemes"]["BearerAuth"]["type"],
        "http"
    );
}

#[tokio::test]
async fn docs_render_scalar_api_reference() {
    let app = build_router(config(), Arc::new(FakeGenerator::default()), usage_store());

    let (status, headers, bytes) = get(app, "/docs").await;
    let html = String::from_utf8(bytes).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert!(
        headers[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
    assert!(html.contains("https://cdn.jsdelivr.net/npm/@scalar/api-reference"));
    assert!(html.contains("Scalar.createApiReference"));
    assert!(html.contains("url: '/openapi.json'"));
}
