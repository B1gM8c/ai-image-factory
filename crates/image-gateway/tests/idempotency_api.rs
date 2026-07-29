use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gpt_image_2_gateway::{
    AppConfig, EditJob, GeneratedImage, GenerationJob, ImageGatewayError, ImageGenerator,
    InMemoryUsageStore, build_router,
};
use image::{ImageBuffer, ImageFormat, Rgba};
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Clone)]
struct CountingGenerator {
    calls: Arc<AtomicUsize>,
    delay: Duration,
    image: Vec<u8>,
}

impl CountingGenerator {
    fn new(delay: Duration) -> Self {
        let image = ImageBuffer::from_pixel(1024, 1024, Rgba([255u8, 255, 255, 255]));
        let mut cursor = std::io::Cursor::new(Vec::new());
        image
            .write_to(&mut cursor, ImageFormat::Png)
            .expect("encode test image");
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            delay,
            image: cursor.into_inner(),
        }
    }
}

#[async_trait]
impl ImageGenerator for CountingGenerator {
    async fn generate(&self, job: GenerationJob) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        Ok((0..job.n)
            .map(|_| GeneratedImage {
                bytes: self.image.clone(),
            })
            .collect())
    }

    async fn edit(&self, job: EditJob) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        Ok((0..job.n)
            .map(|_| GeneratedImage {
                bytes: self.image.clone(),
            })
            .collect())
    }
}

#[tokio::test]
async fn completed_idempotent_request_is_not_executed_again() {
    let generator = CountingGenerator::new(Duration::ZERO);
    let app = app(generator.clone());
    let body = generation_body("one prompt");

    let first = send_raw(app.clone(), "retry-key", body.clone()).await;
    let second = send_raw(app, "retry-key", body).await;

    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
    assert_eq!(second.2, first.2);
    assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn completed_streaming_request_replays_identical_sse_bytes() {
    let generator = CountingGenerator::new(Duration::ZERO);
    let app = app(generator.clone());
    let mut body = generation_body("streaming prompt");
    body["stream"] = json!(true);

    let first = send_raw(app.clone(), "stream-retry-key", body.clone()).await;
    let second = send_raw(app, "stream-retry-key", body).await;

    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
    assert_eq!(first.1[header::CONTENT_TYPE], "text/event-stream");
    assert_eq!(second.1[header::CONTENT_TYPE], "text/event-stream");
    assert_eq!(second.2, first.2);
    assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn idempotency_key_reuse_with_different_body_conflicts() {
    let generator = CountingGenerator::new(Duration::ZERO);
    let app = app(generator.clone());

    assert_eq!(
        send(app.clone(), "same-key", generation_body("first prompt"))
            .await
            .0,
        StatusCode::OK
    );
    let conflict = send(app, "same-key", generation_body("second prompt")).await;

    assert_error(conflict, StatusCode::CONFLICT, "idempotency_conflict");
    assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_idempotent_requests_have_one_provider_execution() {
    let generator = CountingGenerator::new(Duration::from_millis(100));
    let app = app(generator.clone());
    let body = generation_body("concurrent prompt");

    let (left, right) = tokio::join!(
        send(app.clone(), "concurrent-key", body.clone()),
        send(app, "concurrent-key", body),
    );

    let statuses = [left.0, right.0];
    assert!(statuses.contains(&StatusCode::OK));
    let rejected = if left.0 == StatusCode::OK {
        right
    } else {
        left
    };
    assert_error(rejected, StatusCode::CONFLICT, "idempotency_in_progress");
    assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_idempotency_key_is_rejected_before_provider_execution() {
    let generator = CountingGenerator::new(Duration::ZERO);
    let response = send(
        app(generator.clone()),
        "contains space",
        generation_body("prompt"),
    )
    .await;

    assert_error(response, StatusCode::BAD_REQUEST, "invalid_idempotency_key");
    assert_eq!(generator.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn completed_edit_request_replays_without_provider_execution() {
    let generator = CountingGenerator::new(Duration::ZERO);
    let app = app(generator.clone());
    let body = edit_body([255, 0, 0, 255], false);

    let first = send_edit_raw(app.clone(), "edit-retry-key", body.clone()).await;
    let second = send_edit_raw(app, "edit-retry-key", body).await;

    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
    assert_eq!(second.2, first.2);
    assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn completed_streaming_edit_replays_the_edit_event_kind() {
    let generator = CountingGenerator::new(Duration::ZERO);
    let app = app(generator.clone());
    let body = edit_body([0, 255, 0, 255], true);

    let first = send_edit_raw(app.clone(), "edit-stream-key", body.clone()).await;
    let second = send_edit_raw(app, "edit-stream-key", body).await;

    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.2, first.2);
    assert_eq!(first.1[header::CONTENT_TYPE], "text/event-stream");
    assert!(String::from_utf8_lossy(&first.2).contains("image_edit.completed"));
    assert!(!String::from_utf8_lossy(&first.2).contains("image_generation.completed"));
    assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn edit_idempotency_hash_covers_input_bytes() {
    let generator = CountingGenerator::new(Duration::ZERO);
    let app = app(generator.clone());

    let first = send_edit_raw(
        app.clone(),
        "edit-input-conflict",
        edit_body([0, 0, 255, 255], false),
    )
    .await;
    let conflict = send_edit_raw(
        app,
        "edit-input-conflict",
        edit_body([255, 255, 0, 255], false),
    )
    .await;

    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(conflict.0, StatusCode::CONFLICT);
    let body: Value = serde_json::from_slice(&conflict.2).expect("conflict JSON");
    assert_eq!(body["error"]["code"], "idempotency_conflict");
    assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_idempotent_edits_have_one_provider_execution() {
    let generator = CountingGenerator::new(Duration::from_millis(100));
    let app = app(generator.clone());
    let body = edit_body([255, 0, 255, 255], false);

    let (left, right) = tokio::join!(
        send_edit_raw(app.clone(), "edit-concurrent", body.clone()),
        send_edit_raw(app, "edit-concurrent", body),
    );

    assert!([left.0, right.0].contains(&StatusCode::OK));
    let rejected = if left.0 == StatusCode::OK {
        right
    } else {
        left
    };
    assert_eq!(rejected.0, StatusCode::CONFLICT);
    let body: Value = serde_json::from_slice(&rejected.2).expect("in-progress JSON");
    assert_eq!(body["error"]["code"], "idempotency_in_progress");
    assert_eq!(generator.calls.load(Ordering::SeqCst), 1);
}

fn app(generator: CountingGenerator) -> axum::Router {
    build_router(
        config(),
        Arc::new(generator),
        Arc::new(InMemoryUsageStore::default()),
    )
}

fn generation_body(prompt: &str) -> Value {
    json!({
        "model": "gpt-image-2",
        "prompt": prompt,
        "size": "1024x1024",
        "output_format": "png"
    })
}

fn edit_body(pixel: [u8; 4], stream: bool) -> Value {
    let image = ImageBuffer::from_pixel(1, 1, Rgba(pixel));
    let mut cursor = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut cursor, ImageFormat::Png)
        .expect("encode edit input");
    json!({
        "model": "gpt-image-2",
        "prompt": "edit this image",
        "images": [{
            "image_url": format!(
                "data:image/png;base64,{}",
                STANDARD.encode(cursor.into_inner())
            )
        }],
        "size": "1024x1024",
        "output_format": "png",
        "stream": stream
    })
}

async fn send(app: axum::Router, key: &str, body: Value) -> (StatusCode, Value) {
    let (status, _, body) = send_raw(app, key, body).await;
    (
        status,
        serde_json::from_slice(&body).expect("JSON response"),
    )
}

async fn send_raw(app: axum::Router, key: &str, body: Value) -> (StatusCode, HeaderMap, Vec<u8>) {
    send_raw_to(app, "/v1/images/generations", key, body).await
}

async fn send_edit_raw(
    app: axum::Router,
    key: &str,
    body: Value,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    send_raw_to(app, "/v1/images/edits", key, body).await
}

async fn send_raw_to(
    app: axum::Router,
    path: &str,
    key: &str,
    body: Value,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(header::CONTENT_TYPE, "application/json")
                .header("idempotency-key", key)
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    (status, headers, body.to_vec())
}

fn assert_error(response: (StatusCode, Value), status: StatusCode, code: &str) {
    assert_eq!(response.0, status);
    assert_eq!(response.1["error"]["code"], code);
}

fn config() -> AppConfig {
    AppConfig {
        bind: SocketAddr::from(([127, 0, 0, 1], 0)),
        auth_token: Some("test-token".to_string()),
        admin_token: Some("admin-token".to_string()),
        legacy_admin_auth_enabled: true,
        database_url: None,
        generation_admission_contract: Default::default(),
        enable_xai_video_api: false,
        five_hour_image_limit: 10,
        seven_day_image_limit: 50,
        five_hour_video_second_limit: i32::MAX as u32,
        seven_day_video_second_limit: i32::MAX as u32,
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
